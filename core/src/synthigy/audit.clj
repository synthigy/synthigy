(ns synthigy.audit
  "Audit substrate — protocol contract for `AuditProvider` implementations
   and the entity/relation opt-in policy that gates which mutations get
   persisted to `/history`.

  Substrate stores every opted-in entity- and relation-mutation event in
  two attribute-primary tables and answers the five `/history` ops:
    :get-at | :events | :diff | :timeline | :since

  Provider implementations ship as the observability substrate
  (`:synthigy/observability`, shadowed-namespace pattern). Operators pick
  the backend via alias on the classpath:
    :duckdb     — embedded columnar (OSS: dev + small-prod)
    :clickhouse — analytical scale (Pro)
  Both bind a single record implementing BOTH AuditProvider + LogStore.

  Drainer integration: when an audit provider is bound, the drainer calls
  the corresponding `write-*-deltas!` method synchronously per batch *before*
  deleting the source queue rows. The provider is the gate that decides
  which envelopes get persisted — filtering by the compiled audit policy
  (per-entity opt-in via `[:configuration :audit/persist]`, or the
  SYNTHIGY_AUDIT_ALL env override). Envelopes the provider rejects are
  still published to `delta/dispatch!` for live notifications and SDK
  cache invalidation; they just don't end up in `/history`.

  With no provider bound (live-only mode), the drainer publishes onto
  `delta/dispatch!` and deletes the queue rows unconditionally."
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [patcho.lifecycle :as lifecycle]
            [synthigy.log :as log]))

;; Dynamic binding holding the currently-loaded provider, or nil.
;; Each provider module sets this on start, clears on stop.
(defonce ^:dynamic *audit-provider* nil)

;; One-shot guard so the missing-sink warning fires at most once per process.
(defonce ^:private warned-missing-provider (atom false))

(defn warn-missing-provider!
  "Substrate drainers call this when a batch has envelopes to persist but
  `*audit-provider*` is nil.

  Running with no provider is a VALID live-only mode (see ns docstring), so
  this stays SILENT unless an audit sink was clearly intended: the
  `:synthigy/observability` module is registered (its backend jar is on the
  classpath) yet not started. In that case audit-log deltas are being
  drained and dropped, so warn ONCE to surface the misconfiguration."
  []
  (when (and (some? (lifecycle/module-info :synthigy/observability))
             (not (lifecycle/started? :synthigy/observability))
             (compare-and-set! warned-missing-provider false true))
    (log/warn {:id ::no-audit-provider
               :data {:action :writing :subject :iam-audit}}
              (str "Draining audit deltas with no :audit-provider bound while "
                   ":synthigy/observability is registered but not started — "
                   "audit-log writes are being DROPPED. Start :synthigy/observability "
                   "to persist them."))))

;; Drainer binds this to its open transaction before calling
;; `write-*-deltas!`. Providers must use this connectable (when bound) so
;; both queue DELETE and audit-store INSERTs land in the same atomic unit
;; on single-writer backends like SQLite. PG/CH providers can ignore it
;; (they have their own connectivity model).
(def ^:dynamic *audit-tx* nil)

(defprotocol AuditProvider
  "Seven-method contract for an audit substrate implementation. Writers are
  called synchronously by the drainer per batch (throwing on failure keeps
  queue rows for the next retry). Readers serve the `/history` endpoint."

  (write-entity-deltas! [this envelopes]
    "Write entity-track envelopes durably. Drainer batch.
     Each envelope's `:delta :data` carries :record-xid, :entity-xid, :tenant,
     :actor, :request, :scope, :ts plus :before/:after attribute maps.")

  (write-relation-deltas! [this envelopes]
    "Write relation-track envelopes durably. Drainer batch.
     Each envelope's `:delta :data` carries :from-xid, :to-xid, :tenant,
     :actor, :request, :scope, :ts.")

  (get-at [this opts]
    "Return a record's state as of timestamp T.
     opts: {:record-xid :at [:tenant?] [:include-deleted?]}
     Returns: {<attribute-xid> <value> ...} or nil if no rows.")

  (events [this opts]
    "Return events for a record (or any record) over a time range.
     opts: {:record-xid? :between [t1 t2] [:tenant?] [:limit] [:track :entity|:relation]}
     Returns: vector of normalized event maps.")

  (diff [this opts]
    "Diff a record's state between two timestamps.
     opts: {:record-xid :from-ts :to-ts [:tenant?]}
     Returns: {:before {...} :after {...} :changed [<attr-xid> ...]}")

  (timeline [this opts]
    "Return events grouped by :request, :actor, or :scope.
     opts: {:between [t1 t2] :group-by [:request|:actor|:scope] [:tenant?] [:limit]}
     Returns: map keyed by the chosen grouping → vector of events.")

  (since [this opts]
    "Return events strictly after cursor timestamp T, oldest-first.
     opts: {:cursor <ts> [:tenant?] [:limit] [:track]}
     Returns: vector of events."))

;; ============================================================================
;; Audit policy — entity/relation opt-in set, recompiled when the deployed
;; model changes. Providers consult these predicates inside their writer
;; methods to filter envelopes before persistence. The drainer is unchanged;
;; it publishes everything it drains regardless of policy.
;; ============================================================================

(defonce ^:private audit-policy
  (atom {:audit-all? false :entities #{} :relations #{}}))

(defn- audit-all-env?
  "Read SYNTHIGY_AUDIT_ALL via environ — same convention as the rest of the
   system (encryption, log/clickhouse, etc.). Truthy value (\"true\"/\"1\"/
   \"yes\", case-insensitive) flips the policy into audit-everything mode,
   overriding per-entity opt-in."
  []
  (when-let [v (env :synthigy-audit-all)]
    (contains? #{"true" "1" "yes"} (str/lower-case (str v)))))

(defn audit-policy-snapshot
  "Return the currently-compiled policy. For diagnostics / tests."
  []
  @audit-policy)

(defn audited-entity?
  "True iff envelopes for the given entity-xid should be persisted by the
   audit provider. False ⇒ live publish only (no /history row). Always
   returns a boolean.

   Pure predicate over the compiled policy. The IAM-lifecycle gate lives
   in `persistence-enabled?`, applied by the audit write path."
  [entity-xid]
  (let [p @audit-policy]
    (boolean (or (:audit-all? p)
                 (contains? (:entities p) entity-xid)))))

(defn audited-relation?
  "True iff envelopes for the given relation-xid should be persisted by
   the audit provider. A relation is audited when either endpoint entity
   opts in (or when SYNTHIGY_AUDIT_ALL is set). Always returns a boolean.

   Pure predicate over the compiled policy. See `persistence-enabled?`
   for the IAM-lifecycle gate."
  [relation-xid]
  (let [p @audit-policy]
    (boolean (or (:audit-all? p)
                 (contains? (:relations p) relation-xid)))))

(defn persistence-enabled?
  "Runtime gate consulted by the audit write path. When `:synthigy/iam`
   isn't in the running lifecycle, envelopes lack `:actor`/`:scope`
   attribution (no principal) so we skip persistence and keep `/history`
   clean. Live delta dispatch is unaffected — subscribers still receive
   every envelope.

   Kept separate from `audited-entity?` / `audited-relation?` so the
   compile-time policy predicates stay pure and unit-testable."
  []
  (boolean (lifecycle/started? :synthigy/iam)))

(defn- persist?
  "Truthy iff this entity opts into audit-substrate persistence. Inlined
   rather than calling `synthigy.dataset.core/audit-persist?` so this ns
   stays a leaf — no require on dataset.core. The model encodes the flag
   at `[:configuration :audit/persist]`; the helper there and this reader
   must move together."
  [entity]
  (boolean (get-in entity [:configuration :audit/persist])))

(defn- coerce-coll
  "Model entities/relations may be either a map (xid-keyed) or a sequential
   collection of records. Normalize to a seq of records."
  [coll]
  (cond
    (nil? coll)        nil
    (sequential? coll) coll
    (map? coll)        (vals coll)
    :else              (seq coll)))

(defn recompile-policy!
  "Walk `model`, compute the opted-in entity + relation xid sets, swap the
   policy atom. Re-reads SYNTHIGY_AUDIT_ALL each time so an operator can
   flip the override and trigger a recompile (e.g., redeploy) to pick it
   up without restarting the JVM. Safe on nil model (clears the sets).

   Pure function of the model + SYNTHIGY_AUDIT_ALL env. The IAM-running
   gate lives in `audited-entity?` / `audited-relation?` — the policy
   stays compiled so it's queryable for diagnostics, while predicates
   correctly return false when IAM is absent.

   All xids in the policy are nanoid strings — the substrate is xid-only
   by design; entity audit triggers are skipped under any other id mode."
  [model]
  (let [entities  (coerce-coll (some-> model :entities))
        relations (coerce-coll (some-> model :relations))
        audit-entities    (filter persist? entities)
        audit-entity-xids (into #{} (keep :xid) audit-entities)
        audit-relation-xids
        (into #{}
              (comp (filter (fn [rel]
                              (or (contains? audit-entity-xids (:from rel))
                                  (contains? audit-entity-xids (:to rel)))))
                    (keep :xid))
              relations)]
    (reset! audit-policy
            {:audit-all? (boolean (audit-all-env?))
             :entities  audit-entity-xids
             :relations audit-relation-xids})))
