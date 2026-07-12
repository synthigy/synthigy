(ns synthigy.server.subscription
  "Server-agnostic subscription management with full-set-replace semantics.

   Provides one REST endpoint that replaces the full subscription state
   for a session, plus an SSE event stream for receiving notifications.

   ## Endpoints

   POST /data/subscription/set     — set the full subscription state
   GET  /data/subscription/status  — list active subscriptions
   GET  /data/events               — SSE stream of filtered notifications

   ## Request body

   Single `\"subscriptions\"` array; each item carries an explicit
   `\"type\"`. The full array is the session state — items absent on
   subsequent POSTs are dropped. Empty array clears everything.

     POST /data/subscription/set
     {\"subscriptions\":
       [{\"type\":\"data\"
         \"records\":[\"u-abc\" \"u-def\"]
         \"operations\":[\"update\" \"link\"]}
        {\"type\":\"deployed-model\"}
        {\"type\":\"runtime-model\"}]}

   ## Subscription Types

   - \"data\"           — Per-record change events with the record-shaped
                         envelope (before/after/actor/txid/scope/tenant/
                         request). Required field: `records` (non-empty
                         array of xids). Optional: `operations`.
   - \"entity\"         — Coalesced cache-invalidation poke per entity.
                         Required field: `entities` (non-empty array of
                         entity names — case/whitespace folded
                         server-side). Optional: `operations`. Server
                         emits one `entity/touched` event per entity per
                         100ms window — provenance redacted (no
                         record-xid, no actor, no before/after). RBAC
                         gate at subscribe: principal must be able to
                         read each named entity. The `entity` field on
                         the poke is the client's verbatim original
                         string (echo-back).
   - \"relation\"       — Coalesced poke per relation (link/unlink).
                         Required field: `relations` (non-empty array of
                         `entity<sep>label` strings where `<sep>` is
                         `.`, `->`, ` - `, or bare `-`). Optional:
                         `operations`. Server emits one
                         `relation/touched` event per relation per 100ms
                         window — endpoints redacted. RBAC gate at
                         subscribe. The `relation` field on the poke
                         echoes the client's verbatim original string.
   - \"deployed-model\" — ERD model deployment events (raw shape)
   - \"runtime-model\"  — ERD model deployment events (augmented shape)

   ## Record-scoped semantics (data)

   Record events fire when the envelope's `record-xid` is in the
   subscriber's `records` set.

   Relation events fire when **either** endpoint of a link is in the
   subscriber's `records` set — the subscription doesn't care which
   side of the relation the record sits on. The wire carries a
   `data: [subscribed-xid, other-xid]` tuple where position 0 is the
   endpoint that landed the match (server rotates). When BOTH endpoints
   are in the subscriber's set the server emits two events, one per
   perspective, with rotated tuples and identical ts/txid/provenance.

   There is no `entity` field, no `relations` narrower, no firehose.
   A subscriber's surface is the xids they already loaded; envelopes
   carry no entity name decoration because the subscriber already knows
   what their xids are.

   ## Event Format (SSE)

   The SSE event field is always `\"data\"` for the data + entity +
   relation tracks; the wire `type` field carries the verb
   (`record/insert`, `relation/link`, `entity/touched`, etc.). Model
   deploy events use the subscription type as the SSE event name
   (`\"deployed-model\"` / `\"runtime-model\"`) — historical convention
   inconsistency, not a bug. See the subscriptions-substrate memo for
   the full record-shaped wire spec (2026-06-01).

   ## Session lifetime

   A session lives between the client's POST /set and the close of its
   last SSE event stream. When the last `/data/events` connection closes
   the identity is torn down immediately — every delta subscription is
   unregistered, the batch + model channels are closed. Reconnect = open
   a new SSE + POST /set again."
  (:require
   [clojure.core.async :as async]
   [clojure.set :as set]
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.iam.access :as access]
   [synthigy.json :as json]
   [synthigy.server.auth :as auth]
   [synthigy.server.data :as data]
   [synthigy.server.subscriptions.notifier :as notifier]
   [patcho.lifecycle :as lifecycle])
  (:import
   [java.time Instant]))

(declare ensure-delta-loop! ensure-model-loop! cleanup-identity!
         ensure-model-chan fan-out!)

;;; ============================================================================
;;; Configuration
;;; ============================================================================

(def supported-types
  "Subscription types currently implemented.

  - \"data\"           — Per-record envelopes (record-shaped 2026-06-01).
                         Required: `records`. Optional: `operations`.
                         Emits `record/insert`/`update`/`delete` and
                         `relation/link`/`unlink` with the trim spec —
                         no entity-name / element / from-eid / to-eid
                         decoration; relation events ship
                         `data: [yours, other]`.
  - \"entity\"         — Coalesced cache-invalidation pokes per entity
                         name. Required: `entities`. Optional:
                         `operations`. Emits `entity/touched` carrying
                         only `{type, entity, ts}`; `entity` echoes the
                         client's verbatim original string.
  - \"relation\"       — Coalesced pokes per relation; names accept any
                         separator in `.`, `->`, ` - `, or `-` between
                         entity and label. Required: `relations`.
                         Optional: `operations`. Emits `relation/touched`
                         echoing the client's verbatim original string.
  - \"deployed-model\" — raw deployed model deploys (modeler).
  - \"runtime-model\"  — augmented runtime model deploys (data console).

  Both model subscription types share one publisher topic
  (:model/deployed) and one model-loop per identity; the loop fans out
  one event per *type* the identity is subscribed to."
  #{"data" "entity" "relation" "deployed-model" "runtime-model"})

;;; ============================================================================
;;; State
;;; ============================================================================

(defonce ^:private subscriptions-state (atom {}))

;; SSE output channels per identity: {identity-key #{<channels>}}
(defonce ^:private event-streams (atom {}))

;; Per-identity set of running loop kinds:
;; {identity-key #{:data :entity :relation :model}}
;; Replaces four parallel sets so cleanup is a single swap!.
(defonce ^:private loops (atom {}))

(defn- claim-loop!
  "Atomically claim the loop slot for [identity-key kind]. Returns true iff
   this caller won the slot (the kind was not already running). Collapses the
   former loop-running?→start-loop! check-then-act into one swap! so two
   concurrent SSE connects for the same identity can't both spawn a go-loop."
  [identity-key kind]
  (let [[old _] (swap-vals! loops update identity-key (fnil conj #{}) kind)]
    (not (contains? (get old identity-key) kind))))

(defn- end-loop! [identity-key kind]
  (swap! loops (fn [m]
                 (let [s' (disj (get m identity-key #{}) kind)]
                   (if (seq s')
                     (assoc m identity-key s')
                     (dissoc m identity-key))))))

;;; ============================================================================
;;; Identity
;;; ============================================================================

(defn request-identity
  "Extract subscription identity key from the request + authenticated
   context. Returns [sub client_id] tuple.

   With IAM off (bare server) there are no claims — identity falls back
   to the client-supplied `x-synthigy-client` header (or `client` query
   param) so independent bare consumers get separate sessions. Unnamed
   bare clients all share one \"anonymous\" session: last /set wins and
   closing the last SSE stream tears it down for everyone — name your
   client if you run more than one."
  [request iam]
  (if iam
    (let [{:keys [sub client_id]} (:claims iam)]
      [(or sub (some-> (:principal iam) id/extract)) client_id])
    ["anonymous" (or (get-in request [:headers "x-synthigy-client"])
                     (get-in request [:query-params "client"]))]))

;;; ============================================================================
;;; Subscription Management
;;; ============================================================================

(defn- data-sub-key
  "delta-registry key for an identity's data subscription. One subscription
   per identity, scoped by the union of every entity + relation the identity
   has currently asked for. Re-registering with this key replaces the prior
   interest in delta's registry."
  [identity-key]
  [::data identity-key])

;; Data-track drop→resync, mirroring the notifier (/subscribe) path. The
;; batch-chan is now a FIXED buffer so `offer!` returns false on overflow (the
;; drop signal); `:lagged` flags it, and the go-loop heals in place by replaying
;; missed record events from the durable store. Bounded so a persistently-slow
;; client can't loop forever. (The entity/relation/model tracks stay sliding —
;; they deliver idempotent pokes where a dropped signal is harmless.)
(def ^:private max-data-resyncs 5)

(defn- ensure-batch-chan
  "Per-identity FIXED-buffer channel onto which the delta handler offers
   incoming envelopes; the delta processing go-loop pulls from it, translates
   each envelope into an SSE event, fans out. Also lazily allocates the
   per-identity resync atoms (`:lagged` / `:last-seq` / `:resyncs`). Created on
   first set call; closed on cleanup-identity! Atomic: concurrent callers
   converge via swap!'s CAS (extra allocations get GC'd)."
  [identity-key]
  (-> (swap! subscriptions-state update-in [identity-key :data]
             (fn [d]
               (cond-> (or d {})
                 (nil? (:batch-chan d)) (assoc :batch-chan (async/chan 100))
                 (nil? (:lagged d))     (assoc :lagged   (atom false))
                 (nil? (:last-seq d))   (assoc :last-seq (atom 0))
                 (nil? (:resyncs d))    (assoc :resyncs  (atom 0)))))
      (get-in [identity-key :data :batch-chan])))

(defn- flag-data-lagged!
  "Mark the identity's data stream as having dropped a live event."
  [identity-key]
  (some-> (get-in @subscriptions-state [identity-key :data :lagged]) (reset! true)))

(defn- collect-data-interest
  "Walk the identity's current :data state and compute the substrate
   interest descriptor. With the records-only spec, a subscriber's union
   of watched record xids serves BOTH the entity track (as
   `:record-xids`) and the relation track (as `:endpoint-xids`, OR-match
   on either side of the relation). Empty when nothing is subscribed."
  [identity-key]
  (let [records (get-in @subscriptions-state [identity-key :data :records])
        ops     (get-in @subscriptions-state [identity-key :data :operations])
        ops-set (when (seq ops)
                  ;; Expand the coarse vocabulary into fine ops so the
                  ;; substrate matcher (which sees `:insert`/`:update`/
                  ;; `:delete`/`:link`/`:unlink`) matches what the
                  ;; subscriber asked for.
                  (set (mapcat (fn [op]
                                 (case op
                                   "change" [:insert :update]
                                   "delete" [:delete]
                                   "insert" [:insert]
                                   "update" [:update]
                                   "link"   [:link]
                                   "unlink" [:unlink]
                                   ;; Unknown op — passed through as
                                   ;; keyword so the matcher rejects it
                                   ;; cleanly rather than silently matching.
                                   [(keyword op)]))
                               ops)))]
    (cond-> {}
      (seq records) (assoc :record-xids   records
                           :endpoint-xids records)
      ops-set       (assoc :ops ops-set))))

(defn- recompute-data-subscription!
  "Re-register the identity's single delta subscription with the current
   union interest. delta/subscribe! under the same key replaces the prior
   binding atomically. When nothing is subscribed, unsubscribe entirely.
   Idempotent — safe to call after every subscription set!."
  [identity-key]
  (let [interest (collect-data-interest identity-key)
        sub-key  (data-sub-key identity-key)]
    (if (seq interest)
      (let [ch (ensure-batch-chan identity-key)]
        (delta/subscribe!
         sub-key interest
         ;; Fixed batch-chan: offer! false on overflow = a drop here too.
         (fn [envelope]
           (when-not (async/offer! ch envelope)
             (flag-data-lagged! identity-key)))
         ;; on-drop from delta's own fixed buffer (upstream of the batch-chan).
         (fn [_env] (flag-data-lagged! identity-key))))
      (delta/unsubscribe! sub-key))))

(defn- data-resync-envelopes
  "If the identity's data stream was flagged lagged, replay the record events
   it missed from the durable store — reconstructed by the shared
   `notifier/replay-envelopes` and filtered to this subscriber's interest with
   the SAME `delta/matches?` the live path uses (no leak). Returns the
   envelopes to fold into the current batch so they translate + fan out
   identically; nil otherwise. CAS so one go-loop pass wins; bounded by
   `max-data-resyncs`, then degrade (client reconnects + re-/sets to catch up)."
  [identity-key]
  (let [{:keys [lagged last-seq resyncs]} (get-in @subscriptions-state [identity-key :data])]
    (when (and lagged (compare-and-set! lagged true false))
      (if (< @resyncs max-data-resyncs)
        (let [interest (collect-data-interest identity-key)
              envs     (filterv #(delta/matches? interest %)
                                (notifier/replay-envelopes @last-seq 10000))]
          (swap! resyncs inc)
          (log/debug {:id ::data-resync
                      :data {:action :recovering :subject :delta
                             :from @last-seq :count (count envs)}}
                     "Healed dropped /data/events records via store replay")
          envs)
        (do (log/warn {:id ::data-resync-budget-exhausted
                       :data {:action :recovering :subject :delta}}
                      "Data resync budget exhausted; client should reconnect")
            nil)))))

(defn- normalize-data-item
  "Validate a `{:type \"data\" ...}` subscription item under the records-
   only spec.

   Required:
     - `records`: non-empty array of xid strings.
   Optional:
     - `operations`: array of vocab strings (coarse: `change`/`delete`;
       fine: `insert`/`update`/`delete`/`link`/`unlink`).

   Rejects the legacy entity-firehose fields (`entity`, `relations`,
   `firehose`) with 400 + a clear migration message — there's no
   transparent shim because the semantics differ. Returns a normalized
   record `{:records #{...} :operations #{...}|nil}`."
  [item]
  ;; Hard-reject the legacy entity-firehose form on `data` items. Now
  ;; that `entity` and `relation` are first-class subscription types,
  ;; finding `entity` / `relations` on a `{type: \"data\"}` item is a
  ;; caller mistake (mixed-up payload) — flag it loud.
  (let [legacy-keys (set/intersection (set (keys item))
                                      #{"entity" "entities" "relations" "firehose"
                                        :entity :entities :relations :firehose})]
    (when (seq legacy-keys)
      (throw (ex-info (str "`data` subscription items take `records` only. "
                           "Found " (pr-str legacy-keys) " — did you mean "
                           "`{type: \"entity\", entities: [...]}` or "
                           "`{type: \"relation\", relations: [...]}` ?")
                      {:code "INVALID_SUBSCRIPTION"
                       :unknown-fields legacy-keys}))))
  (let [records-spec (or (get item "records") (get item :records))
        operations   (when-let [ops (or (get item "operations") (get item :operations))]
                       (when-not (sequential? ops)
                         (throw (ex-info "operations must be an array"
                                         {:code "INVALID_OPERATIONS"})))
                       (set ops))]
    (when (or (nil? records-spec) (not (sequential? records-spec)))
      (throw (ex-info "Missing required field `records` on data subscription item"
                      {:code "MISSING_RECORDS"})))
    (let [records (into #{} (filter string?) records-spec)]
      (when (empty? records)
        (throw (ex-info "`records` must be a non-empty array of xid strings"
                        {:code "EMPTY_RECORDS"})))
      {:records    records
       :operations operations})))

;;; ============================================================================
;;; Entity + Relation Track Management
;;; ============================================================================
;;;
;;; The `data` track delivers per-row envelopes filtered by record xids,
;;; with the record-shaped envelope (before/after/actor/txid/scope/tenant/
;;; request — attribute-key-keyed). The `entity` and `relation` tracks are
;;; the cache-invalidation siblings: the subscriber declares which entities
;;; OR relations it cares about, and gets a coalesced `entity/touched` or
;;; `relation/touched` poke per name per 100ms window. Provenance is fully
;;; redacted — no record xid, no actor, no before/after, no op verb. The
;;; consumer re-queries to learn whatever it needs.
;;;
;;; RBAC at subscribe time: the principal must be able to read the entity
;;; (or both sides of the relation). Cheap one-shot check that prevents
;;; entity-name enumeration. There is no per-envelope RLS — too expensive
;;; on the delivery path, and unnecessary since the poke leaks no
;;; record-level information.
;;;
;;; The substrate matcher already supports `:entity-xids` and
;;; `:relation-xids` narrowing (delta.clj). The two tracks differ in:
;;;   - what interest they build (entity-xids vs relation-xids)
;;;   - what they emit (entity/touched vs relation/touched)
;;;   - what they coalesce by (entity name vs relation name)

;; ── shared utilities ─────────────────────────────────────────────────────────

(defn- coalesce-loop!
  "Spin a per-identity go-loop draining `batch-chan`, batching for 100ms,
   running `translate-fn` over every envelope, coalescing by
   `coalesce-key-fn`, then fanning out via fan-out!. `loop-kind` is the
   keyword (:entity or :relation) registered in the shared `loops` atom
   to keep one loop per identity-key per kind."
  [identity-key loop-kind batch-chan translate-fn coalesce-key-fn]
  (when (claim-loop! identity-key loop-kind)
    (async/go-loop []
      (if-let [first-env (async/<! batch-chan)]
        (let [batch (loop [batch [first-env]]
                      (let [[val port] (async/alts! [batch-chan (async/timeout 100)])]
                        (if (and val (= port batch-chan))
                          (recur (conj batch val))
                          batch)))
              translated (into [] (keep translate-fn) batch)
              coalesced  (->> translated
                              (group-by coalesce-key-fn)
                              (vals)
                              (mapv last))]
          (when (seq coalesced)
            (fan-out! identity-key coalesced))
          (recur))
        (end-loop! identity-key loop-kind)))))

(defn- ensure-track-batch-chan!
  "Per-identity sliding channel for `track-key` (`:entity` or `:relation`).
   Atomic — see ensure-batch-chan."
  [identity-key track-key]
  (-> (swap! subscriptions-state update-in [identity-key track-key :batch-chan]
             #(or % (async/chan (async/sliding-buffer 100))))
      (get-in [identity-key track-key :batch-chan])))

;; ── entity track ─────────────────────────────────────────────────────────────

(defn- entity-sub-key   [identity-key] [::entity   identity-key])
(defn- relation-sub-key [identity-key] [::relation identity-key])

;; Wire accepts any of these between entity and label:
;;   "Movie.actors"        — dot (XSQL-adjacent)
;;   "Movie->actors"       — arrow (matches XSQL join syntax)
;;   "User Group - users"  — natural-language form (space-dash-space)
;;   "user_group-users"    — bare dash between code-style tokens
;; Each half is then run through sql-query/normalize-entity-name (lowercase,
;; whitespace→underscore, camelCase→snake_case) before lookup, so
;; "User Group - users", "user_group-users", "user group ->users",
;; "userGroup.users" all resolve to the same relation-id.
(def ^:private relation-separator-re
  #"\s*(?:->|\.|\s-\s)\s*|-")

(defn- split-relation-name
  "Split a wire-supplied relation reference into `[entity-part label-part]`.
   Returns nil if no recognized separator is present."
  [s]
  (let [s (str/trim s)
        parts (str/split s relation-separator-re 2)]
    (when (= 2 (count parts))
      (mapv str/trim parts))))

(def ^:private allowed-entity-fields
  #{"type" "entities" "operations"
    :type :entities :operations})

(def ^:private allowed-relation-fields
  #{"type" "relations" "operations"
    :type :relations :operations})

(defn- parse-operations
  "Parse the optional `operations` field on an entity/relation item.
   Returns a set of fine-op keywords matching the substrate matcher's
   `:ops` vocabulary, or nil when absent (= no narrowing)."
  [item]
  (when-let [ops (or (get item "operations") (get item :operations))]
    (when-not (sequential? ops)
      (throw (ex-info "operations must be an array"
                      {:code "INVALID_OPERATIONS"})))
    (set (mapcat (fn [op]
                   (case op
                     "change" [:insert :update]
                     "delete" [:delete]
                     "insert" [:insert]
                     "update" [:update]
                     "link"   [:link]
                     "unlink" [:unlink]
                     [(keyword op)]))
                 ops))))

(defn- normalize-entity-item
  "Validate a `{:type \"entity\" ...}` item. Resolves each name to its
   entity-xid via the deployed schema; checks RBAC read on each entity;
   throws UNKNOWN_ENTITY for typos or ENTITY_NOT_READABLE for IAM denials.

   Returns `{:name-by-xid {xid client-string ...} :ops #{...}|nil}` —
   keys are the substrate-matching xids, values are the literal strings
   the client supplied so SSE pokes can echo them back verbatim
   (first-write-wins when multiple equivalent forms resolve to the same
   xid)."
  [item]
  (let [unknown-keys (set/difference (set (keys item)) allowed-entity-fields)]
    (when (seq unknown-keys)
      (throw (ex-info (str "Unknown fields on entity subscription item: "
                           (pr-str unknown-keys))
                      {:code "INVALID_SUBSCRIPTION"
                       :unknown-fields unknown-keys}))))
  (let [entities-spec (or (get item "entities") (get item :entities))]
    (when (or (nil? entities-spec) (not (sequential? entities-spec)))
      (throw (ex-info "Missing required field `entities` on entity subscription item"
                      {:code "MISSING_ENTITIES"})))
    (let [names (into [] (filter string?) entities-spec)]
      (when (empty? names)
        (throw (ex-info "`entities` must be a non-empty array of entity name strings"
                        {:code "EMPTY_ENTITIES"})))
      (let [name-by-xid
            (reduce (fn [acc n]
                      (let [eid (sql-query/resolve-entity n)]
                        (when-not (daccess/entity-allows? eid #{:read})
                          (throw (ex-info (str "No read access to entity: " n)
                                          {:code "ENTITY_NOT_READABLE"
                                           :entity n})))
                        (if (contains? acc eid)
                          acc                ; first-write-wins on duplicates
                          (assoc acc eid n))))
                    {}
                    names)]
        {:name-by-xid name-by-xid
         :ops         (parse-operations item)}))))

(defn- normalize-relation-item
  "Validate a `{:type \"relation\" ...}` item. Relation references are
   `entity<sep>label` where <sep> is any of `.`, `->`, ` - `, or bare `-`
   (see relation-separator-re). Both halves go through
   normalize-entity-name (case/whitespace folded), so all four forms below
   resolve to the same relation:

     \"User Group - users\"
     \"user_group-users\"
     \"user group ->users\"
     \"User Group.users\"

   RBAC: principal must read the relation.

   Returns `{:name-by-xid {xid client-string ...} :ops #{...}|nil}` —
   first-write-wins on duplicates (client's first form for each unique
   relation is what gets echoed in SSE pokes)."
  [item]
  (let [unknown-keys (set/difference (set (keys item)) allowed-relation-fields)]
    (when (seq unknown-keys)
      (throw (ex-info (str "Unknown fields on relation subscription item: "
                           (pr-str unknown-keys))
                      {:code "INVALID_SUBSCRIPTION"
                       :unknown-fields unknown-keys}))))
  (let [relations-spec (or (get item "relations") (get item :relations))]
    (when (or (nil? relations-spec) (not (sequential? relations-spec)))
      (throw (ex-info "Missing required field `relations` on relation subscription item"
                      {:code "MISSING_RELATIONS"})))
    (let [names (into [] (filter string?) relations-spec)]
      (when (empty? names)
        (throw (ex-info "`relations` must be a non-empty array of entity/label strings"
                        {:code "EMPTY_RELATIONS"})))
      (let [name-by-xid
            (reduce (fn [acc n]
                      (let [[entity-part label] (split-relation-name n)]
                        (when (or (str/blank? entity-part)
                                  (str/blank? label))
                          (throw (ex-info
                                  (str "Relation must be `entity<sep>label` "
                                       "where <sep> is `.`, `->`, ` - `, or `-`: " n)
                                  {:code "INVALID_RELATION_NAME"
                                   :relation n})))
                        (let [rel-id (sql-query/resolve-relation entity-part label)]
                          (when-not (daccess/relation-allows? rel-id #{:read})
                            (throw (ex-info (str "No read access to relation: " n)
                                            {:code "RELATION_NOT_READABLE"
                                             :relation n})))
                          (if (contains? acc rel-id)
                            acc              ; first-write-wins on duplicates
                            (assoc acc rel-id n)))))
                    {}
                    names)]
        {:name-by-xid name-by-xid
         :ops         (parse-operations item)}))))

;; ── recompute (re-register substrate subscription) ───────────────────────────

(defn- recompute-entity-subscription!
  [identity-key]
  (let [names   (get-in @subscriptions-state [identity-key :entity :name-by-xid])
        xids    (set (keys names))
        ops     (get-in @subscriptions-state [identity-key :entity :ops])
        sub-key (entity-sub-key identity-key)]
    (if (seq xids)
      (let [ch (ensure-track-batch-chan! identity-key :entity)]
        (delta/subscribe!
         sub-key (cond-> {:entity-xids xids} (seq ops) (assoc :ops ops))
         (fn [envelope] (async/offer! ch envelope))))
      (delta/unsubscribe! sub-key))))

(defn- recompute-relation-subscription!
  [identity-key]
  (let [names   (get-in @subscriptions-state [identity-key :relation :name-by-xid])
        xids    (set (keys names))
        ops     (get-in @subscriptions-state [identity-key :relation :ops])
        sub-key (relation-sub-key identity-key)]
    (if (seq xids)
      (let [ch (ensure-track-batch-chan! identity-key :relation)]
        (delta/subscribe!
         sub-key (cond-> {:relation-xids xids} (seq ops) (assoc :ops ops))
         (fn [envelope] (async/offer! ch envelope))))
      (delta/unsubscribe! sub-key))))

;; ── translate (substrate envelope → SSE event) ───────────────────────────────

(defn- translate-entity-delta
  "Stripped SSE shape for entity pokes: `{type, entity, ts}` only. No
   record-xid, no actor, no txid, no scope/tenant, no op verb (coalesced
   events can mix ops). The `entity` field is the client's verbatim
   original string — echoed from per-identity `name-by-xid`. Returns nil
   if the envelope's entity-xid isn't in the subscriber's set (shouldn't
   happen — substrate already gated)."
  [{:keys [delta]} name-by-xid]
  (when-let [client-name (get name-by-xid (-> delta :data :entity-xid))]
    {:event "data"
     :data  {:type   "entity/touched"
             :entity client-name
             :ts     (-> delta :data :ts)}}))

(defn- translate-relation-delta
  "Stripped SSE shape for relation pokes: `{type, relation, ts}` only.
   The `relation` field is the client's verbatim original string. Returns
   nil if the envelope's relation-xid isn't in the subscriber's set."
  [{:keys [element delta]} name-by-xid]
  (when-let [client-name (get name-by-xid element)]
    {:event "data"
     :data  {:type     "relation/touched"
             :relation client-name
             :ts       (-> delta :data :ts)}}))

;; ── per-identity drain loops ─────────────────────────────────────────────────

(defn- ensure-entity-delta-loop!
  [identity-key]
  (let [batch-chan (get-in @subscriptions-state [identity-key :entity :batch-chan])
        translate  (fn [env]
                     (translate-entity-delta
                      env
                      (get-in @subscriptions-state [identity-key :entity :name-by-xid])))]
    (coalesce-loop! identity-key :entity batch-chan
                    translate (comp :entity :data))))

(defn- ensure-relation-delta-loop!
  [identity-key]
  (let [batch-chan (get-in @subscriptions-state [identity-key :relation :batch-chan])
        translate  (fn [env]
                     (translate-relation-delta
                      env
                      (get-in @subscriptions-state [identity-key :relation :name-by-xid])))]
    (coalesce-loop! identity-key :relation batch-chan
                    translate (comp :relation :data))))

;; Pure state-transformer fns — one per track. Each takes the full
;; subscriptions-state map and returns the next one. set-subscriptions!
;; threads all three through a single swap!, so the whole "what does the
;; session look like after this POST" computation is one atomic step
;; instead of three (and they're trivially testable without an atom).
(defn- apply-data-track
  [state identity-key validated records ops]
  (if (seq validated)
    (-> state
        (assoc-in [identity-key :data :records]    records)
        (assoc-in [identity-key :data :operations] ops))
    (cond-> state
      (get-in state [identity-key :data])
      (update-in [identity-key :data] dissoc :records :operations))))

(defn- apply-entity-track
  [state identity-key validated name-by-xid ops]
  (if (seq validated)
    (-> state
        (assoc-in [identity-key :entity :name-by-xid] name-by-xid)
        (assoc-in [identity-key :entity :ops]         ops))
    (cond-> state
      (get-in state [identity-key :entity])
      (update-in [identity-key :entity] dissoc :name-by-xid :ops))))

(defn- apply-relation-track
  [state identity-key validated name-by-xid ops]
  (if (seq validated)
    (-> state
        (assoc-in [identity-key :relation :name-by-xid] name-by-xid)
        (assoc-in [identity-key :relation :ops]         ops))
    (cond-> state
      (get-in state [identity-key :relation])
      (update-in [identity-key :relation] dissoc :name-by-xid :ops))))

(defn- replace-model-state!
  "Apply the model-subscription side of a set call. `desired-types` is the
   set of model subscription types the identity now wants — anything in
   the prior state but not here is torn down."
  [identity-key desired-types principal]
  (if (seq desired-types)
    (do (ensure-model-chan identity-key)
        (swap! subscriptions-state assoc-in [identity-key :model :types]
               desired-types)
        (ensure-model-loop! identity-key principal))
    (do (when-let [ch (get-in @subscriptions-state [identity-key :model :chan])]
          (async/unsub dataset/publisher :model/deployed ch)
          (async/close! ch))
        (swap! subscriptions-state update identity-key dissoc :model))))

(defn set-subscriptions!
  "Replace the full subscription state for `identity-key` with the items
   in the request body. Items must be a `\"subscriptions\"` array, each
   carrying an explicit `\"type\"`:

     {\"type\" \"data\"     \"records\"   [...] \"operations\" [...]}
     {\"type\" \"entity\"   \"entities\"  [...] \"operations\" [...]}
     {\"type\" \"relation\" \"relations\" [...] \"operations\" [...]}
     {\"type\" \"deployed-model\"}
     {\"type\" \"runtime-model\"}

   Multiple items of the same type union together. Anything in the prior
   state but not in the new array is dropped. An empty array clears the
   session. Returns `{:ok true}`."
  [identity-key body principal]
  (access/with-principal principal
    (let [items (or (get body "subscriptions") (get body :subscriptions) [])
          item-type (fn [item] (or (get item "type") (get item :type) "data"))]
      (doseq [item items]
        (let [t (item-type item)]
          (when-not (supported-types t)
            (throw (ex-info (str "Unsupported subscription type: " t)
                            {:code "UNSUPPORTED_TYPE" :type t
                             :supported (vec supported-types)})))))
      (let [data-items     (filter #(= "data"     (item-type %)) items)
            entity-items   (filter #(= "entity"   (item-type %)) items)
            relation-items (filter #(= "relation" (item-type %)) items)
            model-types    (into #{}
                                 (keep (fn [item]
                                         (let [t (item-type item)]
                                           (when (#{"deployed-model" "runtime-model"} t) t))))
                                 items)
            validated-data     (mapv normalize-data-item     data-items)
            validated-entity   (mapv normalize-entity-item   entity-items)
            validated-relation (mapv normalize-relation-item relation-items)
            ;; Union per track. Records (data track) are sets; name maps
            ;; (entity/relation tracks) are {xid client-string} where
            ;; first-wins preserves the form the client sent earliest in
            ;; the array if equivalents arrive across items.
            first-wins (fn [a _b] a)
            all-records (reduce set/union #{} (map :records validated-data))
            all-operations (if (and (seq validated-data)
                                    (every? :operations validated-data))
                             (reduce set/union #{} (map :operations validated-data))
                             nil)
            all-entity-names   (reduce (partial merge-with first-wins) {}
                                       (map :name-by-xid validated-entity))
            all-entity-ops     (when (and (seq validated-entity) (every? :ops validated-entity))
                                 (reduce set/union #{} (map :ops validated-entity)))
            all-relation-names (reduce (partial merge-with first-wins) {}
                                       (map :name-by-xid validated-relation))
            all-relation-ops   (when (and (seq validated-relation) (every? :ops validated-relation))
                                 (reduce set/union #{} (map :ops validated-relation)))]
        ;; data / entity / relation tracks — one atomic transition.
        (swap! subscriptions-state
               (fn [s]
                 (-> s
                     (apply-data-track     identity-key validated-data     all-records         all-operations)
                     (apply-entity-track   identity-key validated-entity   all-entity-names    all-entity-ops)
                     (apply-relation-track identity-key validated-relation all-relation-names  all-relation-ops))))
        ;; model track — owns its own channel + go-loop lifecycle, can't be folded.
        (replace-model-state! identity-key model-types principal)
        ;; recompute every substrate sub
        (recompute-data-subscription!     identity-key)
        (recompute-entity-subscription!   identity-key)
        (recompute-relation-subscription! identity-key)
        (let [[user-xid client-id] identity-key]
          (log/info {:id ::subscription-set
                     :user-xid user-xid
                     :data {:action :set
                            :subject :subscription
                            :client client-id
                            :record-count   (count all-records)
                            :entity-count   (count all-entity-names)
                            :relation-count (count all-relation-names)
                            :operations (when all-operations (vec all-operations))
                            :model-tracks (when (seq model-types) (vec model-types))}}
                    "Subscription state replaced"))
        (cond
          (and (empty? validated-data)
               (empty? validated-entity)
               (empty? validated-relation)
               (empty? model-types))
          (cleanup-identity! identity-key)

          :else
          (do (when (seq validated-data)     (ensure-delta-loop!          identity-key))
              (when (seq validated-entity)   (ensure-entity-delta-loop!   identity-key))
              (when (seq validated-relation) (ensure-relation-delta-loop! identity-key))))
        {:ok true}))))

(defn- model-fingerprint
  "Compute set of visible entity+relation IDs for a principal's access context."
  [model principal]
  (access/with-principal principal
    (let [entity-ids (into #{}
                           (comp (map id/extract)
                                 (filter #(daccess/entity-allows? % #{:read})))
                           (core/get-entities model))
          relation-ids (into #{}
                             (keep (fn [rel]
                                     (let [rid (id/extract rel)
                                           fid (id/extract (:from rel))
                                           tid (id/extract (:to rel))]
                                       (when (or (daccess/relation-allows? rid [fid tid] #{:read})
                                                 (daccess/relation-allows? rid [tid fid] #{:read}))
                                         rid))))
                             (core/get-relations model))]
      (into entity-ids relation-ids))))

(defn- ensure-model-chan
  "Get or create the model event channel for an identity. Atomic: uses
   swap-vals! so only the thread that actually installed the channel runs
   async/sub (sub-ing the same chan twice would register it twice on the
   publisher)."
  [identity-key]
  (let [[old new] (swap-vals! subscriptions-state update-in
                              [identity-key :model :chan]
                              #(or % (async/chan (async/sliding-buffer 10))))
        ch        (get-in new [identity-key :model :chan])]
    (when-not (get-in old [identity-key :model :chan])
      (async/sub dataset/publisher :model/deployed ch))
    ch))

(defn subscription-status
  "Returns current subscriptions for identity, grouped by type. Entity and
   relation tracks report the client's verbatim original strings, so
   `subscription-status` is the exact echo of what the client submitted.
   Echoed name lists are sorted so repeated calls return stable output."
  [identity-key]
  (let [sub            (get @subscriptions-state identity-key)
        data-state     (:data sub)
        entity-state   (:entity sub)
        relation-state (:relation sub)
        data-sub       (when (seq (:records data-state))
                         (cond-> {:type "data"
                                  :records (vec (:records data-state))}
                           (seq (:operations data-state))
                           (assoc :operations (vec (:operations data-state)))))
        entity-sub     (when (seq (:name-by-xid entity-state))
                         (cond-> {:type "entity"
                                  :entities (vec (sort (vals (:name-by-xid entity-state))))}
                           (seq (:ops entity-state))
                           (assoc :operations (mapv name (:ops entity-state)))))
        relation-sub   (when (seq (:name-by-xid relation-state))
                         (cond-> {:type "relation"
                                  :relations (vec (sort (vals (:name-by-xid relation-state))))}
                           (seq (:ops relation-state))
                           (assoc :operations (mapv name (:ops relation-state)))))
        model-subs     (for [t (get-in sub [:model :types])] {:type t})]
    {:subscriptions (vec (cond->> model-subs
                           data-sub     (cons data-sub)
                           entity-sub   (cons entity-sub)
                           relation-sub (cons relation-sub)))}))

;;; ============================================================================
;;; Delta → Event Translation
;;; ============================================================================

(defn- translate-attributes
  "Rewrite a `before` / `after` map from attribute-xid keys (substrate
   capture shape) to attribute-key keys (user-facing /data shape) via the
   shared attribute-key-index. Unrecognized keys pass through untouched
   so envelope evolution doesn't drop data."
  [attrs attr-key-index]
  (when attrs
    (reduce-kv (fn [m k v]
                 (assoc m (or (get attr-key-index k) k) v))
               {}
               attrs)))

(defn- translate-delta
  "Translate a substrate envelope into a vector of SSE-shaped maps (0,
   1, or 2 entries).

   The substrate matcher has already gated by `records` + `operations`
   before the envelope lands here, so the translator's only job is to
   shape the SSE payload — strip modeling-internal decoration, rename the
   wire type namespace, translate attribute keys, and (for relations)
   resolve from-the-subscriber's-perspective endpoint ordering.

   Two-events rule for relations: if BOTH endpoints are in the
   subscriber's records set, emit two events with `:data` rotated
   (subscriber gets one poke per watched xid). Same ts/txid/provenance.

   Wire shape per event:

     record event ({substrate :entity/<op>} → wire `record/<op>`):
       {:type \"record/insert\" | \"record/update\" | \"record/delete\"
        :record-xid <xid>
        :before {<attr-key> v ...}      ; absent on insert
        :after  {<attr-key> v ...}      ; absent on delete
        :ts :tenant :scope :actor :request :txid}

     relation event ({substrate :relation/<op>}):
       {:type \"relation/link\" | \"relation/unlink\"
        :data [<subscribed-xid> <other-xid>]
        :ts :tenant :scope :actor :request :txid}"
  [{:keys [delta]} attr-key-index data-records]
  (let [{:keys [type data]} delta
        track (some-> type namespace)
        op    (some-> type name)]
    (case track
      "entity"
      (when op
        (let [base (-> (select-keys data [:ts :tenant :scope :actor :request :txid])
                       (assoc :type       (str "record/" op)
                              :record-xid (:record-xid data)))
              with-before (cond-> base
                            (contains? data :before)
                            (assoc :before (translate-attributes
                                            (:before data) attr-key-index)))
              with-after  (cond-> with-before
                            (contains? data :after)
                            (assoc :after (translate-attributes
                                           (:after data) attr-key-index)))]
          [{:event "data" :data with-after}]))

      "relation"
      (when (#{"link" "unlink"} op)
        (let [from-xid (:from-xid data)
              to-xid   (:to-xid data)
              from?    (contains? data-records from-xid)
              to?      (contains? data-records to-xid)
              base     (-> (select-keys data [:ts :tenant :scope :actor :request :txid])
                           (assoc :type (str "relation/" op)))
              event-for (fn [subscribed other]
                          {:event "data"
                           :data  (assoc base :data [subscribed other])})]
          (cond
            (and from? to?) [(event-for from-xid to-xid)
                             (event-for to-xid from-xid)]
            from?           [(event-for from-xid to-xid)]
            to?             [(event-for to-xid from-xid)]
            ;; Neither endpoint subscribed — shouldn't happen (substrate
            ;; matcher already gated by `:endpoint-xids`); drop quietly.
            :else           [])))

      nil)))

;;; ============================================================================
;;; Event Streams (SSE Output)
;;; ============================================================================

(defn create-event-stream
  "Create a new SSE output channel for an identity.
   Returns the channel."
  [identity-key]
  (let [ch (async/chan (async/sliding-buffer 50))
        [user-xid client-id] identity-key
        stream-count (inc (count (get @event-streams identity-key)))]
    (swap! event-streams update identity-key (fnil conj #{}) ch)
    (log/info {:id ::sse-opened
               :user-xid user-xid
               :data {:action :opened
                      :subject :sse-connection
                      :client client-id
                      :stream-count stream-count}}
              "SSE connection opened")
    ch))

(defn destroy-event-stream
  "Remove an SSE output channel. When the last stream for an identity
   closes, immediately tear the session down — every delta subscription
   is unregistered, batch + model channels are closed, all state is
   dropped. Reconnect = open a new SSE + POST /set."
  [identity-key stream-chan]
  (async/close! stream-chan)
  (swap! event-streams update identity-key disj stream-chan)
  (let [[user-xid client-id] identity-key
        remaining (count (get @event-streams identity-key))
        session-final? (zero? remaining)]
    (log/info {:id ::sse-closed
               :user-xid user-xid
               :data {:action :closed
                      :subject :sse-connection
                      :client client-id
                      :stream-count remaining
                      :session-final? session-final?}}
              "SSE connection closed")
    (when session-final?
      (swap! event-streams dissoc identity-key)
      (cleanup-identity! identity-key))))

(defn- fan-out!
  "Send events to all active SSE streams for an identity.
   Detects and removes closed channels.
   Uses offer! which never blocks — sliding buffers drop old values."
  [identity-key events]
  (when-let [streams (seq (get @event-streams identity-key))]
    (let [[user-xid client-id] identity-key]
      (log/debug {:id ::sse-pushed
                  :user-xid user-xid
                  :data {:action :pushed
                         :subject :sse-payload
                         :client client-id
                         :stream-count (count streams)
                         :event-count (count events)}}
                 "SSE payload pushed"))
    (doseq [stream streams]
      ;; offer! returns nil on a closed channel — any nil mid-burst
      ;; means the stream is gone; tear it down.
      (when (some #(nil? (async/offer! stream %)) events)
        (destroy-event-stream identity-key stream)))))

;;; ============================================================================
;;; Delta Processing Loop
;;; ============================================================================

(defn- ensure-delta-loop!
  "Start the per-identity delta processing go-loop if not running.
   Drains the identity's batch channel (which the delta/subscribe!
   handler offers envelopes onto), accumulates a 100ms window, translates
   each substrate envelope into 0-2 SSE events, fans them out to SSE
   streams.

   `translate-delta` returns a vector per envelope (0 events on a
   never-should-happen miss, 1 normally, 2 when a relation envelope hits
   a subscriber with BOTH endpoints in their records set). The 100ms
   accumulation is a yield-point only — no cross-record merging happens
   anywhere on this path."
  [identity-key]
  (when (claim-loop! identity-key :data)
    (let [batch-chan (get-in @subscriptions-state [identity-key :data :batch-chan])]
      (async/go-loop []
        (if-let [first-env (async/<! batch-chan)]
          (let [batch (loop [batch [first-env]]
                        (let [[val port] (async/alts! [batch-chan (async/timeout 100)])]
                          (if (and val (= port batch-chan))
                            (recur (conj batch val))
                            batch)))
                ;; Heal any dropped events in place: fold store-replayed
                ;; envelopes into this batch (deduped by seq, batch wins so the
                ;; live full-fidelity envelope beats the reconstruction), so
                ;; they translate + fan out exactly like live events.
                replay       (data-resync-envelopes identity-key)
                all          (if (seq replay)
                               (->> (concat replay batch)
                                    (reduce (fn [m e] (assoc m (:seq e) e)) (sorted-map))
                                    vals vec)
                               batch)
                attr-key-idx (sql-query/attribute-key-index)
                records      (get-in @subscriptions-state [identity-key :data :records])
                events       (into []
                                   (mapcat #(translate-delta % attr-key-idx records))
                                   all)
                last-atom    (get-in @subscriptions-state [identity-key :data :last-seq])
                mx           (reduce max 0 (keep :seq all))]
            (when (and last-atom (pos? mx))
              (swap! last-atom max mx))
            (when (seq events)
              (fan-out! identity-key events))
            (recur))
          (end-loop! identity-key :data))))))

;;; ============================================================================
;;; Model Processing Loop
;;; ============================================================================

(defn- ensure-model-loop!
  "Start the model event go-loop for an identity if not running.
   Closes over the principal for per-user model filtering."
  [identity-key principal]
  (when (claim-loop! identity-key :model)
    (let [model-chan (get-in @subscriptions-state [identity-key :model :chan])
          superuser? (access/with-principal principal
                       (daccess/superuser?))
          fingerprint (atom (when-not superuser?
                              (model-fingerprint (dataset/deployed-model) principal)))]
      (async/go-loop []
        (if-let [{:keys [model]} (async/<! model-chan)]
          (let [notify? (if superuser?
                          true
                          (let [new-fp (model-fingerprint model principal)
                                old-fp @fingerprint]
                            (when (not= new-fp old-fp)
                              (reset! fingerprint new-fp)
                              true)))]
            (when notify?
              ;; Fan out one event per active model subscription type.
              ;; Both "deployed-model" and "runtime-model" fire on the
              ;; same trigger; the client re-fetches via the matching
              ;; /data op.
              (let [types (get-in @subscriptions-state [identity-key :model :types])
                    ts    (str (Instant/now))]
                (when (seq types)
                  (fan-out! identity-key
                            (mapv (fn [t] {:event t
                                           :data {:action "deploy"
                                                  :timestamp ts}})
                                  types)))))
            (recur))
          ;; Channel closed
          (end-loop! identity-key :model))))))

(comment
  (require '[synthigy.transit :refer [->transit]])
  (fan-out!
   ["admin" "SYNTHIGYCOMPONENTSPUBLICCLIENTFOROAUTHFLOWPOPUPWIN"]
   [{:event "deployed-model"
     :data {:action "deploy"
            :timestamp (str (Instant/now))
            :model (->transit (dataset/deployed-model))}}]))

;;; ============================================================================
;;; Cleanup
;;; ============================================================================

(defn- cleanup-identity!
  "Tear down every resource owned by `identity-key` — delta-registry
   subscriptions (data + entity + relation), batch + model channels,
   every open SSE stream, and the per-identity loop state."
  [identity-key]
  (delta/unsubscribe! (data-sub-key     identity-key))
  (delta/unsubscribe! (entity-sub-key   identity-key))
  (delta/unsubscribe! (relation-sub-key identity-key))
  (doseq [track-key [:data :entity :relation]]
    (when-let [batch-chan (get-in @subscriptions-state [identity-key track-key :batch-chan])]
      (async/close! batch-chan)))
  (when-let [model-chan (get-in @subscriptions-state [identity-key :model :chan])]
    (async/unsub dataset/publisher :model/deployed model-chan)
    (async/close! model-chan))
  (doseq [stream (get @event-streams identity-key)]
    (async/close! stream))
  (swap! subscriptions-state dissoc identity-key)
  (swap! event-streams       dissoc identity-key)
  (swap! loops               dissoc identity-key))

(defn cleanup-all!
  "Clean up all subscriptions and streams."
  []
  (doseq [identity-key (keys @subscriptions-state)]
    (cleanup-identity! identity-key)))

;;; ============================================================================
;;; Ring Handlers
;;; ============================================================================

(defn set-handler
  "Ring handler for POST /data/subscription/set — replaces the full
   subscription state for the request's identity. The body's
   `\"subscriptions\"` array IS the new state; anything previously
   subscribed but not in this array is dropped."
  [request]
  ;; Same optional-IAM contract as /data (see data.clj handlers): when
  ;; :synthigy/iam is running a valid token is mandatory; when it isn't
  ;; (bare server) requests pass through unauthenticated and access
  ;; checks fall back to the dataset.access allow-all facade.
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (let [body (data/parse-request-body request)]
        (if-not body
          (data/json-response 400 {:error {:message "Invalid or missing JSON body"
                                           :code "INVALID_BODY"}})
          (try
            (access/with-principal (:principal iam)
              (data/json-response 200 (set-subscriptions! (request-identity request iam) body (:principal iam))))
            (catch clojure.lang.ExceptionInfo e
              (let [code (:code (ex-data e))]
                (data/json-response (case code
                                      "FORBIDDEN" 403
                                      400)
                                    {:error {:message (ex-message e) :code code}})))))))))

(defn status-handler
  "Ring handler for GET /data/subscription/status"
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (data/json-response 200 (subscription-status (request-identity request iam))))))

;;; ============================================================================
;;; SSE Format
;;; ============================================================================

(defn format-sse
  "Format an event map as SSE text."
  [{:keys [event data]}]
  (str "event: " event "\n"
       "data: " (json/write-str data) "\n\n"))

(def keepalive-msg ": keepalive\n\n")

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(lifecycle/register-module!
 :synthigy/subscriptions
 ;; substrate owns the drainer that calls delta/dispatch! — subscriptions are
 ;; a consumer of that bus, so the drainer MUST be up first or subscribers
 ;; register interest and silently never receive events. (observability, the
 ;; other consumer, already declares this; we were relying on server's dep
 ;; ordering by accident.)
 ;;
 ;; No :synthigy/iam dep — subscriptions work on a bare server. When IAM
 ;; is running the handlers require a token and RBAC-gate each subscribed
 ;; entity/relation; when it isn't, access falls back to the
 ;; dataset.access allow-all facade (same optional-IAM contract as /data).
 {:depends-on [:synthigy/dataset :synthigy/substrate]
  :doc "Live /data subscriptions — record/stats deltas over SSE"
  :start (fn []
           (log/info {:id ::lifecycle-start
                      :data {:action :starting :subject :subscriptions}}
                     "Starting subscription system"))
  :stop  (fn []
           (log/info {:id ::lifecycle-stop
                      :data {:action :stopping :subject :subscriptions}}
                     "Stopping subscription system")
           (cleanup-all!)
           (log/info {:id ::lifecycle-stopped
                      :data {:action :stopped :subject :subscriptions}}
                     "All subscriptions cleaned up"))})
