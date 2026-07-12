(ns synthigy.log.topics
  "Topic classification for log signals — the single source of truth for
   *what a log is ABOUT*, an axis orthogonal to level (*how severe it is*).

   ## Why topics

   Namespace describes where code LIVES; level describes SEVERITY. Neither
   describes a signal's SUBJECT or its AUDIENCE. `synthigy.server` holds both
   module lifecycle (a System concern) and `request-completed` (Traffic) — the
   namespace can't separate them. And one event often has several audiences: a
   dataset deploy is both a System event (a module did something) and a Dataset
   event (the schema changed). ns+level forces it into one lens; a topic SET
   lets it live in all the right ones.

   ## The model

   Every signal classifies to a set drawn from `all-topics`. Lenses subscribe
   to a topic (`:system`, `:dataset`, …) instead of guessing from ns+level:

       System   = topics ∋ :system
       Dataset  = topics ∋ :dataset
       Auth     = topics ∋ :auth
       Requests = topics ∋ :traffic

   `classify` derives the set from any explicit `:topics` on the signal PLUS
   rules over `:id` / `:data {:action :subject}` / `:ns` / `:level`. So the
   ~446 existing callsites get sensible topics with zero churn; you only add an
   explicit `:topics` where derivation is ambiguous.

   ## Design notes

   - An exact `:id` mapping (`id->topics`) is AUTHORITATIVE: it covers the
     per-request firehose (`request-completed`, `op-completed`, SSE/subscribe)
     whose meaning the namespace actively misrepresents. When an id matches
     here, ns/action/subject rules are skipped so a traffic signal can never
     leak into `:system` via its `synthigy.server` namespace.
   - `:diagnostic` is LEVEL-derived: every `:debug`/`:trace` signal also gets
     `:diagnostic`, so 'show me all internal traces' is one subscription while
     the signal keeps its subject topic too.
   - ERROR/FATAL safety net: a non-traffic error always gets `:system`, so no
     failure is invisible to operators regardless of subsystem or whether the
     callsite remembered `:data {:action …}`. (HTTP 5xx envelopes stay in
     `:traffic` — the Requests lens owns them with their request context; true
     backend crashes like `uncaught-exception` carry no `:traffic` and surface
     in System.)

   This namespace is intentionally NOT wired into the pipeline or the wire
   schema yet — it's the taxonomy made concrete and testable first."
  (:require
   [clojure.string :as str]))

(def all-topics
  "The closed vocabulary of topics a signal may be tagged with."
  #{:system :dataset :auth :traffic :audit :diagnostic})

;;; ============================================================================
;;; Rule tables — the taxonomy, as data
;;; ============================================================================

(def ^:private id->topics
  "Exact `:id` → topics. AUTHORITATIVE — when an id matches here, no other
   rule runs. Reserved for signals the namespace misclassifies: the
   per-request/op firehose."
  {:synthigy.server/request-completed #{:traffic}
   ;; A per-/data-op completion is TRAFFIC, not dataset. The Dataset lens is
   ;; for dataset MODEL operations (deploys/schema/migrations); per-request
   ;; reads/writes belong to the Requests/Traffic lens. Keeping op-completed
   ;; out of :dataset keeps that topic focused on schema, not query volume.
   :synthigy.server.data/op-completed #{:traffic}
   :synthigy.admin/admin-request      #{:traffic}})

(def ^:private traffic-id-name-prefixes
  "An `:id` whose NAME starts with one of these is authoritative traffic,
   regardless of namespace or action (e.g. `subscribe-connected` carries
   `:action :started` but is connection traffic, not a lifecycle event)."
  ["sse-" "subscribe-"])

(def ^:private lifecycle-actions
  "`:data :action` verbs that mark a module-lifecycle event → `:system`."
  #{:starting :started :stopping :stopped
    :setup :setup-complete :cleanup :cleanup-complete
    :initialized :ready :not-initialized})

(def ^:private deploy-actions
  "`:data :action` verbs that mark a deploy/schema/patch event →
   `:system` + `:dataset` (operators AND data folks both care)."
  #{:deploying :deployed :deploy-failed
    :recalling :recalled :recall-failed
    :destroying :destroyed :destroy-failed
    :migrating :migrated :installing :installed
    :upgrading :upgraded :patching :patched})

(def ^:private subject->topics
  "`:data :subject` noun → topics. Splits the security surface: identity
   FLOWS (tokens, sessions, codes) are `:auth`; security INFRASTRUCTURE
   (encryption, keypair, persistence substrate) is `:system`."
  {;; system infrastructure
   :http-server       #{:system}
   :admin-server      #{:system}
   :port-file         #{:system}
   :admin             #{:system}
   :db-backend        #{:system}
   :encryption        #{:system}
   :dek               #{:system}
   :keypair           #{:system}
   :oauth-persistence #{:system}
   ;; dataset
   :dataset           #{:dataset}
   :dataset-schema    #{:dataset}
   :dataset-model     #{:dataset}
   :dataset-features  #{:dataset}
   :deployed-model    #{:dataset}
   :deploy-history    #{:dataset}
   :model             #{:dataset}
   :id-format         #{:dataset}
   :id-triggers       #{:dataset}
   :xid               #{:dataset}
   :xid-migration     #{:dataset}
   :meta-table        #{:dataset}
   :meta-tables       #{:dataset}
   :data-tables       #{:dataset}
   ;; auth / identity flows
   :oauth             #{:auth}
   :oauth-clients     #{:auth}
   :oauth-store       #{:auth}
   :client-secret     #{:auth}
   :client-secrets    #{:auth}
   :authorization-code #{:auth}
   :device-code       #{:auth}
   :token             #{:auth}
   :session           #{:auth}
   :role-access       #{:auth}
   :iam-access        #{:auth}
   :iam-schema        #{:auth}
   :iam-model         #{:auth}
   :iam-connector     #{:auth}
   :iam-defaults      #{:auth}
   ;; audit / observability
   :iam-audit         #{:audit}
   :observability     #{:audit :system}   ; substrate status is also a System concern
   :subscribe         #{:traffic}})

(def ^:private ns-prefix->topics
  "Fallback base topic by `:ns` prefix. ORDERED — first (most specific) match
   wins. Note `synthigy.server` defaults to `:system`: traffic ids in that ns
   are already short-circuited by `id->topics` / `traffic-id-name-prefixes`,
   so what's left (boot/lifecycle) is genuinely system."
  [["synthigy.oauth"          #{:auth}]
   ["synthigy.oidc"           #{:auth}]
   ["synthigy.iam.audit"      #{:audit}]
   ["synthigy.iam.encryption" #{:system}]
   ["synthigy.iam"            #{:auth}]
   ["synthigy.dataset"        #{:dataset}]
   ["synthigy.observability"  #{:audit}]
   ["synthigy.substrate"      #{:audit}]
   ["synthigy.database"       #{:system}]
   ["synthigy.admin"          #{:system}]
   ["synthigy.log"            #{:system}]
   ["synthigy.server"         #{:system}]])

;;; ============================================================================
;;; Classification
;;; ============================================================================

(defn- id-name [id]
  (when (keyword? id) (name id)))

(defn- authoritative-traffic? [id]
  (or (contains? id->topics id)
      (when-let [n (id-name id)]
        (some #(str/starts-with? n %) traffic-id-name-prefixes))))

(defn- ns-base-topics [ns-str]
  (let [s (str ns-str)]
    (or (some (fn [[prefix topics]]
                (when (str/starts-with? s prefix) topics))
              ns-prefix->topics)
        #{})))

(defn classify
  "Return the set of topics for a Telemere `signal` map. Pure. Reads
   `:topics` (explicit override/augment), `:id`, `:level`, and
   `:data {:action :subject}`. Always returns a non-empty subset of
   `all-topics` (`#{:diagnostic}` when nothing else matches)."
  [{:keys [id ns level data] :as signal}]
  (let [explicit (set (:topics signal))
        action   (:action data)
        subject  (:subject data)
        diag?    (contains? #{:debug :trace} level)
        err?     (contains? #{:error :fatal} level)]
    (if (authoritative-traffic? id)
      (cond-> (into explicit (or (id->topics id) #{:traffic}))
        diag? (conj :diagnostic))
      (let [t (cond-> explicit
                (contains? lifecycle-actions action) (conj :system)
                (contains? deploy-actions action)    (into #{:system :dataset})
                (contains? subject->topics subject)  (into (subject->topics subject))
                true                                 (into (ns-base-topics ns)))
            t (cond-> t
                (and err? (not (contains? t :traffic))) (conj :system)
                diag?                                   (conj :diagnostic))]
        (if (empty? t) #{:diagnostic} t)))))

(defn has-topic?
  "True when `signal` classifies to `topic`."
  [signal topic]
  (contains? (classify signal) topic))
