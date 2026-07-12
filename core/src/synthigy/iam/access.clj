(ns synthigy.iam.access
  (:require
   [clojure.core.async :as async]
   [clojure.set :as set]
   [synthigy.log :as log]
   [synthigy.data :refer [*ROOT* *SYNTHIGY*]]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access.protocol :as access.protocol]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]))

;;; ============================================================================
;;; Dynamic Context Vars
;;; ============================================================================
;;;
;;; Unidirectional identifier discipline:
;;;
;;;   ROLES → RBAC → xid set (*role-xids*)
;;;     Looked up in *rules* / *scopes*, which are keyed by id-key.
;;;     Stable across deploys; appears in token claims.
;;;
;;;   GROUPS → RLS → _eid set (*group-eids*)
;;;     Joined against bigint membership columns in RLS predicates.
;;;     Local-only; never crosses the wire.
;;;
;;; Both projections are computed ONCE when the principal binds for a
;;; request (`with-principal`) and read by a single dyn-var deref on the
;;; hot path — no per-call set construction.

(defonce ^{:dynamic true
           :doc "Materialized principal map for the current request scope. See synthigy.iam.context/get-user-details for shape."}
  *principal* nil)

(defonce ^{:dynamic true
           :doc "Pre-materialized set of role id-keys (xids) for the current request. RBAC lookups intersect against this."}
  *role-xids* nil)

(defonce ^{:dynamic true
           :doc "Pre-materialized set of group _eids (longs) for the current request. RLS :group match emits these into IN (?,?,?)."}
  *group-eids* nil)

(defonce ^:dynamic *rules* nil)
(defonce ^:dynamic *scopes* nil)

(defn project-role-xids
  "Public so the `with-principal` macro can resolve it at call sites."
  [principal]
  (when principal
    (into #{} (keys (:roles principal)))))

(defn project-group-eids
  "Public so the `with-principal` macro can resolve it at call sites."
  [principal]
  (when principal
    (into #{} (keep :_eid) (vals (:groups principal)))))

(defmacro with-principal
  "Bind `*principal*`, `*role-xids*`, and `*group-eids*` for the duration
   of `body`. Projections are computed once at bind time so every access
   check on the hot path is a single dyn-var deref."
  [principal & body]
  `(let [p# ~principal]
     (binding [*principal*   p#
               *role-xids*   (project-role-xids p#)
               *group-eids*  (project-group-eids p#)]
       ~@body)))

(defn- role-xids
  "Fast accessor. Returns the pre-bound projection when called inside
   `with-principal`; falls back to recomputing from `*principal*` for
   legacy `(binding [*principal* …] …)` callsites and tests."
  []
  (or *role-xids* (project-role-xids *principal*)))

(defn- group-eids*
  []
  (or *group-eids* (project-group-eids *principal*)))

;; Key under which the IAM rules+scopes invalidation subscription is
;; registered in `synthigy.dataset.delta`. Single subscription per process.
(def ^:private delta-sub-key ::rules-and-scopes-invalidator)

;;; ============================================================================
;;; RULES
;;; ============================================================================

(defn get-roles-access-data
  []
  (dataset/search-entity
   :iam/user-role
   nil
   {(id/key) nil
    :name nil
     ;; Entities
    :write_entities [{:selections {(id/key) nil}}]
    :read_entities [{:selections {(id/key) nil}}]
    :delete_entities [{:selections {(id/key) nil}}]
    :owned_entities [{:selections {(id/key) nil}}]
     ;; Relations
    :to_read_relations [{:selections {(id/key) nil}}]
    :to_write_relations [{:selections {(id/key) nil}}]
    :to_delete_relations [{:selections {(id/key) nil}}]
    :from_read_relations [{:selections {(id/key) nil}}]
    :from_write_relations [{:selections {(id/key) nil}}]
    :from_delete_relations [{:selections {(id/key) nil}}]}))

(comment
  (dataset/search-entity :iam/user nil {(id/key) nil
                                        :name nil})
  (dataset/search-entity :iam/user-role nil {(id/key) nil
                                             :name nil}))

(defn transform-roles-data
  [data]
  (letfn [(x-entity [result role rule entities]
            (reduce
             (fn [r entity]
               (update-in r [:entity entity rule] (fnil conj #{}) role))
             result
             entities))
          (x-relation [result role direction rule relations]
            (reduce
             (fn [r relation]
               (let [{:keys [from to]} (dataset/deployed-relation relation)
                     from-id (id/extract from)
                     to-id (id/extract to)
                     k (if (= direction :to)
                         [to-id from-id]
                         [from-id to-id])]
                 (update-in r [:relation relation k rule] (fnil conj #{}) role)))
             result
             relations))]
    (reduce
     (fn [r role-data]
       (let [role-id (id/extract role-data)
             {:keys [write_entities read_entities delete_entities owned_entities
                     to_read_relations to_write_relations to_delete_relations
                     from_read_relations from_write_relations from_delete_relations]} role-data]
         (-> r
             (x-entity role-id :read (map id/extract read_entities))
             (x-entity role-id :write (map id/extract write_entities))
             (x-entity role-id :delete (map id/extract delete_entities))
             (x-entity role-id :owners (map id/extract owned_entities))
              ;; From and to refer to entities. There is no from and to, both are
              ;; from. Because of modeling and how relations are stored, users
              ;; that read model, read it in inverted... This is why at this point
              ;; we have to invert back rules, so that they follow first mindfuck
              ;; logic... donno
             (x-relation role-id :to :read (map id/extract from_read_relations))
             (x-relation role-id :to :write (map id/extract from_write_relations))
             (x-relation role-id :to :delete (map id/extract from_delete_relations))
             (x-relation role-id :from :read (map id/extract to_read_relations))
             (x-relation role-id :from :write (map id/extract to_write_relations))
             (x-relation role-id :from :delete (map id/extract to_delete_relations)))))
     nil
     data)))

(comment
  (dataset/deployed-relation #uuid "7efa7244-ae20-4248-9792-7623d12cea9e")
  (get-roles-access-data))

(defn load-rules
  []
  (alter-var-root #'*rules* (fn [_] (transform-roles-data (get-roles-access-data)))))

(defn superuser?
  ([] (superuser? (role-xids)))
  ([roles]
   (or
    ;; No principal bound — internal/system path
    (nil? *principal*)
    ;; Principal is the SYNTHIGY system user (match by _eid OR id-key)
    (let [synthigy-eid (:_eid *SYNTHIGY*)
          synthigy-id  (id/extract *SYNTHIGY*)
          principal-eid (:_eid *principal*)
          principal-id  (id/extract *principal*)]
      (or (and synthigy-eid principal-eid (= synthigy-eid principal-eid))
          (and synthigy-id principal-id (= synthigy-id principal-id))))
    ;; Principal has the ROOT role
    (contains? (or roles #{}) (id/extract *ROOT*)))))

(defn entity-allows?
  ([entity rules] (entity-allows? entity rules (role-xids)))
  ([entity rules roles]
   (try
     (cond
       (nil? entity) false
       (or (nil? *rules*) (superuser? roles)) true
       (not (core/rbac-enabled? (dataset/deployed-entity entity))) true
       :else (letfn [(ok? [rule]
                       (boolean (not-empty (set/intersection roles (get-in *rules* [:entity entity rule])))))]
               (boolean (some ok? rules))))
     (catch Throwable ex
       (log/error! {:id ::entity-allows-failed
                    :msg "Couldn't evaluate entity-allows"
                    :data {:entity entity :rules rules :roles roles}}
                   ex)
       (throw ex)))))

(defn relation-allows?
  ([relation direction rules] (relation-allows? relation direction rules (role-xids)))
  ([relation direction rules roles]
   (try
     (cond
       (or (nil? *rules*) (superuser? roles)) true
       (not (core/relation-rbac-enabled? (dataset/deployed-relation relation) direction)) true
       :else (letfn [(ok? [rule]
                       (boolean
                        (not-empty
                         (set/intersection roles (get-in *rules* [:relation relation direction rule])))))]
               (some ok? rules)))
     (catch Throwable ex
       (log/error! {:id ::relation-allows-failed
                    :msg "Couldn't evaluate relation-allows"
                    :data {:relation relation
                           :direction direction
                           :rules rules
                           :roles roles}}
                   ex)
       (throw ex)))))

(defn roles-allowed?
  [roles]
  (let [current (role-xids)]
    (or
     (superuser?)
     (nil? current)
     (not-empty (set/intersection roles current)))))

(defn scope-allowed?
  ([permission]
   (scope-allowed? (role-xids) permission))
  ([roles scope]
   (or
    (superuser?)
    (nil? *scopes*)
    (reduce-kv
     (fn [_ _ scopes]
       (if (contains? scopes scope) (reduced true)
           false))
     false
     (select-keys *scopes* roles)))))

(comment
  (def roles #{#uuid "7fc035e2-812e-4861-a25c-eb172b39577f"
               #uuid "48ef8d6d-e067-4e31-b4db-2a1ae49a0fcb"
               #uuid "082ef416-d35c-40ab-a5ff-c68ff871ba4e"})
  (time (scope-allowed? roles "dataset:delete")))

;; SCOPES
(defn get-roles-scope-data
  []
  (dataset/search-entity
   :iam/user-role
   nil
   {(id/key) nil
    :name nil
    :scopes [{:selections {(id/key) nil
                           :name nil}}]}))

(defn transform-scope-data
  [roles]
  (reduce
   (fn [r role-data]
     (let [role-id (id/extract role-data)
           scopes (:scopes role-data)]
       (assoc r role-id (set (remove nil? (map :name scopes))))))
   {}
   roles))

(defn load-scopes
  []
  (alter-var-root #'*scopes* (fn [_] (transform-scope-data (get-roles-scope-data)))))

(defn roles-scopes
  [roles]
  (reduce set/union (vals (select-keys *scopes* roles))))

(comment
  (def roles
    [#uuid "0a757182-9a8e-11ee-87ee-02a535895d2d"
     #uuid "228df5f6-86c7-4308-8a9e-4a578c5e4af7"
     #uuid "7fc035e2-812e-4861-a25c-eb172b39577f"]))

(defn- debounced
  "Returns a fn that re-arms a 5s timer on each call; only the latest
   call's timer actually fires `f`. Earlier scheduled gos detect a tick
   mismatch and bail. Used to coalesce bursts of rule/scope-touching
   deltas into a single reload."
  [ms f]
  (let [latest (atom 0)]
    (fn [_env]
      (let [tick (swap! latest inc)]
        (async/go
          (async/<! (async/timeout ms))
          (when (= tick @latest)
            (try (f)
                 (catch Throwable e
                   (log/error! {:id ::debounced-reload-failed} e)))))))))

(defn start
  []
  (let [model        (dataset/deployed-model)
        ;; :iam/user-role is a magic-keyword alias resolved via the id
        ;; registry, not by `core/get-entity` directly. Resolve to the
        ;; runtime xid first, then look up the entity record.
        role-xid     (some-> (id/entity :iam/user-role) str)
        role-entity  (core/get-entity model role-xid)
        relations    (core/focus-entity-relations model role-entity)
        ;; Exclude the to-Permission and to-User relations — those fire
        ;; for unrelated reasons and would thrash the reload. (Same UUIDs
        ;; the legacy code disj'd from the per-element subscribe set.)
        excluded     #{#uuid "16ca53f4-0fe3-4122-93dd-1e86fd1b58db"
                       #uuid "1a2cc45d-1301-4fdd-bb02-650362165b37"}
        relation-xids (into #{}
                            (comp (remove #(contains? excluded (id/extract %)))
                                  (keep #(some-> % id/extract str)))
                            relations)]
    (log/info {:id ::subscribing-delta
               :data {:role-xid role-xid
                      :relation-count (count relation-xids)}}
              "Subscribing to role+relation deltas")
    (delta/subscribe!
      delta-sub-key
      (cond-> {}
        role-xid           (assoc :entity-xids #{role-xid})
        (seq relation-xids) (assoc :relation-xids relation-xids))
      (debounced 5000
                 (fn []
                   (log/info {:id ::reloading-role-access
                              :data {:action :reloading :subject :role-access}}
                             "Reloading role access (rules + scopes)")
                   (load-rules)
                   (load-scopes))))
    (load-rules)
    (load-scopes)))

(defn stop
  []
  (log/info {:id ::stopping :data {:action :stopping :subject :iam-access}} "Stopping IAM access control")

  (delta/unsubscribe! delta-sub-key)

  ;; Clear rules and scopes
  (alter-var-root #'*rules* (constantly nil))
  (alter-var-root #'*scopes* (constantly nil))

  (log/info {:id ::stopped :data {:action :stopped :subject :iam-access}} "IAM access control stopped")
  nil)

;;; ============================================================================
;;; Dataset Access Protocol Implementation
;;; ============================================================================

(defrecord IAMAccessControl []
  access.protocol/AccessControl

  (entity-allows? [_ entity-id operations]
    (entity-allows? entity-id (vec operations) (role-xids)))

  (relation-allows? [_ relation-id operations]
    ;; Coarse-grained "can the principal touch this relation at all?" check
    ;; — used by callers that don't carry direction (e.g. the per-relation
    ;; gate in fused.clj). Allow if either direction allows.
    (let [{:keys [from to]} (dataset/deployed-relation relation-id)
          from-id (id/extract from)
          to-id   (id/extract to)
          ops     (vec operations)
          roles   (role-xids)]
      (or (relation-allows? relation-id [from-id to-id] ops roles)
          (relation-allows? relation-id [to-id from-id] ops roles))))

  (relation-allows? [_ relation-id from-to operations]
    (relation-allows? relation-id from-to (vec operations) (role-xids)))

  (scope-allowed? [_ scope]
    (scope-allowed? (role-xids) scope))

  (roles-allowed? [_ role-ids]
    (roles-allowed? role-ids))

  (superuser? [_]
    (superuser? (role-xids)))

  (get-principal [_]
    *principal*)

  (principal-eid [_]
    (:_eid *principal*))

  (role-ids [_]
    (or (role-xids) #{}))

  (group-eids [_]
    (or (group-eids*) #{})))
