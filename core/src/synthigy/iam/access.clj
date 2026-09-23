;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.iam.access
  (:require
   [clojure.core.async :as async]
   [clojure.set :as set]
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.data :refer [*ROOT* *SYNTHIGY*]]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access.protocol :as access.protocol]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.iam.keys]))

(defonce ^{:dynamic true
           :doc "Materialized principal map for the current request scope. See synthigy.iam.context/get-user-details for shape."}
  *principal* nil)

(defonce ^{:dynamic true
           :doc "Pre-materialized set of role id-keys (xids) for the current request. RBAC lookups intersect against this."}
  *role-xids* nil)

(defonce ^{:dynamic true
           :doc "Pre-materialized set of role _eids (longs) for the current request. RLS :role match emits these into IN (?,?,?). Same roles as *role-xids*, different identifier form."}
  *role-eids* nil)

(defonce ^{:dynamic true
           :doc "Pre-materialized set of group _eids (longs) for the current request. RLS :group match emits these into IN (?,?,?)."}
  *group-eids* nil)

(defonce ^:dynamic *rules* nil)
(defonce ^:dynamic *scopes* nil)

(defn project-role-xids
  [principal]
  (when principal
    (into #{} (keys (:roles principal)))))

(defn project-role-eids
  [principal]
  (when principal
    (into #{} (keep :_eid) (vals (:roles principal)))))

(defn project-group-eids
  [principal]
  (when principal
    (into #{} (keep :_eid) (vals (:groups principal)))))

(defmacro with-principal
  "Bind `*principal*` and its role/group projections for the duration of `body`."
  [principal & body]
  `(let [p# ~principal]
     (binding [*principal*   p#
               *role-xids*   (project-role-xids p#)
               *role-eids*   (project-role-eids p#)
               *group-eids*  (project-group-eids p#)]
       ~@body)))

(defn role-xids
  []
  (or *role-xids* (project-role-xids *principal*)))

(defn role-eids*
  []
  (or *role-eids* (project-role-eids *principal*)))

(defn group-eids*
  []
  (or *group-eids* (project-group-eids *principal*)))

(def ^:private delta-sub-key ::rules-and-scopes-invalidator)

(def entity-grants
  "CRUDOB entity rule -> the User Role grant relation carrying it."
  {:create :iam/role->create-entities
   :read   :iam/role->read-entities
   :update :iam/role->update-entities
   :delete :iam/role->delete-entities
   :owners :iam/role->owned-entities
   :browse :iam/role->browse-entities})

(def relation-grants
  "[traversal-direction rule] -> the User Role grant relation carrying it. The
   direction is deliberately the opposite of the relation's name — see the
   name-by-TARGET section in docs iam/access.md."
  {[:to   :read]   :iam/role->from-read-relations
   [:to   :write]  :iam/role->from-write-relations
   [:to   :delete] :iam/role->from-delete-relations
   [:from :read]   :iam/role->to-read-relations
   [:from :write]  :iam/role->to-write-relations
   [:from :delete] :iam/role->to-delete-relations})

(defn grant-field
  "Wire field on User Role for a grant relation, or nil when that relation is
   not in the deployed model. Derived from the deployed labels so a renamed
   grant follows automatically — never hardcode the field name."
  [relation-key]
  (let [role-id (id/entity :iam/user-role)
        {:keys [from from-label to-label]}
        (dataset/deployed-relation (id/relation relation-key))]
    (when-let [label (if (= role-id (id/extract from)) to-label from-label)]
      (keyword (naming/normalize-name (str/trim label))))))

(defn get-roles-access-data
  []
  (let [grant [{:selections {(id/key) nil}}]
        fields (keep grant-field (concat (vals entity-grants) (vals relation-grants)))]
    (dataset/search-entity
     :iam/user-role
     nil
     (into {(id/key) nil :name nil} (map #(vector % grant)) fields))))

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
    (let [field (fn [rule->relation k] (grant-field (get rule->relation k)))
          ;; pre-CRUDOB models carry ONE relation for both halves (create
          ;; entities IS write entities renamed), so it must satisfy :update too
          merged-write? (nil? (field entity-grants :update))
          granted (fn [role-data f] (when f (map id/extract (get role-data f))))]
      (reduce
       (fn [r role-data]
         (let [role-id (id/extract role-data)
               r (reduce-kv
                  (fn [r rule relation-key]
                    (let [f (grant-field relation-key)
                          ids (granted role-data f)
                          ids (if (and merged-write? (= :update rule))
                                (granted role-data (field entity-grants :create))
                                ids)]
                      (x-entity r role-id rule ids)))
                  r
                  entity-grants)]
           (reduce-kv
            (fn [r [direction rule] relation-key]
              (x-relation r role-id direction rule
                          (granted role-data (grant-field relation-key))))
            r
            relation-grants)))
       nil
       data))))

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
    (nil? *principal*)
    (let [synthigy-eid (:_eid *SYNTHIGY*)
          synthigy-id  (id/extract *SYNTHIGY*)
          principal-eid (:_eid *principal*)
          principal-id  (id/extract *principal*)]
      (or (and synthigy-eid principal-eid (= synthigy-eid principal-eid))
          (and synthigy-id principal-id (= synthigy-id principal-id))))
    (contains? (or roles #{}) (id/extract *ROOT*)))))

(defn roles->schema-principal
  "Resolve role names to a synthetic principal for `with-principal`; the caller
   must hold every requested role or be superuser."
  [role-names]
  (let [wanted (set role-names)
        found  (dataset/search-entity
                :iam/user-role
                {:name {:_in (vec wanted)}}
                {(id/key) nil :_eid nil :name nil})
        by-name (into {} (map (juxt :name identity)) found)
        missing (remove by-name wanted)]
    (when (seq missing)
      (throw (ex-info (str "Unknown role(s): " (str/join ", " missing))
                      {:code "UNKNOWN_ROLE" :roles (vec missing)})))
    (let [target-xids (into #{} (map id/extract) found)]
      (when-not (or (superuser?)
                    (and (seq target-xids)
                         (set/subset? target-xids (role-xids))))
        (throw (ex-info (str "Not permitted to project schema for role(s): "
                             (str/join ", " wanted))
                        {:code "FORBIDDEN" :roles (vec wanted)}))))
    {:roles (into {} (map (juxt id/extract identity)) found)}))

(defn entity-allows?
  "Whether a principal holding `roles` may perform any of `rules` on `entity`."
  ([entity rules] (entity-allows? entity rules (role-xids)))
  ([entity rules roles]
   (try
     (cond
       (nil? entity) false
       (superuser? roles) true
       (not (core/rbac-enabled? (dataset/deployed-entity entity))) true
       :else (letfn [(ok? [rule]
                       (boolean (not-empty (set/intersection roles (get-in *rules* [:entity entity rule])))))]
               (boolean (or (ok? :owners)
                            (and (some #{:read} rules) (ok? :browse))
                            (some ok? rules)))))
     (catch Throwable ex
       (log/error! {:id ::entity-allows-failed
                    :msg "Couldn't evaluate entity-allows"
                    :data {:entity entity :rules rules :roles roles}}
                   ex)
       (throw ex)))))

(defn credential-attribute?
  "The User column the login path authenticates against."
  [attribute]
  (and attribute (= (id/data :iam.user/password) (id/extract attribute))))

(defn attribute-allows?
  "Whether the principal may perform `op` (:read / :write) on `attribute` of
   `entity`. The login credential is superuser-only either way — writing it is
   becoming that user, reading it is holding the auth material."
  ([entity attribute op] (attribute-allows? entity attribute op (role-xids)))
  ([entity attribute op roles]
   (boolean
    (if (credential-attribute? attribute)
      (superuser? roles)
      (or (superuser? roles)
          (core/attribute-allows-op? entity attribute op (or roles #{})))))))

(defn relation-allows?
  ([relation direction rules] (relation-allows? relation direction rules (role-xids)))
  ([relation direction rules roles]
   (try
     (cond
       (superuser? roles) true
       (not (core/relation-rbac-enabled? (dataset/deployed-relation relation) direction)) true
       ;; Owning BOTH endpoint entities implies the link.
       (let [[from-e to-e] direction
             owns? (fn [e] (boolean (not-empty (set/intersection
                                                roles (get-in *rules* [:entity e :owners])))))]
         (and from-e to-e (owns? from-e) (owns? to-e)))
       true
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

(defn scopes-deployed?
  "True when the OAuth store model's User Role → scopes relation is deployed."
  []
  (let [model (dataset/deployed-model)
        role (core/get-entity model (some-> (id/entity :iam/user-role) str))]
    (boolean (some #(and (:active %) (= "scopes" (:to-label %)))
                   (core/focus-entity-relations model role)))))

(defn load-scopes
  "Scopes are OAuth's model, not IAM's — on installations where the OAuth
   store hasn't deployed (or leveled) yet, there is no scope concept and
   *scopes* stays nil, which disables scope checks by design."
  []
  (if (scopes-deployed?)
    (alter-var-root #'*scopes* (fn [_] (transform-scope-data (get-roles-scope-data))))
    (log/info {:id ::no-scope-model
               :data {:subject :role-access}}
              "OAuth store model not deployed; scope enforcement inactive")))

(defn roles-scopes
  [roles]
  (reduce set/union (vals (select-keys *scopes* roles))))

(defn roles-scope-ids
  "Scope row ids (not names) granted to any of `roles`."
  [roles]
  (into #{}
        (comp (mapcat :scopes) (map id/extract))
        (dataset/search-entity :iam/user-role
                               {(id/key) {:_in (vec roles)}}
                               {:scopes [{:selections {(id/key) nil}}]})))

(defn debounced
  "Return a delta handler that runs `f` once, `ms` after the last call in a
   burst."
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
  (log/info {:id ::starting :data {:action :starting :subject :iam-access}}
            "Starting IAM access control")
  (let [model        (dataset/deployed-model)
        role-xid     (some-> (id/entity :iam/user-role) str)
        role-entity  (core/get-entity model role-xid)
        relations    (core/focus-entity-relations model role-entity)
        relation-xids (into #{} (keep #(some-> % id/extract str)) relations)]
    (log/info {:id ::subscribing-delta
               :data {:role-xid role-xid
                      :relation-count (count relation-xids)}}
              "Subscribing to role+relation deltas")
    (load-rules)
    (load-scopes)
    (delta/subscribe!
     delta-sub-key
     (cond-> {}
       role-xid            (assoc :entity-xids #{role-xid})
       (seq relation-xids) (assoc :relation-xids relation-xids))
     (debounced 5000
                (fn []
                  (log/info {:id ::reloading-role-access
                             :data {:action :reloading :subject :role-access}}
                            "Reloading role access (rules + scopes)")
                  (load-rules)
                  (load-scopes))))
    (log/info {:id ::started
               :data {:action :started :subject :iam-access
                      :rules  (count *rules*)
                      :scopes (count *scopes*)}}
              "IAM access control started")))

(defn stop
  []
  (log/info {:id ::stopping :data {:action :stopping :subject :iam-access}} "Stopping IAM access control")

  (delta/unsubscribe! delta-sub-key)

  (alter-var-root #'*rules* (constantly nil))
  (alter-var-root #'*scopes* (constantly nil))

  (log/info {:id ::stopped :data {:action :stopped :subject :iam-access}} "IAM access control stopped")
  nil)

(defrecord IAMAccessControl []
  access.protocol/AccessControl

  (entity-allows? [_ entity-id operations]
    (entity-allows? entity-id (vec operations) (role-xids)))

  (relation-allows? [_ relation-id operations]
    (let [{:keys [from to]} (dataset/deployed-relation relation-id)
          from-id (id/extract from)
          to-id   (id/extract to)
          ops     (vec operations)
          roles   (role-xids)]
      (or (relation-allows? relation-id [from-id to-id] ops roles)
          (relation-allows? relation-id [to-id from-id] ops roles))))

  (relation-allows? [_ relation-id from-to operations]
    (relation-allows? relation-id from-to (vec operations) (role-xids)))

  (attribute-allows? [_ entity-id attribute-id op]
    (let [entity (dataset/deployed-entity entity-id)
          target (str (if (keyword? attribute-id) (name attribute-id) attribute-id))
          attribute (some #(when (= target (str (id/extract %))) %)
                          (:attributes entity))]
      (attribute-allows? entity attribute op (role-xids))))

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

  (role-eids [_]
    (or (role-eids*) #{}))

  (group-eids [_]
    (or (group-eids*) #{}))

  access.protocol/RLSBypass
  (rls-bypass? [_ entity-id operation]
    (let [roles (role-xids)
          ok?   (fn [rule]
                  (boolean (not-empty (set/intersection roles (get-in *rules* [:entity entity-id rule])))))]
      (or (ok? :owners)
          (and (= :read operation) (ok? :browse))))))
