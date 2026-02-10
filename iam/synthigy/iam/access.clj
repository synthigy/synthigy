(ns synthigy.iam.access
  (:require
    [clojure.core.async :as async]
    [clojure.set :as set]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [synthigy.data :refer [*ROOT* *SYNTHIGY*]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.access :as dataset.access]
    [synthigy.dataset.access.protocol :as access.protocol]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]))

;;; ============================================================================
;;; Dynamic Context Vars
;;; ============================================================================

(defonce ^:dynamic *user* nil)
(defonce ^:dynamic *roles* nil)
(defonce ^:dynamic *groups* nil)
(defonce ^:dynamic *rules* nil)
(defonce ^:dynamic *scopes* nil)

;; Store delta channel subscription for cleanup
(defonce ^:private delta-subscription-state (atom {:chan nil
                                                   :subscribed-elements #{}}))

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
  ([] (superuser? *roles*))
  ([roles]
   (or
     (nil? *user*)
    ;; Check if *user* is directly the SYNTHIGY _eid
     (and (some? *user*) (some? (:_eid *SYNTHIGY*)) (= *user* (:_eid *SYNTHIGY*)))
    ;; Check if *user* is the SYNTHIGY entity ID (can be UUID or map with entity ID)
     (let [user-id (if (map? *user*) (id/extract *user*) *user*)
           synthigy-id (id/extract *SYNTHIGY*)]
       (and (some? user-id) (some? synthigy-id) (= user-id synthigy-id)))
    ;; Check if both have _eid and they match
     (and (map? *user*) (some? (:_eid *user*)) (some? (:_eid *SYNTHIGY*)) (= (:_eid *user*) (:_eid *SYNTHIGY*)))
    ;; Check if user has ROOT role
     (contains? roles (id/extract *ROOT*)))))

(defn entity-allows?
  ([entity rules] (entity-allows? entity rules *roles*))
  ([entity rules roles]
   (try
     (cond
       ;; Nil entity always denies access
       (nil? entity) false
       ;; No rules or superuser bypasses access control
       (or (nil? *rules*) (superuser? roles)) true
       ;; Check rules for specific entity
       :else (letfn [(ok? [rule]
                       (boolean (not-empty (set/intersection roles (get-in *rules* [:entity entity rule])))))]
               (boolean (some ok? rules))))
     (catch Throwable ex
       (log/error "[IAM] Couldn't evaluate entity-allows for roles %s" roles)
       (throw ex)))))

(defn relation-allows?
  ([relation direction rules] (relation-allows? relation direction rules *roles*))
  ([relation direction rules roles]
   (try
     (if (or (nil? *rules*) (superuser? roles)) true
         (letfn [(ok? [rule]
                   (boolean
                     (not-empty
                       (set/intersection roles (get-in *rules* [:relation relation direction rule])))))]
           (some ok? rules)))
     (catch Throwable ex
       (log/error "[IAM] Couldn't evaluate relation-allows for roles %s" roles)
       (throw ex)))))

(defn roles-allowed?
  [roles]
  (or
    (superuser?)
    (nil? *roles*)
    (not-empty (set/intersection roles *roles*))))

(defn scope-allowed?
  ([permission]
   (scope-allowed? *roles* permission))
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

(defn start
  []
  (when (#{"true" "TRUE" "YES" "yes" "y" "1"} (env :synthigy-iam-enforce-access))
    (let [model (dataset/deployed-model)
          role-entity (core/get-entity model :iam/user-role)
          relations (core/focus-entity-relations model role-entity)
          relation-ids (set (map id/extract relations))
          all-ids (->
                    relation-ids
                     ;; Disj permissions and users
                    (disj #uuid "16ca53f4-0fe3-4122-93dd-1e86fd1b58db"
                          #uuid "1a2cc45d-1301-4fdd-bb02-650362165b37")
                    (conj :iam/user-role))
          delta-chan (async/chan (async/sliding-buffer 1))]

      ;; Store channel and subscribed elements for cleanup
      (reset! delta-subscription-state {:chan delta-chan
                                        :subscribed-elements all-ids})

      (doseq [element all-ids]
        (log/infof "[IAM] Subscribing to dataset delta channel for: %s" element)
        (async/sub core/*delta-publisher* element delta-chan))
      ;; Start idle service that will listen on delta changes
      (async/go-loop
        [_ (async/<! delta-chan)]
        (log/debugf "[IAM] Received something at delta channel")
        ;; When first delta change is received start inner loop
        (loop [[idle-value] (async/alts!
                              [;; That will check for new delta values
                               delta-chan
                             ;; Or timeout
                               (async/go
                                 (async/<! (async/timeout 5000))
                                 ::TIMEOUT)])]
          (log/debugf "[IAM] Next idle value is: %s" idle-value)
          ;; IF timeout is received than reload rules
          (if (= ::TIMEOUT idle-value)
            (do
              (log/info "[IAM] Reloading role access!")
              (load-rules)
              (load-scopes))
            ;; Otherwise some other delta has been received and
            ;; inner loop will be repeated
            (recur (async/alts!
                     [;; That will check for new delta values
                      delta-chan
                     ;; Or timeout
                      (async/go
                        (async/<! (async/timeout 5000))
                        ::TIMEOUT)]))))
        ;; when reloading is complete, wait for new delta value
        ;; and repeat process
        (recur (async/<! delta-chan)))
      (load-rules)
      (load-scopes))))

(defn enforced? [] (some? *rules*))

(defn stop
  []
  (log/info "[IAM] Stopping access control...")

  ;; Clean up delta channel subscriptions
  (let [{:keys [chan subscribed-elements]} @delta-subscription-state]
    (when chan
      (log/info "[IAM] Closing delta channel and unsubscribing...")
      ;; Unsubscribe from all elements
      (doseq [element subscribed-elements]
        (when core/*delta-publisher*
          (async/unsub core/*delta-publisher* element chan)))
      ;; Close the channel (this will cause go-loop to exit)
      (async/close! chan)
      ;; Clear state
      (reset! delta-subscription-state {:chan nil
                                        :subscribed-elements #{}})))

  ;; Clear rules and scopes
  (alter-var-root #'*rules* (constantly nil))
  (alter-var-root #'*scopes* (constantly nil))

  (log/info "[IAM] Access control stopped")
  nil)

;;; ============================================================================
;;; Dataset Access Protocol Implementation
;;; ============================================================================

(defrecord IAMAccessControl []
  access.protocol/AccessControl

  (entity-allows? [_ entity-id operations]
    ;; Call existing IAM function with operations as vector
    (entity-allows? entity-id (vec operations) *roles*))

  (relation-allows? [_ relation-id operations]
    ;; Without direction, check if superuser or rules are nil
    ;; This is used when direction doesn't matter or isn't known
    (if (or (nil? *rules*) (superuser? *roles*))
      true
      ;; Otherwise require direction to be specified
      false))

  (relation-allows? [_ relation-id from-to operations]
    ;; Call existing IAM function with from-to direction and operations as vector
    (relation-allows? relation-id from-to (vec operations) *roles*))

  (scope-allowed? [_ scope]
    ;; Call existing IAM function
    (scope-allowed? *roles* scope))

  (roles-allowed? [_ role-ids]
    ;; Call existing IAM function
    (roles-allowed? role-ids))

  (superuser? [_]
    ;; Call existing IAM function
    (superuser? *roles*))

  (get-user [_]
    ;; Return current user from context
    *user*)

  (get-roles [_]
    ;; Return current roles from context
    (or *roles* #{}))

  (get-groups [_]
    ;; Return current groups from context
    (or *groups* #{})))
