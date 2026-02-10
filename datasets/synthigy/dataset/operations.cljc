(ns synthigy.dataset.operations
  "ERDModel operations - implementations of ERDModelActions protocol.

  These implement CRUD operations for manipulating ERDModel records:
  - Entity operations: add/get/set/update/remove/replace
  - Relation operations: add/get/set/update/remove/create
  - Query operations: get-entities, get-relations, get-entity-relations

  ID access uses the current ID provider (id/key returns :euuid or :xid)."
  (:require
    [clojure.data]
    [clojure.set]
    [synthigy.dataset.core :as dataset]
    [synthigy.dataset.id :as id]))

;;; ============================================================================
;;; ID Helpers
;;; ============================================================================


(extend-protocol synthigy.dataset.core/ERDModelActions
  #?(:clj synthigy.dataset.core.ERDModel
     :cljs synthigy.dataset.core/ERDModel)

  ;; Entity Operations
  (get-entity [{:keys [entities clones]} id]
    (if-let [e (get entities id)]
      e
      (when-some [{:keys [entity position]} (get clones id)]
        (when-some [entity (get entities entity)]
          (assoc entity
            (id/key) id
            :position position
            :clone true
            :original (id/extract entity))))))

  (add-entity [this entity]
    (let [id (id/extract entity)]
      (assert (not-any? #{id} (map id/extract (dataset/get-entities this)))
              (str "Model already contains entity " id ":" (:name entity)))
      (let [id (or (id/extract entity) (id/generate))]
        (assoc-in this [:entities id] (assoc entity (id/key) id)))))

  (set-entity [this {:keys [clone]
                     :as entity}]
    (if clone
      (assoc-in this [:clones (id/extract entity) :position] (:position entity))
      (assoc-in this [:entities (id/extract entity)] entity)))

  (update-entity [this id f]
    (dataset/set-entity this (f (dataset/get-entity this id))))

  (remove-entity [{:keys [entities]
                   :as this} entity]
    (let [id (id/extract entity)
          entity (dataset/get-entity this id)
          relations' (map id/extract (dataset/get-entity-relations this entity))]
      (->
        this
        (update :clones (fn [clones]
                          (if (dataset/cloned? entity)
                            (dissoc clones (id/extract entity))
                            clones)))
        (update :entities dissoc id)
        (update :relations #(reduce dissoc % relations')))))

  (replace-entity
    [this {:keys [position]
           :as entity} replacement]
    (let [id (id/extract entity)
          id' (id/extract replacement)]
      (assert (some #{id} (map id/extract (dataset/get-entities this)))
              (str "Model doesn't contain entity " id ":" (:name entity)))
      (assert (not-any? #{id'} (map id/extract (dataset/get-entities this)))
              (str "Model already contains entity " id '":" (:name replacement)))
      (let [old-relations (dataset/get-entity-relations this entity)]
        (reduce
          (fn [model {:keys [from to]
                      :as relation}]
            ;; Add relation by checking which direction to change
            (dataset/add-relation
              model
              (cond-> relation
                (= id (id/extract from))
                (-> (assoc :from id') (update :to (id/key)))

                (= id (id/extract to))
                (-> (assoc :to id') (update :from (id/key))))))
          (->
            this
            ;; Remove entity removes all old connections
            (dataset/remove-entity entity)
            ;; Add new entity as replacement at the same position
            (dataset/add-entity (assoc replacement :position position)))
          ;; reduce all old connections and reconnect
          old-relations))))

  (get-entities [{:keys [entities]}]
    (vec (mapv val (sort-by :name entities))))

  ;; Relation Operations
  (get-relation [{:keys [relations]
                  :as this} id]
    (some-> (get relations id)
            (update :from (partial dataset/get-entity this))
            (update :to (partial dataset/get-entity this))))

  (add-relation [this relation]
    (let [id (id/extract relation)]
      (update this :relations assoc id relation)))

  (set-relation [this relation]
    (assoc-in this [:relations (id/extract relation)]
              (->
                relation
                (update :from (id/key))
                (update :to (id/key)))))

  (update-relation [this id f]
    (dataset/set-relation this (f (dataset/get-relation this id))))

  (remove-relation [this relation]
    (update this :relations dissoc (id/extract relation)))

  (create-relation
    ([this from to cardinality path]
     (let [id (id/generate)]
       (assoc-in this [:relations id]
                 (dataset/map->ERDRelation
                   {(id/key) id
                    :from (id/extract from)
                    :to (id/extract to)
                    :cardinality cardinality
                    :path path}))))

    ([this from to cardinality]
     (dataset/create-relation this from to cardinality nil))

    ([this from to]
     (dataset/create-relation this from to "o2o")))

  (get-relations [{:keys [relations]
                   :as this}]
    (reduce
      (fn [relations relation]
        (conj relations
              (-> relation
                  (update :from (partial dataset/get-entity this))
                  (update :to (partial dataset/get-entity this)))))
      []
      (mapv val relations)))

  (get-relations-between
    [this entity1 entity2]
    (let [e1 (id/extract entity1)
          e2 (id/extract entity2)
          valid? #{e1 e2}]
      (filter
        (fn [{{t1 (id/key)} :from
              {t2 (id/key)} :to}]
          (= #{t1 t2} valid?))
        (dataset/get-relations this))))

  (get-entity-relations [{:keys [relations]
                          :as this} entity]
    (let [id (id/extract entity)
          looking-for #{id}]
      (reduce
        (fn [r {:keys [from to]
                :as relation}]
          (let [relation' (->
                            relation
                            (update :from (partial dataset/get-entity this))
                            (update :to (partial dataset/get-entity this)))]
            (cond-> r
              (looking-for from) (conj relation')
              (looking-for to) (conj relation'))))
        []
        (vals relations)))))
