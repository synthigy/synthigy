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

(ns synthigy.dataset.operations
  "ERDModelActions protocol implementation: CRUD over ERDModel
   entities/relations."
  (:require
    [clojure.data]
    [clojure.set]
    [synthigy.dataset.core :as dataset]
    [synthigy.dataset.id :as id]))

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
    ;; euuid-first dual id (not native id/generate) keeps the entity
    ;; portable/transform-safe
    (let [entity (if (id/extract entity) entity (merge entity (id/new-model-node-id)))
          id (id/extract entity)]
      (assert (not-any? #{id} (map id/extract (dataset/get-entities this)))
              (str "Model already contains entity " id ":" (:name entity)))
      (assoc-in this [:entities id] (assoc entity (id/key) id))))

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
            (dataset/add-relation
              model
              (cond-> relation
                (= id (id/extract from))
                (-> (assoc :from id') (update :to (id/key)))

                (= id (id/extract to))
                (-> (assoc :to id') (update :from (id/key))))))
          (->
            this
            (dataset/remove-entity entity)
            (dataset/add-entity (assoc replacement :position position)))
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
     ;; model node: euuid-first dual id, map-keyed by the active form
     (let [node-id (id/new-model-node-id)
           id (id/extract node-id)]
       (assoc-in this [:relations id]
                 (dataset/map->ERDRelation
                   (merge {:from (id/extract from)
                           :to (id/extract to)
                           :cardinality cardinality
                           :path path}
                          node-id)))))

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
