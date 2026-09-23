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

(ns synthigy.dataset.runtime
  "Composes the runtime model — deployed model augmented with identity, audit, and
   reference-typed attrs as first-class entries. Composition order matters in
   `build`: identity, then audit, then reference. See memory: project_runtime_model_plan.md"
  (:require
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id])
  (:import
   [synthigy.dataset.id UUIDProvider NanoIDProvider]))

(defn find-entity
  "Looks up an entity by either id form (UUID or xid string) — entities carry
   both."
  [model id]
  (or (get (:entities model) id)
      (some #(when (or (= id (:euuid %)) (= id (:xid %))) %)
            (vals (:entities model)))))

(defn append-attrs
  "Appends attrs to entity.attributes, skipping ones whose name is already
   present."
  [entity new-attrs]
  (let [existing (into #{} (map :name) (:attributes entity))
        addins   (remove #(contains? existing (:name %)) new-attrs)]
    (cond-> entity
      (seq addins) (update :attributes (fnil into []) addins))))

(defn enhance-each-entity [model f]
  (update model :entities
          (fn [es] (into {} (map (fn [[k v]] [k (f v)])) es))))

(defn system-attrs-for
  "Emits only the active provider's id attr (xid under NanoID, euuid under UUID) — emitting
   both leaked the non-active id onto /schema and /data."
  [provider entity]
  (let [eid   (id/extract* provider entity)
        field (name (id/key* provider))]
    [(merge (id/derive* provider eid field)
            {:name field :type "uuid" :active true})]))

(extend-protocol enhance/ModelEnhancement
  UUIDProvider
  (enhance-model [provider model]
    (enhance-each-entity
     model
     #(append-attrs % (system-attrs-for provider %))))

  NanoIDProvider
  (enhance-model [provider model]
    (enhance-each-entity
     model
     #(append-attrs % (system-attrs-for provider %)))))

(defn audit-attrs-for [entity]
  (let [eid (id/extract entity)]
    ;; who-columns come from core/audit-ref-attrs — same derived ids RLS
    ;; resolves against, never inline this again
    (into
     (cond-> []
       (core/audit-modified? entity)
       (conj (merge (id/derive-id eid "modified_on")
                    {:name "modified_on" :type "timestamp" :active true}))

       (core/audit-created? entity)
       (conj (merge (id/derive-id eid "created_on")
                    {:name "created_on" :type "timestamp" :active true})))
     (core/audit-ref-attrs entity))))

(defrecord AuditEnhancer []
  enhance/ModelEnhancement
  (enhance-model [_ model]
    (enhance-each-entity model #(append-attrs % (audit-attrs-for %)))))

(defn attr->relation
  "Builds an ERDRelation from a ref-typed attr; the relation's identity is the
   attr's identity."
  [from-entity attr to-entity]
  (core/map->ERDRelation
   {:euuid         (:euuid attr)
    :xid           (:xid attr)
    :from          (id/extract from-entity)
    :to            (id/extract to-entity)
    :from-label    nil
    :to-label      (:name attr)
    :cardinality   "m2o"
    :path          nil
    :configuration nil
    :active        true
    :claimed-by    nil}))

(defn entity-reference-relations
  "All synthetic relations contributed by entity — one per ref-typed attr with a
   resolvable target."
  [model entity]
  (keep
   (fn [attr]
     (when-let [target-id (core/reference-entity-id (:type attr))]
       (when-let [to-entity (find-entity model target-id)]
         (attr->relation entity attr to-entity))))
   (:attributes entity)))

(defrecord ReferenceEnhancer []
  enhance/ModelEnhancement
  (enhance-model [_ model]
    (let [new-rels (mapcat (partial entity-reference-relations model)
                           (vals (:entities model)))]
      (reduce (fn [m rel]
                (let [k (id/extract rel)]
                  (if (get-in m [:relations k])
                    m
                    (assoc-in m [:relations k] rel))))
              model
              new-rels))))

(defn build
  "Composes the runtime view: identity attrs + audit attrs + reference-typed-attrs-as-relations.
   Idempotent; nil-safe."
  [model]
  (when model
    (->> model
         (enhance/enhance-model (id/current-provider))
         (enhance/enhance-model (->AuditEnhancer))
         (enhance/enhance-model (->ReferenceEnhancer)))))
