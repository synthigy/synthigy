;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.meta
  "Write rules for the Dataset meta-model rows;
  Dataset Entity, Attribute and Relation rows are the engine's,
  roles only reference them."
  (:require
   [clojure.set :as set]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.schema :as sql-schema]
   [synthigy.dataset.version :as version]))

(def engine-entities [:dataset/entity :dataset/entity-attribute :dataset/relation])

(def engine-relations [:dataset/version->entities :dataset/version->relations :dataset/entity->attributes])

(def model-entities (into [:dataset/dataset :dataset/version] engine-entities))

(def engine-owned-problem
  {:code "ENGINE_OWNED"
   :rule "engine_owned"
   :message "Dataset Entity, Attribute and Relation rows belong to the engine; a role references them by id only."})

(def version-link-problem
  {:code "ENGINE_OWNED"
   :rule "engine_owned"
   :message "Versions leave a dataset only by recall or destroy."})

(def purge-problem
  {:code "FORBIDDEN_OP"
   :rule "engine_owned"
   :message "Purge never reaches Dataset meta-model rows; delete the dataset or version instead."})

(defn problem-ex [{:keys [message] :as problem}]
  (ex-info message (dissoc problem :message)))

(defn meta-row-problems
  "Engine-owned rows or links that write `analysis` changes beyond an id reference."
  [analysis]
  (concat
   (for [entity-key engine-entities
         [_ row] (get-in analysis [:entity (version/table entity-key)])
         :when (seq (version/content-keys entity-key row))]
     engine-owned-problem)
   (for [relation-key engine-relations
         :when (seq (version/relation-writes analysis relation-key))]
     engine-owned-problem)))

(defn assert-meta-rows!
  "Refuse a principal's write that creates, changes or links an engine-owned row."
  [analysis]
  (when-not (dataset/engine-write?)
    (when-let [problem (first (meta-row-problems analysis))]
      (throw (problem-ex problem))))
  analysis)

(defn assert-write!
  "Every meta-model write rule, once per write."
  [analysis]
  (if (::checked analysis)
    analysis
    (-> analysis
        assert-meta-rows!
        version/assert-versions!
        version/assert-names!
        (assoc ::checked true))))

(defn assert-meta-delete!
  [data]
  (when-not (dataset/engine-write?)
    (throw (problem-ex engine-owned-problem)))
  data)

(defn reach
  "Entity and relation ids that `selection` reaches from entity `entity-id`."
  [entity-id selection]
  (loop [todo (list [entity-id (dataset/normalize-selection selection)])
         entities #{}
         relations #{}]
    (if-let [[eid sel] (first todo)]
      (let [schema-relations (:relations (sql-schema/deployed-schema-entity eid))
            nested (for [[k v] sel
                         :let [{:keys [relation to]} (get schema-relations k)]
                         :when (and relation (sequential? v))
                         cfg v]
                     [relation to (:selections cfg)])]
        (recur (into (rest todo) (map (fn [[_ to s]] [to s])) nested)
               (conj entities eid)
               (into relations (map first) nested)))
      {:entities entities :relations relations})))

(defn assert-purge!
  "Refuse a purge whose selection reaches a meta-model row."
  [entity-id selection]
  (let [{:keys [entities]} (reach entity-id selection)]
    (when (seq (set/intersection entities (into #{} (map id/entity) model-entities)))
      (throw (problem-ex purge-problem)))))

(defn assert-slice!
  "Refuse a principal's slice of an engine-owned link, or a non-superuser's slice of a dataset's versions."
  [entity-id selection]
  (when-not (dataset/engine-write?)
    (let [{:keys [relations]} (reach entity-id selection)]
      (when (seq (set/intersection relations (into #{} (map id/relation) engine-relations)))
        (throw (problem-ex engine-owned-problem)))
      (when (and (contains? relations (id/relation :dataset/dataset->versions))
                 (not (access/superuser?)))
        (throw (problem-ex version-link-problem))))))

(defmethod enhance/write [Object (id/entity :dataset/dataset :xid)] [_ _ analysis _] (assert-write! analysis))
(defmethod enhance/write [Object (id/entity :dataset/version :xid)] [_ _ analysis _] (assert-write! analysis))
(defmethod enhance/write [Object (id/entity :dataset/entity :xid)] [_ _ analysis _] (assert-write! analysis))
(defmethod enhance/write [Object (id/entity :dataset/entity-attribute :xid)] [_ _ analysis _] (assert-write! analysis))
(defmethod enhance/write [Object (id/entity :dataset/relation :xid)] [_ _ analysis _] (assert-write! analysis))

(defmethod enhance/delete [Object (id/entity :dataset/dataset :xid)] [_ _ data _] (version/assert-dataset-delete! data))
(defmethod enhance/delete [Object (id/entity :dataset/version :xid)] [_ _ data _] (version/assert-version-delete! data))
(defmethod enhance/delete [Object (id/entity :dataset/entity :xid)] [_ _ data _] (assert-meta-delete! data))
(defmethod enhance/delete [Object (id/entity :dataset/entity-attribute :xid)] [_ _ data _] (assert-meta-delete! data))
(defmethod enhance/delete [Object (id/entity :dataset/relation :xid)] [_ _ data _] (assert-meta-delete! data))
