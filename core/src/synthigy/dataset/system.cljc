;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.system
  "What a deploy changes on the engine-owned system entities and relations, and the field-key collisions it adds."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.naming :as naming]))

(def attribute-keys [:name :type :constraint :configuration :active])

(def relation-keys [:from :to :from-label :to-label :cardinality :configuration :active])

(defn system-model?
  "True when `model` is a version model of a system dataset: its RLS on system entities counts."
  [model]
  (true? (get-in model [:configuration :system])))

(defn mark
  "The system mark of an entity (:closed or :open) or relation (:closed), nil when not system."
  [x]
  (get-in x [:configuration :system]))

(defn entity-marks
  "Entity id -> system mark over `models`; a system model's mark wins over a copy's."
  [models]
  (let [collect (fn [ms]
                  (reduce (fn [acc m]
                            (reduce-kv (fn [a eid e] (if-let [k (mark e)] (assoc a eid k) a))
                                       acc (:entities m)))
                          {} ms))]
    (merge (collect (remove system-model? models))
           (collect (filter system-model? models)))))

(defn relation-marks
  "Ids of the relations `model` marks system."
  [model]
  (reduce-kv (fn [a rid r] (cond-> a (mark r) (conj rid))) #{} (:relations model)))

(defn model-system-ids
  "System entities of `model` (id -> :closed or :open), its system relation ids, and the system `datasets`."
  [model datasets]
  {:entities (entity-marks [model])
   :relations (relation-marks model)
   :datasets (set datasets)})

(defn open?
  "True when system entity `eid` takes guards from any dataset."
  [{:keys [entities]} eid]
  (= :open (get entities eid)))

(defn relation-label [r]
  (str (get-in r [:from :name]) " " (:to-label r) " " (get-in r [:to :name])))

(defn attribute-value [k a]
  (case k
    :active (not (false? (:active a)))
    :configuration (not-empty (:configuration a))
    (get a k)))

(defn relation-value [k r]
  (case k
    (:from :to) (id/extract (get r k))
    :configuration (not-empty (:configuration r))
    :active (not (false? (:active r)))
    (get r k)))

(defn entity-changes
  [before after]
  (let [entity {:entity (:name before) :entity_id (id/extract before)}
        config #(not-empty (dissoc (:configuration %) :rls))
        by-id #(into {} (map (juxt id/extract identity)) (:attributes %))
        attrs-before (by-id before)
        attrs-after (by-id after)
        attribute #(assoc entity :attribute (:name %) :attribute_id (id/extract %))]
    (concat
     (when (not= (:name before) (:name after))
       [(assoc entity :field :name)])
     (when (not= (false? (:active before)) (false? (:active after)))
       [(assoc entity :field :active)])
     (for [k (sort (distinct (concat (keys (config before)) (keys (config after)))))
           :when (not= (get (config before) k) (get (config after) k))]
       (assoc entity :field k))
     (for [aid (distinct (map id/extract (concat (:attributes before) (:attributes after))))
           :let [a (get attrs-before aid)
                 b (get attrs-after aid)]
           change (cond
                    (nil? a) [(assoc (attribute b) :change :added)]
                    (nil? b) [(assoc (attribute a) :change :removed)]
                    :else (for [k attribute-keys
                                :when (not= (attribute-value k a) (attribute-value k b))]
                            (assoc (attribute a) :field k)))]
       change))))

(defn relation-changes
  [before after]
  (let [relation {:relation (relation-label before) :relation_id (id/extract before)}]
    (for [k relation-keys
          :when (not= (relation-value k before) (relation-value k after))]
      (assoc relation :field k))))

(defn system-changes
  "Changes `preview` makes to the system entities and relations (`system-ids`) of `global`, RLS excluded; a dropped one counts as removed."
  [global preview {:keys [entities relations]}]
  (vec
   (concat
    (for [before (core/get-entities global)
          :when (contains? entities (id/extract before))
          :let [after (core/get-entity preview (id/extract before))]
          change (if after
                   (entity-changes before after)
                   [{:entity (:name before) :entity_id (id/extract before) :change :removed}])]
      change)
    (for [before (core/get-relations global)
          :when (contains? relations (id/extract before))
          :let [after (core/get-relation preview (id/extract before))]
          change (if after
                   (relation-changes before after)
                   [{:relation (relation-label before) :relation_id (id/extract before) :change :removed}])]
      change))))

(defn without-system-rls
  "`model` without RLS on the closed system entities of `system-ids`; only system datasets declare it."
  [model {:keys [entities] :as system-ids}]
  (reduce (fn [m e]
            (let [eid (id/extract e)]
              (if (and (contains? entities eid)
                       (not (open? system-ids eid))
                       (get-in e [:configuration :rls]))
                (core/set-entity m (update e :configuration dissoc :rls))
                m)))
          model
          (core/get-entities model)))

(defn version-rls
  "A version's `model` with only the RLS it may declare: all of it for a system model, else none on closed system entities."
  [model marks]
  (cond-> model
    (not (system-model? model)) (without-system-rls {:entities marks})))

(defn latest-per-dataset
  "The last version of each dataset in `versions`, in their order."
  [versions]
  (let [latest (reduce #(assoc %1 (id/extract (:dataset %2)) (id/extract %2)) {} versions)]
    (filterv #(= (id/extract %) (get latest (id/extract (:dataset %)))) versions)))

(defn rls-guards
  [rls]
  (when (:enabled rls)
    (into #{} (map #(update % :operation set)) (:guards rls))))

(defn system-rls-additions
  "Closed system entities whose RLS in `model` enables or adds guards that `global` lacks, as wire details."
  [global model {:keys [entities] :as system-ids}]
  (vec (for [e (core/get-entities model)
             :let [eid (id/extract e)
                   rls (get-in e [:configuration :rls])]
             :when (and (contains? entities eid) (not (open? system-ids eid)) (:enabled rls))
             :let [system (rls-guards (get-in (core/get-entity global eid) [:configuration :rls]))
                   added (remove #(contains? system %) (rls-guards rls))]
             :when (or (nil? system) (seq added))]
         {:entity (:name e) :entity_id eid :guards (mapv :id added)})))

(defn open-rls-problems
  "On open system entities: a guard of `model` reusing a `global` guard id with other content, or RLS switched on where `global` has it off."
  [global model {:keys [entities]}]
  (reduce-kv
   (fn [acc eid kind]
     (let [rls (get-in (core/get-entity model eid) [:configuration :rls])]
       (if (and (= :open kind) (:enabled rls))
         (let [present (get-in (core/get-entity global eid) [:configuration :rls])
               by-id (reduce #(assoc %1 (:id %2) (update %2 :operation set)) {} (:guards present))
               entity (:name (core/get-entity model eid))
               replaced (reduce (fn [a g]
                                  (let [old (get by-id (:id g))]
                                    (cond-> a
                                      (and old (not= old (update g :operation set))) (conj (:id g)))))
                                [] (:guards rls))]
           (cond-> acc
             (seq replaced) (conj {:entity entity :entity_id eid :guards replaced :change :replaced})
             (not (:enabled present)) (conj {:entity entity :entity_id eid :change :rls-enabled})))
         acc)))
   [] entities))

(defn open-rls-text [{:keys [entity guards change]}]
  (case change
    :replaced (str entity " (" (count guards) (if (= 1 (count guards)) " guard replaced)" " guards replaced)"))
    :rls-enabled (str entity " (RLS switched on)")))

(defn system-marks
  "What `preview` marks system that `global` does not: entities, relations, and the version `model` itself unless its dataset is a system dataset."
  [global preview model system-dataset?]
  (let [entities (entity-marks [global])
        relations (relation-marks global)]
    (cond-> (-> []
                (into (comp (remove #(contains? entities %))
                            (map (fn [eid] {:entity (:name (core/get-entity preview eid)) :entity_id eid})))
                      (keys (entity-marks [preview])))
                (into (comp (remove relations)
                            (map (fn [rid] {:relation (relation-label (core/get-relation preview rid)) :relation_id rid})))
                      (relation-marks preview)))
      (and (system-model? model) (not system-dataset?)) (conj {:model true}))))

(defn rls-addition-text [{:keys [entity guards]}]
  (str entity (when (seq guards) (str " (" (count guards) (if (= 1 (count guards)) " guard)" " guards)")))))

(defn label
  "Keyword or wire string as text, namespace kept."
  [x]
  (if (keyword? x) (subs (str x) 1) (str x)))

(defn change-text [{:keys [entity attribute relation field change]}]
  (str (or relation (cond-> entity attribute (str "." attribute)))
       " "
       (if change
         (label change)
         (str (label field) " changed"))))

(defn conflict-key [{:keys [entity name]}]
  [(id/extract entity) name])

(defn new-field-conflicts
  "Field-key conflicts in `preview` that `global` does not already have."
  [global preview]
  (let [existing (into #{} (map conflict-key) (naming/entity-field-conflicts global))]
    (vec (remove #(contains? existing (conflict-key %)) (naming/entity-field-conflicts preview)))))

(defn new-name-conflicts
  "Entity-name conflicts in `preview` that `global` does not already have, as wire details."
  [global preview]
  (let [existing (into #{} (map :name) (naming/entity-name-conflicts global))]
    (vec (for [{:keys [name entities]} (naming/entity-name-conflicts preview)
               :when (not (contains? existing name))]
           {:table name
            :entities (mapv (fn [e] {:entity (:name e) :entity_id (id/extract e)}) entities)}))))

(defn name-conflict-text [{:keys [table entities]}]
  (str table ": " (str/join ", " (map #(str (:entity %) " (" (:entity_id %) ")") entities))))

(defn conflict-details
  "Wire shape of a field-key conflict: names and ids, no records."
  [{:keys [entity name sources]}]
  {:entity (:name entity)
   :entity_id (id/extract entity)
   :field name
   :sources (mapv (fn [{:keys [kind display field ref]}]
                    (case kind
                      :attribute {:kind :attribute :name display :attribute_id (id/extract ref)}
                      :relation {:kind :relation :name display :label field
                                 :relation (str (get-in ref [:from :name]) " → " (get-in ref [:to :name]))
                                 :relation_id (id/extract ref)}))
                  sources)})

(defn conflict-text [{:keys [entity field sources]}]
  (str entity "." field ": "
       (str/join ", " (map (fn [{:keys [kind name relation]}]
                             (if (= "attribute" (label kind))
                               (str "attribute " name)
                               (str "relation label " name " (" relation ")")))
                           sources))))
