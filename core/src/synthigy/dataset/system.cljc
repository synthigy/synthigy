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

(defn model-ids
  "Entity and relation ids of `models`."
  [models]
  {:entities (into #{} (comp (mapcat core/get-entities) (map id/extract)) models)
   :relations (into #{} (comp (mapcat core/get-relations) (map id/extract)) models)})

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
  "`model` without RLS on the system entities of `system-ids`; only the system datasets declare it."
  [model {:keys [entities]}]
  (reduce (fn [m e]
            (if (and (contains? entities (id/extract e)) (get-in e [:configuration :rls]))
              (core/set-entity m (update e :configuration dissoc :rls))
              m))
          model
          (core/get-entities model)))

(defn rls-guards
  [rls]
  (when (:enabled rls)
    (into #{} (map #(update % :operation set)) (:guards rls))))

(defn system-rls-additions
  "System entities whose RLS in `model` enables or adds guards that `global` lacks, as wire details."
  [global model {:keys [entities]}]
  (vec (for [e (core/get-entities model)
             :let [eid (id/extract e)
                   rls (get-in e [:configuration :rls])]
             :when (and (contains? entities eid) (:enabled rls))
             :let [system (rls-guards (get-in (core/get-entity global eid) [:configuration :rls]))
                   added (remove #(contains? system %) (rls-guards rls))]
             :when (or (nil? system) (seq added))]
         {:entity (:name e) :entity_id eid :guards (mapv :id added)})))

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
