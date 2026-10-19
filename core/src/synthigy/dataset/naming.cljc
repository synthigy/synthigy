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

(ns synthigy.dataset.naming
  "Name normalization and collision detection for entities, attributes and relation labels."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]))

(defn normalize
  "Snake_case form of a display name, as the SQL layer names tables and columns."
  [s]
  (when s
    (-> s
        str/lower-case
        (str/replace #"[\s_\-.\$\[\]\{\}\#]+" "_"))))

(defn active? [x]
  (not (false? (:active x))))

(defn entity-name-conflicts
  "Active entities whose normalized names collide, as {:name :entities}."
  [model]
  (when model
    (->> (core/get-entities model)
         (filter active?)
         (remove #(str/blank? (:name %)))
         (group-by #(normalize (:name %)))
         (keep (fn [[nname group]]
                 (when (> (count group) 1)
                   {:name nname :entities (vec group)})))
         seq)))

(defn entity-rename-conflict
  "The other active entity that renaming `entity-id` to `new-name` would collide with."
  [model entity-id new-name]
  (let [nname (normalize new-name)]
    (when (and model (not (str/blank? nname)))
      (some (fn [e]
              (when (and (active? e)
                         (= nname (normalize (:name e)))
                         (not= entity-id (id/extract e)))
                e))
            (core/get-entities model)))))

(defn attribute-rename-conflict
  "The other active attribute that renaming `attribute-id` to `new-name` would collide with."
  [attributes attribute-id new-name]
  (let [nname (normalize new-name)]
    (when-not (str/blank? nname)
      (some (fn [a]
              (when (and (active? a)
                         (= nname (normalize (:name a)))
                         (not= attribute-id (id/extract a)))
                a))
            attributes))))

(defn find-inactive-attribute-match
  "The inactive attribute whose normalized name matches `new-name`, to reactivate instead of duplicating."
  [attributes new-name]
  (let [nname (normalize new-name)]
    (when-not (str/blank? nname)
      (some (fn [a]
              (when (and (not (active? a))
                         (= nname (normalize (:name a))))
                a))
            attributes))))

(defn attribute-sources
  [entity]
  (->> (:attributes entity)
       (filter active?)
       (remove #(str/blank? (:name %)))
       (map (fn [a] {:name (normalize (:name a)) :display (:name a)
                     :kind :attribute :ref a}))))

(defn relation-label-sources
  "Field sources the active relation labels put on `entity-id`: :to-label on :from, :from-label on :to."
  [model entity-id]
  (->> (core/get-relations model)
       (filter active?)
       (mapcat (fn [r]
                 (let [from-id (id/extract (:from r))
                       to-id   (id/extract (:to r))]
                   (cond-> []
                     (and (= entity-id from-id) (not (str/blank? (:to-label r))))
                     (conj {:name (normalize (:to-label r)) :display (:to-label r)
                            :kind :relation :field :to-label :ref r})
                     (and (= entity-id to-id) (not (str/blank? (:from-label r))))
                     (conj {:name (normalize (:from-label r)) :display (:from-label r)
                            :kind :relation :field :from-label :ref r})))))))

(defn entity-field-sources
  [model entity]
  (concat (attribute-sources entity)
          (relation-label-sources model (id/extract entity))))

(defn entity-field-conflicts
  "Field names used more than once on an active entity, attributes and relation labels together, as {:entity :name :sources}."
  [model]
  (when model
    (->> (core/get-entities model)
         (filter active?)
         (mapcat (fn [e]
                   (->> (entity-field-sources model e)
                        (group-by :name)
                        (keep (fn [[nname srcs]]
                                (when (> (count srcs) 1)
                                  {:entity e :name nname :sources (vec srcs)}))))))
         seq)))

(defn relation-label-conflict
  "The field source on `origin-entity` that setting `label-field` of `relation-id` to `new-name` would collide with."
  [model origin-entity relation-id label-field new-name]
  (let [nname (normalize new-name)]
    (when-not (str/blank? nname)
      (some (fn [{:keys [name kind ref field] :as src}]
              (when (and (= nname name)
                         (not (and (= kind :relation)
                                   (= relation-id (id/extract ref))
                                   (= label-field field))))
                src))
            (entity-field-sources model origin-entity)))))

(defn attribute-relation-conflict
  "The relation label on `entity` that an attribute renamed to `new-name` would collide with."
  [model entity new-name]
  (let [nname (normalize new-name)]
    (when-not (str/blank? nname)
      (some (fn [{:keys [name] :as src}]
              (when (= nname name) src))
            (relation-label-sources model (id/extract entity))))))
