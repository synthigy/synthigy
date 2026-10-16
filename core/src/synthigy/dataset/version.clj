;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.version
  "Dataset Version rules: a version name is used once per dataset, and only deploy and recall set or change a deployed version."
  (:require
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
   [synthigy.log :as log]))

(defn as-engine
  "Calls `f` seeing every row, as the engine."
  [f]
  (binding [access/*access-control* (access/->AllowAllAccess)]
    (f)))

(defn table [entity-key]
  (entity->table-name (core/get-entity (dataset/deployed-model) (id/entity entity-key))))

(defn content-keys
  "Keys of `row` that write an attribute of entity `entity-key`."
  [entity-key row]
  (let [attribute-keys (into #{}
                             (comp (filter :active) (map (comp keyword normalize-name :name)))
                             (:attributes (core/get-entity (dataset/deployed-model) (id/entity entity-key))))]
    (filter attribute-keys (keys row))))

(defn relation-writes
  "Each pair of relation `relation-key` that write `analysis` links or clears, as {:from :from/tmp :to/tmp}."
  [analysis relation-key]
  (let [rid (id/relation relation-key)]
    (for [rels [(:relations/one analysis) (:relations/many analysis)]
          [{:keys [relation from]} pairs] rels
          :when (= rid relation)
          [a b] pairs]
      {:from from :from/tmp a :to/tmp b})))

(defn deployed? [row]
  (boolean (#{true 1} (:deployed row))))

;;; Version names

(defn name-holders
  "Other versions of dataset `dataset-id` already named `version-name`."
  [dataset-id version-name version-id]
  (let [k (id/key)]
    (->> (as-engine
          #(dataset/search-entity
            :dataset/dataset
            {:_where {k {:_eq dataset-id}}}
            {k nil
             :versions [{:selections {k nil :deployed nil}
                         :args {:_where {:name {:_eq version-name}}}}]}))
         (mapcat :versions)
         (remove #(= version-id (get % k))))))

(defn name-problem
  "VERSION_NAME_TAKEN for `version` when another version of its dataset holds its name, else nil."
  [{version-name :name :as version}]
  (when-let [dataset-id (and version-name (id/extract (:dataset version)))]
    (when-let [holders (not-empty (name-holders dataset-id version-name (id/extract version)))]
      (let [deployed (boolean (some deployed? holders))]
        {:code "VERSION_NAME_TAKEN"
         :rule "version_name"
         :message (str "Version " version-name " already exists in this dataset"
                       (when deployed " and is deployed") ".")
         :hint "Save under the next free version name."
         :details {:version version-name :dataset dataset-id :deployed deployed}}))))

(defn written-names
  "Each Dataset Version row in write `analysis` that carries a name, as {:name :dataset {k id} k id}."
  [analysis]
  (let [k (id/key)
        versions (table :dataset/version)
        datasets (table :dataset/dataset)
        links (for [rels [(:relations/one analysis) (:relations/many analysis)]
                    [{from :from/table to :to/table} pairs] rels
                    :when (= #{from to} #{versions datasets})
                    [a b] pairs]
                (if (= from versions) [a b] [b a]))
        dataset-of (into {} links)]
    (for [[tmp row] (get-in analysis [:entity versions])
          :when (:name row)
          :let [vid (get row k)
                dataset-id (or (when-let [d (dataset-of tmp)] (get-in analysis [:entity datasets d k]))
                               (some-> (when vid (dataset/stored-version {k vid})) :dataset id/extract))]
          :when dataset-id]
      (cond-> {:name (:name row) :dataset {k dataset-id}}
        vid (assoc k vid)))))

(defn assert-names!
  "Refuse a principal's write that names a version like another version of its dataset; engine writes only warn."
  [analysis]
  (doseq [version (written-names analysis)
          :let [problem (name-problem version)]
          :when problem]
    (if (dataset/engine-write?)
      (log/warn {:id ::name-taken
                 :data {:action :writing :subject :dataset-version :details (:details problem)}}
                (:message problem))
      (throw (ex-info (:message problem) (dissoc problem :message)))))
  analysis)

;;; Deployed versions

(def deploy-state-problem
  {:code "ENGINE_OWNED"
   :rule "engine_owned"
   :message "Only deploy and recall set a version's deployed state."})

(defn immutable-problem [version details]
  {:code "IMMUTABLE_VERSION_ALTERED"
   :rule "deploy_immutable"
   :message (str "Deployed version " (:name version) " is immutable; create a new version to change it.")
   :details details})

(defn unlink-problem [details]
  {:code "IMMUTABLE_VERSION_ALTERED"
   :rule "deploy_immutable"
   :message "Deployed versions leave a dataset only by recall or destroy."
   :details details})

(defn stored-versions
  "Stored rows of version ids `ids`, by id."
  [ids]
  (when (seq ids)
    (let [k (id/key)]
      (into {}
            (map (juxt #(get % k) identity))
            (as-engine
             #(dataset/search-entity :dataset/version {k {:_in (vec (distinct ids))}}
                                     {k nil :name nil :deployed nil :model nil
                                      :dataset [{:selections {k nil}}]}))))))

(defn stored-dataset
  "Id and deployed version ids of the dataset `row` names by id or name."
  [row]
  (let [k (id/key)
        args (cond (get row k) {k (get row k)}
                   (:name row) {:name (:name row)})]
    (when args
      (as-engine
       #(dataset/get-entity :dataset/dataset args
                            {k nil :versions [{:selections {k nil} :args {:deployed {:_eq true}}}]})))))

(defn changed-keys
  "Content keys of `row` whose value differs from deployed version `stored`."
  [row stored]
  (remove (fn [k]
            (case k
              :name (= (:name row) (:name stored))
              :model (= (dataset/persisted-model (:model row)) (dataset/persisted-model (:model stored)))
              :deployed (= (deployed? row) (deployed? stored))
              false))
          (content-keys :dataset/version row)))

(defn version-problems
  "Problems write `analysis` causes on version deployed state and on deployed versions."
  [analysis]
  (let [k (id/key)
        vtable (table :dataset/version)
        dtable (table :dataset/dataset)
        version-entity (id/entity :dataset/version)
        rows (get-in analysis [:entity vtable])
        links (relation-writes analysis :dataset/dataset->versions)]
    (when (or (seq rows) (seq links))
      (let [stored (stored-versions (keep #(get % k) (vals rows)))
            stored-of #(get stored (get-in rows [% k]))
            dataset-id (memoize #(let [row (get-in analysis [:entity dtable %])]
                                   (or (get row k) (some-> row stored-dataset id/extract))))
            oriented (for [{:keys [from] a :from/tmp b :to/tmp} links]
                       (if (= from version-entity)
                         {:version a :dataset b :side :version}
                         {:version b :dataset a :side :dataset}))
            replaced (when-not (:write/stack? analysis)
                       (group-by :dataset (filter #(= :dataset (:side %)) oriented)))]
        (concat
         (for [[_ row] rows
               :let [s (get stored (get row k))]
               :when (or (some? (:deployed_on row))
                         (and (contains? row :deployed) (not= (deployed? row) (deployed? s))))]
           deploy-state-problem)
         (for [[_ row] rows
               :let [s (get stored (get row k))]
               :when (deployed? s)
               :let [changed (changed-keys row s)]
               :when (seq changed)]
           (immutable-problem s {:version (:name s) :changed (vec (sort changed))}))
         (for [{:keys [version dataset]} oriented
               :let [s (some-> version stored-of)]
               :when (deployed? s)
               :when (or (nil? dataset)
                         (not= (id/extract (:dataset s)) (dataset-id dataset)))]
           (immutable-problem s {:version (:name s) :moved true}))
         (for [[d pairs] replaced
               :let [stored-d (some-> (get-in analysis [:entity dtable d]) stored-dataset)
                     listed (into #{} (keep #(some-> (:version %) stored-of id/extract)) pairs)
                     dropped (remove #(contains? listed (id/extract %)) (:versions stored-d))]
               :when (seq dropped)]
           (unlink-problem {:dataset (id/extract stored-d)
                            :unlinked (mapv id/extract dropped)})))))))

(defn assert-versions!
  "Refuse a non-superuser write that sets a version's deployed state or changes, moves or unlinks a deployed version."
  [analysis]
  (when-not (or (dataset/engine-write?) (access/superuser?))
    (when-let [problem (first (version-problems analysis))]
      (throw (ex-info (:message problem) (dissoc problem :message)))))
  analysis)

(defn assert-version-delete!
  "Refuse a non-superuser plain delete of a deployed version; recall removes those."
  [data]
  (when-not (or (dataset/engine-write?) (access/superuser?))
    (when-let [vid (id/extract data)]
      (when-let [s (get (stored-versions [vid]) vid)]
        (when (deployed? s)
          (let [problem (immutable-problem s {:version (:name s) :deleted true})]
            (throw (ex-info (:message problem) (dissoc problem :message))))))))
  data)

(defn assert-dataset-delete!
  "Refuse a non-superuser plain delete of a dataset that has deployed versions; destroy removes those."
  [data]
  (when-not (or (dataset/engine-write?) (access/superuser?))
    (when-let [stored (stored-dataset data)]
      (when (seq (:versions stored))
        (let [problem (unlink-problem {:dataset (id/extract stored) :deleted true})]
          (throw (ex-info (:message problem) (dissoc problem :message)))))))
  data)
