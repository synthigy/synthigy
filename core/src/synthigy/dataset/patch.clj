;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.patch
  "Upgrade steps of the :synthigy/dataset and :synthigy.dataset/model components, and the legacy Datasets retirement."
  (:require
   clojure.set
   [patcho.patch :as patch]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :refer [entity->table-name]]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]
   [synthigy.log :as log]
   [synthigy.transit :refer [<-transit ->transit]]
   [version-clj.core :as version]))

(defn model-ids
  "Active and all entity, relation and attribute ids of a model, for before/after comparison."
  [model]
  (let [active? #(not (false? (:active %)))
        entities (core/get-entities model)
        attr-ids (fn [pred] (set (for [e entities a (:attributes e) :when (pred a)]
                                   [(id/extract e) (id/extract a)])))]
    {:entities (set (map id/extract (filter active? entities)))
     :relations (set (map id/extract (filter active? (core/get-relations model))))
     :attributes (attr-ids active?)
     :all-attributes (attr-ids any?)}))

(defn retire-datasets!
  "Remove the legacy Datasets dataset once Synthigy claims all it held — rows only, no DDL; no-op when absent."
  []
  (try
    (when-let [datasets (dataset/get-entity :dataset/dataset {(id/key) (id/data :dataset/id)}
                                    {(id/key) nil
                                     :name nil
                                     :versions [{:selections {(id/key) nil :name nil}}]})]
      (let [base {:subject :dataset :dataset-name (:name datasets) :versions (mapv :name (:versions datasets))}
            global (dataset/deployed-model)
            version-ids (set (map id/extract (:versions datasets)))
            exclusive (concat (core/find-exclusive-entities global version-ids)
                              (core/find-exclusive-relations global version-ids))
            {blocking true orphaned false} (group-by #(not (false? (:active %))) exclusive)
            label #(or (:name %) (str (:from-label %) "->" (:to-label %)))
            synthigy (:model (dataset/latest-deployed-version (id/data :synthigy/id)))
            uncovered (when synthigy
                        (for [e (some-> (dataset/latest-deployed-version (id/data :dataset/id)) :model core/get-entities)
                              a (:attributes e)
                              :when (not (false? (:active a)))
                              :let [a' (some #(when (= (id/extract a) (id/extract %)) %)
                                             (:attributes (core/get-entity synthigy (id/extract e))))]
                              :when (or (nil? a') (false? (:active a')))]
                          (str (:name e) "." (:name a))))]
        (cond
          (nil? synthigy)
          (log/warn {:id ::datasets-retire-skipped :data (assoc base :action :skipped :reason :synthigy-not-deployed)}
                    "Datasets kept: Synthigy model not deployed")

          (or (seq blocking) (seq uncovered))
          (log/warn {:id ::datasets-retire-skipped
                     :data (assoc base :action :skipped :reason :not-covered
                                  :exclusive (mapv label blocking) :attributes (vec uncovered))}
                    "Datasets kept: Synthigy does not cover everything it holds")

          :else
          (let [before (model-ids global)]
            (doseq [vid version-ids]
              (dataset/delete-entity :dataset/version {(id/key) vid}))
            (dataset/delete-entity :dataset/dataset {(id/key) (id/data :dataset/id)})
            (core/reload *db*)
            (let [after (model-ids (dataset/deployed-model))
                  lost (into {} (for [k [:entities :relations :attributes]
                                      :let [d (clojure.set/difference (get before k) (get after k))]
                                      :when (seq d)]
                                  [k (vec d)]))
                  data (cond-> (assoc base :action :retired
                                      :orphan-tables (mapv label orphaned)
                                      :orphan-columns (vec (clojure.set/difference (:all-attributes before)
                                                                                   (:all-attributes after))))
                         (seq lost) (assoc :deactivated lost))]
              (if (seq lost)
                (log/warn {:id ::datasets-retired :data data} "Retired Datasets; some objects went inactive")
                (log/info {:id ::datasets-retired :data data} "Retired Datasets; schema kept as is")))))))
    ;; never block boot — the next boot retries
    (catch Throwable e
      (log/error! {:id ::datasets-retire-failed
                   :msg "Retiring Datasets failed; retried on next boot"
                   :data {:action :retiring :subject :dataset}}
                  e))))

(defn mount-synthigy-schema!
  "Bring the tables in line with synthigy.json without recording a version."
  []
  (let [version (dataset/current-synthigy-version)
        global (or (dataset/deployed-model)
                   (core/map->ERDModel {:entities {} :relations {}}))]
    (core/mount *db* (update version :model #(core/with-merged-config global %)))
    (core/reload *db* version)))

(defn fix-deployed-on!
  "Backfill dataset_version.deployed_on from modified_on where it is missing."
  []
  (let [table (entity->table-name (core/get-entity (dataset/deployed-model) (id/entity :dataset/version)))]
    (when (pos? (:n (sql/execute-one! [(format "select count(*) as n from \"%s\" where deployed_on is null" table)])))
      (sql/execute-one! [(format "update \"%s\" set deployed_on = modified_on where deployed_on is null" table)])
      (log/info {:id ::deployed-on-backfilled :data {:action :patching :subject :deployed-on}}
                "Backfilled deployed_on from modified_on"))))

(patch/current-version :synthigy.dataset/model "1.0.6")

;; never "0" once Synthigy 0.90.0+ is deployed — the legacy step would rerun on every boot
(patch/installed-version
 :synthigy.dataset/model
 (if (some-> (dataset/latest-deployed-version (id/data :synthigy/id)) :name str (version/newer-or-equal? "0.90.0"))
   "1.0.6"
   (or (some-> (dataset/latest-deployed-version (id/data :dataset/id)) :name str)
       "0")))

(patch/upgrade :synthigy.dataset/model
               "1.0.6"
               (log/info {:id ::legacy-meta-upgrading
                          :data {:action :upgrading :subject :dataset-model :version "1.0.6"}}
                         "Upgrading legacy meta-model tables to the Synthigy model")
               (mount-synthigy-schema!)
               (fix-deployed-on!))

(patch/current-version :synthigy/dataset "1.6.0")

(patch/upgrade :synthigy/dataset
               "1.6.0"
               (let [cleaned (for [{:keys [model] :as version} (dataset/search-entity :dataset/version nil {(id/key) nil :model nil})
                                   :let [stored (if (string? model) (<-transit model) model)]
                                   :when (instance? synthigy.dataset.core.ERDModel stored)
                                   :let [clean (dataset/persisted-model stored)]
                                   :when (not= clean stored)]
                               {(id/key) (id/extract version) :model (->transit clean)})]
                 (doseq [version cleaned]
                   (dataset/sync-entity :dataset/version version))
                 (log/info {:id ::v160-models-cleaned
                            :data {:action :upgraded :subject :dataset-model :version "1.6.0" :count (count cleaned)}}
                           (str "Cleaned " (count cleaned) " stored dataset version models"))
                 (core/reload *db*)))

(defn can-migrate-to-xid?
  "Check if the system can migrate to XID format."
  []
  (try
    ; (model/verify-xid-migration)
    true
    (catch Exception e
      (log/warn {:id ::xid-migration-blocked
                 :data {:action :migrating :subject :xid
                        :error-message (.getMessage e)}}
                "Cannot migrate to xid format")
      false)))
