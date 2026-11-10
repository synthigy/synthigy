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

(ns synthigy.dataset.patch
  "Upgrade steps of the :synthigy/dataset and :synthigy.dataset/model components, and the legacy Datasets retirement."
  (:require
   clojure.set
   [patcho.patch :as patch]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :refer [entity->table-name]]
   [synthigy.dataset.system :as system]
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

(def removed-attribute-types #{"currency" "timeperiod" "transit"})

(defn retire-attribute
  [attribute]
  (cond-> (assoc attribute :type "json")
    (#{"currency" "transit"} (some-> (:type attribute) name)) (assoc :active false)))

(defn retire-removed-attribute-types!
  "Move attribute rows off currency/timeperiod/transit before a deploy drops those types: system rows to their shipped type, others to json (currency and transit inactive); currency also in every stored version model."
  []
  (let [model (dataset/deployed-model)
        table (entity->table-name (core/get-entity model (id/entity :dataset/entity-attribute)))
        version-table (entity->table-name (core/get-entity model (id/entity :dataset/version)))
        id-column (name (id/key))
        shipped (into {} (for [entity (core/get-entities (:model (dataset/current-synthigy-version)))
                               attribute (:attributes entity)]
                           [(str (id/extract attribute)) (some-> (:type attribute) name)]))
        rows (sql/execute! (into [(format "select \"%s\" as id, name, type from \"%s\" where type in (?, ?, ?)" id-column table)]
                                 removed-attribute-types))
        target (fn [{:keys [id type] :as row}]
                 (if-let [t (shipped (str id))]
                   {:type t :active true}
                   (select-keys (retire-attribute row) [:type :active])))
        versions (sql/execute! [(format "select _eid, model from \"%s\" where model like '%%\"~:type\":\"currency\"%%'" version-table)])]
    (doseq [{:keys [id] :as row} rows
            :let [{:keys [type active]} (target row)]]
      (sql/execute-one! [(format "update \"%s\" set type = ?, active = coalesce(?, active) where \"%s\" = ?" table id-column)
                         type active id]))
    (doseq [{eid :_eid stored :model} versions]
      (let [retire #(if (= "currency" (some-> (:type %) name)) (retire-attribute %) %)
            model' (update (<-transit stored) :entities update-vals
                           (fn [entity] (update entity :attributes #(mapv retire %))))]
        (sql/execute-one! [(format "update \"%s\" set model = ? where _eid = ?" version-table) (->transit model') eid])))
    (when (or (seq rows) (seq versions))
      (log/info {:id ::removed-attribute-types-retired
                 :data {:action :migrated :subject :dataset-model
                        :versions (count versions)
                        :attributes (mapv #(merge (select-keys % [:id :name :type]) (target %)) rows)}}
                "Moved attributes off currency/timeperiod/transit"))
    (count rows)))

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
               (retire-removed-attribute-types!)
               (mount-synthigy-schema!)
               (fix-deployed-on!))

(patch/current-version :synthigy/dataset "1.9.0")

(defn shipped-system-versions
  "The system model versions core ships."
  []
  [(dataset/current-synthigy-version) (dataset/<-resource "dataset/oauth_session.json")])

(defn mark-model
  "`model` carrying the shipped system marks of `entity-marks` and `relation-marks`, a system model when `system?`."
  [model system? entity-marks relation-marks]
  (cond-> (-> model
              (assoc :version 2)
              (update :entities #(reduce-kv (fn [m eid e]
                                              (cond-> m
                                                (contains? entity-marks eid)
                                                (assoc eid (assoc-in e [:configuration :system] (get entity-marks eid)))))
                                            % %))
              (update :relations #(reduce-kv (fn [m rid r]
                                               (cond-> m
                                                 (contains? relation-marks rid)
                                                 (assoc rid (assoc-in r [:configuration :system] :closed))))
                                             % %)))
    system? (assoc-in [:configuration :system] true)))

(defn mark-system-models!
  "Model format 2: stored versions of the shipped system datasets carry the shipped system marks; every stored model is stamped :version 2."
  []
  (let [shipped (shipped-system-versions)
        datasets (into #{} (map (comp id/extract :dataset)) shipped)
        entity-marks (system/entity-marks (map :model shipped))
        relation-marks (into #{} (mapcat (comp system/relation-marks :model)) shipped)
        marked (for [{:keys [model dataset] :as version}
                     (dataset/search-entity :dataset/version nil {(id/key) nil :model nil
                                                                  :dataset [{:selections {(id/key) nil}}]})
                     :let [stored (if (string? model) (<-transit model) model)]
                     :when (instance? synthigy.dataset.core.ERDModel stored)
                     :let [system? (contains? datasets (id/extract dataset))
                           upgraded (if system?
                                  (mark-model stored true entity-marks relation-marks)
                                  (assoc stored :version 2))]
                     :when (not= upgraded stored)]
                 {(id/key) (id/extract version) :model (->transit upgraded)})]
    (doseq [version marked]
      (dataset/sync-entity :dataset/version version))
    (log/info {:id ::v180-models-marked
               :data {:action :upgraded :subject :dataset-model :version "1.8.0" :count (count marked)}}
              (str "Model format 2: marked " (count marked) " stored dataset version models"))
    (core/reload *db*)))

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

(patch/upgrade :synthigy/dataset
               "1.8.0"
               (mark-system-models!))

(defn with-audit-actions
  "`model` whose copies of the entities in `actions` (entity id -> audit actions) carry those actions too."
  [model actions]
  (reduce-kv (fn [m eid as]
               (if-let [e (core/get-entity m eid)]
                 (core/set-entity m (update-in e [:configuration :audit :actions] #(into (set %) as)))
                 m))
             model
             actions))

(defn upgrade-stored-models!
  "Model format 3: every stored dataset version model has the merged audit flags as stamp attributes, on every copy of an entity."
  []
  (let [actions (into {} (keep (fn [e] (when-let [as (not-empty (get-in e [:configuration :audit :actions]))]
                                         [(id/extract e) as])))
                      (some-> (dataset/deployed-model) core/get-entities))
        upgraded (for [{:keys [model] :as version} (dataset/search-entity :dataset/version nil {(id/key) nil :model nil})
                       :let [stored (if (string? model) (<-transit model) model)]
                       :when (instance? synthigy.dataset.core.ERDModel stored)
                       :let [upgraded (core/upgrade-model (with-audit-actions stored actions))]
                       :when (not= upgraded stored)]
                   {(id/key) (id/extract version) :model (->transit upgraded)})]
    (doseq [version upgraded]
      (dataset/sync-entity :dataset/version version))
    (log/info {:id ::v190-models-upgraded
               :data {:action :upgraded :subject :dataset-model :version "1.9.0" :count (count upgraded)}}
              (str "Model format 3: upgraded " (count upgraded) " stored dataset version models"))
    (core/reload *db*)))

(patch/upgrade :synthigy/dataset
               "1.9.0"
               (upgrade-stored-models!))

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
