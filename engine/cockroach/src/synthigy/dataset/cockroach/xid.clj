;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.cockroach.xid
  "CockroachDB legacy euuid → xid import."
  (:require
    [clojure.string :as str]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.patch.model :as model]
    [synthigy.dataset.sql.naming :as naming :refer [entity->table-name relation->table-name]]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.db.sql :as sql]
    [synthigy.log :as log]))


(defn table-exists?
  [table-name]
  (let [result (sql/execute-one!
                 ["SELECT COUNT(*) as cnt FROM information_schema.tables
                   WHERE table_name = ? AND table_schema = 'public'"
                  table-name])]
    (pos? (:cnt result 0))))

(defn get-all-entity-tables
  "Entity tables named by the deployed schema; may include tables missing from the DB."
  []
  (let [schema (sql-query/deployed-schema)]
    (vec (distinct (keep :table (vals schema))))))

(defn get-existing-entity-tables
  []
  (let [all-tables (get-all-entity-tables)
        existing (filterv table-exists? all-tables)
        missing (remove (set existing) all-tables)]
    (when (seq missing)
      (log/warn {:id ::missing-tables-skipped
                 :data {:count (count missing) :tables (vec missing)}}
                "Skipping missing tables"))
    existing))

(defn get-all-relation-tables
  []
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)]
    (vec (distinct (map relation->table-name relations)))))

(defn rename-relation-tables!
  "Renames relation tables from euuid- to xid-based names; call BEFORE switching the provider."
  []
  (log/info {:id ::relation-tables-rename-starting}
            "Renaming relation tables")
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)
        renamed (atom 0)]
    (doseq [relation relations]
      (let [relation-with-xid (if (:xid relation)
                                relation
                                (assoc relation :xid (id/uuid->nanoid (:euuid relation))))
            old-name (naming/relation->table-name-for-id relation-with-xid :euuid)
            new-name (naming/relation->table-name-for-id relation-with-xid :xid)]
        (when (and new-name (not= old-name new-name))
          (when (table-exists? old-name)
            (log/info {:id ::relation-table-renamed
                       :data {:from old-name :to new-name}}
                      "Renaming relation table")
            (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" old-name new-name)])
            (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                   (str old-name "_fidx") (str new-name "_fidx"))])
            (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                   (str old-name "_tidx") (str new-name "_tidx"))])
            (swap! renamed inc)))))
    (log/info {:id ::relation-tables-rename-complete
               :data {:renamed @renamed :relations (count relations)}}
              "Relation tables renamed")
    {:renamed @renamed
     :relations (count relations)}))

(defn revert-relation-tables!
  "Reverts relation table names from xid- back to euuid-based names (migration reset)."
  []
  (log/info {:id ::relation-tables-revert-starting}
            "Reverting relation table names")
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)
        reverted (atom 0)]
    (doseq [relation relations]
      (let [xid (or (:xid relation) (id/uuid->nanoid (:euuid relation)))
            relation-with-xid (assoc relation :xid xid)]
        (when xid
          (let [xid-name (naming/relation->table-name-for-id relation-with-xid :xid)
                euuid-name (naming/relation->table-name-for-id relation-with-xid :euuid)]
            (when (not= xid-name euuid-name)
              (when (table-exists? xid-name)
                (log/info {:id ::relation-table-reverted
                           :data {:from xid-name :to euuid-name}}
                          "Reverting relation table")
                (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" xid-name euuid-name)])
                (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                       (str xid-name "_fidx") (str euuid-name "_fidx"))])
                (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                       (str xid-name "_tidx") (str euuid-name "_tidx"))])
                (swap! reverted inc)))))))
    (log/info {:id ::relation-tables-revert-complete
               :data {:reverted @reverted :relations (count relations)}}
              "Relation tables reverted")
    {:reverted @reverted
     :relations (count relations)}))

(defn column-exists?
  [table-name column-name]
  (let [result (sql/execute-one!
                 [(str "SELECT COUNT(*) as cnt FROM information_schema.columns "
                       "WHERE table_name = ? AND column_name = ? AND table_schema = 'public'")
                  table-name column-name])]
    (pos? (:cnt result 0))))

(defn add-xid-column!
  [table-name]
  (if (column-exists? table-name "xid")
    (do
      (log/debug {:id ::xid-column-exists :data {:table table-name}}
                 "xid column already exists")
      :exists)
    (do
      (log/info {:id ::xid-column-added :data {:table table-name}}
                "Adding xid column to table")
      (sql/execute! [(format "ALTER TABLE \"%s\" ADD COLUMN xid VARCHAR(64)" table-name)])
      :added)))

(defn add-xid-columns-to-all-tables!
  []
  (let [tables (get-existing-entity-tables)
        results (doall (map (fn [table]
                              (try
                                [table (add-xid-column! table)]
                                (catch Exception e
                                  (log/error! {:id ::xid-column-add-failed
                                               :data {:table table}}
                                              e)
                                  [table :error])))
                            tables))
        grouped (group-by second results)]
    {:added (count (:added grouped))
     :existed (count (:exists grouped))
     :errors (count (:error grouped))
     :tables tables}))


(def ^:private default-batch-size
  10000)

(defn generate-xids-for-table!
  "Backfills missing xids in JVM-side batches via id/uuid->nanoid."
  ([table-name]
   (generate-xids-for-table! table-name {}))
  ([table-name {:keys [batch-size]
                :or {batch-size default-batch-size}}]
   (log/info {:id ::xid-generation-starting
              :data {:table table-name :batch-size batch-size}}
             "Generating XIDs for table")
   (let [select-sql (format "SELECT _eid, euuid FROM \"%s\" WHERE xid IS NULL ORDER BY _eid LIMIT %d"
                            table-name batch-size)
         update-sql (format "UPDATE \"%s\" SET xid = ? WHERE _eid = ?" table-name)]
     (loop [total 0
            batches 0]
       (let [records (sql/execute! [select-sql])]
         (if (empty? records)
           (do
             (log/info {:id ::xid-generation-complete
                        :data {:table table-name :updated total :batches batches}}
                       "Generated XIDs for table")
             {:table table-name
              :updated total
              :batches batches})
           (let [params (mapv (fn [{:keys [_eid euuid]}]
                                [(id/uuid->nanoid euuid) _eid])
                              records)
                 batch-count (count params)]
             (sql/execute-batch! (into [update-sql] params))
             (log/debug {:id ::xid-batch-updated
                         :data {:table table-name :batch (inc batches) :records batch-count}}
                        "Batch updated")
             (recur (+ total batch-count)
                    (inc batches)))))))))

(defn get-all-tables-with-xid
  "Every public table with an xid column, not just those in the deployed schema."
  []
  (let [results (sql/execute!
                  ["SELECT table_name FROM information_schema.columns
                    WHERE column_name = 'xid' AND table_schema = 'public'"])]
    (mapv :table_name results)))

(defn add-xid-unique-constraint!
  [table-name]
  (let [constraint-name (str table-name "_xid_unique")]
    (try
      (sql/execute! [(format "ALTER TABLE \"%s\" ADD CONSTRAINT %s UNIQUE (xid)"
                             table-name constraint-name)])
      (log/info {:id ::xid-unique-constraint-added :data {:table table-name}}
                "Added UNIQUE constraint on xid")
      :added
      (catch Exception e
        (if (str/includes? (.getMessage e) "already exists")
          (do
            (log/debug {:id ::xid-unique-constraint-exists :data {:table table-name}}
                       "UNIQUE constraint already exists on xid")
            :exists)
          (throw e))))))

(defn add-xid-not-null-constraint!
  [table-name]
  (try
    (sql/execute! [(format "ALTER TABLE \"%s\" ALTER COLUMN xid SET NOT NULL" table-name)])
    (log/info {:id ::xid-not-null-constraint-added :data {:table table-name}}
              "Added NOT NULL constraint on xid")
    :added
    (catch Exception e
      (if (str/includes? (.getMessage e) "already set")
        (do
          (log/debug {:id ::xid-not-null-constraint-exists :data {:table table-name}}
                     "NOT NULL constraint already set on xid")
          :exists)
        (throw e)))))

(defn add-xid-index!
  [table-name]
  (let [index-name (str table-name "_xid_idx")]
    (try
      (sql/execute! [(format "CREATE INDEX IF NOT EXISTS %s ON \"%s\" (xid)"
                             index-name table-name)])
      (log/info {:id ::xid-index-added :data {:table table-name :index index-name}}
                "Added xid index")
      :added
      (catch Exception e
        (if (str/includes? (.getMessage e) "already exists")
          :exists
          (throw e))))))

(defn add-constraints!
  "Adds the xid UNIQUE constraint and index to every table with xid; run after the backfill."
  []
  (log/info {:id ::constraints-starting} "Adding XID constraints")
  (let [tables (get-all-tables-with-xid)
        results (doall
                  (map (fn [table]
                         (try
                           {:table table
                            :unique (add-xid-unique-constraint! table)
                            :index (add-xid-index! table)}
                           (catch Exception e
                             (log/error! {:id ::constraint-add-failed
                                          :data {:table table}}
                                         e)
                             {:table table
                              :error (.getMessage e)})))
                       tables))]
    {:tables results
     :count (count tables)}))

(defn xid-trigger-function
  "SQL for `prevent_xid_update()`, which REJECTS an xid change (PG silently keeps the old xid)."
  []
  "CREATE OR REPLACE FUNCTION prevent_xid_update()
RETURNS TRIGGER AS $$
BEGIN
  IF (OLD).xid IS NOT NULL AND (NEW).xid IS DISTINCT FROM (OLD).xid THEN
    RAISE EXCEPTION 'xid is immutable';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;")

(defn xid-trigger-name
  [table-name]
  (format "prevent_xid_update_trigger_%s"
          (str/replace table-name #"[^a-zA-Z0-9_]" "_")))

(defn xid-trigger-drop
  [table-name]
  (format "DROP TRIGGER IF EXISTS %s ON \"%s\";"
          (xid-trigger-name table-name)
          table-name))

(defn xid-trigger
  "Plain CREATE TRIGGER (no OR REPLACE on CRDB); run `xid-trigger-drop` first."
  [table-name]
  (format
    "CREATE TRIGGER %s
  BEFORE UPDATE ON \"%s\"
  FOR EACH ROW
  EXECUTE FUNCTION prevent_xid_update();"
    (xid-trigger-name table-name)
    table-name))

(defn create-xid-immutability-triggers!
  []
  (let [tables (get-all-tables-with-xid)]
    (log/info {:id ::immutability-triggers-starting
               :data {:tables (count tables)}}
              "Creating XID immutability triggers")
    ;; never CREATE OR REPLACE here — CRDB refuses while triggers depend on the fn
    (let [exists? (boolean
                   (seq (sql/execute!
                         ["select 1 from pg_proc where proname = 'prevent_xid_update'"])))]
      (if exists?
        (log/debug {:id ::trigger-function-already-exists}
                   "prevent_xid_update() already present; skipping CREATE")
        (do (sql/execute! [(xid-trigger-function)])
            (log/debug {:id ::trigger-function-created}
                       "Created PostgreSQL/CRDB trigger function for xid"))))
    (doseq [table tables]
      (try
        (sql/execute! [(xid-trigger-drop table)])
        (sql/execute! [(xid-trigger table)])
        (log/debug {:id ::xid-trigger-created :data {:table table}}
                   "Created xid trigger on table")
        (catch Exception e
          (log/warn {:id ::xid-trigger-failed
                     :data {:table table :error (.getMessage e)}
                     :error e}
                    "Could not create xid trigger"))))

    {:created (count tables)
     :tables tables}))

(defn migrate-meta-table!
  [table-name]
  (log/info {:id ::meta-table-migrating :data {:action :migrating :subject :meta-table :table table-name}}
            "Migrating meta-table")
  (let [column-added (if (column-exists? table-name "xid")
                       false
                       (do
                         (sql/execute! [(format "ALTER TABLE %s ADD COLUMN xid VARCHAR(64)" table-name)])
                         true))]
    (let [rows (sql/execute! [(format "SELECT _eid, euuid FROM %s WHERE xid IS NULL" table-name)])
          updated (atom 0)]
      (doseq [{:keys [_eid euuid]} rows]
        (let [xid (id/uuid->nanoid euuid)]
          (sql/execute! [(format "UPDATE %s SET xid = ? WHERE _eid = ?" table-name)
                         xid _eid])
          (swap! updated inc)))

      (log/info {:id ::meta-table-migrated
                 :data {:action :migrated :subject :meta-table
                        :table table-name
                        :column (if column-added "added" "existed")
                        :updated @updated}}
                "Meta-table migrated")

      {:table table-name
       :added-column column-added
       :updated @updated})))

(defn migrate-meta-tables!
  "Adds and backfills xid on dataset_entity, dataset_relation and dataset_entity_attribute."
  []
  (log/info {:id ::meta-tables-migration-starting :data {:action :migrating :subject :meta-tables}}
            "Migrating meta-tables")
  (let [results {:entity (migrate-meta-table! "dataset_entity")
                 :relation (migrate-meta-table! "dataset_relation")
                 :attribute (migrate-meta-table! "dataset_entity_attribute")}
        total-updated (+ (get-in results [:entity :updated] 0)
                         (get-in results [:relation :updated] 0)
                         (get-in results [:attribute :updated] 0))]
    (log/info {:id ::meta-tables-migration-complete
               :data {:action :migrated :subject :meta-tables :total-updated total-updated}}
              "Meta-tables migrated")
    results))

(defn migrate-all-tables!
  "Adds xid columns to data tables, backfills them, then adds the UNIQUE constraint and index."
  []
  (log/info {:id ::data-tables-migration-starting :data {:action :migrating :subject :data-tables}}
            "Starting XID migration on data tables")
  (log/info {:id ::data-tables-step-1} "Step 1: Adding xid columns")
  (let [column-result (add-xid-columns-to-all-tables!)]
    (log/info {:id ::data-tables-columns-added
               :data {:added (:added column-result)
                      :existed (:existed column-result)}}
              "Added xid columns")
    (log/info {:id ::data-tables-step-2} "Step 2: Generating XIDs for all records")
    (let [tables (get-all-tables-with-xid)
          gen-results (doall (map generate-xids-for-table! tables))
          total-updated (reduce + (map :updated gen-results))]
      (log/info {:id ::data-tables-xids-generated
                 :data {:total-updated total-updated :tables (count tables)}}
                "Generated XIDs across tables")
      (log/info {:id ::data-tables-step-3} "Step 3: Adding UNIQUE constraints")
      (let [constraint-results (doall
                                 (map (fn [table]
                                        (try
                                          {:table table
                                           :unique (add-xid-unique-constraint! table)
                                           :index (add-xid-index! table)}
                                          (catch Exception e
                                            (log/error! {:id ::constraint-add-failed
                                                         :data {:table table}}
                                                        e)
                                            {:table table :error (.getMessage e)})))
                                      tables))]
        (log/info {:id ::data-tables-constraints-added
                   :data {:tables (count tables)}}
                  "Added constraints to tables")

        {:columns column-result
         :generated {:total total-updated
                     :tables gen-results}
         :constraints constraint-results}))))

(defn migrate-to-xid!
  "Full deterministic euuid → xid migration, ending with the provider switch."
  []
  (log/info {:id ::migration-starting :data {:action :migrating :subject :xid}}
            "Starting complete XID migration (deterministic)")
  (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 1 :of 6 :name "migrate-meta-tables"}}
            "Migrating meta-tables")
  (let [meta-result (migrate-meta-tables!)]
    (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 2 :of 6 :name "transform-stored-models"}}
              "Transforming stored models")
    (let [model-result (model/transform-stored-models! :xid)]
      (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 3 :of 6 :name "migrate-data-tables"}}
                "Migrating data tables")
      (let [data-result (migrate-all-tables!)]
        (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 4 :of 6 :name "rename-relation-tables"}}
                  "Renaming relation tables")
        (let [rename-result (rename-relation-tables!)]
          (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 5 :of 6 :name "add-immutability-triggers"}}
                    "Adding immutability triggers")
          (create-xid-immutability-triggers!)
          (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 6 :of 6 :name "switch-provider"}}
                    "Switching to NanoID provider")
          (dataset/set-format! "xid")
          (id/set-provider! (id/->NanoIDProvider))
          (dataset/save-model! nil)
          (dataset/reload)

          (log/info {:id ::migration-complete :data {:action :migrated :subject :xid}}
                    "XID migration complete")

          {:meta-tables meta-result
           :models model-result
           :data-tables data-result
           :relation-tables rename-result})))))

(defn verify-xid-migration
  "Throws unless every row of every existing entity table has an xid."
  []
  (log/info {:id ::verification-starting :data {:action :verifying :subject :xid-migration}} "Verifying XID migration")
  (let [tables (get-existing-entity-tables)
        results (doall
                  (map (fn [table]
                         (let [missing (sql/execute-one!
                                         [(format "SELECT COUNT(*) as cnt FROM \"%s\" WHERE xid IS NULL"
                                                  table)])]
                           {:table table
                            :missing (:cnt missing 0)}))
                       tables))
        missing-tables (filter #(pos? (:missing %)) results)]
    (if (empty? missing-tables)
      (do
        (log/info {:id ::verification-complete :data {:tables (count tables)}}
                  "XID migration verified: all records have XIDs")
        {:verified true
         :tables (count tables)})
      (do
        (log/error {:id ::verification-incomplete
                    :data {:tables-with-missing (count missing-tables)}}
                   "XID migration incomplete")
        (doseq [{:keys [table missing]} missing-tables]
          (log/error {:id ::verification-table-missing
                      :data {:table table :missing missing}}
                     "Records missing XIDs in table"))
        (throw (ex-info "Missing XIDs"
                        {:missing missing-tables
                         :verified false}))))))

(defn drop-xid-column!
  [table-name]
  (if (column-exists? table-name "xid")
    (do
      (log/info {:id ::xid-column-dropped :data {:table table-name}}
                "Dropping xid column")
      (sql/execute! [(format "ALTER TABLE \"%s\" DROP COLUMN xid" table-name)])
      :dropped)
    :not-exists))

(defn reset-xid-migration!
  "DESTRUCTIVE: fully reverts the xid import (models, relation names, xid columns, stamp, provider)."
  []
  (log/warn {:id ::reset-starting}
            "RESETTING XID MIGRATION")
  (log/info {:id ::reset-step :data {:step 1 :of 5 :name "revert-stored-models"}}
            "Reverting stored models to euuid format")
  (let [model-result (model/transform-stored-models! :euuid)]
    ;; BEFORE dropping xid columns
    (log/info {:id ::reset-step :data {:step 2 :of 5 :name "revert-relation-tables"}}
              "Reverting relation table names")
    (let [revert-result (revert-relation-tables!)]
      (log/info {:id ::reset-step :data {:step 3 :of 5 :name "drop-xid-columns"}}
                "Dropping xid columns")
      (let [tables-with-xid (get-all-tables-with-xid)
            _ (log/info {:id ::reset-tables-found
                         :data {:count (count tables-with-xid)}}
                        "Found tables with xid column")
            drop-results (doall
                           (map (fn [table]
                                  {:table table
                                   :result (drop-xid-column! table)})
                                tables-with-xid))
            dropped-count (count (filter #(= :dropped (:result %)) drop-results))]
        ;; never skip — a stale "xid" stamp boots the reverted DB as migrated
        (log/info {:id ::reset-step :data {:step 4 :of 5 :name "clear-format-stamp"}}
                  "Clearing stored format stamp")
        (dataset/clear-format!)
        (log/info {:id ::reset-step :data {:step 5 :of 5 :name "restore-euuid-provider"}}
                  "Restoring euuid provider")
        (id/set-provider! (id/->UUIDProvider))
        (dataset/save-model! nil)
        (dataset/reload)

        (log/info {:id ::reset-complete
                   :data {:models-reverted (:total model-result 0)
                          :relation-tables-renamed (:reverted revert-result 0)
                          :xid-columns-dropped dropped-count}}
                  "Reset complete")

        {:models model-result
         :relation-tables revert-result
         :tables {:dropped dropped-count
                  :total (count tables-with-xid)
                  :details drop-results}}))))
