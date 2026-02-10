(ns synthigy.dataset.postgres.xid
  "PostgreSQL XID migration utilities.

  All conversions are deterministic via id/uuid->nanoid (bidirectional).
  No mapping files or lookup tables needed.

  Main entry points:
  - migrate-to-xid!       - Run full migration
  - reset-xid-migration!  - Revert migration (destructive, for testing)
  - verify-xid-migration  - Check all records have XIDs

  Workflow:
    (xid/migrate-to-xid!)  ; That's it — fully deterministic"
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.patch.model :as model]
    [synthigy.dataset.sql.naming :as naming :refer [entity->table-name relation->table-name]]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.db.sql :as sql]))


;;; ============================================================================
;;; Table Discovery
;;; ============================================================================

(defn table-exists?
  "Check if a table exists in the database.

  Args:
    table-name - Name of the table

  Returns:
    Boolean indicating if table exists"
  [table-name]
  (let [result (sql/execute-one!
                 ["SELECT COUNT(*) as cnt FROM information_schema.tables
                   WHERE table_name = ? AND table_schema = 'public'"
                  table-name])]
    (pos? (:cnt result 0))))

(defn get-all-entity-tables
  "Gets all entity table names from the deployed schema.

  NOTE: Returns tables from schema, may include tables that don't exist in DB.
  Use get-existing-entity-tables for safety.

  Returns:
    Vector of table name strings"
  []
  (let [schema (sql-query/deployed-schema)]
    (vec (distinct (keep :table (vals schema))))))

(defn get-existing-entity-tables
  "Gets entity tables that actually exist in the database.

  Filters out tables from schema that don't exist in DB (corrupted/partial state).

  Returns:
    Vector of existing table name strings"
  []
  (let [all-tables (get-all-entity-tables)
        existing (filterv table-exists? all-tables)
        missing (remove (set existing) all-tables)]
    (when (seq missing)
      (log/warnf "Skipping %d missing tables: %s" (count missing) (vec missing)))
    existing))

(defn get-all-relation-tables
  "Gets all relation (junction) table names from the deployed schema.

  Returns:
    Vector of table name strings"
  []
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)]
    (vec (distinct (map relation->table-name relations)))))

(defn rename-relation-tables!
  "Rename relation tables from EUUID-based to XID-based names.
   Must be called BEFORE switching ID provider."
  []
  (log/info "[XID] === Renaming relation tables ===")
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)
        renamed (atom 0)]
    (doseq [relation relations]
      (let [;; Resolve xid deterministically if not present in relation
            relation-with-xid (if (:xid relation)
                                relation
                                (assoc relation :xid (id/uuid->nanoid (:euuid relation))))
            old-name (naming/relation->table-name-for-id relation-with-xid :euuid)
            new-name (naming/relation->table-name-for-id relation-with-xid :xid)]
        (when (and new-name (not= old-name new-name))
          (when (table-exists? old-name)
            (log/infof "[XID] Renaming relation table: %s -> %s" old-name new-name)
            ;; Rename table
            (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" old-name new-name)])
            ;; Rename indexes
            (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                   (str old-name "_fidx") (str new-name "_fidx"))])
            (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                   (str old-name "_tidx") (str new-name "_tidx"))])
            (swap! renamed inc)))))
    (log/infof "[XID] Renamed %d relation tables" @renamed)
    {:renamed @renamed
     :relations (count relations)}))

(defn revert-relation-tables!
  "Revert relation table names from XID-based back to EUUID-based names.
   Used during migration reset."
  []
  (log/info "[XID] === Reverting relation table names ===")
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)
        reverted (atom 0)]
    (doseq [relation relations]
      (let [;; Resolve xid deterministically if not in model
            xid (or (:xid relation) (id/uuid->nanoid (:euuid relation)))
            relation-with-xid (assoc relation :xid xid)]
        (when xid
          (let [xid-name (naming/relation->table-name-for-id relation-with-xid :xid)
                euuid-name (naming/relation->table-name-for-id relation-with-xid :euuid)]
            (when (not= xid-name euuid-name)
              (when (table-exists? xid-name)
                (log/infof "[XID] Reverting relation table: %s -> %s" xid-name euuid-name)
                ;; Rename table
                (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" xid-name euuid-name)])
                ;; Rename indexes
                (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                       (str xid-name "_fidx") (str euuid-name "_fidx"))])
                (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                       (str xid-name "_tidx") (str euuid-name "_tidx"))])
                (swap! reverted inc)))))))
    (log/infof "[XID] Reverted %d relation tables" @reverted)
    {:reverted @reverted
     :relations (count relations)}))

;;; ============================================================================
;;; Column Management
;;; ============================================================================

(defn column-exists?
  "Check if a column exists in a table.

  Args:
    table-name - Name of the table
    column-name - Name of the column

  Returns:
    Boolean indicating if column exists"
  [table-name column-name]
  (let [result (sql/execute-one!
                 [(str "SELECT COUNT(*) as cnt FROM information_schema.columns "
                       "WHERE table_name = ? AND column_name = ? AND table_schema = 'public'")
                  table-name column-name])]
    (pos? (:cnt result 0))))

(defn add-xid-column!
  "Add xid column to a table if it doesn't exist.

  Args:
    table-name - Name of the table

  Returns:
    :added if column was added, :exists if already exists"
  [table-name]
  (if (column-exists? table-name "xid")
    (do
      (log/debugf "xid column already exists in %s" table-name)
      :exists)
    (do
      (log/infof "Adding xid column to %s" table-name)
      (sql/execute! [(format "ALTER TABLE \"%s\" ADD COLUMN xid VARCHAR(64)" table-name)])
      :added)))

(defn add-xid-columns-to-all-tables!
  "Add xid column to all entity and relation tables.

  Returns:
    Map with :added (count) and :existed (count)"
  []
  (let [tables (get-existing-entity-tables)
        results (doall (map (fn [table]
                              (try
                                [table (add-xid-column! table)]
                                (catch Exception e
                                  (log/errorf e "Failed to add xid column to %s" table)
                                  [table :error])))
                            tables))
        grouped (group-by second results)]
    {:added (count (:added grouped))
     :existed (count (:exists grouped))
     :errors (count (:error grouped))
     :tables tables}))


(def ^:private default-batch-size
  "Default batch size for XID generation. Balances memory usage vs round-trips."
  10000)

(defn generate-xids-for-table!
  "Generate XID for all records in a table that don't have one.

  Uses deterministic uuid->nanoid conversion — every euuid maps to
  exactly one xid, no mapping needed.

  Batched processing for large tables:
  - Fetches records in batches (default 10,000)
  - Uses batch UPDATE for efficiency
  - Memory-bounded: only batch-size rows in memory at once

  Args:
    table-name - Name of the table
    opts - Optional map with:
           :batch-size - Number of records per batch (default 10,000)

  Returns:
    Map with :table, :updated (total count), :batches (number of batches)"
  ([table-name]
   (generate-xids-for-table! table-name {}))
  ([table-name {:keys [batch-size]
                :or {batch-size default-batch-size}}]
   (log/infof "Generating XIDs for table: %s (batch-size: %d)" table-name batch-size)
   (let [select-sql (format "SELECT _eid, euuid FROM \"%s\" WHERE xid IS NULL ORDER BY _eid LIMIT %d"
                            table-name batch-size)
         update-sql (format "UPDATE \"%s\" SET xid = ? WHERE _eid = ?" table-name)]
     (loop [total 0
            batches 0]
       (let [records (sql/execute! [select-sql])]
         (if (empty? records)
           (do
             (log/infof "Generated %d XIDs for %s in %d batches" total table-name batches)
             {:table table-name
              :updated total
              :batches batches})
           (let [;; Build batch params: [[xid1 eid1] [xid2 eid2] ...]
                 params (mapv (fn [{:keys [_eid euuid]}]
                                [(id/uuid->nanoid euuid) _eid])
                              records)
                 batch-count (count params)]
             ;; Batch update
             (sql/execute-batch! (into [update-sql] params))
             (log/debugf "Batch %d: updated %d records" (inc batches) batch-count)
             (recur (+ total batch-count)
                    (inc batches)))))))))

;;; ============================================================================
;;; Constraints and Indexes
;;; ============================================================================

(defn get-all-tables-with-xid
  "Get all tables in the database that have an xid column.

  This finds ALL tables with xid, not just those in deployed-schema.
  Important for ensuring test tables and other tables get constraints."
  []
  (let [results (sql/execute!
                  ["SELECT table_name FROM information_schema.columns
                    WHERE column_name = 'xid' AND table_schema = 'public'"])]
    (mapv :table_name results)))

(defn add-xid-unique-constraint!
  "Add UNIQUE constraint to xid column.

  Args:
    table-name - Name of the table

  Returns:
    :added or :exists"
  [table-name]
  (let [constraint-name (str table-name "_xid_unique")]
    (try
      (sql/execute! [(format "ALTER TABLE \"%s\" ADD CONSTRAINT %s UNIQUE (xid)"
                             table-name constraint-name)])
      (log/infof "Added UNIQUE constraint on %s.xid" table-name)
      :added
      (catch Exception e
        (if (str/includes? (.getMessage e) "already exists")
          (do
            (log/debugf "UNIQUE constraint already exists on %s.xid" table-name)
            :exists)
          (throw e))))))

(defn add-xid-not-null-constraint!
  "Add NOT NULL constraint to xid column.

  Args:
    table-name - Name of the table

  Returns:
    :added or :exists"
  [table-name]
  (try
    (sql/execute! [(format "ALTER TABLE \"%s\" ALTER COLUMN xid SET NOT NULL" table-name)])
    (log/infof "Added NOT NULL constraint on %s.xid" table-name)
    :added
    (catch Exception e
      (if (str/includes? (.getMessage e) "already set")
        (do
          (log/debugf "NOT NULL constraint already set on %s.xid" table-name)
          :exists)
        (throw e)))))

(defn add-xid-index!
  "Add index on xid column for faster lookups.

  Args:
    table-name - Name of the table

  Returns:
    :added or :exists"
  [table-name]
  (let [index-name (str table-name "_xid_idx")]
    (try
      (sql/execute! [(format "CREATE INDEX IF NOT EXISTS %s ON \"%s\" (xid)"
                             index-name table-name)])
      (log/infof "Added index %s" index-name)
      :added
      (catch Exception e
        (if (str/includes? (.getMessage e) "already exists")
          :exists
          (throw e))))))

(defn add-constraints!
  "Add NOT NULL and UNIQUE constraints after XIDs are populated.

  Should be called after all XIDs are generated.

  NOTE: Uses get-all-tables-with-xid to find ALL tables with xid column,
  not just those in deployed-schema. This ensures test tables and other
  tables created outside the main schema also get constraints.

  Returns:
    Map with constraint statistics"
  []
  (log/info "Adding XID constraints...")
  (let [tables (get-all-tables-with-xid)
        results (doall
                  (map (fn [table]
                         (try
                           {:table table
                            :unique (add-xid-unique-constraint! table)
                            :index (add-xid-index! table)}
                           (catch Exception e
                             (log/errorf e "Failed to add constraints to %s" table)
                             {:table table
                              :error (.getMessage e)})))
                       tables))]
    {:tables results
     :count (count tables)}))

;;; ============================================================================
;;; XID Immutability Triggers
;;; ============================================================================

(defn postgres-xid-trigger-function
  "Returns SQL to create the PostgreSQL trigger function for XID immutability."
  []
  "CREATE OR REPLACE FUNCTION prevent_xid_update()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.xid IS NOT NULL AND NEW.xid IS DISTINCT FROM OLD.xid THEN
    -- Keep the old value instead of raising an exception
    NEW.xid := OLD.xid;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;")

(defn postgres-xid-trigger
  "Returns SQL to create a PostgreSQL trigger for XID immutability on a table.

  Args:
    table-name - Name of the table to protect

  Returns:
    SQL string to create the trigger"
  [table-name]
  (format
    "CREATE OR REPLACE TRIGGER prevent_xid_update_trigger_%s
  BEFORE UPDATE ON \"%s\"
  FOR EACH ROW
  EXECUTE FUNCTION prevent_xid_update();"
    (str/replace table-name #"[^a-zA-Z0-9_]" "_")
    table-name))

(defn create-xid-immutability-triggers!
  "Creates XID immutability triggers on all tables with xid column.

  Returns:
    Map with :created (count) and :tables (list)"
  []
  (let [tables (get-all-tables-with-xid)]
    (log/infof "Creating XID immutability triggers for %d tables" (count tables))

    ;; Create the trigger function once
    (sql/execute! [(postgres-xid-trigger-function)])
    (log/debug "Created PostgreSQL trigger function for xid")

    ;; Create trigger on each table
    (doseq [table tables]
      (try
        (sql/execute! [(postgres-xid-trigger table)])
        (log/debugf "Created xid trigger on table: %s" table)
        (catch Exception e
          (log/warnf "Could not create xid trigger for %s: %s" table (.getMessage e)))))

    {:created (count tables)
     :tables tables}))

;;; ============================================================================
;;; Meta-Table Migration (Schema Tables)
;;; ============================================================================
;;
;; Meta-tables store the schema definitions:
;; - dataset_entity: Entity definitions
;; - dataset_relation: Relation definitions
;; - dataset_entity_attribute: Attribute definitions
;;
;; These must be migrated BEFORE transforming stored models to ensure
;; XIDs in model blobs match XIDs in meta-table rows.

(defn migrate-meta-table!
  "Add xid column to a meta-table and populate XIDs via uuid->nanoid.

  Args:
    table-name - Name of the meta-table

  Returns:
    Map with :table, :added-column, :updated"
  [table-name]
  (log/infof "[XID] Migrating meta-table: %s" table-name)

  ;; Add xid column if not exists
  (let [column-added (if (column-exists? table-name "xid")
                       false
                       (do
                         (sql/execute! [(format "ALTER TABLE %s ADD COLUMN xid VARCHAR(64)" table-name)])
                         true))]

    ;; Populate XIDs for rows that don't have one
    (let [rows (sql/execute! [(format "SELECT _eid, euuid FROM %s WHERE xid IS NULL" table-name)])
          updated (atom 0)]
      (doseq [{:keys [_eid euuid]} rows]
        (let [xid (id/uuid->nanoid euuid)]
          (sql/execute! [(format "UPDATE %s SET xid = ? WHERE _eid = ?" table-name)
                         xid _eid])
          (swap! updated inc)))

      (log/infof "[XID] Meta-table %s: column=%s, updated=%d rows"
                 table-name
                 (if column-added "added" "existed")
                 @updated)

      {:table table-name
       :added-column column-added
       :updated @updated})))

(defn migrate-meta-tables!
  "Migrate all schema meta-tables (dataset_entity, dataset_relation, dataset_entity_attribute).

  Uses deterministic uuid->nanoid — no mapping needed.

  Returns:
    Map with results for each meta-table"
  []
  (log/info "[XID] === Migrating meta-tables ===")
  (let [results {:entity (migrate-meta-table! "dataset_entity")
                 :relation (migrate-meta-table! "dataset_relation")
                 :attribute (migrate-meta-table! "dataset_entity_attribute")}
        total-updated (+ (get-in results [:entity :updated] 0)
                         (get-in results [:relation :updated] 0)
                         (get-in results [:attribute :updated] 0))]
    (log/infof "[XID] Meta-tables migrated: %d total XIDs populated" total-updated)
    results))

;;; ============================================================================
;;; Full Migration
;;; ============================================================================

(defn migrate-all-tables!
  "Add xid columns to all entity tables, populate XIDs, and add constraints.

  Uses deterministic uuid->nanoid for all records.

  Steps:
  1. Add xid columns (if not exist) to deployed-schema tables
  2. Generate XIDs for all records
  3. Add UNIQUE constraint and index to each table

  Returns:
    Map with migration statistics"
  []
  (log/info "Starting XID migration on data tables...")

  ;; Step 1: Add xid columns to deployed-schema tables
  (log/info "Step 1: Adding xid columns...")
  (let [column-result (add-xid-columns-to-all-tables!)]
    (log/infof "Added xid columns: %d new, %d existed"
               (:added column-result) (:existed column-result))

    ;; Step 2: Generate XIDs for all tables with xid column
    ;; (includes tables created via deploy! outside deployed-schema)
    (log/info "Step 2: Generating XIDs for all records...")
    (let [tables (get-all-tables-with-xid)
          gen-results (doall (map generate-xids-for-table! tables))
          total-updated (reduce + (map :updated gen-results))]
      (log/infof "Generated %d XIDs across %d tables" total-updated (count tables))

      ;; Step 3: Add constraints to each table immediately after populating
      (log/info "Step 3: Adding UNIQUE constraints...")
      (let [constraint-results (doall
                                 (map (fn [table]
                                        (try
                                          {:table table
                                           :unique (add-xid-unique-constraint! table)
                                           :index (add-xid-index! table)}
                                          (catch Exception e
                                            (log/errorf e "Failed to add constraints to %s" table)
                                            {:table table :error (.getMessage e)})))
                                      tables))]
        (log/infof "Added constraints to %d tables" (count tables))

        {:columns column-result
         :generated {:total total-updated
                     :tables gen-results}
         :constraints constraint-results}))))

;;; ============================================================================
;;; Complete XID Migration Orchestration
;;; ============================================================================

(defn migrate-to-xid!
  "Complete XID migration. Fully deterministic — no mapping needed.

  Every euuid maps to exactly one xid via uuid->nanoid, and back via nanoid->uuid.

  Steps:
  1. Migrate meta-tables (add xid column, populate via uuid->nanoid)
  2. Transform stored models (deterministic conversion in model blobs)
  3. Migrate data tables (add xid columns, populate via uuid->nanoid)
  4. Rename relation tables (EUUID-based → XID-based names)
  5. Add constraints & triggers
  6. Switch provider, save & reload

  Returns:
    Map with results from each step"
  []
  (log/info "[XID] ====================================================")
  (log/info "[XID] Starting complete XID migration (deterministic)")
  (log/info "[XID] ====================================================")

  ;; Step 1: Migrate meta-tables
  (log/info "[XID] Step 1/6: Migrating meta-tables...")
  (let [meta-result (migrate-meta-tables!)]

    ;; Step 2: Transform stored models
    (log/info "[XID] Step 2/6: Transforming stored models...")
    (let [model-result (model/transform-stored-models! :xid)]

      ;; Step 3: Migrate data tables
      (log/info "[XID] Step 3/6: Migrating data tables...")
      (let [data-result (migrate-all-tables!)]

        ;; Step 4: Rename relation tables (BEFORE provider switch!)
        (log/info "[XID] Step 4/6: Renaming relation tables...")
        (let [rename-result (rename-relation-tables!)]

          ;; Step 5: Add immutability triggers (constraints already added in step 3)
          (log/info "[XID] Step 5/6: Adding immutability triggers...")
          (create-xid-immutability-triggers!)

          ;; Step 6: Switch provider, save & reload
          (log/info "[XID] Step 6/6: Switching to NanoID provider...")
          (dataset/set-format! "xid")
          (id/set-provider! (id/->NanoIDProvider))
          (dataset/save-model! nil)
          (dataset/reload)

          (log/info "[XID] ====================================================")
          (log/info "[XID] XID migration complete!")
          (log/info "[XID] ====================================================")

          {:meta-tables meta-result
           :models model-result
           :data-tables data-result
           :relation-tables rename-result})))))

;;; ============================================================================
;;; Verification
;;; ============================================================================

(defn verify-xid-migration
  "Verify all records have XIDs.

  Returns:
    Map with verification results. Throws if any records missing XIDs."
  []
  (log/info "Verifying XID migration...")
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
        (log/info "XID migration verified: all records have XIDs")
        {:verified true
         :tables (count tables)})
      (do
        (log/errorf "XID migration incomplete: %d tables have missing XIDs"
                    (count missing-tables))
        (doseq [{:keys [table missing]} missing-tables]
          (log/errorf "  %s: %d records missing XIDs" table missing))
        (throw (ex-info "Missing XIDs"
                        {:missing missing-tables
                         :verified false}))))))

;;; ============================================================================
;;; Migration Reset (Testing Only)
;;; ============================================================================

(defn drop-xid-column!
  "Drop xid column from a table if it exists.

  Args:
    table-name - Name of the table

  Returns:
    :dropped or :not-exists"
  [table-name]
  (if (column-exists? table-name "xid")
    (do
      (log/infof "[XID] Dropping xid column from %s" table-name)
      (sql/execute! [(format "ALTER TABLE \"%s\" DROP COLUMN xid" table-name)])
      :dropped)
    :not-exists))

(defn reset-xid-migration!
  "Drop all xid columns and revert models — for testing only.

  WARNING: This is destructive!

  Steps:
  1. Revert stored models to euuid format
  2. Revert relation table names (XID-based → EUUID-based)
  3. Drop xid columns from all tables

  Returns:
    Map with results"
  []
  (log/warn "[XID] ====================================================")
  (log/warn "[XID] RESETTING XID MIGRATION")
  (log/warn "[XID] ====================================================")

  ;; Step 1: Revert models to euuid format first (before dropping columns)
  (log/info "[XID] Step 1: Reverting stored models to euuid format...")
  (let [model-result (model/transform-stored-models! :euuid)]

    ;; Step 2: Revert relation table names (BEFORE dropping xid columns!)
    (log/info "[XID] Step 2: Reverting relation table names...")
    (let [revert-result (revert-relation-tables!)]

      ;; Step 3: Find all tables with xid column and drop it
      (log/info "[XID] Step 3: Dropping xid columns...")
      (let [tables-with-xid (get-all-tables-with-xid)
            _ (log/infof "[XID] Found %d tables with xid column" (count tables-with-xid))
            drop-results (doall
                           (map (fn [table]
                                  {:table table
                                   :result (drop-xid-column! table)})
                                tables-with-xid))
            dropped-count (count (filter #(= :dropped (:result %)) drop-results))]

        (log/info "[XID] ====================================================")
        (log/infof "[XID] Reset complete: %d models reverted, %d relation tables renamed, %d xid columns dropped"
                   (:total model-result 0) (:reverted revert-result 0) dropped-count)
        (log/info "[XID] ====================================================")

        {:models model-result
         :relation-tables revert-result
         :tables {:dropped dropped-count
                  :total (count tables-with-xid)
                  :details drop-results}}))))
