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
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.patch.model :as model]
    [synthigy.dataset.postgres.patch :as postgres.patch]
    [synthigy.dataset.sql.naming :as naming :refer [entity->table-name relation->table-name]]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.db.sql :as sql]
    [synthigy.env :as env]
    [synthigy.log :as log]
    [synthigy.transit :refer [<-transit ->transit]]))


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
      (log/warn {:id ::missing-tables-skipped
                 :data {:count (count missing) :tables (vec missing)}}
                "Skipping missing tables"))
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
  (log/info {:id ::relation-tables-rename-starting}
            "Renaming relation tables")
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
            (log/info {:id ::relation-table-renamed
                       :data {:from old-name :to new-name}}
                      "Renaming relation table")
            ;; Rename table
            (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" old-name new-name)])
            ;; Rename indexes
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
  "Revert relation table names from XID-based back to EUUID-based names.
   Used during migration reset."
  []
  (log/info {:id ::relation-tables-revert-starting}
            "Reverting relation table names")
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
                (log/info {:id ::relation-table-reverted
                           :data {:from xid-name :to euuid-name}}
                          "Reverting relation table")
                ;; Rename table
                (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" xid-name euuid-name)])
                ;; Rename indexes
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
      (log/debug {:id ::xid-column-exists :data {:table table-name}}
                 "xid column already exists")
      :exists)
    (do
      (log/info {:id ::xid-column-added :data {:table table-name}}
                "Adding xid column to table")
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
  "Default `_eid` stride for the server-side backfill. Each stride is one
  autocommit UPDATE; bigger = fewer round-trips but longer per-statement lock
  and more dead tuples per transaction. 50k keeps each chunk sub-second on
  task_log-scale tables while bounding bloat."
  50000)

(def uuid->nanoid-plpgsql
  "DDL for the server-side `uuid_to_nanoid(uuid)` function.

  This is the in-database twin of `synthigy.dataset.id/uuid->nanoid`: same
  Base58 alphabet, same big-endian 128-bit → Base58 conversion, same '1'
  left-pad to 22 chars. It MUST stay byte-identical to the Clojure version or
  the backfill corrupts ids silently — verified over the zero/all-FF edge
  cases plus thousands of random UUIDs.

  Having the conversion run inside Postgres is what lets `generate-xids-for-table!`
  backfill 16M-row tables (e.g. task_log) with a set-based `UPDATE … SET xid =
  uuid_to_nanoid(euuid)` instead of streaming every row to the app and back."
  "CREATE OR REPLACE FUNCTION uuid_to_nanoid(u uuid)
RETURNS text AS $$
DECLARE
  alphabet text := '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  hexchars text := '0123456789abcdef';
  hex text;
  n numeric := 0;
  i int;
  d int;
  r int;
  s text := '';
BEGIN
  IF u IS NULL THEN
    RETURN NULL;
  END IF;
  hex := replace(u::text, '-', '');
  -- hex (big-endian) → numeric via Horner
  FOR i IN 1..32 LOOP
    d := position(substr(hex, i, 1) in hexchars) - 1;
    n := n * 16 + d;
  END LOOP;
  -- numeric → Base58, most-significant digit first
  WHILE n > 0 LOOP
    r := (n % 58)::int;
    s := substr(alphabet, r + 1, 1) || s;
    n := div(n, 58);
  END LOOP;
  -- left-pad with the zero digit ('1') to a fixed 22 chars
  RETURN lpad(s, 22, '1');
END;
$$ LANGUAGE plpgsql IMMUTABLE;")

(defn install-uuid->nanoid-fn!
  "Install (idempotently) the server-side `uuid_to_nanoid(uuid)` function used
  by the set-based backfill. Safe to call repeatedly — CREATE OR REPLACE."
  []
  (sql/execute! [uuid->nanoid-plpgsql])
  (log/debug {:id ::uuid-to-nanoid-fn-installed}
             "Installed server-side uuid_to_nanoid function"))

(defn generate-xids-for-table!
  "Backfill XIDs for every euuid-keyed row missing one, entirely server-side.

  The conversion runs in Postgres via `uuid_to_nanoid(euuid)` (see
  `install-uuid->nanoid-fn!`), so no row data crosses the wire — the app only
  drives `_eid`-range chunks. Each chunk is one autocommit `UPDATE` over a PK
  index range, so the whole backfill is O(n) and bounded in lock/bloat per
  statement. This is what makes 16M-row tables (task_log) tractable; the old
  per-row fetch-convert-update path timed out on them.

  Args:
    table-name - Name of the table (must have `_eid` PK + `euuid` column)
    opts - Optional map with:
           :batch-size - `_eid` stride per chunk (default 50,000)

  Returns:
    Map with :table, :updated (total count), :batches (number of chunks)"
  ([table-name]
   (generate-xids-for-table! table-name {}))
  ([table-name {:keys [batch-size]
                :or {batch-size default-batch-size}}]
   (log/info {:id ::xid-generation-starting
              :data {:table table-name :batch-size batch-size}}
             "Generating XIDs for table")
   (install-uuid->nanoid-fn!)
   ;; Bound the work to the `_eid` window that still needs a backfill. On a
   ;; fully-migrated table this returns nil and we skip entirely (cheap
   ;; idempotent re-run).
   (let [{:keys [lo hi]} (sql/execute-one!
                           [(format "SELECT min(_eid) AS lo, max(_eid) AS hi FROM \"%s\" WHERE xid IS NULL"
                                    table-name)])]
     (if (nil? lo)
       (do
         (log/info {:id ::xid-generation-complete
                    :data {:table table-name :updated 0 :batches 0}}
                   "Generated XIDs for table")
         {:table table-name :updated 0 :batches 0})
       ;; Walk `[lo, hi]` in fixed `_eid` strides. The PK index serves each
       ;; range scan, so chunks never re-walk migrated rows — O(n) overall.
       (let [update-sql (format "UPDATE \"%s\" SET xid = uuid_to_nanoid(euuid) WHERE xid IS NULL AND _eid >= ? AND _eid < ?"
                                table-name)]
         (loop [start lo
                total 0
                batches 0]
           (if (> start hi)
             (do
               (log/info {:id ::xid-generation-complete
                          :data {:table table-name :updated total :batches batches}}
                         "Generated XIDs for table")
               {:table table-name
                :updated total
                :batches batches})
             (let [end (+ start batch-size)
                   res (sql/execute-one! [update-sql start end])
                   updated (:next.jdbc/update-count res 0)]
               (log/debug {:id ::xid-batch-updated
                           :data {:table table-name :batch (inc batches) :records updated}}
                          "Batch updated")
               (recur end
                      (+ total updated)
                      (inc batches))))))))))

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
  "Add NOT NULL constraint to xid column.

  Args:
    table-name - Name of the table

  Returns:
    :added or :exists"
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
      (log/info {:id ::xid-index-added :data {:table table-name :index index-name}}
                "Added xid index")
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
    (log/info {:id ::immutability-triggers-starting
               :data {:tables (count tables)}}
              "Creating XID immutability triggers")

    ;; Create the trigger function once
    (sql/execute! [(postgres-xid-trigger-function)])
    (log/debug {:id ::trigger-function-created}
               "Created PostgreSQL trigger function for xid")

    ;; Create trigger on each table
    (doseq [table tables]
      (try
        (sql/execute! [(postgres-xid-trigger table)])
        (log/debug {:id ::xid-trigger-created :data {:table table}}
                   "Created xid trigger on table")
        (catch Exception e
          (log/warn {:id ::xid-trigger-failed
                     :data {:table table :error (.getMessage e)}
                     :error e}
                    "Could not create xid trigger"))))

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
  (log/info {:id ::meta-table-migrating :data {:action :migrating :subject :meta-table :table table-name}}
            "Migrating meta-table")

  ;; Add xid column if not exists, then populate XIDs for rows that don't have
  ;; one — set-based, same server-side conversion as the data tables (these
  ;; tables are small, but this keeps one conversion path + zero round-trips).
  (let [column-added (if (column-exists? table-name "xid")
                       false
                       (do
                         (sql/execute! [(format "ALTER TABLE %s ADD COLUMN xid VARCHAR(64)" table-name)])
                         true))
        res (sql/execute-one!
              [(format "UPDATE %s SET xid = uuid_to_nanoid(euuid) WHERE xid IS NULL" table-name)])
        updated (:next.jdbc/update-count res 0)]

    (log/info {:id ::meta-table-migrated
               :data {:action :migrated :subject :meta-table
                      :table table-name
                      :column (if column-added "added" "existed")
                      :updated updated}}
              "Meta-table migrated")

    {:table table-name
     :added-column column-added
     :updated updated}))

(defn migrate-meta-tables!
  "Migrate all schema meta-tables (dataset_entity, dataset_relation, dataset_entity_attribute).

  Uses deterministic uuid->nanoid — no mapping needed.

  Returns:
    Map with results for each meta-table"
  []
  (log/info {:id ::meta-tables-migration-starting :data {:action :migrating :subject :meta-tables}}
            "Migrating meta-tables")
  (install-uuid->nanoid-fn!)
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

;;; ============================================================================
;;; Legacy Audit-Config Normalization (Model Blobs)
;;; ============================================================================
;;
;; Real EYWA tables carry `modified_by` columns (modified-auditing) but their
;; stored data models declare NO audit CONFIG. The modern schema augmentation
;; (`iam/audit` `augment-schema-impl`) gates the `modified_by`/`created_by`
;; schema fields ENTIRELY on `[:configuration :audit :actions]`, so without
;; this the schema rebuilt by `migrate-to-xid!` would not declare those fields —
;; diverging from the columns that actually exist on disk. This runs BEFORE the
;; euuid→xid model transform; since it only touches `:configuration` (no ids)
;; it is format-agnostic and the later transform carries the config through.

(defn audited-table-names
  "Set of public table names carrying a `modified_by` column — i.e. the tables
   legacy EYWA modified-audits. Read from live information_schema so it matches
   exactly the tables `reconcile-legacy-audit-columns!` brought up to the modern
   created+modified layout."
  []
  (set (map :table_name
            (sql/execute! ["SELECT DISTINCT table_name FROM information_schema.columns
                            WHERE table_schema = 'public' AND column_name = 'modified_by'"]))))

(defn normalize-stored-audit-config!
  "Bring legacy EYWA model blobs in line with their audit COLUMNS.

   For every deployed `dataset_version`, unions `#{:modified :created}` into
   `[:configuration :audit :actions]` on each entity whose table carries
   `modified_by`. Entities whose tables are NOT audited are left untouched, so we
   never declare audit fields with no backing column. Existing `:actions` (and any
   `:who`/`:when` config) are preserved — never clobbered.

   Format-agnostic and idempotent (the union is a no-op once the actions are
   present). Postgres / legacy-EYWA import only — call BEFORE `migrate-to-xid!`."
  []
  (log/info {:id ::audit-config-normalize-starting :data {:action :patching :subject :model}}
            "Normalizing legacy audit config across stored models")
  (let [audited (audited-table-names)
        ;; euuid PK at call time (pre-migrate); `deployed = true` mirrors
        ;; `transform-stored-models!` so every dataset's active model is covered.
        versions (sql/execute! ["SELECT euuid, name, model FROM dataset_version
                                 WHERE deployed = true"])
        stats (atom {:versions 0 :entities 0})]
    (doseq [{:keys [euuid name model]} versions]
      (when model
        (let [m (<-transit model)
              changed (atom 0)
              entities' (reduce-kv
                          (fn [acc eid entity]
                            (let [audited? (and (string? (:name entity))
                                                (contains? audited (entity->table-name entity)))
                                  actions (get-in entity [:configuration :audit :actions])]
                              (if (and audited?
                                       (not (and (contains? actions :modified)
                                                 (contains? actions :created))))
                                (do (swap! changed inc)
                                    (assoc acc eid
                                           (assoc-in entity [:configuration :audit :actions]
                                                     (into #{:modified :created} actions))))
                                (assoc acc eid entity))))
                          {}
                          (:entities m))]
          (when (pos? @changed)
            (sql/execute! ["UPDATE dataset_version SET model = ? WHERE euuid = ?"
                           (->transit (assoc m :entities entities')) euuid])
            (swap! stats #(-> % (update :versions inc) (update :entities + @changed)))
            (log/info {:id ::audit-config-normalized
                       :data {:action :patching :subject :model
                              :version name :entities @changed}}
                      "Normalized audit config for stored model")))))
    (log/info {:id ::audit-config-normalize-complete
               :data {:action :patched :subject :model
                      :versions (:versions @stats) :entities (:entities @stats)}}
              "Normalized legacy audit config across stored models")
    @stats))

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
  (log/info {:id ::data-tables-migration-starting :data {:action :migrating :subject :data-tables}}
            "Starting XID migration on data tables")

  ;; Step 1: Add xid columns to deployed-schema tables
  (log/info {:id ::data-tables-step-1} "Step 1: Adding xid columns")
  (let [column-result (add-xid-columns-to-all-tables!)]
    (log/info {:id ::data-tables-columns-added
               :data {:added (:added column-result)
                      :existed (:existed column-result)}}
              "Added xid columns")

    ;; Step 2: Generate XIDs for every euuid-keyed DATA table (has both `_eid`
    ;; and `euuid`). `get-all-tables-with-xid` is deliberately broad and also
    ;; catches xid-native infra side-tables (e.g. `__iam_auth_connector`, which
    ;; has an xid PK but no `_eid`/`euuid`) — those have nothing to backfill and
    ;; would fail the `SELECT _eid, euuid …` query, so filter them out.
    (log/info {:id ::data-tables-step-2} "Step 2: Generating XIDs for all records")
    (let [tables (->> (get-all-tables-with-xid)
                      (filterv (fn [t] (and (column-exists? t "_eid")
                                            (column-exists? t "euuid")))))
          gen-results (doall (map generate-xids-for-table! tables))
          total-updated (reduce + (map :updated gen-results))]
      (log/info {:id ::data-tables-xids-generated
                 :data {:total-updated total-updated :tables (count tables)}}
                "Generated XIDs across tables")

      ;; Step 3: Add constraints to each table immediately after populating
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
  (log/info {:id ::migration-starting :data {:action :migrating :subject :xid}}
            "Starting complete XID migration (deterministic)")

  ;; Step 1: Migrate meta-tables
  (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 1 :of 6 :name "migrate-meta-tables"}}
            "Migrating meta-tables")
  (let [meta-result (migrate-meta-tables!)]

    ;; Step 2: Transform stored models
    (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 2 :of 6 :name "transform-stored-models"}}
              "Transforming stored models")
    (let [model-result (model/transform-stored-models! :xid)]

      ;; Step 3: Migrate data tables
      (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 3 :of 6 :name "migrate-data-tables"}}
                "Migrating data tables")
      (let [data-result (migrate-all-tables!)]

        ;; Step 4: Rename relation tables (BEFORE provider switch!)
        (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 4 :of 6 :name "rename-relation-tables"}}
                  "Renaming relation tables")
        (let [rename-result (rename-relation-tables!)]

          ;; Step 5: Add immutability triggers (constraints already added in step 3)
          (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 5 :of 6 :name "add-immutability-triggers"}}
                    "Adding immutability triggers")
          (create-xid-immutability-triggers!)

          ;; Step 6: Switch provider, save & reload
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

;;; ============================================================================
;;; Verification
;;; ============================================================================

(defn verify-xid-migration
  "Verify all records have XIDs.

  Returns:
    Map with verification results. Throws if any records missing XIDs."
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
      (log/info {:id ::xid-column-dropped :data {:table table-name}}
                "Dropping xid column")
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
  (log/warn {:id ::reset-starting}
            "RESETTING XID MIGRATION")

  ;; Step 1: Revert models to euuid format first (before dropping columns)
  (log/info {:id ::reset-step :data {:step 1 :of 3 :name "revert-stored-models"}}
            "Reverting stored models to euuid format")
  (let [model-result (model/transform-stored-models! :euuid)]

    ;; Step 2: Revert relation table names (BEFORE dropping xid columns!)
    (log/info {:id ::reset-step :data {:step 2 :of 3 :name "revert-relation-tables"}}
              "Reverting relation table names")
    (let [revert-result (revert-relation-tables!)]

      ;; Step 3: Find all tables with xid column and drop it
      (log/info {:id ::reset-step :data {:step 3 :of 3 :name "drop-xid-columns"}}
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

;;; ============================================================================
;;; ID Format Detection (PostgreSQL-specific)
;;; ============================================================================

(defn- ensure-id-trigger-function!
  "Ensures the `prevent_<id>_update()` function exists for the ACTIVE id field.
  Required before any id-immutability triggers can be created — in either xid
  or euuid mode. The generic `postgres-id-trigger-function` keys on `(id/field)`,
  so in xid mode it emits exactly the same `prevent_xid_update()` body as the
  legacy xid-specific helper, and in euuid mode it emits `prevent_euuid_update()`
  (previously never created — euuid per-table triggers referenced a missing fn)."
  []
  (log/debug {:id ::id-trigger-function-ensuring :data {:id-key (id/key)}}
             "Ensuring id immutability trigger function exists")
  (sql/execute-one! [(postgres.patch/postgres-id-trigger-function)]))

(defn detect-format
  "Detect ID format for Postgres based on existing tables.
  Called when no format is stored in patcho.

  Returns:
    \"xid\"   - fresh install (no dataset_version) or migrated (has xid column)
    \"euuid\" - legacy Postgres deployment (has dataset_version with euuid only)"
  []
  (cond
    ;; No dataset_version table → fresh install → xid. euuid is NOT a
    ;; fresh-deploy format: it exists only as a legacy EYWA source brought in
    ;; via import. A stray SYNTHIGY_ID_FORMAT=euuid is ignored (and flagged) so
    ;; we never create a euuid provider over xid-only fresh-create DDL.
    (not (table-exists? "dataset_version"))
    (do
      (when (= "euuid" env/id-format)
        (log/warn {:id ::euuid-fresh-deploy-unsupported}
                  "SYNTHIGY_ID_FORMAT=euuid ignored — fresh installs are xid-only; euuid exists only as a legacy source to import. Deploying xid."))
      (log/info {:id ::id-format-fresh :data {:format "xid"}}
                "Fresh Postgres (no dataset_version), using xid")
      "xid")

    ;; Has xid column → already migrated → xid
    (column-exists? "dataset_version" "xid")
    (do
      (log/info {:id ::id-format-migrated :data {:action :migrated :subject :id-format :format "xid"}}
                "Migrated Postgres (has xid column), using xid")
      "xid")

    ;; Has euuid only → legacy → euuid
    :else
    (do
      (log/info {:id ::id-format-legacy :data {:format "euuid"}}
                "Legacy Postgres (euuid only), using euuid")
      "euuid")))

(defn legacy-deployment?
  "Returns true if this is a legacy Postgres deployment.
  Legacy = has dataset_version table but no stored format in patcho.
  Used to skip destructive setup operations on existing deployments."
  []
  (and (nil? (dataset/current-format))
       (table-exists? "dataset_version")))

(defn detect-and-set-provider!
  "Detect ID format and set provider for Postgres.
  Called from both setup and start.

  If format is stored → use it.
  If not stored → detect from existing tables, set provider, store format.

  For XID mode, also ensures the trigger function exists."
  []
  (if-let [stored (dataset/current-format)]
    ;; Format stored - use it
    (do
      (log/info {:id ::id-format-stored-used :data {:format stored}}
                "Using stored ID format")
      (dataset/initialize-provider! stored)
      (ensure-id-trigger-function!))

    ;; Not stored - detect and store
    (let [format (detect-format)]
      (log/info {:id ::id-format-detected :data {:format format}}
                "Detected ID format")
      (dataset/initialize-provider! format)
      (ensure-id-trigger-function!)
      (dataset/set-format! format))))

;;; ============================================================================
;;; Legacy Schema Reconciliation (euuid → modern meta/audit layout)
;;; ============================================================================

(defn reconcile-legacy-meta-columns!
  "Bridge a pre-`deployed_on` (legacy) deployment forward: idempotently add the
   meta-model columns that newer dataset versions expect — `deployed_on` on
   dataset_version, `active` on dataset_entity / dataset_relation — and backfill
   sensible values (deployed_on from modified_on; active true). The boot-time
   version detection (`deployed-versions`, `latest-deployed-version`) and the
   model patches READ these columns, so they must exist BEFORE patcho runs —
   otherwise the patch that would add them never gets to run (chicken-and-egg).
   Idempotent (`ADD COLUMN IF NOT EXISTS`); a no-op on already-current schemas."
  []
  (doseq [stmt ["ALTER TABLE dataset_version  ADD COLUMN IF NOT EXISTS deployed_on timestamp"
                "ALTER TABLE dataset_entity   ADD COLUMN IF NOT EXISTS active boolean"
                "ALTER TABLE dataset_relation ADD COLUMN IF NOT EXISTS active boolean"
                "UPDATE dataset_version  SET deployed_on = modified_on WHERE deployed = true AND deployed_on IS NULL"
                "UPDATE dataset_entity   SET active = true WHERE active IS NULL"
                "UPDATE dataset_relation SET active = true WHERE active IS NULL"]]
    (try
      (sql/execute-one! [stmt])
      (catch Throwable ex
        (log/warn {:id ::legacy-meta-reconcile-skip
                   :data {:action :patching :subject :meta-table :sql stmt}}
                  (.getMessage ex))))))

(defn reconcile-legacy-audit-columns!
  "Bring a legacy EYWA schema's audit columns up to the modern Synthigy layout.
   Real EYWA tables carry `modified_by`/`modified_on` (modified-auditing) but NOT
   `created_by`/`created_on` — yet modern Synthigy's audit-augmented schema
   declares and QUERIES created_by/created_on for every audited entity, so a
   query against a legacy table fails with `column \"created_by\" does not exist`.
   For every audited table (one carrying `modified_by`), add the missing
   created-audit columns (plain — the FK + preserve triggers are (re)installed by
   the IAM audit module at start). Idempotent plpgsql loop; no-op once present."
  []
  (try
    (sql/execute-one!
      ["DO $$
        DECLARE t text;
        BEGIN
          FOR t IN SELECT table_name FROM information_schema.columns
                   WHERE table_schema = 'public' AND column_name = 'modified_by'
          LOOP
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS created_by bigint', t);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp', t);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp', t);
          END LOOP;
        END $$;"])
    (log/info {:id ::legacy-audit-reconciled :data {:action :patching :subject :data-tables}}
              "Reconciled legacy audit columns (added created_by/created_on where missing)")
    (catch Throwable ex
      (log/warn {:id ::legacy-audit-reconcile-failed :data {:action :patching :subject :data-tables}}
                (.getMessage ex)))))

;;; ============================================================================
;;; Legacy EYWA → Synthigy import (Postgres only)
;;;
;;; Synthigy is xid. euuid exists ONLY as a legacy EYWA source layout. This is
;;; the one-time, idempotent importer that converts a connected legacy EYWA
;;; (euuid) database into the native Synthigy (xid) layout. The runtime stays
;;; xid-only; this is the only euuid-aware path besides boot detection.
;;; ============================================================================

(defn legacy-eywa-layout?
  "True when the connected Postgres is running on a LEGACY euuid layout — i.e.
   the active id format is euuid. Synthigy is xid-native; any euuid layout is a
   legacy EYWA database to be imported to xid (one-time). Format, not structure,
   is the right signal: a modern euuid deploy already carries `xid` columns on the
   meta-schema, so a 'no xid column' test would miss it. After the import flips
   the provider to xid this returns false — making the import gate idempotent."
  []
  (= :euuid (id/key)))

(defn import-eywa->synthigy!
  "One-time, idempotent forward migration of a LEGACY EYWA (euuid) Postgres
   database into the native Synthigy (xid) layout. No-op (returns
   `{:skipped :already-synthigy}`) on an already-native DB. After it runs the
   database is xid forever and the runtime treats it like any native deploy.

   Requires the active provider to be euuid (boot detection sets this when it
   sees a legacy layout). Steps: (0) reconcile the legacy meta-columns the
   migration reads (`deployed_on`, `active`) + audit columns/config, (1-6)
   `migrate-to-xid!`, then verify every row carries an xid.

   Postgres ONLY — SQLite/CRDB never carry a legacy euuid layout."
  []
  (if-not (legacy-eywa-layout?)
    (do (log/info {:id ::eywa-import-skip :data {:action :migrating :subject :xid}}
                  "Native Synthigy (xid) layout — no EYWA import needed")
        {:skipped :already-synthigy})
    (do (log/warn {:id ::eywa-import-starting :data {:action :migrating :subject :xid}}
                  "Legacy EYWA database detected — importing to Synthigy xid layout")
        ;; transform-stored-models! reads dataset_version.deployed_on and the
        ;; model patches read `active` — add those columns BEFORE migrating.
        (reconcile-legacy-meta-columns!)
        ;; Audit bring-up (#3), in two halves, BOTH before the schema-building
        ;; reload inside migrate-to-xid!: (i) COLUMNS — real EYWA has
        ;; modified-auditing but not created-auditing, and the modern audit
        ;; schema queries created_by/created_on;
        (reconcile-legacy-audit-columns!)
        ;; (ii) MODEL CONFIG — legacy models declare no [:configuration :audit
        ;; :actions], which is what augment-schema gates the audit fields on, so
        ;; the columns above would otherwise be invisible to the rebuilt schema.
        (normalize-stored-audit-config!)
        (let [result (migrate-to-xid!)]
          (verify-xid-migration)
          (log/info {:id ::eywa-import-complete :data {:action :migrated :subject :xid}}
                    "EYWA → Synthigy xid import complete and verified")
          (assoc result :imported true)))))
