(ns synthigy.dataset.postgres.patch
  "PostgreSQL-specific dataset feature patches.

  This namespace defines patches for the :synthigy/dataset topic.
  These patches handle database transforms and feature evolution that
  are specific to PostgreSQL.

  While the topic :synthigy/dataset is defined in synthigy.dataset
  (database-agnostic), the actual patch implementations are database-specific.

  Examples of feature patches:
  - Schema transforms (integer → bigint conversions)
  - Constraint modifications (NOT NULL removals)
  - Index optimizations
  - New storage features

  To level dataset features:
    (require '[patcho.patch :as patch])
    (patch/level! :synthigy/dataset)"
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [next.jdbc :as jdbc]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming
     :as naming
     :refer [normalize-name
             relation->table-name
             entity->relation-field
             entity->table-name]]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.db :refer [*db*]]
    [synthigy.db.postgres]  ; Load Postgres JDBCBackend implementation
    [synthigy.db.sql :as sql :refer [execute-one!]]))

;;; ============================================================================
;;; ID Immutability Triggers
;;; ============================================================================

(defn postgres-id-trigger-function
  "Returns SQL to create the PostgreSQL trigger function for ID immutability.

  This function prevents updates to the ID column (euuid/xid) once it's set.
  Instead of throwing an exception, it silently keeps the old value."
  []
  (format
    "CREATE OR REPLACE FUNCTION prevent_%s_update()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.%s IS NOT NULL AND NEW.%s IS DISTINCT FROM OLD.%s THEN
    -- Keep the old value instead of raising an exception
    NEW.%s := OLD.%s;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;"
    (id/field) (id/field) (id/field) (id/field) (id/field) (id/field)))

(defn postgres-id-trigger
  "Returns SQL to create a PostgreSQL trigger for ID immutability on a table.

  Args:
    table-name - Name of the table to protect

  Returns:
    SQL string to create the trigger"
  [table-name]
  (format
    "CREATE OR REPLACE TRIGGER prevent_%s_update_trigger
  BEFORE UPDATE ON \"%s\"
  FOR EACH ROW
  EXECUTE FUNCTION prevent_%s_update();"
    (id/field) table-name (id/field)))

(defn drop-postgres-id-trigger
  "Returns SQL to drop a PostgreSQL ID immutability trigger.

  Args:
    table-name - Name of the table

  Returns:
    SQL string to drop the trigger"
  [table-name]
  (format
    "DROP TRIGGER IF EXISTS prevent_%s_update_trigger ON \"%s\";"
    (id/field) table-name))

(defn drop-postgres-id-trigger-function
  "Returns SQL to drop the PostgreSQL trigger function for ID immutability."
  []
  (format "DROP FUNCTION IF EXISTS prevent_%s_update() CASCADE;" (id/field)))

;;; ============================================================================
;;; SQLite ID Immutability Triggers
;;; ============================================================================

(defn sqlite-id-trigger
  "Returns SQL to create a SQLite trigger for ID immutability on a table.

  SQLite triggers use WHEN clause and RAISE() instead of stored functions.

  Args:
    table-name - Name of the table to protect

  Returns:
    SQL string to create the trigger"
  [table-name]
  (format
    "CREATE TRIGGER IF NOT EXISTS prevent_%s_update_trigger
  BEFORE UPDATE ON \"%s\"
  FOR EACH ROW
  WHEN NEW.%s != OLD.%s
BEGIN
  SELECT RAISE(ABORT, 'Cannot modify %s value');
END;"
    (id/field) table-name (id/field) (id/field) (id/field)))

;; Backwards-compatible alias
(def sqlite-euuid-trigger sqlite-id-trigger)

(defn drop-sqlite-id-trigger
  "Returns SQL to drop a SQLite ID immutability trigger.

  Args:
    table-name - Name of the table

  Returns:
    SQL string to drop the trigger"
  [table-name]
  (format
    "DROP TRIGGER IF EXISTS prevent_%s_update_trigger;"
    (id/field)))

(defn get-entity-tables
  "Gets all entity table names from the deployed schema.

  Returns:
    Vector of table name strings"
  []
  (let [schema (sql-query/deployed-schema)]
    (vec (distinct (keep :table (vals schema))))))

(defn create-id-immutability-triggers!
  "Creates ID immutability triggers on all entity tables.

  This should be called after deploying a schema to ensure ID (euuid/xid)
  values cannot be changed once set.

  Args:
    db - Database connection (from *db* binding)

  Returns:
    Map with :created (count of triggers created) and :tables (list of table names)"
  ([]
   (let [tables (get-entity-tables)]

     (log/infof "Creating ID immutability triggers for %d tables (column: %s)" (count tables) (id/field))

     ;; Create the trigger function once
     ;; Note: This works for both PostgreSQL and CockroachDB (PostgreSQL-compatible)
     (sql/execute! [(postgres-id-trigger-function)])
     (log/debug "Created PostgreSQL/CockroachDB trigger function")

     ;; Create trigger on each table
     (doseq [table tables]
       (try
         (sql/execute! [(postgres-id-trigger table)])
         (catch Throwable ex
           (log/errorf ex "[Postgres/Patch] Couldn't create immutability trigger for: %s" table)))
       (log/debugf "Created trigger on table: %s" table))

     {:created (count tables)
      :tables tables
      :type :postgres})))

;; Backward compatibility alias
(def create-euuid-immutability-triggers! create-id-immutability-triggers!)

(defn remove-id-immutability-triggers!
  "Removes ID immutability triggers from all entity tables.

  This is primarily for rollback or testing purposes.

  Args:
    db - Database connection (from *db* binding)

  Returns:
    Map with :removed (count of triggers removed) and :tables (list of table names)"
  []
  (let [tables (get-entity-tables)]

    (log/infof "Removing ID immutability triggers from %d tables! (column: %s)"
               (count tables) (id/field))
    (doseq [table tables]
      (sql/execute! *db* [(drop-postgres-id-trigger table)])
      (log/debugf "Dropped trigger from table: %s" table))

        ;; Drop the trigger function
    (sql/execute! *db* [(drop-postgres-id-trigger-function)])
    (log/debugf "Dropped PostgreSQL/CockroachDB trigger function")

    {:removed (count tables)
     :tables tables
     :type :postgres}))

;; Backward compatibility alias
(def remove-euuid-immutability-triggers! remove-id-immutability-triggers!)

;;; ============================================================================
;;; Patch Helper Functions
;;; ============================================================================

(defn is-mandatory-attribute?
  "Check if attribute has mandatory constraint in the dataset model"
  [attribute]
  (= "mandatory" (:constraint attribute)))

(defn get-column-constraints
  "Get column constraints from database including NOT NULL"
  [table-name column-name]
  (try
    (execute-one! ["SELECT column_name, is_nullable, data_type
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ? AND table_schema = 'public'"
                   table-name column-name])
    (catch Exception _
      (log/warn "Could not get column constraints for" table-name column-name)
      nil)))

(defn get-relation-indexes
  [relation]
  (let [table (relation->table-name relation)]
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (let [metadata (.getMetaData conn)
            indexes (.getIndexInfo metadata nil "public" table false false)]
        (loop [col indexes
               result []]
          (let [idx (.next col)]
            (if-not idx result
                    (recur col (conj result
                                     {:index (.getString col "INDEX_NAME")
                                      :column (.getString col "COLUMN_NAME")
                                      :type (if (.getBoolean col "NON_UNIQUE") :non-unique :unique)})))))))))

(defn get-column-type
  "Get current column type from database"
  [table-name column-name]
  (try
    (execute-one! ["SELECT data_type
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ? AND table_schema = 'public'"
                   table-name column-name])
    (catch Exception _
      (log/warn "Could not get column type for" table-name column-name)
      nil)))

(defn fix-on-reference-indexes
  []
  (let [statements (let [model (dataset/deployed-model)
                         relations (core/get-relations model)]
                     (reduce
                       (fn [r {:keys [from to]
                               :as relation}]
                         (let [table (relation->table-name relation)
                               from-field (entity->relation-field from)
                               to-field (entity->relation-field to)
                               indexes (set (map :index (get-relation-indexes relation)))]
                           (cond-> r
                             (not (indexes (str table \_ "fidx")))
                             (conj (format "create index %s_fidx on \"%s\" (%s);" table table from-field))
                               ;;
                             (not (indexes (str table \_ "tidx")))
                             (conj (format "create index %s_tidx on \"%s\" (%s);" table table to-field)))))
                       []
                       relations))]
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info statement)
          (catch Throwable ex
            (log/error ex statement)))))))

(defn fix-on-delete-set-null-to-references
  []
  (let [model (dataset/deployed-model)
        entities (core/get-entities model)]
    (reduce
      (fn [r entity]
        (let [user-table-name "user"
              entity-table (entity->table-name entity)
              modified_by (str entity-table \_ "modified_by_fkey")
              refered-attributes (filter
                                   (comp
                                     #{"user" "group" "role"}
                                     :type)
                                   (:attributes entity))
                ;;
              current-result
              (conj r
                    (format "alter table \"%s\" drop constraint \"%s\"" entity-table modified_by)
                    (format
                      "alter table \"%s\" add constraint \"%s\" foreign key (modified_by) references \"%s\"(_eid) on delete set null"
                      entity-table modified_by user-table-name))]
          (reduce
            (fn [r {attribute-name :name
                    attribute-type :type}]
              (let [attribute-column (normalize-name attribute-name)
                    constraint-name (str entity-table \_ attribute-column "_fkey")
                    refered-table (case attribute-type
                                    "user" "user"
                                    "group" "user_group"
                                    "role" "user_role")]
                (conj
                  r
                  (format "alter table \"%s\" drop constraint %s" entity-table constraint-name)
                  (format
                    "alter table \"%s\" add constraint \"%s\" foreign key (%s) references \"%s\"(_eid) on delete set null"
                    entity-table constraint-name attribute-column refered-table))))
            current-result
            refered-attributes)))
      []
      entities)))

(defn fix-mandatory-constraints
  "Remove NOT NULL constraints from all mandatory fields in the dataset model.
   This allows more flexible data entry by making all fields nullable at the database level
   while still preserving the logical mandatory constraint in the dataset model."
  []
  (let [model (dataset/deployed-model)
        entities (core/get-entities model)
        statements (reduce
                     (fn [statements entity]
                       (let [entity-table (entity->table-name entity)

                              ;; Get all mandatory attributes for this entity
                             mandatory-attributes (filter is-mandatory-attribute? (:attributes entity))

                              ;; Generate ALTER TABLE statements for mandatory attributes
                             attribute-statements
                             (reduce
                               (fn [attr-statements {:keys [name]}]
                                 (let [column-name (normalize-name name)
                                       column-info (get-column-constraints entity-table column-name)
                                       has-not-null? (and column-info (= "NO" (:is_nullable column-info)))]
                                   (if has-not-null?
                                     (conj attr-statements
                                           (format "ALTER TABLE \"%s\" ALTER COLUMN %s DROP NOT NULL"
                                                   entity-table column-name))
                                     attr-statements)))
                               []
                               mandatory-attributes)]

                         (concat statements attribute-statements)))
                     []
                     entities)]
      ;; Execute the statements
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info "✅" statement)
          (catch Throwable ex
            (log/error "❌ EX:" ex)
            (log/error "   " statement)))))))

(defn fix-int-types
  "Fix integer types by converting integer columns to bigint for:
   - All int type attributes
   - All references to user/group/role entities (foreign keys)
   - Skip if column is already bigint"
  []
  (let [model (dataset/deployed-model)
        entities (core/get-entities model)
        statements (reduce
                     (fn [statements entity]
                       (let [entity-table (entity->table-name entity)

                              ;; Handle modified_by column (always references user)
                             modified-by-type (get-column-type entity-table "modified_by")
                             modified-by-statements
                             (if (and modified-by-type (= "integer" (:data_type modified-by-type)))
                               [(format "ALTER TABLE \"%s\" ALTER COLUMN modified_by TYPE bigint" entity-table)]
                               [])

                              ;; Handle entity attributes
                             attribute-statements
                             (reduce
                               (fn [attr-statements {:keys [name type]}]
                                 (if-not (contains? #{"user" "group" "role" "int"} type)
                                   attr-statements
                                   (let [column-name (normalize-name name)
                                         current-type (get-column-type entity-table column-name)
                                         needs-conversion? (and current-type (= "integer" (:data_type current-type)))]
                                     (if needs-conversion?
                                       (conj attr-statements
                                             (format "ALTER TABLE \"%s\" ALTER COLUMN %s TYPE bigint"
                                                     entity-table column-name))
                                       attr-statements))))
                               []
                               (:attributes entity))]

                         (concat statements modified-by-statements attribute-statements)))
                     []
                     entities)]
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info "✅" statement)
          (catch Throwable ex
            (log/error "❌ EX:" ex)
            (log/error "   " statement)))))))

(defn fix-deployed-on
  []
  (let [model (dataset/deployed-model)
        dataset-version-entity (core/get-entity model (id/entity :dataset/version))
        table-name (entity->table-name dataset-version-entity)]
    (try
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (execute-one! con [(format "update \"%s\" set deployed_on = modified_on" table-name)]))
      (catch Throwable ex
        (log/errorf ex "[Dataset] Couldn't patch deployed_on... There were some DB issues")))))

;;; ============================================================================
;;; Dataset Model Versioning (:synthigy.dataset/model)
;;; ============================================================================
;;
;; This topic tracks the version of the dataset META-MODEL schema itself
;; (the entities/relations/attributes that describe datasets).
;;
;; The meta-model is stored in resources/dataset/dataset.json and defines:
;; - Dataset, DatasetVersion entities
;; - Entity, Relation, Attribute entities
;; - Model structure and constraints
;;
;; Model patches deploy new versions via dataset/deploy! and are tracked
;; in the __deploy_history table (same versioning system as user models).
;;

;; Current model version (from resources file)
(patch/current-version :synthigy.dataset/model (:name (dataset/current-dataset-version)))

;; Installed model version from __deploy_history
(patch/installed-version
  :synthigy.dataset/model
  (or (some-> (dataset/latest-deployed-version (id/data :dataset/id))
              :name
              str)
      "0"))

;;; ============================================================================
;;; Dataset Model Patches
;;; ============================================================================

;; Patch 1.0.0 - Initial dataset meta-model deployment
(patch/upgrade :synthigy.dataset/model
               "1.0.0"
               (log/info "[Dataset Model] Deploying meta-model v1.0.0 (removing Dataset Entity/Relation)")
               (dataset/deploy! (dataset/current-dataset-version)))

;; Patch 1.0.2 - Remove UI layout attributes
(patch/upgrade :synthigy.dataset/model
               "1.0.2"
               (log/info "[Dataset Model] Deploying meta-model v1.0.2 (removing UI layout attributes)")
               (log/info "[Dataset Model] Width/Height/Position/Type/Path will become inactive")
               (dataset/deploy! (dataset/current-dataset-version))
               (log/info "[Dataset Model] Migration complete - UI attributes preserved as inactive columns"))

(patch/upgrade :synthigy.dataset/model
               "1.0.3"
               (log/info "[Dataset Model] Deploying meta-model v1.0.3 (removing UI layout attributes)")
               (log/info "[Dataset Model] Adding deployed_on to dataset versions")
               (dataset/deploy! (dataset/current-dataset-version))
               (fix-deployed-on)
               (log/info "[Dataset Model] Migration complete"))

;;; ============================================================================
;;; PostgreSQL Dataset Feature Patches
;;; ============================================================================

;; Patch 0.5.0 - Database schema fixes (PostgreSQL only)
;; (Matches EYWA's 0.5.0 patch for compatibility)
(patch/upgrade :synthigy/dataset
               "0.5.0"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info "[Dataset] Fixing integer types (integer → bigint)")
                 (fix-int-types)
                 (log/info "[Dataset] Removing NOT NULL constraints from mandatory fields")
                 (fix-mandatory-constraints)))

;; Patch 1.0.0 - Initial feature version marker (PostgreSQL only)
(patch/upgrade :synthigy/dataset
               "1.0.0"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info "[Dataset] PostgreSQL dataset features initialized at v1.0.0")))

;; Patch 1.0.1 - EUUID Immutability Triggers (PostgreSQL only)
;; This patch installs triggers on all entity tables to ensure EUUID values
;; cannot be modified once set. This enables order-independent mapping in
;; store-entity-records
(patch/upgrade :synthigy/dataset
               "1.0.1"
               ;; Guard: only run on PostgreSQL (skip for SQLite, etc.)
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info "[Dataset] Installing EUUID immutability triggers...")
                 (log/info "[Dataset] This enables order-independent mapping for CockroachDB/SQLite compatibility")
                 (try
                   (let [result (create-euuid-immutability-triggers!)]
                     (log/info (format "[Dataset] ✅ Created %d EUUID immutability triggers (%s)"
                                       (:created result)
                                       (name (:type result))))
                     (log/info (format "[Dataset] Protected tables: %s"
                                       (str/join ", " (:tables result)))))
                   (catch Exception e
                     (log/error e "[Dataset] ❌ Failed to create EUUID immutability triggers")
                     (throw e)))
                 (log/info "[Dataset] EUUID immutability migration complete")))
