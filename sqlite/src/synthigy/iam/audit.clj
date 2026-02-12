(ns synthigy.iam.audit
  "SQLite-specific audit enhancement implementation.

  Extends the dataset.enhance/AuditEnhancement protocol for SQLite databases.
  This includes DDL generation (columns, triggers), schema augmentation, and
  mutation enhancement.

  Patch Management:
  - Uses topic :synthigy.iam/audit (shared across all database implementations)
  - Registers upgrade patch for version 1.0.0
  - Protocol extension happens at namespace load (top-level)
  - Patch only performs migration (setup!)

  To level this component:
    (require '[patcho.patch :as patch])
    (patch/level! :synthigy.iam/audit)"
  (:require
    [clojure.tools.logging :as log]
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.dataset :refer [deployed-model deployed-entity]]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.enhance :as enhance]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
    [synthigy.db :refer [*db*]]
    [synthigy.db.sql :refer [execute! execute-one!]]
    [synthigy.db.sqlite]  ; Load SQLite JDBCBackend implementation
    ))

;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn- enhance-audit-data
  "Populates modified_by and created_by values during mutations.

  Reads the current user from access control context and injects the user EID
  into mutation data.

  Args:
    entity-id - Entity UUID being mutated
    data      - Mutation data structure (from analyze-data)

  Returns:
    Enhanced data with modified_by and created_by values populated for all records"
  [entity-id data]
  (let [current-user-eid (some-> (access/current-user) :_eid)
        table (entity->table-name (deployed-entity entity-id))]
    (if-not current-user-eid
      data
      (update-in data [:entity table]
                 (fn [mapping]
                   (reduce-kv
                     (fn [data tmp-id _]
                       (-> data
                           (assoc-in [tmp-id :modified_by] current-user-eid)
                           (assoc-in [tmp-id :created_by] current-user-eid)))
                     mapping
                     mapping))))))

;; ============================================================================
;; SQLite-Specific Protocol Implementations
;; ============================================================================

(defn- transform-audit-impl
  "Adds audit columns and triggers to newly created entity tables (SQLite).

  Adds four audit fields:
  - modified_by: User who last modified the record
  - modified_on: Timestamp of last modification
  - created_by: User who created the record (preserved on UPDATE)
  - created_on: Timestamp of creation (immutable after INSERT)

  Uses SQLite-specific features:
  - INTEGER foreign key to user table
  - strftime('%%Y-%%m-%%d %%H:%%M:%%f', 'now') for automatic timestamps
  - SQLite triggers (no separate functions needed)"
  [db tx entities]
  (log/info "[Audit/SQLite] Adding audit columns and triggers to new entity tables")

  (doseq [{:keys [name]
           :as entity} entities
          :let [table (entity->table-name entity)
                trigger-name (str "update_" (normalize-name name) "_modified_on")
                preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]]

    ;; Add modified_by column (references user table)
    (execute! tx
              [(format "ALTER TABLE \"%s\" ADD COLUMN modified_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                       table)])

    ;; Add modified_on column (auto-set on INSERT)
    ;; Note: ALTER TABLE only allows literal defaults, not expressions like strftime()
    ;; Trigger handles millisecond precision on updates
    (execute! tx
              [(format "ALTER TABLE \"%s\" ADD COLUMN modified_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                       table)])

    ;; Add created_by column (references user table)
    (execute! tx
              [(format "ALTER TABLE \"%s\" ADD COLUMN created_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                       table)])

    ;; Add created_on column (auto-set on INSERT, immutable)
    ;; Note: ALTER TABLE only allows literal defaults, not expressions like strftime()
    (execute! tx
              [(format "ALTER TABLE \"%s\" ADD COLUMN created_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                       table)])

    ;; Add trigger for UPDATE (auto-update modified_on)
    (execute! tx
              [(format "DROP TRIGGER IF EXISTS %s"
                       trigger-name)])
    (execute! tx
              [(format "CREATE TRIGGER %s
                        AFTER UPDATE ON \"%s\"
                        FOR EACH ROW
                        BEGIN
                          UPDATE \"%s\" SET modified_on = strftime('%%Y-%%m-%%d %%H:%%M:%%f', 'now') WHERE _eid = NEW._eid;
                        END"
                       trigger-name table table)])

    ;; Add trigger to preserve created_* fields on UPDATE
    ;; SQLite doesn't allow preventing field updates directly, so we restore them
    (execute! tx
              [(format "DROP TRIGGER IF EXISTS %s"
                       preserve-trigger-name)])
    (execute! tx
              [(format "CREATE TRIGGER %s
                        AFTER UPDATE ON \"%s\"
                        FOR EACH ROW
                        WHEN (NEW.created_by != OLD.created_by OR NEW.created_on != OLD.created_on)
                        BEGIN
                          UPDATE \"%s\" SET created_by = OLD.created_by, created_on = OLD.created_on WHERE _eid = NEW._eid;
                        END"
                       preserve-trigger-name table table)])

    (log/debugf "[Audit/SQLite] Added audit columns and triggers to table: %s" table)))

(defn- augment-schema-impl
  "Returns audit field and relation definitions for Postgres runtime schema.

  Returns:
    {:fields {:modified_by {...} :modified_on {...} :created_by {...} :created_on {...}}
     :relations {:modified_by {...} :created_by {...}}}"
  [db entity]
  (let [entity-id (id/extract entity)
        entity-table (entity->table-name entity)
        user-entity (core/reference-entity-uuid "user")
        user-table (when user-entity
                     (some-> (deployed-model)
                             (core/get-entity user-entity)
                             entity->table-name))]
    (cond->
      {:fields {:modified_on {:key :modified_on
                              :type "timestamp"}
                :created_on {:key :created_on
                             :type "timestamp"}}}
      (and user-entity user-table)
      (->
        (assoc-in [:fields :modified_by]
                  {:key :modified_by
                   :type "user"
                   :reference/entity user-entity})
        (assoc-in [:fields :created_by]
                  {:key :created_by
                   :type "user"
                   :reference/entity user-entity})
        (assoc-in [:relations :modified_by]
                  {:from entity-id
                   :from/field :modified_by
                   :from/table entity-table
                   :to :iam/user
                   :to/field :_eid
                   :to/table user-table
                   :table entity-table  ; M2O: no link table
                   :type :one})
        (assoc-in [:relations :created_by]
                  {:from entity-id
                   :from/field :created_by
                   :from/table entity-table
                   :to :iam/user
                   :to/field :_eid
                   :to/table user-table
                   :table entity-table  ; M2O: no link table
                   :type :one})))))

;; ============================================================================
;; Protocol Extension (Top-Level - Runs at Namespace Load)
;; ============================================================================

(extend-protocol enhance/AuditEnhancement
  synthigy.db.SQLite
  (transform-audit [db tx entities]
    (transform-audit-impl db tx entities))
  (augment-schema [db entity]
    (augment-schema-impl db entity))
  (audit [db entity-id data tx]
    (enhance-audit-data entity-id data)))

;; ============================================================================
;; Migration Utilities
;; ============================================================================

(defn setup!
  "Adds audit columns to ALL existing entity tables in the SQLite database.

  Adds four audit fields to each entity table:
  - modified_by: User who last modified the record
  - modified_on: Timestamp of last modification
  - created_by: User who created the record (preserved on UPDATE)
  - created_on: Timestamp of creation (immutable after INSERT)

  This is a migration utility for databases that were created before
  audit enhancement was enabled.

  WARNING: This modifies the database schema. Use with caution.

  Args:
    db    - SQLite database instance
    model - ERD model containing entities

  Returns:
    {:added-columns count :added-triggers count}"
  []
  (log/info "[Audit/SQLite] Adding audit columns to existing entity tables")

  (let [db *db*
        model (synthigy.dataset/deployed-model)
        entities (core/get-entities model)
        results (atom {:added-columns 0
                       :added-triggers 0})]

    (with-open [conn (jdbc/get-connection (:datasource db))]
      (doseq [{:keys [name]
               :as entity} entities
              :let [table (entity->table-name entity)
                    trigger-name (str "update_" (normalize-name name) "_modified_on")
                    preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]]

        (try
          ;; Check if columns exist (SQLite doesn't have IF NOT EXISTS for ALTER TABLE in older versions)
          (let [existing-columns (try
                                   (set (map :name (jdbc/execute! conn
                                                                  [(format "PRAGMA table_info(\"%s\")" table)])))
                                   (catch Exception _ #{}))]

            ;; Add modified_by column if it doesn't exist
            (when-not (contains? existing-columns "modified_by")
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD COLUMN modified_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                 table)])
              (swap! results update :added-columns inc))

            ;; Add modified_on column if it doesn't exist
            ;; Note: ALTER TABLE only allows literal defaults, not expressions
            (when-not (contains? existing-columns "modified_on")
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD COLUMN modified_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                                 table)])
              (swap! results update :added-columns inc))

            ;; Add created_by column if it doesn't exist
            (when-not (contains? existing-columns "created_by")
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD COLUMN created_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                 table)])
              (swap! results update :added-columns inc))

            ;; Add created_on column if it doesn't exist
            ;; Note: ALTER TABLE only allows literal defaults, not expressions
            (when-not (contains? existing-columns "created_on")
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD COLUMN created_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                                 table)])
              (swap! results update :added-columns inc)))

          ;; Add trigger for UPDATE (auto-update modified_on)
          (execute! conn
                    [(format "DROP TRIGGER IF EXISTS %s"
                             trigger-name)])
          (execute! conn
                    [(format "CREATE TRIGGER %s
                              AFTER UPDATE ON \"%s\"
                              FOR EACH ROW
                              BEGIN
                                UPDATE \"%s\" SET modified_on = strftime('%%Y-%%m-%%d %%H:%%M:%%f', 'now') WHERE _eid = NEW._eid;
                              END"
                             trigger-name table table)])
          (swap! results update :added-triggers inc)

          ;; Add preserve trigger
          (execute! conn
                    [(format "DROP TRIGGER IF EXISTS %s"
                             preserve-trigger-name)])
          (execute! conn
                    [(format "CREATE TRIGGER %s
                              AFTER UPDATE ON \"%s\"
                              FOR EACH ROW
                              WHEN (NEW.created_by != OLD.created_by OR NEW.created_on != OLD.created_on)
                              BEGIN
                                UPDATE \"%s\" SET created_by = OLD.created_by, created_on = OLD.created_on WHERE _eid = NEW._eid;
                              END"
                             preserve-trigger-name table table)])
          (swap! results update :added-triggers inc)

          (log/infof "[Audit/SQLite] Added audit to table: %s" table)

          (catch Exception e
            (log/errorf e "[Audit/SQLite] Failed to add audit to table: %s" table)))))

    (synthigy.dataset/reload)
    (log/infof "[Audit/SQLite] Setup complete: %s" @results)
    @results))

;; ============================================================================
;; Component Version Registration (Patcho)
;; ============================================================================

(patch/current-version :synthigy.iam/audit "1.0.1")

(comment
  (patch/read-version *db* :synthigy.iam/audit))

;; ============================================================================
;; Version Patches (SQLite-specific)
;; ============================================================================

(patch/upgrade :synthigy.iam/audit
               "1.0.0"
               (log/info "[Audit/SQLite] Installing SQLite audit enhancement v1.0.0")
               ;; Add audit columns to all existing tables (migration only)
               (setup!))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (log/info "[Audit/SQLite] Upgrading to v1.0.1: Adding created_by and created_on fields")

               ;; Add new columns to all existing tables
               (setup!)

               ;; Backfill created_by from modified_by for existing records
               (log/info "[Audit/SQLite] Backfilling created_by from modified_by...")
               (let [entities (core/get-entities (deployed-model))
                     backfill-count (atom 0)]
                 (with-open [conn (jdbc/get-connection (:datasource *db*))]
                   (doseq [{:keys [name]
                            :as entity} entities
                           :let [table (entity->table-name entity)]]
                     (try
                       (let [{result :jdbc.next/update-count}
                             (execute-one! conn
                                           [(format "UPDATE \"%s\" SET created_by = modified_by WHERE created_by IS NULL"
                                                    table)])]
                         (def result result)
                         (swap! backfill-count + (or (first result) 0))
                         (log/debugf "[Audit/SQLite] Backfilled %s records in table: %s"
                                     (or (first result) 0) table))
                       (catch Exception e
                         (log/errorf e "[Audit/SQLite] Failed to backfill table: %s" table)))))
                 (log/infof "[Audit/SQLite] Backfilled %d total records with created_by" @backfill-count))

               (log/info "[Audit/SQLite] v1.0.1 upgrade complete"))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/audit
  {:depends-on [:synthigy/iam]
   :setup (fn []
            ;; One-time: Apply audit enhancement to all entity tables
            (log/info "[AUDIT] Setting up audit enhancement...")
            (setup!)
            (log/info "[AUDIT] Audit enhancement setup complete"))
   :start (fn [] (core/reload *db*))})
