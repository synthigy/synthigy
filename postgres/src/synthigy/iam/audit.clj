(ns synthigy.iam.audit
  "Postgres-specific audit enhancement implementation.

  Extends the dataset.enhance/AuditEnhancement protocol for Postgres databases.
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
    [synthigy.dataset.lacinia.enhance :as lacinia-enhance]
    [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
    [synthigy.db :refer [*db*]]
    [synthigy.db.postgres]  ; Load Postgres JDBCBackend implementation
    [synthigy.db.sql :refer [execute! execute-one!]]))

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
;; Postgres-Specific Protocol Implementations
;; ============================================================================

(defn- user-table-exists?
  "Check if the user table exists in the database."
  [tx]
  (let [result (execute-one! tx
                             ["SELECT COUNT(*) as cnt FROM information_schema.tables
                               WHERE table_name = 'user' AND table_schema = 'public'"])]
    (pos? (:cnt result 0))))

(defn- transform-audit-impl
  "Adds audit columns and triggers to newly created entity tables (Postgres).

  Adds four audit fields:
  - modified_by: User who last modified the record (FK to user, if user table exists)
  - modified_on: Timestamp of last modification
  - created_by: User who created the record (FK to user, if user table exists)
  - created_on: Timestamp of creation (immutable after INSERT)

  Uses Postgres-specific features:
  - PL/pgSQL trigger functions
  - bigint foreign key to user table (only if user table exists)
  - localtimestamp for automatic timestamps
  - Triggers to preserve created_by and created_on on UPDATE

  Note: If user table doesn't exist yet (e.g., during dataset meta-model setup),
  the modified_by and created_by columns are added without FK constraints.
  FK constraints can be added later when IAM is set up."
  [_ tx entities]
  (let [has-user-table (user-table-exists? tx)]
    (log/infof "[Audit/Postgres] Adding audit columns and triggers to new entity tables (user table: %s)"
               (if has-user-table "exists" "not yet created"))

    ;; Create trigger function (once, idempotent)
    (execute! tx
              ["CREATE OR REPLACE FUNCTION update_modified_on()
                RETURNS TRIGGER AS $$
                BEGIN
                  NEW.modified_on = localtimestamp;
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql"])

    ;; Create trigger function to preserve created_* fields (once, idempotent)
    (execute! tx
              ["CREATE OR REPLACE FUNCTION preserve_created_audit()
                RETURNS TRIGGER AS $$
                BEGIN
                  -- Preserve created_by: use old value if exists, otherwise new
                  IF TG_OP = 'UPDATE' THEN
                    NEW.created_by = COALESCE(OLD.created_by, NEW.created_by);
                    -- Always preserve created_on (immutable after creation)
                    NEW.created_on = OLD.created_on;
                  END IF;
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql"])

    (doseq [{:keys [name]
             :as entity} entities
            :let [table (entity->table-name entity)
                  trigger-name (str "update_" (normalize-name name) "_modified_on")]]

      ;; Add modified_by column (with FK only if user table exists)
      (execute! tx
                [(if has-user-table
                   (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                           table)
                   (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint"
                           table))])

      ;; Add modified_on column (auto-set on INSERT)
      (execute! tx
                [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp"
                         table)])

      ;; Add created_by column (with FK only if user table exists)
      (execute! tx
                [(if has-user-table
                   (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                           table)
                   (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint"
                           table))])

      ;; Add created_on column (auto-set on INSERT, immutable)
      (execute! tx
                [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp"
                         table)])

      ;; Add trigger for UPDATE (auto-update modified_on)
      (execute! tx
                [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                         trigger-name table)])
      (execute! tx
                [(format "CREATE TRIGGER %s
                          BEFORE UPDATE ON \"%s\"
                          FOR EACH ROW
                          EXECUTE FUNCTION update_modified_on()"
                         trigger-name table)])

      ;; Add trigger to preserve created_* fields on UPDATE
      (let [preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]
        (execute! tx
                  [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                           preserve-trigger-name table)])
        (execute! tx
                  [(format "CREATE TRIGGER %s
                            BEFORE UPDATE ON \"%s\"
                            FOR EACH ROW
                            EXECUTE FUNCTION preserve_created_audit()"
                           preserve-trigger-name table)]))

      (log/debugf "[Audit/Postgres] Added audit columns and triggers to table: %s" table))))

(defn- augment-schema-impl
  "Returns audit field and relation definitions for Postgres runtime schema.

  Returns:
    {:fields {:modified_by {...} :modified_on {...} :created_by {...} :created_on {...}}
     :relations {:modified_by {...} :created_by {...}}}"
  [_ entity]
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
  synthigy.db.Postgres
  (transform-audit [db tx entities]
    (transform-audit-impl db tx entities))
  (augment-schema [db entity]
    (augment-schema-impl db entity))
  (audit [_ entity-id data _]
    (enhance-audit-data entity-id data)))

;; ============================================================================
;; Lacinia GraphQL Schema Enhancement (Postgres)
;; ============================================================================

(extend-protocol lacinia-enhance/SchemaEnhancement
  synthigy.db.Postgres
  (get-audit-fields [_ _]
    {:modified_by
     {:type 'User
      :description "User who last modified this record"}
     :modified_on
     {:type '(non-null Timestamp)
      :description "Timestamp when this record was last modified"}
     :created_by
     {:type 'User
      :description "User who created this record"}
     :created_on
     {:type '(non-null Timestamp)
      :description "Timestamp when this record was created"}})

  (get-search-operators [_ _]
    [{:name "modified_by" :type "user" :active true}
     {:name "modified_on" :type "timestamp" :active true}
     {:name "created_by" :type "user" :active true}
     {:name "created_on" :type "timestamp" :active true}])

  (get-order-operators [_ entity]
    [{:to (deployed-entity (id/entity :iam/user))
      :to-label "modified_by"
      :cardinality "o2o"}
     {:to (deployed-entity (id/entity :iam/user))
      :to-label "created_by"
      :cardinality "o2o"}])

  (get-query-args [_ _]
    [{:name "modified_on" :type "timestamp" :active true}
     {:name "modified_by" :type "user" :active true}
     {:name "created_on" :type "timestamp" :active true}
     {:name "created_by" :type "user" :active true}]))

;; ============================================================================
;; Migration Utilities
;; ============================================================================

(defn setup!
  "Adds audit columns to ALL existing entity tables in the Postgres database.

  Adds four audit fields to each entity table:
  - modified_by: User who last modified the record
  - modified_on: Timestamp of last modification
  - created_by: User who created the record (preserved on UPDATE)
  - created_on: Timestamp of creation (immutable after INSERT)

  This is a migration utility for databases that were created before
  audit enhancement was enabled.

  WARNING: This modifies the database schema. Use with caution.

  Args:
    db    - Postgres database instance
    model - ERD model containing entities

  Returns:
    {:added-columns count :added-triggers count}"
  [db model]
  (log/info "[Audit/Postgres] Adding audit columns to existing entity tables")

  (let [entities (core/get-entities model)
        results (atom {:added-columns 0
                       :added-triggers 0})]

    (with-open [conn (jdbc/get-connection (:datasource db))]
      ;; Create trigger function (idempotent)
      (execute! conn
                ["CREATE OR REPLACE FUNCTION update_modified_on()
                  RETURNS TRIGGER AS $$
                  BEGIN
                    NEW.modified_on = localtimestamp;
                    RETURN NEW;
                  END;
                  $$ LANGUAGE plpgsql"])

      ;; Create trigger function to preserve created_* fields (idempotent)
      (execute! conn
                ["CREATE OR REPLACE FUNCTION preserve_created_audit()
                  RETURNS TRIGGER AS $$
                  BEGIN
                    -- Preserve created_by: use old value if exists, otherwise new
                    IF TG_OP = 'UPDATE' THEN
                      NEW.created_by = COALESCE(OLD.created_by, NEW.created_by);
                      -- Always preserve created_on (immutable after creation)
                      NEW.created_on = OLD.created_on;
                    END IF;
                    RETURN NEW;
                  END;
                  $$ LANGUAGE plpgsql"])

      (doseq [{:keys [name]
               :as entity} entities
              :let [table (entity->table-name entity)
                    trigger-name (str "update_" (normalize-name name) "_modified_on")]]

        (try
          ;; Add modified_by column (without FK first, then add FK separately)
          (execute! conn
                    [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint"
                             table)])
          ;; Add FK constraint if not exists
          (try
            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD CONSTRAINT \"%s_modified_by_fkey\" FOREIGN KEY (modified_by) REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                               table table)])
            (catch Exception _ nil)) ; Constraint may already exist
          (swap! results update :added-columns inc)

          ;; Add modified_on column
          (execute! conn
                    [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp"
                             table)])
          (swap! results update :added-columns inc)

          ;; Add created_by column (without FK first, then add FK separately)
          (execute! conn
                    [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint"
                             table)])
          ;; Add FK constraint if not exists
          (try
            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD CONSTRAINT \"%s_created_by_fkey\" FOREIGN KEY (created_by) REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                               table table)])
            (catch Exception _ nil)) ; Constraint may already exist
          (swap! results update :added-columns inc)

          ;; Add created_on column
          (execute! conn
                    [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp"
                             table)])
          (swap! results update :added-columns inc)

          ;; Add trigger
          (execute! conn
                    [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                             trigger-name table)])
          (execute! conn
                    [(format "CREATE TRIGGER %s
                              BEFORE UPDATE ON \"%s\"
                              FOR EACH ROW
                              EXECUTE FUNCTION update_modified_on()"
                             trigger-name table)])
          (swap! results update :added-triggers inc)

          ;; Add preserve trigger
          (let [preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]
            (execute! conn
                      [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                               preserve-trigger-name table)])
            (execute! conn
                      [(format "CREATE TRIGGER %s
                                BEFORE UPDATE ON \"%s\"
                                FOR EACH ROW
                                EXECUTE FUNCTION preserve_created_audit()"
                               preserve-trigger-name table)])
            (swap! results update :added-triggers inc))

          (log/infof "[Audit/Postgres] Added audit to table: %s" table)

          (catch Exception e
            (log/errorf e "[Audit/Postgres] Failed to add audit to table: %s" table)))))

    (log/infof "[Audit/Postgres] Setup complete: %s" @results)
    @results))

;; ============================================================================
;; Component Version Registration (Patcho)
;; ============================================================================

(patch/current-version :synthigy.iam/audit "1.0.2")


(comment
  (patch/read-version *db* :synthigy.iam/audit))

;; ============================================================================
;; Version Patches (Postgres-specific)
;; ============================================================================

(patch/upgrade :synthigy.iam/audit
               "1.0.0"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info "[Audit/Postgres] Installing Postgres audit enhancement v1.0.0")
                 ;; Add audit columns to all existing tables (migration only)
                 (setup! *db* (deployed-model))))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info "[Audit/Postgres] Upgrading to v1.0.1: Adding created_by and created_on fields")

                 ;; Add new columns to all existing tables
                 (setup! *db* (deployed-model))

                 ;; Backfill created_by from modified_by for existing records
                 (log/info "[Audit/Postgres] Backfilling created_by from modified_by...")
                 (let [entities (core/get-entities (deployed-model))
                       backfill-count (atom 0)]
                   (with-open [conn (jdbc/get-connection (:datasource *db*))]
                     (doseq [{:as entity} entities
                             :let [table (entity->table-name entity)]]
                       (try
                         (let [{result :jdbc.next/update-count}
                               (execute-one! conn
                                             [(format "UPDATE \"%s\" SET created_by = modified_by WHERE created_by IS NULL"
                                                      table)])]
                           (swap! backfill-count + (or (first result) 0))
                           (log/debugf "[Audit/Postgres] Backfilled %s records in table: %s"
                                       (or (first result) 0) table))
                         (catch Exception e
                           (log/errorf e "[Audit/Postgres] Failed to backfill table: %s" table)))))
                   (log/infof "[Audit/Postgres] Backfilled %d total records with created_by" @backfill-count))

                 (log/info "[Audit/Postgres] v1.0.1 upgrade complete")))

(patch/upgrade :synthigy.iam/audit
               "1.0.2"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info "[Audit/Postgres] Upgrading to v1.0.2: Adding FK constraints to audit columns")
                 ;; Re-run setup to add FK constraints to tables created before user table existed
                 (setup! *db* (deployed-model))
                 (log/info "[Audit/Postgres] v1.0.2 upgrade complete")))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/audit
  {:depends-on [:synthigy/iam]
   :setup (fn []
            ;; One-time: Apply audit enhancement to all entity tables
            (log/info "[AUDIT] Setting up audit enhancement...")
            (setup! *db* (deployed-model))
            (log/info "[AUDIT] Audit enhancement setup complete"))
   :start (fn [] (core/reload *db*))})
