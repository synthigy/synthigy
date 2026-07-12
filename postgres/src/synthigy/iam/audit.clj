(ns synthigy.iam.audit
  "Postgres-specific PRINCIPAL-AWARE audit enhancement.

   Extends `synthigy.dataset.enhance/AuditEnhancement` with the
   `*_by` column variant + write-time fill from the bound `*principal*`.
   Layered on top of the timestamp-only default established by
   `synthigy.dataset.postgres.audit-enhancer` — that ns is loaded first
   from the postgres aggregator and provides the bare-mode behaviour.

   The protocol swap happens at LIFECYCLE BOUNDARIES, not ns load:
     :start  → re-extend with principal-aware impl + retrofit `_by`
               columns onto any tables that were created in bare mode.
     :stop   → re-extend with the timestamp-only default so the system
               degrades cleanly when IAM is stopped.

   This is what makes bare-server viable as a product: tables created
   without IAM still get `modified_on`/`created_on`; later starting IAM
   adds `_by` semantics without losing the timestamps.

   Patch Management:
   - Patcho topic: :synthigy.iam/audit
   - Patches are migration-only — protocol extension happens via the
     `:start` hook, not patches."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.dataset :refer [deployed-model deployed-entity]]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.enhance :as enhance]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.postgres :as dataset-postgres]
    [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
    [synthigy.db :refer [*db*]]
    [synthigy.db.postgres]  ; Load Postgres JDBCBackend implementation
    [synthigy.db.sql :refer [execute! execute-one!]]
    [synthigy.log :as log]))

;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn- enhance-audit-data
  "Populates audit values during mutations based on entity audit config.

  Only populates fields that are enabled in entity's :audit :actions config.

  Args:
    entity-id - Entity UUID being mutated
    data      - Mutation data structure (from analyze-data)

  Returns:
    Enhanced data with audit values populated for all records"
  [entity-id data]
  (let [entity (deployed-entity entity-id)
        current-user-eid (some-> (access/current-principal) :_eid)
        table (entity->table-name entity)]
    (if-not current-user-eid
      data
      (let [modified? (core/audit-modified? entity)
            created? (core/audit-created? entity)]
        (if-not (or modified? created?)
          data
          (update-in data [:entity table]
                     (fn [mapping]
                       (reduce-kv
                         (fn [data tmp-id _]
                           (cond-> data
                             modified? (assoc-in [tmp-id :modified_by] current-user-eid)
                             created? (assoc-in [tmp-id :created_by] current-user-eid)))
                         mapping
                         mapping))))))))

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
  (let [has-user-table (user-table-exists? tx)
        user-table-name-arg (if has-user-table "exists" "not yet created")]
    (log/info {:id ::transform-audit-starting
               :data {:user-table user-table-name-arg}}
              "Adding audit columns and triggers to new entity tables")

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
                  modified? (core/audit-modified? entity)
                  created? (core/audit-created? entity)]
            :when (or modified? created?)]

      ;; Modified audit columns + trigger
      (when modified?
        (execute! tx
                  [(if has-user-table
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                             table)
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint"
                             table))])
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp"
                           table)])
        (let [trigger-name (str "update_" (normalize-name name) "_modified_on")]
          (execute! tx
                    [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                             trigger-name table)])
          (execute! tx
                    [(format "CREATE TRIGGER %s
                              BEFORE UPDATE ON \"%s\"
                              FOR EACH ROW
                              EXECUTE FUNCTION update_modified_on()"
                             trigger-name table)])))

      ;; Created audit columns + trigger
      (when created?
        (execute! tx
                  [(if has-user-table
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                             table)
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint"
                             table))])
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp"
                           table)])
        (let [preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]
          (execute! tx
                    [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                             preserve-trigger-name table)])
          (execute! tx
                    [(format "CREATE TRIGGER %s
                              BEFORE UPDATE ON \"%s\"
                              FOR EACH ROW
                              EXECUTE FUNCTION preserve_created_audit()"
                             preserve-trigger-name table)])))

      (log/debug {:id ::table-audited
                  :data {:table table :modified modified? :created created?}}
                 "Added audit columns and triggers to table"))))

(defn- augment-schema-impl
  "Returns audit field and relation definitions for Postgres runtime schema.
  Only includes fields enabled in entity's audit config."
  [_ entity]
  (let [modified? (core/audit-modified? entity)
        created? (core/audit-created? entity)]
    (if-not (or modified? created?)
      {}
      (let [entity-id (id/extract entity)
            entity-table (entity->table-name entity)
            user-entity (core/reference-entity-uuid "user")
            user-table (when user-entity
                         (some-> (deployed-model)
                                 (core/get-entity user-entity)
                                 entity->table-name))]
        (cond-> {:fields {}}
          modified?
          (assoc-in [:fields :modified_on] {:key :modified_on :type "timestamp"})

          created?
          (assoc-in [:fields :created_on] {:key :created_on :type "timestamp"})

          (and modified? user-entity user-table)
          (-> (assoc-in [:fields :modified_by]
                        {:key :modified_by :type "user" :reference/entity user-entity})
              (assoc-in [:relations :modified_by]
                        {:from entity-id
                         :from/field :modified_by
                         :from/table entity-table
                         :to user-entity
                         :to/field :_eid
                         :to/table user-table
                         :table entity-table
                         :type :one}))

          (and created? user-entity user-table)
          (-> (assoc-in [:fields :created_by]
                        {:key :created_by :type "user" :reference/entity user-entity})
              (assoc-in [:relations :created_by]
                        {:from entity-id
                         :from/field :created_by
                         :from/table entity-table
                         :to user-entity
                         :to/field :_eid
                         :to/table user-table
                         :table entity-table
                         :type :one})))))))

;; ============================================================================
;; Protocol Swap — driven by lifecycle, not ns load
;; ============================================================================

(defn install-principal-aware!
  "Re-extend `AuditEnhancement` against Postgres with the principal-aware
   impl (adds `_by` columns + FK to user + write-time fill from
   `*principal*`). Idempotent; safe to call anywhere. Called by the
   `:synthigy/audit` module's `:start`."
  []
  (extend-protocol enhance/AuditEnhancement
    synthigy.db.Postgres
    (transform-audit [db tx entities] (transform-audit-impl db tx entities))
    (augment-schema  [db entity]      (augment-schema-impl db entity))
    (audit           [_ entity-id data _] (enhance-audit-data entity-id data))))

(defn uninstall-principal-aware!
  "Revert to the timestamp-only default that the dataset.postgres
   namespace installs at load. Called by `:stop`. New deploys get
   bare-mode audit semantics; previously-added `_by` columns stay
   (just no longer auto-populated)."
  []
  (dataset-postgres/install-default-audit-enhancement!))

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
  (log/info {:id ::setup-starting}
            "Adding audit columns to existing entity tables")

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
                    modified? (core/audit-modified? entity)
                    created? (core/audit-created? entity)]
              :when (or modified? created?)]

        (try
          (when modified?
            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint"
                               table)])
            (try
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD CONSTRAINT \"%s_modified_by_fkey\" FOREIGN KEY (modified_by) REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                 table table)])
              (catch Exception _ nil))
            (swap! results update :added-columns inc)

            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp"
                               table)])
            (swap! results update :added-columns inc)

            (let [trigger-name (str "update_" (normalize-name name) "_modified_on")]
              (execute! conn
                        [(format "DROP TRIGGER IF EXISTS %s ON \"%s\""
                                 trigger-name table)])
              (execute! conn
                        [(format "CREATE TRIGGER %s
                                  BEFORE UPDATE ON \"%s\"
                                  FOR EACH ROW
                                  EXECUTE FUNCTION update_modified_on()"
                                 trigger-name table)])
              (swap! results update :added-triggers inc)))

          (when created?
            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint"
                               table)])
            (try
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD CONSTRAINT \"%s_created_by_fkey\" FOREIGN KEY (created_by) REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                 table table)])
              (catch Exception _ nil))
            (swap! results update :added-columns inc)

            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp"
                               table)])
            (swap! results update :added-columns inc)

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
              (swap! results update :added-triggers inc)))

          (log/info {:id ::table-setup-complete
                     :data {:table table :modified modified? :created created?}}
                    "Added audit to table")

          (catch Exception e
            (log/error! {:id ::table-setup-failed :data {:table table}} e)))))

    (log/info {:id ::setup-complete :data @results} "Setup complete")
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
                 (log/info {:id ::installing-v100 :data {:action :installing :subject :iam-audit :version "1.0.0"}}
                           "Installing audit enhancement v1.0.0")
                 ;; Add audit columns to all existing tables (migration only)
                 (setup! *db* (deployed-model))))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info {:id ::upgrading-v101 :data {:action :upgrading :subject :iam-audit :version "1.0.1"}}
                           "Upgrading to v1.0.1: adding created_by and created_on fields")

                 ;; Add new columns to all existing tables
                 (setup! *db* (deployed-model))

                 ;; Backfill created_by from modified_by for existing records
                 (log/info {:id ::backfilling-created-by :data {:action :migrating :subject :created-by}}
                           "Backfilling created_by from modified_by")
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
                           (log/debug {:id ::table-backfilled
                                       :data {:rows (or (first result) 0) :table table}}
                                      "Backfilled records"))
                         (catch Exception e
                           (log/error! {:id ::table-backfill-failed :data {:table table}} e)))))
                   (log/info {:id ::backfill-complete :data {:action :migrated :subject :created-by :total @backfill-count}}
                             "Backfilled total records with created_by"))

                 (log/info {:id ::v101-complete :data {:action :upgraded :subject :iam-audit :version "1.0.1"}} "v1.0.1 upgrade complete")))

(patch/upgrade :synthigy.iam/audit
               "1.0.2"
               (when (instance? synthigy.db.Postgres *db*)
                 (log/info {:id ::upgrading-v102 :data {:action :upgrading :subject :iam-audit :version "1.0.2"}}
                           "Upgrading to v1.0.2: adding FK constraints to audit columns")
                 ;; Re-run setup to add FK constraints to tables created before user table existed
                 (setup! *db* (deployed-model))
                 (log/info {:id ::v102-complete :data {:action :upgraded :subject :iam-audit :version "1.0.2"}} "v1.0.2 upgrade complete")))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/audit
  {:depends-on [:synthigy/iam]
   :doc "Audit trail — principal `_by` columns + change capture"
   :setup (fn []
            ;; One-time: retrofit `_by` columns + FK constraints onto any
            ;; tables that exist but lack them (e.g. tables created in a
            ;; previous bare-mode session). Idempotent.
            (log/info {:id ::lifecycle-setup-starting :data {:action :setup :subject :audit-fields}}
                      "Retrofitting principal-aware audit columns onto existing tables")
            (setup! *db* (deployed-model))
            (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete :subject :audit-fields}}
                      "Audit retrofit complete"))
   :start (fn []
            ;; Swap the AuditEnhancement protocol from timestamp-only
            ;; (the default loaded at boot) to principal-aware. Every
            ;; subsequent deploy + write picks up the IAM-enriched impl.
            (install-principal-aware!)
            (log/info {:id ::lifecycle-started :data {:action :started :subject :principal-audit}}
                      "Principal-aware audit enhancement active")
            (core/reload *db*))
   :stop (fn []
           ;; Restore timestamp-only behaviour. Subsequent deploys add
           ;; only `modified_on`/`created_on` + their UPDATE triggers.
           (uninstall-principal-aware!)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped :subject :principal-audit}}
                     "Reverted to timestamp-only audit enhancement"))})
