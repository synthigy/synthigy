(ns synthigy.iam.audit
  "SQLite PRINCIPAL-AWARE audit enhancement.

   Mirror of synthigy.iam.audit (postgres). Layered on the SQLite
   timestamp-only default established by
   `synthigy.dataset.sqlite.audit-enhancer`. Protocol swap is driven by
   the `:synthigy/audit` lifecycle module's :start/:stop hooks; not by
   ns load.

   See the postgres counterpart for the design rationale."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.dataset :refer [deployed-model deployed-entity]]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.enhance :as enhance]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sqlite :as dataset-sqlite]
    [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
    [synthigy.db :refer [*db*]]
    [synthigy.db.sql :refer [execute! execute-one!]]
    [synthigy.db.sqlite]  ; Load SQLite JDBCBackend implementation
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
;; SQLite-Specific Protocol Implementations
;; ============================================================================

(defn- table-columns
  "Set of column names on table; empty if table doesn't exist."
  [tx table]
  (try
    (->> (jdbc/execute! tx [(format "PRAGMA table_info(\"%s\")" table)])
         (map :name)
         (into #{}))
    (catch Exception _ #{})))

(defn- transform-audit-impl
  "Ensure audit columns + triggers on every audited entity table (SQLite).

  Idempotent: column existence is probed via PRAGMA before each ALTER, so
  re-running over already-audited tables is cheap. Triggers are always
  dropped + recreated to pick up trigger-body changes (e.g. enum/name
  drift).

  Called by:
    - deploy! per :new/entities (legacy path, still works)
    - deploy! at the end over ALL current entities (substrate reconcile),
      so flipping :audit ON for an existing entity actually adds columns
      + triggers without needing a one-off Patcho upgrade.

  Four audit fields:
    modified_by  - INTEGER FK to user(_eid) ON DELETE SET NULL
    modified_on  - TEXT, CURRENT_TIMESTAMP default, bumped by AFTER UPDATE trigger
    created_by   - INTEGER FK to user(_eid) ON DELETE SET NULL
    created_on   - TEXT, CURRENT_TIMESTAMP default, preserved on UPDATE"
  [_db tx entities]
  (log/info {:id ::transform-audit-starting :data {:count (count entities)}}
            "Reconciling audit columns + triggers")

  (doseq [{:keys [name]
           :as entity} entities
          :let [table (entity->table-name entity)
                modified? (core/audit-modified? entity)
                created? (core/audit-created? entity)
                cols (when (or modified? created?) (table-columns tx table))]
          :when (or modified? created?)]

    (when modified?
      (when-not (contains? cols "modified_by")
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN modified_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                           table)]))
      (when-not (contains? cols "modified_on")
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN modified_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                           table)]))
      (let [trigger-name (str "update_" (normalize-name name) "_modified_on")]
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
                           trigger-name table table)])))

    (when created?
      (when-not (contains? cols "created_by")
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN created_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                           table)]))
      (when-not (contains? cols "created_on")
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN created_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                           table)]))
      (let [preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]
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
                           preserve-trigger-name table table)])))

    (log/debug {:id ::table-audited
                :data {:table table :modified modified? :created created?}}
               "Reconciled audit on table")))

(defn- augment-schema-impl
  "Returns audit field and relation definitions for SQLite runtime schema.
  Only includes fields enabled in entity's audit config."
  [db entity]
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
;; Protocol Extension (Top-Level - Runs at Namespace Load)
;; ============================================================================

(defn install-principal-aware!
  "Re-extend `AuditEnhancement` against SQLite with principal-aware impl."
  []
  (extend-protocol enhance/AuditEnhancement
    synthigy.db.SQLite
    (transform-audit [db tx entities] (transform-audit-impl db tx entities))
    (augment-schema  [db entity]      (augment-schema-impl db entity))
    (audit           [db entity-id data tx] (enhance-audit-data entity-id data))))

(defn uninstall-principal-aware!
  "Revert SQLite to the timestamp-only default installed by
   synthigy.dataset.sqlite at ns load."
  []
  (dataset-sqlite/install-default-audit-enhancement!))

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
  (log/info {:id ::setup-starting}
            "Adding audit columns to existing entity tables")

  (let [db *db*
        model (synthigy.dataset/deployed-model)
        entities (core/get-entities model)
        results (atom {:added-columns 0
                       :added-triggers 0})]

    (with-open [conn (jdbc/get-connection (:datasource db))]
      (doseq [{:keys [name]
               :as entity} entities
              :let [table (entity->table-name entity)
                    modified? (core/audit-modified? entity)
                    created? (core/audit-created? entity)]
              :when (or modified? created?)]

        (try
          (let [existing-columns (try
                                   (set (map :name (jdbc/execute! conn
                                                                  [(format "PRAGMA table_info(\"%s\")" table)])))
                                   (catch Exception _ #{}))]

            (when modified?
              (when-not (contains? existing-columns "modified_by")
                (execute! conn
                          [(format "ALTER TABLE \"%s\" ADD COLUMN modified_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                   table)])
                (swap! results update :added-columns inc))

              (when-not (contains? existing-columns "modified_on")
                (execute! conn
                          [(format "ALTER TABLE \"%s\" ADD COLUMN modified_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                                   table)])
                (swap! results update :added-columns inc))

              (let [trigger-name (str "update_" (normalize-name name) "_modified_on")]
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
                (swap! results update :added-triggers inc)))

            (when created?
              (when-not (contains? existing-columns "created_by")
                (execute! conn
                          [(format "ALTER TABLE \"%s\" ADD COLUMN created_by INTEGER REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                   table)])
                (swap! results update :added-columns inc))

              (when-not (contains? existing-columns "created_on")
                (execute! conn
                          [(format "ALTER TABLE \"%s\" ADD COLUMN created_on TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
                                   table)])
                (swap! results update :added-columns inc))

              (let [preserve-trigger-name (str "preserve_" (normalize-name name) "_created_audit")]
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
                (swap! results update :added-triggers inc))))

          (log/info {:id ::table-setup-complete
                     :data {:table table :modified modified? :created created?}}
                    "Added audit to table")

          (catch Exception e
            (log/error! {:id ::table-setup-failed :data {:table table}} e)))))

    (synthigy.dataset/reload)
    (log/info {:id ::setup-complete :data @results} "Setup complete")
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
               (log/info {:id ::installing-v100 :data {:action :installing :subject :iam-audit :version "1.0.0"}}
                         "Installing audit enhancement v1.0.0")
               ;; Add audit columns to all existing tables (migration only)
               (setup!))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (log/info {:id ::upgrading-v101 :data {:action :upgrading :subject :iam-audit :version "1.0.1"}}
                         "Upgrading to v1.0.1: adding created_by and created_on fields")

               ;; Add new columns to all existing tables
               (setup!)

               ;; Backfill created_by from modified_by for existing records
               (log/info {:id ::backfilling-created-by :data {:action :migrating :subject :created-by}}
                         "Backfilling created_by from modified_by")
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
                         (log/debug {:id ::table-backfilled
                                     :data {:rows (or (first result) 0) :table table}}
                                    "Backfilled records"))
                       (catch Exception e
                         (log/error! {:id ::table-backfill-failed :data {:table table}} e)))))
                 (log/info {:id ::backfill-complete :data {:action :migrated :subject :created-by :total @backfill-count}}
                           "Backfilled total records with created_by"))

               (log/info {:id ::v101-complete :data {:action :upgraded :subject :iam-audit :version "1.0.1"}} "v1.0.1 upgrade complete"))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/audit
  {:depends-on [:synthigy/iam]
   :doc "Audit trail — principal _by columns + change capture"
   :setup (fn []
            ;; One-time: retrofit `_by` columns onto any tables that
            ;; lack them (e.g. tables created in a previous bare session).
            (log/info {:id ::lifecycle-setup-starting :data {:action :setup :subject :audit-fields}}
                      "Retrofitting principal-aware audit columns (SQLite)")
            (setup!)
            (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete :subject :audit-fields}}
                      "SQLite audit retrofit complete"))
   :start (fn []
            (install-principal-aware!)
            (log/info {:id ::lifecycle-started :data {:action :started :subject :principal-audit}}
                      "Principal-aware audit enhancement active (SQLite)")
            (core/reload *db*))
   :stop (fn []
           (uninstall-principal-aware!)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped :subject :principal-audit}}
                     "Reverted to timestamp-only audit enhancement (SQLite)"))})
