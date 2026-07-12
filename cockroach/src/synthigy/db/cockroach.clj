(ns synthigy.db.cockroach
  "CockroachDB connection management and lifecycle.

  CRDB speaks the Postgres wire protocol so we reuse `org.postgresql.Driver`
  and `jdbc:postgresql://…` URLs. Differences from PG that this module
  papers over: `pg_terminate_backend` doesn't exist (clear-connections
  becomes a best-effort no-op), `CREATE EXTENSION \"uuid-ossp\"` parses
  but is a no-op (UUID is built-in), and default port is 26257."
  (:require
    [camel-snake-kebab.core :as csk]
    [clojure.string :as str]
    [environ.core :refer [env]]
    [next.jdbc :as jdbc]
    [next.jdbc.quoted :refer [postgres]]
    [next.jdbc.result-set :as rs]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.db :as db]
    [synthigy.db.sql :as sql :refer [execute-one!]]
    [synthigy.log :as log])
  (:import
    [com.zaxxer.hikari HikariDataSource]
    [java.sql ResultSet ResultSetMetaData]
    [org.postgresql.util PGobject]
    [synthigy.db Cockroach]))

(defn cockroach-connected? [datasource] (when datasource (not (.isClosed datasource))))

(defn connect
  "Connects to CockroachDB and returns HikariDataSource instance"
  [{:keys [host port user db password max-connections]
    :or {max-connections 2}
    :as data}]
  (let [url (str "jdbc:postgresql://" host \: port \/ db)
        datasource (doto
                     (HikariDataSource.)
                     (.setDriverClassName "org.postgresql.Driver")
                     (.setJdbcUrl url)
                     (.setUsername user)
                     (.setPassword password)
                     (.setLeakDetectionThreshold 2000)
                     (.setInitializationFailTimeout 0)
                     (.addDataSourceProperty "connectionInitSql" "SET TIME ZONE 'UTC'")
                     (.setMaximumPoolSize max-connections)
                     (.setConnectionTestQuery "select 1")
                     (.setKeepaliveTime 5000)
                     (.setConnectionTimeout 30000)
                     (.setIdleTimeout 30000)
                     (.setValidationTimeout 5000))]
    (when-not (cockroach-connected? datasource)
      (throw (ex-info "Couldn't connect to CockroachDB" data)))
    (log/info {:id ::connected :data {:user user :url url}}
              "Connected to CockroachDB")
    (db/map->Cockroach (assoc data :datasource datasource))))

(defn clear-connections
  "No-op on CRDB — `pg_terminate_backend` doesn't exist and CRDB's
  DROP DATABASE handles active sessions itself. Kept for parity with
  the postgres backend so callers can target it uniformly."
  [_con _db-name]
  nil)

(defn check-connection-params
  [{:keys [host db user password]
    :as data}]
  (letfn [(check [x message]
            (when-not x (throw (ex-info message data))))]
    (check host "COCKROACH_HOST not specified")
    (check db "COCKROACH_DB not specified")
    (check user "COCKROACH_USER not specified")
    ;; Password is OPTIONAL on CRDB insecure clusters (default root user).
    ;; Skip the check; HikariCP accepts an empty password.
    (when-not user (check user "COCKROACH_USER not specified"))))

(defn from-env
  "Builds Cockroach instance from environment variables.
   Falls back to POSTGRES_* names when COCKROACH_* aren't set, since
   most operators reuse their PG envvars."
  []
  (let [host (env :cockroach-host (env :postgres-host "localhost"))
        port (env :cockroach-port (env :postgres-port 26257))
        db   (env :cockroach-db   (env :postgres-db   "synthigy"))
        password (env :cockroach-password (env :postgres-password ""))
        user (env :cockroach-user (env :postgres-user "root"))
        data (hash-map :host host
                       :port port
                       :db db
                       :password password
                       :user user
                       :max-connections (Integer/parseInt (env :hikari-max-pool-size "20")))]
    (check-connection-params data)
    data))

(defn admin-from-env
  "Builds Cockroach admin instance from environment variables. CRDB
  doesn't have a separate 'postgres' admin database — admin uses the
  same root user against `defaultdb`."
  []
  (let [host (env :cockroach-host (env :postgres-host "localhost"))
        port (env :cockroach-port (env :postgres-port 26257))
        admin-db (env :cockroach-admin-db (env :postgres-admin-db "defaultdb"))
        password (env :cockroach-admin-password (env :cockroach-password (env :postgres-password "")))
        user (env :cockroach-admin-user (env :cockroach-user (env :postgres-user "root")))
        data (hash-map :host host
                       :port port
                       :db admin-db
                       :password password
                       :user user
                       :max-connections (Integer/parseInt (env :hikari-max-pool-size "20")))]
    (check-connection-params data)
    data))

(defn create-db
  "Setup new database using admin account. Returns HikariDataSource to the new db.

  CRDB note: `CREATE EXTENSION \"uuid-ossp\"` parses on CRDB but is a no-op
  (UUID is built-in). We still issue it for parity with the PG backend so
  Postgres-shaped dataset code that calls `uuid_generate_v4()`-style fns
  fails fast at runtime rather than silently."
  [{:keys [host]
    :as admin} database-name]
  (let [admin-db (connect admin)]
    (log/info {:id ::creating-database :data {:database database-name :host host}}
              "Creating database")
    (try
      (with-open [connection (jdbc/get-connection (:datasource admin-db))]
        (jdbc/execute-one!
          connection
          [(format "create database %s" database-name)]))
      (let [db (connect (assoc admin :db database-name))]
        (try
          (with-open [connection (jdbc/get-connection (:datasource db))]
            (jdbc/execute-one!
              connection
              ["create extension \"uuid-ossp\""]))
          (catch Throwable ex
            (log/warn {:id ::uuid-ossp-failed :error ex}
                      "Couldn't create uuid-ossp extension (CRDB no-ops this; safe to ignore)")))
        (log/info {:id ::database-created :data {:database database-name :host host}}
                  "Database created")
        db)
      (catch Throwable ex
        (log/error! {:id ::create-database-failed :data {:database database-name}} ex)
        (throw ex))
      (finally
        (.close (:datasource admin-db))))))

(defn drop-db
  "Removes DB from CockroachDB cluster"
  [{:keys [host]
    :as admin} database]
  (log/info {:id ::dropping-database :data {:database database :host host}}
            "Dropping database")
  (let [admin (connect admin)]
    (try
      (with-open [con (:datasource admin)]
        (clear-connections con database)
        (jdbc/execute-one!
          con
          [(format "drop database if exists %s cascade" database)]))
      (finally
        (.close (:datasource admin)))))
  nil)

(defn backup
  "CRDB has its own BACKUP/RESTORE statements; the template-clone pattern
  used in the postgres backend doesn't apply. Phase A leaves this
  unimplemented."
  [_admin _database _backup]
  (throw (ex-info "synthigy.db.cockroach/backup not implemented — use CRDB's BACKUP statement" {})))

;;; ============================================================================
;;; Patcho VersionStore Implementation
;;; ============================================================================

(defn ensure-version-table!
  "Creates __component_versions__ table if it doesn't exist."
  [{:keys [datasource]}]
  (jdbc/execute-one!
    datasource
    ["CREATE TABLE IF NOT EXISTS __component_versions__ (
       id BIGSERIAL PRIMARY KEY,
       component TEXT NOT NULL UNIQUE,
       version TEXT NOT NULL,
       updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     )"]))

(extend-type Cockroach
  patch/VersionStore

  (read-version [db topic]
    (if-let [row (jdbc/execute-one!
                   (:datasource db)
                   ["SELECT version FROM __component_versions__ WHERE component = ? ORDER BY updated_at desc"
                    (str topic)])]
      (:__component_versions__/version row)
      "0"))

  (write-version [db topic version]
    (jdbc/execute-one!
      (:datasource db)
      ["INSERT INTO __component_versions__ (component, version, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP)
       ON CONFLICT (component)
       DO UPDATE SET version = EXCLUDED.version, updated_at = CURRENT_TIMESTAMP"
       (str topic)
       version])))

;;; ============================================================================
;;; Patcho LifecycleStore Implementation
;;; ============================================================================

(defn ensure-lifecycle-table!
  "Creates __lifecycle_state__ table if it doesn't exist."
  [{:keys [datasource]}]
  (jdbc/execute-one!
    datasource
    ["CREATE TABLE IF NOT EXISTS __lifecycle_state__ (
      topic TEXT PRIMARY KEY,
      setup_complete BOOLEAN DEFAULT FALSE,
      cleanup_complete BOOLEAN DEFAULT FALSE,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )"]))


(defonce connection-agent (agent {:running? true}))

(defn monitor-connection
  [{:keys [running? period]
    :or {running? true
         period 10000}
    :as data} database]
  (if-not running? data
          (do
            (when-not (cockroach-connected? (:datasource db/*db*))
              (try
                (when (nil? database)
                  (throw
                    (ex-info
                      "Database not specified"
                      data)))
                (when-let [db (connect database)]
                  (alter-var-root #'db/*db* (constantly db))
                  nil)
                (catch Throwable e
                  (log/error! {:id ::connect-failed} e))))
            (send-off *agent* monitor-connection database)
            (Thread/sleep period))))

(defn start-connection-monitor
  [database]
  (send-off connection-agent (fn [_] {:running? true
                                      :period 10000}))
  (send-off connection-agent monitor-connection database))

(defn stop-connection-monitor
  []
  (send-off connection-agent (fn [x] (assoc x :running? false)))
  (when-some [db db/*db*]
    (when (cockroach-connected? (:datasource db))
      (.close (:datasource db))
      (alter-var-root #'db/*db* (constantly nil)))))

(defn start
  "Initializes database connection and returns HikariDataSource instance"
  ([] (start (from-env)))
  ([database]
   (log/info {:id ::connecting :data (dissoc database :password)}
             "Connecting to Cockroach")
   (when-let [db (connect database)]
     (alter-var-root #'db/*db* (constantly db))
     (ensure-lifecycle-table! db)
     (ensure-version-table! db)
     nil)
   (start-connection-monitor database)))

(defn stop
  ([]
   (stop-connection-monitor)
   (alter-var-root #'db/*db* (constantly nil))))

(extend-type Cockroach
  lifecycle/LifecycleStore

  (read-lifecycle-state [db topic]
    (if-let [row (jdbc/execute-one!
                   (:datasource db)
                   ["SELECT setup_complete, cleanup_complete
                    FROM __lifecycle_state__
                    WHERE topic = ?"
                    (name topic)])]
      {:setup-complete? (:__lifecycle_state__/setup_complete row)
       :cleanup-complete? (:__lifecycle_state__/cleanup_complete row)}
      {:setup-complete? false
       :cleanup-complete? false}))

  (write-lifecycle-state [db topic state]
    (jdbc/execute-one!
      (:datasource db)
      ["INSERT INTO __lifecycle_state__ (topic, setup_complete, cleanup_complete, updated_at)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP)
       ON CONFLICT (topic)
       DO UPDATE SET
         setup_complete = EXCLUDED.setup_complete,
         cleanup_complete = EXCLUDED.cleanup_complete,
         updated_at = CURRENT_TIMESTAMP"
       (name topic)
       (:setup-complete? state)
       (:cleanup-complete? state)])))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/database
  {:depends-on [:synthigy/transit]
   :doc "JDBC pool + DB provisioning (CockroachDB)"
   :setup (fn []
            (let [admin (admin-from-env)
                  config (from-env)
                  db-name (:db config)]
              (log/info {:id ::backend-starting :data {:action :starting :subject :db-backend :database db-name}}
                        "Setting up database")

              (let [db (try
                         (create-db admin db-name)
                         (catch Exception e
                           (if (re-find #"already exists" (.getMessage e))
                             (do
                               (log/info {:id ::database-exists :data {:database db-name}}
                                         "Database already exists, connecting")
                               (connect config))
                             (throw e))))]

                (log/info {:id ::creating-patcho-tables}
                          "Creating patcho tables")
                (ensure-lifecycle-table! db)
                (ensure-version-table! db)

                (alter-var-root #'db/*db* (constantly db))
                (patch/set-store! db)
                (lifecycle/set-store! db)

                (log/info {:id ::backend-started :data {:action :started :subject :db-backend :database db-name}}
                          "Setup complete"))))

   :cleanup (fn []
              (let [admin (admin-from-env)
                    config (from-env)]
                (log/info {:id ::cleanup-starting :data {:database (:db config)}}
                          "Dropping database")
                (drop-db admin (:db config))
                (log/info {:id ::cleanup-complete :data {:database (:db config)}}
                          "Cleanup complete")))

   :start (fn []
            (log/info {:id ::lifecycle-starting :data {:action :starting}}
                      "Starting database connection")
            (when-not (cockroach-connected? (:datasource db/*db*))
              (start))
            (patch/set-store! db/*db*)
            (lifecycle/set-store! db/*db*)
            (log/info {:id ::lifecycle-started :data {:action :started}}
                      "Database connection started"))

   :stop (fn []
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}}
                     "Stopping database connection")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}}
                     "Database connection stopped"))})

;;; ============================================================================
;;; JDBCBackend Protocol Implementation
;;; ============================================================================

(defn- cockroach-result-builder
  "Creates CockroachDB result set builder function. Same shape as PG —
  CRDB returns PG-wire-compatible result sets."
  [_return-type]
  (rs/as-maps-adapter
    rs/as-unqualified-modified-maps
    (fn [^ResultSet rs ^ResultSetMetaData _rsmeta ^Integer i]
      (.getObject rs i))))

(def ^:private defaults
  "CockroachDB next.jdbc options for each return type. The PG identifier
  quoter from next.jdbc.quoted works on CRDB unchanged (same PG-wire
  identifier syntax)."
  {:graphql
   {:builder-fn (cockroach-result-builder :graphql)
    :table-fn postgres
    :label-fn str/lower-case
    :qualifier-fn name
    :column-fn postgres}

   :edn
   {:builder-fn (cockroach-result-builder :edn)
    :table-fn postgres
    :label-fn (fn [w]
                (let [special (re-find #"^_+" w)]
                  (keyword (str special (csk/->kebab-case-string w)))))
    :qualifier-fn (comp str/lower-case name)
    :column-fn postgres}

   :raw
   {:builder-fn (cockroach-result-builder :raw)
    :table-fn postgres
    :label-fn identity
    :qualifier-fn name
    :column-fn postgres}})

(extend-type Cockroach
  sql/JDBCBackend

  (jdbc-options [_db return-type]
    (get defaults return-type (:raw defaults))))


(comment
  (lifecycle/setup! :synthigy/database)
  (lifecycle/start! :synthigy/database)
  (lifecycle/cleanup! :synthigy/database))
