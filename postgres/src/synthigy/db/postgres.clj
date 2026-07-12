(ns synthigy.db.postgres
  "PostgreSQL connection management and lifecycle"
  (:require
    [camel-snake-kebab.core :as csk]
    ;; NOTE: Cheshire removed - using synthigy.json (jsonista) instead
    ;; buddy-core brings Cheshire as transitive dep but we don't use it directly
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
    [java.sql Connection DriverManager ResultSet ResultSetMetaData]
    [org.postgresql.util PGobject]
    [synthigy.db Postgres])
  (:gen-class))

(defn postgres-connected? [datasource] (when datasource (not (.isClosed datasource))))

(defn listen-connection
  "Open a dedicated raw connection for LISTEN/NOTIFY, bypassing the Hikari pool.

   A LISTEN connection is held for the whole process lifetime (pgjdbc requires
   the same physical connection to receive notifications). Drawing it from a
   Hikari pool would permanently pin a pool slot and — because Hikari can't tell
   an intentionally long-lived connection from a leaked one — trip its leak
   detector on every boot. So cache-coherence listeners (substrate wake, IAM
   connector chain) take a raw `DriverManager` connection instead.

   Pulls the JDBC coordinates straight off the configured `HikariDataSource`.
   Caller owns the returned connection and must close it on teardown."
  ^Connection [^HikariDataSource ds]
  (DriverManager/getConnection (.getJdbcUrl ds) (.getUsername ds) (.getPassword ds)))

(defn- build-drainer-pool
  "Build a small, dedicated Hikari pool for the substrate drainer + wake
   source so they never compete with the writer pool for connections.

   Sizing: default 2 conns for SKIP LOCKED drain transactions. The
   PostgresNotify LISTEN no longer lives here — it runs off-pool via
   `listen-connection` — so the pool is purely the drainer's working set.
   Override via POSTGRES_DRAINER_POOL_SIZE."
  [{:keys [host port user db password]
    :as data}]
  (let [size (Integer/parseInt (env :postgres-drainer-pool-size "2"))
        url  (str "jdbc:postgresql://" host \: port \/ db)
        ds   (doto (HikariDataSource.)
               (.setDriverClassName "org.postgresql.Driver")
               (.setJdbcUrl url)
               (.setUsername user)
               (.setPassword password)
               (.setPoolName "synthigy-drainer")
               (.setLeakDetectionThreshold 5000)
               (.setInitializationFailTimeout 0)
               (.addDataSourceProperty "connectionInitSql" "SET TIME ZONE 'UTC'")
               (.setMaximumPoolSize size)
               (.setMinimumIdle 1)
               (.setConnectionTestQuery "select 1")
               ;; Hikari ignores keepaliveTime < 30s; 30s is the floor.
               (.setKeepaliveTime 30000)
               (.setConnectionTimeout 30000)
               (.setIdleTimeout 60000)
               (.setValidationTimeout 5000))]
    (when-not (postgres-connected? ds)
      (throw (ex-info "Couldn't connect drainer pool" data)))
    (log/info {:id ::drainer-pool-connected
               :data {:action :started :subject :drainer-pool :size size}}
              "Drainer pool connected")
    ds))

(defn connect
  "Connects to PostgreSQL server and returns HikariDataSource instance"
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
                     ;; Hikari ignores keepaliveTime < 30s; 30s is the floor.
                     (.setKeepaliveTime 30000)
                     (.setConnectionTimeout 30000)
                     ;; No idleTimeout: minimumIdle defaults to maxPoolSize, so
                     ;; this is a fixed-size pool where idleTimeout has no effect
                     ;; (Hikari warns about it). Keep the pool warm instead.
                     (.setValidationTimeout 5000))]
    (when-not (postgres-connected? datasource)
      (throw (ex-info "Couldn't connect to Postgres" data)))
    (log/info {:id ::connected :data {:user user :url url}}
              "Connected to PostgresDB")
    (db/map->Postgres (assoc data :datasource datasource))))

(defn clear-connections
  "Terminates all connections to a PostgreSQL database.

  Uses PostgreSQL-specific pg_terminate_backend function to forcefully
  disconnect all active connections to the specified database.

  Args:
    con - JDBC connection (typically from admin database)
    db-name - Name of database to clear connections from

  Returns:
    Result of pg_terminate_backend query

  Example:
    (with-open [admin-con (jdbc/get-connection admin-datasource)]
      (clear-connections admin-con \"my_database\"))"
  [con db-name]
  (jdbc/execute-one!
    con
    [(str "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='" db-name "';")]))

(defn check-connection-params
  [{:keys [host db user password]
    :as data}]
  (letfn [(check [x message]
            (when-not x (throw (ex-info message data))))]
    (check host "POSTGRES_HOST not specified")
    (check db "POSTGRES_DB not specified")
    (check user "POSTGRES_USER not specified")
    (check password "POSTGRES_PASSWORD not specified")))

(defn from-env
  "Builds Postgres instance from environment variables"
  []
  (let [host (env :postgres-host "localhost")
        port (env :postgres-port 5432)
        db (env :postgres-db "synthigy")
        password (env :postgres-password "password")
        user (env :postgres-user "postgres")
        data (hash-map :host host
                       :port port
                       :db db
                       :password password
                       :user user
                       :max-connections (Integer/parseInt (env :hikari-max-pool-size "20")))]
    (check-connection-params data)
    data))

(defn admin-from-env
  "Builds Postgres instance from environment variables. Admin db should be
  used to create new db for new tenants"
  []
  (let [host (env :postgres-host "localhost")
        port (env :postgres-port 5432)
        admin-db (env :postgres-admin-db (env :postgres-db "postgres"))
        password (env :postgres-admin-password (env :postgres-password "password"))
        user (env :postgres-admin-user (env :postgres-user "postgres"))
        data (hash-map :host host
                       :port port
                       :db admin-db
                       :password password
                       :user user
                       :max-connections (Integer/parseInt (env :hikari-max-pool-size "20")))]
    (check-connection-params data)
    data))

(defn- db-exists?
  "True when a database named `database-name` already exists on the server."
  [datasource database-name]
  (with-open [connection (jdbc/get-connection datasource)]
    (some? (jdbc/execute-one!
             connection
             ["SELECT 1 FROM pg_database WHERE datname = ?" database-name]))))

(defn create-db
  "Ensure the database exists and return a HikariDataSource connection to it.

  Checks `pg_database` first: if the database already exists we just connect —
  no `CREATE DATABASE` attempt, so the common every-boot path doesn't raise
  (and log) a benign 'already exists' error. Only a genuinely fresh database
  triggers the CREATE + uuid-ossp extension setup."
  [{:keys [host]
    :as admin} database-name]
  (let [admin-db (connect admin)]
    (try
      (if (db-exists? (:datasource admin-db) database-name)
        (do
          (log/info {:id ::database-exists :data {:database database-name :host host}}
                    "Database already exists, connecting")
          (connect (assoc admin :db database-name)))
        (do
          (log/info {:id ::creating-database :data {:database database-name :host host}}
                    "Creating database")
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
                          "Couldn't create uuid-ossp extension")
                (throw ex)))
            (log/info {:id ::database-created :data {:database database-name :host host}}
                      "Database created")
            db)))
      (catch Throwable ex
        ;; Genuine failure (or a lost CREATE race against another booter) —
        ;; the caller's setup still guards "already exists" defensively.
        (log/error! {:id ::create-database-failed :data {:database database-name}} ex)
        (throw ex))
      (finally
        (.close (:datasource admin-db))))))

(defn drop-db
  "Removes DB from Postgres server"
  [{:keys [host]
    :as admin} database]
  (log/info {:id ::dropping-database :data {:database database :host host}}
            "Dropping database")
  (let [admin (connect admin)]
    (try
      (with-open [con (:datasource admin)]
        (clear-connections con database)
        ;; Raw next.jdbc — `synthigy.db.sql/execute-one!` dispatches on a
        ;; JDBCBackend and needs `*db*` bound; admin DDL runs with no `*db*`.
        (jdbc/execute-one!
          con
          [(format "drop database if exists %s" database)]))
      (finally
        (.close (:datasource admin)))))
  nil)

(defn backup
  "Function will connect to admin db and backup 'database' under 'backup' name"
  [admin database backup]
  (let [db (connect admin)]
    (with-open [con (jdbc/get-connection (:datasource db))]
      (jdbc/execute!
        con
        [(format "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='%s'" database)])
      (jdbc/execute!
        con
        [(format "create database %s with template '%s'" backup database)]))
    true))



;;; ============================================================================
;;; Patcho VersionStore Implementation
;;; ============================================================================

(defn ensure-version-table!
  "Creates __component_versions__ table if it doesn't exist.
   Called during setup and automatically by read-version and write-version."
  [{:keys [datasource]}]
  (jdbc/execute-one!
    datasource
    ["CREATE TABLE IF NOT EXISTS __component_versions__ (
       id BIGSERIAL PRIMARY KEY,
       component TEXT NOT NULL UNIQUE,
       version TEXT NOT NULL,
       updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     )"]))

(extend-type Postgres
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
  "Creates __lifecycle_state__ table if it doesn't exist.
   Called during setup and automatically by read-lifecycle-state and write-lifecycle-state."
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
            (when-not (postgres-connected? (:datasource db/*db*))
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
    (when-let [ds (:drainer-datasource db)]
      (when (postgres-connected? ds) (.close ds)))
    (when (postgres-connected? (:datasource db))
      (.close (:datasource db))
      (alter-var-root #'db/*db* (constantly nil)))))

(defn start
  "Initializes database connection and returns HikariDataSource instance"
  ([] (start (from-env)))
  ([database]
   (log/info {:id ::connecting :data (dissoc database :password)}
             "Connecting to Postgres")
   (when-let [db (connect database)]
     (let [drainer-ds (build-drainer-pool database)
           db         (assoc db :drainer-datasource drainer-ds)]
       (alter-var-root #'db/*db* (constantly db))
       (ensure-lifecycle-table! db)
       (ensure-version-table! db)
       nil))
   (start-connection-monitor database)))

(defn stop
  ([]
   (stop-connection-monitor)
   (alter-var-root #'db/*db* (constantly nil))))

(extend-type Postgres
  lifecycle/LifecycleStore

  ;; NOTE: Only :setup-complete? and :cleanup-complete? are persisted.
  ;; The :started? flag is RUNTIME-ONLY and must NOT be stored.
  ;; This ensures :start functions execute fresh on every JVM restart.

  (read-lifecycle-state [db topic]
    (if-let [row (jdbc/execute-one!
                   (:datasource db)
                   ["SELECT setup_complete, cleanup_complete
                    FROM __lifecycle_state__
                    WHERE topic = ?"
                    (name topic)])]
      ;; Return only persistent state - never include :started?
      {:setup-complete? (:__lifecycle_state__/setup_complete row)
       :cleanup-complete? (:__lifecycle_state__/cleanup_complete row)}
      {:setup-complete? false
       :cleanup-complete? false}))

  (write-lifecycle-state [db topic state]
    ;; Only persist setup/cleanup state - :started? is ignored (runtime-only)
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
   :doc "JDBC pool + DB provisioning (PostgreSQL)"
   :setup (fn []
            ;; One-time: Create database, patcho tables, and set stores
            (let [admin (admin-from-env)
                  config (from-env)
                  db-name (:db config)]
              (log/info {:id ::backend-starting :data {:action :starting :subject :db-backend :database db-name}}
                        "Setting up database")

              ;; Create database (or connect if exists)
              (let [db (try
                         (create-db admin db-name)
                         (catch Exception e
                           (if (re-find #"already exists" (.getMessage e))
                             (do
                               (log/info {:id ::database-exists :data {:database db-name}}
                                         "Database already exists, connecting")
                               (connect config))
                             (throw e))))]

                ;; Create patcho tables
                (log/info {:id ::creating-patcho-tables}
                          "Creating patcho tables")
                (ensure-lifecycle-table! db)
                (ensure-version-table! db)

                ;; Set *db* and stores so lifecycle can track setup completion
                (alter-var-root #'db/*db* (constantly db))
                (patch/set-store! db)
                (lifecycle/set-store! db)

                (log/info {:id ::backend-started :data {:action :started :subject :db-backend :database db-name}}
                          "Setup complete"))))

   :cleanup (fn []
              ;; One-time: Drop database (DESTRUCTIVE)
              (let [admin (admin-from-env)
                    config (from-env)]
                (log/info {:id ::cleanup-starting :data {:database (:db config)}}
                          "Dropping database")
                (drop-db admin (:db config))
                (log/info {:id ::cleanup-complete :data {:database (:db config)}}
                          "Cleanup complete")))

   :start (fn []
            ;; Runtime: Ensure connection pool and stores are set
            (log/info {:id ::lifecycle-starting :data {:action :starting}}
                      "Starting database connection")
            (when-not (postgres-connected? (:datasource db/*db*))
              (start))
            (patch/set-store! db/*db*)
            (lifecycle/set-store! db/*db*)
            (log/info {:id ::lifecycle-started :data {:action :started}}
                      "Database connection started"))

   :stop (fn []
           ;; Runtime: Close connections
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}}
                     "Stopping database connection")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}}
                     "Database connection stopped"))})

;;; ============================================================================
;;; JDBCBackend Protocol Implementation
;;; ============================================================================

(defn- postgres-result-builder
  "Creates PostgreSQL-specific result set builder function.

  Handles PostgreSQL-specific types:
  - jsonb: Decoded to Clojure maps/vectors
  - Other types: Pass through as-is

  Args:
    return-type - :graphql, :edn, or :raw

  Returns:
    next.jdbc builder-fn that processes ResultSet rows"
  [_return-type]
  (rs/as-maps-adapter
    rs/as-unqualified-modified-maps
    (fn [^ResultSet rs ^ResultSetMetaData _rsmeta ^Integer i]
      ;; Return raw values - JSON decoding handled by decoders in sql/query.clj
      (.getObject rs i))))

(def ^:private defaults
  "PostgreSQL-specific next.jdbc options for each return type.

  :graphql - String keys, lowercase column names
  :edn - Keyword keys, kebab-case column names
  :raw - String keys, original column names"
  {:graphql
   {:builder-fn (postgres-result-builder :graphql)
    :table-fn postgres
    :label-fn str/lower-case
    :qualifier-fn name
    :column-fn postgres}

   :edn
   {:builder-fn (postgres-result-builder :edn)
    :table-fn postgres
    :label-fn (fn [w]
                (let [special (re-find #"^_+" w)]
                  (keyword (str special (csk/->kebab-case-string w)))))
    :qualifier-fn (comp str/lower-case name)
    :column-fn postgres}

   :raw
   {:builder-fn (postgres-result-builder :raw)
    :table-fn postgres
    :label-fn identity
    :qualifier-fn name
    :column-fn postgres}})

(extend-type Postgres
  sql/JDBCBackend

  (jdbc-options [_db return-type]
    (get defaults return-type (:raw defaults))))


(comment
  (lifecycle/setup! :synthigy/database)
  (lifecycle/start! :synthigy/database)
  (lifecycle/cleanup! :synthigy/database))
