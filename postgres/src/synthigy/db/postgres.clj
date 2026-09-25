;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.db.postgres
  "PostgreSQL connection management and lifecycle"
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
   [java.sql Connection DriverManager ResultSet ResultSetMetaData]
   [org.postgresql.util PGobject PSQLException ServerErrorMessage]
   [synthigy.db Postgres])
  (:gen-class))

(defn postgres-connected? [datasource] (when datasource (not (.isClosed datasource))))

(defn listen-connection
  "Open a dedicated raw connection for LISTEN/NOTIFY, bypassing the Hikari pool.

   A LISTEN connection is held for the whole process lifetime (pgjdbc requires
   the same physical connection to receive notifications). Drawing it from a
   Hikari pool would permanently pin a pool slot and — because Hikari can't tell
   an intentionally long-lived connection from a leaked one — trip its leak
   detector on every boot. So cache-coherence listeners (plug wake, IAM
   connector chain) take a raw `DriverManager` connection instead.

   Pulls the JDBC coordinates straight off the configured `HikariDataSource`.
   Caller owns the returned connection and must close it on teardown."
  ^Connection [^HikariDataSource ds]
  (DriverManager/getConnection (.getJdbcUrl ds) (.getUsername ds) (.getPassword ds)))

(defn- build-drainer-pool
  "Build a small, dedicated Hikari pool for the plug drainer + wake
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
               (.setConnectionInitSql "SET TIME ZONE 'UTC'")
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
                     (.setConnectionInitSql "SET TIME ZONE 'UTC'")
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
                       :max-connections (Integer/parseInt (env :postgres-pool-size "20")))]
    (check-connection-params data)
    data))

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
            ;; One-time: connect (the operator provisions the database
            ;; itself — see check-connection-params), create patcho tables,
            ;; set stores
           (let [config (from-env)
                 db-name (:db config)
                 db (connect config)]
             (log/info {:id ::backend-starting :data {:action :starting :subject :db-backend :database db-name}}
                       "Setting up database")
             (ensure-lifecycle-table! db)
             (ensure-version-table! db)
             (alter-var-root #'db/*db* (constantly db))
             (patch/set-store! db)
             (lifecycle/set-store! db)
             (log/info {:id ::backend-started :data {:action :started :subject :db-backend :database db-name}}
                       "Setup complete")))

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
           ;; Release the patcho stores BEFORE closing the pool — they hold
           ;; THIS datasource. Leaving them pointed at a closed pool wedges the
           ;; next `start!` beyond recovery: patcho reads the lifecycle store in
           ;; `setup!`, which runs BEFORE this module's `:start` fn, so nothing
           ;; downstream ever gets the chance to re-point them.
           ;;
           ;; nil, not a fresh AtomStore, is deliberate: an empty atom store
           ;; reports `setup-complete?` false and would RE-RUN setup (schema
           ;; creation). nil makes `start!` skip setup entirely, and `:start`
           ;; re-points both stores at the new pool.
          (patch/set-store! nil)
          (lifecycle/set-store! nil)
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
    return-type - :edn or :raw

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

  :edn - Keyword keys, kebab-case column names
  :raw - String keys, original column names"
  {:edn
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

;;; ============================================================================
;;; Dialect Protocol Implementation
;;; ============================================================================

(extend-type Postgres
  db/Dialect

  (json-param [_ s] (doto (PGobject.) (.setType "jsonb") (.setValue s)))
  (json-column [_ v] (if (instance? PGobject v) (.getValue ^PGobject v) v))

  (table-exists? [_ table]
    (boolean (:to_regclass (execute-one! ["SELECT to_regclass(?)" (str "public." table)]))))

  (column-exists? [_ table column]
    (boolean (execute-one!
              ["SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = ?"
               table column])))

  (ddl [_] {:serial-pk "SERIAL PRIMARY KEY" :json "jsonb" :now "now()"})
  (json-text [_ expr] (str "(" expr " #>> '{}')"))
  (json-get-text [_ expr k] (str expr "->>'" k "'"))
  (json-remove [_ expr k] (str expr " - '" k "'"))
  (cast-placeholder [_ type] (str "?::" type))
  (template-sql [_ raw-sql] raw-sql))

;;; ============================================================================
;;; Error Translation
;;; ============================================================================

(def sqlstate-codes
  {"23505" "UNIQUE_VIOLATION"
   "23502" "NOT_NULL_VIOLATION"
   "23503" "FK_VIOLATION"
   "23514" "CHECK_VIOLATION"
   "42804" "TYPE_MISMATCH"
   "42846" "TYPE_MISMATCH"
   "42P06" "SCHEMA_CONFLICT"
   "42P07" "SCHEMA_CONFLICT"
   "42701" "SCHEMA_CONFLICT"
   "42710" "SCHEMA_CONFLICT"
   "42P01" "SCHEMA_DRIFT"
   "42703" "SCHEMA_DRIFT"
   "42704" "SCHEMA_DRIFT"
   "2BP01" "DEPENDENT_OBJECTS"
   "55P03" "LOCKED"
   "40P01" "LOCKED"
   "40001" "LOCKED"
   "57014" "TIMEOUT"
   "P0001" "GUARD_VIOLATION"})

(def retryable-codes #{"LOCKED" "TIMEOUT" "DB_UNAVAILABLE"})

(defn sqlstate->code
  [state]
  (or (sqlstate-codes state)
      (when (and state (<= 2 (count state)))
        (case (subs state 0 2)
          "22" "INVALID_VALUE"
          "23" "CONSTRAINT_VIOLATION"
          ("08" "53" "57") "DB_UNAVAILABLE"
          nil))
      "DB_ERROR"))

(defn parse-key-detail
  "Parse a `Key (a, b)=(x, y) ...` detail into {:columns [...] :values [...]}."
  [detail]
  (when-let [[_ cols vals] (and detail (re-find #"^Key \((.+?)\)=\((.*)\)" detail))]
    (let [columns (mapv str/trim (str/split cols #","))
          values  (mapv str/trim (str/split vals #","))]
      {:columns columns
       :values  (if (= (count columns) (count values)) values [vals])})))

(defn translate-psql-exception
  [^PSQLException e]
  (let [state      (.getSQLState e)
        code       (sqlstate->code state)
        ^ServerErrorMessage m (.getServerErrorMessage e)
        primary    (or (some-> m .getMessage) (.getMessage e))
        table      (some-> m .getTable)
        constraint (some-> m .getConstraint)
        {:keys [columns values]} (parse-key-detail (some-> m .getDetail))
        columns    (or (not-empty columns) (some-> m .getColumn vector))
        target     (str table (when (= 1 (count columns)) (str "." (first columns))))
        message    (case code
                     "UNIQUE_VIOLATION"
                     (str target " must be unique"
                          (when (seq values) (str "; duplicate value " (str/join ", " values))))
                     "NOT_NULL_VIOLATION" (str target " is required")
                     "FK_VIOLATION"
                     (str "Referenced record does not exist"
                          (when (seq values) (str " (" target " = " (str/join ", " values) ")")))
                     "TIMEOUT"        "Database statement timed out"
                     "DB_UNAVAILABLE" "Database unavailable"
                     primary)]
    (ex-info message
             (cond-> {:code    code
                      :details (cond-> {:sqlstate state}
                                 table         (assoc :entity table)
                                 (seq columns) (assoc :attributes columns)
                                 (seq values)  (assoc :values values)
                                 constraint    (assoc :constraint constraint))}
               (some-> m .getHint) (assoc :hint (.getHint m))
               (retryable-codes code) (assoc :retryable true)))))

(extend-type Postgres
  db/Translator

  (translate-db-exception [_ e]
    (when (instance? PSQLException e)
      (translate-psql-exception e))))

(comment
  (lifecycle/setup! :synthigy/database)
  (lifecycle/start! :synthigy/database))
