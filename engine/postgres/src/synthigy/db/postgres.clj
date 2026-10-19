;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

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
   [synthigy.log :as log]
   [synthigy.timestamp :as ts])
  (:import
   [com.zaxxer.hikari HikariDataSource]
   [java.sql Connection DriverManager ResultSet ResultSetMetaData Types]
   [java.time LocalDateTime]
   [org.postgresql.util PGobject PSQLException ServerErrorMessage]
   [synthigy.db Postgres])
  (:gen-class))

(defn postgres-connected? [datasource] (when datasource (not (.isClosed datasource))))

(defn listen-connection
  "Raw off-pool connection for LISTEN/NOTIFY; the caller must close it."
  ^Connection [^HikariDataSource ds]
  (DriverManager/getConnection (.getJdbcUrl ds) (.getUsername ds) (.getPassword ds)))

(defn build-drainer-pool
  "Dedicated Hikari pool for the plug drainer (POSTGRES_DRAINER_POOL_SIZE, default 2)."
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
                     (.setKeepaliveTime 30000)
                     (.setConnectionTimeout 30000)
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

(defn ensure-version-table!
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

(defn ensure-lifecycle-table!
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

  ;; never persist :started? — it is runtime-only, so :start runs fresh on every JVM
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

(defn ensure-login-page-table!
  [{:keys [datasource]}]
  (jdbc/execute-one!
   datasource
   ["CREATE TABLE IF NOT EXISTS __login_pages__ (
      name TEXT NOT NULL,
      version INTEGER NOT NULL,
      zip BYTEA NOT NULL,
      digest TEXT NOT NULL,
      size INTEGER NOT NULL,
      uploaded_by TEXT,
      uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      active BOOLEAN NOT NULL DEFAULT FALSE,
      PRIMARY KEY (name, version)
    )"]))

(extend-type Postgres
  db/LoginPageStore

  (put-login-page [db name zip digest uploaded-by]
    (ensure-login-page-table! db)
    (:version
     (jdbc/execute-one!
      (:datasource db)
      ["INSERT INTO __login_pages__ (name, version, zip, digest, size, uploaded_by)
        SELECT ?, COALESCE(MAX(version), 0) + 1, ?, ?, ?, ?
        FROM __login_pages__ WHERE name = ?
        RETURNING version"
       name zip digest (alength ^bytes zip) uploaded-by name]
      {:builder-fn rs/as-unqualified-kebab-maps})))

  (login-pages [db]
    (ensure-login-page-table! db)
    (jdbc/execute!
     (:datasource db)
     ["SELECT name, version, digest, size, uploaded_by, uploaded_at, active
       FROM __login_pages__ ORDER BY name, version DESC"]
     {:builder-fn rs/as-unqualified-kebab-maps}))

  (login-page-zip [db name version]
    (ensure-login-page-table! db)
    (:zip
     (jdbc/execute-one!
      (:datasource db)
      ["SELECT zip FROM __login_pages__ WHERE name = ? AND version = ?" name version]
      {:builder-fn rs/as-unqualified-kebab-maps})))

  (active-login-pages [db]
    (ensure-login-page-table! db)
    (into {}
          (map (juxt :name #(select-keys % [:version :digest])))
          (jdbc/execute!
           (:datasource db)
           ["SELECT name, version, digest FROM __login_pages__ WHERE active"]
           {:builder-fn rs/as-unqualified-kebab-maps})))

  (activate-login-page [db name version]
    (ensure-login-page-table! db)
    (jdbc/execute-one!
     (:datasource db)
     ["UPDATE __login_pages__ SET active = COALESCE(version = ?, FALSE) WHERE name = ?"
      version name])
    nil)

  (delete-login-page [db name version]
    (ensure-login-page-table! db)
    (jdbc/execute-one!
     (:datasource db)
     ["DELETE FROM __login_pages__ WHERE name = ? AND version = ?" name version])
    nil))

(lifecycle/register-module!
 :synthigy/database
 {:depends-on [:synthigy/transit]
  :doc "JDBC pool + DB provisioning (PostgreSQL)"
  :setup (fn []
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
           (log/info {:id ::lifecycle-starting :data {:action :starting}}
                     "Starting database connection")
           (when-not (postgres-connected? (:datasource db/*db*))
             (start))
           (patch/set-store! db/*db*)
           (lifecycle/set-store! db/*db*)
           (log/info {:id ::lifecycle-started :data {:action :started}}
                     "Database connection started"))

  :stop (fn []
          (log/info {:id ::lifecycle-stopping :data {:action :stopping}}
                    "Stopping database connection")
          ;; must nil the stores BEFORE closing the pool — never a fresh AtomStore (re-runs setup)
          (patch/set-store! nil)
          (lifecycle/set-store! nil)
          (stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}}
                    "Database connection stopped"))})

(defn utc-timestamp-columns
  "Indexes of zone-less timestamp columns in a result set."
  [^ResultSetMetaData md]
  (set (filter #(and (= Types/TIMESTAMP (.getColumnType md %))
                     (= "timestamp" (.getColumnTypeName md %)))
               (range 1 (inc (.getColumnCount md))))))

(defn postgres-result-builder
  "next.jdbc builder that reads zone-less timestamp columns as UTC instants."
  [_return-type]
  (fn [^ResultSet rs opts]
    (let [utc? (utc-timestamp-columns (.getMetaData rs))]
      ((rs/as-maps-adapter
        rs/as-unqualified-modified-maps
        (fn [^ResultSet rs _ ^Integer i]
          (if (utc? i)
            (some-> (.getObject rs i LocalDateTime) ts/->date)
            (.getObject rs i))))
       rs opts))))

(def ^:private defaults
  "next.jdbc options per return type: :edn kebab-case keywords, :raw original column names."
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
