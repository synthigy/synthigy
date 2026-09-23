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

(ns synthigy.db.cockroach
  "CockroachDB connection management and lifecycle.

  CRDB speaks the Postgres wire protocol so we reuse `org.postgresql.Driver`
  and `jdbc:postgresql://…` URLs. Differences from PG that this module
  papers over: `CREATE EXTENSION \"uuid-ossp\"` parses but is a no-op (UUID
  is built-in), and default port is 26257."
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
                     (.setConnectionInitSql "SET TIME ZONE 'UTC'")
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
  "Builds Cockroach instance from environment variables."
  []
  (let [host (env :cockroach-host "localhost")
        port (env :cockroach-port 26257)
        db   (env :cockroach-db   "synthigy")
        password (env :cockroach-password "")
        user (env :cockroach-user "root")
        data (hash-map :host host
                       :port port
                       :db db
                       :password password
                       :user user
                       :max-connections (Integer/parseInt (env :cockroach-pool-size "20")))]
    (check-connection-params data)
    data))

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
           ;; Release the patcho stores BEFORE closing the pool — see the same
           ;; comment in synthigy.db.postgres. They hold THIS datasource, and
           ;; `setup!` reads the lifecycle store before the next `:start` runs,
           ;; so a stale one wedges restart beyond recovery. nil (not a fresh
           ;; AtomStore) so `setup-complete?` isn't reported false and setup
           ;; doesn't re-run.
           (patch/set-store! nil)
           (lifecycle/set-store! nil)
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
  {:edn
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

;;; ============================================================================
;;; Dialect Protocol Implementation
;;; ============================================================================
;;; Same PG-wire dialect Postgres uses — CRDB speaks it natively.

(extend-type Cockroach
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


(comment
  (lifecycle/setup! :synthigy/database)
  (lifecycle/start! :synthigy/database))
