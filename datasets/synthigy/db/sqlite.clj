(ns synthigy.db.sqlite
  "SQLite connection management and lifecycle"
  (:require
    [camel-snake-kebab.core :as csk]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.db :as db]
    [synthigy.db.sql :as sql]
    synthigy.env)
  (:import
    [com.zaxxer.hikari HikariDataSource]
    [java.sql ResultSet]
    [synthigy.db SQLite])
  (:gen-class))

;;; ============================================================================
;;; Connection Management
;;; ============================================================================

(defn connect
  "Connects to SQLite database file and returns HikariDataSource instance.

  Args:
    path - File path to SQLite database
    max-connections - Maximum connection pool size (default: 1 for SQLite)

  Returns:
    SQLite record with :path and :datasource (HikariDataSource)"
  [{:keys [path max-connections]
    :or {max-connections 1}  ; SQLite works best with single connection
    :as config}]
  (log/infof "[SQLITE] Connecting to: %s" path)
  (let [;; Add JDBC parameters to improve statement handling and concurrency
        jdbc-url (str "jdbc:sqlite:" path "?journal_mode=WAL&busy_timeout=30000")
        datasource (doto
                     (HikariDataSource.)
                     (.setDriverClassName "org.sqlite.JDBC")
                     (.setJdbcUrl jdbc-url)
                     (.setMaximumPoolSize max-connections)
                     (.setMinimumIdle 0)
                     (.setConnectionTimeout 30000)
                     (.setIdleTimeout 600000)
                     (.setMaxLifetime 1800000))]
    (try
      ;; Test connection
      (with-open [conn (jdbc/get-connection datasource)]
        (jdbc/execute-one! conn ["SELECT 1"]))
      (log/infof "[SQLITE] Connected to: %s (pool size: %d)" path max-connections)
      (db/map->SQLite (assoc config :datasource datasource))
      (catch Exception e
        (log/errorf e "[SQLITE] Failed to connect to: %s" path)
        (throw (ex-info "Couldn't connect to SQLite"
                        {:path path
                         :error (.getMessage e)}
                        e))))))

(defn from-env
  "Builds SQLite connection config from environment variables.

  Environment variables:
    SQLITE_PATH - Path to SQLite database file (default: synthigy.db)

  Returns:
    Config map with :path key"
  []
  (let [path (env :sqlite-path (str synthigy.env/home "/db/synthigy.db"))]
    (log/infof "[SQLITE] Database path from env: %s" path)
    {:path path}))

(defn start
  "Initializes SQLite database connection.

  Args:
    config - Optional config map with :path key

  Side effects:
    - Establishes database connection
    - Binds connection to synthigy.db/*db*
    - Sets up pragmas for better performance

  Returns:
    nil"
  ([] (start (from-env)))
  ([config]
   (when-not (instance? SQLite db/*db*)
     (log/info "[SQLITE] Starting SQLite backend...")
     (when-let [db (connect config)]
     ;; Set SQLite pragmas for better performance
       (try
         (with-open [conn (jdbc/get-connection (:datasource db))]
           (jdbc/execute! conn ["PRAGMA foreign_keys = ON"])
           (jdbc/execute! conn ["PRAGMA journal_mode = WAL"])
           (jdbc/execute! conn ["PRAGMA synchronous = NORMAL"]))
         (catch Exception e
           (log/warn e "[SQLITE] Failed to set pragmas")))

       (alter-var-root #'db/*db* (constantly db))
       (log/info "[SQLITE] SQLite backend started")
       nil))))

(defn stop
  "Stops SQLite database connection.

  Side effects:
    - Clears synthigy.db/*db*

  Returns:
    nil"
  []
  (log/info "[SQLITE] Stopping SQLite backend...")
  ; (alter-var-root #'db/*db* (constantly nil))
  (log/info "[SQLITE] SQLite backend stopped")
  nil)

(defn cleanup-files!
  "Deletes SQLite database and all associated files (WAL, SHM, journal).

  Args:
    path - File path to SQLite database (without suffix)

  Side effects:
    Deletes files: path, path-wal, path-shm, path-journal

  Returns:
    Map of {:deleted [...] :missing [...]}"
  [path]
  (log/infof "[SQLITE] Cleaning up database files: %s" path)
  (let [suffixes ["" "-wal" "-shm" "-journal"]
        results (reduce
                  (fn [acc suffix]
                    (let [f (io/file (str path suffix))]
                      (if (.exists f)
                        (do
                          (.delete f)
                          (update acc :deleted conj (str path suffix)))
                        (update acc :missing conj (str path suffix)))))
                  {:deleted []
                   :missing []}
                  suffixes)]
    (log/infof "[SQLITE] Cleanup complete: deleted %d files" (count (:deleted results)))
    results))

;;; ============================================================================
;;; Patcho VersionStore Implementation
;;; ============================================================================

(defn- ensure-version-table!
  "Creates __component_versions__ table if it doesn't exist.

  Args:
    db - SQLite database record

  Side effects:
    Creates table in database"
  [{:keys [datasource]}]
  (jdbc/execute-one!
    datasource
    ["CREATE TABLE IF NOT EXISTS __component_versions__ (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       component TEXT NOT NULL UNIQUE,
       version TEXT NOT NULL,
       updated_at TEXT DEFAULT CURRENT_TIMESTAMP
     )"]))

(extend-type SQLite
  patch/VersionStore

  (read-version [db topic]
    (ensure-version-table! db)
    (if-let [row (jdbc/execute-one!
                   (:datasource db)
                   ["SELECT version FROM __component_versions__
                     WHERE component = ?
                     ORDER BY updated_at DESC
                     LIMIT 1"
                    (str topic)])]
      (:__component_versions__/version row)
      "0"))

  (write-version [db topic version]
    (ensure-version-table! db)
    (jdbc/execute-one!
      (:datasource db)
      ["INSERT INTO __component_versions__ (component, version, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (component)
        DO UPDATE SET version = excluded.version, updated_at = CURRENT_TIMESTAMP"
       (str topic)
       version])))

;;; ============================================================================
;;; Patcho LifecycleStore Implementation
;;; ============================================================================

(defn- ensure-lifecycle-table!
  "Creates __lifecycle_state__ table if it doesn't exist.

  Args:
    db - SQLite database record

  Side effects:
    Creates table in database"
  [{:keys [datasource]}]
  (jdbc/execute-one!
    datasource
    ["CREATE TABLE IF NOT EXISTS __lifecycle_state__ (
       topic TEXT PRIMARY KEY,
       setup_complete INTEGER DEFAULT 0,
       cleanup_complete INTEGER DEFAULT 0,
       updated_at TEXT DEFAULT CURRENT_TIMESTAMP
     )"]))

(extend-type SQLite
  lifecycle/LifecycleStore

  ;; NOTE: Only :setup-complete? and :cleanup-complete? are persisted.
  ;; The :started? flag is RUNTIME-ONLY and must NOT be stored.
  ;; This ensures :start functions execute fresh on every JVM restart.

  (read-lifecycle-state [db topic]
    (ensure-lifecycle-table! db)
    (if-let [row (jdbc/execute-one!
                   (:datasource db)
                   ["SELECT setup_complete, cleanup_complete
                     FROM __lifecycle_state__
                     WHERE topic = ?"
                    (name topic)])]
      ;; Return only persistent state - never include :started?
      {:setup-complete? (= 1 (:__lifecycle_state__/setup_complete row))
       :cleanup-complete? (= 1 (:__lifecycle_state__/cleanup_complete row))}
      {:setup-complete? false
       :cleanup-complete? false}))

  (write-lifecycle-state [db topic state]
    ;; Only persist setup/cleanup state - :started? is ignored (runtime-only)
    (ensure-lifecycle-table! db)
    (jdbc/execute-one!
      (:datasource db)
      ["INSERT INTO __lifecycle_state__ (topic, setup_complete, cleanup_complete, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (topic)
        DO UPDATE SET
          setup_complete = excluded.setup_complete,
          cleanup_complete = excluded.cleanup_complete,
          updated_at = CURRENT_TIMESTAMP"
       (name topic)
       (if (:setup-complete? state) 1 0)
       (if (:cleanup-complete? state) 1 0)])))


(comment
  (lifecycle/setup! :synthigy/iam))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/database
  {:depends-on [:synthigy/transit]
   :setup (fn []
            ;; One-time: Create database file (SQLite auto-creates on connect)
            (log/info "[DATABASE] SQLite database will be created on first connection")
            (start)
            (lifecycle/set-store! db/*db*)
            (ensure-lifecycle-table! db/*db*))
   :cleanup (fn []
              ;; One-time: Delete database file (DESTRUCTIVE - parity with PostgreSQL)
              (let [{:keys [path datasource]} db/*db*]
                (log/infof "[DATABASE] SQLite cleanup - deleting database: %s" path)
                ;; Close datasource if open
                (when datasource
                  (try
                    (.close datasource)
                    (catch Exception e
                      (log/warn e "[DATABASE] Error closing datasource"))))
                ;; Delete database file
                (cleanup-files! path)
                (alter-var-root #'db/*db* (constantly nil))
                ;; Reset lifecycle store to fresh in-memory atom
                (lifecycle/reset-store!)))
   :start (fn []
            ;; Runtime: Connect to database
            (log/info "[DATABASE] Starting SQLite connection...")
            (start)
            (patch/set-store! db/*db*)
            (lifecycle/set-store! db/*db*)
            (log/info "[DATABASE] SQLite connection started"))
   :stop (fn []
           ;; Runtime: Close connections
           (log/info "[DATABASE] Stopping SQLite connection...")
           (stop)
           (log/info "[DATABASE] SQLite connection stopped"))})

;;; ============================================================================
;;; JDBCBackend Protocol Implementation
;;; ============================================================================

(defn- sqlite-result-builder
  "Creates SQLite-specific result set builder function.

  Returns raw values - JSON decoding handled by decoders in sql/query.clj

  Args:
    _return-type - unused, kept for API consistency

  Returns:
    next.jdbc builder-fn that processes ResultSet rows"
  [_return-type]
  (rs/as-maps-adapter
    rs/as-unqualified-modified-maps
    (fn [^ResultSet rs _rsmeta ^Integer i]
      ;; Return raw values - JSON decoding handled by decoders in sql/query.clj
      (.getObject rs i))))

(def ^:private defaults
  "SQLite-specific next.jdbc options for each return type.

  :graphql - String keys, lowercase column names
  :edn - Keyword keys, kebab-case column names
  :raw - String keys, original column names"
  {:graphql
   {:builder-fn (sqlite-result-builder :graphql)
    :label-fn str/lower-case
    :qualifier-fn name}

   :edn
   {:builder-fn (sqlite-result-builder :edn)
    :label-fn (fn [w]
                (let [special (re-find #"^_+" w)]
                  (keyword (str special (csk/->kebab-case-string w)))))
    :qualifier-fn (comp str/lower-case name)}

   :raw
   {:builder-fn (sqlite-result-builder :raw)
    :label-fn identity
    :qualifier-fn name}})

(extend-type SQLite
  sql/JDBCBackend

  (jdbc-options [_db return-type]
    (get defaults (or return-type :raw) (:raw defaults))))
