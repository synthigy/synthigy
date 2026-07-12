(ns synthigy.db.sqlite
  "SQLite connection management and lifecycle"
  (:require
    [camel-snake-kebab.core :as csk]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [environ.core :refer [env]]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.db :as db]
    [synthigy.db.sql :as sql]
    synthigy.env
    [synthigy.log :as log])
  (:import
    [com.zaxxer.hikari HikariDataSource]
    [java.sql ResultSet]
    [org.sqlite SQLiteException]
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
  (log/info {:id ::connecting :data {:path path}} "Connecting to SQLite database")
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
                     (.setMaxLifetime 1800000)
                     ;; SQLite FK enforcement is per-connection and OFF by
                     ;; default. The pragmas set in `start` only land on the
                     ;; one connection used to set them; pool-managed conns
                     ;; pick this init-sql up on first acquire. Without this,
                     ;; `ON DELETE CASCADE` is silently inert.
                     (.setConnectionInitSql "PRAGMA foreign_keys = ON"))]
    (try
      ;; Test connection
      (with-open [conn (jdbc/get-connection datasource)]
        (jdbc/execute-one! conn ["SELECT 1"]))
      (log/info {:id ::connected :data {:path path :pool-size max-connections}}
                "Connected to SQLite database")
      (db/map->SQLite (assoc config :datasource datasource))
      (catch Exception e
        (log/error! {:id ::connect-failed :data {:path path}} e)
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
    (log/info {:id ::path-from-env :data {:path path}} "Database path from env")
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
     (log/info {:id ::backend-starting :data {:action :starting :subject :db-backend}} "Starting SQLite backend...")
     (when-let [db (connect config)]

       (alter-var-root #'db/*db* (constantly db))
       (log/info {:id ::backend-started :data {:action :started :subject :db-backend}} "SQLite backend started")
       nil))))

(defn stop
  "Stops SQLite database connection.

  Side effects:
    - Clears synthigy.db/*db*

  Returns:
    nil"
  []
  (log/info {:id ::backend-stopping :data {:action :stopping :subject :db-backend}} "Stopping SQLite backend...")
  ; (alter-var-root #'db/*db* (constantly nil))
  (log/info {:id ::backend-stopped :data {:action :stopped :subject :db-backend}} "SQLite backend stopped")
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
  (log/info {:id ::cleanup-starting :data {:path path}} "Cleaning up database files")
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
    (log/info {:id ::cleanup-complete :data {:deleted-count (count (:deleted results))}}
              "Cleanup complete")
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
   :doc "JDBC pool + DB provisioning (SQLite)"
   :setup (fn []
            ;; One-time: Create database file (SQLite auto-creates on connect)
            (log/info {:id ::will-be-created} "SQLite database will be created on first connection")
            (start)
            (lifecycle/set-store! db/*db*)
            (ensure-lifecycle-table! db/*db*))
   :cleanup (fn []
              ;; One-time: Delete database file (DESTRUCTIVE - parity with PostgreSQL)
              (let [{:keys [path datasource]} db/*db*]
                (log/info {:id ::lifecycle-cleanup-delete :data {:path path}}
                          "SQLite cleanup - deleting database")
                ;; Close datasource if open
                (when datasource
                  (try
                    (.close datasource)
                    (catch Exception e
                      (log/warn {:id ::datasource-close-failed :error e}
                                "Error closing datasource"))))
                ;; Delete database file
                (cleanup-files! path)
                (alter-var-root #'db/*db* (constantly nil))
                ;; Reset lifecycle store to fresh in-memory atom
                (lifecycle/reset-store!)))
   :start (fn []
            ;; Runtime: Connect to database
            (log/info {:id ::lifecycle-starting :data {:action :starting}} "Starting SQLite connection...")
            (start)
            (patch/set-store! db/*db*)
            (lifecycle/set-store! db/*db*)
            (log/info {:id ::lifecycle-started :data {:action :started}} "SQLite connection started"))
   :stop (fn []
           ;; Runtime: Close connections
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "Stopping SQLite connection...")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "SQLite connection stopped"))})

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

;;; ============================================================================
;;; DB Error Translation
;;; ============================================================================
;;
;; SQLite's JDBC driver formats constraint failures as
;;   "[SQLITE_CONSTRAINT_*]  ... (KIND constraint failed: T.col[, T.col ...])"
;; The parens-suffix is stable enough to drive translation. We dispatch on
;; substring rather than result-code to also cover wrapped/rewrapped exception
;; layers; unrecognized exceptions return nil and fall through to the
;; INTERNAL_ERROR path in the request handler.
;;
;; SQLite exposes column names for UNIQUE and NOT NULL failures. FK and CHECK
;; messages don't carry per-attribute info; PG when added will populate
;; :details more broadly via PSQLException server fields.

(defn- parse-constraint-targets
  "Parse the trailing 'KIND constraint failed: T.col[, T.col2 ...]' segment
   into {:entity name :attrs [name ...]}. Returns nil if not parseable."
  [^String msg]
  (when-let [tail (second (re-find #"constraint failed:\s*(.+?)\s*\)?$" msg))]
    (let [refs   (clojure.string/split tail #",\s*")
          parts  (mapv (fn [r] (clojure.string/split (clojure.string/trim r) #"\.")) refs)
          entity (first (first parts))
          attrs  (mapv second parts)]
      (when (and entity (every? some? attrs))
        {:entity entity :attrs attrs}))))

(defn- translate-sqlite-exception
  [^SQLiteException e]
  (let [msg (or (.getMessage e) "")]
    (cond
      (clojure.string/includes? msg "UNIQUE constraint failed")
      (let [{:keys [entity attrs]} (parse-constraint-targets msg)]
        (ex-info (cond
                   (and entity (= 1 (count attrs)))
                   (str entity "." (first attrs) " must be unique")
                   (and entity (seq attrs))
                   (str entity " must be unique on (" (clojure.string/join ", " attrs) ")")
                   :else "Field must be unique")
                 {:code "UNIQUE_VIOLATION"
                  :details (cond-> {:rule "unique"}
                             entity      (assoc :entity entity)
                             (seq attrs) (assoc :attributes attrs))}))

      (clojure.string/includes? msg "NOT NULL constraint failed")
      (let [{:keys [entity attrs]} (parse-constraint-targets msg)
            attr (first attrs)]
        (ex-info (if (and entity attr)
                   (str entity "." attr " is required")
                   "Field is required")
                 {:code "NOT_NULL_VIOLATION"
                  :details (cond-> {:rule "required"}
                             entity (assoc :entity entity)
                             attr   (assoc :attributes [attr]))}))

      (clojure.string/includes? msg "FOREIGN KEY constraint failed")
      (ex-info "Referenced record does not exist"
               {:code "FK_VIOLATION"
                :details {:rule "reference"}})

      (clojure.string/includes? msg "CHECK constraint failed")
      (ex-info "Field violates a check constraint"
               {:code "CHECK_VIOLATION"
                :details {:rule "check"}})

      (or (clojure.string/includes? msg "SQLITE_BUSY")
          (clojure.string/includes? msg "SQLITE_INTERRUPT"))
      (ex-info "Database statement timed out"
               {:code "TIMEOUT"})

      :else nil)))

(extend-type SQLite
  db/Translator

  (translate-db-exception [_ e]
    (when (instance? SQLiteException e)
      (translate-sqlite-exception e))))
