;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
    [synthigy.db.dialect :as dialect]
    [synthigy.db.sql :as sql]
    synthigy.env
    [synthigy.log :as log])
  (:import
    [com.zaxxer.hikari HikariDataSource]
    [java.sql ResultSet]
    [org.sqlite SQLiteErrorCode SQLiteException]
    [synthigy.db SQLite])
  (:gen-class))

(defn connect
  "Opens a Hikari pool over the SQLite file at `path`; returns the SQLite record."
  [{:keys [path max-connections]
    :or {max-connections 8}
    :as config}]
  (log/info {:id ::connecting :data {:path path}} "Connecting to SQLite database")
  (io/make-parents (str path))
  (let [jdbc-url (str "jdbc:sqlite:" path "?journal_mode=WAL&busy_timeout=30000&transaction_mode=IMMEDIATE")
        datasource (doto
                     (HikariDataSource.)
                     (.setDriverClassName "org.sqlite.JDBC")
                     (.setJdbcUrl jdbc-url)
                     (.setMaximumPoolSize max-connections)
                     (.setMinimumIdle 0)
                     (.setConnectionTimeout 30000)
                     (.setIdleTimeout 600000)
                     (.setMaxLifetime 1800000)
                     ;; never drop this — FK enforcement is per connection, CASCADE is inert without it
                     (.setConnectionInitSql "PRAGMA foreign_keys = ON"))]
    (try
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
  "SQLite connection config: the instance's db/synthigy.db, pool size from SQLITE_POOL_SIZE."
  []
  (let [path (str synthigy.env/db-dir "/synthigy.db")]
    (log/info {:id ::path-from-env :data {:path path}} "SQLite database path")
    {:path path
     :max-connections (Integer/parseInt (env :sqlite-pool-size "8"))}))

(defn freelist-count
  [db]
  (with-open [c (jdbc/get-connection (:datasource db))]
    (long (first (vals (jdbc/execute-one! c ["PRAGMA freelist_count"]))))))

(defn ensure-incremental-vacuum!
  "Put the file in auto_vacuum=INCREMENTAL once; an existing file needs one full VACUUM for the mode to take."
  [db]
  (with-open [c (jdbc/get-connection (:datasource db))]
    (let [mode (long (first (vals (jdbc/execute-one! c ["PRAGMA auto_vacuum"]))))]
      (when (not= 2 mode)
        (log/info {:id ::enabling-incremental-vacuum
                   :data {:action :migrating :subject :db-backend :auto-vacuum mode}}
                  "Switching SQLite file to incremental auto_vacuum")
        (jdbc/execute! c ["PRAGMA auto_vacuum = INCREMENTAL"])
        (jdbc/execute! c ["VACUUM"])))))

(defn incremental-vacuum!
  "Return up to `pages` free pages to the OS; cheap when the freelist is empty."
  [db pages]
  ;; executeUpdate, never execute!/execute — the pragma frees one page per step and only executeUpdate steps it to the end
  (with-open [^java.sql.Connection c (jdbc/get-connection (:datasource db))
              st (.createStatement c)]
    (.executeUpdate st (str "PRAGMA incremental_vacuum(" (long pages) ")"))))

(defn start
  "Connects and binds synthigy.db/*db* unless a SQLite backend is already bound."
  ([] (start (from-env)))
  ([config]
   (when-not (instance? SQLite db/*db*)
     (log/info {:id ::backend-starting :data {:action :starting :subject :db-backend}} "Starting SQLite backend...")
     (when-let [db (connect config)]
       (ensure-incremental-vacuum! db)
       (alter-var-root #'db/*db* (constantly db))
       (log/info {:id ::backend-started :data {:action :started :subject :db-backend}} "SQLite backend started")
       nil))))

(defn stop
  []
  (log/info {:id ::backend-stopping :data {:action :stopping :subject :db-backend}} "Stopping SQLite backend...")
  ; (alter-var-root #'db/*db* (constantly nil))
  (log/info {:id ::backend-stopped :data {:action :stopped :subject :db-backend}} "SQLite backend stopped")
  nil)

(defn cleanup-files!
  "Deletes the database file and its -wal/-shm/-journal siblings; returns {:deleted :missing}."
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

(defn ensure-version-table!
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

(defn ensure-lifecycle-table!
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

  (read-lifecycle-state [db topic]
    (ensure-lifecycle-table! db)
    (if-let [row (jdbc/execute-one!
                   (:datasource db)
                   ["SELECT setup_complete, cleanup_complete
                     FROM __lifecycle_state__
                     WHERE topic = ?"
                    (name topic)])]
      ;; never include :started? — it is runtime-only
      {:setup-complete? (= 1 (:__lifecycle_state__/setup_complete row))
       :cleanup-complete? (= 1 (:__lifecycle_state__/cleanup_complete row))}
      {:setup-complete? false
       :cleanup-complete? false}))

  (write-lifecycle-state [db topic state]
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

(defn ensure-login-page-table!
  [{:keys [datasource]}]
  (jdbc/execute-one!
    datasource
    ["CREATE TABLE IF NOT EXISTS __login_pages__ (
       name TEXT NOT NULL,
       version INTEGER NOT NULL,
       zip BLOB NOT NULL,
       digest TEXT NOT NULL,
       size INTEGER NOT NULL,
       uploaded_by TEXT,
       uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
       active INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY (name, version)
     )"]))

(extend-type SQLite
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
    (mapv #(update % :active (fn [a] (= 1 a)))
          (jdbc/execute!
            (:datasource db)
            ["SELECT name, version, digest, size, uploaded_by, uploaded_at, active
              FROM __login_pages__ ORDER BY name, version DESC"]
            {:builder-fn rs/as-unqualified-kebab-maps})))

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
            ["SELECT name, version, digest FROM __login_pages__ WHERE active = 1"]
            {:builder-fn rs/as-unqualified-kebab-maps})))

  (activate-login-page [db name version]
    (ensure-login-page-table! db)
    (jdbc/execute-one!
      (:datasource db)
      ["UPDATE __login_pages__ SET active = COALESCE(version = ?, 0) WHERE name = ?"
       version name])
    nil)

  (delete-login-page [db name version]
    (ensure-login-page-table! db)
    (jdbc/execute-one!
      (:datasource db)
      ["DELETE FROM __login_pages__ WHERE name = ? AND version = ?" name version])
    nil))


(comment
  (lifecycle/setup! :synthigy/iam))

(lifecycle/register-module!
  :synthigy/database
  {:depends-on [:synthigy/transit]
   :doc "JDBC pool + DB provisioning (SQLite)"
   :setup (fn []
            (log/info {:id ::will-be-created} "SQLite database will be created on first connection")
            (start)
            (lifecycle/set-store! db/*db*)
            (ensure-lifecycle-table! db/*db*))
   :cleanup (fn []
              ;; DESTRUCTIVE — deletes the database file
              (let [{:keys [path datasource]} db/*db*]
                (log/info {:id ::lifecycle-cleanup-delete :data {:path path}}
                          "SQLite cleanup - deleting database")
                (when datasource
                  (try
                    (.close datasource)
                    (catch Exception e
                      (log/warn {:id ::datasource-close-failed :error e}
                                "Error closing datasource"))))
                (cleanup-files! path)
                (alter-var-root #'db/*db* (constantly nil))
                (lifecycle/reset-store!)))
   :start (fn []
            (log/info {:id ::lifecycle-starting :data {:action :starting}} "Starting SQLite connection...")
            (start)
            (patch/set-store! db/*db*)
            (lifecycle/set-store! db/*db*)
            (log/info {:id ::lifecycle-started :data {:action :started}} "SQLite connection started"))
   :stop (fn []
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "Stopping SQLite connection...")
           ;; never close the pool before nil-ing the patcho stores — a stale store wedges restart
           (patch/set-store! nil)
           (lifecycle/set-store! nil)
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "SQLite connection stopped"))})

(defn sqlite-result-builder
  [_return-type]
  (rs/as-maps-adapter
    rs/as-unqualified-modified-maps
    (fn [^ResultSet rs _rsmeta ^Integer i]
      (.getObject rs i))))

(def ^:private defaults
  {:edn
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

(defn parse-constraint-targets
  "{:entity :attrs} from the driver's 'KIND constraint failed: T.col, …' tail, or nil."
  [^String msg]
  (when-let [tail (second (re-find #"constraint failed:\s*(.+?)\s*\)?$" msg))]
    (let [refs   (str/split tail #",\s*")
          parts  (mapv (fn [r] (str/split (str/trim r) #"\.")) refs)
          entity (first (first parts))
          attrs  (mapv second parts)]
      (when (and entity (every? some? attrs))
        {:entity entity :attrs attrs}))))

(defn sqlite-detail
  "The driver's `(...)` explanation after the result-code description, or the whole message."
  [^SQLiteException e]
  (let [msg  (str/replace (or (.getMessage e) "") #"^\[[A-Z_]+\]\s*" "")
        desc (some-> ^SQLiteErrorCode (.getResultCode e) .message)
        i    (when (seq desc) (str/index-of msg desc))
        tail (str/trim (if i (subs msg (+ i (count desc))) msg))]
    (if (and (str/starts-with? tail "(") (str/ends-with? tail ")"))
      (subs tail 1 (dec (count tail)))
      tail)))

(defn sqlite-schema-code
  [detail]
  (condp re-find detail
    #"already exists|^duplicate column name"       "SCHEMA_CONFLICT"
    #"^no such (table|column|index|view|trigger)"  "SCHEMA_DRIFT"
    #"^error in (index|view|trigger) .* after (drop|rename)" "DEPENDENT_OBJECTS"
    #"^Cannot add a NOT NULL column"               "NOT_NULL_VIOLATION"
    nil))

(defn translate-sqlite-exception
  [^SQLiteException e]
  (let [msg    (or (.getMessage e) "")
        code   (some-> ^SQLiteErrorCode (.getResultCode e) .name)
        detail (sqlite-detail e)]
    (cond
      (str/includes? msg "UNIQUE constraint failed")
      (let [{:keys [entity attrs]} (parse-constraint-targets msg)]
        (ex-info (cond
                   (and entity (= 1 (count attrs)))
                   (str entity "." (first attrs) " must be unique")
                   (and entity (seq attrs))
                   (str entity " must be unique on (" (str/join ", " attrs) ")")
                   :else "Field must be unique")
                 {:code "UNIQUE_VIOLATION"
                  :details (cond-> {:rule "unique"}
                             entity      (assoc :entity entity)
                             (seq attrs) (assoc :attributes attrs))}))

      (str/includes? msg "NOT NULL constraint failed")
      (let [{:keys [entity attrs]} (parse-constraint-targets msg)
            attr (first attrs)]
        (ex-info (if (and entity attr)
                   (str entity "." attr " is required")
                   "Field is required")
                 {:code "NOT_NULL_VIOLATION"
                  :details (cond-> {:rule "required"}
                             entity (assoc :entity entity)
                             attr   (assoc :attributes [attr]))}))

      (str/includes? msg "FOREIGN KEY constraint failed")
      (ex-info "Referenced record does not exist"
               {:code "FK_VIOLATION"
                :details {:rule "reference"}})

      (str/includes? msg "CHECK constraint failed")
      (ex-info "Field violates a check constraint"
               {:code "CHECK_VIOLATION"
                :details {:rule "check"}})

      (= "SQLITE_CONSTRAINT_TRIGGER" code)
      (ex-info detail {:code "GUARD_VIOLATION"})

      (or (#{"SQLITE_BUSY" "SQLITE_LOCKED" "SQLITE_INTERRUPT"} code)
          (str/includes? msg "SQLITE_BUSY")
          (str/includes? msg "SQLITE_INTERRUPT"))
      (ex-info "Database statement timed out"
               {:code "TIMEOUT" :retryable true})

      (some #(some-> code (str/starts-with? %))
            ["SQLITE_FULL" "SQLITE_IOERR" "SQLITE_CANTOPEN"])
      (ex-info "Database unavailable" {:code "DB_UNAVAILABLE" :retryable true})

      :else
      (if-let [schema-code (sqlite-schema-code detail)]
        (ex-info detail {:code schema-code})
        (ex-info detail {:code "DB_ERROR"})))))

(extend-type SQLite
  db/Translator

  (translate-db-exception [_ e]
    (when (instance? SQLiteException e)
      (translate-sqlite-exception e))))

(extend-type SQLite
  db/Dialect

  (json-param [_ s] s)
  (json-column [_ v] v)

  (table-exists? [_ table]
    (boolean (sql/execute-one! ["SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?" table])))

  ;; never pass untrusted `table` here — PRAGMA takes no bind params
  (column-exists? [_ table column]
    (boolean (some #(= column (:name %))
                   (sql/execute! [(format "PRAGMA table_info(%s)" table)]))))

  (ddl [_] {:serial-pk "INTEGER PRIMARY KEY AUTOINCREMENT" :json "TEXT" :now "CURRENT_TIMESTAMP"})
  (json-text [_ expr] expr)
  (json-get-text [_ expr k] (str "json_extract(" expr ", '$.\"" k "\"')"))
  (json-remove [_ expr k] (str "json_remove(" expr ", '$.\"" k "\"')"))
  (cast-placeholder [_ _type] "?")
  (template-sql [_ raw-sql] (-> raw-sql dialect/strip-casts dialect/ilike->like)))
