(ns synthigy.iam.connector.sqlite
  "SQLite-backed CredentialsProvider.

  SQLite is single-process by design — no LISTEN/NOTIFY equivalent — so the
  in-memory cache is invalidated only on local writes. That's correct for
  single-node deployments which is the only configuration SQLite supports.

  Lifecycle is identical in shape to the Postgres provider; differences:
   - JSON stored as TEXT (no JSONB type)
   - BOOLEAN stored as INTEGER (0/1)
   - TIMESTAMPTZ stored as TEXT (ISO 8601 via current_timestamp)
   - xids generated in Clojure via `id/generate-xid` (no gen_random_uuid())"
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sqlite]  ; load SQLite JDBCBackend impl
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.connector :as connector]
   [synthigy.json :as json]
   [synthigy.log :as log]))

;; =============================================================================
;; Row <-> connector conversion (SQLite stores JSON as TEXT, bool as INTEGER)
;; =============================================================================

(defn- row->connector [row]
  (when row
    (let [config (let [s (or (:config row) "{}")]
                   (try (json/read-str s) (catch Throwable _ {})))
          enabled (let [v (:enabled row)]
                    (cond (boolean? v) v
                          (number? v)  (not (zero? v))
                          :else true))
          type-v (:type row)]
      (merge
       (or config {})
       {:xid      (:xid row)
        :name     (:name row)
        :type     (cond (keyword? type-v) type-v (string? type-v) (keyword type-v))
        :priority (:priority row)
        :domain   (:domain row)
        :enabled  enabled}))))

(defn- connector->params
  "Bind params for INSERT/UPDATE."
  [c]
  (let [known #{:euuid :xid :name :type :priority :domain :enabled}
        config (apply dissoc c known)]
    {:xid      (or (:xid c) (id/generate-xid))
     :name     (or (:name c) (some-> (:type c) name) "unnamed")
     :type     (some-> (:type c) name)
     :priority (or (:priority c) 1000)
     :domain   (:domain c)
     :enabled  (if (some? (:enabled c)) (if (:enabled c) 1 0) 1)
     :config   (json/->json config)}))

;; =============================================================================
;; SQL
;; =============================================================================

(def ^:private sql-list-chain
  "SELECT xid, name, type, priority, domain, enabled, config
     FROM __iam_auth_connector
    WHERE enabled = 1
 ORDER BY priority ASC, xid ASC")

(def ^:private sql-find-by-id
  "SELECT xid, name, type, priority, domain, enabled, config
     FROM __iam_auth_connector
    WHERE xid = ?
    LIMIT 1")

(def ^:private sql-upsert
  "INSERT INTO __iam_auth_connector
     (xid, name, type, priority, domain, enabled, config, created_on, modified_on)
   VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
   ON CONFLICT(xid) DO UPDATE SET
     name        = excluded.name,
     type        = excluded.type,
     priority    = excluded.priority,
     domain      = excluded.domain,
     enabled     = excluded.enabled,
     config      = excluded.config,
     modified_on = current_timestamp
   RETURNING xid, name, type, priority, domain, enabled, config")

(def ^:private sql-delete
  "DELETE FROM __iam_auth_connector WHERE xid = ?")

;; =============================================================================
;; Provider record
;; =============================================================================

(defn- query-rows [datasource sql params]
  (jdbc/execute! datasource (into [sql] params)
                 {:builder-fn rs/as-unqualified-maps}))

(defrecord SqliteCredentialsProvider [datasource cache]
  connector/CredentialsProvider

  (-list-chain [_]
    (or @cache
        (let [chain (->> (query-rows datasource sql-list-chain [])
                         (mapv row->connector))]
          (reset! cache chain)
          chain)))

  (-find-connector [_ id]
    (-> (query-rows datasource sql-find-by-id [(str id)])
        first
        row->connector))

  (-save-connector! [this c]
    (let [{:keys [xid name type priority domain enabled config]} (connector->params c)
          row (-> (jdbc/execute-one! datasource
                                     [sql-upsert xid name type priority domain enabled config]
                                     {:builder-fn rs/as-unqualified-maps})
                  row->connector)]
      (connector/-refresh! this)
      (try (iam/publish :iam.connector/changed {:id (:xid row)})
           (catch Throwable _))
      row))

  (-delete-connector! [this id]
    (jdbc/execute-one! datasource [sql-delete (str id)])
    (connector/-refresh! this)
    (try (iam/publish :iam.connector/changed {:id id})
         (catch Throwable _)))

  (-refresh! [_] (reset! cache nil))
  (-start! [_] nil)
  (-stop! [_] (reset! cache nil)))

(defn make-provider
  "Construct a SqliteCredentialsProvider for the given SQLite `db`."
  [db]
  (->SqliteCredentialsProvider (:datasource db) (atom nil)))

;; =============================================================================
;; Table installation (idempotent — safe to call on every boot)
;; =============================================================================

(defn- ensure-table!
  "Create __iam_auth_connector table + index + default seed if missing.
  Idempotent — guards against drift where __component_versions__ records the
  patch as installed but the table was dropped externally."
  [ds]
  (jdbc/execute! ds
    ["CREATE TABLE IF NOT EXISTS __iam_auth_connector (
        xid         TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        type        TEXT NOT NULL,
        priority    INTEGER NOT NULL DEFAULT 1000,
        domain      TEXT,
        enabled     INTEGER NOT NULL DEFAULT 1,
        config      TEXT NOT NULL DEFAULT '{}',
        created_on  TEXT NOT NULL DEFAULT current_timestamp,
        modified_on TEXT NOT NULL DEFAULT current_timestamp
      )"])
  (jdbc/execute! ds
    ["CREATE INDEX IF NOT EXISTS __iam_auth_connector_chain_idx
        ON __iam_auth_connector (enabled, priority)"])
  (jdbc/execute! ds
    ["INSERT INTO __iam_auth_connector (xid, name, type, priority, enabled, config)
      SELECT ?, 'Local database', 'database', 1000, 1, '{}'
      WHERE NOT EXISTS (SELECT 1 FROM __iam_auth_connector)"
     (id/generate-xid)]))

;; =============================================================================
;; Patcho component — table + default seed (no trigger; single-process)
;; =============================================================================

(patch/current-version :synthigy.iam/connector "1.0.0")

(patch/upgrade :synthigy.iam/connector
               "1.0.0"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::installing-v100 :data {:action :installing :subject :iam-connector :version "1.0.0"}}
                           "Installing __iam_auth_connector v1.0.0")
                 (ensure-table! (:datasource *db*))
                 (log/info {:id ::table-ready}
                           "__iam_auth_connector ready")))

;; =============================================================================
;; Lifecycle module
;; =============================================================================

(lifecycle/register-module!
 :synthigy.iam/connector
 {:depends-on [:synthigy/iam]
  :doc "IAM credentials provider — login lookups + cache"
  :start (fn []
           (when (instance? synthigy.db.SQLite *db*)
             (log/info {:id ::provider-starting}
                       "Starting SQLite credentials provider")
             (patch/level! :synthigy.iam/connector)
             ;; Defensive: re-run idempotent DDL in case the table was dropped
             ;; externally while __component_versions__ still records 1.0.0.
             (ensure-table! (:datasource *db*))
             (connector/set-credentials-provider! (make-provider *db*))
             (log/info {:id ::provider-ready}
                       "Provider ready")))
  :stop (fn []
          (log/info {:id ::provider-stopping}
                    "Stopping SQLite credentials provider")
          (when (instance? SqliteCredentialsProvider connector/*credentials-provider*)
            (connector/-stop! connector/*credentials-provider*)))})
