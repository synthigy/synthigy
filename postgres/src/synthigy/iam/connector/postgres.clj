(ns synthigy.iam.connector.postgres
  "Postgres-backed CredentialsProvider.

  Stores connector rows in `__iam_auth_connector`, a side-table managed
  directly via Patcho patches (not through the ERD machinery — connectors
  are infrastructure, not domain entities).

  Cross-node refresh is wired through `LISTEN/NOTIFY` on the
  `__iam_auth_connector` channel — a row-level trigger fires on every write,
  every node listening invalidates its in-memory cache. No Redis needed
  for single-cluster Postgres deployments.

  Lifecycle:
    1. Patch `1.0.0` creates table, trigger, function, and seeds the
       default `:database` connector.
    2. The `:synthigy.iam/connector` lifecycle module instantiates a
       PostgresCredentialsProvider on start and registers it with
       `synthigy.iam.connector/set-credentials-provider!`."
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.db :refer [*db*]]
   [synthigy.db.postgres]  ; load Postgres JDBCBackend impl
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.connector :as connector]
   [synthigy.json :as json]
   [synthigy.log :as log])
  (:import
   [java.sql Connection]
   [org.postgresql PGNotification]
   [org.postgresql.util PGobject]))

;; =============================================================================
;; JSONB helpers
;; =============================================================================

(defn- ->jsonb [m]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/->json (or m {})))))

(defn- <-jsonb [v]
  (cond
    (nil? v) nil
    (instance? PGobject v) (try (json/read-str (.getValue ^PGobject v))
                                (catch Throwable _ nil))
    (string? v) (try (json/read-str v) (catch Throwable _ nil))
    :else v))

(defn- row->connector
  "Convert a row read from `__iam_auth_connector` into a connector map ready for
   the multimethod dispatch. The DB's `config` JSONB is merged on top of the
   top-level columns so a connector spec is one flat map."
  [row]
  (when row
    (let [config (<-jsonb (or (:__iam_auth_connector/config row) (:config row)))]
      (merge
       (or config {})
       {:xid      (or (:__iam_auth_connector/xid row)      (:xid row))
        :name     (or (:__iam_auth_connector/name row)     (:name row))
        :type     (let [t (or (:__iam_auth_connector/type row) (:type row))]
                    (cond (keyword? t) t (string? t) (keyword t)))
        :priority (or (:__iam_auth_connector/priority row) (:priority row))
        :domain   (or (:__iam_auth_connector/domain row)   (:domain row))
        :enabled  (or (:__iam_auth_connector/enabled row)  (:enabled row))}))))

(defn- connector->row
  "Map an inbound connector spec back into row columns for INSERT/UPDATE.
   Anything not in the known columns is folded into `config`."
  [c]
  (let [known #{:euuid :xid :name :type :priority :domain :enabled}
        config (apply dissoc c known)]
    {:xid      (or (:xid c) (id/generate-xid))
     :name     (or (:name c) (some-> (:type c) name) "unnamed")
     :type     (some-> (:type c) name)
     :priority (or (:priority c) 1000)
     :domain   (:domain c)
     :enabled  (if (some? (:enabled c)) (boolean (:enabled c)) true)
     :config   (->jsonb config)}))

;; =============================================================================
;; SQL
;; =============================================================================

(def ^:private sql-list-chain
  "SELECT xid, name, type, priority, domain, enabled, config
     FROM __iam_auth_connector
    WHERE enabled = TRUE
 ORDER BY priority ASC, xid ASC")

(def ^:private sql-find-by-id
  "SELECT xid, name, type, priority, domain, enabled, config
     FROM __iam_auth_connector
    WHERE xid = ?
    LIMIT 1")

(def ^:private sql-upsert
  "INSERT INTO __iam_auth_connector
     (xid, name, type, priority, domain, enabled, config)
   VALUES
     (?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT (xid) DO UPDATE
      SET name = EXCLUDED.name,
          type = EXCLUDED.type,
          priority = EXCLUDED.priority,
          domain = EXCLUDED.domain,
          enabled = EXCLUDED.enabled,
          config = EXCLUDED.config,
          modified_on = now()
   RETURNING xid, name, type, priority, domain, enabled, config")

(def ^:private sql-delete
  "DELETE FROM __iam_auth_connector WHERE xid = ?")

;; =============================================================================
;; Provider record
;; =============================================================================

(defn- query-rows [datasource sql params]
  (jdbc/execute! datasource (into [sql] params)
                 {:builder-fn rs/as-unqualified-maps}))

(defrecord PostgresCredentialsProvider [datasource cache listener-thread listener-stop]
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

  (-save-connector! [this connector]
    (let [{:keys [xid name type priority domain enabled config]} (connector->row connector)
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

  (-start! [this]
    ;; Spawn a long-lived LISTEN connection that invalidates the cache on
    ;; any NOTIFY from the trigger. The connection is opened raw, off-pool
    ;; (db.postgres/listen-connection) — pgjdbc requires the same physical
    ;; conn for LISTEN, and holding a pooled conn for the process lifetime
    ;; would pin a Hikari slot and trip its leak detector.
    (let [stop? (atom false)
          thread
          (doto
           (Thread.
            ^Runnable
            (fn []
              (try
                (with-open [conn ^Connection (synthigy.db.postgres/listen-connection datasource)
                            stmt (.createStatement conn)]
                  (.execute stmt "LISTEN \"__iam_auth_connector\"")
                  (let [pg-conn (.unwrap conn org.postgresql.PGConnection)]
                    (loop []
                      (when-not @stop?
                        (when-let [^"[Lorg.postgresql.PGNotification;" notes
                                   (.getNotifications pg-conn 1000)]
                          (doseq [^PGNotification _n notes]
                            (log/debug {:id ::notify-received}
                                       "NOTIFY received — invalidating cache")
                            (connector/-refresh! this)))
                        (recur)))))
                (catch InterruptedException _ nil)
                (catch Throwable ex
                  (log/warn {:id ::listen-loop-exited :error ex}
                            "LISTEN loop exited")))))
            (.setName "synthigy.iam.connector.postgres-listener")
            (.setDaemon true)
            (.start))]
      (reset! listener-stop stop?)
      (reset! listener-thread thread))
    nil)

  (-stop! [_]
    (when-let [stop? @listener-stop] (reset! stop? true))
    (when-let [^Thread t @listener-thread] (.interrupt t))
    (reset! cache nil)
    nil))

(defn make-provider
  "Construct a PostgresCredentialsProvider for the given Postgres `db`."
  [db]
  (->PostgresCredentialsProvider
   (:datasource db)
   (atom nil)
   (atom nil)
   (atom nil)))

;; =============================================================================
;; Patcho component — table, trigger, default seed
;; =============================================================================

(defn- ensure-table!
  "Create __iam_auth_connector table + index + NOTIFY trigger + default seed if missing.
  Idempotent — guards against drift where __component_versions__ records the patch
  as installed but the table was dropped externally."
  [ds]
  (jdbc/execute! ds
    ["CREATE TABLE IF NOT EXISTS __iam_auth_connector (
        xid         VARCHAR(64) PRIMARY KEY,
        name        TEXT NOT NULL,
        type        TEXT NOT NULL,
        priority    INT  NOT NULL DEFAULT 1000,
        domain      TEXT,
        enabled     BOOLEAN NOT NULL DEFAULT TRUE,
        config      JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_on  TIMESTAMPTZ NOT NULL DEFAULT now(),
        modified_on TIMESTAMPTZ NOT NULL DEFAULT now()
      )"])
  ;; Forward-migrate legacy tables to xid-only: drop the vestigial euuid PK
  ;; column. xid is already UNIQUE NOT NULL, so it remains the functional key
  ;; (ON CONFLICT (xid) still works). Idempotent — no-op once euuid is gone.
  (jdbc/execute! ds
    ["ALTER TABLE __iam_auth_connector DROP COLUMN IF EXISTS euuid"])
  (jdbc/execute! ds
    ["CREATE INDEX IF NOT EXISTS __iam_auth_connector_chain_idx
        ON __iam_auth_connector (enabled, priority)"])
  (jdbc/execute! ds
    ["CREATE OR REPLACE FUNCTION __iam_auth_connector_notify_fn()
      RETURNS trigger AS $$
      BEGIN
        PERFORM pg_notify(
          '__iam_auth_connector',
          json_build_object(
            'op', TG_OP,
            'xid', COALESCE(NEW.xid, OLD.xid)
          )::text);
        RETURN COALESCE(NEW, OLD);
      END;
      $$ LANGUAGE plpgsql"])
  (jdbc/execute! ds
    ["DROP TRIGGER IF EXISTS __iam_auth_connector_notify_trg
        ON __iam_auth_connector"])
  (jdbc/execute! ds
    ["CREATE TRIGGER __iam_auth_connector_notify_trg
        AFTER INSERT OR UPDATE OR DELETE ON __iam_auth_connector
        FOR EACH ROW EXECUTE FUNCTION __iam_auth_connector_notify_fn()"])
  (jdbc/execute! ds
    ["INSERT INTO __iam_auth_connector (xid, name, type, priority, enabled, config)
      SELECT ?, 'Local database', 'database', 1000, TRUE, '{}'::jsonb
      WHERE NOT EXISTS (SELECT 1 FROM __iam_auth_connector)"
     (id/generate-xid)]))

(patch/current-version :synthigy.iam/connector "1.0.0")

(patch/upgrade :synthigy.iam/connector
               "1.0.0"
               (when (instance? synthigy.db.Postgres *db*)
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
           (when (instance? synthigy.db.Postgres *db*)
             (log/info {:id ::provider-starting}
                       "Starting Postgres credentials provider")
             (patch/level! :synthigy.iam/connector)
             ;; Defensive: re-run idempotent DDL in case the table was dropped
             ;; externally while __component_versions__ still records 1.0.0.
             (ensure-table! (:datasource *db*))
             (connector/set-credentials-provider! (make-provider *db*))
             (log/info {:id ::provider-ready}
                       "Provider ready")))
  :stop (fn []
          (log/info {:id ::provider-stopping}
                    "Stopping Postgres credentials provider")
          (when (instance? PostgresCredentialsProvider connector/*credentials-provider*)
            (connector/-stop! connector/*credentials-provider*)))})
