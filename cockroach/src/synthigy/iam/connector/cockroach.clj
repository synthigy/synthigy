(ns synthigy.iam.connector.cockroach
  "CockroachDB-backed CredentialsProvider.

  Stores connector rows in `__iam_auth_connector`, a side-table managed
  directly via Patcho patches (not through the ERD machinery — connectors
  are infrastructure, not domain entities).

  Differences vs `synthigy.iam.connector.postgres`:
    - **Cross-node refresh is polling, not LISTEN/NOTIFY.** CRDB has no
      LISTEN/NOTIFY, so the listener thread is replaced with a 5-second
      poller that invalidates the local cache unconditionally each tick.
      Re-read is cheap (single SELECT, sub-millisecond) and the chain is
      always small (<100 rows). Window of staleness ≤ 5s; configurable
      via `:poll-ms` on the provider record.
    - The NOTIFY trigger and trigger-fn are NOT installed (CRDB has no
      `pg_notify`). Patch v1.0.0 only creates the table + index + seed.

  Lifecycle:
    1. Patch `1.0.0` creates table and seeds the default `:database`
       connector.
    2. The `:synthigy.iam/connector` lifecycle module instantiates a
       CockroachCredentialsProvider on start and registers it with
       `synthigy.iam.connector/set-credentials-provider!`."
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.db :refer [*db*]]
   [synthigy.db.cockroach]  ; load Cockroach JDBCBackend impl
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.connector :as connector]
   [synthigy.json :as json]
   [synthigy.log :as log])
  (:import
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

(defrecord CockroachCredentialsProvider
  [datasource cache poller-thread poller-stop poll-ms]
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
    ;; CRDB has no LISTEN/NOTIFY. Cross-node refresh is a periodic
    ;; cache invalidation: every `poll-ms` we drop the local cache so
    ;; the next `-list-chain` call re-reads from the DB. Window of
    ;; staleness is bounded by `poll-ms` (default 5s). Re-read is cheap
    ;; (single SELECT, sub-millisecond, chain is small).
    (let [stop? (atom false)
          thread
          (doto
           (Thread.
            ^Runnable
            (fn []
              (try
                (loop []
                  (when-not @stop?
                    (try
                      (Thread/sleep (long poll-ms))
                      (when-not @stop?
                        (log/debug {:id ::poll-tick}
                                   "Polling cache invalidation")
                        (connector/-refresh! this))
                      (catch InterruptedException _ (reset! stop? true))
                      (catch Throwable ex
                        (log/warn {:id ::poll-error :error ex}
                                  "Poll iteration error; sleeping then retrying")
                        (try (Thread/sleep 1000)
                             (catch InterruptedException _ (reset! stop? true)))))
                    (recur)))
                (catch Throwable ex
                  (log/warn {:id ::poll-loop-exited :error ex}
                            "Poll loop exited")))))
            (.setName "synthigy.iam.connector.cockroach-poller")
            (.setDaemon true)
            (.start))]
      (reset! poller-stop stop?)
      (reset! poller-thread thread))
    nil)

  (-stop! [_]
    (when-let [stop? @poller-stop] (reset! stop? true))
    (when-let [^Thread t @poller-thread] (.interrupt t))
    (reset! cache nil)
    nil))

(defn make-provider
  "Construct a CockroachCredentialsProvider for the given Cockroach `db`.

   Optional `:poll-ms` (default 5000) controls how often the cache is
   invalidated — lower values shrink the cross-node staleness window
   at the cost of slightly more DB read traffic per node."
  ([db]
   (make-provider db {}))
  ([db {:keys [poll-ms] :or {poll-ms 5000}}]
   (->CockroachCredentialsProvider
    (:datasource db)
    (atom nil)
    (atom nil)
    (atom nil)
    poll-ms)))

;; =============================================================================
;; Patcho component — table, trigger, default seed
;; =============================================================================

(defn- ensure-table!
  "Create __iam_auth_connector table + index + default seed if missing.
  CRDB version omits the NOTIFY trigger (no `pg_notify` available);
  cross-node cache refresh is done by the poller in -start!.
  Idempotent — guards against drift where __component_versions__ records
  the patch as installed but the table was dropped externally."
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
  ;; Forward-migrate legacy tables to xid-only. CRDB (unlike PG) can't DROP a
  ;; PRIMARY KEY column directly — repoint the PK to xid first, then drop euuid.
  ;; Best-effort: fresh tables are already xid-PK, and CRDB has 0 prod installs,
  ;; so any failure here (already-migrated, or unsupported) is non-fatal.
  (try
    (jdbc/execute! ds
      ["ALTER TABLE __iam_auth_connector ALTER PRIMARY KEY USING COLUMNS (xid)"])
    (jdbc/execute! ds
      ["ALTER TABLE __iam_auth_connector DROP COLUMN IF EXISTS euuid"])
    (catch Throwable e
      (log/debug {:id ::euuid-migration-skipped :data {:error (.getMessage e)}}
                 "euuid column migration skipped (already xid-only or unsupported)")))
  (jdbc/execute! ds
    ["CREATE INDEX IF NOT EXISTS __iam_auth_connector_chain_idx
        ON __iam_auth_connector (enabled, priority)"])
  (jdbc/execute! ds
    ["INSERT INTO __iam_auth_connector (xid, name, type, priority, enabled, config)
      SELECT ?, 'Local database', 'database', 1000, TRUE, '{}'::jsonb
      WHERE NOT EXISTS (SELECT 1 FROM __iam_auth_connector)"
     (id/generate-xid)]))

(patch/current-version :synthigy.iam/connector "1.0.0")

(patch/upgrade :synthigy.iam/connector
               "1.0.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::installing-v100 :data {:action :installing :subject :iam-connector :version "1.0.0"}}
                           "Installing __iam_auth_connector v1.0.0 (CRDB — no trigger)")
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
           (when (instance? synthigy.db.Cockroach *db*)
             (log/info {:id ::provider-starting}
                       "Starting Cockroach credentials provider (polling)")
             (patch/level! :synthigy.iam/connector)
             (ensure-table! (:datasource *db*))
             (connector/set-credentials-provider! (make-provider *db*))
             (log/info {:id ::provider-ready}
                       "Provider ready")))
  :stop (fn []
          (log/info {:id ::provider-stopping}
                    "Stopping Cockroach credentials provider")
          (when (instance? CockroachCredentialsProvider connector/*credentials-provider*)
            (connector/-stop! connector/*credentials-provider*)))})
