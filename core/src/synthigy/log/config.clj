(ns synthigy.log.config
  "DB-backed log routing configuration.

  Stores root-level, per-namespace overrides, and per-handler routing
  (console + store) in a single-row database table. On startup the
  `:synthigy/log.config` Patcho module reads the stored config and applies
  it as an overlay on top of the env-var bootstrap that `:synthigy/log`
  already applied.

  A polling loop checks the table every `poll-interval-ms` (default 10 s,
  override via `SYNTHIGY_LOG_POLL_INTERVAL_MS`) and applies changes
  immediately when `updated_at` advances — no restart required.

  The table is DB-agnostic: SQLite uses TEXT, PostgreSQL stores the same
  TEXT column. Both backends support the `ON CONFLICT` UPSERT syntax
  required by the single-row idiom.

  ## Public API

      (config/get-config)          ; current config from DB (nil if not set)
      (config/set-config! config)  ; write + apply immediately
      (config/clear-config!)       ; delete the stored row (revert to env)

  ## Wire format (admin API / DB storage)

      {:root_level   \"info\"
       :ns_overrides [{:pattern \"synthigy.dataset.*\" :level \"debug\"}]
       :console      {:level \"info\" :ns nil}
       :store        {:level \"warn\" :ns \"synthigy.*\"}}

  Either `:console` or `:store` (or both) may be omitted to leave that
  component's routing untouched. Setting a component's `:level` to nil
  clears its min-level override.

  This wire shape REPLACES the pre-observability `:sinks` map (which had
  one entry per registered Sink). With observability substrate, there are
  exactly two routing surfaces — the console handler and the store-bridge
  handler — so the wire mirrors that."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [jsonista.core :as json]
    [patcho.lifecycle :as lifecycle]
    [synthigy.db.sql :refer [execute! execute-one!]]
    [synthigy.log :as log]))

;;; ============================================================================
;;; JSON helpers
;;; ============================================================================

(def ^:private mapper
  (json/object-mapper {:decode-key-fn keyword}))

(defn- ->json [v] (json/write-value-as-string v))
(defn- <-json [s] (when s (json/read-value s mapper)))

;;; ============================================================================
;;; DB schema
;;; ============================================================================

(defn ensure-table!
  "Create the synthigy_log_config table if it does not exist. Idempotent."
  []
  (execute!
    ["CREATE TABLE IF NOT EXISTS synthigy_log_config
      (id      INTEGER NOT NULL DEFAULT 1 PRIMARY KEY,
       config  TEXT    NOT NULL,
       updated_at TEXT NOT NULL,
       CONSTRAINT synthigy_log_config_singleton CHECK (id = 1))"]))

;;; ============================================================================
;;; Wire ↔ internal conversion
;;; ============================================================================

(def ^:private valid-levels
  #{"trace" "debug" "info" "warn" "error" "fatal"})

(defn- parse-level [s]
  (let [lc (some-> s str str/lower-case str/trim)]
    (when (valid-levels lc) (keyword lc))))

(defn- wire-handler->internal
  "Convert a wire handler routing map ({:level :ns}) to internal shape
   ({:min-level :ns-filter}). Returns nil for nil/empty input so callers
   can detect 'no change'."
  [m]
  (when (map? m)
    {:min-level (parse-level (:level m))
     :ns-filter (when-not (str/blank? (str (:ns m))) (:ns m))}))

(defn- internal-handler->wire
  [{:keys [min-level ns-filter]}]
  {:level (some-> min-level name)
   :ns    ns-filter})

(defn wire->internal
  "Convert admin wire format to the internal routing map accepted by
   `synthigy.log/apply-routing!`."
  [{:keys [root_level ns_overrides console store]}]
  (cond-> {:root-level   (parse-level root_level)
           :ns-overrides (mapv (fn [{:keys [pattern level]}]
                                 [pattern (parse-level level)])
                               (or ns_overrides []))}
    (some? console) (assoc :console (wire-handler->internal console))
    (some? store)   (assoc :store   (wire-handler->internal store))))

(defn internal->wire
  "Convert the internal routing snapshot (from `log/routing-snapshot`)
   back to admin wire format."
  [{:keys [root-level ns-overrides console store source]}]
  (cond-> {:root_level   (some-> root-level name)
           :ns_overrides (mapv (fn [[pattern level]]
                                 {:pattern pattern :level (some-> level name)})
                               (or ns-overrides []))
           :source       (some-> source name)}
    (some? console) (assoc :console (internal-handler->wire console))
    (some? store)   (assoc :store   (internal-handler->wire store))))

;;; ============================================================================
;;; Read / write
;;; ============================================================================

(defn get-config
  "Stored config map (wire format, keyword keys), or nil if no config
   has been saved yet."
  []
  (when-let [row (execute-one!
                   ["SELECT config, updated_at FROM synthigy_log_config WHERE id = 1"])]
    (let [config-str (or (get row "config") (get row :config))]
      (when config-str (<-json config-str)))))

(defn- current-actor-xid
  "xid of the principal currently bound, or nil. Uses `resolve` to avoid
   compile-time coupling to IAM."
  []
  (try
    (when-let [v (resolve 'synthigy.iam.access/*principal*)]
      (some-> @v :xid))
    (catch Throwable _ nil)))

(defn set-config!
  "Persist `config` (wire format) and apply immediately. Emits
   `:synthigy.log.config/config-changed` with the actor (when IAM is on)."
  [config]
  (let [json-str   (->json config)
        updated-at (str (java.time.Instant/now))
        actor      (current-actor-xid)]
    (execute!
      ["INSERT INTO synthigy_log_config (id, config, updated_at) VALUES (1, ?, ?)
        ON CONFLICT (id) DO UPDATE SET config = excluded.config,
                                       updated_at = excluded.updated_at"
       json-str updated-at])
    (log/apply-routing! (wire->internal config))
    (log/info {:id ::config-changed
               :data (cond-> {:action       :changed
                              :subject      :log-config
                              :root-level   (:root_level config)
                              :ns-overrides (vec (:ns_overrides config))}
                       actor (assoc :actor-xid actor))}
              "Log config changed")
    config))

(defn clear-config!
  "Delete the stored config row and revert routing to env defaults.
   Idempotent. Emits `:synthigy.log.config/config-cleared`."
  []
  (let [actor (current-actor-xid)]
    (execute! ["DELETE FROM synthigy_log_config WHERE id = 1"])
    (log/revert-to-env-routing!)
    (log/info {:id ::config-cleared
               :data (cond-> {:action  :cleared
                              :subject :log-config}
                       actor (assoc :actor-xid actor))}
              "Log config cleared, reverted to env defaults")
    nil))

;;; ============================================================================
;;; Validation
;;; ============================================================================

(defn validate-config
  "Return nil on success, or a string describing the first validation error."
  [{:keys [root_level ns_overrides console store]}]
  (cond
    (and root_level (nil? (parse-level root_level)))
    (str "root_level must be one of " valid-levels "; got " (pr-str root_level))

    (and ns_overrides (not (sequential? ns_overrides)))
    "ns_overrides must be an array"

    (some (fn [{:keys [pattern level]}]
            (or (str/blank? pattern)
                (nil? (parse-level level))))
          (or ns_overrides []))
    "each ns_overrides entry must have a non-blank 'pattern' and a valid 'level'"

    (and console (not (map? console)))
    "console must be an object"

    (and store (not (map? store)))
    "store must be an object"

    (let [lvl (:level console)]
      (and lvl (nil? (parse-level lvl))))
    (str "console.level must be one of " valid-levels " or null")

    (let [lvl (:level store)]
      (and lvl (nil? (parse-level lvl))))
    (str "store.level must be one of " valid-levels " or null")))

;;; ============================================================================
;;; Polling loop
;;; ============================================================================

(def ^:private default-poll-ms 10000)

(defn- env-poll-ms []
  (or (when-let [s (env :synthigy-log-poll-interval-ms)]
        (try (Long/parseLong (str/trim s)) (catch Throwable _ nil)))
      default-poll-ms))

(defonce ^:private poll-thread (atom nil))

(defn- poll-loop [interval-ms]
  (loop [last-updated nil]
    (try (Thread/sleep interval-ms) (catch InterruptedException _ nil))
    (when-not (.isInterrupted (Thread/currentThread))
      (let [row (try
                  (execute-one!
                    ["SELECT config, updated_at FROM synthigy_log_config WHERE id = 1"])
                  (catch Throwable _ nil))
            config-str  (when row (or (get row "config") (get row :config)))
            updated-at  (when row (or (get row "updated_at") (get row :updated_at)))]
        (cond
          (and config-str updated-at (not= updated-at last-updated))
          (try
            (log/apply-routing! (wire->internal (<-json config-str)))
            (log/debug {:id ::config-reloaded
                        :data {:action :loaded :subject :log-config
                               :updated-at updated-at}}
                       "Log routing config reloaded from DB")
            (catch Throwable e
              (log/error! {:id ::config-reload-failed
                           :data {:action :loading :subject :log-config}
                           :msg "Failed to apply log config from DB"}
                          e)))

          (and last-updated (nil? updated-at))
          (try
            (log/revert-to-env-routing!)
            (log/debug {:id ::config-cleared
                        :data {:action :loaded :subject :log-config}}
                       "Log config row removed; reverted to env defaults")
            (catch Throwable e
              (log/error! {:id ::config-revert-failed
                           :data {:action :loading :subject :log-config}
                           :msg "Failed to revert log routing to env defaults"}
                          e))))
        (recur updated-at)))))

(defn start-polling!
  "Start the background polling thread. Replaces any existing thread."
  ([] (start-polling! (env-poll-ms)))
  ([interval-ms]
   (when-let [t @poll-thread]
     (.interrupt t)
     (reset! poll-thread nil))
   (let [t (Thread.
             ^Runnable #(poll-loop interval-ms)
             "synthigy-log-config-poll")]
     (.setDaemon t true)
     (.start t)
     (reset! poll-thread t))))

(defn stop-polling!
  "Interrupt and discard the polling thread. Idempotent."
  []
  (when-let [t @poll-thread]
    (.interrupt t)
    (reset! poll-thread nil))
  nil)

;;; ============================================================================
;;; Lifecycle module
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/log.config
  {:depends-on [:synthigy/database]
   :doc "DB-backed log routing — levels + destinations, hot-reloaded"
   :start (fn []
            (log/info {:id ::starting
                       :data {:action :starting :subject :log-config}}
                      "Starting log config module")
            ;; Best-effort: this module rides with the data plane
            ;; (:synthigy/dataset depends on it), so a transient DB error
            ;; here MUST NOT gate dataset startup. Degrade to the env-var
            ;; bootstrap that :synthigy/log already applied; the poll loop
            ;; (self-guarded) will pick up DB config once it's reachable.
            (try
              (ensure-table!)
              (when-let [cfg (get-config)]
                (log/apply-routing! (wire->internal cfg))
                (log/info {:id ::config-loaded
                           :data {:action :loaded :subject :log-config}}
                          "Log routing config loaded from DB"))
              (catch Throwable e
                (log/warn {:id ::config-load-failed
                           :error e
                           :data {:action :starting :subject :log-config}}
                          "Log config load failed — running on env defaults; polling will retry")))
            (start-polling!)
            (log/info {:id ::started
                       :data {:action :started :subject :log-config}}
                      "Log config polling started"))
   :stop (fn []
           (stop-polling!)
           (log/info {:id ::stopped
                      :data {:action :stopped :subject :log-config}}
                     "Log config polling stopped"))})
