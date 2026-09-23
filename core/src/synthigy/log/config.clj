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

(ns synthigy.log.config
  "DB-backed log routing configuration, hot-reloaded from a single-row table."
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

(defn ->json [v] (json/write-value-as-string v))
(defn <-json [s] (when s (json/read-value s mapper)))

;;; ============================================================================
;;; DB schema
;;; ============================================================================

(defn ensure-table!
  "Create the synthigy_log_config table if it does not exist."
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

(defn parse-level [s]
  (let [lc (some-> s str str/lower-case str/trim)]
    (when (valid-levels lc) (keyword lc))))

(defn wire-handler->internal
  [m]
  (when (map? m)
    {:min-level (parse-level (:level m))
     :ns-filter (when-not (str/blank? (str (:ns m))) (:ns m))}))

(defn internal-handler->wire
  [{:keys [min-level ns-filter]}]
  {:level (some-> min-level name)
   :ns    ns-filter})

(defn wire->internal
  "Convert admin wire format to the internal routing map for
   `synthigy.log/apply-routing!`."
  [{:keys [root_level ns_overrides store]}]
  (cond-> {:root-level   (parse-level root_level)
           :ns-overrides (mapv (fn [{:keys [pattern level]}]
                                 [pattern (parse-level level)])
                               (or ns_overrides []))}
    (some? store)   (assoc :store   (wire-handler->internal store))))

(defn internal->wire
  "Convert an internal routing snapshot back to admin wire format."
  [{:keys [root-level ns-overrides store source]}]
  (cond-> {:root_level   (some-> root-level name)
           :ns_overrides (mapv (fn [[pattern level]]
                                 {:pattern pattern :level (some-> level name)})
                               (or ns-overrides []))
           :source       (some-> source name)}
    (some? store)   (assoc :store   (internal-handler->wire store))))

;;; ============================================================================
;;; Read / write
;;; ============================================================================

(defn get-config
  "Stored config map (wire format), or nil if none saved."
  []
  (when-let [row (execute-one!
                   ["SELECT config, updated_at FROM synthigy_log_config WHERE id = 1"])]
    (let [config-str (or (get row "config") (get row :config))]
      (when config-str (<-json config-str)))))

(defn set-config!
  "Persist `config` (wire format) and apply immediately."
  [config]
  (let [json-str   (->json config)
        updated-at (str (java.time.Instant/now))
        actor      (log/current-principal-xid)]
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
  "Delete the stored config row and revert routing to env defaults."
  []
  (let [actor (log/current-principal-xid)]
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
  [{:keys [root_level ns_overrides store]}]
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

    (and store (not (map? store)))
    "store must be an object"

    (let [lvl (:level store)]
      (and lvl (nil? (parse-level lvl))))
    (str "store.level must be one of " valid-levels " or null")))

;;; ============================================================================
;;; Polling loop
;;; ============================================================================

(def ^:private default-poll-ms 10000)

(defn env-poll-ms []
  (or (when-let [s (env :synthigy-log-poll-interval-ms)]
        (try (Long/parseLong (str/trim s)) (catch Throwable _ nil)))
      default-poll-ms))

(defonce ^:private poll-thread (atom nil))

(defn poll-loop [interval-ms]
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
  "Start the background polling thread, replacing any existing one."
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
  "Interrupt and discard the polling thread."
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
            ;; Best-effort — a transient DB error here must not gate dataset
            ;; startup.
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
