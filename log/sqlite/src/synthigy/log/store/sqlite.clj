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

(ns synthigy.log.store.sqlite
  "Local log + traffic store in a byte-capped SQLite file."
  (:require
   [patcho.patch :as patch]
   [synthigy.log :as log]
   [synthigy.log.store.protocol :as store]
   [synthigy.log.store.ring :as ring]
   [synthigy.log.store.sqlite.conn :as conn]
   [synthigy.log.store.sqlite.logs :as logs]
   [synthigy.log.store.sqlite.traffic :as sqlite.traffic]
   [synthigy.traffic :as traffic]))

(defonce last-traffic-prune (atom 0))

(defrecord SQLiteLogStore [tp config]
  store/LogStore
  (write-signal! [_ signal]
    (when (and tp signal) (tp signal)))

  (recent [this {:keys [limit ns-pattern level since]}]
    (store/search this (cond-> {:order-by [:inst :desc] :limit (or limit 100)}
                         ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                         level      (update :where (fnil assoc {}) :level level)
                         since      (assoc :since since))))

  (search [_ opts]
    (conn/with-conn [c (:logs-url config)]
      (logs/search c opts)))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [rows (store/search this (cond-> {:order-by [:inst :asc] :limit (or limit 100)}
                                    cursor     (assoc :since cursor)
                                    ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                                    level      (update :where (fnil assoc {}) :level level)))]
      {:rows rows :next-cursor (or (some-> (last rows) :inst) cursor)}))

  (clear! [_]
    (conn/with-conn [c (:logs-url config)]
      (conn/exec! c "DELETE FROM synthigy_logs"))
    nil)

  (health [_]
    (let [s (logs/transport-stats tp)]
      (merge s
             {:up?     (boolean (:running? s))
              :backend :sqlite
              :path    (:logs-path config)}
             (try (conn/with-conn [c (:logs-url config)]
                    {:rows (conn/row-count c "synthigy_logs") :bytes (conn/file-bytes c)})
                  (catch Throwable _ nil)))))

  (snapshot [_] nil)

  traffic/TrafficStore
  (flush-deltas! [_ rows]
    (try
      (conn/with-conn [c (:logs-url config)]
        (sqlite.traffic/insert! c rows)
        (let [now-m (quot (System/currentTimeMillis) 60000)]
          (when (> (- now-m @last-traffic-prune) 1440)
            (reset! last-traffic-prune now-m)
            (sqlite.traffic/prune! c now-m))))
      (catch Throwable _ nil))
    nil)

  (query-window [_ {:keys [from to]}]
    (try
      (conn/with-conn [c (:logs-url config)] (sqlite.traffic/window c from to))
      (catch Throwable _ [])))

  (traffic-health [_]
    {:up?     true
     :backend :sqlite
     :rows    (try (conn/with-conn [c (:logs-url config)] (conn/row-count c "traffic_minutes"))
                   (catch Throwable _ nil))}))

(defn open-store
  "Open the logs database, apply the schema and start the log writer."
  [config]
  (let [cfg (conn/resolve-url config)
        _   (conn/ensure-parent-dir! (:logs-path cfg))
        tp  (logs/make-transport cfg)]
    (conn/with-conn [c (:logs-url cfg)]
      (logs/apply-schema! c)
      (sqlite.traffic/apply-schema! c))
    (->SQLiteLogStore tp cfg)))

(defn close-store! [{:keys [tp]}]
  (try (logs/close-transport! tp) (catch Throwable _)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(defonce state (atom nil))

(defn setup []
  (let [cfg (conn/resolve-url (conn/env-config))]
    (conn/ensure-parent-dir! (:logs-path cfg))
    (conn/with-conn [c (:logs-url cfg)]
      (logs/apply-schema! c)
      (sqlite.traffic/apply-schema! c))
    (log/info {:id ::setup-complete
               :data {:action :setup-complete :subject :log-store :backend :sqlite :path (:logs-path cfg)}}
              "SQLite log store schema applied")))

(defn drain-snapshot! [outgoing incoming]
  (let [signals (try (store/snapshot outgoing) (catch Throwable _ nil))]
    (when (seq signals)
      (doseq [sig signals]
        (try (store/write-signal! incoming sig) (catch Throwable _)))
      (log/info {:id ::ring-drained
                 :data {:action :written :subject :log-store :backend :sqlite :signals (count signals)}}
                (str "Drained " (count signals) " buffered signal(s) into the SQLite log store")))))

(defn start []
  (let [cfg      (conn/env-config)
        rec      (open-store cfg)
        outgoing @#'store/*log-store*]
    (patch/level! :synthigy.log/sqlite)
    ;; swap before draining — draining first leaves a race the width of the drain
    (alter-var-root #'store/*log-store* (constantly rec))
    (drain-snapshot! outgoing rec)
    (alter-var-root #'traffic/*traffic-store* (constantly rec))
    (reset! state {:record rec})
    (log/info {:id ::started
               :data {:action :started :subject :log-store :backend :sqlite :path (:logs-path cfg)}}
              "SQLite log store started")))

(defn stop []
  (alter-var-root #'traffic/*traffic-store* (constantly (traffic/create-in-memory-store)))
  (when-let [{:keys [record]} @state]
    (close-store! record))
  (alter-var-root #'store/*log-store* (constantly (ring/create-ring)))
  (reset! state nil)
  (log/info {:id ::stopped :data {:action :stopped :subject :log-store :backend :sqlite}}
            "SQLite log store stopped"))

(defn cleanup []
  (let [cfg (conn/resolve-url (conn/env-config))]
    (conn/with-conn [c (:logs-url cfg)]
      (logs/drop-schema! c)
      (sqlite.traffic/drop-schema! c))))

(patch/current-version :synthigy.log/sqlite "1.0.0")
