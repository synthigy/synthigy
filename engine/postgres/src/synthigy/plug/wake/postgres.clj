;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.plug.wake.postgres
  "PostgreSQL LISTEN/NOTIFY wake source for the plug drainer."
  (:require
    [next.jdbc :as jdbc]
    [synthigy.db.postgres :as db.postgres]
    [synthigy.log :as log]
    [synthigy.plug.wake :as wake])
  (:import
    [java.sql Connection]
    [org.postgresql PGConnection]))

(defn open-listen!
  "Open a dedicated connection and LISTEN on `channel`; `{:conn :pg-conn}`."
  [datasource channel]
  (let [^Connection conn (db.postgres/listen-connection datasource)]
    (try
      (jdbc/execute! conn [(str "LISTEN " channel)])
      {:conn conn :pg-conn (.unwrap conn PGConnection)}
      (catch Throwable e
        (try (.close conn) (catch Throwable _))
        (throw e)))))

(defn reconnect!
  "Swap the broken LISTEN connection in `state` for a fresh one, sleeping `timeout-ms` when that fails."
  [datasource channel state timeout-ms broken]
  (try (.close ^Connection (:conn broken)) (catch Throwable _))
  (try
    (let [fresh (open-listen! datasource channel)]
      ;; never swap onto a state stop-source! already changed — close ours instead
      (if (compare-and-set! state broken (assoc fresh :stopped? false))
        (do (log/info {:id ::listen-reconnected
                       :data {:action :reconnected :subject :wake-source :channel channel}}
                      "PostgresNotify listening again")
            :poll)
        (do (.close ^Connection (:conn fresh))
            :stop)))
    (catch Throwable e
      (log/warn {:id ::reconnect-failed
                 :data {:channel channel :retry-ms timeout-ms}}
                (.getMessage e))
      (try (Thread/sleep (long timeout-ms)) :poll
           (catch InterruptedException _ :stop)))))

(defrecord PostgresNotify [datasource channel state]
  wake/WakeSource

  (start-source! [_]
    (when-not @state
      (reset! state (assoc (open-listen! datasource channel) :stopped? false))
      (log/info {:id ::listen-started
                 :data {:action :started :subject :wake-source
                        :channel channel}}
                "PostgresNotify listening"))
    nil)

  (wait! [_ timeout-ms]
    (let [{:keys [^PGConnection pg-conn stopped?] :as s} @state]
      (cond
        (or (nil? s) stopped?) :stop
        :else (try
                (let [notifs (.getNotifications pg-conn (int timeout-ms))]
                  (if (and notifs (pos? (alength notifs)))
                    :wakeup
                    :poll))
                (catch InterruptedException _
                  :stop)
                (catch Throwable e
                  (if (not (identical? s @state))
                    :stop
                    (do (log/warn {:id ::listen-error
                                   :data {:channel channel}}
                                  (.getMessage e))
                        (reconnect! datasource channel state timeout-ms s))))))))

  (signal! [_]
    nil)

  (stop-source! [_]
    (when-let [{:keys [^Connection conn] :as s} @state]
      (reset! state (assoc s :stopped? true))
      (try (.close conn) (catch Throwable _))
      (reset! state nil)
      (log/info {:id ::listen-stopped
                 :data {:action :stopped :subject :wake-source
                        :channel channel}}
                "PostgresNotify closed"))
    nil))

(defn postgres-notify
  "PostgresNotify on `channel` (default synthigy_delta_ready, must match the trigger's pg_notify)."
  ([datasource]
   (postgres-notify datasource "synthigy_delta_ready"))
  ([datasource channel]
   (->PostgresNotify datasource channel (atom nil))))
