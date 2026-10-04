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
  "PostgreSQL LISTEN/NOTIFY wake source for the plug drainer.

   Holds a dedicated JDBC connection that LISTENs on a pg_notify
   channel. The plug's audit triggers emit `pg_notify('<channel>',
   '')` after every queue insert, so the database itself signals all
   listeners — `signal!` is a no-op on the app side.

   This is the canonical PG wake source. Operators who want fanout
   beyond a single Postgres cluster (e.g. cross-region replicas reading
   off a logical replication slot) should use a different transport
   (NATS / Kafka)."
  (:require
    [next.jdbc :as jdbc]
    [synthigy.db.postgres :as db.postgres]
    [synthigy.log :as log]
    [synthigy.plug.wake :as wake])
  (:import
    [java.sql Connection]
    [org.postgresql PGConnection]))

;; State held inside the record as a mutable atom rather than as
;; record fields. Keeps the record value semantically equal across
;; start/stop cycles (records compare structurally on field values).
;;
;; The atom carries a map {:conn ^Connection :pg-conn ^PGConnection
;; :stopped? bool}. nil before start, nil after stop.

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
    ;; No-op: the trigger emits pg_notify when a queue row is inserted.
    ;; The DB delivers the signal to every LISTENing connection in the
    ;; cluster. App code calling wake-drainer! is a friendly no-op here.
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
  "Construct a PostgresNotify wake source.

   Args:
     datasource — HikariDataSource. start-source! opens a dedicated raw
                  connection off-pool (via db.postgres/listen-connection)
                  using its JDBC coordinates, held until stop-source! — so
                  the permanent LISTEN never consumes a pool slot.
     channel    — LISTEN channel name. Must match the channel the
                  plug trigger fires `pg_notify` on. Defaults to
                  `synthigy_delta_ready`."
  ([datasource]
   (postgres-notify datasource "synthigy_delta_ready"))
  ([datasource channel]
   (->PostgresNotify datasource channel (atom nil))))
