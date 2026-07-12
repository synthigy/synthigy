(ns synthigy.substrate.wake.postgres
  "PostgreSQL LISTEN/NOTIFY wake source for the substrate drainer.

   Holds a dedicated JDBC connection that LISTENs on a pg_notify
   channel. The substrate's audit triggers emit `pg_notify('<channel>',
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
    [synthigy.substrate.wake :as wake])
  (:import
    [java.sql Connection]
    [org.postgresql PGConnection]))

;; State held inside the record as a mutable atom rather than as
;; record fields. Keeps the record value semantically equal across
;; start/stop cycles (records compare structurally on field values).
;;
;; The atom carries a map {:conn ^Connection :pg-conn ^PGConnection
;; :stopped? bool}. nil before start, nil after stop.

(defrecord PostgresNotify [datasource channel state]
  wake/WakeSource

  (start-source! [_]
    (when-not @state
      (let [conn (db.postgres/listen-connection datasource)
            pg-conn (.unwrap conn PGConnection)]
        (try
          (jdbc/execute! conn [(str "LISTEN " channel)])
          (reset! state {:conn conn :pg-conn pg-conn :stopped? false})
          (log/info {:id ::listen-started
                     :data {:action :started :subject :wake-source
                            :channel channel}}
                    "PostgresNotify listening")
          (catch Throwable e
            (.close conn)
            (throw e)))))
    nil)

  (wait! [_ timeout-ms]
    (if-let [{:keys [^PGConnection pg-conn stopped?]} @state]
      (cond
        stopped? :stop
        :else    (try
                   (let [notifs (.getNotifications pg-conn (int timeout-ms))]
                     (if (and notifs (pos? (alength notifs)))
                       :wakeup
                       :poll))
                   (catch InterruptedException _
                     :stop)
                   (catch Throwable e
                     (log/warn {:id ::listen-error
                                :data {:channel channel}}
                               (.getMessage e))
                     ;; Treat transport hiccups as a safety poll —
                     ;; drainer ticks once, then retries the listen.
                     :poll)))
      :stop))

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
                  substrate trigger fires `pg_notify` on. Defaults to
                  `synthigy_delta_ready`."
  ([datasource]
   (postgres-notify datasource "synthigy_delta_ready"))
  ([datasource channel]
   (->PostgresNotify datasource channel (atom nil))))
