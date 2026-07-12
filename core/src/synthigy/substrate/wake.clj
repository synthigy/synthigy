(ns synthigy.substrate.wake
  "Pluggable wake-signal source for the substrate drainer.

   ## Pattern
   Same shape as `audit/*audit-provider*`: ONE dynvar `*wake-source*`,
   defaults to `Noop`. The active `:synthigy/subscriptions.<backend>`
   module installs its DB-natural default on :start if the var is still
   the Noop sentinel — exactly the same way the audit-provider pattern
   defaults to the DB-backed audit store unless ClickHouse alter-var-roots
   itself in. Operators override by alter-var-root'ing `*wake-source*`
   BEFORE starting the subscriptions module.

   ## Built-in implementations

   - `Noop`             — sentinel default. `signal!` is nil; `wait!` is
                          a sleep. Active until a subscriptions module
                          installs the real default.
   - `LocalChannel`     — in-process core.async channel; single-node
                          fanout. The default `:start` installs this
                          for SQLite + CRDB.
   - `PostgresNotify`   — JDBC connection LISTENing for `pg_notify` from
                          a trigger. The default `:start` installs this
                          for Postgres. Lives in
                          `synthigy.substrate.wake.postgres`.
   - `Composite`        — race N sources; signal fans to all. Use to
                          combine fast-local with cross-cluster:
                          `(composite (local-channel) (->NatsSource …))`.
   - User-defined       — NATS subject, Kafka topic, CRDB changefeed,
                          Redis Pub/Sub, etc. Implementers extend
                          `WakeSource` from their own ns; no synthigy-core
                          dep on their client libs.

   ## Drainer body

   All substrate backends share the same drainer body — only the
   WakeSource varies. The substrate's `:start` reads `*wake-source*` once
   and parks the drainer thread on `wait!`. App-path mutations call
   `signal-drainer!` post-commit to nudge."
  (:require
    [clojure.core.async :as async]
    [synthigy.log :as log]))

;; ============================================================================
;; Protocol
;; ============================================================================

(defprotocol WakeSource
  (start-source! [this]
    "Initialize the transport — open the channel, acquire the conn,
     subscribe to the subject. Idempotent: a second call returns
     immediately. Returns nil.")
  (wait! [this timeout-ms]
    "Block up to `timeout-ms` for a wake signal. Returns one of:
       :wakeup — a signal arrived (drain immediately)
       :poll   — timeout elapsed (drain as a safety net)
       :stop   — source closed; caller should exit its loop")
  (signal! [this]
    "Nudge the drainer that there's work pending. Called from app-path
     code post-commit. Implementations that rely on the DB or broker to
     push the wake (LISTEN/NOTIFY, NATS-driven) treat this as a no-op
     — the signal already happened upstream when the row was committed.")
  (stop-source! [this]
    "Tear down the transport. Idempotent. After this returns, `wait!`
     yields `:stop` and the drainer loop should exit."))

;; ============================================================================
;; Noop — does nothing. Sentinel default for `*wake-source*` before a
;; substrate's subscriptions module sets the real source. Implementing
;; the protocol (rather than using nil) means `signal!` calls from app
;; code never have to nil-check, and tests that don't start a substrate
;; still see a well-defined wake source.
;; ============================================================================

(defrecord Noop []
  WakeSource
  (start-source! [_] nil)
  (wait!         [_ timeout-ms]
    ;; If a drainer parks on this for real (it shouldn't — no
    ;; substrate :start ran), just sleep so it doesn't spin.
    (try (Thread/sleep (long timeout-ms)) (catch InterruptedException _))
    :poll)
  (signal!       [_] nil)
  (stop-source!  [_] nil))

(def ^:dynamic *wake-source*
  "The one active WakeSource for this process. Default is `Noop`.

   A substrate's `subscriptions.<backend>` lifecycle module installs the
   backend-appropriate default at :start (PostgresNotify for postgres,
   LocalChannel for sqlite + cockroach) — unless an operator has
   already overridden via `alter-var-root` (typical when plugging in
   NATS / Kafka / changefeed / Composite).

   Tests can `binding` this to a mock that records `signal!` calls."
  (->Noop))

(defn signal-drainer!
  "Public seam called by app-path code post-commit to nudge the
   drainer. Delegates to the active `*wake-source*`. Backend-specific
   `wake-drainer!` aliases in `substrate.<backend>` keep working but
   now just call this."
  []
  (signal! *wake-source*))

;; ============================================================================
;; LocalChannel — in-process core.async channel
;; ============================================================================
;;
;; Single-process wake. App-path writes call `signal!` which `offer!`s
;; onto the channel (dropping-buffer 1 coalesces concurrent wakes:
;; the drainer drains-to-empty per wake, so one wake covers N puts).
;; Cross-process / cross-node wake requires a different source.

(defrecord LocalChannel [chan-atom]
  WakeSource
  (start-source! [_]
    (when-not @chan-atom
      (reset! chan-atom (async/chan (async/dropping-buffer 1))))
    nil)
  (wait! [_ timeout-ms]
    (if-let [c @chan-atom]
      (let [t (async/timeout timeout-ms)
            [v port] (async/alts!! [c t])]
        (cond
          (= port t) :poll
          (some? v)  :wakeup
          :else      :stop))
      :stop))
  (signal! [_]
    (when-let [c @chan-atom]
      (async/offer! c :work)))
  (stop-source! [_]
    (when-let [c @chan-atom]
      (async/close! c)
      (reset! chan-atom nil))))

(defn local-channel
  "Construct a LocalChannel wake source.

   Use as the default for substrates with no native push transport
   (SQLite, CRDB without changefeeds). Multi-container deployments
   should compose this with a cross-node source like NATS so other
   nodes also wake on remote writes."
  []
  (->LocalChannel (atom nil)))

;; ============================================================================
;; Composite — combine multiple sources
;; ============================================================================
;;
;; `wait!` forwards to whichever underlying source fires first.
;; `signal!` fans to ALL sources so cross-node listeners (NATS) get
;; notified even though the in-proc channel would also be enough for
;; the local drainer. This lets a cluster combine "fast local + push
;; remote + slow safety-poll" behind a single seam.

(defrecord Composite [sources]
  WakeSource
  (start-source! [_]
    (doseq [s sources] (start-source! s)))
  (wait! [_ timeout-ms]
    ;; Race the sources. Each gets its own bounded wait; whichever
    ;; returns :wakeup first wins. If all return :poll, the composite
    ;; also returns :poll (safety-tick the drainer). :stop on any
    ;; closes the whole composite.
    (let [;; Each source runs its own wait in a future so we can
          ;; race them. Bound each to a small slice of the timeout so
          ;; no single slow source starves the others. This is a
          ;; conservative scheduler — refine later if hot.
          slice (max 50 (long (/ timeout-ms (count sources))))
          futures (mapv (fn [s] (future (wait! s slice))) sources)
          start (System/nanoTime)
          deadline-ns (+ start (* timeout-ms 1000000))]
      (loop []
        (let [done (some (fn [^java.util.concurrent.Future f]
                           (when (.isDone f) f))
                         futures)
              now (System/nanoTime)]
          (cond
            done                    (let [v @done]
                                      (doseq [^java.util.concurrent.Future f futures
                                              :when (not= f done)]
                                        (.cancel f true))
                                      (case v
                                        :stop :stop
                                        :wakeup :wakeup
                                        :poll (recur)))
            (>= now deadline-ns)    (do
                                      (doseq [^java.util.concurrent.Future f futures]
                                        (.cancel f true))
                                      :poll)
            :else                   (do (Thread/sleep 5) (recur)))))))
  (signal! [_]
    (doseq [s sources]
      (try (signal! s)
           (catch Throwable e
             (log/warn {:id ::composite-signal-failed} (.getMessage e))))))
  (stop-source! [_]
    (doseq [s sources]
      (try (stop-source! s)
           (catch Throwable _)))))

(defn composite
  "Construct a Composite wake source from N underlying sources. Fires
   on whichever wakes first; signal fans to all."
  [& sources]
  (->Composite (vec sources)))
