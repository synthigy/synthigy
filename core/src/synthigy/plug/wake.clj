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

(ns synthigy.plug.wake
  "Pluggable wake-signal source for the plug drainer — see
   docs/core/synthigy/plug/wake.md for the built-in implementations and the
   *wake-source* override pattern."
  (:require
    [clojure.core.async :as async]
    [synthigy.log :as log]))

;; ============================================================================
;; Protocol
;; ============================================================================

(defprotocol WakeSource
  (start-source! [this]
    "Initialize the transport. Idempotent.")
  (wait! [this timeout-ms]
    "Block up to timeout-ms; returns :wakeup, :poll, or :stop.")
  (signal! [this]
    "Nudge the drainer post-commit; a no-op when the transport pushes the wake itself.")
  (stop-source! [this]
    "Tear down the transport. Idempotent."))

;; Sentinel default for *wake-source*; sleeps in wait! so a mistakenly-parked
;; drainer doesn't spin.
(defrecord Noop []
  WakeSource
  (start-source! [_] nil)
  (wait!         [_ timeout-ms]
    (try (Thread/sleep (long timeout-ms)) (catch InterruptedException _))
    :poll)
  (signal!       [_] nil)
  (stop-source!  [_] nil))

(def ^:dynamic *wake-source*
  "The one active WakeSource for this process. Default is `Noop`.

   A plug's `subscriptions.<backend>` lifecycle module installs the
   backend-appropriate default at :start (PostgresNotify for postgres,
   LocalChannel for sqlite + cockroach) — unless an operator has
   already overridden via `alter-var-root` (typical when plugging in
   NATS / Kafka / changefeed / Composite).

   Tests can `binding` this to a mock that records `signal!` calls."
  (->Noop))

(defn signal-drainer!
  "Public seam for app-path code to nudge the drainer post-commit."
  []
  (signal! *wake-source*))

;; In-process core.async channel; single-node fanout only.
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

(defn local-channel []
  (->LocalChannel (atom nil)))

;; wait! races the sources; signal! fans to all so cross-node listeners (e.g.
;; NATS) wake too.
(defrecord Composite [sources]
  WakeSource
  (start-source! [_]
    (doseq [s sources] (start-source! s)))
  (wait! [_ timeout-ms]
    (let [slice (max 50 (long (/ timeout-ms (count sources))))
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
  "Construct a Composite wake source from N underlying sources."
  [& sources]
  (->Composite (vec sources)))
