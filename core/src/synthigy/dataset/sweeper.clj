;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.sweeper
  "Clears expired row locks: sweeps on start, then sleeps to the next expiry, one to five minutes."
  (:require
   [synthigy.db :as db]
   [synthigy.log :as log])
  (:import
   [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(def min-ms 60000)
(def max-ms 300000)

(defonce state (atom nil))

(defn sweep!
  "Runs one sweep; returns ms until the next one."
  []
  (try
    (let [secs (when (satisfies? db/RecordLockProtocol db/*db*)
                 (db/sweep-locks db/*db*))]
      (if secs
        (-> (long (* 1000 secs)) (max min-ms) (min max-ms))
        max-ms))
    (catch Throwable e
      (log/error! {:id ::sweep-failed
                   :msg "Record lock sweep failed"
                   :data {:action :cleanup :subject :record-lock}}
                  e)
      min-ms)))

(defn run [^LinkedBlockingQueue wakes stopped]
  (loop []
    (when-not @stopped
      (let [swept (System/currentTimeMillis)
            delay (sweep!)]
        (when (.poll wakes delay TimeUnit/MILLISECONDS)
          (loop []
            (.clear wakes)
            (let [left (- (+ swept min-ms) (System/currentTimeMillis))]
              (when (and (pos? left) (not @stopped))
                (.poll wakes left TimeUnit/MILLISECONDS)
                (recur)))))
        (recur)))))

(defn wake!
  "Asks a sleeping sweeper to reschedule after a claim."
  []
  (some-> ^LinkedBlockingQueue (:wakes @state) (.offer ::claim)))

(defn start! []
  (when-not @state
    (let [wakes (LinkedBlockingQueue.)
          stopped (atom false)
          thread (doto (Thread. ^Runnable (bound-fn [] (run wakes stopped)) "synthigy-lock-sweeper")
                   (.setDaemon true))]
      (reset! state {:wakes wakes :stopped stopped :thread thread})
      (.start thread)
      (log/info {:id ::started :data {:action :started :subject :record-lock}} "Record lock sweeper started"))))

(defn stop! []
  (when-let [{:keys [wakes stopped ^Thread thread]} @state]
    (reset! stopped true)
    (.offer ^LinkedBlockingQueue wakes ::stop)
    (.join thread 5000)
    (reset! state nil)
    (log/info {:id ::stopped :data {:action :stopped :subject :record-lock}} "Record lock sweeper stopped")))
