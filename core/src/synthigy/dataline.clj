;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataline
  "Dataline — the audit, log-store and traffic modules as one unit: started together, watched, reattached and reported."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.audit.protocol :as audit]
   [synthigy.log :as log]
   [synthigy.log.store.protocol :as store])
  (:import
   [java.time Duration Instant]))

(def parts
  "One implementation of each is on every classpath."
  [:synthigy/audit :synthigy.log/store :synthigy/traffic])

(def watch-ms 30000)

;; a few held audit deltas are normal between drainer and persister ticks
(def held-grace-ms 60000)

(defonce health-state (atom {:status :starting :issues []}))

(defonce failing-parts (atom #{}))

(defonce log-drops (atom {:dropped 0 :rising-at nil}))

(defonce watcher (atom nil))

(defn start-part!
  "Start one part, logging a failure once per outage and the recovery when it comes back."
  [part]
  (try
    (lifecycle/start! part)
    (when (contains? @failing-parts part)
      (swap! failing-parts disj part)
      (log/info {:id ::part-reattached :data {:action :started :subject :dataline :module part}}
                "Dataline part reattached"))
    true
    (catch Throwable t
      (when-not (contains? @failing-parts part)
        (swap! failing-parts conj part)
        (log/error! {:id ::part-failed
                     :msg "Dataline part failed to start — the server keeps serving, retrying every 30s"
                     :data {:action :starting :subject :dataline :module part}}
                    t))
      false)))

(defn logs-dropping?
  "True while a durable log store keeps dropping signals; the in-memory ring evicts by design."
  [{:keys [backend dropped]}]
  (let [now (System/currentTimeMillis)
        n   (long (or dropped 0))
        {:keys [rising-at] prev :dropped} @log-drops]
    (cond
      (= :ring backend) false
      (> n (long prev)) (do (reset! log-drops {:dropped n :rising-at now}) true)
      :else (boolean (and rising-at (< (- now rising-at) (* 2 watch-ms)))))))

(defn compute-health []
  (let [down     (vec (remove lifecycle/started? parts))
        {:keys [failing-since error]} @audit/persist-state
        pending  (try (some-> audit/*pending-stats* (apply [])) (catch Throwable _ nil))
        held     (long (or (:held pending) 0))
        ^Instant oldest (:oldest pending)
        age-ms   (when oldest (.toMillis (Duration/between oldest (Instant/now))))
        lh       (try (some-> store/*log-store* store/health) (catch Throwable _ nil))
        dropping (logs-dropping? lh)
        issues   (cond-> []
                   (seq down)
                   (conj {:kind :parts-down :parts (mapv str down)})
                   failing-since
                   (conj {:kind :audit-failing :since (str failing-since) :error error})
                   (and (not failing-since) (pos? held) age-ms (> age-ms held-grace-ms))
                   (conj {:kind :audit-held})
                   dropping
                   (conj {:kind :logs-dropped :dropped (:dropped lh)}))]
    {:status     (if (empty? issues) :ok :degraded)
     :issues     issues
     :audit      {:held held :oldest (some-> oldest str)}
     :logs       {:backend (:backend lh) :dropped (:dropped lh)}
     :checked-at (str (Instant/now))}))

(defn refresh! []
  (let [h (compute-health)]
    (when (not= (:status h) (:status @health-state))
      (if (= :ok (:status h))
        (log/info {:id ::healthy :data {:action :ready :subject :dataline}} "Dataline healthy")
        (log/warn {:id ::degraded :data {:action :degraded :subject :dataline :issues (:issues h)}}
                  "Dataline degraded")))
    (reset! health-state h)))

(defn health
  "The last computed Dataline health; the watcher refreshes it every 30s."
  []
  @health-state)

(defn healthy? []
  (= :ok (:status @health-state)))

(defn ready?
  "A part's dependencies are all running — the watcher never starts a dependency someone stopped."
  [part]
  (every? lifecycle/started? (:depends-on (lifecycle/module-info part))))

(defn attach!
  "Start every part that isn't running; a failure is logged, never thrown."
  []
  (doseq [p parts :when (not (lifecycle/started? p))] (start-part! p)))

(defn reattach!
  "The watcher's attach!: only parts whose dependencies are still up."
  []
  (doseq [p parts :when (and (not (lifecycle/started? p)) (ready? p))] (start-part! p)))

(defn stop-watcher! []
  (when-let [^Thread t @watcher]
    (reset! watcher nil)
    (.interrupt t)))

(defn start-watcher! []
  (when-not @watcher
    (let [t (Thread. ^Runnable
                     (fn []
                       (loop []
                         (when (= (Thread/currentThread) @watcher)
                           (when (try (Thread/sleep (long watch-ms)) true (catch InterruptedException _ false))
                             (try (reattach!) (refresh!)
                                  (catch Throwable t
                                    (log/error! {:id ::watch-failed :data {:action :verifying :subject :dataline}} t)))
                             (recur)))))
                     "synthigy-dataline-watch")]
      (.setDaemon t true)
      (reset! watcher t)
      (.start t))))

(defn start!
  "Start Dataline — a part that fails never stops the server; the watcher keeps retrying it."
  []
  (attach!)
  (refresh!)
  (start-watcher!))

;; no :depends-on — a hard dependency would make one failing part throw the whole boot
(lifecycle/register-module!
 :synthigy/dataline
 {:doc "Dataline — audit history, logs and traffic"
  :start start!
  :stop stop-watcher!})
