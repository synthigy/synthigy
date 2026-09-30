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

(defn attach!
  "Start every part that isn't running, then the :synthigy/dataline unit once all are."
  []
  (doseq [p parts :when (not (lifecycle/started? p))] (start-part! p))
  (when (and (every? lifecycle/started? parts) (not (lifecycle/started? :synthigy/dataline)))
    (lifecycle/start! :synthigy/dataline)))

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
                             (try (attach!) (refresh!)
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

(lifecycle/register-module!
 :synthigy/dataline
 {:depends-on parts
  :doc "Dataline — audit history, logs and traffic"
  :start (fn [])
  :stop  (fn [])})
