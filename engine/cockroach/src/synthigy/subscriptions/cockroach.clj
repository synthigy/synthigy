;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.subscriptions.cockroach
  "The :synthigy/plug lifecycle module for CockroachDB: owns the delta plug and its drainer."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.log :as log]
    [synthigy.plug.cockroach :as plug]
    [synthigy.plug.wake :as wake]))

(def ^:private model-watch-key ::plug-reconcile)

(defn start
  "Starts the plug; public because dataset's :start calls it directly (patcho would loop)."
  []
  (when-not (= "synthigy.db.Cockroach" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/subscriptions (cockroach impl) requires Cockroach backend"
                    {:db-class (class db/*db*)})))
  (when (instance? synthigy.plug.wake.Noop wake/*wake-source*)
    (alter-var-root #'wake/*wake-source* (constantly (wake/local-channel)))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "LocalChannel"}}
              "Default CRDB wake source installed"))
  (plug/reconcile-plug! db/*db* (dataset/deployed-model))
  (let [last-fp (atom (plug/trigger-fingerprint (dataset/deployed-model)))]
    (dataset/add-model-watch!
      model-watch-key
      (fn [_k _ref _old new-model]
        (try
          (let [new-fp (plug/reconcile-plug-diff! db/*db* @last-fp new-model)]
            (when new-fp (reset! last-fp new-fp)))
          (catch Throwable e
            (log/error! {:id ::reconcile-on-watch-failed
                         :data {:action :reconciling :subject :plug}}
                        e))))))
  (plug/start-drainer! db/*db*)
  (log/info {:id ::started
             :data {:action :started :subject :plug :backend :cockroach}}
            "CockroachDB plug started"))

(defn stop []
  (dataset/remove-model-watch! model-watch-key)
  (plug/stop-drainer! db/*db*)
  (try
    (jdbc/with-transaction [tx (:datasource db/*db*)]
      (plug/strip-all! db/*db* tx))
    (catch Throwable e
      (log/warn {:id ::strip-on-stop-failed
                 :data {:action :stopping :subject :plug}}
                (.getMessage e))))
  (log/info {:id ::stopped
             :data {:action :stopped :subject :plug :backend :cockroach}}
            "CockroachDB plug stopped"))

(lifecycle/register-module!
  :synthigy/plug
  {:depends-on [:synthigy/dataset]
   :doc "Change-event plug — polling drainer + wake channel"
   :start start
   :stop  stop})
