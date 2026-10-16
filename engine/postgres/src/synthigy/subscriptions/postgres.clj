;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.subscriptions.postgres
  "`:synthigy/plug` lifecycle module, Postgres impl — owns the PG delta plug."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.log :as log]
    [synthigy.plug.postgres :as plug]
    [synthigy.plug.wake :as wake]
    [synthigy.plug.wake.postgres :as wake-pg]))

(def ^:private model-watch-key ::plug-reconcile)

(defn start []
  ;; never compare Class objects — a :reload-all of synthigy.db leaves *db* on a stale Class
  (when-not (= "synthigy.db.Postgres" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/plug (postgres impl) requires Postgres backend"
                    {:db-class (class db/*db*)})))
  (when (instance? synthigy.plug.wake.Noop wake/*wake-source*)
    (let [ds (or (:drainer-datasource db/*db*) (:datasource db/*db*))]
      (alter-var-root #'wake/*wake-source*
                      (constantly (wake-pg/postgres-notify ds))))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "PostgresNotify"}}
              "Default Postgres wake source installed"))
  (plug/reconcile-plug! db/*db* (dataset/deployed-model))
  (dataset/add-model-watch!
    model-watch-key
    (fn [_k _ref _old new-model]
      (try
        (plug/reconcile-plug! db/*db* new-model)
        (catch Throwable e
          (log/error! {:id ::reconcile-on-watch-failed
                       :data {:action :reconciling :subject :plug}}
                      e)))))
  (plug/start-drainer! db/*db*)
  (log/info {:id ::started
             :data {:action :started :subject :plug :backend :postgres}}
            "Postgres plug started"))

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
             :data {:action :stopped :subject :plug :backend :postgres}}
            "Postgres plug stopped"))

(lifecycle/register-module!
  :synthigy/plug
  {:depends-on [:synthigy/dataset]
   :doc "Change-event plug — LISTEN/NOTIFY fan-out"
   :start start
   :stop  stop})
