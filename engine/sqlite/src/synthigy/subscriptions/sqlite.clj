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

(ns synthigy.subscriptions.sqlite
  "`:synthigy/plug` lifecycle module, SQLite impl — owns the SQLite delta plug."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.log :as log]
    [synthigy.plug.sqlite :as plug]
    [synthigy.plug.wake :as wake]))

(def ^:private model-watch-key ::plug-reconcile)

(defn start []
  ;; never compare Class objects — a :reload-all of synthigy.db leaves *db* on a stale Class
  (when-not (= "synthigy.db.SQLite" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/subscriptions (sqlite impl) requires SQLite backend"
                    {:db-class (class db/*db*)})))
  (when (instance? synthigy.plug.wake.Noop wake/*wake-source*)
    (alter-var-root #'wake/*wake-source* (constantly (wake/local-channel)))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "LocalChannel"}}
              "Default SQLite wake source installed"))
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
             :data {:action :started :subject :plug :backend :sqlite}}
            "SQLite plug started"))

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
             :data {:action :stopped :subject :plug :backend :sqlite}}
            "SQLite plug stopped"))

(lifecycle/register-module!
  :synthigy/plug
  {:depends-on [:synthigy/dataset]
   :doc "Change-event plug — in-process channel fan-out"
   :start start
   :stop  stop})
