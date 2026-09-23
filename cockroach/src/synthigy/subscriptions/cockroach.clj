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

(ns synthigy.subscriptions.cockroach
  "Patcho lifecycle module — `:synthigy/subscriptions (cockroach impl)`.

   CRDB owner of the delta plug. Lifecycle shape mirrors
   `synthigy.subscriptions.sqlite` because CRDB and SQLite share the
   polling-drainer + in-process `*drain-wakeup*` pattern (no LISTEN/NOTIFY
   on either side). Backend-specific differences (jsonb_build_object vs
   json_object, GUC propagation vs `_ctx` table, plpgsql functions vs
   inline trigger bodies) live in `synthigy.plug.cockroach`.

   On :start, reconciles plug triggers against the currently-deployed
   model, registers a `dataset/add-model-watch!` so every `save-model!`
   re-reconciles, and starts the drainer thread. On :stop, removes the
   watch, stops the drainer, and drops every plug trigger + function
   via `plug/strip-all!`."
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
  "Public so dataset/start can autostart us without going through Patcho
   (which would re-enter dataset/start via the dep check and loop)."
  []
  (when-not (= "synthigy.db.Cockroach" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/subscriptions (cockroach impl) requires Cockroach backend"
                    {:db-class (class db/*db*)})))
  ;; Install the CRDB-appropriate default wake source unless an operator
  ;; has already plugged in something else (NATS, Composite, …). Mirrors
  ;; the `audit/*audit-provider*` pattern: DB-natural default, override
  ;; by alter-var-root.
  (when (instance? synthigy.plug.wake.Noop wake/*wake-source*)
    (alter-var-root #'wake/*wake-source* (constantly (wake/local-channel)))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "LocalChannel"}}
              "Default CRDB wake source installed"))
  (plug/reconcile-plug! db/*db* (dataset/deployed-model))
  ;; Diff reconcile (CRDB-only): on every save-model! the watch compares
  ;; the previous trigger-fingerprint against the new model and only
  ;; (re)installs triggers for entities/relations whose trigger-relevant
  ;; shape actually changed; removed ones get their triggers + emit
  ;; functions explicitly dropped. A v2 deploy that adds one entity
  ;; reinstalls one entity's triggers, not all N. Sqlite/postgres still
  ;; do the full O(N) reconcile because their per-trigger DDL is cheap;
  ;; CRDB pays ~100ms per CREATE TRIGGER through the declarative schema
  ;; changer, so the diff matters here.
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

(defn- stop []
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
