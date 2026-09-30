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

(ns synthigy.subscriptions.postgres
  "Patcho lifecycle module — `:synthigy/plug (postgres impl)`.

   Owns the PG delta plug. On :start, reconciles plug triggers +
   emit functions against the currently-deployed model, registers a
   `dataset/add-model-watch!` so every `save-model!` re-reconciles, and
   starts the drainer thread. On :stop, removes the watch, stops the
   drainer, and drops the plug triggers + emit functions (decision 1
   from PLUG_OWNERSHIP_PLAN — clean removal).

   This is the OSS plug-owner module. Consumers (SSE subscriptions,
   the observability plug, future Pro consumers) sit on top of the
   drainer's `delta/dispatch!`; this module doesn't know about them.

   Backend-specific Patcho module: depends on `:synthigy/dataset`,
   validates that *db* is Postgres at start."
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

(defn- start []
  ;; Compare by class name (string) so a `:reload-all` of synthigy.db
  ;; doesn't break the check — after reload the existing *db* instance
  ;; carries a class with the same FQN but a different Class object.
  (when-not (= "synthigy.db.Postgres" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/plug (postgres impl) requires Postgres backend"
                    {:db-class (class db/*db*)})))
  ;; Install the PG-natural default wake source (PostgresNotify, LISTENing
  ;; on the same channel the plug trigger fires pg_notify on) unless
  ;; an operator has overridden — typical override is a Composite for
  ;; cross-cluster fanout. Mirrors the audit-provider pattern: DB-natural
  ;; default, override by alter-var-root.
  (when (instance? synthigy.plug.wake.Noop wake/*wake-source*)
    ;; LISTEN holds one connection for the full lifetime of the plug;
    ;; route it through the drainer pool so it doesn't permanently consume
    ;; a writer-pool slot.
    (let [ds (or (:drainer-datasource db/*db*) (:datasource db/*db*))]
      (alter-var-root #'wake/*wake-source*
                      (constantly (wake-pg/postgres-notify ds))))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "PostgresNotify"}}
              "Default Postgres wake source installed"))
  ;; Reconcile against whatever is deployed right now. nil-safe: on a cold
  ;; boot before any model has loaded, no-op; the watch below picks up the
  ;; first save-model! once it arrives.
  (plug/reconcile-plug! db/*db* (dataset/deployed-model))
  ;; Every subsequent save-model! re-reconciles. add-watch fires
  ;; synchronously inside the swap! that updates *deployed-model*, so there's
  ;; no race window for a freshly-deployed entity to be missing its triggers.
  (dataset/add-model-watch!
    model-watch-key
    (fn [_k _ref _old new-model]
      (try
        (plug/reconcile-plug! db/*db* new-model)
        (catch Throwable e
          (log/error! {:id ::reconcile-on-watch-failed
                       :data {:action :reconciling :subject :plug}}
                      e)))))
  ;; Drainer pulls from __entity_delta_queue + __relation_delta_queue,
  ;; fans out via delta/dispatch! to live subscribers, optionally invokes
  ;; the audit provider if one is bound.
  (plug/start-drainer! db/*db*)
  (log/info {:id ::started
             :data {:action :started :subject :plug :backend :postgres}}
            "Postgres plug started"))

(defn- stop []
  (dataset/remove-model-watch! model-watch-key)
  (plug/stop-drainer! db/*db*)
  ;; Clean removal (decision 1): strip every plug trigger + emit
  ;; function. Returns the schema to bare-server state. Queue tables stay
  ;; — empty UNLOGGED tables are cheap, and re-:start reconciles them back.
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
