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

(ns synthigy.subscriptions.sqlite
  "Patcho lifecycle module — `:synthigy/subscriptions (sqlite impl)`.

   SQLite mirror of `:synthigy/subscriptions (postgres impl)`. Owns the SQLite
   delta plug. On :start, reconciles plug triggers against the
   currently-deployed model, registers a `dataset/add-model-watch!` so
   every `save-model!` re-reconciles, and starts the drainer thread.
   On :stop, removes the watch, stops the drainer, and drops every
   plug trigger via `plug/strip-all!` (decision a from the
   plan — clean removal).

   Mirrors `synthigy.subscriptions.postgres` in shape and lifecycle
   semantics. Backend-specific differences (LISTEN/NOTIFY vs polling +
   `*drain-wakeup*`, GUCs vs `_ctx` table) live in `synthigy.plug.sqlite`."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.log :as log]
    [synthigy.plug.sqlite :as plug]
    [synthigy.plug.wake :as wake]))

(def ^:private model-watch-key ::plug-reconcile)

(defn- start []
  ;; Compare by class name (string) so a `:reload-all` of synthigy.db
  ;; doesn't break the check — after reload the existing *db* instance
  ;; carries a class with the same FQN but a different Class object.
  (when-not (= "synthigy.db.SQLite" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/subscriptions (sqlite impl) requires SQLite backend"
                    {:db-class (class db/*db*)})))
  ;; Install the SQLite-appropriate default wake source unless the
  ;; operator has overridden. Mirrors the audit-provider pattern.
  (when (instance? synthigy.plug.wake.Noop wake/*wake-source*)
    (alter-var-root #'wake/*wake-source* (constantly (wake/local-channel)))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "LocalChannel"}}
              "Default SQLite wake source installed"))
  ;; Reconcile against whatever model is deployed right now. nil-safe.
  (plug/reconcile-plug! db/*db* (dataset/deployed-model))
  ;; Every subsequent save-model! re-reconciles. add-watch fires
  ;; synchronously inside the swap! that updates *deployed-model*, so
  ;; there's no race window for a freshly-deployed entity to be missing
  ;; its triggers.
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
  ;; fans out via delta/dispatch!, optionally invokes audit provider.
  (plug/start-drainer! db/*db*)
  (log/info {:id ::started
             :data {:action :started :subject :plug :backend :sqlite}}
            "SQLite plug started"))

(defn- stop []
  (dataset/remove-model-watch! model-watch-key)
  (plug/stop-drainer! db/*db*)
  ;; Clean removal (decision 1): strip every plug trigger.
  ;; Queue tables (__entity_delta_queue, __relation_delta_queue, _ctx)
  ;; stay — cheap, and re-:start reconciles triggers back.
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
