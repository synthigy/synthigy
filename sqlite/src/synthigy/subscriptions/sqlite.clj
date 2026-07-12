(ns synthigy.subscriptions.sqlite
  "Patcho lifecycle module — `:synthigy/subscriptions (sqlite impl)`.

   SQLite mirror of `:synthigy/subscriptions (postgres impl)`. Owns the SQLite
   delta substrate. On :start, reconciles substrate triggers against the
   currently-deployed model, registers a `dataset/add-model-watch!` so
   every `save-model!` re-reconciles, and starts the drainer thread.
   On :stop, removes the watch, stops the drainer, and drops every
   substrate trigger via `substrate/strip-all!` (decision a from the
   plan — clean removal).

   Mirrors `synthigy.subscriptions.postgres` in shape and lifecycle
   semantics. Backend-specific differences (LISTEN/NOTIFY vs polling +
   `*drain-wakeup*`, GUCs vs `_ctx` table) live in `synthigy.substrate.sqlite`."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.log :as log]
    [synthigy.substrate.sqlite :as substrate]
    [synthigy.substrate.wake :as wake]))

(def ^:private model-watch-key ::substrate-reconcile)

(defn- start []
  ;; Compare by class name (string) so a `:reload-all` of synthigy.db
  ;; doesn't break the check — after reload the existing *db* instance
  ;; carries a class with the same FQN but a different Class object.
  (when-not (= "synthigy.db.SQLite" (.getName (class db/*db*)))
    (throw (ex-info ":synthigy/subscriptions (sqlite impl) requires SQLite backend"
                    {:db-class (class db/*db*)})))
  ;; Install the SQLite-appropriate default wake source unless the
  ;; operator has overridden. Mirrors the audit-provider pattern.
  (when (instance? synthigy.substrate.wake.Noop wake/*wake-source*)
    (alter-var-root #'wake/*wake-source* (constantly (wake/local-channel)))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "LocalChannel"}}
              "Default SQLite wake source installed"))
  ;; Reconcile against whatever model is deployed right now. nil-safe.
  (substrate/reconcile-substrate! db/*db* (dataset/deployed-model))
  ;; Every subsequent save-model! re-reconciles. add-watch fires
  ;; synchronously inside the swap! that updates *deployed-model*, so
  ;; there's no race window for a freshly-deployed entity to be missing
  ;; its triggers.
  (dataset/add-model-watch!
    model-watch-key
    (fn [_k _ref _old new-model]
      (try
        (substrate/reconcile-substrate! db/*db* new-model)
        (catch Throwable e
          (log/error! {:id ::reconcile-on-watch-failed
                       :data {:action :reconciling :subject :substrate}}
                      e)))))
  ;; Drainer pulls from __entity_delta_queue + __relation_delta_queue,
  ;; fans out via delta/dispatch!, optionally invokes audit provider.
  (substrate/start-drainer! db/*db*)
  (log/info {:id ::started
             :data {:action :started :subject :substrate :backend :sqlite}}
            "SQLite substrate started"))

(defn- stop []
  (dataset/remove-model-watch! model-watch-key)
  (substrate/stop-drainer! db/*db*)
  ;; Clean removal (decision 1): strip every substrate trigger.
  ;; Queue tables (__entity_delta_queue, __relation_delta_queue, _ctx)
  ;; stay — cheap, and re-:start reconciles triggers back.
  (try
    (jdbc/with-transaction [tx (:datasource db/*db*)]
      (substrate/strip-all! db/*db* tx))
    (catch Throwable e
      (log/warn {:id ::strip-on-stop-failed
                 :data {:action :stopping :subject :substrate}}
                (.getMessage e))))
  (log/info {:id ::stopped
             :data {:action :stopped :subject :substrate :backend :sqlite}}
            "SQLite substrate stopped"))

(lifecycle/register-module!
  :synthigy/substrate
  {:depends-on [:synthigy/dataset]
   :doc "Change-event substrate — in-process channel fan-out"
   :start start
   :stop  stop})
