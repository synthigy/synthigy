(ns synthigy.subscriptions.cockroach
  "Patcho lifecycle module — `:synthigy/subscriptions (cockroach impl)`.

   CRDB owner of the delta substrate. Lifecycle shape mirrors
   `synthigy.subscriptions.sqlite` because CRDB and SQLite share the
   polling-drainer + in-process `*drain-wakeup*` pattern (no LISTEN/NOTIFY
   on either side). Backend-specific differences (jsonb_build_object vs
   json_object, GUC propagation vs `_ctx` table, plpgsql functions vs
   inline trigger bodies) live in `synthigy.substrate.cockroach`.

   On :start, reconciles substrate triggers against the currently-deployed
   model, registers a `dataset/add-model-watch!` so every `save-model!`
   re-reconciles, and starts the drainer thread. On :stop, removes the
   watch, stops the drainer, and drops every substrate trigger + function
   via `substrate/strip-all!`."
  (:require
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.db :as db]
    [synthigy.log :as log]
    [synthigy.substrate.cockroach :as substrate]
    [synthigy.substrate.wake :as wake]))

(def ^:private model-watch-key ::substrate-reconcile)

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
  (when (instance? synthigy.substrate.wake.Noop wake/*wake-source*)
    (alter-var-root #'wake/*wake-source* (constantly (wake/local-channel)))
    (log/info {:id ::wake-source-defaulted
               :data {:action :installed :subject :wake-source
                      :impl "LocalChannel"}}
              "Default CRDB wake source installed"))
  (substrate/reconcile-substrate! db/*db* (dataset/deployed-model))
  ;; Diff reconcile (CRDB-only): on every save-model! the watch compares
  ;; the previous trigger-fingerprint against the new model and only
  ;; (re)installs triggers for entities/relations whose trigger-relevant
  ;; shape actually changed; removed ones get their triggers + emit
  ;; functions explicitly dropped. A v2 deploy that adds one entity
  ;; reinstalls one entity's triggers, not all N. Sqlite/postgres still
  ;; do the full O(N) reconcile because their per-trigger DDL is cheap;
  ;; CRDB pays ~100ms per CREATE TRIGGER through the declarative schema
  ;; changer, so the diff matters here.
  (let [last-fp (atom (substrate/trigger-fingerprint (dataset/deployed-model)))]
    (dataset/add-model-watch!
      model-watch-key
      (fn [_k _ref _old new-model]
        (try
          (let [new-fp (substrate/reconcile-substrate-diff! db/*db* @last-fp new-model)]
            (when new-fp (reset! last-fp new-fp)))
          (catch Throwable e
            (log/error! {:id ::reconcile-on-watch-failed
                         :data {:action :reconciling :subject :substrate}}
                        e))))))
  (substrate/start-drainer! db/*db*)
  (log/info {:id ::started
             :data {:action :started :subject :substrate :backend :cockroach}}
            "CockroachDB substrate started"))

(defn- stop []
  (dataset/remove-model-watch! model-watch-key)
  (substrate/stop-drainer! db/*db*)
  (try
    (jdbc/with-transaction [tx (:datasource db/*db*)]
      (substrate/strip-all! db/*db* tx))
    (catch Throwable e
      (log/warn {:id ::strip-on-stop-failed
                 :data {:action :stopping :subject :substrate}}
                (.getMessage e))))
  (log/info {:id ::stopped
             :data {:action :stopped :subject :substrate :backend :cockroach}}
            "CockroachDB substrate stopped"))

(lifecycle/register-module!
  :synthigy/substrate
  {:depends-on [:synthigy/dataset]
   :doc "Change-event substrate — polling drainer + wake channel"
   :start start
   :stop  stop})
