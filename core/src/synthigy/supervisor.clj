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

(ns synthigy.supervisor
  "JSON-RPC-over-stdio supervision channel, the same contract for a `clj -M` process and a released jar."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]
    [synthigy.boot :as boot]
    [synthigy.dataline :as dataline]
    [synthigy.env :as senv]
    [synthigy.json :as json]
    [synthigy.log :as log]))

(defonce ^:private method-table (atom {}))

(defonce ^:private start-modules (atom nil))

(defonce ^:private activity (atom nil))

(defonce start-error (atom nil))

(defn progress
  "The one long-running operation this engine is doing, or `{:op nil}`."
  []
  (or @activity {:op nil}))

(defn progress-running?
  ([] (some? @activity))
  ([op] (= op (:op @activity))))

(defn progress-begin!
  "Claim the activity slot; false when an operation is already running."
  [op phase]
  (compare-and-set! activity nil {:op op :phase phase :started-at (java.util.Date.)}))

(defn progress-update!
  [m]
  (swap! activity #(when % (merge % m)))
  nil)

(defn progress-step!
  [step of phase]
  (progress-update! {:step step :of of :phase phase :detail nil :patch nil :done nil :total nil}))

(defn progress-phase!
  "Enter a new phase, dropping the previous phase's detail and counters."
  [phase]
  (progress-update! {:phase phase :detail nil :patch nil :done nil :total nil}))

(defn progress-patching!
  "Announce a patcho topic about to level; a topic already current clears the line."
  [topic from to]
  (progress-update! {:detail nil :done nil :total nil
                     :patch (when (not= (str from) (str to))
                              {:topic (str topic) :from (str from) :to (str to)})}))

(defn progress-finish! []
  (reset! activity nil)
  nil)

(defn register-method!
  "Register `(fn [params] result)` for `method`; throw ex-info with `:code` to answer an error."
  [method handler]
  (swap! method-table assoc method handler)
  nil)

(defn unregister-method! [method]
  (swap! method-table dissoc method)
  nil)

(defn dispatch
  "One JSON-RPC request map in, a response map out; nil for a notification."
  [{:keys [id method params]}]
  (if-let [handler (get @method-table method)]
    (try
      (let [result (handler params)]
        (when id {:jsonrpc "2.0" :id id :result result}))
      (catch clojure.lang.ExceptionInfo e
        (when id
          {:jsonrpc "2.0" :id id
           :error {:code (or (:code (ex-data e)) -32603) :message (ex-message e)}}))
      (catch Throwable e
        (when id
          {:jsonrpc "2.0" :id id
           :error {:code -32603 :message (or (ex-message e) "internal error")}})))
    (when id
      {:jsonrpc "2.0" :id id
       :error {:code -32601 :message (str "method not found: " method)}})))

(register-method! "ping" (fn [_] {:pong true}))

(register-method! "host.alert"
  (fn [{:keys [resource low availableBytes totalBytes at]}]
    (let [data {:subject :host :resource (keyword resource) :state (if low :low :ok)
                :available-bytes availableBytes :total-bytes totalBytes :at at}
          msg (format "Host %s %s: %.1f GiB free of %.1f GiB" resource (if low "is running low" "is fine again")
                      (/ (double (or availableBytes 0)) 1073741824.0) (/ (double (or totalBytes 0)) 1073741824.0))]
      (if low
        (log/warn {:id ::host-low :data data} msg)
        (log/info {:id ::host-ok :data data} msg)))
    nil))

;; --- db.probe: param-driven backend probes -------------------------------

(defonce ^:private db-probes (atom {}))

(defn register-db-probe! [backend f]
  (swap! db-probes assoc backend f)
  nil)

(register-method! "db.probe"
  (fn [{:keys [db] :as params}]
    (if-let [f (get @db-probes (keyword (or db "postgres")))]
      (f params)
      (throw (ex-info (str "no db probe for backend '" db "' on this classpath")
                      {:code -32602})))))

(defn env-list [k]
  (seq (remove str/blank? (str/split (or (env k) "") #"[\s,]+"))))

(defn app-modules
  "The SYNTHIGY_MODULES start list, or nil when unset."
  []
  (some->> (env-list :synthigy-modules)
           (mapv #(keyword (str/replace % #"^:" "")))))

(register-method! "server.start"
  (fn [_]
    ;; two modules of one dimension boot fine and then answer with the wrong backend — refuse
    (when-let [dupes (not-empty (boot/duplicate-backends))]
      (throw (ex-info (str "Classpath carries more than one backend module: "
                           (str/join ", " (map (fn [[kind n]] (str n " " kind)) dupes))
                           ". Exactly one engine, one audit, one log-store and one server adapter module belong "
                           "on a launch classpath — the JVM would silently run whichever "
                           "comes first.")
                      {:code -32000 :duplicates dupes})))
    (cond
      (empty? @start-modules)
      (throw (ex-info "no start modules configured — serve! was called without :modules"
                      {:code -32000}))

      (not (progress-begin! :boot "starting"))
      {:success false :error (str "busy: " (:phase (progress)))}

      :else
      ;; a failure answers {:success false …}, never throws — the daemon keeps the warm engine for diagnosis
      (try
        (boot/start! (or (app-modules) @start-modules)
                     {:on-phase progress-phase! :namespaces (env-list :synthigy-require)})
        (reset! start-error nil)
        {:success true :message "Server started"}
        (catch Throwable t
          (reset! start-error (boot/root-cause-message t))
          (if (:legacy (ex-data t))
            {:success false :legacy true :message (ex-message t)}
            (do (log/error! {:id ::server-start-failed
                             :msg "server.start failed — engine stays warm for diagnosis"
                             :data {:action :starting :subject :http-server}}
                            t)
                {:success false :error (boot/root-cause-message t)})))
        (finally (progress-finish!))))))

(register-method! "server.stop"
  (fn [_]
    (run! lifecycle/stop! (filter lifecycle/module-info (cons (first @start-modules) (app-modules))))
    {:success true :message "Server stopped"}))

(register-method! "server.status"
  (fn [_]
    (let [running? (boolean (some-> (first @start-modules) lifecycle/started?))]
      {:running running?
       :observability (dataline/healthy?)
       :dataline (dataline/health)
       :console (when (lifecycle/module-info :synthigy/console)
                  (lifecycle/started? :synthigy/console))
       :message (if running? "Server is running" "Server is stopped")})))

(defonce restartable (atom #{}))

(defn restartable!
  "Let the portal stop, start and restart `topic` on its own; only for modules the server never needs."
  [topic]
  (swap! restartable conj topic)
  nil)

(defn restartable? [topic]
  (boolean (and (contains? @restartable topic) (lifecycle/module-info topic))))

(defn module-topic
  "The restartable module `params` names, or an error."
  [{:keys [module]}]
  (let [topic (keyword (str/replace (str module) #"^:" ""))]
    (when-not (restartable? topic)
      (throw (ex-info (str "not a module that restarts on its own: " module) {:code -32602})))
    topic))

(defn env-overrides
  "`env` (\"SYNTHIGY_FILES_X\" -> value) as setting keys; only the module's own SYNTHIGY_<NAME>_ keys."
  [topic env]
  (let [prefix (str "SYNTHIGY_" (str/upper-case (str/replace (name topic) "-" "_")) "_")]
    (into {}
          (for [[k v] env :let [k (name k)]]
            (if (str/starts-with? k prefix)
              [(keyword (str/lower-case (str/replace k "_" "-"))) (str v)]
              (throw (ex-info (str k " does not belong to " topic) {:code -32602})))))))

(defn dependents
  "Started modules that need `topic`."
  [topic]
  (filterv #(and (not= % topic) (lifecycle/started? %) (contains? (boot/closure %) topic))
           (keys (:modules (lifecycle/system-report)))))

(defn missing-dependencies
  "Direct dependencies of `topic` that are not running."
  [topic]
  (vec (sort (remove lifecycle/started? (:depends-on (lifecycle/module-info topic))))))

(defn refuse-cold-start
  "A refusal while a dependency of `topic` is down, so a warm engine never boots behind server.start."
  [topic]
  (when-let [missing (not-empty (missing-dependencies topic))]
    {:success false :module (str topic)
     :error (str topic " needs " (str/join ", " missing) " running first; its settings apply when it starts")}))

(defn module-op!
  "Run `f` in the activity slot; a failure answers {:success false}, never throws."
  [op topic f]
  (if-not (progress-begin! op (str op " " topic))
    {:success false :error (str "busy: " (:op (progress)))}
    (try
      (f)
      {:success true :module (str topic) :started (lifecycle/started? topic)}
      (catch Throwable t
        (log/error! {:id ::module-op-failed
                     :msg "Module operation failed — the engine keeps running"
                     :data {:action op :subject :module :module topic}}
                    t)
        {:success false :module (str topic) :error (boot/root-cause-message t)})
      (finally (progress-finish!)))))

(defonce stopped-with (atom {}))

(defn start-with-dependents!
  "Start `topic`, then try each of `after` even when it fails."
  [topic after]
  (try
    (lifecycle/start! topic)
    (finally
      (doseq [m after]
        (try (lifecycle/start! m) (catch Throwable _ nil))))))

(defn with-not-started [result after]
  (cond-> result
    (seq after) (assoc :not-started (mapv str (remove lifecycle/started? after)))))

(register-method! "module.stop"
  (fn [params]
    (let [topic (module-topic params)
          after (dependents topic)]
      (module-op! :stopping topic
                  (fn []
                    (lifecycle/stop! topic)
                    (swap! stopped-with update topic (fnil into []) after))))))

(register-method! "module.start"
  (fn [params]
    (let [topic (module-topic params)
          overrides (env-overrides topic (:env params))
          after (get @stopped-with topic [])]
      (swap! senv/overrides merge overrides)
      (with-not-started
        (or (refuse-cold-start topic)
            (module-op! :starting topic
                        (fn []
                          (start-with-dependents! topic after)
                          (swap! stopped-with dissoc topic))))
        after))))

(register-method! "module.restart"
  (fn [params]
    (let [topic (module-topic params)
          overrides (env-overrides topic (:env params))
          after (into (dependents topic) (get @stopped-with topic))]
      (swap! senv/overrides merge overrides)
      (with-not-started
        (or (refuse-cold-start topic)
            (module-op! :restarting topic
                        (fn []
                          (lifecycle/stop! topic)
                          (start-with-dependents! topic after)
                          (swap! stopped-with dissoc topic))))
        after))))

(register-method! "modules"
  (fn [_]
    {:modules
     (vec (for [[topic {:keys [status depends-on doc error]}] (sort-by key (:modules (lifecycle/system-report)))]
            (cond-> {:topic (str topic)
                     :status (name status)
                     :depends-on (mapv str depends-on)
                     :restartable (restartable? topic)}
              doc (assoc :doc doc)
              error (assoc :error (:message error)
                           :error-at (str (.toInstant ^java.util.Date (:timestamp error)))))))
     :start-error @start-error}))

(register-method! "shutdown"
  (fn [_]
    (log/warn {:id ::shutdown-requested
               :data {:action :stopping :subject :admin}}
              "Shutdown requested via supervision channel")
    (future (Thread/sleep 1000) (System/exit 0))
    {:success true :message "Shutdown initiated"}))

(defn write-frame!
  "One JSON-RPC message as a single flushed line on *out*; locked so frames never interleave."
  [msg]
  (locking System/out
    (println (json/write-str msg))
    (flush)))

(defonce pending (atom {}))

(defonce call-counter (atom 0))

(defn call-portal
  "Ask the supervising portal to run `method`; answers its result, throws its error or after `timeout-ms`."
  [method params timeout-ms]
  (when-not (log/supervised?)
    (throw (ex-info "No portal supervises this engine" {:code :no-portal})))
  (let [id (str "engine-" (swap! call-counter inc))
        answer (promise)]
    (swap! pending assoc id answer)
    (try
      (write-frame! {:jsonrpc "2.0" :id id :method method :params params})
      (let [{:keys [result error] :as msg} (deref answer timeout-ms ::timeout)]
        (cond
          (= ::timeout msg) (throw (ex-info (str "The portal did not answer " method " in time") {:code :portal-timeout}))
          error (throw (ex-info (str (:message error)) {:code :portal-error :rpc error}))
          :else result))
      (finally (swap! pending dissoc id)))))

(defn answer!
  "Hand a portal response to the call waiting for it; false when nobody is."
  [{:keys [id] :as msg}]
  (if-let [p (get @pending id)]
    (do (deliver p msg) true)
    false))

(defn response? [msg]
  (and (contains? msg :id) (not (contains? msg :method))))

(defonce ^:private serving (atom nil))

(defn serve!
  "Start the stdio read-dispatch-respond loop on a daemon thread; a no-op if already running."
  ([] (serve! nil))
  ([{:keys [modules]}]
   (when modules
     (reset! start-modules (vec modules)))
   (try (require 'synthigy.supervisor.probe) (catch Throwable _))
   (when (nil? @serving)
     (let [rdr (java.io.BufferedReader.
                 (java.io.InputStreamReader. System/in "UTF-8"))
           t (Thread.
               (fn []
                 (loop []
                   (if-let [line (.readLine rdr)]
                     (do
                       (when-not (str/blank? line)
                         ;; each frame on its own thread — a long verb must never block ping/doctor
                         (future
                           (try
                             (let [msg (json/read-str line)]
                               (if (response? msg)
                                 (answer! msg)
                                 (when-let [resp (dispatch msg)]
                                   (write-frame! resp))))
                             (catch Throwable e
                               (log/error! {:id ::frame-failed
                                            :msg "Failed to handle supervision frame"
                                            :data {:action :reading :subject :supervisor}}
                                           e)))))
                       (recur))
                     (when (log/supervised?)
                       (System/exit 0)))))
               "synthigy-supervisor")]
       (.setDaemon t true)
       (.start t)
       (reset! serving t)))
   nil))

(defn stop!
  "Interrupts the serve! thread. Idempotent."
  []
  (when-let [t @serving]
    (.interrupt ^Thread t))
  (reset! serving nil)
  nil)
