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
    (if (seq @start-modules)
      ;; a failure answers {:success false …}, never throws — the daemon keeps the warm engine for diagnosis
      (try
        (progress-begin! :boot "starting")
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
        (finally (when (progress-running? :boot) (progress-finish!))))
      (throw (ex-info "no start modules configured — serve! was called without :modules"
                      {:code -32000})))))

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

(register-method! "modules"
  (fn [_]
    {:modules
     (vec (for [[topic {:keys [status depends-on doc error]}] (sort-by key (:modules (lifecycle/system-report)))]
            (cond-> {:topic (str topic)
                     :status (name status)
                     :depends-on (mapv str depends-on)}
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
                             (when-let [resp (dispatch (json/read-str line))]
                               (write-frame! resp))
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
