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

(ns synthigy.supervisor
  "JSON-RPC-over-stdio supervision channel — the protocol half of
   docs/plans/PLAN-PORTAL-SUPERVISOR.md. Lives in core, not a server
   backend or the uberjar entry point, so a `clj -M` dev process is
   supervisable on the same contract as a released jar: `synthigy
   supervise -- clj -M:...:dev` and a supervised `java -jar` both just call
   `serve!` over their inherited stdin/stdout."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset.id :as id]
    [synthigy.json :as json]
    [synthigy.log :as log]))

(defonce ^:private method-table (atom {}))

(defonce ^:private start-modules (atom nil))

(defonce ^:private activity (atom nil))

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
  "Handler is (fn [params] result); params is the request's :params, already
   keywordized. Throw ex-info with :code (a JSON-RPC error code, default
   -32603) to answer with an error instead of a result."
  [method handler]
  (swap! method-table assoc method handler)
  nil)

(defn unregister-method! [method]
  (swap! method-table dissoc method)
  nil)

(defn dispatch
  "One JSON-RPC request/notification map (keywordized) in, a response map
   out — or nil for a notification (no :id) and for anything else that gets
   no reply."
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
;; Backends register a probe (postgres/sqlite); the verb classifies what is
;; behind a set of credentials/a path — empty / foreign tables / current
;; Synthigy (with pending patch upgrades) / legacy euuid — WITHOUT touching
;; the engine's own lifecycle or env. This is the operator console's Test
;; button speaking Synthigy's own knowledge instead of driver heuristics.

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

(defn observability-disabled?
  "SYNTHIGY_OBSERVABILITY_ENGINE=none — bundled or not, the operator said no."
  []
  (= "none" (env :synthigy-observability-engine)))

(defn start-observability! []
  (try (require 'synthigy.observability) (catch Throwable _))
  (when (and (not (observability-disabled?))
             (lifecycle/module-info :synthigy/observability))
    (try
      (lifecycle/start! :synthigy/observability)
      (catch Throwable t
        (log/error! {:id ::observability-failed
                     :msg "Observability start failed — continuing server-only"
                     :data {:action :starting :subject :observability}}
                    t)))))

(defn root-cause-message [^Throwable t]
  (loop [e t]
    (if-let [c (.getCause e)] (recur c) (or (ex-message e) (str (class e))))))

(def shadowed-resources
  {"database" "synthigy/core__init.class"
   "observability" "synthigy/observability__init.class"})

(defn duplicate-backends
  "Backend kinds appearing more than once on the classpath, as {kind count}.
   Each database module ships its own `synthigy.core` and each observability
   module its own `synthigy.observability`, so two of a kind means the JVM
   silently runs whichever came first."
  ([] (duplicate-backends (.getContextClassLoader (Thread/currentThread))))
  ([^ClassLoader loader]
   (into {}
         (keep (fn [[kind resource]]
                 (let [n (count (enumeration-seq (.getResources loader resource)))]
                   (when (> n 1) [kind n]))))
         shadowed-resources)))

(register-method! "server.start"
  (fn [_]
    ;; A classpath carrying two database (or two observability) modules boots
    ;; fine and then answers with the wrong backend — refuse instead.
    (when-let [dupes (not-empty (duplicate-backends))]
      (throw (ex-info (str "Classpath carries more than one backend module: "
                           (str/join ", " (map (fn [[kind n]] (str n " " kind)) dupes))
                           ". Exactly one database and one observability module belong "
                           "on a launch classpath — the JVM would silently run whichever "
                           "comes first.")
                      {:code -32000 :duplicates dupes})))
    (if-let [modules (seq @start-modules)]
      ;; Dataset first, alone: its boot detection sets the id provider, and a
      ;; LEGACY euuid layout must never have IAM/OAuth setup plowed into it —
      ;; the one-time migrate.xid verb (and a restart) is the only way forward.
      ;; A lifecycle failure answers {:success false :error …}, never a thrown
      ;; error — the daemon must keep supervising the warm engine so the
      ;; operator console can show the failure (a sealed encryption boot, say)
      ;; and offer the fix, instead of the whole daemon dying.
      (try
        (progress-begin! :boot "starting :synthigy/dataset")
        (lifecycle/start! :synthigy/dataset)
        (if (= :euuid (id/key))
          {:success false :legacy true
           :message (str "Legacy euuid-keyed database detected — Synthigy is "
                         "xid-native. Run the migration before starting the server.")}
          (do (progress-phase! "starting :synthigy/server")
              (apply lifecycle/start! modules)
              (progress-phase! "starting observability")
              (start-observability!)
              {:success true :message "Server started"}))
        (catch Throwable t
          (log/error! {:id ::server-start-failed
                       :msg "server.start failed — engine stays warm for diagnosis"
                       :data {:action :starting :subject :http-server}}
                      t)
          {:success false :error (root-cause-message t)})
        (finally (when (progress-running? :boot) (progress-finish!))))
      (throw (ex-info "no start modules configured — serve! was called without :modules"
                      {:code -32000})))))

(register-method! "server.stop"
  (fn [_]
    (lifecycle/stop! (first @start-modules))
    {:success true :message "Server stopped"}))

(register-method! "server.status"
  (fn [_]
    (let [running? (boolean (some-> (first @start-modules) lifecycle/started?))]
      {:running running?
       ;; nil = not on this classpath OR explicitly disabled — either way
       ;; the operator console renders it as absent, never as pending
       :observability (when (and (not (observability-disabled?))
                                 (lifecycle/module-info :synthigy/observability))
                        (lifecycle/started? :synthigy/observability))
       :console (when (lifecycle/module-info :synthigy/console)
                  (lifecycle/started? :synthigy/console))
       :message (if running? "Server is running" "Server is stopped")})))

(register-method! "shutdown"
  (fn [_]
    (log/warn {:id ::shutdown-requested
               :data {:action :stopping :subject :admin}}
              "Shutdown requested via supervision channel")
    (future (Thread/sleep 1000) (System/exit 0))
    {:success true :message "Shutdown initiated"}))

(defn- write-frame!
  "One JSON-RPC message as a single line on *out*, flushed immediately —
   the whole framing contract. Locked on System/out: `serve!`'s read loop
   runs on its own thread, so a response frame must never interleave with
   another thread's write mid-line."
  [msg]
  (locking System/out
    (println (json/write-str msg))
    (flush)))

(defonce ^:private serving (atom nil))

(defn serve!
  "Start the stdio read-dispatch-respond loop on a daemon thread; a no-op if
   already running. One JSON-RPC frame per line on *in*: a line that fails
   to parse or dispatch is logged (never to stdout — see
   synthigy.log/supervised?) and the loop continues, since a malformed
   frame is the parent's bug to fix, not a reason to die. `:modules` is the
   lifecycle target `server.start` starts (its first entry is what
   `server.stop`/`server.status` act on). When supervised, stdin EOF means
   the parent died — the process exits rather than run orphaned."
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
                         ;; each frame on its own thread — a long verb
                         ;; (server.start running migrations) must never
                         ;; block ping/doctor; write-frame! serializes the
                         ;; interleaved responses on System/out
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
