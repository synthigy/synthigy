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

(ns synthigy.log
  "Central logging API for Synthigy. Telemere is an implementation detail."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [jsonista.core :as json]
    [patcho.lifecycle :as lifecycle]
    [synthigy.log.pipeline :as pipeline]
    [synthigy.log.store :as store]
    [synthigy.log.topics :as topics]
    [synthigy.node :as node]
    [taoensso.telemere :as t]))

;;; ============================================================================
;;; Level + ns-filter parsing
;;; ============================================================================

(def ^:private valid-levels
  #{:trace :debug :info :warn :error :fatal})

(defn parse-level [s]
  (let [k (some-> s str/lower-case keyword)]
    (when (valid-levels k) k)))

(defn env-root-level []
  (or (parse-level (env :synthigy-log-level)) :info))

(defn supervised?
  "True when SYNTHIGY_SUPERVISED=1 — the parent process owns this process's
   stdout as an exclusive JSON-RPC channel (see synthigy.supervisor and
   docs/plans/PLAN-PORTAL-SUPERVISOR.md, the framing rule), so any log
   output must go to stderr instead."
  []
  (= "1" (env :synthigy-supervised)))

(defn env-ns-overrides
  "Parse SYNTHIGY_LOG_NS into a sequence of [ns-pattern level] pairs."
  []
  (when-let [s (env :synthigy-log-ns)]
    (->> (str/split s #",")
         (map str/trim)
         (remove str/blank?)
         (keep (fn [pair]
                 (let [[ns lvl] (str/split pair #"=" 2)
                       ns (some-> ns str/trim)
                       lvl (parse-level lvl)]
                   (when (and ns lvl) [ns lvl])))))))

;;; ============================================================================
;;; JSON wire formatter — locked schema v2
;;; ============================================================================

(def wire-schema-version
  "Version of the JSON wire schema. Bumped on incompatible field changes."
  2)

(def default-redactions
  "Keys whose values must never appear in log :data or :ctx."
  #{:password :token :access-token :refresh-token :id-token
    :secret :client-secret :authorization :api-key :cookie})

(defn redact
  ([m] (redact m default-redactions))
  ([m keys-to-redact]
   (reduce (fn [acc k]
             (if (contains? acc k) (assoc acc k "<redacted>") acc))
           m keys-to-redact)))

(def ^:private promoted-ctx-keys
  [:request-id :user-xid :tenant])

(defn current-principal-xid
  "xid of the principal bound in the current scope, or nil."
  []
  (try
    (when-let [v (resolve 'synthigy.iam.access/*principal*)]
      (some-> @v :xid))
    (catch Throwable _ nil)))

(def ^:private json-mapper
  (json/object-mapper
    {:encode-key-fn
     (fn [k]
       (let [s (cond
                 (keyword? k)
                 (if-let [n (namespace k)] (str n "/" (name k)) (name k))
                 :else (str k))]
         (str/replace s \- \_)))}))

(defn keyword->wire-id
  [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (some? id)              (str id)))

(defn throwable->wire
  [x]
  (cond
    (instance? Throwable x)
    (let [^Throwable t x
          sw (java.io.StringWriter.)
          pw (java.io.PrintWriter. sw)]
      (.printStackTrace t pw)
      (.flush pw)
      [(.getName (class t)) (.getMessage t) (.toString sw)])

    (some? x) [nil nil (str x)]
    :else     [nil nil nil]))

(defn signal->json-line
  "Serialize a Telemere signal to a single-line JSON string in the locked wire
   schema."
  [{:keys [inst level ns id msg_ data ctx error] :as signal}]
  (let [[err-class err-msg err-trace] (throwable->wire error)
        ctx-map      (or ctx {})
        residual-ctx (apply dissoc ctx-map promoted-ctx-keys)
        m (doto (java.util.LinkedHashMap.)
            (.put "v"           wire-schema-version)
            (.put "inst"        (when inst (str inst)))
            (.put "level"       (some-> level name))
            (.put "ns"          ns)
            (.put "id"          (keyword->wire-id id))
            (.put "msg"         (when msg_ (force msg_)))
            (.put "request_id"  (or (:request-id signal) (get ctx-map :request-id)))
            (.put "user_xid"    (or (:user-xid signal)   (get ctx-map :user-xid)))
            (.put "tenant"      (or (:tenant signal)     (get ctx-map :tenant)))
            (.put "host"        (let [h (:host signal)]
                                  (cond
                                    (string? h) h
                                    (map? h)    (or (:name h) @node/hostname)
                                    :else       @node/hostname)))
            ;; (subs (str kw) 1), NOT `name` — :traffic/sse must stay
            ;; "traffic/sse"
            (.put "topics"      (->> (:topics signal)
                                     (map (fn [t] (if (keyword? t) (subs (str t) 1) (str t))))
                                     sort vec))
            (.put "data"        (or data {}))
            (.put "ctx"         residual-ctx)
            (.put "error_class" err-class)
            (.put "error_msg"   err-msg)
            (.put "error_trace" err-trace))]
    (json/write-value-as-string m json-mapper)))

;;; ============================================================================
;;; Raw handler escape hatch — `add-sink!` / `remove-sink!` / `list-sinks`
;;; ============================================================================

(defonce ^:private raw-handlers  (atom #{}))
(defonce ^:private raw-resources (atom {}))

(defn add-sink!
  "Install a raw handler fn (taking one JSON-line String per signal) as a log
   sink."
  [sink-id write-fn & {:keys [ns-filter min-level close-fn]}]
  (try (t/remove-handler! sink-id) (catch Throwable _))
  (let [handler-opts (cond-> {}
                       ns-filter (assoc :ns-filter ns-filter)
                       min-level (assoc :min-level min-level))
        wrapped (fn [signal]
                  (try
                    (write-fn (or (get signal pipeline/line-key)
                                  (signal->json-line signal)))
                    (catch Throwable t
                      (binding [*out* *err*]
                        (println
                          (str "synthigy.log/sink[" sink-id "] threw: "
                               (some-> t .getClass .getName) ": "
                               (.getMessage t)))))))]
    (t/add-handler! sink-id wrapped handler-opts)
    (swap! raw-handlers conj sink-id)
    (when close-fn (swap! raw-resources assoc sink-id close-fn))
    sink-id))

(defn remove-sink!
  "Remove a raw handler, running its :close-fn if provided."
  [sink-id]
  (try (t/remove-handler! sink-id) (catch Throwable _))
  (when-let [cf (get @raw-resources sink-id)]
    (try (cf) (catch Throwable _))
    (swap! raw-resources dissoc sink-id))
  (swap! raw-handlers disj sink-id)
  nil)

(defn list-sinks []
  (set @raw-handlers))

;;; ============================================================================
;;; Store-bridge handler id + routing state
;;; ============================================================================

(def store-handler-id   :synthigy/store-bridge)

(defonce ^:private store-routing   (atom nil))
(defonce ^:private current-routing (atom nil))
(defonce ^:private env-routing     (atom nil))

;;; ============================================================================
;;; Store-bridge handler install/replace
;;; ============================================================================

(defn install-store-bridge!
  "(Re)install the bridge handler forwarding every signal to the bound
   `*log-store*`."
  [{:keys [min-level ns-filter] :as routing}]
  (try (t/remove-handler! store-handler-id) (catch Throwable _))
  (let [opts    (cond-> {}
                  ns-filter (assoc :ns-filter ns-filter)
                  min-level (assoc :min-level min-level))
        handler (fn [signal]
                  (try (store/write-signal! @#'store/*log-store* signal)
                       (catch Throwable t
                         (binding [*out* *err*]
                           (println
                             (str "synthigy.log/store-bridge threw: "
                                  (some-> t .getClass .getName) ": "
                                  (.getMessage t)))))))]
    (t/add-handler! store-handler-id handler opts)
    (reset! store-routing routing)
    nil))

(defn uninstall-store-bridge! []
  (try (t/remove-handler! store-handler-id) (catch Throwable _))
  (reset! store-routing nil))

;;; ============================================================================
;;; Routing — public API consumed by synthigy.log.config + admin endpoints
;;; ============================================================================

(defn store-health
  "Health snapshot of the currently-bound `*log-store*`."
  []
  (try (store/health @#'store/*log-store*) (catch Throwable _ {:up? false})))

(defn sink-health
  "Health snapshot for a handler id; only the store bridge resolves."
  [id]
  (when (= id store-handler-id)
    (store-health)))

(defn expand-ns-pattern
  "Expand a logical-ns override pattern to also install its real code-namespace
   equivalents."
  [pattern]
  (into [pattern]
        (keep (fn [[real canonical]]
                (when (and (not= real canonical)
                           (str/starts-with? (str pattern) canonical))
                  (let [remainder (subs (str pattern) (count canonical))]
                    (str real (if (str/blank? remainder) "*" remainder))))))
        pipeline/default-ns-aliases))

(defn set-ns-min-level!
  [ns-pat lvl]
  (doseq [p (expand-ns-pattern ns-pat)]
    (t/set-min-level! nil p lvl)))

(defn apply-routing!
  "Apply a new routing config to the live handler graph without restart."
  [{:keys [root-level ns-overrides store]}]
  (when (and root-level (valid-levels root-level))
    (t/set-min-level! root-level))
  (doseq [[pattern level] ns-overrides
          :when (and pattern (valid-levels level))]
    (set-ns-min-level! pattern level))
  (when (some? store)   (install-store-bridge! store))
  (swap! current-routing
         (fn [r] (merge (or r {})
                        {:source :db}
                        (when root-level   {:root-level root-level})
                        (when ns-overrides {:ns-overrides (vec ns-overrides)}))))
  nil)

(defn routing-snapshot
  "Current routing state, or nil before `install!` has run."
  []
  (when-let [base @current-routing]
    (assoc base :store @store-routing)))

(defn revert-to-env-routing!
  "Re-apply the env-var bootstrap routing, undoing any DB overlay."
  []
  (when-let [{:keys [root-level ns-overrides store]} @env-routing]
    (let [env-patterns (set (map first (or ns-overrides [])))
          db-patterns  (map first (:ns-overrides @current-routing))]
      (doseq [pattern (remove env-patterns db-patterns)]
        (try (t/set-min-level! nil pattern nil) (catch Throwable _))))
    (when root-level (t/set-min-level! root-level))
    (doseq [[pattern level] (or ns-overrides [])
            :when (and pattern (valid-levels level))]
      (t/set-min-level! nil pattern level))
    (when store   (install-store-bridge! store))
    (reset! current-routing @env-routing)
    nil))

;;; ============================================================================
;;; Set ns levels (test convenience)
;;; ============================================================================

(defn set-ns-levels!
  "Apply per-namespace [ns-pattern level] min-level overrides."
  [pairs]
  (doseq [[ns-pat lvl] pairs]
    (when (and ns-pat (valid-levels lvl))
      (set-ns-min-level! ns-pat lvl))))

;;; ============================================================================
;;; Pipeline install
;;; ============================================================================

(defn install-pipeline! []
  (t/set-xfn!
    (pipeline/compose-stages
      [(pipeline/normalize-ns-stage)
       (pipeline/enrich-host-stage #(deref node/hostname))
       (pipeline/enrich-ctx-stage promoted-ctx-keys)
       (pipeline/enrich-topics-stage topics/classify)
       (pipeline/redact-stage default-redactions)
       (pipeline/serialize-stage signal->json-line)])))

;;; ============================================================================
;;; install! / shutdown!
;;; ============================================================================

(defn shutdown!
  "Tear down handlers + pipeline. Idempotent."
  []
  ;; Evicting :default/console is load-bearing — Telemere installs it on ns
  ;; load.
  (try (t/remove-handler! :default/console) (catch Throwable _))
  (uninstall-store-bridge!)
  (doseq [sink-id (vec @raw-handlers)]
    (try (remove-sink! sink-id) (catch Throwable _)))
  (try (t/set-xfn! nil) (catch Throwable _))
  nil)

(defn install!
  "Configure Telemere: pipeline + store-bridge handler."
  []
  (shutdown!)
  (install-pipeline!)
  (let [root-level   (env-root-level)
        ns-overrides (env-ns-overrides)
        store-r      {}]                        ; always on
    (install-store-bridge! store-r)
    (t/set-min-level! root-level)
    (doseq [[ns-pat lvl] ns-overrides]
      (set-ns-min-level! ns-pat lvl))
    (let [routing {:root-level   root-level
                   :ns-overrides (vec ns-overrides)
                   :store        store-r
                   :source       :env}]
      (reset! current-routing routing)
      (reset! env-routing     routing))
    {:store-bridged? true
     :root-level     root-level
     :ns-overrides   (vec ns-overrides)}))

;;; ============================================================================
;;; Taps — runtime-attached ring-buffer handlers (REPL + operator console)
;;; ============================================================================

(defonce ^:private taps (atom {}))

(defonce ^:private tap-seq (java.util.concurrent.atomic.AtomicLong.))

(defn tap!
  "Capture signals matching `ns-pattern` into a ring buffer keyed by `tap-id`,
   numbered from one JVM-wide sequence; re-tapping an id replaces it."
  [tap-id ns-pattern & {:keys [n min-level xform] :or {n 200 xform identity}}]
  (when (contains? @taps tap-id)
    (try (t/remove-handler! tap-id) (catch Throwable _)))
  (let [base (.get ^java.util.concurrent.atomic.AtomicLong tap-seq)
        buf  (atom {:base base :seq base :q clojure.lang.PersistentQueue/EMPTY})]
    (swap! taps assoc tap-id {:buf buf :min-level min-level :ns-pattern ns-pattern})
    (t/add-handler! tap-id
      (fn [signal]
        (let [item (xform signal)]
          (locking buf
            (let [s (.incrementAndGet ^java.util.concurrent.atomic.AtomicLong tap-seq)]
              (swap! buf (fn [{:keys [q] :as b}]
                           (let [q (conj q [s item])]
                             (assoc b :seq s :q (if (> (count q) n) (pop q) q)))))))))
      (cond-> {}
        ns-pattern (assoc :ns-filter ns-pattern)
        min-level  (assoc :min-level min-level)))
    tap-id))

(defn tap-info
  "The options a tap was installed with, or nil when `tap-id` is not tapped."
  [tap-id]
  (some-> @taps (get tap-id) (dissoc :buf)))

(defn recent
  "Captured items for `tap-id` as a vector (oldest first)."
  [tap-id]
  (some->> (get @taps tap-id) :buf deref :q (mapv second)))

(defn tail
  "Items captured after sequence number `after`, at most `limit`, each assoc'd
   with its `:seq`."
  [tap-id after limit]
  (when-let [{:keys [base seq q]} (some-> (get @taps tap-id) :buf deref)]
    (let [oldest (or (ffirst q) (inc seq))
          items  (into []
                       (comp (filter (fn [[s _]] (> s after)))
                             (take limit)
                             (map (fn [[s item]] (assoc item :seq s))))
                       q)]
      {:entries items
       :next    (or (:seq (peek items)) (max after seq))
       :dropped (> oldest (inc (max after base)))})))

(defn recent-lines
  "Captured signals as `[ts level ns msg]` tuples."
  [tap-id]
  (mapv (fn [{:keys [inst level ns msg_]}]
          [(str inst) level ns (some-> msg_ force)])
        (recent tap-id)))

(defn signal->entry
  "Flatten a signal into the operator console's log entry; never carries `:data`."
  [{:keys [inst level ns id msg_ error] :as signal}]
  (cond-> {:ts     (some-> inst str)
           :level  (some-> level name)
           :ns     (some-> ns str)
           :id     (keyword->wire-id id)
           :msg    (some-> msg_ force str)
           :topics (->> (or (:topics signal) (topics/classify signal))
                        (map (fn [t] (if (keyword? t) (subs (str t) 1) (str t))))
                        sort vec)}
    error (assoc :error {:class   (.getName (class error))
                         :message (ex-message error)
                         :trace   (mapv str (take 15 (.getStackTrace ^Throwable error)))})))

(defn untap!
  "Stop capturing for `tap-id` and discard its buffer."
  [tap-id]
  (try (t/remove-handler! tap-id) (catch Throwable _))
  (swap! taps dissoc tap-id)
  nil)

(defn list-taps []
  (vec (keys @taps)))

;;; ============================================================================
;;; Callsite API — level macros
;;; ============================================================================

(defn expand-level
  [lvl args]
  (case (count args)
    1 (let [a (first args)]
        (if (map? a)
          `(t/log! ~(assoc a :level lvl))
          `(t/log! ~lvl ~a)))
    2 (let [[opts msg] args]
        (if (map? opts)
          `(t/log! ~(assoc opts :level lvl) ~msg)
          `(t/signal! (assoc ~opts :level ~lvl :kind :log :msg ~msg))))))

(defmacro trace [& args] (expand-level :trace args))
(defmacro debug [& args] (expand-level :debug args))
(defmacro info  [& args] (expand-level :info  args))
(defmacro warn  [& args] (expand-level :warn  args))
(defmacro error
  "Emit an :error-level signal; for attaching a throwable use `error!`."
  [& args] (expand-level :error args))
(defmacro fatal [& args] (expand-level :fatal args))

(defmacro error! [& args] `(t/error! ~@args))

(defmacro spy!
  "Wrap body, emit :start/:end with elapsed-ms. Returns body value."
  [& args] `(t/spy! ~@args))

(defmacro event!
  "Emit an :event-kind signal. Requires `:id`."
  [& args] `(t/event! ~@args))

(defmacro log!
  "Escape hatch to Telemere's `log!` macro for direct control."
  [& args] `(t/log! ~@args))

(defmacro signal!
  "Escape hatch to Telemere's `signal!`."
  [& args] `(t/signal! ~@args))

(defmacro with-ctx
  "Run body with extra context merged into Telemere's *ctx*."
  [ctx-map & body] `(t/with-ctx+ ~ctx-map ~@body))

(defn request-id
  "The current request's correlation id from Telemere's *ctx*, or nil outside a
   request."
  []
  (:request-id t/*ctx*))

;;; ============================================================================
;;; Patcho lifecycle bridge
;;; ============================================================================

(defn install-lifecycle-hook!
  "Route every patcho module transition into the log, suppressing patcho's
   stdout default."
  []
  (alter-var-root
    #'lifecycle/*on-lifecycle-event*
    (constantly
      (fn [{:keys [phase topic error]}]
        (case phase
          :started (info {:id ::module-started
                          :data {:action :started :subject :module :module topic}}
                         (str "Module " topic " started"))
          :stopped (info {:id ::module-stopped
                          :data {:action :stopped :subject :module :module topic}}
                         (str "Module " topic " stopped"))
          ;; t/error! takes (opts error) — the message rides in :msg, not a
          ;; third arg
          :start-failed (error! {:id   ::module-start-failed
                                 :msg  (str "Module " topic " failed to start")
                                 :data {:action :starting :subject :module :module topic}}
                                error)
          nil)))))

(defn restore-default-lifecycle-hook!
  "Hand the lifecycle hook back to patcho's stdout printer."
  []
  (alter-var-root #'lifecycle/*on-lifecycle-event*
                  (constantly lifecycle/default-on-lifecycle-event)))

;;; ============================================================================
;;; Lifecycle module
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/log
  {:depends-on []
   :doc "pure logging, zero deps"
   ;; Ordering is load-bearing: the started-log must come AFTER install!,
   ;; the stopping-log BEFORE shutdown!.
   :start (fn []
            (let [{:keys [root-level]} (install!)]
              (install-lifecycle-hook!)
              ;; Supervised: stdout is the JSON-RPC control channel
              ;; exclusively (see synthigy.supervisor), so console output goes
              ;; to stderr. Re-uses :default/console — same id the load-time
              ;; branch below installs, so `shutdown!` keeps evicting it in one
              ;; place — but WARN+ from here on: the store bridge is up now and
              ;; holds every signal, so INFO on stderr is pure duplication.
              (when (supervised?)
                (t/add-handler! :default/console
                                (let [fmt (t/format-signal-fn)]
                                  (fn [signal]
                                    (binding [*out* *err*] (println (fmt signal)))))
                                {:min-level :warn}))
              (info {:id ::lifecycle-started
                     :data {:action :started :subject :logging
                            :root-level root-level
                            :backend (:backend (store-health))}}
                    "Logging started")))
   :stop  (fn []
            (info {:id ::lifecycle-stopping
                   :data {:action :stopping :subject :logging}}
                  "Stopping logging")
            ;; Hand the hook back BEFORE tearing the bridge down, else patcho's
            ;; own :stopped notify logs into a dead sink.
            (restore-default-lifecycle-hook!)
            (shutdown!))})

;; Load-time, not start-time — merely requiring synthigy.log must silence
;; patcho's stdout default.
(install-lifecycle-hook!)

;; Load-time too: supervised stdout is exclusively the JSON-RPC channel, and
;; Telemere's :default/console (installed on ITS ns load) prints to *out* —
;; under the warm-idle boot every namespace loads before :synthigy/log
;; starts, so load-time signals would leak onto the control channel.
(when (supervised?)
  (try (t/remove-handler! :default/console) (catch Throwable _))
  (t/add-handler! :default/console
                  (let [fmt (t/format-signal-fn)]
                    (fn [signal] (binding [*out* *err*] (println (fmt signal)))))))
