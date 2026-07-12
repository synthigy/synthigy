(ns synthigy.log
  "Central logging API for Synthigy. Telemere is an implementation detail.

  ## Callsite API

  Level macros — pass a string, or an opts map + string:

      (log/info \"User logged in\")
      (log/debug {:id ::pkce-verify
                  :data {:client-id client-id, :grant-type grant-type}}
                 \"PKCE verification\")

  Throwables — use `error!`:

      (log/error! e \"OAuth client reload failed\")
      (log/error! {:id ::client-reload-failed} e)

  Spans — wrap a body to emit start/end signals with elapsed time:

      (log/spy! {:id ::deploy-dataset, :data {:version v}}
        (deploy! v))

  Request-scoped context — every signal in the body inherits :ctx:

      (log/with-ctx {:request-id rid, :user-xid (:xid u), :tenant t}
        (handle-request))

  Escape hatches: `event!`, `log!`, `signal!` re-export Telemere's full
  surface; `add-sink!` / `remove-sink!` install raw Telemere handlers for
  ad-hoc routing.

  ## Convention

  - Always pass an `:id` (namespace-qualified keyword) when the signal will
    drive an alert, dashboard, or test assertion. The id is the contract.
  - Pass structured fields under `:data` rather than format-stringing into
    the message. The store's JSON columns lift `:data` into queryable shape.
  - Don't add `(println ...)` for debugging. Add a `(log/trace ...)` and
    capture via `tap!` (see below).

  ## Sinks model (post-observability-substrate refactor)

  Two Telemere handlers run alongside each other; both optional:

  - **Console** (`:synthigy/console`) — humans-readable pretty output to
    stdout. Installed by default but FILTERED TO `:warn` and above so it
    stays quiet during normal operation. Operators opt into verbose
    console via `SYNTHIGY_LOG_CONSOLE_LEVEL=info` (or `debug`, `trace`).
    `SYNTHIGY_LOG_CONSOLE=false` disables the handler entirely.

  - **Store bridge** (`:synthigy/store-bridge`) — durable + queryable, by
    forwarding every signal to whatever `synthigy.log.store/*log-store*`
    is currently bound. The dynvar is defonce'd to a fresh
    `RingLogStore` at the protocol-ns site, so logs are queryable from
    JVM start — no `install!` step required to seed the store. When
    `:synthigy/observability` starts (DuckDB, ClickHouse, …), it
    `alter-var-root`'s the dynvar to its durable backend FIRST, then
    snapshots the now-orphaned ring and drains its buffered signals into
    that backend. On observability `stop`, a fresh ring is rebound.
    `install!` itself never touches
    `*log-store*` — it only owns the Telemere pipeline + handlers.

  Custom routing (ad-hoc fn handlers like ringing alerts to a webhook) goes
  through `add-sink!` — a raw Telemere handler escape hatch.

  ## Env

      SYNTHIGY_LOG_LEVEL   Root min-level (trace|debug|info|warn|error|fatal).
                           Default: info.
      SYNTHIGY_LOG_NS      Per-ns overrides, comma-separated.
                           Example: 'synthigy.dataset.postgres=debug,com.zaxxer.hikari=error'
      SYNTHIGY_LOG_HOST           Override the `:host` column. Defaults to
                                  JVM hostname.
      SYNTHIGY_LOG_CONSOLE        Set to 'false' to skip installing the
                                  console handler at startup. Default 'true'.
      SYNTHIGY_LOG_CONSOLE_LEVEL  Min level for the console handler. Default
                                  'warn' — info/debug stay out of the
                                  terminal unless you opt in here. The
                                  durable store (`:synthigy/observability`)
                                  always sees the full root-level stream
                                  regardless."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [jsonista.core :as json]
    [patcho.lifecycle :as lifecycle]
    [synthigy.log.pipeline :as pipeline]
    [synthigy.log.store :as store]
    [synthigy.log.topics :as topics]
    [taoensso.telemere :as t]))

;;; ============================================================================
;;; Level + ns-filter parsing
;;; ============================================================================

(def ^:private valid-levels
  #{:trace :debug :info :warn :error :fatal})

(defn- parse-level [s]
  (let [k (some-> s str/lower-case keyword)]
    (when (valid-levels k) k)))

(defn- env-root-level []
  (or (parse-level (env :synthigy-log-level)) :info))

(defn- env-ns-overrides
  "Parse SYNTHIGY_LOG_NS into a sequence of [ns-pattern level] pairs.
   Format: 'a.b=debug,c.d.*=warn'"
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

(defn- env-console-enabled?
  "SYNTHIGY_LOG_CONSOLE=false disables the console handler; default true."
  []
  (let [v (env :synthigy-log-console)]
    (or (str/blank? (str v))
        (not (contains? #{"false" "0" "no"}
                        (some-> v str/lower-case str/trim))))))

(defn- env-console-level
  "Min-level for the console handler. Defaults to :warn — the terminal
   stays quiet unless an operator opts into more via
   SYNTHIGY_LOG_CONSOLE_LEVEL=info|debug|trace. The bound store still
   sees the full stream — this only filters what reaches stdout."
  []
  (or (parse-level (env :synthigy-log-console-level)) :warn))

;;; ============================================================================
;;; JSON wire formatter — locked schema v2
;;;
;;; v, inst, level, ns, id, msg,
;;; request_id, user_xid, tenant, host, topics,
;;; data, ctx,
;;; error_class, error_msg, error_trace
;;;
;;; v2 added `topics` (a JSON array of topic name strings) — the classified
;;; audience/subject of the signal, orthogonal to level. See synthigy.log.topics.
;;; ============================================================================

(def wire-schema-version
  "Version of the JSON wire schema. Bumped on incompatible field changes.
   v2: added the `topics` array."
  2)

(def ^:private hostname
  (delay
    (or (env :synthigy-log-host)
        (env :hostname)
        (try (.getHostName (java.net.InetAddress/getLocalHost))
             (catch Throwable _ nil)))))

(def default-redactions
  "Keys whose values must never appear in log :data or :ctx. The `redact`
   helper replaces matching values with \"<redacted>\". Apply at call sites
   that may carry credentials before passing maps into a log call."
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

(def ^:private json-mapper
  (json/object-mapper
    {:encode-key-fn
     (fn [k]
       (let [s (cond
                 (keyword? k)
                 (if-let [n (namespace k)] (str n "/" (name k)) (name k))
                 :else (str k))]
         (str/replace s \- \_)))}))

(defn- keyword->wire-id
  [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (some? id)              (str id)))

(defn- throwable->wire
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
  "Serialize a Telemere signal to a single-line JSON string in the locked
   wire schema (v=1)."
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
                                    (map? h)    (or (:name h) @hostname)
                                    :else       @hostname)))
            (.put "topics"      (->> (:topics signal)
                                     (map (fn [t] (if (keyword? t) (name t) (str t))))
                                     sort vec))
            (.put "data"        (or data {}))
            (.put "ctx"         residual-ctx)
            (.put "error_class" err-class)
            (.put "error_msg"   err-msg)
            (.put "error_trace" err-trace))]
    (json/write-value-as-string m json-mapper)))

;;; ============================================================================
;;; Raw handler escape hatch — `add-sink!` / `remove-sink!` / `list-sinks`
;;;
;;; A raw handler is a 1-arg function that consumes a single JSON-line String.
;;; Use this for quick ad-hoc routing (alert webhook, debug ring, etc.). The
;;; pipeline already serializes the wire line once per signal; this wrapper
;;; reads the cached line.
;;; ============================================================================

(defonce ^:private raw-handlers  (atom #{}))
(defonce ^:private raw-resources (atom {}))

(defn add-sink!
  "Install a raw handler fn as a log sink. `sink-id` is a unique keyword;
   re-installing with the same id replaces the previous handler.

   Options:
     :ns-filter — Telemere ns-filter (glob string or {:allow :disallow} map).
     :min-level — minimum level for this handler.
     :close-fn  — 0-arg fn invoked on `remove-sink!` / `shutdown!`."
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
  "Remove a raw handler. If a `:close-fn` was provided, it runs. Idempotent."
  [sink-id]
  (try (t/remove-handler! sink-id) (catch Throwable _))
  (when-let [cf (get @raw-resources sink-id)]
    (try (cf) (catch Throwable _))
    (swap! raw-resources dissoc sink-id))
  (swap! raw-handlers disj sink-id)
  nil)

(defn list-sinks
  "Set of currently-installed raw handler ids (escape-hatch sinks only)."
  []
  (set @raw-handlers))

;;; ============================================================================
;;; Console + store-bridge handler ids + routing state
;;; ============================================================================

(def console-handler-id :synthigy/console)
(def store-handler-id   :synthigy/store-bridge)

(defonce ^:private console-routing (atom nil))
(defonce ^:private store-routing   (atom nil))
(defonce ^:private current-routing (atom nil))
(defonce ^:private env-routing     (atom nil))

;; ----------------------------------------------------------------------------
;; Provider pattern.
;;
;; `synthigy.log.store/*log-store*` is defonce'd to a fresh `RingLogStore`
;; at the protocol-ns site (mirrors `synthigy.dataset.access/*access-control*`
;; defaulting to `AllowAllAccess`). The dynvar is never nil and `install!`
;; never mutates it — only the backend module (`:synthigy/observability`)
;; touches it on `:start`/`:stop`.
;;
;; That means the bridge handler has one code path: it can write to
;; `@#'store/*log-store*` unconditionally. Pre-observability signals land
;; in the ring; observability `:start` swaps the dynvar to the durable
;; backend first, then snapshots the orphaned ring and drains it. Observability
;; `:stop` rebinds a fresh ring.
;; ----------------------------------------------------------------------------

;;; ============================================================================
;;; Console + store-bridge handler install/replace
;;; ============================================================================

(defn- install-console-handler!
  "(Re)install the pretty console handler with given routing opts."
  [{:keys [min-level ns-filter] :as routing}]
  (try (t/remove-handler! console-handler-id) (catch Throwable _))
  (let [handler (t/handler:console)
        opts    (cond-> {}
                  ns-filter (assoc :ns-filter ns-filter)
                  min-level (assoc :min-level min-level))]
    (t/add-handler! console-handler-id handler opts)
    (reset! console-routing routing)
    nil))

(defn- uninstall-console-handler! []
  (try (t/remove-handler! console-handler-id) (catch Throwable _))
  (reset! console-routing nil))

(defn- install-store-bridge!
  "(Re)install the bridge handler that forwards every signal to whatever
   `synthigy.log.store/*log-store*` is currently bound. The dynvar is
   defonce'd to a `RingLogStore` so this never needs a nil-check; when
   `:synthigy/observability` starts, it snapshots the ring then
   `alter-var-root`'s the dynvar to the durable backend, and subsequent
   signals stream there transparently.

   Routing options (min-level, ns-filter) apply at the Telemere handler
   boundary before the bridge fires."
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

(defn- uninstall-store-bridge! []
  (try (t/remove-handler! store-handler-id) (catch Throwable _))
  (reset! store-routing nil))

;;; ============================================================================
;;; Routing — public API consumed by synthigy.log.config + admin endpoints
;;; ============================================================================

(defn store-health
  "Health snapshot of the currently-bound `*log-store*`. Resolves to the
   in-memory ring (`:backend :ring`) by default, unless
   `:synthigy/observability` is up — at which point the durable backend
   (DuckDB, ClickHouse) reports its own shape."
  []
  (try (store/health @#'store/*log-store*) (catch Throwable _ {:up? false})))

(defn sink-health
  "Compat wrapper: takes the handler id and returns its health snapshot.
   `:synthigy/store-bridge` resolves to the bound store's health.
   `:synthigy/console` returns `{:up? <handler-installed?>}`.
   Other ids return nil — raw escape-hatch sinks have no health surface."
  [id]
  (cond
    (= id store-handler-id)
    (store-health)

    (= id console-handler-id)
    {:up? (some? @console-routing)}

    :else nil))

(defn apply-routing!
  "Apply a new routing config to the live handler graph without restart.

   `config` shape (NEW — replaces the pre-observability per-sink map):

     :root-level    keyword level — sets the global minimum
     :ns-overrides  seq of [ns-pattern level] pairs
     :console       {:min-level kw-or-nil, :ns-filter str-or-nil}  ; optional
     :store         {:min-level kw-or-nil, :ns-filter str-or-nil}  ; optional

   Either component may be omitted (it then keeps its existing routing).
   Set its `:min-level` to nil to clear an override. Called by
   `synthigy.log.config` after loading from the database."
  [{:keys [root-level ns-overrides console store]}]
  (when (and root-level (valid-levels root-level))
    (t/set-min-level! root-level))
  (doseq [[pattern level] ns-overrides
          :when (and pattern (valid-levels level))]
    (t/set-min-level! nil pattern level))
  (when (some? console) (install-console-handler! console))
  (when (some? store)   (install-store-bridge! store))
  (swap! current-routing
         (fn [r] (merge (or r {})
                        {:source :db}
                        (when root-level   {:root-level root-level})
                        (when ns-overrides {:ns-overrides (vec ns-overrides)}))))
  nil)

(defn routing-snapshot
  "Return the current routing state: root-level, ns-overrides, per-component
   (console + store) routing, source (:env on startup, :db after DB overlay).
   Returns nil before `install!` has been called."
  []
  (when-let [base @current-routing]
    (assoc base
           :console @console-routing
           :store   @store-routing)))

(defn revert-to-env-routing!
  "Re-apply the env-var bootstrap routing, undoing any DB overlay. Called
   by `synthigy.log.config/clear-config!`."
  []
  (when-let [{:keys [root-level ns-overrides console store]} @env-routing]
    (let [env-patterns (set (map first (or ns-overrides [])))
          db-patterns  (map first (:ns-overrides @current-routing))]
      (doseq [pattern (remove env-patterns db-patterns)]
        (try (t/set-min-level! nil pattern nil) (catch Throwable _))))
    (when root-level (t/set-min-level! root-level))
    (doseq [[pattern level] (or ns-overrides [])
            :when (and pattern (valid-levels level))]
      (t/set-min-level! nil pattern level))
    (when console (install-console-handler! console))
    (when store   (install-store-bridge! store))
    (reset! current-routing @env-routing)
    nil))

;;; ============================================================================
;;; Set ns levels (test convenience)
;;; ============================================================================

(defn set-ns-levels!
  "Apply per-namespace min-level overrides programmatically. Each entry is
   [ns-pattern level], e.g. [\"synthigy.iam.access\" :warn]."
  [pairs]
  (doseq [[ns-pat lvl] pairs]
    (when (and ns-pat (valid-levels lvl))
      (t/set-min-level! nil ns-pat lvl))))

;;; ============================================================================
;;; Pipeline install
;;; ============================================================================

(defn- install-pipeline! []
  (t/set-xfn!
    (pipeline/compose-stages
      [(pipeline/enrich-host-stage #(deref hostname))
       (pipeline/enrich-ctx-stage promoted-ctx-keys)
       (pipeline/enrich-topics-stage topics/classify)
       (pipeline/redact-stage default-redactions)
       (pipeline/serialize-stage signal->json-line)])))

;;; ============================================================================
;;; install! / shutdown!
;;; ============================================================================

(defn shutdown!
  "Tear down handlers + pipeline. Idempotent. Also evicts Telemere's
   bundled `:default/console` handler so a re-install doesn't
   double-print.

   Note: `*log-store*` is intentionally NOT mutated — the dynvar belongs
   to the `:synthigy/observability` lifecycle, not to log's own. After
   shutdown the ring (or whichever backend was bound) remains usable for
   reads."
  []
  (try (t/remove-handler! :default/console) (catch Throwable _))
  (uninstall-console-handler!)
  (uninstall-store-bridge!)
  (doseq [sink-id (vec @raw-handlers)]
    (try (remove-sink! sink-id) (catch Throwable _)))
  (try (t/set-xfn! nil) (catch Throwable _))
  nil)

(defn install!
  "Configure Telemere: pipeline + console handler (when enabled) +
   store-bridge handler. Does NOT touch `synthigy.log.store/*log-store*`
   — that dynvar is defonce'd to a fresh `RingLogStore` at the
   protocol-ns site and is owned by the `:synthigy/observability`
   lifecycle from then on.

   Returns `{:console? :store-bridged? :root-level :ns-overrides}`."
  []
  (shutdown!)
  (install-pipeline!)
  (let [root-level   (env-root-level)
        ns-overrides (env-ns-overrides)
        console?     (env-console-enabled?)
        console-r    (when console? {:min-level (env-console-level)})
        store-r      {}]                        ; always on
    (when console? (install-console-handler! console-r))
    (install-store-bridge! store-r)
    (t/set-min-level! root-level)
    (doseq [[ns-pat lvl] ns-overrides]
      (t/set-min-level! nil ns-pat lvl))
    (let [routing {:root-level   root-level
                   :ns-overrides (vec ns-overrides)
                   :console      console-r
                   :store        store-r
                   :source       :env}]
      (reset! current-routing routing)
      (reset! env-routing     routing))
    {:console?       console?
     :store-bridged? true
     :root-level     root-level
     :ns-overrides   (vec ns-overrides)}))

;;; ============================================================================
;;; REPL taps — ring-buffer signal capture, filtered by namespace
;;; ============================================================================

(defonce ^:private taps (atom {}))

(defn tap!
  "Capture signals matching `ns-pattern` into an in-memory ring buffer keyed
   by `tap-id`. Use `(recent tap-id)` to read them back, `(untap! tap-id)` to stop.

   Options: :n — buffer capacity (default 200).

   Example:
     (synthigy.log/tap! :sql \"synthigy.dataset.sql*\")
     (synthigy.log/recent :sql)"
  [tap-id ns-pattern & {:keys [n] :or {n 200}}]
  (when (contains? @taps tap-id)
    (try (t/remove-handler! tap-id) (catch Throwable _)))
  (let [buf (atom clojure.lang.PersistentQueue/EMPTY)]
    (swap! taps assoc tap-id buf)
    (t/add-handler! tap-id
      (fn [signal]
        (swap! buf #(let [q (conj % signal)]
                      (if (> (count q) n) (pop q) q))))
      {:ns-filter ns-pattern})
    tap-id))

(defn recent
  "Captured signals for `tap-id` as a vector (oldest first)."
  [tap-id]
  (some-> @taps (get tap-id) deref vec))

(defn recent-lines
  "Convenience: captured signals as `[ts level ns msg]` tuples."
  [tap-id]
  (mapv (fn [{:keys [inst level ns msg_]}]
          [(str inst) level ns (some-> msg_ force)])
        (recent tap-id)))

(defn untap!
  "Stop capturing for `tap-id` and discard its buffer."
  [tap-id]
  (try (t/remove-handler! tap-id) (catch Throwable _))
  (swap! taps dissoc tap-id)
  nil)

(defn list-taps
  "Currently active tap-ids."
  []
  (vec (keys @taps)))

;;; ============================================================================
;;; Callsite API — level macros
;;; ============================================================================

(defn- expand-level
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
  "Emit an :error-level signal. For attaching a throwable, use `error!`."
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

;;; ============================================================================
;;; Lifecycle module
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/log
  {:depends-on []
   :doc "pure logging, zero deps"
   :start (fn [] (install!))
   :stop  (fn [] (shutdown!))})
