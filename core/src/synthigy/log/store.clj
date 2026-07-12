(ns synthigy.log.store
  "Log substrate — `LogStore` protocol contract, the single shared dynvar
  that holds the currently-bound store, and the in-memory `RingLogStore`
  that ships as the dynvar's default.

  ## Provider pattern

  Mirrors `synthigy.dataset.access/*access-control*` (defonce'd to a
  permissive `AllowAllAccess` record) and `synthigy.audit/*audit-provider*`
  in shape:

    `*log-store*` is defonce'd to a fresh `RingLogStore` so the dynvar
    is never nil. Reads and writes flow against whatever store is bound.
    A backend module (`:synthigy/observability`) is the ONLY thing that
    swaps the dynvar at runtime — via `alter-var-root` on `:start` (after
    snapshotting and draining the outgoing store) and back to a fresh
    ring on `:stop`. `synthigy.log/install!` does NOT touch this dynvar;
    it only owns the Telemere pipeline + handlers.

  ## Two reference implementations

    `RingLogStore`              — in-memory bounded ring; default
    `:synthigy/observability`   — DuckDB (embedded) or ClickHouse (HTTP);
                                  shadowed-ns picked by classpath alias

  ## Why ring as the default

  Logs are queryable from the moment the JVM is up — boot-phase signals
  land in the ring, `synthigy.log.query/query` works, the cockpit Logs
  lens renders. When observability starts, it `alter-var-root`'s the dynvar
  to its durable record FIRST (so new signals land durably at once), then
  `(store/snapshot s)` the now-orphaned ring and replays each buffered
  signal into the durable backend. The old ring drops out of reference.
  Observability stop rebinds a fresh `(create-ring)` so the queryable
  surface survives backend teardown.

  ## Wire-row shape

  `search`/`recent`/`tail` return rows keyed with snake_case keywords
  matching the wire-schema-v1 columns DuckDB and ClickHouse emit
  (`:request_id`, `:user_xid`, `:error_class`, …), so cockpit + HTTP
  serializers can't tell which store backed the response."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]])
  (:import
   [java.io PrintWriter StringWriter]
   [java.time Instant]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.regex Pattern]))

;;; ============================================================================
;;; Protocol
;;; ============================================================================

(defprotocol LogStore
  "Seven-method contract for a log substrate implementation. Writers are
  called by the Telemere bridge handler per signal. Readers serve the
  cockpit Logs lens. `snapshot` lets the observability module hand off
  buffered state when swapping backends."

  (write-signal! [this signal]
    "Consume one Telemere signal map durably. Async-ok; the caller does
     not wait. Must not throw — drop the signal, increment an internal
     counter, optionally print to *err*. Returns nil.")

  (recent [this opts]
    "Return up to `:limit` recent signals (default backend-specific).
     opts: {:limit :ns-pattern :level :since}. Newest-first by default.
     Returns a vector of wire-schema-v1 row maps with snake_case keys
     matching synthigy.log/signal->json-line.")

  (search [this opts]
    "Filtered query over the store. opts is the validated filter-map from
     synthigy.log.query (`:where :since :until :limit :order-by :group-by
     :count?`). Returns a vector of wire-schema-v1 rows, or a Long when
     `:count?` is true, or a vector of {group-by-field value :count n}
     maps when `:group-by` is set.")

  (tail [this opts]
    "Cursor-based follow. opts: {:cursor :ns-pattern :level :limit}.
     Returns {:rows [...] :next-cursor x} — the cursor is opaque, the
     caller passes it back to resume. Stores may implement this on top
     of `recent` if they have no native cursor support.")

  (clear! [this]
    "Empty the store. Dev convenience for in-memory backends; durable
     backends may no-op or implement DELETE. Returns nil.")

  (health [this]
    "Snapshot map describing store liveness. Conventional keys:
       {:up? boolean :rows long :dropped long :backend keyword
        :path string-or-nil}
     Must not throw.")

  (snapshot [this]
    "Return a vector of raw Telemere signal maps currently held by this
     store, in chronological (oldest-first) order. Used by
     `:synthigy/observability` start to drain the transient default
     `RingLogStore` into a durable backend before swapping `*log-store*`.
     Durable backends should return nil to signal 'no transient state
     to hand off'."))

;;; ============================================================================
;;; RingLogStore — bounded in-memory implementation; defonce default
;;; ============================================================================

(def ^:private default-ring-size 10000)

(defn- env-ring-size []
  (or (try (some-> (env :synthigy-log-ring-size) str/trim Integer/parseInt)
           (catch Throwable _ nil))
      default-ring-size))

(def ^:private promoted-ctx-keys
  [:request-id :user-xid :tenant])

(defn- id->str [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (some? id)              (str id)))

(defn- throwable-msg [^Throwable t]
  (some-> t .getMessage))

(defn- throwable-trace [^Throwable t]
  (when t
    (let [sw (StringWriter.) pw (PrintWriter. sw)]
      (.printStackTrace t pw) (.flush pw) (.toString sw))))

(defn- host-of [signal]
  (let [h (:host signal)]
    (cond (string? h) h
          (map? h)    (:name h)
          :else       nil)))

(defn- signal-field
  "Extract the value at `field` from a Telemere signal, in the shape the
  query predicate evaluator expects. Returns nil if absent. Field
  vocabulary matches `synthigy.log.query/built-in-columns` plus
  `[:data k …]` / `[:ctx k …]` paths."
  [signal field]
  (cond
    (= field :level) (some-> (:level signal) name)
    (= field :ns)    (:ns signal)
    (= field :id)    (id->str (:id signal))
    (= field :msg)   (some-> (:msg_ signal) force)
    (= field :inst)  (:inst signal)
    (= field :host)  (host-of signal)
    (= field :topics) (:topics signal)
    (= field :request-id)
    (or (:request-id signal) (get-in signal [:ctx :request-id]))
    (= field :user-xid)
    (or (:user-xid signal)   (get-in signal [:ctx :user-xid]))
    (= field :tenant)
    (or (:tenant signal)     (get-in signal [:ctx :tenant]))
    (= field :v) 1
    (= field :error-class)
    (when-let [e (:error signal)]
      (cond (instance? Throwable e) (.getName (class e))
            :else (some-> e class .getName)))
    (= field :error-msg)
    (when-let [e (:error signal)]
      (if (instance? Throwable e) (throwable-msg e) (str e)))
    (= field :error-trace)
    (when-let [e (:error signal)]
      (if (instance? Throwable e) (throwable-trace e) (str e)))

    (vector? field)
    (let [[head & path] field
          path (vec path)]
      (case head
        :data (get-in (:data signal) path)
        :ctx  (get-in (:ctx signal)  path)
        nil))

    :else nil))

(defn- normalize-scalar
  "Filter values for `:level` / `:id` are typically keywords at call sites
  but the field projection returns strings. Coerce so equality works."
  [field v]
  (cond
    (#{:level :id} field) (cond (keyword? v) (name v)
                                (string? v)  v
                                :else        (str v))
    :else v))

(defn- normalize-set [field s]
  (into #{} (map #(normalize-scalar field %)) s))

(defn- nil-safe-compare
  "Generic compare that does not throw on nil. Nil sorts last regardless
  of direction (so empty fields don't dominate ordering)."
  [a b]
  (cond (= a b) 0
        (nil? a) 1
        (nil? b) -1
        :else    (try (compare a b) (catch Throwable _ 0))))

(defn- cmp-op [op a b]
  (let [c (nil-safe-compare a b)]
    (case op
      :>  (pos? c)
      :<  (and (neg? c) (some? a))
      :>= (and (or (zero? c) (pos? c)) (some? a))
      :<= (and (or (zero? c) (neg? c)) (some? a)))))

(defn- str-or-nil [x] (when x (str x)))

(defn- tuple-pred [field [op arg]]
  (case op
    := (let [arg' (normalize-scalar field arg)]
         #(= (signal-field % field) arg'))
    :!= (let [arg' (normalize-scalar field arg)]
          #(not= (signal-field % field) arg'))
    :> #(cmp-op :> (signal-field % field) arg)
    :< #(cmp-op :< (signal-field % field) arg)
    :>= #(cmp-op :>= (signal-field % field) arg)
    :<= #(cmp-op :<= (signal-field % field) arg)
    :in (let [s (normalize-set field arg)]
          #(contains? s (signal-field % field)))
    :contains    #(when-let [s (str-or-nil (signal-field % field))]
                    (str/includes? s arg))
    :icontains   (let [needle (str/lower-case arg)]
                   #(when-let [s (str-or-nil (signal-field % field))]
                      (str/includes? (str/lower-case s) needle)))
    :starts-with #(when-let [s (str-or-nil (signal-field % field))]
                    (str/starts-with? s arg))
    :ends-with   #(when-let [s (str-or-nil (signal-field % field))]
                    (str/ends-with? s arg))
    :matches     (let [^Pattern p arg]
                   #(when-let [s (str-or-nil (signal-field % field))]
                      (boolean (re-find p s))))
    :exists?     #(some? (signal-field % field))
    :absent?     #(nil? (signal-field % field))
    :has         (let [needle (name arg)]
                   #(contains? (into #{} (map name) (or (signal-field % field) #{}))
                               needle))))

(defn- where-pred
  "Compile a `:where` map into a (signal → bool) predicate. Implicit AND."
  [where]
  (if (empty? where)
    (constantly true)
    (let [preds (mapv
                  (fn [[field v]]
                    (cond
                      (set? v)
                      (let [s (normalize-set field v)]
                        #(contains? s (signal-field % field)))
                      (and (vector? v) (keyword? (first v)))
                      (tuple-pred field v)
                      :else
                      (let [v' (normalize-scalar field v)]
                        #(= (signal-field % field) v'))))
                  where)]
      (fn [sig]
        (loop [ps preds]
          (cond (empty? ps) true
                ((first ps) sig) (recur (rest ps))
                :else false))))))

(def ^:private duration-re
  #"^(\d+)\s*([smhd])$")

(defn- parse-time-ref
  "Parse a string into an Instant. Accepts ISO-8601 instants or short
  durations (`10s`, `5m`, `2h`, `1d`) interpreted as 'now - duration'."
  [s]
  (when (and s (string? s))
    (let [s (str/trim s)]
      (or (try (Instant/parse s) (catch Throwable _ nil))
          (when-let [[_ n unit] (re-matches duration-re s)]
            (let [n      (Long/parseLong n)
                  millis (* n (case unit
                                "s" 1000
                                "m" 60000
                                "h" 3600000
                                "d" 86400000))]
              (.minusMillis (Instant/now) millis)))))))

(defn- within-window [since-inst until-inst]
  (cond
    (and since-inst until-inst)
    #(when-let [^Instant t (:inst %)]
       (and (not (.isBefore t since-inst))
            (not (.isAfter  t until-inst))))
    since-inst
    #(when-let [^Instant t (:inst %)]
       (not (.isBefore t since-inst)))
    until-inst
    #(when-let [^Instant t (:inst %)]
       (not (.isAfter t until-inst)))
    :else (constantly true)))

(defn- signal->wire-row
  [signal]
  (let [ctx-map      (or (:ctx signal) {})
        residual-ctx (apply dissoc ctx-map promoted-ctx-keys)
        e            (:error signal)
        err-class    (when e (if (instance? Throwable e)
                               (.getName (class e))
                               (some-> e class .getName)))
        err-msg      (when e (if (instance? Throwable e)
                               (throwable-msg e)
                               (str e)))
        err-trace    (when e (if (instance? Throwable e)
                               (throwable-trace e)
                               (str e)))]
    {:v           1
     :inst        (some-> ^Instant (:inst signal) str)
     :level       (some-> (:level signal) name)
     :ns          (:ns signal)
     :id          (id->str (:id signal))
     :msg         (some-> (:msg_ signal) force)
     :request_id  (or (:request-id signal) (get ctx-map :request-id))
     :user_xid    (or (:user-xid signal)   (get ctx-map :user-xid))
     :tenant      (or (:tenant signal)     (get ctx-map :tenant))
     :host        (host-of signal)
     :topics      (->> (:topics signal) (map name) sort vec)
     :data        (or (:data signal) {})
     :ctx         residual-ctx
     :error_class err-class
     :error_msg   err-msg
     :error_trace err-trace}))

(def ^:private default-limit 100)

(defn- order-by-key-fn [field]
  (fn [sig] (signal-field sig field)))

(defn- run-search
  [signals {:keys [where since until limit order-by count?] :as opts}]
  (let [group-field (:group-by opts)
        since-i  (parse-time-ref since)
        until-i  (parse-time-ref until)
        wpred    (where-pred where)
        in-win   (within-window since-i until-i)
        survivors (->> signals
                       (filter in-win)
                       (filter wpred))
        [ofield odir] (or order-by [:inst :desc])
        sorted   (sort-by (order-by-key-fn ofield)
                          (if (= odir :asc)
                            nil-safe-compare
                            #(nil-safe-compare %2 %1))
                          survivors)
        limit    (or limit default-limit)]
    (cond
      count?
      (long (count survivors))

      group-field
      (->> survivors
           (clojure.core/group-by #(signal-field % group-field))
           (mapv (fn [[v sigs]]
                   {group-field v :count (long (count sigs))})))

      :else
      (->> sorted (take limit) (mapv signal->wire-row)))))

(defn- push!
  [buffer-atom ^AtomicLong dropped max-size signal]
  (let [popped? (volatile! false)]
    (swap! buffer-atom
           (fn [q]
             (let [q' (conj q signal)]
               (if (> (count q') max-size)
                 (do (vreset! popped? true) (pop q'))
                 q'))))
    (when @popped? (.incrementAndGet dropped))))

(defrecord RingLogStore [buffer dropped max-size]
  LogStore

  (write-signal! [_ signal]
    (when signal
      (try (push! buffer dropped max-size signal)
           (catch Throwable _)))
    nil)

  (recent [this {:keys [limit ns-pattern level since]}]
    (let [opts (cond-> {:order-by [:inst :desc] :limit (or limit default-limit)}
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level)
                 since      (assoc :since since))]
      (search this opts)))

  (search [_ opts]
    (run-search (seq @buffer) opts))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [opts (cond-> {:order-by [:inst :asc] :limit (or limit default-limit)}
                 cursor     (assoc :since cursor)
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level))
          rows (search this opts)]
      {:rows rows :next-cursor (or (some-> (last rows) :inst) cursor)}))

  (clear! [_]
    (reset! buffer clojure.lang.PersistentQueue/EMPTY)
    (.set ^AtomicLong dropped 0)
    nil)

  (health [_]
    {:up?      true
     :backend  :ring
     :path     nil
     :rows     (long (count @buffer))
     :capacity (long max-size)
     :dropped  (.get ^AtomicLong dropped)})

  (snapshot [_]
    (vec @buffer)))

(defn create-ring
  "Build a fresh `RingLogStore`. `:size` overrides the default
   (env `SYNTHIGY_LOG_RING_SIZE` or 10000)."
  ([] (create-ring nil))
  ([{:keys [size]}]
   (->RingLogStore (atom clojure.lang.PersistentQueue/EMPTY)
                   (AtomicLong. 0)
                   (long (or size (env-ring-size))))))

;;; ============================================================================
;;; The dynvar — defonce'd to a fresh ring so it's never nil
;;; ============================================================================

(defonce ^:dynamic *log-store* (create-ring))
