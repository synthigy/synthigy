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

(ns synthigy.log.store.clickhouse
  "Log store and traffic store in ClickHouse: `logs` and `traffic_minutes`, written by a batching writer thread."
  (:require
   [clojure.string :as str]
   [patcho.patch :as patch]
   [synthigy.clickhouse :as ch]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.log.store.protocol :as store]
   [synthigy.log.store.ring :as ring]
   [synthigy.log.store.signal :as signal]
   [synthigy.traffic :as traffic])
  (:import
   [java.util ArrayList]
   [java.util.concurrent ArrayBlockingQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

;;; ============================================================================
;;; Schema
;;; ============================================================================

(defn logs-ddl
  "The logs table; rows older than `retention-days` expire (applied when the table is created)."
  [database retention-days]
  (str "CREATE TABLE IF NOT EXISTS " database ".logs (
          inst         DateTime64(3)            CODEC(ZSTD),
          level        LowCardinality(String)   CODEC(ZSTD),
          ns           LowCardinality(String)   CODEC(ZSTD),
          id           LowCardinality(String)   CODEC(ZSTD),
          msg          String                   CODEC(ZSTD),
          request_id   String                   CODEC(ZSTD),
          user_xid     LowCardinality(String)   CODEC(ZSTD),
          tenant       LowCardinality(String)   CODEC(ZSTD),
          host         LowCardinality(String)   CODEC(ZSTD),
          topics       Array(LowCardinality(String)) CODEC(ZSTD),
          data         String                   CODEC(ZSTD),
          ctx          String                   CODEC(ZSTD),
          error_class  LowCardinality(String)   CODEC(ZSTD),
          error_msg    String                   CODEC(ZSTD),
          error_trace  String                   CODEC(ZSTD),
          ip           String                   CODEC(ZSTD),
          date         Date MATERIALIZED toDate(inst),
          INDEX ip_bf ip TYPE bloom_filter GRANULARITY 4
        )
        ENGINE = MergeTree
        PARTITION BY date
        ORDER BY (tenant, inst, level)
        TTL date + INTERVAL " (long retention-days) " DAY
        SETTINGS index_granularity = 8192"))

(defn hist-col-name [b] (str "hist_" (if (= b :inf) "inf" b)))

(def hist-cols
  (mapv hist-col-name (conj (vec traffic/hist-bounds) :inf)))

(defn traffic-minutes-ddl
  "Delta rows, one per node per minute; SummingMergeTree sums same-key rows, which is what `flush-deltas!` relies on."
  [database]
  (str "CREATE TABLE IF NOT EXISTS " database ".traffic_minutes (
          node            LowCardinality(String),
          minute          Int64,
          ok              Int64,
          err             Int64,
          " (str/join ",\n          " (map #(str % " Int64") hist-cols)) ",
          ops             Int64,
          sse             Int64,
          subscriptions   Int64,
          deltas_dropped  Int64,
          date            Date MATERIALIZED toDate(toDateTime(minute * 60))
        )
        ENGINE = SummingMergeTree
        PARTITION BY date
        ORDER BY (node, minute)
        TTL date + INTERVAL 90 DAY
        SETTINGS index_granularity = 8192"))

(defn migrations-ddl [db]
  [(str "ALTER TABLE " db ".logs ADD COLUMN IF NOT EXISTS topics Array(LowCardinality(String)) CODEC(ZSTD)")
   (str "ALTER TABLE " db ".logs ADD COLUMN IF NOT EXISTS ip String CODEC(ZSTD)")
   (str "ALTER TABLE " db ".logs ADD INDEX IF NOT EXISTS ip_bf ip TYPE bloom_filter GRANULARITY 4")])

(defn apply-schema! [client {:keys [database log-retention-days] :as config}]
  (ch/exec-ddl! client config (ch/create-db-ddl database))
  (ch/exec-ddl! client config (logs-ddl database log-retention-days))
  (ch/exec-ddl! client config (traffic-minutes-ddl database))
  (doseq [ddl (migrations-ddl database)]
    (try (ch/exec-ddl! client config ddl) (catch Throwable _))))

(defn drop-schema! [client {:keys [database] :as config}]
  (ch/exec-ddl! client config (str "DROP TABLE IF EXISTS " database ".logs"))
  (ch/exec-ddl! client config (str "DROP TABLE IF EXISTS " database ".traffic_minutes")))

;;; ============================================================================
;;; Rows
;;; ============================================================================

(defn throwable->parts [x]
  (cond
    (instance? Throwable x) [(.getName (class x)) (.getMessage ^Throwable x) (signal/throwable-trace x)]
    (some? x)               [nil nil (str x)]
    :else                   [nil nil nil]))

(defn signal->log-row
  "A Telemere signal as a `logs` row — strings throughout, as JSONEachRow wants."
  [{:keys [inst level ns id msg_ data ctx error] :as sig}]
  (let [[err-class err-msg err-trace] (throwable->parts error)
        ctx-map (or ctx {})]
    {:inst        (ch/inst->ch-string inst)
     :level       (ch/str-or-empty (some-> level name))
     :ns          (ch/str-or-empty (when ns (str ns)))
     :id          (ch/str-or-empty (signal/id->str id))
     :msg         (ch/str-or-empty (when msg_ (try (force msg_) (catch Throwable _ ""))))
     :request_id  (ch/str-or-empty (or (:request-id sig) (get ctx-map :request-id)))
     :user_xid    (ch/str-or-empty (or (:user-xid sig) (get ctx-map :user-xid)))
     :tenant      (ch/str-or-empty (or (:tenant sig) (get ctx-map :tenant)))
     :host        (or (signal/host-of sig) "")
     ;; (subs (str kw) 1), not `name` — :traffic/sse must stay "traffic/sse"
     :topics      (->> (:topics sig) (map #(if (keyword? %) (subs (str %) 1) (str %))) sort vec)
     :data        (json/->json (or data {}))
     :ctx         (json/->json (apply dissoc ctx-map signal/promoted-ctx-keys))
     :error_class (ch/str-or-empty err-class)
     :error_msg   (ch/str-or-empty err-msg)
     :error_trace (ch/str-or-empty err-trace)
     :ip          (ch/str-or-empty (or (:ip sig) (get ctx-map :ip)))}))

;; never on audit — wait_for_async_insert=0 returns before CH has flushed
(def async-insert-settings {"async_insert" 1 "wait_for_async_insert" 0})

(defn insert-logs! [client config signals]
  (ch/insert! client config "logs" (mapv signal->log-row signals) async-insert-settings))

(defn traffic-row->ch-row [row]
  (into {:node (:node row) :minute (long (:minute row))
         :ok (long (:ok row 0)) :err (long (:err row 0))}
        (concat
         (map (fn [b] [(keyword (hist-col-name b)) (long (get-in row [:hist b] 0))]) traffic/hist-bounds)
         [[:hist_inf (long (get-in row [:hist :inf] 0))]
          [:ops (long (:ops row 0))]
          [:sse (long (:sse row 0))]
          [:subscriptions (long (:subscriptions row 0))]
          [:deltas_dropped (long (:deltas-dropped row 0))]])))

(defn insert-traffic-rows! [client config rows]
  (ch/insert! client config "traffic_minutes" (mapv traffic-row->ch-row rows) async-insert-settings))

;;; ============================================================================
;;; Writer thread — bounded queue, batched POST with retry
;;; ============================================================================

(defn backoff-ms [attempt {:keys [retry-min-ms retry-max-ms]}]
  (min (long retry-max-ms) (* (long retry-min-ms) (long (Math/pow 2 attempt)))))

(defn send-batch-with-retry! [client config signals]
  (loop [attempt 0]
    (let [outcome (try (insert-logs! client config signals) {:ok? true}
                       (catch Throwable t {:ok? false :err t}))]
      (cond
        (:ok? outcome) true
        (>= attempt (long (:retry-attempts config)))
        (do (binding [*out* *err*]
              (println (str "synthigy.log.store.clickhouse: log batch dropped after " (inc attempt) " attempts: "
                            (some-> (:err outcome) .getClass .getName) ": " (some-> (:err outcome) .getMessage))))
            false)
        :else (do (Thread/sleep (backoff-ms attempt config)) (recur (inc attempt)))))))

(defn drain-batch! [^ArrayBlockingQueue queue ^long batch-rows ^long batch-ms]
  (let [batch (ArrayList.)]
    (when-let [first-sig (.poll queue batch-ms TimeUnit/MILLISECONDS)]
      (.add batch first-sig)
      (.drainTo queue batch (dec batch-rows)))
    batch))

(defn writer-loop!
  [{:keys [^ArrayBlockingQueue queue client ^AtomicLong written ^AtomicLong dropped
           ^AtomicLong batches ^AtomicBoolean running? config]}]
  (let [{:keys [batch-rows batch-ms]} config
        flush! (fn [^ArrayList batch]
                 (if (send-batch-with-retry! client config batch)
                   (.addAndGet written (.size batch))
                   (.addAndGet dropped (.size batch))))]
    (while (.get running?)
      (try
        (let [batch (drain-batch! queue batch-rows batch-ms)]
          (when (pos? (.size batch))
            (flush! batch)
            (.incrementAndGet batches)))
        (catch InterruptedException _ (.set running? false))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.log.store.clickhouse: writer loop threw: "
                          (some-> t .getClass .getName) ": " (.getMessage t)))))))
    (let [tail (ArrayList.)]
      (.drainTo queue tail)
      (when (pos? (.size tail)) (flush! tail)))))

(defprotocol TransportOps
  (close-transport! [this])
  (transport-stats [this]))

(deftype Transport [^ArrayBlockingQueue queue ^AtomicLong written ^AtomicLong dropped
                    ^AtomicLong batches ^AtomicBoolean running? ^Thread writer config]
  clojure.lang.IFn
  (invoke [_ sig]
    (when-not (.offer queue sig) (.incrementAndGet dropped))
    nil)

  TransportOps
  (close-transport! [_]
    (.set running? false)
    (.interrupt writer)
    (try (.join writer (long (:close-timeout-ms config))) (catch InterruptedException _))
    nil)
  (transport-stats [_]
    {:queued (.size queue) :written (.get written) :dropped (.get dropped)
     :batches (.get batches) :running? (.get running?)}))

(defn make-transport ^Transport [client config]
  (let [queue    (ArrayBlockingQueue. (int (:buffer-size config)))
        running? (AtomicBoolean. true)
        written  (AtomicLong. 0)
        dropped  (AtomicLong. 0)
        batches  (AtomicLong. 0)
        thread   (doto (Thread. ^Runnable #(writer-loop! {:queue queue :client client :written written
                                                          :dropped dropped :batches batches
                                                          :running? running? :config config})
                                 "synthigy-log-ch-writer")
                   (.setDaemon true)
                   .start)]
    (->Transport queue written dropped batches running? thread config)))

;;; ============================================================================
;;; Query compilation — filter-map → CH SQL with {pN:Type} parameters
;;; ============================================================================

(defn value->ch-type [v]
  (cond (boolean? v) :Bool
        (integer? v) :Int64
        (float? v)   :Float64
        :else        :String))

(defn json-extract-fn [ch-type]
  (case ch-type
    :Int64   "JSONExtractInt"
    :Float64 "JSONExtractFloat"
    :Bool    "JSONExtractBool"
    :String  "JSONExtractString"))

(defn field-expr
  "`:field` → column; `[:data k …]` → JSONExtract* typed by the comparison value."
  [field for-value-type]
  (cond
    (keyword? field) (ch/kebab->snake field)
    (vector? field)  (let [[root & path] field]
                       (str (json-extract-fn for-value-type) "(" (ch/kebab->snake root) ", "
                            (str/join ", " (map #(str "'" (signal/json-key %) "'") path)) ")"))
    :else (throw (ex-info (str "Unsupported field reference: " (pr-str field)) {:field field}))))

(defn compile-comparison [field [op value] state]
  (let [t (value->ch-type value)]
    (str (field-expr field t) " "
         (case op := "=" :!= "!=" :> ">" :< "<" :>= ">=" :<= "<=")
         " " (ch/bind! state value t))))

(defn compile-membership [field values state]
  (when (empty? values)
    (throw (ex-info ":in / set value must be non-empty" {:field field})))
  (let [t (value->ch-type (first values))]
    (str (field-expr field t) " IN (" (str/join ", " (map #(ch/bind! state % t) values)) ")")))

(defn compile-text [field [op s] state]
  (let [col (field-expr field :String)
        ph  (ch/bind! state s :String)]
    (case op
      :contains    (str "position(" col ", " ph ") > 0")
      :icontains   (str "positionCaseInsensitive(" col ", " ph ") > 0")
      :starts-with (str "startsWith(" col ", " ph ")")
      :ends-with   (str "endsWith(" col ", " ph ")"))))

(defn compile-matches [field [_ pattern] state]
  (str "match(" (field-expr field :String) ", " (ch/bind! state (str pattern) :String) ")"))

(defn compile-presence [field [op]]
  (if (vector? field)
    (let [[root & path] field
          base (str "JSONHas(" (ch/kebab->snake root) ", "
                    (str/join ", " (map #(str "'" (signal/json-key %) "'") path)) ")")]
      (if (= op :exists?) base (str "NOT " base)))
    ;; columns are non-Nullable: CH stores "missing" as the empty string
    (let [col (ch/kebab->snake field)]
      (case op
        :exists? (str col " <> ''")
        :absent? (str col " = ''")))))

(defn compile-has [field [_ topic] state]
  (str "has(" (ch/kebab->snake field) ", " (ch/bind! state topic :String) ")"))

(defn compile-where-entry [[field value] state]
  (cond
    (and (vector? value) (keyword? (first value)))
    (let [op (first value)]
      (cond
        (#{:= :!= :> :< :>= :<=} op)                   (compile-comparison field value state)
        (= :in op)                                     (compile-membership field (second value) state)
        (#{:contains :icontains :starts-with :ends-with} op) (compile-text field value state)
        (= :matches op)                                (compile-matches field value state)
        (#{:exists? :absent?} op)                      (compile-presence field value)
        (= :has op)                                    (compile-has field value state)
        :else (throw (ex-info (str "Unknown operator: " op) {:field field :value value}))))
    (set? value) (compile-membership field value state)
    :else        (compile-comparison field [:= value] state)))

(defn compile-where [where state]
  (when (seq where)
    (str/join " AND " (map #(compile-where-entry % state) where))))

(defn duration->seconds [^String s]
  (when-let [[_ n unit] (re-matches #"(\d+)\s*([smhd])" s)]
    (let [n (Long/parseLong n)]
      (case unit "s" n "m" (* n 60) "h" (* n 3600) "d" (* n 86400)))))

(defn compile-time-bound [kind value state]
  (when value
    (let [op (if (= :since kind) ">" "<")]
      (if-let [secs (duration->seconds value)]
        (str "inst " op " (now() - INTERVAL " secs " SECOND)")
        (str "inst " op " parseDateTime64BestEffort(" (ch/bind! state value :String) ", 3)")))))

(defn compile-order-by [order-by group-by]
  (let [[field direction] (or order-by (if group-by [:count :desc] [:inst :desc]))]
    (str (cond (= :count field) "count"
               (vector? field)  (field-expr field :String)
               :else            (ch/kebab->snake field))
         " " (str/upper-case (name direction)))))

(defn compile-log-sql
  "A validated filter-map as `{:sql :params}` against <db>.logs."
  [filter-map database]
  (let [state   (ch/new-params)
        {:keys [where since until limit order-by group-by count? bucket-ms] :or {limit 100}} filter-map
        b       (when bucket-ms (long bucket-ms))
        clauses (remove str/blank? [(compile-where where state)
                                    (compile-time-bound :since since state)
                                    (compile-time-bound :until until state)])
        select  (cond b        (str "intDiv(toUnixTimestamp64Milli(inst), " b ") * " b " AS bucket, count() AS count")
                      count?   "count() AS count"
                      group-by (str (ch/kebab->snake group-by) ", count() AS count")
                      :else    "*")]
    {:sql    (str "SELECT " select " FROM " database ".logs"
                  (when (seq clauses) (str " WHERE " (str/join " AND " clauses)))
                  (cond b " GROUP BY bucket" group-by (str " GROUP BY " (ch/kebab->snake group-by)))
                  (cond b " ORDER BY bucket" count? nil :else (str " ORDER BY " (compile-order-by order-by group-by)))
                  (when-not (or count? b) (str " LIMIT " limit))
                  " FORMAT JSONEachRow")
     :params (:params @state)}))

(defn parse-nested-json [row]
  (reduce (fn [r k] (if (string? (get r k)) (try (update r k json/<-json) (catch Throwable _ r)) r))
          row [:data :ctx]))

(defn run-query [client config {:keys [sql params]}]
  (mapv parse-nested-json (ch/parse-rows (ch/query! client config sql params))))

(defn ->wire-row
  "A `logs` row in the wire-schema shape the other stores return: `:v`, ISO `:inst`."
  [row]
  (-> row (assoc :v 1) (update :inst ch/ch->iso)))

(defn search [client config {:keys [count? group-by bucket-ms] :as opts}]
  (let [compiled (compile-log-sql opts (:database config))
        rows     (run-query client config compiled)]
    (cond
      bucket-ms (mapv (fn [r] [(ch/parse-count (:bucket r)) (ch/parse-count (:count r))]) rows)
      count?    (some-> rows first :count ch/parse-count)
      group-by  (let [col (keyword (ch/kebab->snake group-by))]
                  (mapv (fn [r] {group-by (get r col) :count (ch/parse-count (:count r))}) rows))
      :else     (mapv ->wire-row rows))))

;;; ============================================================================
;;; Traffic reads — GROUP BY sums unmerged SummingMergeTree parts
;;; ============================================================================

(defn query-traffic-window [client config from to]
  (let [state (ch/new-params)
        sql   (str "SELECT node, minute, sum(ok) AS ok, sum(err) AS err, "
                   (str/join ", " (map #(str "sum(" % ") AS " %) hist-cols))
                   ", sum(ops) AS ops, sum(sse) AS sse, sum(subscriptions) AS subscriptions,"
                   " sum(deltas_dropped) AS deltas_dropped"
                   " FROM " (:database config) ".traffic_minutes"
                   " WHERE minute >= " (ch/bind! state (long from) :Int64)
                   " AND minute <= " (ch/bind! state (long to) :Int64)
                   " GROUP BY node, minute FORMAT JSONEachRow")]
    (mapv (fn [r]
            {:node (:node r) :minute (:minute r) :ok (:ok r) :err (:err r)
             :hist (into {} (map (fn [b] [b (get r (keyword (hist-col-name b)) 0)]))
                         (conj (vec traffic/hist-bounds) :inf))
             :ops (:ops r) :sse (:sse r) :subscriptions (:subscriptions r)
             :deltas-dropped (:deltas_dropped r)})
          (ch/parse-rows (ch/query! client config sql (:params @state))))))

;;; ============================================================================
;;; Store
;;; ============================================================================

(defrecord ClickHouseLogStore [^Transport tp client config]
  store/LogStore
  (write-signal! [_ sig]
    (when sig (tp sig)))

  (recent [this {:keys [limit ns-pattern level since]}]
    (store/search this (cond-> {:order-by [:inst :desc] :limit (or limit 100)}
                         ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                         level      (update :where (fnil assoc {}) :level level)
                         since      (assoc :since since))))

  (search [_ opts]
    (search client config opts))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [rows (store/search this (cond-> {:order-by [:inst :asc] :limit (or limit 100)}
                                    cursor     (assoc :since cursor)
                                    ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                                    level      (update :where (fnil assoc {}) :level level)))]
      {:rows rows :next-cursor (or (some-> (last rows) :inst) cursor)}))

  (clear! [_]
    (ch/exec-ddl! client config (str "DROP TABLE IF EXISTS " (:database config) ".logs"))
    (ch/exec-ddl! client config (logs-ddl (:database config) (:log-retention-days config)))
    nil)

  (health [_]
    (let [s (transport-stats tp)]
      (merge s {:up?      (boolean (:running? s))
                :backend  :clickhouse
                :url      (:url config)
                :database (:database config)
                :rows     (try (ch/row-count client config "logs") (catch Throwable _ nil))})))

  (snapshot [_] nil)

  traffic/TrafficStore
  (flush-deltas! [_ rows]
    (when (seq rows) (insert-traffic-rows! client config rows))
    nil)

  (query-window [_ {:keys [from to]}]
    (try (query-traffic-window client config from to) (catch Throwable _ [])))

  (traffic-health [_]
    {:up? true :backend :clickhouse
     :rows (try (ch/row-count client config "traffic_minutes") (catch Throwable _ nil))}))

(defn open-store
  "Connect with `config` over the CLICKHOUSE_* environment, apply the schema and start the writer."
  [config]
  (let [cfg    (merge (ch/env-config) (select-keys config (keys ch/default-config)))
        client (ch/new-client cfg)]
    (apply-schema! client cfg)
    (->ClickHouseLogStore (make-transport client cfg) client cfg)))

(defn close-store! [{:keys [tp]}]
  (try (close-transport! tp) (catch Throwable _)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(defonce state (atom nil))

(defn drain-snapshot! [outgoing incoming]
  (let [signals (try (store/snapshot outgoing) (catch Throwable _ nil))]
    (when (seq signals)
      (doseq [sig signals]
        (try (store/write-signal! incoming sig) (catch Throwable _)))
      (log/info {:id ::ring-drained
                 :data {:action :written :subject :log-store :backend :clickhouse :signals (count signals)}}
                (str "Drained " (count signals) " buffered signal(s) into ClickHouse")))))

(defn setup []
  (let [cfg (ch/env-config)]
    (apply-schema! (ch/new-client cfg) cfg)
    (log/info {:id ::setup-complete
               :data {:action :setup-complete :subject :log-store :backend :clickhouse
                      :url (:url cfg) :database (:database cfg)}}
              "ClickHouse log store schema applied")))

(defn start []
  (let [rec      (open-store {})
        outgoing @#'store/*log-store*]
    (patch/level! :synthigy.log/clickhouse)
    ;; swap before draining — draining first leaves a race the width of the drain
    (alter-var-root #'store/*log-store* (constantly rec))
    (drain-snapshot! outgoing rec)
    (alter-var-root #'traffic/*traffic-store* (constantly rec))
    (reset! state {:record rec})
    (log/info {:id ::started
               :data {:action :started :subject :log-store :backend :clickhouse
                      :url (get-in rec [:config :url]) :database (get-in rec [:config :database])}}
              "ClickHouse log store started")))

(defn stop []
  (alter-var-root #'traffic/*traffic-store* (constantly (traffic/create-in-memory-store)))
  (when-let [{:keys [record]} @state]
    (close-store! record))
  (alter-var-root #'store/*log-store* (constantly (ring/create-ring)))
  (reset! state nil)
  (log/info {:id ::stopped :data {:action :stopped :subject :log-store :backend :clickhouse}}
            "ClickHouse log store stopped"))

(defn cleanup []
  (let [cfg (ch/env-config)]
    (drop-schema! (ch/new-client cfg) cfg)))

(patch/current-version :synthigy.log/clickhouse "1.0.0")
