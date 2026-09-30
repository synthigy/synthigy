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

(ns synthigy.log.store.duckdb
  "Logs and traffic in an embedded DuckDB file, capped by rows and age."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [patcho.patch :as patch]
   [synthigy.duckdb :as duck]
   synthigy.env
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.log.store.protocol :as store]
   [synthigy.log.store.ring :as ring]
   [synthigy.traffic :as traffic])
  (:import
   [java.sql Connection ResultSet]
   [java.time Instant]
   [java.util ArrayList]
   [java.util.concurrent ArrayBlockingQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]
   [org.duckdb DuckDBConnection]))

(def default-config
  {:logs-path          (str synthigy.env/home "/db/logs.duckdb")
   :buffer-size        8192
   :batch-rows         500
   :batch-ms           250
   :log-max-rows       1500000
   :log-retention-days 30
   :retry-attempts     3
   :retry-min-ms       100
   :retry-max-ms       5000
   :close-timeout-ms   2000})

(defn env-config []
  (merge default-config
         {:logs-path          (or (not-empty (env :duckdb-logs-path)) (:logs-path default-config))
          :buffer-size        (duck/parse-int-or (env :duckdb-buffer-size) (:buffer-size default-config))
          :batch-rows         (duck/parse-int-or (env :duckdb-batch-rows) (:batch-rows default-config))
          :batch-ms           (duck/parse-int-or (env :duckdb-batch-ms) (:batch-ms default-config))
          :log-max-rows       (duck/parse-int-or (env :duckdb-log-max-rows) (:log-max-rows default-config))
          :log-retention-days (duck/parse-int-or (env :duckdb-log-retention-days) (:log-retention-days default-config))}))

;;; ============================================================================
;;; Schema
;;; ============================================================================

(defn hist-col-name [b] (str "hist_" (if (= b :inf) "inf" b)))

(def hist-cols
  (mapv hist-col-name (conj (vec traffic/hist-bounds) :inf)))

(def schema-ddl
  ["CREATE TABLE IF NOT EXISTS synthigy_logs (
      inst        TIMESTAMP,
      level       VARCHAR,
      ns          VARCHAR,
      id          VARCHAR,
      msg         VARCHAR,
      request_id  VARCHAR,
      user_xid    VARCHAR,
      tenant      VARCHAR,
      host        VARCHAR,
      topics      VARCHAR,
      data        JSON,
      ctx         JSON,
      error_class VARCHAR,
      error_msg   VARCHAR,
      error_trace VARCHAR
    )"
   (str "CREATE TABLE IF NOT EXISTS traffic_minutes (
      node            VARCHAR,
      minute          BIGINT,
      ok              BIGINT,
      err             BIGINT,
      " (str/join ",\n      " (map #(str % " BIGINT") hist-cols)) ",
      ops             BIGINT,
      sse             BIGINT,
      subscriptions   BIGINT,
      deltas_dropped  BIGINT
    )")])

;;; ============================================================================
;;; Signal → row
;;; ============================================================================

(def promoted-ctx-keys [:request-id :user-xid :tenant])

(defn topics->delimited
  "A topic set as a pipe-bounded sorted string (`|system|dataset|`), so membership is a bounded LIKE."
  [topics]
  (let [names (->> topics (map (fn [t] (if (keyword? t) (subs (str t) 1) (str t)))) sort)]
    (if (seq names) (str "|" (str/join "|" names) "|") "")))

(defn delimited->topics [s]
  (if (str/blank? s) [] (->> (str/split s #"\|") (remove str/blank?) vec)))

(defn throwable->parts [x]
  (cond
    (instance? Throwable x)
    (let [^Throwable t x
          sw (java.io.StringWriter.)
          pw (java.io.PrintWriter. sw)]
      (.printStackTrace t pw) (.flush pw)
      [(.getName (class t)) (.getMessage t) (.toString sw)])
    (some? x) [nil nil (str x)]
    :else     [nil nil nil]))

(defn id->str [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (some? id)              (str id)))

(defn signal->log-row
  "Project a Telemere signal map to the synthigy_logs column shape."
  [{:keys [inst level ns id msg_ data ctx error] :as signal}]
  (let [[err-class err-msg err-trace] (throwable->parts error)
        ctx-map  (or ctx {})
        host-raw (:host signal)]
    {:inst        (duck/inst->utc inst)
     :level       (some-> level name)
     :ns          (when ns (str ns))
     :id          (id->str id)
     :msg         (when msg_ (try (force msg_) (catch Throwable _ nil)))
     :request_id  (or (:request-id signal) (get ctx-map :request-id))
     :user_xid    (or (:user-xid signal) (get ctx-map :user-xid))
     :tenant      (or (:tenant signal) (get ctx-map :tenant))
     :host        (cond (string? host-raw) host-raw (map? host-raw) (:name host-raw))
     :topics      (topics->delimited (:topics signal))
     :data        (json/->json (or data {}))
     :ctx         (json/->json (apply dissoc ctx-map promoted-ctx-keys))
     :error_class err-class
     :error_msg   err-msg
     :error_trace err-trace}))

(def insert-log-sql "INSERT INTO synthigy_logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

(def log-columns [:inst :level :ns :id :msg :request_id :user_xid :tenant :host :topics
                  :data :ctx :error_class :error_msg :error_trace])

(defn insert-log-batch! [^Connection conn signals]
  (when (seq signals)
    (with-open [ps (.prepareStatement conn insert-log-sql)]
      (doseq [s signals]
        (let [row (signal->log-row s)]
          (.setObject ps 1 (:inst row))
          (doseq [[i k] (map-indexed vector (rest log-columns))]
            (.setString ps (int (+ 2 i)) (get row k))))
        (.addBatch ps))
      (.executeBatch ps))))

;;; ============================================================================
;;; Retention
;;; ============================================================================

(def log-prune-check-ms (* 5 60000))

(defonce last-log-prune (atom 0))

(defn prune-logs!
  "Delete synthigy_logs rows past :log-retention-days or beyond :log-max-rows, whichever bites first."
  [^Connection conn {:keys [log-max-rows log-retention-days]}]
  (let [cutoff (duck/inst->utc (.minus (Instant/now) (long log-retention-days) java.time.temporal.ChronoUnit/DAYS))]
    (with-open [ps (.prepareStatement conn "DELETE FROM synthigy_logs WHERE inst < ?")]
      (.setObject ps 1 cutoff)
      (.executeUpdate ps)))
  (duck/exec! conn (str "DELETE FROM synthigy_logs WHERE inst < "
                        "(SELECT min(inst) FROM "
                        "(SELECT inst FROM synthigy_logs ORDER BY inst DESC LIMIT " (long log-max-rows) "))")))

(defn maybe-prune-logs! [conn config]
  (let [now-ms (System/currentTimeMillis)]
    (when (> (- now-ms @last-log-prune) log-prune-check-ms)
      (reset! last-log-prune now-ms)
      (try (prune-logs! conn config) (catch Throwable _ nil)))))

;;; ============================================================================
;;; Writer
;;; ============================================================================

(defn backoff-ms [attempt {:keys [retry-min-ms retry-max-ms]}]
  (min (long retry-max-ms) (* (long retry-min-ms) (long (Math/pow 2 attempt)))))

(defn send-log-batch-with-retry! [conn config signals]
  (let [max-attempts (long (:retry-attempts config))]
    (loop [attempt 0]
      (let [outcome (try (insert-log-batch! conn signals) {:ok? true}
                         (catch Throwable t {:ok? false :err t}))]
        (cond
          (:ok? outcome) true
          (>= attempt max-attempts)
          (do (binding [*out* *err*]
                (println (str "synthigy.log.store.duckdb: log batch dropped after "
                              (inc attempt) " attempts: "
                              (some-> ^Throwable (:err outcome) .getClass .getName) ": "
                              (some-> ^Throwable (:err outcome) .getMessage))))
              false)
          :else
          (do (Thread/sleep (long (backoff-ms attempt config)))
              (recur (inc attempt))))))))

(defn drain-log-batch! [^ArrayBlockingQueue queue batch-rows batch-ms]
  (let [batch (ArrayList.)
        first-sig (.poll queue (long batch-ms) TimeUnit/MILLISECONDS)]
    (when first-sig
      (.add batch first-sig)
      (.drainTo queue batch (int (dec batch-rows))))
    batch))

(defn log-writer-loop!
  [{:keys [^ArrayBlockingQueue queue ^Connection conn ^AtomicLong written
           ^AtomicLong dropped ^AtomicLong batches ^AtomicBoolean running? config]}]
  (let [{:keys [batch-rows batch-ms]} config]
    (while (.get running?)
      (try
        (let [batch (drain-log-batch! queue batch-rows batch-ms)]
          (when (pos? (.size batch))
            (let [ok? (send-log-batch-with-retry! conn config batch)]
              (.incrementAndGet batches)
              (if ok? (.addAndGet written (.size batch)) (.addAndGet dropped (.size batch))))))
        (maybe-prune-logs! conn config)
        (catch InterruptedException _ (.set running? false))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.log.store.duckdb: log writer loop threw: "
                          (some-> t .getClass .getName) ": " (.getMessage t)))))))
    (let [tail (ArrayList.)]
      (.drainTo queue tail)
      (when (pos? (.size tail))
        (if (send-log-batch-with-retry! conn config tail)
          (.addAndGet written (.size tail))
          (.addAndGet dropped (.size tail)))))
    (try (.close conn) (catch Throwable _))))

(defprotocol TransportOps
  (close-transport! [this])
  (transport-stats [this])
  (transport-connection ^Connection [this]))

(deftype Transport [^ArrayBlockingQueue queue
                    ^Connection conn
                    ^AtomicLong written
                    ^AtomicLong dropped
                    ^AtomicLong batches
                    ^AtomicBoolean running?
                    ^Thread writer
                    config]
  clojure.lang.IFn
  (invoke [_ signal]
    (when-not (.offer queue signal) (.incrementAndGet dropped))
    nil)

  TransportOps
  (close-transport! [_]
    (.set running? false)
    (.interrupt writer)
    (try (.join writer (long (:close-timeout-ms config))) (catch InterruptedException _))
    (try (.close conn) (catch Throwable _))
    nil)
  (transport-stats [_]
    {:queued   (.size queue)
     :written  (.get written)
     :dropped  (.get dropped)
     :batches  (.get batches)
     :running? (.get running?)})
  (transport-connection [_] conn))

(defn make-transport ^Transport [^Connection conn config]
  (let [queue    (ArrayBlockingQueue. (int (:buffer-size config)))
        running? (AtomicBoolean. true)
        written  (AtomicLong. 0)
        dropped  (AtomicLong. 0)
        batches  (AtomicLong. 0)
        ;; a duplicate, never a second getConnection — for :memory: that would be another, empty database
        log-conn (.duplicate ^DuckDBConnection conn)
        state    {:queue queue :conn log-conn :written written :dropped dropped
                  :batches batches :running? running? :config config}
        thread   (doto (Thread. ^Runnable (fn [] (log-writer-loop! state)) "synthigy-duckdb-log-writer")
                   (.setDaemon true)
                   .start)]
    (->Transport queue conn written dropped batches running? thread config)))

;;; ============================================================================
;;; Query compilation — filter-map → SQL + params
;;; ============================================================================

(defn kebab->snake [k] (str/replace (name k) "-" "_"))

(defn field-expr
  "A column, or `[:data k1 k2 …]` as json_extract_string(data, '$.k1.k2')."
  [field]
  (cond
    (keyword? field) (kebab->snake field)
    (vector? field)
    (let [[root & path] field]
      (str "json_extract_string(" (kebab->snake root) ", '$." (str/join "." (map kebab->snake path)) "')"))
    :else
    (throw (ex-info (str "Unsupported field reference: " (pr-str field)) {:field field}))))

(defn numeric-cast [expr] (str "TRY_CAST(" expr " AS DOUBLE)"))

(defn comparand-expr [field operand]
  (let [e (field-expr field)]
    (if (and (vector? field) (number? operand)) (numeric-cast e) e)))

(defn new-params [] (atom []))

(defn bind! [params v]
  (swap! params conj v)
  "?")

(defn normalize-value [v]
  (if (keyword? v) (subs (str v) 1) v))

(defn compile-comparison [field [op value] params]
  (let [sql-op (case op := "=" :!= "!=" :> ">" :< "<" :>= ">=" :<= "<=")]
    (str (comparand-expr field value) " " sql-op " " (bind! params (normalize-value value)))))

(defn compile-membership [field values params]
  (when (empty? values)
    (throw (ex-info ":in / set value must be non-empty" {:field field})))
  (let [fe (if (and (vector? field) (every? number? values))
             (numeric-cast (field-expr field))
             (field-expr field))]
    (str fe " IN (" (str/join ", " (mapv #(bind! params (normalize-value %)) values)) ")")))

(defn compile-text [field [op s] params]
  (let [col (field-expr field) ph (bind! params s)]
    (case op
      :contains    (str "position(" ph " IN " col ") > 0")
      :icontains   (str "position(LOWER(" ph ") IN LOWER(" col ")) > 0")
      :starts-with (str "starts_with(" col ", " ph ")")
      :ends-with   (str "ends_with(" col ", " ph ")"))))

(defn compile-where-entry [[field value] params]
  (cond
    (and (vector? value) (keyword? (first value)))
    (let [op (first value)]
      (cond
        (#{:= :!= :> :< :>= :<=} op)                        (compile-comparison field value params)
        (= :in op)                                          (compile-membership field (second value) params)
        (#{:contains :icontains :starts-with :ends-with} op) (compile-text field value params)
        (= :matches op) (str "regexp_matches(" (field-expr field) ", " (bind! params (str (second value))) ")")
        (= :exists? op) (str (field-expr field) " IS NOT NULL")
        (= :absent? op) (str (field-expr field) " IS NULL")
        (= :has op)     (str (field-expr field) " LIKE " (bind! params (str "%|" (normalize-value (second value)) "|%")))
        :else (throw (ex-info (str "Unknown operator: " op) {:field field :value value}))))
    (set? value) (compile-membership field value params)
    :else        (compile-comparison field [:= value] params)))

(defn compile-where [where params]
  (when (seq where)
    (str/join " AND " (map #(compile-where-entry % params) where))))

(defn duration->seconds [^String s]
  (when-let [[_ n unit] (re-matches #"(\d+)\s*([smhd])" s)]
    (let [n (Long/parseLong n)]
      (case unit "s" n "m" (* n 60) "h" (* n 3600) "d" (* n 86400)))))

(defn time-ref->instant [value]
  (or (try (Instant/parse value) (catch Throwable _ nil))
      (when-let [secs (duration->seconds value)]
        (.minusSeconds (Instant/now) (long secs)))))

(defn compile-time-bound [kind value params]
  (when-let [inst (some-> value time-ref->instant)]
    (str "inst " (if (= :since kind) ">" "<") " " (bind! params (duck/inst->utc inst)))))

(defn compile-order-by [order-by group-by]
  (let [[field direction] (or order-by (if group-by [:count :desc] [:inst :desc]))
        expr (cond (= :count field) "count"
                   (vector? field)  (field-expr field)
                   :else            (kebab->snake field))]
    (str expr " " (str/upper-case (name direction)))))

(defn compile-log-sql
  "Compile a validated filter-map to `{:sql :params}` against synthigy_logs."
  [filter-map]
  (let [params (new-params)
        {:keys [where since until limit order-by group-by count? bucket-ms] :or {limit 100}} filter-map
        b       (when bucket-ms (long bucket-ms))
        clauses (remove str/blank? [(compile-where where params)
                                    (compile-time-bound :since since params)
                                    (compile-time-bound :until until params)])
        select  (cond b        (str "(epoch_ms(inst) // " b ") * " b " AS bucket, count(*) AS count")
                      count?   "count(*) AS count"
                      group-by (str (kebab->snake group-by) ", count(*) AS count")
                      :else    "*")]
    {:sql (str "SELECT " select " FROM synthigy_logs"
               (when (seq clauses) (str " WHERE " (str/join " AND " clauses)))
               (cond b " GROUP BY bucket" group-by (str " GROUP BY " (kebab->snake group-by)))
               (cond b " ORDER BY bucket" count? nil :else (str " ORDER BY " (compile-order-by order-by group-by)))
               (when-not (or count? b) (str " LIMIT " limit)))
     :params @params}))

;;; ============================================================================
;;; Query execution
;;; ============================================================================

(defn read-log-row [^ResultSet rs]
  {:v           1
   :inst        (duck/utc->iso (duck/read-utc rs "inst"))
   :level       (.getString rs "level")
   :ns          (.getString rs "ns")
   :id          (.getString rs "id")
   :msg         (.getString rs "msg")
   :request_id  (.getString rs "request_id")
   :user_xid    (.getString rs "user_xid")
   :tenant      (.getString rs "tenant")
   :host        (.getString rs "host")
   :topics      (delimited->topics (.getString rs "topics"))
   :data        (try (json/<-json (.getString rs "data")) (catch Throwable _ nil))
   :ctx         (try (json/<-json (.getString rs "ctx")) (catch Throwable _ nil))
   :error_class (.getString rs "error_class")
   :error_msg   (.getString rs "error_msg")
   :error_trace (.getString rs "error_trace")})

(defn run-query [^Connection conn {:keys [sql params]} read-row]
  (with-open [ps (.prepareStatement conn ^String sql)]
    (duck/bind-params! ps params)
    (with-open [rs (.executeQuery ps)]
      (loop [acc (transient [])]
        (if (.next rs) (recur (conj! acc (read-row rs))) (persistent! acc))))))

;;; ============================================================================
;;; Traffic
;;; ============================================================================

(def traffic-columns
  (into ["node" "minute" "ok" "err"] (into hist-cols ["ops" "sse" "subscriptions" "deltas_dropped"])))

(def insert-traffic-sql
  (str "INSERT INTO traffic_minutes (" (str/join ", " traffic-columns) ") VALUES ("
       (str/join "," (repeat (count traffic-columns) "?")) ")"))

(defn traffic-row-values [row]
  (into [(:node row) (long (:minute row)) (long (:ok row 0)) (long (:err row 0))]
        (concat (map (fn [b] (long (get-in row [:hist b] 0))) traffic/hist-bounds)
                [(long (get-in row [:hist :inf] 0))]
                [(long (:ops row 0)) (long (:sse row 0))
                 (long (:subscriptions row 0)) (long (:deltas-dropped row 0))])))

(defn insert-traffic-batch! [^Connection conn rows]
  (when (seq rows)
    (with-open [ps (.prepareStatement conn insert-traffic-sql)]
      (doseq [r rows]
        (doseq [[i v] (map-indexed vector (traffic-row-values r))]
          (if (string? v) (.setString ps (int (inc i)) v) (.setLong ps (int (inc i)) (long v))))
        (.addBatch ps))
      (.executeBatch ps))))

(defn read-traffic-row [^ResultSet rs]
  {:node           (.getString rs "node")
   :minute         (.getLong rs "minute")
   :ok             (.getLong rs "ok")
   :err            (.getLong rs "err")
   :hist           (into {} (map (fn [b] [b (.getLong rs ^String (hist-col-name b))]))
                         (conj (vec traffic/hist-bounds) :inf))
   :ops            (.getLong rs "ops")
   :sse            (.getLong rs "sse")
   :subscriptions  (.getLong rs "subscriptions")
   :deltas-dropped (.getLong rs "deltas_dropped")})

(def traffic-retention-minutes (* 90 24 60))

(defonce last-traffic-prune (atom 0))

(defn maybe-prune-traffic! [conn]
  (let [now-m (quot (System/currentTimeMillis) 60000)]
    (when (> (- now-m @last-traffic-prune) 1440)
      (reset! last-traffic-prune now-m)
      (try
        (duck/with-duplicate conn
          #(duck/exec! % (str "DELETE FROM traffic_minutes WHERE minute < " (- now-m traffic-retention-minutes))))
        (catch Throwable _ nil)))))

;;; ============================================================================
;;; Store
;;; ============================================================================

(defrecord DuckDBLogStore [^Transport tp config]
  store/LogStore
  (write-signal! [_ signal]
    (when (and tp signal) (tp signal)))

  (recent [this {:keys [limit ns-pattern level since]}]
    (store/search this (cond-> {:order-by [:inst :desc] :limit (or limit 100)}
                         ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                         level      (update :where (fnil assoc {}) :level level)
                         since      (assoc :since since))))

  (search [_ {:keys [count? group-by bucket-ms] :as opts}]
    (let [compiled (compile-log-sql opts)]
      (duck/with-duplicate (transport-connection tp)
        (fn [conn]
          (cond
            bucket-ms (run-query conn compiled (fn [^ResultSet rs] [(.getLong rs "bucket") (.getLong rs "count")]))
            count?    (:count (first (run-query conn compiled (fn [^ResultSet rs] {:count (.getLong rs "count")}))))
            group-by  (let [col (kebab->snake group-by)]
                        (run-query conn compiled (fn [^ResultSet rs] {group-by (.getString rs ^String col)
                                                                      :count   (.getLong rs "count")})))
            :else     (run-query conn compiled read-log-row))))))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [rows (store/search this (cond-> {:order-by [:inst :asc] :limit (or limit 100)}
                                    cursor     (assoc :since cursor)
                                    ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                                    level      (update :where (fnil assoc {}) :level level)))]
      {:rows rows :next-cursor (or (some-> (last rows) :inst) cursor)}))

  (clear! [_]
    (duck/with-duplicate (transport-connection tp) #(duck/exec! % "DELETE FROM synthigy_logs"))
    nil)

  (health [_]
    (let [s (transport-stats tp)]
      (merge s {:up?     (boolean (:running? s))
                :backend :duckdb
                :path    (:logs-path config)
                :rows    (try (duck/with-duplicate (transport-connection tp) #(duck/row-count % "synthigy_logs"))
                              (catch Throwable _ nil))})))

  (snapshot [_] nil)

  traffic/TrafficStore
  (flush-deltas! [_ rows]
    (when (seq rows)
      (duck/with-duplicate (transport-connection tp) #(insert-traffic-batch! % rows)))
    (maybe-prune-traffic! (transport-connection tp))
    nil)

  (query-window [_ {:keys [from to]}]
    (try
      (duck/with-duplicate (transport-connection tp)
        (fn [^Connection conn]
          (with-open [ps (.prepareStatement conn "SELECT * FROM traffic_minutes WHERE minute >= ? AND minute <= ?")]
            (.setLong ps 1 (long from))
            (.setLong ps 2 (long to))
            (with-open [rs (.executeQuery ps)]
              (loop [acc (transient [])]
                (if (.next rs) (recur (conj! acc (read-traffic-row rs))) (persistent! acc)))))))
      (catch Throwable _ [])))

  (traffic-health [_]
    {:up? true :backend :duckdb
     :rows (try (duck/with-duplicate (transport-connection tp) #(duck/row-count % "traffic_minutes"))
                (catch Throwable _ nil))}))

(defn open-store
  "Open the logs database, apply the schema and start the log writer."
  [config]
  (let [cfg  (merge default-config (select-keys config (keys default-config)))
        conn (duck/open-connection (:logs-path cfg) (duck/memory-limit))]
    (apply duck/exec! conn schema-ddl)
    (->DuckDBLogStore (make-transport conn cfg) cfg)))

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
                 :data {:action :written :subject :log-store :backend :duckdb :signals (count signals)}}
                (str "Drained " (count signals) " buffered signal(s) into DuckDB")))))

(defn setup []
  (let [cfg (env-config)]
    (with-open [conn (duck/open-connection (:logs-path cfg) (duck/memory-limit))]
      (apply duck/exec! conn schema-ddl))
    (log/info {:id ::setup-complete
               :data {:action :setup-complete :subject :log-store :backend :duckdb :path (:logs-path cfg)}}
              "DuckDB log store schema applied")))

(defn start []
  (let [cfg      (env-config)
        rec      (open-store cfg)
        outgoing @#'store/*log-store*]
    (patch/level! :synthigy.log/duckdb)
    ;; swap before draining — draining first leaves a race the width of the drain
    (alter-var-root #'store/*log-store* (constantly rec))
    (drain-snapshot! outgoing rec)
    (alter-var-root #'traffic/*traffic-store* (constantly rec))
    (reset! state {:record rec})
    (log/info {:id ::started
               :data {:action :started :subject :log-store :backend :duckdb :path (:logs-path cfg)}}
              "DuckDB log store started")))

(defn stop []
  (alter-var-root #'traffic/*traffic-store* (constantly (traffic/create-in-memory-store)))
  (when-let [{:keys [record]} @state]
    (close-store! record))
  (alter-var-root #'store/*log-store* (constantly (ring/create-ring)))
  (reset! state nil)
  (log/info {:id ::stopped :data {:action :stopped :subject :log-store :backend :duckdb}}
            "DuckDB log store stopped"))

(defn cleanup []
  (with-open [conn (duck/open-connection (:logs-path (env-config)) (duck/memory-limit))]
    (duck/exec! conn "DROP TABLE IF EXISTS synthigy_logs" "DROP TABLE IF EXISTS traffic_minutes")))

(patch/current-version :synthigy.log/duckdb "1.0.0")
