;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.log.store.sqlite.logs
  (:require
   [clojure.string :as str]
   [synthigy.json :as json]
   [synthigy.log.store.sqlite.conn :as conn])
  (:import
   [java.sql Connection PreparedStatement ResultSet]
   [java.time Instant]
   [java.time.temporal ChronoUnit]
   [java.util ArrayList]
   [java.util.concurrent ArrayBlockingQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

(def schema-ddl
  ["CREATE TABLE IF NOT EXISTS synthigy_logs (
      inst        INTEGER,
      level       TEXT,
      ns          TEXT,
      id          TEXT,
      msg         TEXT,
      request_id  TEXT,
      user_xid    TEXT,
      tenant      TEXT,
      host        TEXT,
      topics      TEXT,
      data        TEXT,
      ctx         TEXT,
      error_class TEXT,
      error_msg   TEXT,
      error_trace TEXT)"
   "CREATE INDEX IF NOT EXISTS synthigy_logs_inst ON synthigy_logs(inst)"
   "CREATE INDEX IF NOT EXISTS synthigy_logs_level_inst ON synthigy_logs(level, inst)"
   "CREATE INDEX IF NOT EXISTS synthigy_logs_id_inst ON synthigy_logs(id, inst)"
   "CREATE INDEX IF NOT EXISTS synthigy_logs_request ON synthigy_logs(request_id) WHERE request_id IS NOT NULL"])

(defn apply-schema! [c]
  (apply conn/exec! c schema-ddl))

(defn drop-schema! [c]
  (conn/exec! c "DROP TABLE IF EXISTS synthigy_logs"))

;;; ============================================================================
;;; Signal → row
;;; ============================================================================

(def promoted-ctx-keys [:request-id :user-xid :tenant])

(defn topics->delimited [topics]
  (let [names (->> topics (map (fn [t] (if (keyword? t) (subs (str t) 1) (str t)))) sort)]
    (if (seq names) (str "|" (str/join "|" names) "|") "")))

(defn delimited->topics [s]
  (if (str/blank? s)
    []
    (->> (str/split s #"\|") (remove str/blank?) vec)))

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

(defn signal->row
  "Project a Telemere signal onto the synthigy_logs columns, in column order."
  [{:keys [inst level ns id msg_ data ctx error] :as signal}]
  (let [[err-class err-msg err-trace] (throwable->parts error)
        ctx-map  (or ctx {})
        host-raw (:host signal)]
    [(conn/->micros inst)
     (some-> level name)
     (when ns (str ns))
     (id->str id)
     (when msg_ (try (force msg_) (catch Throwable _ nil)))
     (or (:request-id signal) (get ctx-map :request-id))
     (or (:user-xid signal) (get ctx-map :user-xid))
     (or (:tenant signal) (get ctx-map :tenant))
     (cond (string? host-raw) host-raw (map? host-raw) (:name host-raw))
     (topics->delimited (:topics signal))
     (json/->json (or data {}))
     (json/->json (apply dissoc ctx-map promoted-ctx-keys))
     err-class
     err-msg
     err-trace]))

(def insert-sql
  "INSERT INTO synthigy_logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

(defn insert-batch! [^Connection c signals]
  (when (seq signals)
    (conn/in-tx! c
      (fn []
        (with-open [ps (.prepareStatement c insert-sql)]
          (doseq [s signals]
            (conn/bind-params! ps (signal->row s))
            (.addBatch ps))
          (.executeBatch ps))))))

;;; ============================================================================
;;; Retention — days OR bytes, whichever bites first
;;; ============================================================================

(def prune-check-ms 60000)

(defn prune-by-age! [^Connection c retention-days]
  (let [cutoff (conn/instant->micros (.minus (Instant/now) (long retention-days) ChronoUnit/DAYS))]
    (with-open [ps (.prepareStatement c "DELETE FROM synthigy_logs WHERE inst < ?")]
      (.setLong ps 1 cutoff)
      (.executeUpdate ps))))

(defn prune-by-size!
  "Delete the oldest rows in chunks until live pages fit under `max-mb`."
  [^Connection c max-mb]
  (let [cap (* (long max-mb) 1024 1024)]
    (loop [n 0]
      (let [used (conn/used-bytes c)]
        (when (and (< n 100) (> used cap))
          (let [rows    (conn/row-count c "synthigy_logs")
                chunk   (+ 100 (long (Math/ceil (/ (* rows (- used cap)) (double used)))))
                deleted (with-open [st (.createStatement c)]
                          (.executeUpdate st (str "DELETE FROM synthigy_logs WHERE rowid IN "
                                                  "(SELECT rowid FROM synthigy_logs ORDER BY inst LIMIT "
                                                  chunk ")")))]
            (when (pos? deleted) (recur (inc n)))))))))

(defn prune! [c {:keys [log-retention-days log-max-mb]}]
  (prune-by-age! c log-retention-days)
  (prune-by-size! c log-max-mb)
  (conn/incremental-vacuum! c))

;;; ============================================================================
;;; Writer — bounded queue drained by one thread on its own connection
;;; ============================================================================

(defn backoff-ms [attempt {:keys [retry-min-ms retry-max-ms]}]
  (min (long retry-max-ms) (* (long retry-min-ms) (long (Math/pow 2 attempt)))))

(defn write-with-retry! [c config signals]
  (loop [attempt 0]
    (let [err (try (insert-batch! c signals) nil (catch Throwable t t))]
      (cond
        (nil? err) true
        (>= attempt (long (:retry-attempts config)))
        (do (binding [*out* *err*]
              (println (str "synthigy.log.store.sqlite: log batch dropped after " (inc attempt)
                            " attempts: " (.getName (class err)) ": " (.getMessage ^Throwable err))))
            false)
        :else (do (Thread/sleep (long (backoff-ms attempt config)))
                  (recur (inc attempt)))))))

(defn drain-batch! [^ArrayBlockingQueue queue batch-rows batch-ms]
  (let [batch (ArrayList.)]
    (when-let [first-sig (.poll queue (long batch-ms) TimeUnit/MILLISECONDS)]
      (.add batch first-sig)
      (.drainTo queue batch (int (dec batch-rows))))
    batch))

(defn writer-loop!
  [{:keys [^ArrayBlockingQueue queue ^Connection conn ^AtomicLong written ^AtomicLong dropped
           ^AtomicLong batches ^AtomicBoolean running? config]}]
  (let [{:keys [batch-rows batch-ms]} config
        last-prune (atom 0)
        flush!     (fn [^ArrayList batch]
                     (when (pos? (.size batch))
                       (.incrementAndGet batches)
                       (if (write-with-retry! conn config batch)
                         (.addAndGet written (.size batch))
                         (.addAndGet dropped (.size batch)))))]
    (while (.get running?)
      (try
        (flush! (drain-batch! queue batch-rows batch-ms))
        (let [now (System/currentTimeMillis)]
          (when (> (- now @last-prune) prune-check-ms)
            (reset! last-prune now)
            (try (prune! conn config) (catch Throwable _ nil))))
        (catch InterruptedException _ (.set running? false))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.log.store.sqlite: log writer loop threw: "
                          (.getName (class t)) ": " (.getMessage t)))))))
    (let [tail (ArrayList.)]
      (.drainTo queue tail)
      (flush! tail))))

(defprotocol TransportOps
  (close-transport! [this])
  (transport-stats [this]))

(deftype Transport [^ArrayBlockingQueue queue ^Connection conn ^AtomicLong written
                    ^AtomicLong dropped ^AtomicLong batches ^AtomicBoolean running?
                    ^Thread writer config]
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
     :running? (.get running?)}))

(defn make-transport
  "Start the log writer on its own connection to the logs database."
  ^Transport [config]
  (let [queue    (ArrayBlockingQueue. (int (:buffer-size config)))
        c        (conn/open! (:logs-url config))
        running? (AtomicBoolean. true)
        written  (AtomicLong. 0)
        dropped  (AtomicLong. 0)
        batches  (AtomicLong. 0)
        state    {:queue queue :conn c :written written :dropped dropped
                  :batches batches :running? running? :config config}
        thread   (doto (Thread. ^Runnable (fn [] (writer-loop! state))
                                "synthigy-log-writer")
                   (.setDaemon true)
                   .start)]
    (->Transport queue c written dropped batches running? thread config)))

;;; ============================================================================
;;; Query compilation — filter-map (synthigy.log.query) → SQL + params
;;; ============================================================================

(defn kebab->snake [k] (str/replace (name k) "-" "_"))

(defn json-path [path] (str "'$." (str/join "." (map kebab->snake path)) "'"))

(defn field-expr
  "Text-valued SQL for a field; JSON paths render booleans as 'true'/'false' like the other stores."
  [field]
  (cond
    (keyword? field) (kebab->snake field)
    (vector? field)
    (let [[root & path] field
          col (kebab->snake root)
          p   (json-path path)]
      (str "(CASE json_type(" col ", " p ")"
           " WHEN 'true' THEN 'true' WHEN 'false' THEN 'false'"
           " ELSE CAST(json_extract(" col ", " p ") AS TEXT) END)"))
    :else (throw (ex-info (str "Unsupported field reference: " (pr-str field)) {:field field}))))

(defn numeric-expr [field]
  (let [[root & path] field
        col (kebab->snake root)
        p   (json-path path)]
    (str "(CASE WHEN json_type(" col ", " p ") IN ('integer','real')"
         " THEN json_extract(" col ", " p ") END)")))

(defn normalize-value [v]
  (if (keyword? v) (subs (str v) 1) v))

(defn bind! [params v]
  (swap! params conj v)
  "?")

(defn operand [field v]
  (if (= :inst field) (conn/->micros v) (normalize-value v)))

(defn compile-comparison [field [op value] params]
  (let [sql-op (case op := "=" :!= "!=" :> ">" :< "<" :>= ">=" :<= "<=")
        expr   (if (and (vector? field) (number? value)) (numeric-expr field) (field-expr field))]
    (str expr " " sql-op " " (bind! params (operand field value)))))

(defn compile-membership [field values params]
  (when (empty? values)
    (throw (ex-info ":in / set value must be non-empty" {:field field})))
  (let [expr (if (and (vector? field) (every? number? values)) (numeric-expr field) (field-expr field))
        phs  (mapv #(bind! params (operand field %)) values)]
    (str expr " IN (" (str/join ", " phs) ")")))

(defn compile-text [field [op s] params]
  (let [col (field-expr field)]
    (case op
      :contains    (str "instr(" col ", " (bind! params s) ") > 0")
      :icontains   (str "instr(lower(" col "), lower(" (bind! params s) ")) > 0")
      :starts-with (str "substr(" col ", 1, length(" (bind! params s) ")) = " (bind! params s))
      :ends-with   (str "(" (bind! params s) " = '' OR substr(" col ", -length("
                        (bind! params s) ")) = " (bind! params s) ")"))))

(defn compile-where-entry [[field value] params]
  (cond
    (and (vector? value) (keyword? (first value)))
    (let [op (first value)]
      (cond
        (#{:= :!= :> :< :>= :<=} op) (compile-comparison field value params)
        (= :in op)                   (compile-membership field (second value) params)
        (#{:contains :icontains :starts-with :ends-with} op) (compile-text field value params)
        (= :matches op)  (str "regexp_matches(" (field-expr field) ", " (bind! params (str (second value))) ")")
        (= :exists? op)  (str (field-expr field) " IS NOT NULL")
        (= :absent? op)  (str (field-expr field) " IS NULL")
        (= :has op)      (str "instr(" (field-expr field) ", "
                              (bind! params (str "|" (normalize-value (second value)) "|")) ") > 0")
        :else (throw (ex-info (str "Unknown operator: " op) {:field field :value value}))))
    (set? value) (compile-membership field value params)
    :else        (compile-comparison field [:= value] params)))

(defn duration->seconds [^String s]
  (when-let [[_ n unit] (re-matches #"(\d+)\s*([smhd])" s)]
    (let [n (Long/parseLong n)]
      (case unit "s" n "m" (* n 60) "h" (* n 3600) "d" (* n 86400)))))

(defn time-ref->micros [value]
  (conn/->micros
   (or (conn/->instant value)
       (when-let [secs (and (string? value) (duration->seconds value))]
         (.minusSeconds (Instant/now) (long secs))))))

(defn compile-time-bound [kind value params]
  (when-let [m (and value (time-ref->micros value))]
    (str "inst " (if (= :since kind) ">" "<") " " (bind! params m))))

(defn compile-order-by [order-by group-by]
  (let [[field direction] (or order-by (if group-by [:count :desc] [:inst :desc]))
        expr (cond (= :count field) "count"
                   (vector? field)  (field-expr field)
                   :else            (kebab->snake field))]
    (str expr " " (str/upper-case (name direction)))))

(defn compile-sql
  "Compile a validated filter-map to `{:sql :params}` against synthigy_logs."
  [filter-map]
  (let [params  (atom [])
        {:keys [where since until limit order-by group-by count? bucket-ms] :or {limit 100}} filter-map
        b       (when bucket-ms (long bucket-ms))
        clauses (as-> (mapv #(compile-where-entry % params) where) cs
                  (conj cs (compile-time-bound :since since params))
                  (conj cs (compile-time-bound :until until params))
                  (remove str/blank? cs))
        select  (cond b        (str "(inst / " (* b 1000) ") * " b " AS bucket, count(*) AS count")
                      count?   "count(*) AS count"
                      group-by (str (kebab->snake group-by) ", count(*) AS count")
                      :else    "*")
        sql     (str "SELECT " select " FROM synthigy_logs"
                     (when (seq clauses) (str " WHERE " (str/join " AND " clauses)))
                     (cond b        " GROUP BY bucket"
                           group-by (str " GROUP BY " (kebab->snake group-by)))
                     (cond b      " ORDER BY bucket"
                           count? nil
                           :else  (str " ORDER BY " (compile-order-by order-by group-by)))
                     (when-not (or count? b) (str " LIMIT " (long limit))))]
    {:sql sql :params @params}))

;;; ============================================================================
;;; Query execution
;;; ============================================================================

(defn read-row [^ResultSet rs]
  {:v           1
   :inst        (conn/micros->iso (conn/read-micros rs "inst"))
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

(defn search [c {:keys [count? group-by bucket-ms] :as opts}]
  (let [{:keys [sql params]} (compile-sql opts)]
    (cond
      bucket-ms (conn/query-rows c sql params (fn [^ResultSet rs] [(.getLong rs "bucket") (.getLong rs "count")]))
      count?    (first (conn/query-rows c sql params (fn [^ResultSet rs] (.getLong rs "count"))))
      group-by  (let [col (kebab->snake group-by)]
                  (conn/query-rows c sql params
                                   (fn [^ResultSet rs] {group-by (.getString rs ^String col)
                                                        :count   (.getLong rs "count")})))
      :else     (conn/query-rows c sql params read-row))))
