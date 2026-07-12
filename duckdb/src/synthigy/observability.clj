(ns synthigy.observability
  "DuckDB implementation of Synthigy's observability substrate — durable
  append-only storage for diagnostic logs (`synthigy.log.store/LogStore`)
  AND state-change audit events (`synthigy.audit/AuditProvider`), backed
  by a single embedded DuckDB database.

  This is the DuckDB-flavored `synthigy.observability` namespace. Whichever
  observability alias is on the classpath provides its own
  `synthigy/observability.clj` — same shadowed-namespace pattern Synthigy
  uses for `synthigy.server` (one ns name, one file per backend, classpath
  picks one).

  ## Operator model

      clj -M:postgres:duckdb:httpkit:dev
      (lifecycle/start! :synthigy/observability)

  Starting the module:
  - opens a DuckDB connection (in-memory or file-backed)
  - applies the schema (logs + audit tables) idempotently
  - starts the log-writer thread (batched async INSERT)
  - installs the Telemere bridge handler that streams signals into the log
  - binds `synthigy.log.store/*log-store*` AND `synthigy.audit/*audit-provider*`
    to a single `DuckDBObservability` record that implements both protocols
  - re-compiles the audit policy from the deployed model + installs a
    model-watch so policy stays current as the model evolves

  Stopping reverses all of it. Setup creates the parent directory if file
  mode is selected and applies the schema (tracked once by patcho).
  Cleanup drops the tables.

  ## Storage layout

  Three tables live in the chosen `.duckdb` file (or `:memory:` DB):

      synthigy_logs     (LogStore — diagnostic events)
      audit_entity      (AuditProvider — attribute-grain state changes)
      audit_relation    (AuditProvider — relation edge changes)

  File mode allows cross-table joins (`request_id` correlates a log row
  to the audit events that ran inside the same request). `:memory:` mode
  loses this because each connection gets its own in-process DB —
  acceptable for dev, switch to file for cockpit correlation.

  ## Config

      SYNTHIGY_OBSERVABILITY_PATH         File path (default \":memory:\")
      SYNTHIGY_OBSERVABILITY_BUFFER_SIZE  Log queue cap (default 8192)
      SYNTHIGY_OBSERVABILITY_BATCH_ROWS   Max INSERT batch (default 500)
      SYNTHIGY_OBSERVABILITY_BATCH_MS     Max flush delay ms (default 250)

  ## Why one record for two protocols

  Logs and audit have different SCHEMAS but the SAME backend, the SAME
  connection, the SAME lifecycle. Implementing both on one record keeps
  storage/teardown coordinated — one place that owns the DuckDB
  connection, one writer thread, one set of binding decisions."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.audit :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.log.store :as store])
  (:import
   [java.io File]
   [org.duckdb DuckDBConnection]
   [java.sql Connection DriverManager PreparedStatement ResultSet Timestamp]
   [java.time Instant]
   [java.util ArrayList]
   [java.util.concurrent ArrayBlockingQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

;; Force-load the DuckDB JDBC driver. Modern JDBC drivers register via SPI;
;; this Class.forName is a belt-and-suspenders guard for shaded uberjars
;; that strip META-INF/services entries.
(try (Class/forName "org.duckdb.DuckDBDriver") (catch Throwable _))

;;; ============================================================================
;;; Defaults + env
;;; ============================================================================

(def ^:private default-config
  {:path             ":memory:"
   :buffer-size      8192
   :batch-rows       500
   :batch-ms         250
   :retry-attempts   3
   :retry-min-ms     100
   :retry-max-ms     5000
   :close-timeout-ms 2000})

(defn- parse-int-or [s fallback]
  (or (try (some-> s str/trim Integer/parseInt) (catch Throwable _ nil))
      fallback))

(defn- env-config
  "Read SYNTHIGY_OBSERVABILITY_* overrides on top of defaults."
  []
  (let [base default-config]
    {:path             (or (env :synthigy-observability-path) (:path base))
     :buffer-size      (parse-int-or (env :synthigy-observability-buffer-size) (:buffer-size base))
     :batch-rows       (parse-int-or (env :synthigy-observability-batch-rows)  (:batch-rows base))
     :batch-ms         (parse-int-or (env :synthigy-observability-batch-ms)    (:batch-ms base))
     :retry-attempts   (:retry-attempts   base)
     :retry-min-ms     (:retry-min-ms     base)
     :retry-max-ms     (:retry-max-ms     base)
     :close-timeout-ms (:close-timeout-ms base)}))

;;; ============================================================================
;;; Time helpers
;;; ============================================================================

(defn- inst->sql-ts ^Timestamp [inst]
  (cond
    (nil? inst)                       nil
    (instance? Timestamp inst)        inst
    (instance? Instant inst)          (Timestamp/from inst)
    (instance? java.util.Date inst)   (Timestamp. (.getTime ^java.util.Date inst))
    (string? inst)                    (try (Timestamp/from (Instant/parse inst))
                                           (catch Throwable _ nil))
    :else                             nil))

(defn- ts->iso [^Timestamp ts]
  (when ts (str (.toInstant ts))))

;;; ============================================================================
;;; Connection + schema (logs + audit_entity + audit_relation)
;;; ============================================================================

(defn- jdbc-url [path]
  (if (or (nil? path) (= ":memory:" path) (str/blank? path))
    "jdbc:duckdb:"
    (str "jdbc:duckdb:" path)))

(def ^:private schema-ddl
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
   "CREATE TABLE IF NOT EXISTS audit_entity (
      ts            TIMESTAMP,
      tenant_xid    VARCHAR,
      record_xid    VARCHAR,
      entity_xid    VARCHAR,
      attribute_xid VARCHAR,
      value         JSON,
      op            VARCHAR,
      actor_xid     VARCHAR,
      request_id    VARCHAR,
      scope_xid     VARCHAR,
      txid          VARCHAR,
      seq           BIGINT
    )"
   "CREATE TABLE IF NOT EXISTS audit_relation (
      ts            TIMESTAMP,
      tenant_xid    VARCHAR,
      relation_xid  VARCHAR,
      from_xid      VARCHAR,
      to_xid        VARCHAR,
      op            VARCHAR,
      actor_xid     VARCHAR,
      request_id    VARCHAR,
      scope_xid     VARCHAR,
      txid          VARCHAR,
      seq           BIGINT
    )"])

(defn- open-connection ^Connection [path]
  (DriverManager/getConnection (jdbc-url path)))

(def ^:private migrations-ddl
  "Idempotent column-adds for schema evolution. `CREATE TABLE IF NOT EXISTS`
   is a no-op on a pre-existing table, so new columns (wire-v2 `topics`) must
   be ALTERed in for deployments whose `synthigy_logs` predates the column."
  ["ALTER TABLE synthigy_logs ADD COLUMN IF NOT EXISTS topics VARCHAR"
   "ALTER TABLE audit_entity   ADD COLUMN IF NOT EXISTS seq BIGINT"
   "ALTER TABLE audit_relation ADD COLUMN IF NOT EXISTS seq BIGINT"])

(defn- apply-schema! [^Connection conn]
  (with-open [stmt (.createStatement conn)]
    (doseq [ddl schema-ddl]
      (.execute stmt ^String ddl))
    (doseq [ddl migrations-ddl]
      (try (.execute stmt ^String ddl) (catch Throwable _)))))

(defn- drop-schema! [^Connection conn]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt "DROP TABLE IF EXISTS synthigy_logs")
    (.execute stmt "DROP TABLE IF EXISTS audit_entity")
    (.execute stmt "DROP TABLE IF EXISTS audit_relation")))

;;; ============================================================================
;;; Log signal → row coercion
;;; ============================================================================

(def ^:private promoted-ctx-keys
  [:request-id :user-xid :tenant])

(defn- topics->delimited
  "Store a topic set as a pipe-bounded sorted string — `|system|dataset|` —
   so membership is a portable bounded LIKE (`%|system|%`) with no array-type
   binding. Empty → empty string."
  [topics]
  (let [names (->> topics (map (fn [t] (if (keyword? t) (name t) (str t)))) sort)]
    (if (seq names) (str "|" (str/join "|" names) "|") "")))

(defn- delimited->topics
  "Inverse of `topics->delimited` — back to a vector of name strings for the
   wire row."
  [s]
  (if (str/blank? s)
    []
    (->> (str/split s #"\|") (remove str/blank?) vec)))

(defn- throwable->parts
  [x]
  (cond
    (instance? Throwable x)
    (let [^Throwable t x
          sw (java.io.StringWriter.)
          pw (java.io.PrintWriter. sw)]
      (.printStackTrace t pw) (.flush pw)
      [(.getName (class t)) (.getMessage t) (.toString sw)])

    (some? x) [nil nil (str x)]
    :else     [nil nil nil]))

(defn- id->str [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (some? id)              (str id)))

(defn signal->log-row
  "Project a Telemere signal map to the synthigy_logs column shape."
  [{:keys [inst level ns id msg_ data ctx error] :as signal}]
  (let [[err-class err-msg err-trace] (throwable->parts error)
        ctx-map      (or ctx {})
        residual-ctx (apply dissoc ctx-map promoted-ctx-keys)
        host-raw     (:host signal)
        host         (cond
                       (string? host-raw) host-raw
                       (map? host-raw)    (:name host-raw)
                       :else              nil)]
    {:inst        (inst->sql-ts inst)
     :level       (some-> level name)
     :ns          (when ns (str ns))
     :id          (id->str id)
     :msg         (when msg_ (try (force msg_) (catch Throwable _ nil)))
     :request_id  (or (:request-id signal) (get ctx-map :request-id))
     :user_xid    (or (:user-xid signal)   (get ctx-map :user-xid))
     :tenant      (or (:tenant signal)     (get ctx-map :tenant))
     :host        host
     :topics      (topics->delimited (:topics signal))
     :data        (json/->json (or data {}))
     :ctx         (json/->json residual-ctx)
     :error_class err-class
     :error_msg   err-msg
     :error_trace err-trace}))

;;; ============================================================================
;;; Audit envelope → row coercion
;;; ============================================================================

(defn- attribute-pairs [attrs]
  (->> (or attrs {})
       (mapv (fn [[k v]] [(name k) (json/->json v)]))))

(defn- diff-pairs [before after]
  (->> (or after {})
       (filter (fn [[k v]] (not= v (get before k))))
       (mapv (fn [[k v]] [(name k) (json/->json v)]))))

(defn- str-or-nil [v] (when (some? v) (str v)))

(defn- envelope->entity-rows
  "Flatten audit envelopes into rows for audit_entity. One row per changed
   attribute on insert/update; one sentinel row on delete."
  [envelopes]
  (for [env envelopes
        :let [data       (-> env :delta :data)
              record-xid (str-or-nil (:record-xid data))]
        :when record-xid
        :let [op         (-> env :delta :type name)
              ts         (inst->sql-ts (:ts data))
              tenant     (str-or-nil (:tenant data))
              entity-xid (str-or-nil (:entity-xid data))
              actor      (str-or-nil (:actor data))
              request    (str-or-nil (:request data))
              scope      (str-or-nil (:scope data))
              txid       (or (:txid data) 0)
              sq         (:seq env)]
        row (if (= op "delete")
              [{:ts ts :tenant_xid tenant :record_xid record-xid
                :entity_xid entity-xid :attribute_xid "__delete__"
                :value "null" :op "delete" :actor_xid actor
                :request_id request :scope_xid scope :txid txid :seq sq}]
              (for [[attr-xid value] (case op
                                       "insert" (attribute-pairs (:after data))
                                       "update" (diff-pairs (:before data) (:after data))
                                       nil)]
                {:ts ts :tenant_xid tenant :record_xid record-xid
                 :entity_xid entity-xid :attribute_xid attr-xid
                 :value value :op op :actor_xid actor
                 :request_id request :scope_xid scope :txid txid :seq sq}))]
    row))

(defn- envelope->relation-rows
  [envelopes]
  (for [env envelopes
        :let [data     (-> env :delta :data)
              from-xid (str-or-nil (:from-xid data))
              to-xid   (str-or-nil (:to-xid data))]
        :when (and from-xid to-xid)]
    {:ts            (inst->sql-ts (:ts data))
     :tenant_xid    (str-or-nil (:tenant data))
     :relation_xid  (str-or-nil (:element env))
     :from_xid      from-xid
     :to_xid        to-xid
     :op            (-> env :delta :type name)
     :actor_xid     (str-or-nil (:actor data))
     :request_id    (str-or-nil (:request data))
     :scope_xid     (str-or-nil (:scope data))
     :txid          (or (:txid data) 0)
     :seq           (:seq env)}))

;;; ============================================================================
;;; INSERT — batched prepared statements
;;; ============================================================================

(def ^:private insert-log-sql
  "INSERT INTO synthigy_logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

(def ^:private insert-entity-sql
  "INSERT INTO audit_entity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")

(def ^:private insert-relation-sql
  "INSERT INTO audit_relation VALUES (?,?,?,?,?,?,?,?,?,?,?)")

(defn- bind-log-row! [^PreparedStatement ps row]
  (.setObject ps 1 (:inst row))
  (.setString ps 2 (:level row))
  (.setString ps 3 (:ns row))
  (.setString ps 4 (:id row))
  (.setString ps 5 (:msg row))
  (.setString ps 6 (:request_id row))
  (.setString ps 7 (:user_xid row))
  (.setString ps 8 (:tenant row))
  (.setString ps 9 (:host row))
  (.setString ps 10 (:topics row))
  (.setString ps 11 (:data row))
  (.setString ps 12 (:ctx row))
  (.setString ps 13 (:error_class row))
  (.setString ps 14 (:error_msg row))
  (.setString ps 15 (:error_trace row)))

(defn- bind-entity-row! [^PreparedStatement ps row]
  (.setObject ps 1 (:ts row))
  (.setString ps 2 (:tenant_xid row))
  (.setString ps 3 (:record_xid row))
  (.setString ps 4 (:entity_xid row))
  (.setString ps 5 (:attribute_xid row))
  (.setString ps 6 (:value row))
  (.setString ps 7 (:op row))
  (.setString ps 8 (:actor_xid row))
  (.setString ps 9 (:request_id row))
  (.setString ps 10 (:scope_xid row))
  ;; txid is opaque — SQLite synthesizes UUIDv7 strings, PG/CRDB use BIGINT,
  ;; CH UInt64. Store as VARCHAR so any substrate's id shape fits without
  ;; loss (UUIDv7 is binary-sortable as a string, preserving chronology).
  (.setString ps 11 (when-let [v (:txid row)] (str v)))
  ;; seq — the canonical delta cursor (drainer-assigned, monotonic). Nullable
  ;; for envelopes that predate the sequencer.
  (.setObject ps 12 (:seq row)))

(defn- bind-relation-row! [^PreparedStatement ps row]
  (.setObject ps 1 (:ts row))
  (.setString ps 2 (:tenant_xid row))
  (.setString ps 3 (:relation_xid row))
  (.setString ps 4 (:from_xid row))
  (.setString ps 5 (:to_xid row))
  (.setString ps 6 (:op row))
  (.setString ps 7 (:actor_xid row))
  (.setString ps 8 (:request_id row))
  (.setString ps 9 (:scope_xid row))
  (.setString ps 10 (when-let [v (:txid row)] (str v)))
  (.setObject ps 11 (:seq row)))

(defn- insert-log-batch! [^Connection conn signals]
  (when (seq signals)
    (with-open [ps (.prepareStatement conn insert-log-sql)]
      (doseq [s signals]
        (bind-log-row! ps (signal->log-row s))
        (.addBatch ps))
      (.executeBatch ps))))

(defn- insert-entity-batch! [^Connection conn rows]
  (when (seq rows)
    (with-open [ps (.prepareStatement conn insert-entity-sql)]
      (doseq [r rows] (bind-entity-row! ps r) (.addBatch ps))
      (.executeBatch ps))))

(defn- insert-relation-batch! [^Connection conn rows]
  (when (seq rows)
    (with-open [ps (.prepareStatement conn insert-relation-sql)]
      (doseq [r rows] (bind-relation-row! ps r) (.addBatch ps))
      (.executeBatch ps))))

;;; ============================================================================
;;; Log writer thread — async batched INSERT with retry
;;;
;;; Audit writes are SYNCHRONOUS (called by the drainer per batch, see
;;; AuditProvider methods below). Logs need async because they're called
;;; from every code path and can't block.
;;; ============================================================================

(defn- backoff-ms [attempt {:keys [retry-min-ms retry-max-ms]}]
  (min (long retry-max-ms)
       (* (long retry-min-ms) (long (Math/pow 2 attempt)))))

(defn- send-log-batch-with-retry!
  [^Connection conn config signals]
  (let [max-attempts (long (:retry-attempts config))]
    (loop [attempt 0]
      (let [outcome (try (insert-log-batch! conn signals) {:ok? true}
                         (catch Throwable t {:ok? false :err t}))]
        (cond
          (:ok? outcome) true
          (>= attempt max-attempts)
          (do (binding [*out* *err*]
                (println (str "synthigy.observability: log batch dropped after "
                              (inc attempt) " attempts: "
                              (some-> (:err outcome) .getClass .getName) ": "
                              (some-> (:err outcome) .getMessage))))
              false)
          :else
          (do (Thread/sleep (backoff-ms attempt config))
              (recur (inc attempt))))))))

(defn- drain-log-batch!
  [^ArrayBlockingQueue queue ^long batch-rows ^long batch-ms]
  (let [batch (ArrayList.)
        first-sig (.poll queue batch-ms TimeUnit/MILLISECONDS)]
    (when first-sig
      (.add batch first-sig)
      (.drainTo queue batch (dec batch-rows)))
    batch))

(defn- log-writer-loop!
  [{:keys [^ArrayBlockingQueue queue ^Connection conn ^AtomicLong written
           ^AtomicLong dropped ^AtomicLong batches ^AtomicBoolean running?
           config]}]
  (let [{:keys [batch-rows batch-ms]} config]
    (while (.get running?)
      (try
        (let [batch (drain-log-batch! queue batch-rows batch-ms)]
          (when (pos? (.size batch))
            (let [ok? (send-log-batch-with-retry! conn config batch)]
              (.incrementAndGet batches)
              (if ok?
                (.addAndGet written (.size batch))
                (.addAndGet dropped (.size batch))))))
        (catch InterruptedException _ (.set running? false))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.observability: log writer loop threw: "
                          (some-> t .getClass .getName) ": " (.getMessage t)))))))
    ;; Drain remaining on shutdown — best-effort.
    (let [tail (ArrayList.)]
      (.drainTo queue tail)
      (when (pos? (.size tail))
        (let [ok? (send-log-batch-with-retry! conn config tail)]
          (if ok?
            (.addAndGet written (.size tail))
            (.addAndGet dropped (.size tail))))))
    ;; This thread owns its connection (a duplicate); close it on exit.
    (try (.close conn) (catch Throwable _))))

;;; ============================================================================
;;; Log query compilation — filter-map → SQL + params
;;;
;;; Compiles the spec validated by synthigy.log.query/validate-filter-map.
;;; ============================================================================

(defn- kebab->snake [k]
  (-> (name k) (str/replace "-" "_")))

(defn- column-name [field] (kebab->snake field))

(defn- field-expr
  "Render field reference. Built-in column → column name. `[:data k1 k2 …]`
   → `json_extract_string(data, '$.k1.k2…kN')`.

   The DuckDB `->>` operator is avoided in WHERE predicates: when the key
   doesn't exist in any row, combining `data->>'k' = ?` with another
   predicate occasionally triggers a 'cast to numerical' error in the
   vectorized executor (whole JSON object reported as the cast target).
   `json_extract_string` returns NULL cleanly for missing keys and always
   produces VARCHAR for comparison, regardless of nesting depth."
  [field]
  (cond
    (keyword? field) (column-name field)
    (vector? field)
    (let [[root & path] field
          col (column-name root)
          json-path (str "'$." (str/join "." (map kebab->snake path)) "'")]
      (str "json_extract_string(" col ", " json-path ")"))
    :else
    (throw (ex-info (str "Unsupported field reference: " (pr-str field))
                    {:field field}))))

(defn- numeric-cast
  "Wrap a field expression in `TRY_CAST(… AS DOUBLE)` so a JSON-path value
   (extracted as VARCHAR) compares numerically against a number operand —
   matching the in-memory store's raw-value comparison — instead of throwing
   'Cannot compare VARCHAR and BIGINT' or comparing lexically. TRY_CAST
   yields NULL (not an error) for non-numeric / missing values."
  [expr]
  (str "TRY_CAST(" expr " AS DOUBLE)"))

(defn- comparand-expr
  "Field SQL for a comparison, numeric-cast when a JSON-path field is compared
   against a number. Built-in columns are left as-is."
  [field operand]
  (let [e (field-expr field)]
    (if (and (vector? field) (number? operand)) (numeric-cast e) e)))

(defn- new-params [] (atom []))

(defn- bind! [params v]
  (swap! params conj v)
  "?")

(defn- normalize-value [v]
  (cond (keyword? v) (name v) :else v))

(defn- compile-comparison [field [op value] params]
  (let [sql-op (case op := "=" :!= "!=" :> ">" :< "<" :>= ">=" :<= "<=")
        ph (bind! params (normalize-value value))]
    (str (comparand-expr field value) " " sql-op " " ph)))

(defn- compile-membership [field values params]
  (when (empty? values)
    (throw (ex-info ":in / set value must be non-empty" {:field field})))
  (let [fe  (if (and (vector? field) (every? number? values))
              (numeric-cast (field-expr field))
              (field-expr field))
        phs (mapv (fn [v] (bind! params (normalize-value v))) values)]
    (str fe " IN (" (str/join ", " phs) ")")))

(defn- compile-text [field [op s] params]
  (let [col (field-expr field) ph (bind! params s)]
    (case op
      :contains    (str "position(" ph " IN " col ") > 0")
      :icontains   (str "position(LOWER(" ph ") IN LOWER(" col ")) > 0")
      :starts-with (str "starts_with(" col ", " ph ")")
      :ends-with   (str "ends_with(" col ", " ph ")"))))

(defn- compile-matches [field [_ pattern] params]
  (let [ph (bind! params (str pattern))]
    (str "regexp_matches(" (field-expr field) ", " ph ")")))

(defn- compile-presence [field [op]]
  (let [expr (field-expr field)]
    (case op :exists? (str expr " IS NOT NULL") :absent? (str expr " IS NULL"))))

(defn- compile-has
  "Array-membership for the pipe-bounded `topics` column: `[:has :system]` →
   `topics LIKE '%|system|%'`. Bounded pipes prevent substring false-matches."
  [field [_ topic] params]
  (let [ph (bind! params (str "%|" (normalize-value topic) "|%"))]
    (str (field-expr field) " LIKE " ph)))

(defn- compile-where-entry [[field value] params]
  (cond
    (and (vector? value) (keyword? (first value)))
    (let [op (first value)]
      (cond
        (#{:= :!= :> :< :>= :<=}            op) (compile-comparison field value params)
        (= :in                              op) (compile-membership field (second value) params)
        (#{:contains :icontains
           :starts-with :ends-with}         op) (compile-text field value params)
        (= :matches                         op) (compile-matches field value params)
        (#{:exists? :absent?}               op) (compile-presence field value)
        (= :has                             op) (compile-has field value params)
        :else (throw (ex-info (str "Unknown operator: " op) {:field field :value value}))))
    (set? value) (compile-membership field value params)
    :else        (compile-comparison field [:= value] params)))

(defn- compile-where [where params]
  (when (and where (seq where))
    (str/join " AND " (map #(compile-where-entry % params) where))))

(defn- duration->seconds [^String s]
  (when-let [[_ n unit] (re-matches #"(\d+)\s*([smhd])" s)]
    (let [n (Long/parseLong n)]
      (case unit "s" n "m" (* n 60) "h" (* n 3600) "d" (* n 86400)))))

(defn- time-ref->instant
  "Parse a :since/:until value into an Instant: an ISO-8601 instant, or a
   short duration ('10m','1h','2d') interpreted as now-minus-duration."
  [value]
  (or (try (Instant/parse value) (catch Throwable _ nil))
      (when-let [secs (duration->seconds value)]
        (.minusSeconds (Instant/now) (long secs)))))

(defn- compile-time-bound [kind value params]
  (when value
    (when-let [inst (time-ref->instant value)]
      (let [op (if (= :since kind) ">" "<")
            ;; Bind the cutoff as a java.sql.Timestamp so it travels the SAME
            ;; setObject path as the stored `inst` column — both get identical
            ;; driver-side timezone treatment, so the comparison is symmetric.
            ;; (Casting an ISO-with-Z string literal did NOT match the stored
            ;; value's tz shift, silently widening the window to all rows.)
            ph (bind! params (inst->sql-ts inst))]
        (str "inst " op " " ph)))))

(defn- compile-order-by [order-by group-by]
  (let [[field direction] (or order-by (if group-by [:count :desc] [:inst :desc]))
        expr (cond
               (= :count field) "count"
               (vector? field)  (field-expr field)
               :else            (column-name field))]
    (str expr " " (str/upper-case (name direction)))))

(defn compile-log-sql
  "Compile a validated filter-map to `{:sql :params}` against synthigy_logs."
  [filter-map]
  (let [params  (new-params)
        {:keys [where since until limit order-by group-by count?]
         :or   {limit 100}} filter-map
        clauses (->> [(compile-where where params)
                      (compile-time-bound :since since params)
                      (compile-time-bound :until until params)]
                     (remove str/blank?) (remove nil?))
        where-sql (when (seq clauses) (str " WHERE " (str/join " AND " clauses)))
        select-cols (cond
                      count?   "count(*) AS count"
                      group-by (str (column-name group-by) ", count(*) AS count")
                      :else    "*")
        group-sql (when group-by (str " GROUP BY " (column-name group-by)))
        order-sql (when-not count? (str " ORDER BY " (compile-order-by order-by group-by)))
        limit-sql (when-not count? (str " LIMIT " limit))
        sql (str "SELECT " select-cols " FROM synthigy_logs"
                 (or where-sql "") (or group-sql "") (or order-sql "") (or limit-sql ""))]
    {:sql sql :params @params}))

;;; ============================================================================
;;; Log query execution + row reading
;;; ============================================================================

(defn- read-log-row [^ResultSet rs]
  {:v           1
   :inst        (some-> (.getTimestamp rs "inst") .toInstant str)
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
   :ctx         (try (json/<-json (.getString rs "ctx"))  (catch Throwable _ nil))
   :error_class (.getString rs "error_class")
   :error_msg   (.getString rs "error_msg")
   :error_trace (.getString rs "error_trace")})

(defn- bind-params! [^PreparedStatement ps params]
  (doseq [[i v] (map-indexed vector params)]
    (.setObject ps (inc i) v)))

(defn- execute-log-query [^Connection conn {:keys [sql params]}]
  (with-open [ps (.prepareStatement conn ^String sql)]
    (bind-params! ps params)
    (with-open [rs (.executeQuery ps)]
      (loop [acc (transient [])]
        (if (.next rs) (recur (conj! acc (read-log-row rs))) (persistent! acc))))))

(defn- execute-log-count [^Connection conn {:keys [sql params]}]
  (with-open [ps (.prepareStatement conn ^String sql)]
    (bind-params! ps params)
    (with-open [rs (.executeQuery ps)]
      (when (.next rs) (.getLong rs "count")))))

(defn- execute-log-group [^Connection conn group-key {:keys [sql params]}]
  (let [col (kebab->snake group-key)]
    (with-open [ps (.prepareStatement conn ^String sql)]
      (bind-params! ps params)
      (with-open [rs (.executeQuery ps)]
        (loop [acc (transient [])]
          (if (.next rs)
            (recur (conj! acc {group-key (.getString rs col)
                               :count    (.getLong rs "count")}))
            (persistent! acc)))))))

;;; ============================================================================
;;; Audit query helpers
;;; ============================================================================

(defn- read-entity-event [^ResultSet rs]
  {:track          :entity
   :seq            (.getLong rs "seq")
   :ts             (ts->iso (.getTimestamp rs "ts"))
   :tenant-xid     (.getString rs "tenant_xid")
   :record-xid     (.getString rs "record_xid")
   :entity-xid     (.getString rs "entity_xid")
   :attribute-xid  (.getString rs "attribute_xid")
   :value          (try (json/<-json (.getString rs "value")) (catch Throwable _ nil))
   :op             (.getString rs "op")
   :actor          (.getString rs "actor_xid")
   :request        (.getString rs "request_id")
   :scope          (.getString rs "scope_xid")
   :txid           (.getString rs "txid")})

(defn- read-relation-event [^ResultSet rs]
  {:track        :relation
   :seq          (.getLong rs "seq")
   :ts           (ts->iso (.getTimestamp rs "ts"))
   :tenant-xid   (.getString rs "tenant_xid")
   :relation-xid (.getString rs "relation_xid")
   :from-xid     (.getString rs "from_xid")
   :to-xid       (.getString rs "to_xid")
   :op           (.getString rs "op")
   :actor        (.getString rs "actor_xid")
   :request      (.getString rs "request_id")
   :scope        (.getString rs "scope_xid")
   :txid         (.getString rs "txid")})

(defn- query-audit-events
  [^Connection conn track ^String where-sql params limit order]
  (let [table (if (= track :relation) "audit_relation" "audit_entity")
        order (or order "ts DESC")
        limit-sql (when limit (str " LIMIT " (long limit)))
        sql (str "SELECT * FROM " table
                 (when (not (str/blank? where-sql)) (str " WHERE " where-sql))
                 " ORDER BY " order (or limit-sql ""))]
    (with-open [ps (.prepareStatement conn sql)]
      (bind-params! ps params)
      (with-open [rs (.executeQuery ps)]
        (let [reader (if (= track :relation) read-relation-event read-entity-event)]
          (loop [acc (transient [])]
            (if (.next rs) (recur (conj! acc (reader rs))) (persistent! acc))))))))

(defn- compile-audit-where
  [{:keys [record-xid between cursor seq tenant]}]
  (let [parts (cond-> []
                record-xid (conj ["record_xid = ?" [record-xid]])
                between    (into [["ts >= ?" [(inst->sql-ts (first between))]]
                                  ["ts <= ?" [(inst->sql-ts (second between))]]])
                cursor     (conj ["ts > ?" [(inst->sql-ts cursor)]])
                seq        (conj ["seq > ?" [seq]])
                tenant     (conj ["tenant_xid = ?" [tenant]]))]
    [(str/join " AND " (map first parts))
     (vec (mapcat second parts))]))

(defn- query-get-at
  [^Connection conn record-xid at tenant include-deleted?]
  (let [tenant-clause (when tenant " AND tenant_xid = ?")
        params (cond-> [record-xid (inst->sql-ts at)] tenant (conj tenant))
        sql (str "SELECT attribute_xid, value, op FROM audit_entity"
                 " WHERE record_xid = ? AND ts <= ?" (or tenant-clause "")
                 " QUALIFY ROW_NUMBER() OVER"
                 " (PARTITION BY attribute_xid ORDER BY ts DESC) = 1")]
    (with-open [ps (.prepareStatement conn sql)]
      (bind-params! ps params)
      (with-open [rs (.executeQuery ps)]
        (loop [acc {} deleted? false]
          (cond
            (not (.next rs))
            (if (and deleted? (not include-deleted?)) nil (not-empty acc))

            (= "__delete__" (.getString rs "attribute_xid"))
            (recur acc true)

            :else
            (let [attr (.getString rs "attribute_xid")
                  val  (try (json/<-json (.getString rs "value")) (catch Throwable _ nil))]
              (recur (assoc acc (keyword attr) val) deleted?))))))))

(defn- query-diff
  [conn record-xid from-ts to-ts tenant]
  (let [before  (query-get-at conn record-xid from-ts tenant true)
        after   (query-get-at conn record-xid to-ts   tenant true)
        ks      (into #{} (concat (keys (or before {})) (keys (or after {}))))
        changed (vec (sort (filter (fn [k] (not= (get before k) (get after k))) ks)))]
    {:before (or before {}) :after (or after {}) :changed changed}))

(defn- row-count [^Connection conn ^String table]
  (with-open [stmt (.createStatement conn)
              rs   (.executeQuery stmt (str "SELECT COUNT(*) AS c FROM " table))]
    (when (.next rs) (.getLong rs "c"))))

;;; ============================================================================
;;; Transport — bounded queue + writer thread + connection handle
;;; ============================================================================

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

(defn- make-transport ^Transport [^Connection conn config]
  (let [queue     (ArrayBlockingQueue. (int (:buffer-size config)))
        running?  (AtomicBoolean. true)
        written   (AtomicLong. 0)
        dropped   (AtomicLong. 0)
        batches   (AtomicLong. 0)
        ;; The log-writer thread gets its OWN connection (`.duplicate` shares the
        ;; same DuckDB database, separate statement context) so high-volume log
        ;; writes never contend with audit reads/writes. It closes itself when
        ;; the loop exits. DriverManager can't be reused for :memory: — a fresh
        ;; getConnection would be a different empty DB; duplicate is the only way.
        log-conn  (.duplicate ^DuckDBConnection conn)
        state     {:queue queue :conn log-conn :written written :dropped dropped
                   :batches batches :running? running? :config config}
        thread    (doto (Thread. ^Runnable (fn [] (log-writer-loop! state))
                                 "synthigy-observability-log-writer")
                    (.setDaemon true)
                    .start)]
    (->Transport queue conn written dropped batches running? thread config)))

(defn- with-read-conn
  "Run `(f read-conn)` on a private DuckDB connection — a cheap `.duplicate()` of
   the transport's connection, sharing the same database. Readers thus never run
   a statement on the same connection as the drainer's audit writes (which is the
   only thing DuckDB forbids; concurrent read/write across connections is fine,
   MVCC handles it). Closed after use."
  [tp f]
  (with-open [rc (.duplicate ^DuckDBConnection (transport-connection tp))]
    (f rc)))

;;; ============================================================================
;;; DuckDBObservability — one record, both protocols
;;; ============================================================================

(defrecord DuckDBObservability [^Transport tp config]
  ;; -------------------------------------------------------------------------
  ;; Log side
  ;; -------------------------------------------------------------------------
  store/LogStore

  (write-signal! [_ signal]
    (when (and tp signal) (tp signal)))

  (recent [this {:keys [limit ns-pattern level since]}]
    (let [opts (cond-> {:order-by [:inst :desc] :limit (or limit 100)}
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level)
                 since      (assoc :since since))]
      (store/search this opts)))

  (search [_ {:keys [count? group-by] :as opts}]
    (let [compiled (compile-log-sql opts)]
      (with-read-conn tp
        (fn [conn]
          (cond
            count?   (execute-log-count conn compiled)
            group-by (execute-log-group conn group-by compiled)
            :else    (execute-log-query conn compiled))))))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [opts (cond-> {:order-by [:inst :asc] :limit (or limit 100)}
                 cursor     (assoc :since cursor)
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level))
          rows (store/search this opts)
          next-cursor (some-> (last rows) :inst)]
      {:rows rows :next-cursor (or next-cursor cursor)}))

  (clear! [_]
    (with-read-conn tp
      (fn [conn]
        (with-open [stmt (.createStatement conn)]
          (.execute stmt "DELETE FROM synthigy_logs"))))
    nil)

  (health [_]
    (let [s (transport-stats tp)]
      (merge s
             {:up?     (boolean (:running? s))
              :backend :duckdb
              :path    (:path config)
              :rows    (try (with-read-conn tp (fn [conn] (row-count conn "synthigy_logs")))
                            (catch Throwable _ nil))})))

  (snapshot [_]
    ;; Durable backend — no transient state to hand off. Used at start
    ;; only on the OUTGOING store (typically the default RingLogStore).
    nil)

  ;; -------------------------------------------------------------------------
  ;; Audit side — writers called by drainer (synchronous per batch);
  ;; readers serve the /history endpoint.
  ;; -------------------------------------------------------------------------
  audit/AuditProvider

  ;; The per-entity/relation audit-opt-in decision is frozen at enqueue time
  ;; (the substrate trigger's `audit` flag) and applied by the drainer, so the
  ;; provider no longer re-evaluates the mutable policy here — that drain-time
  ;; re-check raced the write and silently dropped committed records. Only the
  ;; coarse system gate (`persistence-enabled?` — is IAM running for
  ;; attribution) remains. Same for both backends → DuckDB/ClickHouse parity.
  ;;
  ;; Concurrency: DuckDB itself handles concurrent read/write (MVCC). What you
  ;; cannot do is run two statements on ONE JDBC connection at once. So writes
  ;; (drainer thread, the main `conn`) and reads (`with-read-conn` → a cheap
  ;; `.duplicate()` per call) use SEPARATE connections to the same database; the
  ;; log-writer thread has its own too. No locks — each thread owns a connection.
  (write-entity-deltas! [_ envelopes]
    (let [rows (when (audit/persistence-enabled?)
                 (vec (envelope->entity-rows envelopes)))]
      (when (seq rows)
        (insert-entity-batch! (transport-connection tp) rows))))

  (write-relation-deltas! [_ envelopes]
    (let [rows (when (audit/persistence-enabled?)
                 (vec (envelope->relation-rows envelopes)))]
      (when (seq rows)
        (insert-relation-batch! (transport-connection tp) rows))))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (with-read-conn tp
      (fn [conn] (query-get-at conn record-xid at tenant (boolean include-deleted?)))))

  (events [_ {:keys [limit track] :or {track :entity} :as opts}]
    (let [[where-sql params] (compile-audit-where opts)]
      (with-read-conn tp
        (fn [conn] (query-audit-events conn track where-sql params limit nil)))))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (with-read-conn tp
      (fn [conn] (query-diff conn record-xid from-ts to-ts tenant))))

  (timeline [_ {:keys [group-by limit] :or {group-by :request} :as opts}]
    (let [[where-sql params] (compile-audit-where opts)
          rows               (with-read-conn tp
                               (fn [conn] (query-audit-events conn :entity
                                                              where-sql params limit nil)))
          key-fn             (case group-by
                               :request :request :actor :actor :scope :scope :request)]
      (clojure.core/group-by key-fn rows)))

  (since [_ {:keys [limit track] sq :seq :or {track :entity} :as opts}]
    (let [[where-sql params] (compile-audit-where opts)]
      (with-read-conn tp
        (fn [conn] (query-audit-events conn track where-sql params limit
                                       (if sq "seq ASC" "ts ASC")))))))

;;; ============================================================================
;;; Module state
;;;
;;; The Telemere → *log-store* bridge handler is owned by `synthigy.log`
;;; (handler id :synthigy/store-bridge, installed in log/install!). This
;;; module's only routing job is to bind *log-store* in start; the bridge
;;; picks up the new binding on its next signal. Don't install a second
;;; handler here — it would write every signal to the store twice.
;;; ============================================================================

(defonce ^:private state (atom nil))

(def ^:private model-watch-key ::audit-policy-watch)

(defn- ensure-parent-dir!
  "Create the parent dir of `path` if file mode + parent missing. No-op for
   :memory:."
  [path]
  (when (and (string? path) (not= path ":memory:") (not (str/blank? path)))
    (when-let [parent (.getParentFile (File. ^String path))]
      (when-not (.exists parent) (.mkdirs parent)))))

;;; ============================================================================
;;; Lifecycle — setup, start, stop, cleanup
;;; ============================================================================

(defn setup
  "One-time setup: ensure the storage file's parent directory exists, then
   apply the schema. Patcho tracks this as completed in the lifecycle store.

   Idempotent at the SQL layer (CREATE TABLE IF NOT EXISTS); patcho still
   only runs the `:setup` fn once per persistent store."
  []
  (let [cfg (env-config)]
    (ensure-parent-dir! (:path cfg))
    (with-open [conn (open-connection (:path cfg))]
      (apply-schema! conn))
    (log/info {:id ::setup-complete
               :data {:action :setup-complete :subject :observability
                      :backend :duckdb :path (:path cfg)}}
              "Observability schema applied")))

(defn- drain-snapshot!
  "If the outgoing store is a transient backend (RingLogStore), drain its
   buffered signals into the durable record before swapping. Preserves
   boot-phase visibility in the durable store. No-op for nil / for
   durable backends (which return nil from `snapshot`)."
  [outgoing incoming]
  (let [signals (try (store/snapshot outgoing) (catch Throwable _ nil))
        n       (count (or signals []))]
    (when (pos? n)
      (doseq [sig signals]
        (try (store/write-signal! incoming sig) (catch Throwable _)))
      (log/info {:id ::ring-drained
                 :data {:action :written :subject :observability
                        :backend :duckdb :signals n}}
                (str "Drained " n " buffered signal(s) into DuckDB")))))

(defn start
  "Open the persistent connection, start the log writer, bind both
   dynvars, install the audit policy watch.

   Swaps `*log-store*` to the durable record FIRST (so new signals land
   durably at once), then drains the previously-bound store (typically the
   default RingLogStore) so boot-time signals are preserved too.

   `setup` already applied the schema once; we re-apply here too because
   in-memory mode loses state between starts (each connection is a fresh
   DB) and the cost is negligible for file mode."
  []
  (let [cfg      (env-config)
        conn     (open-connection (:path cfg))
        _        (apply-schema! conn)
        tp       (make-transport conn cfg)
        rec      (->DuckDBObservability tp cfg)
        outgoing @#'store/*log-store*]
    (log/info {:id ::starting
               :data {:action :starting :subject :observability
                      :backend :duckdb :path (:path cfg)}}
              "Starting observability substrate")
    ;; Swap FIRST so every new signal lands in the durable store immediately;
    ;; the old ring is then frozen (the bridge no longer references it) and we
    ;; drain its final contents into the durable store afterward. Drained boot
    ;; signals carry their original :inst, so they still sort correctly even
    ;; though they're inserted after some live signals. Doing it the other way
    ;; (drain then swap) leaves a race the width of the whole drain.
    (alter-var-root #'store/*log-store* (constantly rec))
    (drain-snapshot! outgoing rec)
    (alter-var-root #'audit/*audit-provider* (constantly rec))
    (audit/recompile-policy! (dataset/deployed-model))
    (dataset/add-model-watch!
     model-watch-key
     (fn [_k _ref _old new-model] (audit/recompile-policy! new-model)))
    (reset! state {:record rec :transport tp :connection conn :config cfg})
    (log/info {:id ::started
               :data {:action :started :subject :observability :backend :duckdb}}
              "Observability substrate started")))

(defn stop
  "Reverse start: drain the writer, close the connection, rebind
   `*log-store*` to a fresh RingLogStore (so the queryable surface
   survives backend teardown), unbind the audit provider, remove the
   model watch."
  []
  (log/info {:id ::stopping
             :data {:action :stopping :subject :observability :backend :duckdb}}
            "Stopping observability substrate")
  (dataset/remove-model-watch! model-watch-key)
  (audit/recompile-policy! nil)
  (when-let [{:keys [transport]} @state]
    (try (close-transport! transport) (catch Throwable _)))
  (alter-var-root #'store/*log-store* (constantly (store/create-ring)))
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (reset! state nil)
  (log/info {:id ::stopped
             :data {:action :stopped :subject :observability :backend :duckdb}}
            "Observability substrate stopped"))

(defn cleanup
  "Drop the substrate's tables. Reverses `setup`. For file mode this leaves
   the .duckdb file in place (DuckDB has no DROP DATABASE for embedded files
   — operator removes the file separately if they want a true wipe)."
  []
  (let [cfg (env-config)]
    (with-open [conn (open-connection (:path cfg))]
      (drop-schema! conn))
    (log/info {:id ::cleanup-complete
               :data {:action :cleanup-complete :subject :observability
                      :backend :duckdb :path (:path cfg)}}
              "Observability schema dropped")))

;;; ============================================================================
;;; Module registration
;;; ============================================================================

(patch/current-version :synthigy/observability "1.0.0")

(lifecycle/register-module!
 :synthigy/observability
  ;; Substrate sits parallel to :synthigy/server. Depends on the bits we
  ;; actually use: :synthigy/log for the Telemere pipeline (we hang our
  ;; bridge handler off it), :synthigy/substrate so the drainer is up
  ;; before audit writes can land.
 {:depends-on [:synthigy/log :synthigy/substrate]
  :doc "Log/audit analytics sink (DuckDB)"
  :setup   setup
  :start   start
  :stop    stop
  :cleanup cleanup})
