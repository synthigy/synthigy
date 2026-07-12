(ns synthigy.observability
  "ClickHouse implementation of Synthigy's observability substrate — durable
  append-only storage for diagnostic logs (`synthigy.log.store/LogStore`)
  AND state-change audit events (`synthigy.audit/AuditProvider`), backed
  by a ClickHouse cluster over pure HTTP (Java 11+ `java.net.http`).

  This is the ClickHouse-flavored `synthigy.observability` namespace —
  shadowed-namespace pattern (`synthigy.server` style). The DuckDB
  variant lives in `duckdb/src/synthigy/observability.clj`. Operators
  pick ONE backend per environment via alias.

  ## Operator model

      clj -M:postgres:clickhouse:httpkit:dev
      (lifecycle/start! :synthigy/observability)

  Starting the module:
  - validates CH connection env
  - applies the schema (logs + audit_entity + audit_relation) idempotently
  - starts the log-writer thread (batched async HTTP POST)
  - installs the Telemere bridge handler that streams signals into the log
  - binds `synthigy.log.store/*log-store*` AND `synthigy.audit/*audit-provider*`
    to a single `ClickHouseObservability` record that implements both
  - recompiles the audit policy from the deployed model + installs a
    model-watch so policy stays current as the model evolves

  ## Three tables

      <database>.logs            (LogStore — diagnostic events, 30-day TTL)
      <database>.audit_entity    (AuditProvider — attribute-grain state changes)
      <database>.audit_relation  (AuditProvider — relation edge changes)

  Cross-table joins (logs ↔ audit) on `request_id` are first-class — same
  cluster, same database, no shard hop.

  ## Config (env)

      SYNTHIGY_OBSERVABILITY_CH_URL       http://ch:8123  (required)
      SYNTHIGY_OBSERVABILITY_CH_DB        Database name   (default: synthigy)
      SYNTHIGY_OBSERVABILITY_CH_USER      Basic-auth user (optional)
      SYNTHIGY_OBSERVABILITY_CH_PASSWORD  Basic-auth pwd  (optional)
      SYNTHIGY_OBSERVABILITY_BUFFER_SIZE  Log queue cap   (default 8192)
      SYNTHIGY_OBSERVABILITY_BATCH_ROWS   Max INSERT batch (default 500)
      SYNTHIGY_OBSERVABILITY_BATCH_MS     Max flush delay (default 1000 ms)

  ## Why one record for two protocols

  Same connection model, same lifecycle, same operator decision.
  Implementing both on one record keeps storage/teardown coordinated.

  ## Replaces

  This module is the consolidated successor to the legacy split:
    synthigy.log.sink.clickhouse     (write-side log sink)
    synthigy.log.query.clickhouse    (read-side log queries)
    synthigy.audit.clickhouse        (audit writes; readers were :not-implemented)
    synthigy.log.clickhouse          (schema-management patcho module)
  When the synthigy.log registry-strip refactor lands those files retire."
  (:require
    [clojure.string :as str]
    [environ.core :refer [env]]
    [jsonista.core :as jsonista]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.audit :as audit]
    [synthigy.dataset :as dataset]
    [synthigy.json :as json]
    [synthigy.log :as log]
    [synthigy.log.store :as store])
  (:import
    [java.net URI URLEncoder]
    [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                   HttpResponse$BodyHandlers]
    [java.nio.charset StandardCharsets]
    [java.time Duration Instant]
    [java.util ArrayList Base64]
    [java.util.concurrent ArrayBlockingQueue TimeUnit]
    [java.util.concurrent.atomic AtomicBoolean AtomicLong]))

;;; ============================================================================
;;; Defaults + env
;;; ============================================================================

(def ^:private default-config
  {:database         "synthigy"
   :buffer-size      8192
   :batch-rows       500
   :batch-ms         1000
   :retry-attempts   5
   :retry-min-ms     250
   :retry-max-ms     30000
   :connect-timeout-ms 5000
   :request-timeout-ms 30000
   :close-timeout-ms 2000})

(defn- parse-int-or [s fallback]
  (or (try (some-> s str/trim Integer/parseInt) (catch Throwable _ nil))
      fallback))

(defn- env-config
  "Read SYNTHIGY_OBSERVABILITY_CH_* + size/batch overrides on top of
   defaults. Throws on missing :url so the module refuses to start without
   a configured CH endpoint."
  []
  (let [url      (env :synthigy-observability-ch-url)
        _        (when (str/blank? url)
                   (throw (ex-info ":synthigy/observability requires SYNTHIGY_OBSERVABILITY_CH_URL"
                                   {:cause :missing-config})))
        base     default-config]
    {:url              (str/replace url #"/+$" "")
     :database         (or (env :synthigy-observability-ch-db) (:database base))
     :user             (env :synthigy-observability-ch-user)
     :password         (env :synthigy-observability-ch-password)
     :buffer-size      (parse-int-or (env :synthigy-observability-buffer-size) (:buffer-size base))
     :batch-rows       (parse-int-or (env :synthigy-observability-batch-rows)  (:batch-rows base))
     :batch-ms         (parse-int-or (env :synthigy-observability-batch-ms)    (:batch-ms base))
     :retry-attempts   (:retry-attempts     base)
     :retry-min-ms     (:retry-min-ms       base)
     :retry-max-ms     (:retry-max-ms       base)
     :connect-timeout-ms (:connect-timeout-ms base)
     :request-timeout-ms (:request-timeout-ms base)
     :close-timeout-ms (:close-timeout-ms   base)}))

;;; ============================================================================
;;; HTTP transport
;;; ============================================================================

(defn- basic-auth-header
  [user password]
  (when (and user (not (str/blank? user)))
    (let [creds (str user ":" (or password ""))
          enc   (.encodeToString (Base64/getEncoder)
                                 (.getBytes ^String creds StandardCharsets/UTF_8))]
      (str "Basic " enc))))

(defn- new-client ^HttpClient [{:keys [connect-timeout-ms]}]
  ;; Force HTTP/1.1 WITHOUT the h2c upgrade preamble. Java's default
  ;; HttpClient sends `Connection: Upgrade` + `Upgrade: h2c` + `HTTP2-Settings:
  ;; …` headers on every HTTP/1.1 request, advertising a clear-text HTTP/2
  ;; upgrade. ClickHouse's HTTP server treats those headers as a protocol
  ;; negotiation and silently consumes the request body — INSERT returns
  ;; 200 OK with 0 rows landed. Pinning `.version(HTTP_1_1)` on the
  ;; builder still emits the h2c headers; the only reliable suppressor is
  ;; the `jdk.httpclient.HttpClient.protocolVersion` system property
  ;; (which is process-wide), so we set it on the *first* client we
  ;; build. Subsequent clients inherit. The property is idempotent.
  (System/setProperty "jdk.httpclient.HttpClient.protocolVersion" "HTTP_1_1")
  (-> (HttpClient/newBuilder)
      (.version java.net.http.HttpClient$Version/HTTP_1_1)
      (.connectTimeout (Duration/ofMillis (long connect-timeout-ms)))
      .build))

(defn- url-encode [^String s]
  (URLEncoder/encode s "UTF-8"))

(defn- post!
  "POST `body` as CH query body with URL-encoded query string. Returns
   response body String on 2xx, throws ex-info on >=400."
  [^HttpClient client config ^String query ^String body content-type]
  (let [{:keys [url database user password request-timeout-ms]} config
        ;; insert_deduplicate=0 — CH 24.8+ defaults to ON, which dedupes
        ;; identical blocks within a deduplication window. Two batches with
        ;; the same row content (e.g., two boot signals at the same instant
        ;; with the same message) would silently lose the second one. Off
        ;; here because the cockpit/operator wants to see every emit;
        ;; semantic dedup is the caller's choice, not the transport's.
        uri (URI/create
              (str url "/?database=" (url-encode database)
                   "&query=" (url-encode query)
                   "&output_format_json_quote_64bit_integers=0"
                   "&insert_deduplicate=0"))
        builder (-> (HttpRequest/newBuilder)
                    (.uri uri)
                    (.header "Content-Type" content-type)
                    ;; CRITICAL: CH silently drops the INSERT body (returns
                    ;; 200, 0 rows) when Content-Type is something it doesn't
                    ;; recognize as text-like AND no Accept header is set.
                    ;; `application/x-ndjson` triggers this, leading to
                    ;; whole-batch loss with no error. Sending an explicit
                    ;; Accept header sidesteps the heuristic. Bisected via
                    ;; curl: adding `Accept: */*` is the single change that
                    ;; flips the behavior from drop to insert.
                    (.header "Accept" "*/*")
                    (.timeout (Duration/ofMillis (long request-timeout-ms)))
                    (.POST (HttpRequest$BodyPublishers/ofString
                             body StandardCharsets/UTF_8)))
        builder (if-let [auth (basic-auth-header user password)]
                  (.header builder "Authorization" auth)
                  builder)
        resp    (.send client (.build builder)
                       (HttpResponse$BodyHandlers/ofString))
        status  (.statusCode resp)
        body    (.body resp)]
    (when (>= status 400)
      (throw (ex-info (str "ClickHouse responded " status)
                      {:status status :body body :query query})))
    body))

(defn- post-with-params!
  "POST a parameterized query. `params` is `{name {:type Kw :value String}}`;
   each becomes `?param_<name>=<value>` and the query uses `{name:Type}`
   placeholders. Returns body String."
  [^HttpClient client config ^String query params]
  (let [{:keys [url database user password request-timeout-ms]} config
        base-qs (str "?database=" (url-encode database)
                     "&query=" (url-encode query)
                     "&output_format_json_quote_64bit_integers=0")
        param-qs (str/join "&" (map (fn [[n {:keys [value]}]]
                                      (str "param_" n "=" (url-encode value)))
                                    params))
        uri (URI/create (str url "/" base-qs (when (seq params) (str "&" param-qs))))
        builder (-> (HttpRequest/newBuilder)
                    (.uri uri)
                    (.header "Content-Type" "text/plain; charset=utf-8")
                    (.timeout (Duration/ofMillis (long request-timeout-ms)))
                    (.GET))
        builder (if-let [auth (basic-auth-header user password)]
                  (.header builder "Authorization" auth)
                  builder)
        resp    (.send client (.build builder)
                       (HttpResponse$BodyHandlers/ofString))
        status  (.statusCode resp)
        body    (.body resp)]
    (when (>= status 400)
      (throw (ex-info (str "ClickHouse responded " status)
                      {:status status :body body :query query})))
    body))

(defn- exec-ddl!
  "Execute a CH DDL statement (no body return value). Idempotent statements
   are safe to call repeatedly."
  [^HttpClient client config ^String statement]
  (let [{:keys [url user password request-timeout-ms]} config
        uri (URI/create url)
        builder (-> (HttpRequest/newBuilder)
                    (.uri uri)
                    (.timeout (Duration/ofMillis (long request-timeout-ms)))
                    (.POST (HttpRequest$BodyPublishers/ofString
                             statement StandardCharsets/UTF_8)))
        builder (if-let [auth (basic-auth-header user password)]
                  (.header builder "Authorization" auth)
                  builder)
        resp    (.send client (.build builder)
                       (HttpResponse$BodyHandlers/ofString))
        status  (.statusCode resp)]
    (when (>= status 400)
      (throw (ex-info (str "ClickHouse DDL failed: " status)
                      {:status status :body (.body resp) :stmt statement})))
    nil))

;;; ============================================================================
;;; Schema DDL — logs + audit_entity + audit_relation
;;; ============================================================================

(defn- create-db-ddl [database]
  (str "CREATE DATABASE IF NOT EXISTS " database))

(defn- logs-ddl [database]
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
          date         Date MATERIALIZED toDate(inst)
        )
        ENGINE = MergeTree
        PARTITION BY date
        ORDER BY (tenant, inst, level)
        TTL date + INTERVAL 30 DAY
        SETTINGS index_granularity = 8192"))

(defn- audit-entity-ddl [database]
  ;; `txid` is String, not UInt64 — substrates produce different shapes
  ;; (SQLite synthesizes UUIDv7 strings, PG/CRDB use BIGINT, etc.) and a
  ;; numeric column can't hold them all. UUIDv7 is binary-sortable as a
  ;; String, preserving chronology for ORDER BY txid queries.
  (str "CREATE TABLE IF NOT EXISTS " database ".audit_entity (
          ts             DateTime64(3, 'UTC'),
          tenant_xid     String,
          record_xid     String,
          entity_xid     LowCardinality(String),
          attribute_xid  LowCardinality(String),
          value          String,
          op             LowCardinality(String),
          actor_xid      String,
          request_id     String,
          scope_xid      String,
          txid           String,
          seq            Int64 DEFAULT 0,
          INDEX idx_entity_seq seq TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (entity_xid, record_xid, ts)
        SETTINGS index_granularity = 8192"))

(defn- audit-relation-ddl [database]
  (str "CREATE TABLE IF NOT EXISTS " database ".audit_relation (
          ts             DateTime64(3, 'UTC'),
          tenant_xid     String,
          relation_xid   LowCardinality(String),
          from_xid       String,
          to_xid         String,
          op             LowCardinality(String),
          actor_xid      String,
          request_id     String,
          scope_xid      String,
          txid           String,
          seq            Int64 DEFAULT 0,
          INDEX idx_relation_seq seq TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (relation_xid, ts)
        SETTINGS index_granularity = 8192"))

(defn- migrations-ddl
  "Idempotent column-adds for schema evolution. `CREATE TABLE IF NOT EXISTS`
   is a no-op on an existing table, so wire-v2's `topics` must be ALTERed in
   for deployments whose `logs` table predates the column."
  [db]
  [(str "ALTER TABLE " db ".logs ADD COLUMN IF NOT EXISTS "
        "topics Array(LowCardinality(String)) CODEC(ZSTD)")
   (str "ALTER TABLE " db ".audit_entity ADD COLUMN IF NOT EXISTS seq Int64 DEFAULT 0")
   (str "ALTER TABLE " db ".audit_relation ADD COLUMN IF NOT EXISTS seq Int64 DEFAULT 0")])

(defn- apply-schema! [^HttpClient client config]
  (let [db (:database config)]
    (exec-ddl! client config (create-db-ddl db))
    (exec-ddl! client config (logs-ddl db))
    (exec-ddl! client config (audit-entity-ddl db))
    (exec-ddl! client config (audit-relation-ddl db))
    (doseq [ddl (migrations-ddl db)]
      (try (exec-ddl! client config ddl) (catch Throwable _)))))

(defn- drop-schema! [^HttpClient client config]
  (let [db (:database config)]
    (exec-ddl! client config (str "DROP TABLE IF EXISTS " db ".logs"))
    (exec-ddl! client config (str "DROP TABLE IF EXISTS " db ".audit_entity"))
    (exec-ddl! client config (str "DROP TABLE IF EXISTS " db ".audit_relation"))))

;;; ============================================================================
;;; Log signal → row coercion
;;; ============================================================================

(def ^:private promoted-ctx-keys
  [:request-id :user-xid :tenant])

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

(def ^:private ch-instant-formatter
  ;; CH DateTime64(3) JSONEachRow parser SILENTLY DROPS rows whose inst
  ;; lacks an explicit milliseconds field. `Instant.toString()` omits
  ;; `.SSS` on whole-second boundaries (a row emitted at exactly 12:00:00
  ;; serializes as `2026-06-06T12:00:00Z`, not `…00.000Z`). That row never
  ;; lands. Use a formatter that always emits 3 millis digits.
  (-> (java.time.format.DateTimeFormatterBuilder.)
      (.appendInstant 3)
      (.toFormatter)))

(defn- inst->ch-string
  "CH DateTime64(3) accepts ISO-8601 strings via JSONEachRow. Empty string
   maps to epoch — CH doesn't tolerate null on non-Nullable columns. We
   always emit 3 millis digits to avoid CH's silent drop of zero-millis
   timestamps (see ch-instant-formatter)."
  [inst]
  (cond
    (nil? inst)                       "1970-01-01T00:00:00.000Z"
    (instance? Instant inst)          (.format ch-instant-formatter ^Instant inst)
    (instance? java.sql.Timestamp inst)
    (.format ch-instant-formatter (.toInstant ^java.sql.Timestamp inst))
    (instance? java.util.Date inst)
    (.format ch-instant-formatter (.toInstant ^java.util.Date inst))
    (string? inst)
    (if (re-find #"\.\d{1,9}" inst) inst
        ;; String without sub-second component — try to canonicalize.
        (try (.format ch-instant-formatter (Instant/parse inst))
             (catch Throwable _ inst)))
    :else                             (str inst)))

(defn- str-or-empty [v] (if (some? v) (str v) ""))

(defn signal->log-row
  "Project a Telemere signal map to the logs table column shape (Strings
   throughout — CH JSONEachRow consumes the resulting map directly)."
  [{:keys [inst level ns id msg_ data ctx error] :as signal}]
  (let [[err-class err-msg err-trace] (throwable->parts error)
        ctx-map      (or ctx {})
        residual-ctx (apply dissoc ctx-map promoted-ctx-keys)
        host-raw     (:host signal)
        host         (cond
                       (string? host-raw) host-raw
                       (map? host-raw)    (or (:name host-raw) "")
                       :else              "")]
    {:inst        (inst->ch-string inst)
     :level       (str-or-empty (some-> level name))
     :ns          (str-or-empty (when ns (str ns)))
     :id          (str-or-empty (id->str id))
     :msg         (str-or-empty (when msg_ (try (force msg_) (catch Throwable _ ""))))
     :request_id  (str-or-empty (or (:request-id signal) (get ctx-map :request-id)))
     :user_xid    (str-or-empty (or (:user-xid signal)   (get ctx-map :user-xid)))
     :tenant      (str-or-empty (or (:tenant signal)     (get ctx-map :tenant)))
     :host        host
     ;; Native CH Array(String) — JSONEachRow encodes a vector as a JSON array,
     ;; and `:has` queries with has(topics, ?). Sorted for stable round-trips.
     :topics      (->> (:topics signal)
                       (map (fn [t] (if (keyword? t) (name t) (str t))))
                       sort vec)
     :data        (json/->json (or data {}))
     :ctx         (json/->json residual-ctx)
     :error_class (str-or-empty err-class)
     :error_msg   (str-or-empty err-msg)
     :error_trace (str-or-empty err-trace)}))

;;; ============================================================================
;;; Audit envelope → row coercion (mirrors DuckDB impl)
;;; ============================================================================

(defn- attribute-pairs [attrs]
  (->> (or attrs {})
       (mapv (fn [[k v]] [(name k) (json/->json v)]))))

(defn- diff-pairs [before after]
  (->> (or after {})
       (filter (fn [[k v]] (not= v (get before k))))
       (mapv (fn [[k v]] [(name k) (json/->json v)]))))

(defn- envelope->entity-rows
  [envelopes]
  (for [env envelopes
        :let [data       (-> env :delta :data)
              record-xid (str-or-empty (:record-xid data))]
        :when (not (str/blank? record-xid))
        :let [op         (-> env :delta :type name)
              ts         (inst->ch-string (:ts data))
              tenant     (str-or-empty (:tenant data))
              entity-xid (str-or-empty (:entity-xid data))
              actor      (str-or-empty (:actor data))
              request    (str-or-empty (:request data))
              scope      (str-or-empty (:scope data))
              txid       (str-or-empty (:txid data))
              sq         (or (:seq env) 0)]
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
              from-xid (str-or-empty (:from-xid data))
              to-xid   (str-or-empty (:to-xid data))]
        :when (and (not (str/blank? from-xid)) (not (str/blank? to-xid)))]
    {:ts            (inst->ch-string (:ts data))
     :tenant_xid    (str-or-empty (:tenant data))
     :relation_xid  (str-or-empty (:element env))
     :from_xid      from-xid
     :to_xid        to-xid
     :op            (-> env :delta :type name)
     :actor_xid     (str-or-empty (:actor data))
     :request_id    (str-or-empty (:request data))
     :scope_xid     (str-or-empty (:scope data))
     :txid          (str-or-empty (:txid data))
     :seq           (or (:seq env) 0)}))

;;; ============================================================================
;;; JSONEachRow encoding
;;; ============================================================================

(def ^:private write-mapper
  (jsonista/object-mapper {}))

(defn- rows->ndjson
  "Encode rows as newline-delimited JSON (CH's JSONEachRow input format)."
  [rows]
  (let [sb (StringBuilder.)]
    (doseq [r rows]
      (.append sb ^String (jsonista/write-value-as-string r write-mapper))
      (.append sb \newline))
    (.toString sb)))

;;; ============================================================================
;;; Batch INSERT helpers
;;; ============================================================================

(defn- insert-logs! [^HttpClient client config signals]
  (when (seq signals)
    (let [rows (mapv signal->log-row signals)
          body (rows->ndjson rows)
          q    (str "INSERT INTO " (:database config) ".logs FORMAT JSONEachRow")]
      (post! client config q body "application/x-ndjson; charset=utf-8"))))

(defn- insert-entity-rows! [^HttpClient client config rows]
  (when (seq rows)
    (let [body (rows->ndjson rows)
          q    (str "INSERT INTO " (:database config) ".audit_entity FORMAT JSONEachRow")]
      (post! client config q body "application/x-ndjson; charset=utf-8"))))

(defn- insert-relation-rows! [^HttpClient client config rows]
  (when (seq rows)
    (let [body (rows->ndjson rows)
          q    (str "INSERT INTO " (:database config) ".audit_relation FORMAT JSONEachRow")]
      (post! client config q body "application/x-ndjson; charset=utf-8"))))

;;; ============================================================================
;;; Log writer thread — async batched HTTP POST with retry
;;; ============================================================================

(defn- backoff-ms [attempt {:keys [retry-min-ms retry-max-ms]}]
  (min (long retry-max-ms)
       (* (long retry-min-ms) (long (Math/pow 2 attempt)))))

(defn- send-log-batch-with-retry!
  [client config signals]
  (let [max-attempts (long (:retry-attempts config))]
    (loop [attempt 0]
      (let [outcome (try (insert-logs! client config signals) {:ok? true}
                         (catch Throwable t {:ok? false :err t}))]
        (cond
          (:ok? outcome) true
          (>= attempt max-attempts)
          (do (binding [*out* *err*]
                (println (str "synthigy.observability(ch): log batch dropped after "
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
  [{:keys [^ArrayBlockingQueue queue client ^AtomicLong written
           ^AtomicLong dropped ^AtomicLong batches ^AtomicBoolean running?
           config]}]
  (let [{:keys [batch-rows batch-ms]} config]
    (while (.get running?)
      (try
        (let [batch (drain-log-batch! queue batch-rows batch-ms)]
          (when (pos? (.size batch))
            (let [ok? (send-log-batch-with-retry! client config batch)]
              (.incrementAndGet batches)
              (if ok?
                (.addAndGet written (.size batch))
                (.addAndGet dropped (.size batch))))))
        (catch InterruptedException _ (.set running? false))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.observability(ch): log writer loop threw: "
                          (some-> t .getClass .getName) ": " (.getMessage t)))))))
    ;; Drain remaining on shutdown — best-effort.
    (let [tail (ArrayList.)]
      (.drainTo queue tail)
      (when (pos? (.size tail))
        (let [ok? (send-log-batch-with-retry! client config tail)]
          (if ok?
            (.addAndGet written (.size tail))
            (.addAndGet dropped (.size tail))))))))

;;; ============================================================================
;;; Log query compilation — filter-map → CH SQL + URL params
;;;
;;; CH uses {name:Type} placeholders in SQL paired with param_<name> URL
;;; query params. The compiler tracks each value alongside its inferred
;;; type and emits a placeholder.
;;; ============================================================================

(defn- kebab->snake [k]
  (-> (name k) (str/replace "-" "_")))

(defn- column-name [field] (kebab->snake field))

(defn- value->ch-type
  [v]
  (cond
    (boolean? v)          :Bool
    (instance? Long v)    :Int64
    (integer? v)          :Int64
    (instance? Double v)  :Float64
    (float? v)            :Float64
    (keyword? v)          :String
    (string? v)           :String
    :else                 :String))

(defn- json-extract-fn [ch-type]
  (case ch-type
    :Int64   "JSONExtractInt"
    :Float64 "JSONExtractFloat"
    :Bool    "JSONExtractBool"
    :String  "JSONExtractString"))

(defn- field-expr
  "Built-in column keyword → column name. `[:data k1 k2]` →
   `JSONExtract*(data, 'k1', 'k2')` with type inferred from comparison value."
  [field for-value-type]
  (cond
    (keyword? field) (column-name field)
    (vector? field)
    (let [[root & path] field
          fn-name (json-extract-fn for-value-type)
          col     (column-name root)
          quoted  (str/join ", " (map #(str "'" (kebab->snake %) "'") path))]
      (str fn-name "(" col ", " quoted ")"))
    :else
    (throw (ex-info (str "Unsupported field reference: " (pr-str field))
                    {:field field}))))

(defn- new-params [] (atom {:idx 0 :params {}}))

(defn- bind!
  [state v ch-type]
  (let [n (:idx (swap! state update :idx inc))
        nm (str "p" n)
        rendered (cond
                   (boolean? v) (str v)
                   (keyword? v) (clojure.core/name v)
                   :else        (str v))]
    (swap! state assoc-in [:params nm] {:type ch-type :value rendered})
    (str "{" nm ":" (clojure.core/name ch-type) "}")))

(defn- compile-comparison [field [op value] state]
  (let [t   (value->ch-type value)
        col (field-expr field t)
        ph  (bind! state value t)
        sql-op (case op := "=" :!= "!=" :> ">" :< "<" :>= ">=" :<= "<=")]
    (str col " " sql-op " " ph)))

(defn- compile-membership [field values state]
  (when (empty? values)
    (throw (ex-info ":in / set value must be non-empty" {:field field})))
  (let [sample (first values)
        t      (value->ch-type sample)
        col    (field-expr field t)
        phs    (mapv #(bind! state % t) values)]
    (str col " IN (" (str/join ", " phs) ")")))

(defn- compile-text [field [op s] state]
  (let [col (field-expr field :String)
        ph  (bind! state s :String)]
    (case op
      :contains    (str "position(" col ", " ph ") > 0")
      :icontains   (str "positionCaseInsensitive(" col ", " ph ") > 0")
      :starts-with (str "startsWith(" col ", " ph ")")
      :ends-with   (str "endsWith(" col ", " ph ")"))))

(defn- compile-matches [field [_ pattern] state]
  (let [col (field-expr field :String)
        ph  (bind! state (str pattern) :String)]
    (str "match(" col ", " ph ")")))

(defn- compile-presence [field [op] _state]
  (cond
    (vector? field)
    ;; JSON path — JSONHas tells us whether the key exists in the JSON
    ;; object, regardless of its value.
    (let [[root & path] field
          col (column-name root)
          quoted (str/join ", " (map #(str "'" (kebab->snake %) "'") path))
          base (str "JSONHas(" col ", " quoted ")")]
      (if (= op :exists?) base (str "NOT " base)))
    :else
    ;; Top-level column. The schema uses non-Nullable `String` for the
    ;; promoted-ctx columns (request_id, user_xid, tenant, host, …) and
    ;; LowCardinality(String) for level/ns/id/etc. CH stores "missing" as
    ;; the empty string, NEVER as SQL NULL. So `IS NULL` would never match
    ;; and `IS NOT NULL` would always match — both useless.
    ;;
    ;; Use empty-string semantics instead. This mirrors how DuckDB's
    ;; `json_extract_string(...) IS NULL` behaves for missing keys
    ;; (parity invariant verified by dev/parity_check.clj).
    (let [col (column-name field)]
      (case op
        :exists? (str col " <> ''")
        :absent? (str col " = ''")))))

(defn- compile-has
  "Array-membership for the native `topics` Array(String) column:
   `[:has :system]` → `has(topics, {pN:String})`."
  [field [_ topic] state]
  (str "has(" (column-name field) ", " (bind! state topic :String) ")"))

(defn- compile-where-entry [[field value] state]
  (cond
    (and (vector? value) (keyword? (first value)))
    (let [op (first value)]
      (cond
        (#{:= :!= :> :< :>= :<=}            op) (compile-comparison field value state)
        (= :in                              op) (compile-membership field (second value) state)
        (#{:contains :icontains
           :starts-with :ends-with}         op) (compile-text field value state)
        (= :matches                         op) (compile-matches field value state)
        (#{:exists? :absent?}               op) (compile-presence field value state)
        (= :has                             op) (compile-has field value state)
        :else (throw (ex-info (str "Unknown operator: " op) {:field field :value value}))))
    (set? value) (compile-membership field value state)
    :else        (compile-comparison field [:= value] state)))

(defn- compile-where [where state]
  (when (and where (seq where))
    (str/join " AND " (map #(compile-where-entry % state) where))))

(defn- duration->seconds [^String s]
  (when-let [[_ n unit] (re-matches #"(\d+)\s*([smhd])" s)]
    (let [n (Long/parseLong n)]
      (case unit "s" n "m" (* n 60) "h" (* n 3600) "d" (* n 86400)))))

(defn- compile-time-bound [kind value state]
  (when value
    (let [op (if (= :since kind) ">" "<")]
      (if-let [secs (duration->seconds value)]
        (str "inst " op " (now() - INTERVAL " secs " SECOND)")
        (let [ph (bind! state value :String)]
          (str "inst " op " parseDateTime64BestEffort(" ph ", 3)"))))))

(defn- compile-order-by [order-by group-by]
  (let [[field direction] (or order-by (if group-by [:count :desc] [:inst :desc]))
        expr (cond
               (= :count field) "count"
               (vector? field)  (field-expr field :String)
               :else            (column-name field))]
    (str expr " " (str/upper-case (name direction)))))

(defn compile-log-sql
  "Compile a validated filter-map to `{:sql :params}` against <db>.logs.
   `params` is `{name {:type Kw :value String}}`."
  [filter-map database]
  (let [state   (new-params)
        {:keys [where since until limit order-by group-by count?]
         :or   {limit 100}} filter-map
        clauses (->> [(compile-where where state)
                      (compile-time-bound :since since state)
                      (compile-time-bound :until until state)]
                     (remove str/blank?) (remove nil?))
        where-sql (when (seq clauses) (str " WHERE " (str/join " AND " clauses)))
        select-cols (cond
                      count?   "count() AS count"
                      group-by (str (column-name group-by) ", count() AS count")
                      :else    "*")
        group-sql (when group-by (str " GROUP BY " (column-name group-by)))
        order-sql (when-not count? (str " ORDER BY " (compile-order-by order-by group-by)))
        limit-sql (when-not count? (str " LIMIT " limit))
        sql (str "SELECT " select-cols " FROM " database ".logs"
                 (or where-sql "") (or group-sql "") (or order-sql "") (or limit-sql "")
                 " FORMAT JSONEachRow")]
    {:sql sql :params (:params @state)}))

;;; ============================================================================
;;; Response parsing — JSONEachRow → vector of maps
;;; ============================================================================

(def ^:private read-mapper
  (jsonista/object-mapper {:decode-key-fn keyword}))

(defn- parse-rows
  "Parse a CH JSONEachRow body into a vector of keyword-keyed maps."
  [^String body]
  (->> (str/split-lines body)
       (remove str/blank?)
       (mapv #(jsonista/read-value % read-mapper))))

(defn- parse-nested-json
  "CH stores data/ctx/value columns as JSON strings; parse them here so the
   read shape matches the DuckDB impl."
  [keys-to-parse row]
  (reduce (fn [r k]
            (if (string? (get r k))
              (try (update r k json/<-json) (catch Throwable _ r))
              r))
          row
          keys-to-parse))

;;; ============================================================================
;;; Log query execution
;;; ============================================================================

(defn- execute-log-query
  [client config {:keys [sql params]}]
  (let [body (post-with-params! client config sql params)]
    (->> (parse-rows body)
         (mapv #(parse-nested-json [:data :ctx] %)))))

(defn- parse-count
  "CH returns count() as a JSON number when 64bit_integers setting is off."
  [x]
  (cond (integer? x) x
        (string? x)  (try (Long/parseLong x) (catch Throwable _ nil))
        :else        nil))

(defn- execute-log-count
  [client config compiled]
  (let [rows (execute-log-query client config compiled)]
    (some-> rows first :count parse-count)))

(defn- execute-log-group
  [client config group-key compiled]
  (let [rows (execute-log-query client config compiled)
        col  (keyword (kebab->snake group-key))]
    (mapv (fn [r] {group-key (get r col) :count (parse-count (:count r))}) rows)))

;;; ============================================================================
;;; Audit reader queries
;;;
;;; All five readers implemented (CH's argMax aggregator handles the
;;; "latest value per attribute as-of ts" semantics elegantly; the previous
;;; audit/clickhouse.clj stubbed these as :not-implemented).
;;; ============================================================================

(defn- read-entity-event-row [r]
  {:track          :entity
   :seq            (:seq r)
   :ts             (:ts r)
   :tenant-xid     (:tenant_xid r)
   :record-xid     (:record_xid r)
   :entity-xid     (:entity_xid r)
   :attribute-xid  (:attribute_xid r)
   :value          (try (json/<-json (:value r)) (catch Throwable _ (:value r)))
   :op             (:op r)
   :actor          (:actor_xid r)
   :request        (:request_id r)
   :scope          (:scope_xid r)
   :txid           (:txid r)})

(defn- read-relation-event-row [r]
  {:track        :relation
   :seq          (:seq r)
   :ts           (:ts r)
   :tenant-xid   (:tenant_xid r)
   :relation-xid (:relation_xid r)
   :from-xid     (:from_xid r)
   :to-xid       (:to_xid r)
   :op           (:op r)
   :actor        (:actor_xid r)
   :request      (:request_id r)
   :scope        (:scope_xid r)
   :txid         (:txid r)})

(defn- compile-audit-where
  "Build a CH WHERE clause + params for audit reads. Returns `[sql params]`."
  [state {:keys [record-xid between cursor seq tenant]}]
  (let [parts (cond-> []
                record-xid (conj (str "record_xid = " (bind! state record-xid :String)))
                between    (into [(str "ts >= parseDateTime64BestEffort("
                                       (bind! state (inst->ch-string (first between)) :String)
                                       ", 3)")
                                  (str "ts <= parseDateTime64BestEffort("
                                       (bind! state (inst->ch-string (second between)) :String)
                                       ", 3)")])
                cursor     (conj (str "ts > parseDateTime64BestEffort("
                                      (bind! state (inst->ch-string cursor) :String)
                                      ", 3)"))
                seq        (conj (str "seq > " (bind! state seq :Int64)))
                tenant     (conj (str "tenant_xid = " (bind! state tenant :String))))]
    (str/join " AND " parts)))

(defn- query-audit-events
  [client config track opts limit order]
  (let [table (if (= track :relation) "audit_relation" "audit_entity")
        state (new-params)
        where (compile-audit-where state opts)
        order (or order "ts DESC")
        limit-sql (when limit (str " LIMIT " (long limit)))
        sql (str "SELECT * FROM " (:database config) "." table
                 (when (seq where) (str " WHERE " where))
                 " ORDER BY " order (or limit-sql "")
                 " FORMAT JSONEachRow")
        body (post-with-params! client config sql (:params @state))
        rows (parse-rows body)
        reader (if (= track :relation) read-relation-event-row read-entity-event-row)]
    (mapv reader rows)))

(defn- query-get-at
  "CH idiom: argMax(value, ts) GROUP BY attribute_xid for latest value
   per attribute as-of `at`."
  [client config record-xid at tenant include-deleted?]
  (let [state (new-params)
        rec-ph (bind! state record-xid :String)
        at-ph  (bind! state (inst->ch-string at) :String)
        tenant-clause (when tenant
                        (str " AND tenant_xid = " (bind! state tenant :String)))
        sql (str "SELECT attribute_xid,"
                 " argMax(value, ts) AS value,"
                 " argMax(op, ts) AS op"
                 " FROM " (:database config) ".audit_entity"
                 " WHERE record_xid = " rec-ph
                 " AND ts <= parseDateTime64BestEffort(" at-ph ", 3)"
                 (or tenant-clause "")
                 " GROUP BY attribute_xid"
                 " FORMAT JSONEachRow")
        body (post-with-params! client config sql (:params @state))
        rows (parse-rows body)
        deleted? (some #(= "__delete__" (:attribute_xid %)) rows)
        attrs (->> rows
                   (remove #(= "__delete__" (:attribute_xid %)))
                   (reduce (fn [acc r]
                             (assoc acc (keyword (:attribute_xid r))
                                    (try (json/<-json (:value r))
                                         (catch Throwable _ (:value r)))))
                           {}))]
    (if (and deleted? (not include-deleted?)) nil (not-empty attrs))))

(defn- query-diff
  [client config record-xid from-ts to-ts tenant]
  (let [before  (query-get-at client config record-xid from-ts tenant true)
        after   (query-get-at client config record-xid to-ts   tenant true)
        ks      (into #{} (concat (keys (or before {})) (keys (or after {}))))
        changed (vec (sort (filter (fn [k] (not= (get before k) (get after k))) ks)))]
    {:before (or before {}) :after (or after {}) :changed changed}))

;;; ============================================================================
;;; Transport — bounded queue + writer thread + HttpClient
;;; ============================================================================

(defprotocol TransportOps
  (close-transport! [this])
  (transport-stats [this])
  (transport-client [this]))

(deftype Transport [^ArrayBlockingQueue queue
                    ^HttpClient client
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
    nil)
  (transport-stats [_]
    {:queued   (.size queue)
     :written  (.get written)
     :dropped  (.get dropped)
     :batches  (.get batches)
     :running? (.get running?)})
  (transport-client [_] client))

(defn- make-transport ^Transport [^HttpClient client config]
  (let [queue     (ArrayBlockingQueue. (int (:buffer-size config)))
        running?  (AtomicBoolean. true)
        written   (AtomicLong. 0)
        dropped   (AtomicLong. 0)
        batches   (AtomicLong. 0)
        state     {:queue queue :client client :written written :dropped dropped
                   :batches batches :running? running? :config config}
        thread    (doto (Thread. ^Runnable (fn [] (log-writer-loop! state))
                                  "synthigy-observability-ch-writer")
                    (.setDaemon true)
                    .start)]
    (->Transport queue client written dropped batches running? thread config)))

;;; ============================================================================
;;; ClickHouseObservability — one record, both protocols
;;; ============================================================================

(defn- row-count [client config table]
  (let [sql (str "SELECT count() AS c FROM " (:database config) "." table
                 " FORMAT JSONEachRow")
        rows (parse-rows (post-with-params! client config sql {}))]
    (some-> rows first :c parse-count)))

(defrecord ClickHouseObservability [^Transport tp config]
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
    (let [compiled (compile-log-sql opts (:database config))
          client (transport-client tp)]
      (cond
        count?   (execute-log-count client config compiled)
        group-by (execute-log-group client config group-by compiled)
        :else    (execute-log-query client config compiled))))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [opts (cond-> {:order-by [:inst :asc] :limit (or limit 100)}
                 cursor     (assoc :since cursor)
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level))
          rows (store/search this opts)
          next-cursor (some-> (last rows) :inst)]
      {:rows rows :next-cursor (or next-cursor cursor)}))

  (clear! [_]
    ;; CH MergeTree doesn't support TRUNCATE on partitioned tables without
    ;; the appropriate setting; ALTER ... DELETE is async + heavy. For dev
    ;; convenience we DROP + recreate the logs table.
    (exec-ddl! (transport-client tp) config
               (str "DROP TABLE IF EXISTS " (:database config) ".logs"))
    (exec-ddl! (transport-client tp) config (logs-ddl (:database config)))
    nil)

  (health [_]
    (let [s (transport-stats tp) client (transport-client tp)]
      (merge s
             {:up?      (boolean (:running? s))
              :backend  :clickhouse
              :url      (:url config)
              :database (:database config)
              :rows     (try (row-count client config "logs") (catch Throwable _ nil))})))

  (snapshot [_]
    ;; Durable backend — no transient state to hand off.
    nil)

  audit/AuditProvider

  ;; Per-entity/relation audit opt-in is frozen at enqueue (substrate trigger
  ;; `audit` flag) and applied by the drainer — the provider no longer
  ;; re-evaluates the mutable policy at drain time (which raced the write and
  ;; dropped committed records). Only the system gate remains. Mirrors DuckDB.
  (write-entity-deltas! [_ envelopes]
    (let [rows (when (audit/persistence-enabled?)
                 (vec (envelope->entity-rows envelopes)))]
      (when (seq rows)
        (insert-entity-rows! (transport-client tp) config rows))))

  (write-relation-deltas! [_ envelopes]
    (let [rows (when (audit/persistence-enabled?)
                 (vec (envelope->relation-rows envelopes)))]
      (when (seq rows)
        (insert-relation-rows! (transport-client tp) config rows))))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (query-get-at (transport-client tp) config record-xid at tenant
                  (boolean include-deleted?)))

  (events [_ {:keys [limit track] :or {track :entity} :as opts}]
    (query-audit-events (transport-client tp) config track opts limit nil))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (query-diff (transport-client tp) config record-xid from-ts to-ts tenant))

  (timeline [_ {:keys [group-by limit] :or {group-by :request} :as opts}]
    (let [evs (query-audit-events (transport-client tp) config :entity opts limit nil)
          kf  (case group-by
                :request :request :actor :actor :scope :scope :request)]
      (clojure.core/group-by kf evs)))

  (since [_ {:keys [limit track] sq :seq :or {track :entity} :as opts}]
    (query-audit-events (transport-client tp) config track opts limit
                        (if sq "seq ASC" "ts ASC"))))

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

;;; ============================================================================
;;; Lifecycle — setup, start, stop, cleanup
;;; ============================================================================

(defn setup
  "One-time setup: apply CH schema (database + 3 tables, all idempotent
   IF NOT EXISTS). Patcho tracks completion in the lifecycle store."
  []
  (let [cfg    (env-config)
        client (new-client cfg)]
    (apply-schema! client cfg)
    (log/info {:id ::setup-complete
               :data {:action :setup-complete :subject :observability
                      :backend :clickhouse :url (:url cfg) :database (:database cfg)}}
              "Observability schema applied (ClickHouse)")))

(defn- drain-snapshot!
  "Drain the outgoing store's buffered signals into the durable record
   before swapping. No-op for nil / durable backends (which return nil
   from `snapshot`)."
  [outgoing incoming]
  (let [signals (try (store/snapshot outgoing) (catch Throwable _ nil))
        n       (count (or signals []))]
    (when (pos? n)
      (doseq [sig signals]
        (try (store/write-signal! incoming sig) (catch Throwable _)))
      (log/info {:id ::ring-drained
                 :data {:action :written :subject :observability
                        :backend :clickhouse :signals n}}
                (str "Drained " n " buffered signal(s) into ClickHouse")))))

(defn start
  "Open a shared HttpClient, start the log writer, bind both dynvars,
   install the audit policy watch.

   Swaps `*log-store*` to the ClickHouse record FIRST (so new signals land
   durably at once), then drains the previously-bound store (typically the
   default RingLogStore) into it — boot-time signals land in the durable
   backend too."
  []
  (let [cfg      (env-config)
        client   (new-client cfg)
        _        (apply-schema! client cfg)
        tp       (make-transport client cfg)
        rec      (->ClickHouseObservability tp cfg)
        outgoing @#'store/*log-store*]
    (log/info {:id ::starting
               :data {:action :starting :subject :observability
                      :backend :clickhouse :url (:url cfg) :database (:database cfg)}}
              "Starting observability substrate (ClickHouse)")
    ;; Swap FIRST so every new signal lands durably immediately; the old ring
    ;; is then frozen and we drain its final contents afterward. Drained boot
    ;; signals carry their original :inst, so they still sort correctly even
    ;; though inserted after some live signals. See the DuckDB backend for the
    ;; full rationale.
    (alter-var-root #'store/*log-store* (constantly rec))
    (drain-snapshot! outgoing rec)
    (alter-var-root #'audit/*audit-provider* (constantly rec))
    (audit/recompile-policy! (dataset/deployed-model))
    (dataset/add-model-watch!
      model-watch-key
      (fn [_k _ref _old new-model] (audit/recompile-policy! new-model)))
    (reset! state {:record rec :transport tp :client client :config cfg})
    (log/info {:id ::started
               :data {:action :started :subject :observability :backend :clickhouse}}
              "Observability substrate started (ClickHouse)")))

(defn stop
  "Reverse start: drain writer, rebind `*log-store*` to a fresh
   RingLogStore (so the queryable surface survives backend teardown),
   unbind the audit provider, remove the watch."
  []
  (log/info {:id ::stopping
             :data {:action :stopping :subject :observability :backend :clickhouse}}
            "Stopping observability substrate (ClickHouse)")
  (dataset/remove-model-watch! model-watch-key)
  (audit/recompile-policy! nil)
  (when-let [{:keys [transport]} @state]
    (try (close-transport! transport) (catch Throwable _)))
  (alter-var-root #'store/*log-store* (constantly (store/create-ring)))
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (reset! state nil)
  (log/info {:id ::stopped
             :data {:action :stopped :subject :observability :backend :clickhouse}}
            "Observability substrate stopped (ClickHouse)"))

(defn cleanup
  "Drop the substrate's tables. Reverses setup. Leaves the CH database in
   place; operator drops the database separately if they want a true wipe."
  []
  (let [cfg    (env-config)
        client (new-client cfg)]
    (drop-schema! client cfg)
    (log/info {:id ::cleanup-complete
               :data {:action :cleanup-complete :subject :observability
                      :backend :clickhouse :database (:database cfg)}}
              "Observability schema dropped (ClickHouse)")))

;;; ============================================================================
;;; Module registration
;;; ============================================================================

(patch/current-version :synthigy/observability "1.0.0")

(lifecycle/register-module!
  :synthigy/observability
  {:depends-on [:synthigy/log :synthigy/substrate]
   :doc "Log/audit analytics sink (ClickHouse)"
   :setup   setup
   :start   start
   :stop    stop
   :cleanup cleanup})
