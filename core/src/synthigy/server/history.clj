(ns synthigy.server.history
  "Ring handler for the `/history` endpoint — audit substrate query surface.

  Five ops, xid-native wire, same auth/selections grammar as `/data`. The
  endpoint exists only when the observability substrate is loaded
  (`:synthigy/observability`, DuckDB or ClickHouse) so `*audit-provider*`
  is bound; without it, every op returns 404 because the feature
  genuinely doesn't exist in that deployment.

  See `project_history_substrate_contract.md` for the locked op vocabulary
  and operational guards."
  (:require
    [clojure.string :as str]
    [patcho.lifecycle :as lifecycle]
    [synthigy.audit :as audit]
    [synthigy.dataset.id :as id]
    [synthigy.iam.access :as access]
    [synthigy.json :as json]
    [synthigy.log :as log]
    [synthigy.server.auth :as auth]))

;; ============================================================================
;; Request parsing + response shaping
;; ============================================================================

(defn- parse-body
  "Parse the /history request body. Wire-shape decoder: keys are preserved
   as strings (`json/read-str-raw`) so a json-typed selection containing a
   key like \"Movie Actor\" passes through verbatim instead of being
   keywordized by `pkey-fn`'s `[_\\s]+ → -` regex.

   Returns the raw map (string-keyed). Each `execute-op` defmethod uses
   `wire-get` to pull the named opt keys it consumes."
  [request]
  (try
    (let [body-val (:body request)
          body-str (cond
                     (nil? body-val) nil
                     (string? body-val) body-val
                     :else (slurp body-val))]
      (when (and body-str (not= body-str ""))
        (json/read-str-raw body-str)))
    (catch Throwable e
      (log/warn {:id ::body-parse-failed} (.getMessage e))
      nil)))

(defn- wire-get
  "Pull `wire-key` from a string-keyed opts map, accepting both kebab-case
   (`record-xid`) and snake_case (`record_xid`) wire variants. JS callers
   typically send snake_case; Clojure callers and the test suite use
   kebab. Both land on the same internal value. `wire-key` should be
   given in kebab form — `wire-get` derives the snake fallback."
  [opts wire-key]
  (or (get opts wire-key)
      (get opts (str/replace wire-key #"-" "_"))))

(defn- json-response
  [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/->json body)})

(defn- error
  [status code msg & [extra]]
  (json-response status (merge {:error {:message msg :code code}} extra)))

;; ============================================================================
;; Operational guards (per contract memo)
;; ============================================================================

(def ^:private default-limit 100)
(def ^:private max-limit 10000)
(def ^:private default-lower-window-ms (* 30 24 60 60 1000)) ;; 30 days

(defn- now-iso []
  (.toString (java.time.Instant/now)))

(defn- ->iso
  "Coerce any timestamp-shaped value (java.util.Date, java.time.Instant,
   or string) to an ISO-8601 string suitable for TEXT-column comparisons.
   jsonista parses ISO-string JSON values into java.util.Date; the audit
   substrate stores them as TEXT, so we have to reverse the coercion."
  [v]
  (cond
    (nil? v) nil
    (string? v) v
    (instance? java.util.Date v) (.toString (.toInstant ^java.util.Date v))
    (instance? java.time.Instant v) (.toString v)
    :else (str v)))

(defn- thirty-days-ago-iso []
  (.toString (.minusMillis (java.time.Instant/now) default-lower-window-ms)))

(defn- coerce-limit
  [n]
  (-> (or n default-limit) long
      (max 1)
      (min max-limit)))

(defn- normalize-between
  "Returns [t1 t2] strings. Upper bound is required by the contract; if the
   caller omits the upper bound we reject. Lower defaults to now-30d."
  [between]
  (let [[t1 t2] (if (sequential? between) between [nil nil])
        t1 (->iso t1)
        t2 (->iso t2)]
    (cond
      (nil? t2)
      (throw (ex-info "between upper bound is required"
                      {:code "BETWEEN_UPPER_REQUIRED"}))

      :else
      [(or t1 (thirty-days-ago-iso)) t2])))

;; ============================================================================
;; Op dispatch
;; ============================================================================

(defmulti execute-op
  "Dispatch on the wire `:op` keyword. Each method invokes the bound
   `audit/*audit-provider*` after coercing inputs and applying operational
   guards."
  (fn [_provider op _opts] op))

(defmethod execute-op :default
  [_ op _]
  (throw (ex-info (str "Unknown history op: " op)
                  {:code "UNKNOWN_OP" :op op})))

(defmethod execute-op "get-at"
  [provider _ opts]
  (let [record-xid       (wire-get opts "record-xid")
        at               (wire-get opts "at")
        tenant           (wire-get opts "tenant")
        include-deleted? (wire-get opts "include-deleted?")]
    (when (str/blank? (str record-xid))
      (throw (ex-info "record-xid is required" {:code "RECORD_XID_REQUIRED"})))
    (audit/get-at provider {:record-xid record-xid
                            :at (or (->iso at) (now-iso))
                            :tenant tenant
                            :include-deleted? include-deleted?})))

(defmethod execute-op "events"
  [provider _ opts]
  (let [record-xid (wire-get opts "record-xid")
        between    (wire-get opts "between")
        tenant     (wire-get opts "tenant")
        limit      (wire-get opts "limit")
        track      (wire-get opts "track")
        [t1 t2]    (normalize-between between)]
    (audit/events provider {:record-xid record-xid
                            :between [t1 t2]
                            :tenant tenant
                            :limit (coerce-limit limit)
                            :track (some-> track keyword)})))

(defmethod execute-op "diff"
  [provider _ opts]
  (let [record-xid (wire-get opts "record-xid")
        from-ts    (->iso (wire-get opts "from-ts"))
        to-ts      (->iso (wire-get opts "to-ts"))
        tenant     (wire-get opts "tenant")]
    (when (str/blank? (str record-xid))
      (throw (ex-info "record-xid is required" {:code "RECORD_XID_REQUIRED"})))
    (when (or (str/blank? (str from-ts)) (str/blank? (str to-ts)))
      (throw (ex-info "from-ts and to-ts are required"
                      {:code "DIFF_TIMESTAMPS_REQUIRED"})))
    (audit/diff provider {:record-xid record-xid
                          :from-ts from-ts
                          :to-ts to-ts
                          :tenant tenant})))

(defmethod execute-op "timeline"
  [provider _ opts]
  (let [between  (wire-get opts "between")
        group-by (wire-get opts "group-by")
        tenant   (wire-get opts "tenant")
        limit    (wire-get opts "limit")
        [t1 t2]  (normalize-between between)]
    (audit/timeline provider {:between [t1 t2]
                              :group-by (or (some-> group-by keyword) :request)
                              :tenant tenant
                              :limit (coerce-limit limit)})))

(defmethod execute-op "since"
  [provider _ opts]
  (let [cursor (->iso (wire-get opts "cursor"))
        tenant (wire-get opts "tenant")
        limit  (wire-get opts "limit")
        track  (wire-get opts "track")]
    (when (str/blank? (str cursor))
      (throw (ex-info "cursor is required" {:code "CURSOR_REQUIRED"})))
    (audit/since provider {:cursor cursor
                           :tenant tenant
                           :limit (coerce-limit limit)
                           :track (some-> track keyword)})))

;; ============================================================================
;; Tenant pruning
;; ============================================================================

(defn- principal-tenant-xid
  "Derive the request's tenant scope. MVP per Q11: the install-level tenant
   is implicit and the caller can't escape it — the install's primary dataset
   xid is the tenant boundary. Mirrors what `set-audit-context` writes on the
   write side, so reads filter exactly what writes wrote. When the principal
   projection grows :tenant-xid for multi-dataset installs, this can pull
   from there instead."
  [_principal]
  (some-> (id/data :dataset/id) str))

;; ============================================================================
;; Handler
;; ============================================================================

(defn handler
  "Ring handler. Same auth as /data. Body shape:
     {\"op\": \"get-at\" | \"events\" | \"diff\" | \"timeline\" | \"since\",
      \"opts\": { ... op-specific ... }}"
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (cond
      (nil? audit/*audit-provider*)
      (error 404 "HISTORY_UNAVAILABLE"
             "No audit provider loaded — /history endpoint is not active")

      (and iam-active? (not iam))
      (error 401 "UNAUTHORIZED" "Unauthorized")

      :else
      (let [body (parse-body request)
            op   (some-> (get body "op") str)
            opts (or (get body "opts") {})]
        (cond
          (str/blank? op)
          (error 400 "OP_REQUIRED" "op is required")

          :else
          (try
            (access/with-principal (:principal iam)
              (let [tenant   (principal-tenant-xid (:principal iam))
                    full     (cond-> opts
                               tenant (assoc :tenant tenant))
                    result   (execute-op audit/*audit-provider* op full)]
                (json-response 200 {:op op :result result})))
            (catch clojure.lang.ExceptionInfo e
              (let [d (ex-data e)
                    status (or (:status d) 400)]
                (log/info {:id ::op-rejected
                           :data {:action :rejecting :subject :history :op op
                                  :code (:code d)}}
                          (.getMessage e))
                (error status (or (:code d) "BAD_REQUEST") (.getMessage e))))
            (catch Throwable e
              (log/error! {:id ::op-failed
                           :data {:action :running :subject :history :op op}} e)
              (error 500 "INTERNAL_ERROR" (.getMessage e)))))))))
