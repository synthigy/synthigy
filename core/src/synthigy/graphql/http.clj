(ns synthigy.graphql.http
  "Core HTTP GraphQL handler.

   Execution pipeline using Lacinia's public APIs:
   1. Parse request (extract query, variables, operation)
   2. Parse query (parser/parse-query) with caching
   3. Early subscription detection
   4. Prepare & validate (parser/prepare-with-query-variables, validator/validate)
   5. Execute (executor/execute-query + ResolverResult handling)
   6. Format response (JSON with proper status codes)"
  (:require
    [clojure.data.json :as json]
    [clojure.string :as str]
    [com.walmartlabs.lacinia.constants :as constants]
    [com.walmartlabs.lacinia.executor :as executor]
    [com.walmartlabs.lacinia.parser :as parser]
    [com.walmartlabs.lacinia.resolve :as resolve]
    [com.walmartlabs.lacinia.validator :as validator]
    [synthigy.graphql.cache :as cache]
    [synthigy.graphql.core :as core]
    [synthigy.graphql.tracing :as tracing]))

;;; ============================================================================
;;; Request Parsing
;;; ============================================================================

(defn- read-body
  "Read request body as string. Handles InputStream or String."
  [body]
  (when body
    (if (string? body)
      body
      (slurp body))))

(defn- parse-json
  "Parse JSON string to map with keyword keys. Returns nil on failure."
  [s]
  (when-not (str/blank? s)
    (try
      (json/read-str s :key-fn keyword)
      (catch Exception _ nil))))

(defn- validate-params
  "Validate GraphQL parameter types per spec.
   Returns error response or nil if valid."
  [{:keys [query variables]}]
  (cond
    (and (some? query) (not (string? query)))
    (core/error-response "Parameter 'query' must be a string")

    (and (some? variables) (not (map? variables)))
    (core/error-response "Parameter 'variables' must be an object")

    :else nil))

(defn parse-request
  "Extract GraphQL query, variables, and operation name from Ring request.

   Supports:
   - POST application/json: {\"query\": \"...\", \"variables\": {...}}
   - POST application/graphql: raw query string, vars from query params
   - GET: query/variables/operationName in query params

   Validates per GraphQL over HTTP spec:
   - POST must have Content-Type header
   - Parameter types are validated

   Returns map with :query, :variables, :operation-name
   Or error via core/error-response"
  [{:keys [request-method body headers params]
    :or {params {}}}]
  (let [content-type (core/parse-content-type (get headers "content-type"))]
    (cond
      ;; POST without Content-Type header (spec: MUST return 4xx)
      (and (= :post request-method)
           (nil? (get headers "content-type")))
      (core/error-response 400 "Missing Content-Type header")

      ;; POST with JSON body (most common)
      (and (= :post request-method)
           (= :application/json content-type))
      (let [body-str (read-body body)]
        (if (str/blank? body-str)
          (core/error-response 400 "Missing request body")
          (if-let [payload (parse-json body-str)]
            (let [parsed {:query (:query payload)
                          :variables (:variables payload)
                          :operation-name (:operationName payload)}]
              (or (validate-params parsed) parsed))
            (core/error-response 400 "Invalid JSON in request body"))))

      ;; POST with raw GraphQL query
      (and (= :post request-method)
           (= :application/graphql content-type))
      (let [parsed {:query (read-body body)
                    :variables (parse-json (:variables params))
                    :operation-name (:operationName params)}]
        (or (validate-params parsed) parsed))

      ;; GET request - everything in query params
      (= :get request-method)
      (let [parsed {:query (:query params)
                    :variables (parse-json (:variables params))
                    :operation-name (:operationName params)}]
        (or (validate-params parsed) parsed))

      ;; POST with wrong content type
      (= :post request-method)
      (core/error-response 415
                           (str "Unsupported Content-Type: " (get headers "content-type")
                                ". Expected application/json or application/graphql"))

      ;; Other methods
      :else
      (core/error-response 405 "Method not allowed. Use GET or POST."))))

;;; ============================================================================
;;; Query Parsing (Step 2-3)
;;; ============================================================================

(defn- do-parse-query
  "Parse query string using Lacinia parser.
   Returns parsed query or error response."
  [schema query operation-name timing-start]
  (try
    (parser/parse-query schema query operation-name timing-start)
    (catch Exception e
      (core/error-response (str "Parse error: " (.getMessage e))))))

(defn parse-query
  "Parse GraphQL query with caching.

   Args:
     schema         - Compiled Lacinia schema
     query          - Query string
     operation-name - Operation name (may be nil)
     cache          - ParsedQueryCache (may be nil)
     timing-start   - Lacinia timing start (for tracing)

   Returns:
     {:parsed <parsed-query>} or {:parsed <cached> :cached? true} or error response"
  [schema query operation-name cache timing-start]
  (if (str/blank? query)
    (core/error-response "Missing or blank 'query' field")
    (let [cache-key (core/cache-key query operation-name)]
      (if-let [cached (cache/cache-get cache cache-key)]
        {:parsed cached
         :cached? true}
        (let [parsed (do-parse-query schema query operation-name timing-start)]
          (if (core/early-error? parsed)
            parsed
            (do
              (cache/cache-put cache cache-key parsed)
              {:parsed parsed
               :cached? false})))))))

;;; ============================================================================
;;; Operation Type Checks (Step 4)
;;; ============================================================================

(defn- get-operation-type
  "Extract operation type from parsed query."
  [parsed-query]
  (-> parsed-query parser/operations :type))

(defn check-operation
  "Validate operation is allowed for the request method.
   Per GraphQL over HTTP spec:
   - Subscriptions not allowed over HTTP
   - Mutations not allowed over GET (MUST respond 405)

   Args:
     parsed-query   - Parsed GraphQL query
     request-method - HTTP method (:get or :post)

   Returns nil if OK, error response if not allowed."
  [parsed-query request-method]
  (let [op-type (get-operation-type parsed-query)]
    (cond
      (= :subscription op-type)
      (core/error-response 400
                           "Subscriptions not supported over HTTP. Use WebSocket endpoint.")

      (and (= :get request-method) (= :mutation op-type))
      (core/error-response 405
                           "Mutations not allowed over GET. Use POST.")

      :else nil)))

;; Keep old fn for backwards compatibility
(defn check-subscription
  "Reject subscription operations on HTTP endpoint.
   Returns nil if OK, error response if subscription.
   @deprecated Use check-operation instead."
  [parsed-query]
  (when (= :subscription (get-operation-type parsed-query))
    (core/error-response 400
                         "Subscriptions not supported over HTTP. Use WebSocket endpoint.")))

;;; ============================================================================
;;; Prepare & Validate (Step 5)
;;; ============================================================================

(defn prepare-query
  "Inject variables and validate query.

   Args:
     schema       - Compiled Lacinia schema
     parsed-query - From parse-query
     variables    - Variable map (may be nil)

   Returns:
     {:prepared <prepared-query>} or error response"
  [schema parsed-query variables]
  (try
    (let [prepared (parser/prepare-with-query-variables parsed-query variables)
          errors (validator/validate schema prepared {})]
      (if (seq errors)
        (core/errors-response errors)
        {:prepared prepared}))
    (catch Exception e
      (core/error-response (str "Validation error: " (.getMessage e))))))

;;; ============================================================================
;;; Execute (Step 6)
;;; ============================================================================

(defn- await-resolver-result
  "Block until ResolverResult delivers its value.

   ResolverResult is Lacinia's async abstraction. Even sync resolvers
   return through this mechanism."
  [resolver-result]
  (let [p (promise)]
    (resolve/on-deliver! resolver-result #(deliver p %))
    @p))

(defn execute-query
  "Execute prepared query using Lacinia executor.

   Handles ResolverResult abstraction and Throwable results (timeouts).

   Args:
     schema   - Compiled Lacinia schema
     prepared - Prepared query from prepare-query
     context  - Application context for resolvers

   Returns:
     Lacinia result map {:data ... :errors ...}"
  [schema prepared context]
  (try
    (let [;; Build execution context with schema and prepared query
          ;; executor/execute-query takes a single context map containing everything
          exec-context (assoc context
                         constants/parsed-query-key prepared
                         constants/schema-key schema)
          ;; Execute - returns ResolverResult
          resolver-result (executor/execute-query exec-context)
          ;; Await delivery
          result (await-resolver-result resolver-result)]
      ;; Handle Throwable results (since Lacinia 0.36.0, for timeouts)
      ;; Include :data nil to indicate execution error (returns 200 per spec)
      (if (instance? Throwable result)
        {:data nil
         :errors [(core/throwable->error result)]}
        result))
    (catch Throwable t
      ;; Include :data nil to indicate execution error (returns 200 per spec)
      {:data nil
       :errors [(core/throwable->error t)]})))

;;; ============================================================================
;;; Response Formatting (Step 7)
;;; ============================================================================

(defn- extract-max-status
  "Find maximum :status in error extensions."
  [errors]
  (when (seq errors)
    (let [statuses (keep #(get-in % [:extensions :status]) errors)]
      (when (seq statuses)
        (reduce max statuses)))))

(defn- clean-error-status
  "Remove :status from error extensions (internal detail)."
  [error]
  (let [error' (update error :extensions dissoc :status)]
    (if (empty? (:extensions error'))
      (dissoc error' :extensions)
      error')))

(defn format-response
  "Convert GraphQL result to Ring response.

   - Sets Content-Type: application/json
   - Maps error extensions :status to HTTP status
   - Uses 400 for results without :data (parse/validation errors)
   - Cleans internal keys from errors

   Args:
     result  - Lacinia result or early error
     timing  - Optional tracing timing context"
  ([result] (format-response result nil))
  ([result timing]
   (let [;; Handle early errors (from our pipeline)
         early-error? (core/early-error? result)
         errors (if early-error?
                  (::core/errors result)
                  (:errors result))

         ;; Determine status
         explicit-status (::core/status result)
         error-status (extract-max-status errors)
         has-data? (contains? result :data)
         status (or explicit-status
                    error-status
                    (if (or has-data? (not errors)) 200 400))

         ;; Build response body
         body (cond-> {}
                (and (not early-error?) (contains? result :data))
                (assoc :data (:data result))

                errors
                (assoc :errors (mapv clean-error-status errors))

                (and (not early-error?) (:extensions result))
                (assoc :extensions (:extensions result)))

         ;; Add tracing if present
         body (if timing
                (tracing/add-tracing-extension body timing)
                body)]

     {:status status
      :headers {"Content-Type" "application/json"}
      :body (json/write-str body)})))

;;; ============================================================================
;;; Main Execution Pipeline
;;; ============================================================================

(defn execute-pipeline
  "Execute full GraphQL pipeline.

   Steps:
   1. Parse query (with caching)
   2. Check operation type (reject subscriptions, mutations over GET)
   3. Prepare with variables & validate
   4. Execute
   5. Add tracing if enabled

   Args:
     schema-provider - SchemaProvider impl
     query           - Query string
     variables       - Variable map
     operation-name  - Operation name
     context         - App context for resolvers
     opts            - {:cache, :tracing?, :request-method}

   Returns:
     Lacinia result or early error"
  [schema-provider query variables operation-name context
   {:keys [cache tracing? request-method]}]
  (let [schema (core/get-schema schema-provider)
        timing (when tracing? (tracing/create-timing))
        timing-start (when tracing? (tracing/create-lacinia-timing-start))]

    ;; Step 1-2: Parse (with cache)
    (let [parse-start (when tracing? (tracing/start-phase))
          parse-result (parse-query schema query operation-name cache timing-start)
          timing (if (and tracing? (not (:cached? parse-result)))
                   (tracing/record-phase timing :parse parse-start)
                   timing)]

      (if (core/early-error? parse-result)
        parse-result

        (let [{:keys [parsed]} parse-result

              ;; Step 3: Check operation type (subscriptions, mutations over GET)
              op-error (check-operation parsed request-method)]

          (if op-error
            op-error

            ;; Step 4: Prepare & validate
            (let [validate-start (when tracing? (tracing/start-phase))
                  prep-result (prepare-query schema parsed variables)
                  timing (when tracing?
                           (tracing/record-phase timing :validate validate-start))]

              (if (core/early-error? prep-result)
                prep-result

                ;; Step 5: Execute
                (let [{:keys [prepared]} prep-result
                      exec-start (when tracing? (tracing/start-phase))
                      result (execute-query schema prepared context)
                      timing (when tracing?
                               (tracing/record-phase timing :execute exec-start))]

                  ;; Add tracing extension if enabled
                  (if timing
                    (tracing/add-tracing-extension result timing)
                    result))))))))))

;;; ============================================================================
;;; Ring Handler Factory
;;; ============================================================================

(defn handler
  "Create Ring handler for GraphQL HTTP requests.

   Args:
     schema-provider - Schema, function returning schema, or deref-able
     opts            - Options map:
                       :context-fn     - (fn [request] app-context)
                       :cache          - ParsedQueryCache implementation
                       :tracing-header - Header name to enable tracing

   Returns:
     Ring handler (fn [request] response)

   Example:
     (def my-handler
       (handler compiled-schema
                {:context-fn (fn [req] {:db *db* :user (:user req)})
                 :cache (cache/lru-cache 500)
                 :tracing-header \"lacinia-tracing\"}))"
  [schema-provider {:keys [context-fn cache tracing-header]
                    :or {context-fn (constantly {})}}]
  (fn [request]
    (def request request)
    (let [;; Parse HTTP request
          parsed (parse-request request)]
      (if (core/early-error? parsed)
        (format-response parsed)

        (let [{:keys [query variables operation-name]} parsed
              ;; Check tracing header
              tracing? (and tracing-header
                            (get-in request [:headers tracing-header]))
              ;; Build context
              context (cond-> (context-fn request)
                        true (assoc :request request)
                        tracing? (assoc :com.walmartlabs.lacinia/enable-tracing? true))
              ;; Execute pipeline (pass request-method for mutation/GET check)
              result (execute-pipeline schema-provider
                                       query variables operation-name
                                       context
                                       {:cache cache
                                        :tracing? tracing?
                                        :request-method (:request-method request)})]
          (format-response result))))))
