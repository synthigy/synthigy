(ns synthigy.graphql.adapter.pedestal
  "Pedestal adapter with interceptor chain support.

   Provides both:
   - Single interceptor (simple drop-in replacement)
   - Split interceptors (for auth/middleware insertion between stages)"
  (:require
    [synthigy.graphql.core :as core]
    [synthigy.graphql.http :as http]))

;;; ============================================================================
;;; Single Interceptor (Simple Drop-in)
;;; ============================================================================

(defn graphql-interceptor
  "Single interceptor handling complete GraphQL request.

   Drop-in replacement for lacinia-pedestal interceptor chain.
   Use when you don't need interceptors between parse and execute.

   Args:
     schema-provider - Schema, function, atom, or ref
     opts            - Options map (same as http/handler)

   Example:
     (def routes
       #{[\"/graphql\" :post [(graphql-interceptor schema opts)]
          :route-name ::graphql]})"
  [schema-provider opts]
  (let [handler (http/handler schema-provider opts)]
    {:name ::graphql
     :enter (fn [context]
              (assoc context :response (handler (:request context))))}))

;;; ============================================================================
;;; Split Interceptors (For Middleware Insertion)
;;; ============================================================================

(def parse-request-interceptor
  "Parses HTTP request, extracts GraphQL parameters.

   Adds to request:
   - :graphql/query
   - :graphql/variables
   - :graphql/operation-name

   Short-circuits with error response on parse failure.

   Use this when you need to inspect the query before execution
   (e.g., for query complexity analysis, authorization checks)."
  {:name ::parse-request
   :enter (fn [context]
            (let [parsed (http/parse-request (:request context))]
              (if (core/early-error? parsed)
                (assoc context :response (http/format-response parsed))
                (update context :request assoc
                        :graphql/query (:query parsed)
                        :graphql/variables (:variables parsed)
                        :graphql/operation-name (:operation-name parsed)))))
   :leave (fn [context]
            (update context :request dissoc
                    :graphql/query :graphql/variables :graphql/operation-name))})

(defn context-interceptor
  "Builds application context for resolvers.

   Args:
     context-fn - (fn [request] context-map)

   The context-fn receives the full Pedestal request and should return
   a map that will be passed to all GraphQL resolvers.

   Example:
     (context-interceptor
       (fn [req]
         {:db *db*
          :user (get-in req [::auth/user])
          :roles (get-in req [::auth/roles])}))"
  [context-fn]
  {:name ::app-context
   :enter (fn [context]
            (let [app-ctx (context-fn (:request context))]
              (assoc-in context [:request :graphql/context] app-ctx)))
   :leave (fn [context]
            (update context :request dissoc :graphql/context))})

(defn execute-interceptor
  "Executes GraphQL pipeline with parsed parameters from request.

   Expects in request (from parse-request-interceptor):
   - :graphql/query
   - :graphql/variables
   - :graphql/operation-name
   - :graphql/context (from context-interceptor)

   Args:
     schema-provider - Schema, function, atom, or ref
     opts            - {:cache, :tracing-header}"
  [schema-provider {:keys [cache tracing-header]}]
  {:name ::execute
   :enter (fn [context]
            (let [request (:request context)
                  tracing? (and tracing-header
                                (get-in request [:headers tracing-header]))
                  app-ctx (cond-> (or (:graphql/context request) {})
                            true (assoc :request request)
                            tracing? (assoc :com.walmartlabs.lacinia/enable-tracing? true))
                  result (http/execute-pipeline
                           schema-provider
                           (:graphql/query request)
                           (:graphql/variables request)
                           (:graphql/operation-name request)
                           app-ctx
                           {:cache cache
                            :tracing? tracing?
                            :request-method (:request-method request)})]
              (assoc context :response (http/format-response result))))})

(defn default-interceptors
  "Returns default interceptor chain for GraphQL endpoint.

   Provides insertion points for custom interceptors:

   [parse-request-interceptor]      <- Query parsed, available for inspection
   ;; INSERT: auth, rate-limiting, query complexity check
   [context-interceptor]            <- Build resolver context
   ;; INSERT: request logging, metrics
   [execute-interceptor]            <- Execute and respond

   Args:
     schema-provider - Schema, function, atom, or ref
     opts            - Options map:
                       :context-fn     - (fn [request] context-map)
                       :cache          - ParsedQueryCache implementation
                       :tracing-header - Header name to enable tracing

   Example:
     (def graphql-interceptors
       (default-interceptors compiled-schema
                             {:context-fn (fn [req] {:user (::user req)})
                              :cache (cache/lru-cache 500)
                              :tracing-header \"lacinia-tracing\"}))

     ;; Or with custom auth interceptor:
     (def graphql-interceptors
       (let [[parse ctx execute] (default-interceptors schema opts)]
         [parse
          my-auth-interceptor
          ctx
          execute]))"
  [schema-provider {:keys [context-fn cache tracing-header]
                    :or {context-fn (constantly {})}}]
  [parse-request-interceptor
   (context-interceptor context-fn)
   (execute-interceptor schema-provider {:cache cache
                                          :tracing-header tracing-header})])

;;; ============================================================================
;;; Migration Helper
;;; ============================================================================

(defn from-lacinia-pedestal
  "Migration helper - creates interceptors matching lacinia-pedestal API.

   Args:
     compiled-schema - Lacinia compiled schema (or function returning it)
     app-context     - Static app context map (or nil)
     options         - {:parsed-query-cache <cache>}

   This provides API compatibility with lacinia-pedestal's default-interceptors.

   Example:
     ;; Before (lacinia-pedestal):
     (lp/default-interceptors schema app-context {:parsed-query-cache cache})

     ;; After (synthigy.graphql):
     (from-lacinia-pedestal schema app-context {:parsed-query-cache cache})"
  ([compiled-schema]
   (from-lacinia-pedestal compiled-schema nil nil))
  ([compiled-schema app-context]
   (from-lacinia-pedestal compiled-schema app-context nil))
  ([compiled-schema app-context options]
   (default-interceptors
     compiled-schema
     {:context-fn (constantly (or app-context {}))
      :cache (:parsed-query-cache options)
      :tracing-header "lacinia-tracing"})))
