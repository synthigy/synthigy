(ns synthigy.graphql
  "Pedestal GraphQL HTTP interceptors.

   Provides both single interceptor and split interceptors for
   inserting auth/middleware between parse and execute stages."
  (:require
    [synthigy.graphql.core :as core]
    [synthigy.graphql.http :as http]))

;;; ============================================================================
;;; Single Interceptor
;;; ============================================================================

(defn interceptor
  "Single interceptor handling complete GraphQL request.

   Args:
     schema-provider - Compiled schema, (fn [] schema), or atom/ref
     opts            - Options:
                       :context-fn     - (fn [request] context)
                       :cache          - ParsedQueryCache
                       :tracing-header - Header to enable tracing

   Returns:
     Pedestal interceptor"
  [schema-provider opts]
  (let [handler (http/handler schema-provider opts)]
    {:name ::graphql
     :enter (fn [context]
              (assoc context :response (handler (:request context))))}))

;;; ============================================================================
;;; Split Interceptors
;;; ============================================================================

(def parse-request-interceptor
  "Parses HTTP request, extracts GraphQL parameters.

   Adds to request:
   - :graphql/query
   - :graphql/variables
   - :graphql/operation-name

   Short-circuits with error response on parse failure."
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
     context-fn - (fn [request] context-map)"
  [context-fn]
  {:name ::app-context
   :enter (fn [context]
            (let [app-ctx (context-fn (:request context))]
              (assoc-in context [:request :graphql/context] app-ctx)))
   :leave (fn [context]
            (update context :request dissoc :graphql/context))})

(defn execute-interceptor
  "Executes GraphQL pipeline with parsed parameters.

   Expects in request (from parse-request-interceptor):
   - :graphql/query
   - :graphql/variables
   - :graphql/operation-name
   - :graphql/context (from context-interceptor)

   Args:
     schema-provider - Schema provider
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

   Args:
     schema-provider - Schema provider
     opts            - Options:
                       :context-fn     - (fn [request] context-map)
                       :cache          - ParsedQueryCache
                       :tracing-header - Header to enable tracing

   Returns:
     Vector of [parse-request-interceptor context-interceptor execute-interceptor]"
  [schema-provider {:keys [context-fn cache tracing-header]
                    :or {context-fn (constantly {})}}]
  [parse-request-interceptor
   (context-interceptor context-fn)
   (execute-interceptor schema-provider {:cache cache
                                          :tracing-header tracing-header})])
