(ns synthigy.graphql
  "http-kit GraphQL HTTP handler.

   Provides Ring handlers for GraphQL queries and mutations.
   For WebSocket subscriptions, see synthigy.ws."
  (:require
    [clojure.core.async :as async]
    [synthigy.graphql.http :as http]))

(defn handler
  "Ring handler for GraphQL HTTP requests.

   Args:
     schema-provider - Compiled schema, (fn [] schema), or atom/ref
     opts            - Options:
                       :context-fn     - (fn [request] context)
                       :cache          - ParsedQueryCache
                       :tracing-header - Header to enable tracing

   Returns:
     Ring handler (fn [request] response)"
  [schema-provider opts]
  (http/handler schema-provider opts))

(defn wrap-graphql
  "Ring middleware that handles GraphQL at specified path.

   Args:
     next-handler    - Handler for non-GraphQL requests
     path            - GraphQL endpoint path
     schema-provider - Schema provider
     opts            - Handler options

   Returns:
     Ring handler"
  [next-handler path schema-provider opts]
  (let [gql-handler (handler schema-provider opts)]
    (fn [request]
      (if (= (:uri request) path)
        (gql-handler request)
        (next-handler request)))))

(defn async-handler
  "Async Ring handler for http-kit.

   Uses 3-arity Ring async form: (fn [request respond raise])
   Frees http-kit worker thread during execution.

   Args:
     schema-provider - Schema provider
     opts            - Handler options

   Returns:
     Async Ring handler"
  [schema-provider opts]
  (let [sync-handler (handler schema-provider opts)]
    (fn [request respond raise]
      (async/go
        (try
          (respond (sync-handler request))
          (catch Throwable t
            (raise t)))))))
