(ns synthigy.graphql
  "ring-jetty (Jetty 12) GraphQL HTTP handler.

   Provides Ring handlers for GraphQL queries and mutations.
   For WebSocket subscriptions, see synthigy.ws."
  (:require
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
  "Ring middleware that handles GraphQL at specified path."
  [next-handler path schema-provider opts]
  (let [gql-handler (handler schema-provider opts)]
    (fn [request]
      (if (= (:uri request) path)
        (gql-handler request)
        (next-handler request)))))
