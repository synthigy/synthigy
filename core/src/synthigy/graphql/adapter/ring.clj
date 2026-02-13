(ns synthigy.graphql.adapter.ring
  "Ring adapter - middleware and handler wrappers."
  (:require
    [synthigy.graphql.http :as http]))

(defn wrap-graphql
  "Ring middleware that handles GraphQL at specified path.

   Non-matching requests pass through to next handler.

   Args:
     handler         - Next Ring handler in chain
     path            - GraphQL endpoint path (e.g., \"/graphql\")
     schema-provider - Schema, function, atom, or ref
     opts            - Options for http/handler

   Example:
     (-> app
         (wrap-graphql \"/graphql\" compiled-schema
                       {:context-fn (fn [req] {:user (:user req)})
                        :cache (cache/lru-cache 500)}))"
  [handler path schema-provider opts]
  (let [graphql-handler (http/handler schema-provider opts)]
    (fn [request]
      (if (= (:uri request) path)
        (graphql-handler request)
        (handler request)))))

(defn graphql-handler
  "Create standalone Ring handler for GraphQL.

   For use with routing libraries (Compojure, Reitit, etc.)

   Args:
     schema-provider - Schema, function, atom, or ref
     opts            - Options map:
                       :context-fn     - (fn [request] app-context)
                       :cache          - ParsedQueryCache implementation
                       :tracing-header - Header name to enable tracing

   Example with Compojure:
     (routes
       (POST \"/graphql\" [] (graphql-handler schema opts))
       (GET \"/graphql\" [] (graphql-handler schema opts)))

   Example with Reitit:
     [[\"/graphql\" {:post {:handler (graphql-handler schema opts)}
                     :get {:handler (graphql-handler schema opts)}}]]"
  [schema-provider opts]
  (http/handler schema-provider opts))
