(ns synthigy.graphql.ring
  "Clean Ring handler for GraphQL HTTP - use with any router.

   This is the building block for integrating Synthigy GraphQL into your
   own stack (Reitit, Compojure, Bidi, etc.).

   ## With Reitit

   ```clojure
   (require '[reitit.ring :as ring])
   (require '[synthigy.graphql.ring :as graphql])
   (require '[synthigy.graphql.cache :as cache])

   (def graphql-handler
     (graphql/handler
       (fn [] @my-schema)  ; schema provider
       {:context-fn (fn [request]
                      {:db @db
                       :user (:user request)})
        :cache (cache/lru-cache 500)}))

   (def app
     (ring/ring-handler
       (ring/router
         [[\"/graphql\" {:get graphql-handler
                        :post graphql-handler}]
          [\"/api\" ...]])))
   ```

   ## With Compojure

   ```clojure
   (require '[compojure.core :refer [defroutes GET POST]])
   (require '[synthigy.graphql.ring :as graphql])

   (def graphql-handler (graphql/handler @schema {}))

   (defroutes app
     (GET \"/graphql\" [] graphql-handler)
     (POST \"/graphql\" [] graphql-handler))
   ```

   ## Options

   - `:context-fn` - `(fn [request] context-map)` Build resolver context from request
   - `:cache` - `ParsedQueryCache` for query caching (use `synthigy.graphql.cache/lru-cache`)
   - `:tracing-header` - Header name to enable Apollo tracing (e.g., \"lacinia-tracing\")"
  (:require
    [synthigy.graphql.http :as http]))

(defn handler
  "Create Ring handler for GraphQL HTTP requests.

   Args:
     schema-provider - Compiled Lacinia schema, or (fn [] schema), or atom/ref
     opts            - Options map (see namespace docs)

   Returns:
     Ring handler `(fn [request] response)`"
  ([schema-provider]
   (handler schema-provider {}))
  ([schema-provider opts]
   (http/handler schema-provider opts)))

(defn wrap-graphql
  "Ring middleware - handle GraphQL at path, pass other requests through.

   Args:
     handler         - Next Ring handler
     path            - URL path for GraphQL (e.g., \"/graphql\")
     schema-provider - Compiled schema or provider fn
     opts            - Handler options

   Returns:
     Ring handler"
  [next-handler path schema-provider opts]
  (let [gql-handler (handler schema-provider opts)]
    (fn [request]
      (if (= (:uri request) path)
        (gql-handler request)
        (next-handler request)))))
