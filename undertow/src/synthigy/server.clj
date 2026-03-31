(ns synthigy.server
  "Undertow HTTP server implementation for Synthigy.

   Provides a pluggable Undertow server adapter that integrates:
   - OAuth 2.0 + OpenID Connect endpoints
   - GraphQL API (using synthigy.graphql)
   - GraphQL Subscriptions (WebSocket)
   - Authentication middleware
   - CORS configuration
   - SPA serving

   Uses ring-undertow-adapter (Luminus/Kit compatible).

   ## Quick Start

   ```clojure
   (require '[synthigy.server :as server])

   ;; Start with defaults (localhost:7887)
   (server/start)

   ;; Custom port
   (server/start {:port 3000})

   ;; Stop
   (server/stop)
   ```"
  (:require
    [synthigy.json :as json]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [ring.adapter.undertow :refer [run-undertow]]
    [ring.middleware.keyword-params :refer [wrap-keyword-params]]
    [ring.middleware.params :refer [wrap-params]]
    [ring.util.response :as response]
    synthigy.admin
    synthigy.core
    synthigy.dataset.graphql
    synthigy.dataset.lacinia
    [synthigy.graphql :as gql]
    [synthigy.graphql.cache :as cache]
    [synthigy.iam.access :as access]
    synthigy.lacinia
    synthigy.oauth
    [synthigy.oauth.handlers :as oauth]
    synthigy.oauth.persistence
    synthigy.oidc.ldap
    [synthigy.server.auth :as auth]
    [synthigy.server.data :as data]
    [synthigy.server.spa :as spa]
    [synthigy.ws :as ws]))

;;; ============================================================================
;;; GraphQL Handler
;;; ============================================================================

(defn make-graphql-handler
  "Creates a GraphQL Ring handler with IAM context bindings."
  ([] (make-graphql-handler {}))
  ([{:keys [cache tracing-header]
     :or {cache (cache/lru-cache 500)}}]
   (let [schema-fn (fn [] @synthigy.lacinia/compiled)
         context-fn (fn [request]
                      (auth/assoc-iam {} (::auth/iam request)))
         base-handler (gql/handler
                        schema-fn
                        {:cache cache
                         :tracing-header tracing-header
                         :context-fn context-fn})]
     (fn [request]
       (let [iam (auth/authenticate-request request)]
         (binding [access/*user* (:user iam)
                   access/*roles* (:roles iam)
                   access/*groups* (:groups iam)]
           (base-handler (assoc request ::auth/iam iam))))))))

;;; ============================================================================
;;; WebSocket Handler
;;; ============================================================================

(defn- ws-context-fn
  "Build GraphQL context from WebSocket connection params."
  [connection-params]
  (let [token (:access_token connection-params)]
    (if token
      (let [iam (auth/get-token-context token)]
        (auth/assoc-iam {} iam))
      {})))

(defn- ws-on-connect
  "Authenticate WebSocket connection via access_token query param.

  Returns:
    - {} if IAM not started (no auth required)
    - {:token t} if valid token
    - nil if IAM started but invalid/missing token (triggers 401)"
  [request]
  (if-not (lifecycle/started? :synthigy/iam)
    {}  ; IAM not active, allow anonymous
    (let [token (get-in request [:params :access_token])]
      (when (auth/valid-token? token)
        {:token token}))))

(defn make-websocket-handler
  "Creates WebSocket handler for GraphQL subscriptions."
  []
  (ws/handler
    (fn [] @synthigy.lacinia/compiled)
    {:context-fn ws-context-fn
     :on-connect ws-on-connect
     :keep-alive-ms 25000}))

;;; ============================================================================
;;; Simple Router
;;; ============================================================================

(defn- match-route
  "Simple route matching. Returns handler or nil."
  [request routes]
  (let [uri (:uri request)
        method (:request-method request)]
    (some (fn [[route-uri route-method handler]]
            (when (and (= uri route-uri)
                       (or (= route-method :any)
                           (= route-method method)))
              handler))
          routes)))

(defn make-router
  "Creates a simple router from route definitions."
  [routes]
  (fn [request]
    (when-let [handler (match-route request routes)]
      (handler request))))

;;; ============================================================================
;;; Default Routes
;;; ============================================================================

(defn default-routes
  "Returns default route definitions for Synthigy."
  [{:keys [graphql-opts info]
    :or {graphql-opts {}
         info {}}}]
  (let [graphql-handler (make-graphql-handler graphql-opts)
        websocket-handler (make-websocket-handler)
        info-handler (fn [_]
                       {:status 200
                        :headers {"Content-Type" "application/json"}
                        :body (json/write-str info)})]
    [;; Info
     ["/info" :get info-handler]

     ;; OAuth 2.0 Core
     ["/oauth/authorize" :get oauth/authorize]
     ["/oauth/token" :post oauth/token]
     ["/oauth/login" :get oauth/login]
     ["/oauth/login" :post oauth/login]
     ["/oauth/logout" :get oauth/logout]
     ["/oauth/logout" :post oauth/logout]
     ["/oauth/revoke" :post oauth/revoke]
     ["/oauth/introspect" :post oauth/introspect]

     ;; Device Flow (RFC 8628)
     ["/oauth/device/auth" :post oauth/device-authorization]
     ["/oauth/device/activate" :get oauth/device-activation]
     ["/oauth/device/activate" :post oauth/device-activation]

     ;; OpenID Connect
     ["/oauth/userinfo" :get oauth/userinfo]
     ["/oauth/jwks" :get oauth/jwks]
     ["/.well-known/openid-configuration" :get oauth/openid-configuration]

     ;; GraphQL HTTP
     ["/graphql" :get graphql-handler]
     ["/graphql" :post graphql-handler]

     ;; GraphQL WebSocket
     ["/graphql-ws" :get websocket-handler]

     ;; Data API (direct dataset operations, service-to-service)
     ["/data" :post data/handler]]))

;;; ============================================================================
;;; Middleware Stack
;;; ============================================================================

(defn wrap-cors
  "Simple CORS middleware allowing all origins."
  [handler]
  (fn [request]
    (let [response (handler request)
          origin (get-in request [:headers "origin"])]
      (if response
        (update response :headers merge
                {"Access-Control-Allow-Origin" (or origin "*")
                 "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                 "Access-Control-Allow-Headers" "Content-Type, Authorization"
                 "Access-Control-Allow-Credentials" "true"})
        response))))

(defn wrap-options
  "Handle OPTIONS preflight requests."
  [handler]
  (fn [request]
    (if (= :options (:request-method request))
      {:status 204
       :headers {"Access-Control-Allow-Origin" (get-in request [:headers "origin"] "*")
                 "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                 "Access-Control-Allow-Headers" "Content-Type, Authorization"
                 "Access-Control-Allow-Credentials" "true"}}
      (handler request))))

;;; ============================================================================
;;; Static Resource Serving
;;; ============================================================================

(def ^:private content-type-map
  "Map of file extensions to MIME content types."
  {"css" "text/css"
   "js" "application/javascript"
   "png" "image/png"
   "jpg" "image/jpeg"
   "jpeg" "image/jpeg"
   "gif" "image/gif"
   "svg" "image/svg+xml"
   "ico" "image/x-icon"
   "woff" "font/woff"
   "woff2" "font/woff2"})

(defn- get-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (subs path (inc idx))))

(defn wrap-static-resources
  "Middleware to serve static resources from classpath.
   Handles /oauth/css/*, /oauth/js/*, /oauth/images/* paths."
  [handler]
  (fn [request]
    (let [uri (:uri request)]
      (if (and (= :get (:request-method request))
               (or (str/starts-with? uri "/oauth/css/")
                   (str/starts-with? uri "/oauth/js/")
                   (str/starts-with? uri "/oauth/images/")))
        ;; Try to serve from classpath
        (let [resource-path (subs uri 1) ; remove leading /
              resp (response/resource-response resource-path)]
          (if resp
            (let [ext (get-extension uri)
                  content-type (get content-type-map ext "application/octet-stream")]
              (assoc-in resp [:headers "Content-Type"] content-type))
            (handler request)))
        (handler request)))))

(defn make-handler
  "Creates the complete Ring handler with all middleware."
  [{:keys [routes spa-root info graphql-opts]
    :or {info {}}}]
  (let [routes (or routes (default-routes {:info info
                                           :graphql-opts graphql-opts}))
        router (make-router routes)]
    (-> router
        wrap-static-resources
        (spa/wrap-spa {:root spa-root})
        wrap-cors
        wrap-options
        wrap-keyword-params
        wrap-params)))

;;; ============================================================================
;;; Server Lifecycle
;;; ============================================================================

(defonce server (atom nil))

(defn stop
  "Stops the Undertow server."
  []
  (when-let [s @server]
    (log/info "[Undertow] Stopping HTTP server")
    (.stop s)
    (reset! server nil))
  nil)

(defn start
  "Starts the Undertow HTTP server."
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port spa-root info routes]
     :or {host (or (env :synthigy-host) "localhost")
          port (or (some-> (env :synthigy-port) Integer/parseInt) 7887)
          spa-root (env :synthigy-serve)}}]

   (stop)
   (log/infof "[Undertow] Starting HTTP server on %s:%s" host port)

   (let [handler (make-handler {:routes routes
                                :spa-root spa-root
                                :info info})
         s (run-undertow handler {:host host
                                  :port port})]
     (reset! server s)
     (log/infof "[Undertow] Server started on %s:%s" host port)
     (when spa-root
       (log/infof "[Undertow] SPA static files from: %s" spa-root))
     nil)))

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point for Undertow server."
  [& _]
  (try
    (start)
    (log/info "Synthigy Undertow server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error ex "Failed to start Undertow server")
      (.printStackTrace ex)
      (System/exit 1))))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/server
  {:depends-on [:synthigy/admin
                :synthigy/oauth.persistence
                :synthigy/graphql
                :synthigy/ldap
                :synthigy/audit]
   :start (fn []
            (log/info "[SERVER] Starting Undertow server...")
            (stop)
            (start)
            (log/info "[SERVER] Undertow server started"))
   :stop (fn []
           (log/info "[SERVER] Stopping Undertow server...")
           (stop)
           (log/info "[SERVER] Undertow server stopped"))})


(comment
  (lifecycle/print-system-report)
  (do
    (lifecycle/start! :synthigy/dataset)
    (lifecycle/start! :synthigy/graphql)
    (lifecycle/start! :synthigy/server)

    (lifecycle/stop! :synthigy/server)
    (start)))
