(ns synthigy.server
  "http-kit HTTP server implementation for Synthigy.

  Provides a pluggable http-kit server adapter that integrates:
  - OAuth 2.0 + OpenID Connect endpoints
  - GraphQL API (using synthigy.graphql)
  - Authentication middleware
  - CORS configuration
  - SPA serving

  ## Quick Start

  ```clojure
  (require '[synthigy.server :as server])

  ;; Start with defaults (localhost:7887)
  (server/start)

  ;; Custom port
  (server/start {:port 3000})

  ;; Stop
  (server/stop)
  ```

  ## Custom Routing

  For custom routing with reitit, compojure, or other libraries,
  use the provided handlers directly:

  ```clojure
  (require '[synthigy.server :as server])
  (require '[reitit.ring :as reitit])

  (def app
    (reitit/ring-handler
      (reitit/router
        [[\"/oauth\" oauth-routes]    ; Use server/oauth-routes
         [\"/graphql\" {:handler (server/make-graphql-handler)}]
         [\"/api\" my-api-routes]])
      (server/make-default-handler)))
  ```

  See individual handler functions for details."
  (:require
    [clojure.core.async :as async]
    [synthigy.json :as json]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [org.httpkit.server :as httpkit]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
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
    [synthigy.server.subscription :as subscription]
    [synthigy.ws :as ws]))

;;; ============================================================================
;;; GraphQL Handler
;;; ============================================================================

(defn make-graphql-handler
  "Creates a GraphQL Ring handler with IAM context bindings.

  The handler authenticates requests and binds IAM access vars (*user*,
  *roles*, *groups*) during GraphQL execution for resolver authorization.

  IAM data is passed through both:
  - Dynamic vars (convenient for sync resolvers)
  - Context map (async-safe, use auth/get-user etc in resolvers)

  Args:
    opts - Optional configuration:
           :cache          - ParsedQueryCache (default: LRU cache 500)
           :tracing-header - Header name to enable tracing (default: nil)

  Returns:
    Ring handler function"
  ([] (make-graphql-handler {}))
  ([{:keys [cache tracing-header]
     :or {cache (cache/lru-cache 500)}}]
   (let [schema-fn (fn [] @synthigy.lacinia/compiled)
         ;; Context function that adds IAM data from pre-authenticated request
         context-fn (fn [request]
                      (auth/assoc-iam {} (::auth/iam request)))
         base-handler (gql/handler
                        schema-fn
                        {:cache cache
                         :tracing-header tracing-header
                         :context-fn context-fn})]
     ;; Wrap with authentication and IAM bindings
     (fn [request]
       (let [iam (auth/authenticate-request request)]
         (binding [access/*user* (:user iam)
                   access/*roles* (:roles iam)
                   access/*groups* (:groups iam)]
           ;; Pass IAM in request for context-fn to pick up
           (base-handler (assoc request ::auth/iam iam))))))))

;;; ============================================================================
;;; Simple Router (No External Dependencies)
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
  "Creates a simple router from route definitions.

  Routes are vectors of [uri method handler]:
    [[\"/oauth/token\" :post oauth/token]
     [\"/graphql\" :any graphql-handler]]

  For complex routing needs, use reitit or compojure directly
  with the individual handlers.

  Args:
    routes - Vector of route definitions

  Returns:
    Ring handler that dispatches to matching route or returns nil"
  [routes]
  (fn [request]
    (when-let [handler (match-route request routes)]
      (handler request))))

;;; ============================================================================
;;; SSE Handler
;;; ============================================================================

(defn- make-sse-handler
  "Creates a Ring handler for SSE event streaming via http-kit.
   Authenticates, creates an event stream, and streams events to the client."
  []
  (fn [request]
    (let [iam (auth/authenticate-request request)]
      (if-not iam
        {:status 401
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})}
        (let [identity-key (subscription/request-identity iam)
              stream-chan (subscription/create-event-stream identity-key)]
          (httpkit/as-channel request
            {:on-open
             (fn [ch]
               (log/debugf "[SSE] Client connected: %s" identity-key)
               ;; Send SSE headers via initial response
               (httpkit/send! ch
                 {:status 200
                  :headers {"Content-Type" "text/event-stream"
                            "Cache-Control" "no-cache"
                            "Connection" "keep-alive"}}
                 false)
               ;; Start sending events (non-blocking, runs on async thread pool)
               (async/go-loop []
                 (when (httpkit/open? ch)
                   (let [[val port] (async/alts!
                                      [stream-chan
                                       (async/timeout 20000)])]
                     (cond
                       ;; Stream channel closed
                       (and (nil? val) (= port stream-chan))
                       (when (httpkit/open? ch)
                         (httpkit/close ch))

                       ;; Timeout - send keepalive
                       (not= port stream-chan)
                       (do (httpkit/send! ch subscription/keepalive-msg false)
                           (recur))

                       ;; Event received
                       :else
                       (do (httpkit/send! ch (subscription/format-sse val) false)
                           (recur)))))))

             :on-close
             (fn [_ch _status]
               (log/debugf "[SSE] Client disconnected: %s" identity-key)
               (subscription/destroy-event-stream identity-key stream-chan))}))))))

;;; ============================================================================
;;; Default Routes
;;; ============================================================================

(defn default-routes
  "Returns default route definitions for Synthigy.

  Includes:
  - OAuth 2.0 endpoints (/oauth/*)
  - OpenID Connect endpoints (/.well-known/*, /oauth/userinfo, /oauth/jwks)
  - GraphQL endpoint (/graphql)
  - Info endpoint (/info)

  Args:
    opts - Configuration options:
           :graphql-opts - Options for GraphQL handler
           :info         - Server info map for /info endpoint

  Returns:
    Vector of route definitions for make-router"
  [{:keys [graphql-opts info]
    :or {graphql-opts {}
         info {}}}]
  (let [graphql-handler (make-graphql-handler graphql-opts)
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

     ;; GraphQL (both GET and POST)
     ["/graphql" :get graphql-handler]
     ["/graphql" :post graphql-handler]

     ;; Data API (direct dataset operations, service-to-service)
     ["/data" :post data/handler]

     ;; Data Subscriptions
     ["/data/subscription/set" :post subscription/set-handler]
     ["/data/subscription/status" :get subscription/status-handler]
     ["/data/subscription/remove" :post subscription/remove-handler]
     ["/data/events" :get (make-sse-handler)]]))

;;; ============================================================================
;;; Middleware Stack
;;; ============================================================================

(defn wrap-cors
  "Simple CORS middleware allowing all origins.

  For production, configure specific origins."
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
  {"css"  "text/css"
   "js"   "application/javascript"
   "png"  "image/png"
   "jpg"  "image/jpeg"
   "jpeg" "image/jpeg"
   "gif"  "image/gif"
   "svg"  "image/svg+xml"
   "ico"  "image/x-icon"
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

(defn- ws-context-fn
  "Build GraphQL context from WebSocket connection params.
  Extracts IAM context from access_token."
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

(defn make-handler
  "Creates the complete Ring handler with all middleware.

  Args:
    opts - Configuration:
           :routes   - Custom routes (default: default-routes)
           :spa-root - Filesystem path for SPA (optional)
           :info     - Server info map

  Returns:
    Ring handler"
  [{:keys [routes spa-root info graphql-opts]
    :or {info {}}}]
  (let [routes (or routes
                   (default-routes {:info info
                                    :graphql-opts graphql-opts}))
        router (make-router routes)]
    (spa/wrap-spa
      (->
        router
        wrap-static-resources
        wrap-cors
        wrap-options
        wrap-keyword-params
        wrap-params
        data/wrap-preserve-body

      ;; WebSocket for GraphQL subscriptions (first to intercept upgrade)
        (ws/wrap-websocket
          {:schema (fn [] @synthigy.lacinia/compiled)
           :path "/graphql-ws"
           :context-fn ws-context-fn
           :on-connect ws-on-connect
           :keep-alive-ms 25000}))
      {:root spa-root})))

;;; ============================================================================
;;; Server Lifecycle
;;; ============================================================================

(defonce server (atom nil))

(defn stop
  "Stops the http-kit server.

  Returns:
    nil"
  []
  (when-let [stop-fn @server]
    (log/info "[http-kit] Stopping HTTP server")
    (stop-fn :timeout 100)
    (reset! server nil))
  nil)

(defn start
  "Starts the http-kit HTTP server.

  Args:
    opts - Configuration map:
           :host     - Bind address (default: localhost or SYNTHIGY_HOST)
           :port     - Port number (default: 7887 or SYNTHIGY_PORT)
           :spa-root - SPA static files directory (default: SYNTHIGY_SERVE)
           :info     - Server info for /info endpoint
           :routes   - Custom routes (replaces defaults)

  Returns:
    nil"
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port spa-root info routes]
     :or {host (or (env :synthigy-host) "localhost")
          port (or (some-> (env :synthigy-port) Integer/parseInt) 7887)
          spa-root (env :synthigy-serve)}}]

   (stop)
   (log/infof "[http-kit] Starting HTTP server on %s:%s" host port)

   (let [handler (make-handler {:routes routes
                                :spa-root spa-root
                                :info info})
         stop-fn (httpkit/run-server handler {:ip host
                                              :port port})]
     (reset! server stop-fn)
     (log/infof "[http-kit] Server started on %s:%s" host port)
     (when spa-root
       (log/infof "[http-kit] SPA static files from: %s" spa-root))
     nil)))

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point for http-kit server.

  Configuration via environment variables:
    SYNTHIGY_HOST - Bind address (default: localhost)
    SYNTHIGY_PORT - Port (default: 7887)
    SYNTHIGY_SERVE - SPA static files directory"
  [& _]
  (try
    (start)
    (log/info "Synthigy http-kit server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error ex "Failed to start http-kit server")
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
            (log/info "[SERVER] Starting http-kit server...")
            (stop)
            (start)
            (log/info "[SERVER] http-kit server started"))
   :stop (fn []
           (log/info "[SERVER] Stopping http-kit server...")
           (stop)
           (log/info "[SERVER] http-kit server stopped"))})

(comment
  (lifecycle/print-topology-layers)
  (lifecycle/system-report)
  (start {:port 7887})
  (stop))
