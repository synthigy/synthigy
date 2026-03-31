(ns synthigy.server
  "Pedestal HTTP server implementation for Synthigy.

  Provides a pluggable Pedestal server adapter that integrates:
  - OAuth 2.0 + OpenID Connect endpoints
  - GraphQL API (using synthigy.graphql)
  - Authentication interceptors
  - CORS configuration

  Dependencies are optional - users must add Pedestal dependencies
  to their own project:
    io.pedestal/pedestal.service
    io.pedestal/pedestal.route
    io.pedestal/pedestal.jetty (or .tomcat, .immutant, etc.)

  Usage:
    (require '[synthigy.server :as server])

    ;; Start server with defaults (localhost:7887)
    (server/start)

    ;; Start with custom config
    (server/start {:host \"0.0.0.0\"
                   :port 3000
                   :routes my-custom-routes
                   :service-initializer my-service-fn})

    ;; Stop server
    (server/stop)"
  (:require
    [synthigy.json :as json]
    clojure.set
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [io.pedestal.http :as http]
    [io.pedestal.http.content-negotiation :as conneg]
    [io.pedestal.http.cors :as cors]
    [io.pedestal.http.ring-middlewares :as middlewares]
    [io.pedestal.http.route :as route]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [ring.util.response :as response]
    synthigy.admin
    synthigy.core
    synthigy.dataset.graphql
    synthigy.dataset.lacinia
    [synthigy.graphql :as gql]
    [synthigy.graphql.cache :as cache]
    synthigy.lacinia
    [synthigy.iam.access :as access]
    synthigy.oauth
    [synthigy.oauth.handlers :as oauth.handlers]
    synthigy.oauth.persistence
    [synthigy.oidc.ldap :as ldap]
    [synthigy.server.auth :as auth]
    [synthigy.server.data :as data]
    [synthigy.server.spa :as spa]
    [synthigy.server.subscription :as subscription]
    [synthigy.ws :as ws])
  (:import
    [jakarta.websocket Session]))

;;; ============================================================================
;;; Authentication
;;; ============================================================================

;; Authentication is provided by synthigy.server.auth
;; Use auth/authenticate-interceptor in route definitions

(def coerce-body
  "Pedestal interceptor for parsing request body based on Content-Type.

  Supports:
  - application/json -> :json-params
  - application/x-www-form-urlencoded (already handled by Pedestal)

  Parsed params are merged into :params map."
  {:name ::coerce-body
   :enter
   (fn [ctx]
     (let [{{content-type "content-type"} :headers
            body :body
            :as request} (:request ctx)]
       (if (and body content-type (str/starts-with? content-type "application/json"))
         (try
           (let [json-params (json/read-str (slurp body))]
             (assoc ctx :request
                    (update request :params merge json-params)))
           (catch Exception e
             (log/warnf e "Failed to parse JSON body")
             ctx))
         ctx)))})

(def json-response
  "Pedestal interceptor to ensure JSON response Content-Type header."
  {:name ::json-response
   :leave
   (fn [ctx]
     (assoc-in ctx [:response :headers "Content-Type"] "application/json"))})

(defn make-info-interceptor
  "Creates an interceptor that returns server info.

  Args:
    info - Optional map with server metadata (version, name, etc.)

  Returns:
    Pedestal interceptor that responds with info map as JSON"
  [info]
  {:name ::info
   :enter
   (fn [ctx]
     (assoc ctx :response
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (json/write-str (or info {}))}))})

;;; ============================================================================
;;; Content Negotiation
;;; ============================================================================

(def supported-types
  ["text/html"
   "application/transit+json"
   "application/edn"
   "application/json"
   "text/plain"])

(def content-negotiation
  (conneg/negotiate-content supported-types))

;;; ============================================================================
;;; Ring Handler Conversion
;;; ============================================================================

(defn- ring-handler->pedestal-interceptor
  "Converts a Ring handler function to a Pedestal interceptor.

  Args:
    handler - Ring handler function (request -> response)
    name - Optional interceptor name keyword

  Returns:
    Pedestal interceptor"
  ([handler] (ring-handler->pedestal-interceptor handler ::ring-handler))
  ([handler name]
   {:name name
    :enter
    (fn [ctx]
      (assoc ctx :response (handler (:request ctx))))}))

;;; ============================================================================
;;; Route Definitions
;;; ============================================================================

(defn default-routes
  "Creates default Synthigy routes.

  Args:
    info - Optional server info map for /info endpoint

  Returns:
    Set of Pedestal route vectors"
  [info]
  #{["/info" :get [coerce-body content-negotiation (make-info-interceptor info)]
     :route-name ::info]

    ;; Data API (direct dataset operations, service-to-service)
    ["/data" :post [coerce-body
                    (ring-handler->pedestal-interceptor data/handler ::data-api)]
     :route-name ::data-api]

    ;; Data Subscriptions
    ["/data/subscription/set" :post [coerce-body
                                     (ring-handler->pedestal-interceptor subscription/set-handler ::subscription-set)]
     :route-name ::subscription-set]
    ["/data/subscription/status" :get [(ring-handler->pedestal-interceptor subscription/status-handler ::subscription-status)]
     :route-name ::subscription-status]
    ["/data/subscription/remove" :post [coerce-body
                                        (ring-handler->pedestal-interceptor subscription/remove-handler ::subscription-remove)]
     :route-name ::subscription-remove]})

(def oauth-routes
  "OAuth 2.0 and OpenID Connect endpoint routes.

  Uses pre-built handlers from synthigy.oauth.handlers with complete
  middleware stacks (parameter parsing, cookies, PKCE, CORS, etc.).

  OAuth 2.0 Endpoints:
  - /oauth/authorize - Authorization endpoint (authorization_code flow)
  - /oauth/token - Token endpoint (all grant types)
  - /oauth/login - Interactive login page (GET/POST)
  - /oauth/logout - Logout endpoint
  - /oauth/revoke - Token revocation endpoint
  - /oauth/device/auth - Device authorization (RFC 8628)
  - /oauth/device/activate - Device activation page

  OpenID Connect Endpoints:
  - /oauth/userinfo - UserInfo endpoint (STANDARD way to get user claims)
  - /oauth/jwks - JSON Web Key Set
  - /.well-known/openid-configuration - OIDC Discovery

  Note: These handlers already include Ring middleware (cookies, params, etc.)
        so they don't need additional Pedestal interceptors for body parsing."
  #{;; OAuth 2.0 Core
    ["/oauth/authorize" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/authorize ::oauth-authorize)]
     :route-name ::oauth-authorize]

    ["/oauth/token" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/token ::oauth-token)]
     :route-name ::oauth-token]

    ["/oauth/login" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/login ::oauth-login)]
     :route-name ::oauth-login-get]

    ["/oauth/login" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/login ::oauth-login)]
     :route-name ::oauth-login-post]

    ["/oauth/logout" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/logout ::oauth-logout)]
     :route-name ::oauth-logout-get]

    ["/oauth/logout" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/logout ::oauth-logout)]
     :route-name ::oauth-logout-post]

    ["/oauth/revoke" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/revoke ::oauth-revoke)]
     :route-name ::oauth-revoke]

    ["/oauth/introspect" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/introspect ::oauth-introspect)]
     :route-name ::oauth-introspect]

    ;; Device Flow (RFC 8628)
    ["/oauth/device/auth" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/device-authorization ::oauth-device-auth)]
     :route-name ::oauth-device-auth]

    ["/oauth/device/activate" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/device-activation ::oauth-device-activate)]
     :route-name ::oauth-device-activate-get]

    ["/oauth/device/activate" :post
     [(ring-handler->pedestal-interceptor oauth.handlers/device-activation ::oauth-device-activate)]
     :route-name ::oauth-device-activate-post]

    ;; OpenID Connect
    ["/oauth/userinfo" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/userinfo ::oidc-userinfo)]
     :route-name ::oidc-userinfo]

    ["/oauth/jwks" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/jwks ::oidc-jwks)]
     :route-name ::oidc-jwks]

    ["/.well-known/openid-configuration" :get
     [(ring-handler->pedestal-interceptor oauth.handlers/openid-configuration ::oidc-discovery)]
     :route-name ::oidc-discovery]})

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

(def serve-resource
  "Interceptor to serve static resources from classpath.
   Maps URI directly to classpath resource path (without leading slash)."
  {:name ::serve-resource
   :enter (fn [{{:keys [uri]} :request :as ctx}]
            (let [resource-path (subs uri 1) ; remove leading /
                  resp (response/resource-response resource-path)]
              (if resp
                (let [ext (get-extension uri)
                      content-type (get content-type-map ext "application/octet-stream")]
                  (assoc ctx :response
                         (assoc-in resp [:headers "Content-Type"] content-type)))
                (assoc ctx :response {:status 404 :body "Not found"}))))})

(def static-routes
  "Routes for serving static OAuth resources (CSS, JS, images)."
  #{["/oauth/css/*" :get [serve-resource] :route-name ::static-css]
    ["/oauth/js/*" :get [serve-resource] :route-name ::static-js]
    ["/oauth/images/*" :get [serve-resource] :route-name ::static-images]})

(defn- make-iam-execute-interceptor
  "Creates execute interceptor that binds IAM access vars during execution.

  The IAM access module uses dynamic vars (*user*, *roles*, *groups*) that
  resolvers check for authorization. This interceptor binds those vars from
  the authenticated user context before executing the GraphQL query."
  [schema-provider opts]
  (let [execute (gql/execute-interceptor schema-provider opts)]
    {:name ::iam-graphql-execute
     :enter
     (fn [ctx]
       (binding [access/*user* (::auth/user ctx)
                 access/*roles* (::auth/roles ctx)
                 access/*groups* (::auth/groups ctx)]
         ((:enter execute) ctx)))}))

(defn- make-iam-context-interceptor
  "Creates context interceptor that adds IAM data to GraphQL context.

  This provides async-safe access to user/roles/groups via the context map,
  complementing the dynamic var bindings in make-iam-execute-interceptor."
  []
  (gql/context-interceptor
    (fn [ctx]
      ;; The authenticate-interceptor has already put ::auth/user etc on ctx
      ;; Pass them through to GraphQL context for async-safe access
      (auth/assoc-iam {} {:user (::auth/user ctx)
                          :roles (::auth/roles ctx)
                          :groups (::auth/groups ctx)}))))

(defn graphql-routes
  "Creates GraphQL API routes using synthigy.graphql.

  Returns:
    Set of Pedestal route vectors with GraphQL query/mutation endpoint"
  []
  (let [schema-fn (fn [] @synthigy.lacinia/compiled)
        query-cache (cache/lru-cache 500)
        opts {:cache query-cache
              :tracing-header "lacinia-tracing"}
        ;; Use split interceptors with IAM-aware execute
        ;; IAM is passed both via dynamic vars (sync convenience)
        ;; and context map (async-safe)
        interceptors
        [auth/authenticate-interceptor
         gql/parse-request-interceptor
         (make-iam-context-interceptor)
         (make-iam-execute-interceptor schema-fn opts)]]
    #{["/graphql" :post interceptors :route-name ::graphql-api]
      ["/graphql" :get interceptors :route-name ::graphql-api-get]}))

;;; ============================================================================
;;; WebSocket GraphQL Subscriptions
;;; ============================================================================

(defn- authenticate-ws-session
  "Authenticate WebSocket session via access_token query parameter.

  Behavior:
    - IAM not started: allow anonymous
    - IAM started + valid token: allow
    - IAM started + invalid/missing token: close session"
  [^Session session _config]
  (when (lifecycle/started? :synthigy/iam)
    (let [token (ws/get-session-param session "access_token")]
      (when-not (auth/valid-token? token)
        (log/trace ::ws-auth-failed {:session-id (.getId session)})
        (.close session)))))

(defn- ws-context-fn
  "Build GraphQL context from WebSocket connection params.
  Extracts IAM context from access_token."
  [connection-params]
  (let [token (:access_token connection-params)]
    (if token
      (let [iam (auth/get-token-context token)]
        (auth/assoc-iam {} iam))
      {})))

(defn graphql-websocket-endpoint
  "Creates WebSocket endpoint for GraphQL subscriptions.

  Authenticates via access_token query parameter or connection_init payload.
  Binds IAM context for resolver authorization.

  Returns:
    Endpoint map for ::http/websockets"
  []
  (ws/endpoint
    (fn [] @synthigy.lacinia/compiled)
    {:app-context {}
     :context-fn ws-context-fn
     :session-initializer authenticate-ws-session
     :keep-alive-ms 25000}))

;;; ============================================================================
;;; Server Lifecycle
;;; ============================================================================

(defonce server (atom nil))

(defn stop
  "Stops the Pedestal HTTP server and optionally the core system.

  Returns:
    nil

  Side effects:
    - Stops HTTP server
    - Optionally stops core system (database, dataset, IAM, etc.)

  Examples:
    (stop)                      ; Stop HTTP server and core system"
  ([]
   ;; Stop HTTP server
   (when-let [running-server @server]
     (log/info "[Pedestal] Stopping HTTP server")
     (http/stop running-server)
     (reset! server nil))
   nil))

(defn start
  "Starts the full Synthigy system (core + HTTP server).

  This is the main entry point for running Synthigy as a complete application.
  It ensures the core system (database, dataset, IAM) is running before starting
  the HTTP server.

  The function is idempotent - it's safe to call multiple times. If the core
  system is already running (db/*db* is bound), it will only start the HTTP server.

  Startup sequence:
  1. Check if core system is running (*db* bound and not nil)
  2. If not, start core system (db, dataset, IAM, OAuth, etc.)
  3. Start HTTP server (Pedestal + Jetty)

  Args:
    opts - Optional configuration map:
           :host - Bind address (default: \"localhost\" or SYNTHIGY_HOST env var)
           :port - Port number (default: 7887 or SYNTHIGY_PORT env var)
           :routes - Additional routes to merge (default: #{})
           :service-initializer - Function to transform service map (default: identity)
           :info - Server info map for /info endpoint (defaults to patcho version info)
           :spa-root - Filesystem path for SPA static files (default: SYNTHIGY_SERVE env var)
                       Set to nil to disable SPA support

  Returns:
    nil

  Side effects:
    - Starts core system if not already running
    - Starts HTTP server and stores reference in server atom

  Examples:
    ;; Start complete system with defaults
    (start)

    ;; Custom port
    (start {:port 3000})

    ;; Development mode with SPA
    (start {:spa-root \"/var/www/my-app/dist\"})

    ;; Production mode (no SPA, use nginx for static files)
    (start {:spa-root nil})"
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port routes service-initializer info spa-root]
     :or {host (or (env :synthigy-host) "localhost")
          port (or (some-> (env :synthigy-port) Integer/parseInt) 7887)
          routes #{}
          service-initializer identity
          spa-root (env :synthigy-serve)}}]

   ;; Note: Core system should already be started by lifecycle dependencies
   ;; If using this function directly (not via lifecycle), ensure dependencies
   ;; are started first via lifecycle/start! :synthigy/dataset :synthigy/iam etc.

   ;; Start HTTP server (stop previous server if running)
   (log/infof "[Pedestal] Starting HTTP server on %s:%s" host port)
   (stop)

   (let [;; Combine all routes
         all-routes (route/expand-routes
                      (clojure.set/union
                        (default-routes info)
                        oauth-routes
                        static-routes
                        (graphql-routes)
                        routes))
         router (route/router all-routes :map-tree)

         ;; Build interceptor chain
         interceptors (cond-> [;; CORS - allow all origins (configure per-route if needed)
                               (cors/allow-origin {:allowed-origins (constantly true)})
                               (middlewares/content-type {:mime-types {}})
                               route/query-params
                               (route/method-param)
                               router]
                        ;; Add SPA interceptor as LAST (only if spa-root provided)
                        spa-root
                        (conj (spa/make-spa-interceptor {:root spa-root})))

         ;; Create service map
         service-map
         {::http/type :jetty
          ::http/join? false
          ::http/host host
          ::http/port port
          ::http/interceptors interceptors
          ;; WebSocket endpoint for GraphQL subscriptions
          ::http/websockets {"/graphql-ws" (graphql-websocket-endpoint)}}

         ;; Apply user-provided service initializer
         final-service-map (service-initializer service-map)

         ;; Create and start server
         _server (-> final-service-map
                     http/create-server
                     http/start)]

     (reset! server _server)
     (log/infof "Pedestal server started on %s:%s" host port)
     (when spa-root
       (log/infof "SPA static files serving from: %s" spa-root))
     nil)))

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point for Pedestal server application.

  Starts the complete Synthigy system (core + HTTP server) by calling `start`.

  Configuration via environment variables:

  Database backend:
    Selected via deps.edn alias at build time (:postgres or :sqlite)
    Run with: clj -M:postgres:pedestal or clj -M:sqlite:pedestal

  HTTP server:
    SYNTHIGY_HOST - Bind address (default: 'localhost')
    SYNTHIGY_PORT - Port number (default: 7887)
    SYNTHIGY_SERVE - SPA static files directory (optional)

  Application:
    SYNTHIGY_USER - Superuser username
    SYNTHIGY_PASSWORD - Superuser password

  Database-specific (PostgreSQL):
    POSTGRES_HOST - PostgreSQL host
    POSTGRES_PORT - PostgreSQL port
    POSTGRES_DB - PostgreSQL database name
    POSTGRES_USER - PostgreSQL username
    POSTGRES_PASSWORD - PostgreSQL password
    HIKARI_MAX_POOL_SIZE - Connection pool size (default: 20)

  Database-specific (SQLite):
    SQLITE_PATH - SQLite database file path

  See synthigy.db and synthigy.server docstrings for details."
  [& _]
  (try
    ;; Start everything (core system + HTTP server)
    (start)
    (log/info "Synthigy Pedestal server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/errorf ex "Failed to start Synthigy Pedestal server")
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
            ;; Runtime: Start HTTP server (core system started by dependencies)
            (log/info "[SERVER] Starting HTTP server...")
            (stop)
            (start)
            (log/info "[SERVER] HTTP server started"))
   :stop (fn []
           ;; Runtime: Stop HTTP server only (core stopped by lifecycle)
           (log/info "[SERVER] Stopping HTTP server...")
           (stop)
           (log/info "[SERVER] HTTP server stopped"))})

(comment
  (lifecycle/print-topology-layers)
  (lifecycle/system-report)
  (lifecycle/print-system-report)
  (lifecycle/start! :synthigy/dataset)
  (lifecycle/start! :synthigy/audit)
  (lifecycle/start! :synthigy/server))
