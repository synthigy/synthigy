(ns synthigy.server
  "Pedestal HTTP server implementation for Synthigy.

  Provides a pluggable Pedestal server adapter that integrates:
  - OAuth 2.0 + OpenID Connect endpoints
  - GraphQL API with Lacinia-Pedestal
  - Authentication interceptors
  - CORS configuration

  Dependencies are optional - users must add Pedestal dependencies
  to their own project:
    io.pedestal/pedestal.service
    io.pedestal/pedestal.route
    io.pedestal/pedestal.jetty (or .tomcat, .immutant, etc.)

  Usage:
    (require '[synthigy.server :as server])

    ;; Start server with defaults (localhost:8080)
    (server/start)

    ;; Start with custom config
    (server/start {:host \"0.0.0.0\"
                   :port 3000
                   :routes my-custom-routes
                   :service-initializer my-service-fn})

    ;; Stop server
    (server/stop)"
  (:require
    [clojure.data.json :as json]
    clojure.set
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia.pedestal2 :as lp]
    [environ.core :refer [env]]
    [io.pedestal.http :as http]
    [io.pedestal.http.content-negotiation :as conneg]
    [io.pedestal.http.cors :as cors]
    [io.pedestal.http.ring-middlewares :as middlewares]
    [io.pedestal.http.route :as route]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    synthigy.admin
    synthigy.core
    synthigy.dataset.graphql
    [synthigy.dataset.id :as id]
    synthigy.dataset.lacinia
    [synthigy.iam.access :as access]
    [synthigy.iam.context :as iam.context]
    [synthigy.iam.encryption :as encryption]
    synthigy.lacinia
    synthigy.oauth
    [synthigy.oauth.handlers :as oauth.handlers]
    synthigy.oauth.store
    [synthigy.oauth.token :as token]
    [synthigy.oidc.ldap :as ldap]
    [synthigy.server.spa :as spa]))

;;; ============================================================================
;;; Authentication Interceptors
;;; ============================================================================

(def authenticate
  "Pedestal interceptor for OAuth token-based authentication.

  Extracts and validates Bearer token from Authorization header.
  Binds user context (user, roles, groups) to context map for use
  by downstream interceptors and resolvers.

  Authentication flow:
  1. Extract Bearer token from Authorization header
  2. Verify token exists in active tokens registry
  3. Unsign JWT to get claims (sub, sub:uuid)
  4. Load user context from IAM
  5. Bind user/roles/groups to context

  If no valid token, continues without authentication (nil context).
  Individual routes/resolvers are responsible for checking authorization."
  {:name ::authenticate
   :enter
   (fn [ctx]
     (let [{{authorization "authorization"} :headers} (:request ctx)
           bearer-token (when authorization
                          (second (re-find #"Bearer\s+(.+)" authorization)))
           has-token? (and bearer-token (> (count bearer-token) 6))]
       (if (and has-token?
                ;; Verify token exists in active tokens registry
                (contains? (get @token/*tokens* :access_token) bearer-token))
         (try
           ;; Unsign JWT to get claims
           (if-let [claims (encryption/unsign-data bearer-token)]
             (let [{:keys [sub]
                    sub-uuid "sub:uuid"} claims
                   ;; Extract user UUID from claims
                   user-uuid (or (when sub-uuid
                                   (try
                                     (java.util.UUID/fromString sub-uuid)
                                     (catch Exception _
                                       nil)))
                                 sub)
                   ;; Load full user context
                   user-ctx (when user-uuid
                              (iam.context/get-user-context user-uuid))]
               (if user-ctx
                 (assoc ctx
                   ::user (id/extract user-ctx)
                   ::roles (:roles user-ctx)
                   ::groups (:groups user-ctx))
                 (do
                   (log/warnf "User not found for token sub: %s" user-uuid)
                   ctx)))
             (do
               (log/warn "Failed to unsign bearer token")
               ctx))
           (catch Exception e
             (log/errorf e "Error during authentication")
             ctx))
         ;; No valid token - continue without authentication
         ctx)))})

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
           (let [json-params (json/read-str (slurp body) :key-fn keyword)]
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
     :route-name ::info]})

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

(defn graphql-routes
  "Creates GraphQL API routes using Lacinia-Pedestal.

  Returns:
    Set of Pedestal route vectors with GraphQL query/mutation endpoint"
  []
  (let [_schema (fn [] @synthigy.lacinia/compiled)
        options {:api-path "/graphql"}
        ;; Wrap query executor to bind user context from authentication
        wrapped-query-executor
        {:name ::graphql-query-executor
         :enter
         (fn [ctx]
           (let [user (::user ctx)
                 roles (::roles ctx)
                 groups (::groups ctx)]
             (binding [access/*user* user
                       access/*roles* roles
                       access/*groups* groups]
               ((:enter lp/query-executor-handler) ctx))))}
        interceptors
        [authenticate
         lp/initialize-tracing-interceptor
         json-response
         lp/error-response-interceptor
         lp/body-data-interceptor
         lp/graphql-data-interceptor
         lp/status-conversion-interceptor
         lp/missing-query-interceptor
         (lp/query-parser-interceptor _schema (:parsed-query-cache options))
         lp/disallow-subscriptions-interceptor
         lp/prepare-query-interceptor
         (lp/inject-app-context-interceptor nil)
         lp/enable-tracing-interceptor
         wrapped-query-executor]]
    #{["/graphql" :post interceptors :route-name ::graphql-api]}))

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
           :port - Port number (default: 8080 or SYNTHIGY_PORT env var)
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
          port (or (some-> (env :synthigy-port) Integer/parseInt) 8080)
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
          ::http/interceptors interceptors}

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
    SYNTHIGY_PORT - Port number (default: 8080)
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
                :synthigy/oauth.store
                :synthigy/lacinia
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
