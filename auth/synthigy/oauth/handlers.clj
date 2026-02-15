(ns synthigy.oauth.handlers
  "Pre-built OAuth/OIDC endpoint handlers with complete middleware stacks.

  This namespace provides ready-to-use Ring handlers for integrating OAuth 2.0
  and OpenID Connect into any Ring-based web application.

  All handlers are pure Ring functions with zero Pedestal dependencies.
  They work with Compojure, Reitit, Luminus, Kit, and plain Ring applications.

  ## Quick Start

  ### Compojure
  ```clojure
  (require '[synthigy.oauth.handlers :as oauth])

  (defroutes app
    (POST \"/oauth/token\" [] oauth/token)
    (GET \"/oauth/authorize\" [] oauth/authorize)
    (ANY \"/oauth/login\" [] oauth/login)
    (ANY \"/oauth/logout\" [] oauth/logout))
  ```

  ### Reitit
  ```clojure
  (require '[synthigy.oauth.handlers :as oauth])

  (def routes
    [[\"/oauth\"
      [\"/token\" {:post oauth/token}]
      [\"/authorize\" {:get oauth/authorize}]
      [\"/login\" {:get oauth/login :post oauth/login}]
      [\"/logout\" {:get oauth/logout :post oauth/logout}]]])
  ```

  ## Available Handlers

  - `token` - OAuth token endpoint (all grant types)
  - `authorize` - OAuth authorization endpoint
  - `login` - OAuth login page (GET/POST)
  - `logout` - OAuth logout endpoint
  - `revoke` - OAuth token revocation endpoint
  - `device-authorization` - Device code authorization (RFC 8628)
  - `device-activation` - Device code user activation
  - `userinfo` - OpenID Connect UserInfo endpoint
  - `jwks` - JSON Web Key Set endpoint
  - `openid-configuration` - OpenID Connect Discovery endpoint

  ## Middleware Stacks

  Each handler includes the necessary middleware for OAuth compliance:
  - Parameter parsing (query string + form body)
  - Cookie handling (for sessions)
  - Keywordization of parameters
  - OAuth-specific transformations (scope->set, basic auth, etc.)

  ## Notes

  - All handlers are thread-safe and stateless (state is managed via atoms)
  - Session cookies use secure settings (http-only, secure, same-site)
  - PKCE (Proof Key for Code Exchange) is fully supported
  - All OAuth 2.0 flows are supported (authorization_code, refresh_token, client_credentials, device_code)
  - OpenID Connect is fully supported (UserInfo, JWKS, Discovery)"
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [ring.middleware.cookies :refer [wrap-cookies]]
   [ring.middleware.keyword-params :refer [wrap-keyword-params]]
   [ring.middleware.params :refer [wrap-params]]
   [synthigy.oauth :as oauth]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.device-code :as device]
   [synthigy.oauth.introspect :as introspect]
   [synthigy.oauth.login :as login]
   [synthigy.oauth.ring :as ring]
   [synthigy.oauth.token :as token-ns]
   [synthigy.oidc :as oidc]))

;; =============================================================================
;; CORS Configuration
;; =============================================================================

(defn- allowed-origins
  "Get allowed origins from environment configuration.

   Reads from :synthigy-allowed-origins (comma-separated) and
   :synthigy-iam-root-url (fallback/compatibility).

   Returns a set of allowed origin URLs."
  []
  (let [origins-str (env :synthigy-allowed-origins "")
        origins (remove empty? (str/split origins-str #"\s*,\s*"))
        iam-root (env :synthigy-iam-root-url "http://localhost:8080")]
    (set (conj origins iam-root))))

(defn- wrap-identity-provider-cors
  "Wrap handler with CORS middleware configured for identity provider endpoints.

   Uses environment variables to configure allowed origins:
   - SYNTHIGY_ALLOWED_ORIGINS (comma-separated list)
   - SYNTHIGY_IAM_ROOT_URL (fallback, defaults to http://localhost:8080)"
  [handler]
  (ring/wrap-cors handler {:allowed-origins (allowed-origins)}))

;; =============================================================================
;; OAuth 2.0 Core Endpoints
;; =============================================================================

(def token
  "OAuth 2.0 token endpoint with complete middleware stack.

   Handles all OAuth grant types:
   - authorization_code
   - refresh_token
   - client_credentials
   - device_code (urn:ietf:params:oauth:grant-type:device_code)

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-scope->set (OAuth scope transformation)
   - wrap-pkce-validation (PKCE security)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> token-ns/token-handler
      core/wrap-scope->set
      oauth/wrap-pkce-validation
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def revoke
  "OAuth 2.0 token revocation endpoint with complete middleware stack.

   Revokes access tokens or refresh tokens.

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-session-read (session cookie extraction)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> token-ns/revoke-token-handler
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def introspect
  "OAuth 2.0 token introspection endpoint with complete middleware stack (RFC 7662).

   Allows resource servers to query the authorization server about
   token validity and metadata.

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> introspect/introspect-handler
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def authorize
  "OAuth 2.0 authorization endpoint with complete middleware stack.

   Initiates authorization code flow. Either:
   1. Returns authorization code immediately (silent flow with prompt=none)
   2. Redirects to login page to collect user credentials

   Supports PKCE (code_challenge/code_challenge_method).

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-session-read (session cookie extraction)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> oauth/authorization-handler
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; OAuth 2.0 Login/Logout
;; =============================================================================

(def login
  "OAuth login page handler (GET/POST) with complete middleware stack.

   GET: Display login form
   POST: Authenticate user and create session

   Supports both authorization_code and device_code flows.

   IMPORTANT: Session cookies are set with secure attributes:
   - http-only: true
   - secure: true
   - same-site: :none
   - path: /

   Middleware stack:
   - wrap-identity-provider-cors (CORS for cross-origin login)
   - wrap-cookies (session cookie management)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)"
  (-> login/login-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies
      wrap-identity-provider-cors))

(def logout
  "OAuth logout handler with complete middleware stack.

   Terminates session and optionally redirects to post_logout_redirect_uri.
   Requires either id_token_hint or idsrv/session cookie.

   Session cookies are removed (max-age set to 0).

   Middleware stack:
   - wrap-identity-provider-cors (CORS for cross-origin logout)
   - wrap-cookies (session cookie removal)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-session-read (session cookie extraction)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> login/logout-handler
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies
      wrap-identity-provider-cors))

;; =============================================================================
;; OAuth 2.0 Device Code Flow (RFC 8628)
;; =============================================================================

(def device-authorization
  "OAuth 2.0 Device Authorization handler (RFC 8628) with complete middleware stack.

   Initiates device code flow for devices with limited input capabilities
   (smart TVs, CLI tools, IoT devices).

   Returns device_code, user_code, and verification URIs.

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> device/device-authorization-handler
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def device-activation
  "OAuth 2.0 Device Activation handler with complete middleware stack.

   Handles both GET and POST methods:
   - GET: Display activation form (with or without user_code pre-filled)
   - POST: Process activation (confirm/cancel) and redirect to login

   Security validations:
   - User code validation
   - IP address verification
   - User agent verification
   - Expiration check

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)"
  (-> device/device-activation-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; OpenID Connect (OIDC) Endpoints
;; =============================================================================

(def userinfo
  "OpenID Connect UserInfo endpoint handler with complete middleware stack.

   Returns claims about the authenticated end-user.
   Requires valid access token in Authorization header (Bearer scheme).

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)
   - wrap-basic-authorization (HTTP Basic Auth for client credentials)"
  (-> oidc/userinfo-handler
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def jwks
  "JSON Web Key Set (JWKS) endpoint handler with complete middleware stack.

   Returns public keys used for verifying JWT signatures (ID tokens).
   Used by OIDC clients to validate tokens without shared secrets.

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)"
  (-> oidc/jwks-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def openid-configuration
  "OpenID Connect Discovery handler with complete middleware stack.

   Returns OIDC configuration metadata (RFC 8414).
   Provides endpoint URLs and supported features for OIDC clients.

   Middleware stack:
   - wrap-cookies (session tracking)
   - wrap-keyword-params (parameter normalization)
   - wrap-params (query string + form body parsing)"
  (-> oidc/openid-configuration-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; Convenience Collections
;; =============================================================================

(def oauth-routes
  "Map of OAuth 2.0 endpoint paths to handlers.

   Use this for quick integration with routing libraries."
  {"/oauth/token" {:post token}
   "/oauth/authorize" {:get authorize}
   "/oauth/login" {:get login :post login}
   "/oauth/logout" {:get logout :post logout}
   "/oauth/revoke" {:get revoke :post revoke}
   "/oauth/introspect" {:post introspect}
   "/oauth/device/auth" {:post device-authorization}
   "/oauth/device/activate" {:get device-activation :post device-activation}})

(def oidc-routes
  "Map of OpenID Connect endpoint paths to handlers.

   Use this for quick integration with routing libraries."
  {"/oauth/userinfo" {:get userinfo}
   "/oauth/jwks" {:get jwks}
   "/.well-known/openid-configuration" {:get openid-configuration}})

(def all-routes
  "Map of all OAuth/OIDC endpoint paths to handlers.

   Combines oauth-routes and oidc-routes for complete OAuth/OIDC support."
  (merge oauth-routes oidc-routes))
