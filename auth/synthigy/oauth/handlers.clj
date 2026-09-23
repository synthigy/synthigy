;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.handlers
  "Pre-built OAuth/OIDC Ring handlers with full middleware stacks, zero Pedestal
   dependencies. See docs/core/synthigy/oauth/handlers.md."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [ring.middleware.cookies :refer [wrap-cookies]]
   [ring.middleware.keyword-params :refer [wrap-keyword-params]]
   [ring.middleware.params :refer [wrap-params]]
   [synthigy.oauth :as oauth]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.device-code :as device]
   [synthigy.oauth.federated :as federated]
   [synthigy.oauth.introspect :as introspect]
   [synthigy.oauth.login :as login]
   [synthigy.oauth.credentials :as credentials]
   [synthigy.oauth.onboarding :as onboarding]
   [synthigy.oauth.page.status :as page.status]
   [synthigy.oauth.ring :as ring]
   [synthigy.oauth.token :as token-ns]
   [synthigy.oidc :as oidc]))

;; =============================================================================
;; CORS Configuration
;; =============================================================================

(defn allowed-origins
  "Allowed CORS origins: SYNTHIGY_SERVER_ALLOWED_ORIGINS (comma-separated)
   plus SYNTHIGY_IAM_ROOT_URL as fallback."
  []
  (let [origins-str (env :synthigy-server-allowed-origins "")
        origins (remove empty? (str/split origins-str #"\s*,\s*"))
        iam-root (env :synthigy-iam-root-url "http://localhost:7887")]
    (set (conj origins iam-root))))

(defn wrap-identity-provider-cors
  [handler]
  (ring/wrap-cors handler {:allowed-origins (allowed-origins)}))

;; =============================================================================
;; OAuth 2.0 Core Endpoints
;; =============================================================================

(def token
  "OAuth 2.0 token endpoint (all grant types), fully wrapped."
  (-> #'token-ns/token-handler
      core/wrap-scope->set
      oauth/wrap-pkce-validation
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def revoke
  "OAuth 2.0 token revocation endpoint, fully wrapped."
  (-> #'token-ns/revoke-token-handler
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def introspect
  "OAuth 2.0 token introspection endpoint (RFC 7662), fully wrapped."
  (-> #'introspect/introspect-handler
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def authorize
  "OAuth 2.0 authorization endpoint (PKCE-supported); returns a code immediately (prompt=none) or redirects to login."
  (-> #'oauth/authorization-handler
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; OAuth 2.0 Login/Logout
;; =============================================================================

(def login
  "OAuth login page handler (GET displays the form, POST authenticates and creates a session)."
  (-> #'login/login-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies
      wrap-identity-provider-cors))

(def logout
  "OAuth logout handler; terminates the session and optionally redirects to post_logout_redirect_uri."
  (-> #'login/logout-handler
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies
      wrap-identity-provider-cors))

(def oauth-status
  "User-facing terminal status page (/oauth/status, /oauth/device/status) for flows with no client redirect."
  (-> #'page.status/status-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; OAuth 2.0 Device Code Flow (RFC 8628)
;; =============================================================================

(def device-authorization
  "OAuth 2.0 Device Authorization handler (RFC 8628); returns device_code/user_code/verification URIs."
  (-> #'device/device-authorization-handler
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def device-activation
  "OAuth 2.0 Device Activation handler (GET displays the form, POST confirms/cancels)."
  (-> #'device/device-activation-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; OpenID Connect (OIDC) Endpoints
;; =============================================================================

(def userinfo
  "OpenID Connect UserInfo endpoint; requires a valid Bearer access token."
  (-> #'oidc/userinfo-handler
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def jwks
  "JSON Web Key Set endpoint — public keys for verifying ID token signatures."
  (-> #'oidc/jwks-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def openid-configuration
  "OpenID Connect Discovery 1.0 handler — endpoint URLs and supported features."
  (-> #'oidc/openid-configuration-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def oauth-authorization-server
  "OAuth 2.0 Authorization Server Metadata handler (RFC 8414)."
  (-> #'oidc/oauth-authorization-server-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; Federated login (identity brokering — Synthigy as RP to upstream IdPs)
;; =============================================================================

(def federated-start
  "GET /oauth/federated/start?provider=&state= — bounces to the upstream IdP."
  (-> #'federated/start-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def federated-callback
  "GET /oauth/federated/callback — upstream IdP callback."
  (-> #'federated/callback-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def federated-providers
  "GET /oauth/federated/providers — public list of active federation providers."
  #'federated/providers-handler)

(def federated-identities
  "GET /oauth/federated/identities — the current user's linked sign-in methods."
  (-> #'federated/identities-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def federated-unlink
  "POST /oauth/federated/unlink — remove one of the current user's linked identities."
  (-> #'federated/unlink-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def federated-client-identities
  "GET /oauth/federated/client/identities?user=<xid> — client-scoped identity listing for an administered user."
  (-> #'federated/client-identities-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def federated-client-unlink
  "POST /oauth/federated/client/unlink — client-scoped identity unlink for an administered user."
  (-> #'federated/client-unlink-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; Account onboarding/claim — NOT federation-specific (federation is one of the
;; claim methods, alongside password); lives in synthigy.oauth.onboarding.

(def onboard
  "POST /oauth/onboard — mint a one-time onboarding claim link (confidential client whose principal administers the user)."
  (-> #'onboarding/onboard-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def claim
  "GET /oauth/claim?token=... — the onboarding claim page."
  (-> #'onboarding/claim-page-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def claim-password
  "POST /oauth/claim/password — bootstrap method #1 (password) as a claim action."
  (-> #'onboarding/claim-password-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def change-password
  "POST /oauth/password — the authenticated subject replaces its own password."
  (-> #'credentials/change-password-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def onboard-complete
  "POST /oauth/onboard/complete — indirect (non-browser) onboarding completion."
  (-> #'onboarding/onboard-complete-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

;; =============================================================================
;; Convenience Collections
;; =============================================================================

(def oauth-routes
  "Map of OAuth 2.0 endpoint paths to handlers."
  {"/oauth/token" {:post token}
   "/oauth/authorize" {:get authorize}
   "/oauth/login" {:get login :post login}
   "/oauth/logout" {:get logout :post logout}
   "/oauth/revoke" {:get revoke :post revoke}
   "/oauth/introspect" {:post introspect}
   "/oauth/device/auth" {:post device-authorization}
   "/oauth/device/activate" {:get device-activation :post device-activation}
   ;; Federated (social) login — Synthigy brokering to upstream IdPs
   "/oauth/federated/start" {:get federated-start}
   "/oauth/federated/callback" {:get federated-callback}
   "/oauth/federated/providers" {:get federated-providers}
   "/oauth/federated/identities" {:get federated-identities}
   "/oauth/federated/unlink" {:post federated-unlink}
   "/oauth/federated/client/identities" {:get federated-client-identities}
   "/oauth/federated/client/unlink" {:post federated-client-unlink}
   ;; Account onboarding/claim — federation is one claim method among several
   "/oauth/onboard" {:post onboard}
   "/oauth/onboard/complete" {:post onboard-complete}
   "/oauth/claim" {:get claim}
   "/oauth/claim/password" {:post claim-password}
   ;; Subject-driven credential change — not onboarding, no ticket
   "/oauth/password" {:post change-password}
   ;; RFC 8414 - OAuth 2.0 Authorization Server Metadata
   "/.well-known/oauth-authorization-server" {:get oauth-authorization-server}})

(def oidc-routes
  "Map of OpenID Connect endpoint paths to handlers."
  {"/oauth/userinfo" {:get userinfo}
   "/oauth/jwks" {:get jwks}
   ;; OpenID Connect Discovery 1.0
   "/.well-known/openid-configuration" {:get openid-configuration}})

(def all-routes
  "Map of all OAuth/OIDC endpoint paths to handlers."
  (merge oauth-routes oidc-routes))
