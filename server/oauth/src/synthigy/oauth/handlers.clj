;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.handlers
  "Pre-built OAuth/OIDC Ring handlers with full middleware stacks."
  (:require
   [ring.middleware.cookies :refer [wrap-cookies]]
   [ring.middleware.keyword-params :refer [wrap-keyword-params]]
   [ring.middleware.params :refer [wrap-params]]
   [synthigy.oauth :as oauth]
   [synthigy.oauth.account :as account]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.device-code :as device]
   [synthigy.oauth.federated :as federated]
   [synthigy.oauth.introspect :as introspect]
   [synthigy.oauth.login :as login]
   [synthigy.oauth.onboarding :as onboarding]
   [synthigy.oauth.page.status :as page.status]
   [synthigy.oauth.token :as token-ns]
   [synthigy.oidc :as oidc]))

(defn wrap-frame-deny
  "Forbid framing the response (clickjacking); never on /oauth/authorize, the hidden renew frame needs it."
  [handler]
  (fn [request]
    (some-> (handler request)
            (update :headers #(merge {"Content-Security-Policy" "frame-ancestors 'none'"}
                                     %
                                     {"X-Frame-Options" "DENY"})))))

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

(def login
  "OAuth login page handler (GET displays the form, POST authenticates and creates a session)."
  (-> #'login/login-handler
      wrap-frame-deny
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def logout
  "OAuth logout handler; terminates the session and optionally redirects to post_logout_redirect_uri."
  (-> #'login/logout-handler
      wrap-frame-deny
      core/wrap-session-read
      core/wrap-basic-authorization
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def oauth-status
  "User-facing terminal status page (/oauth/status, /oauth/device/status) for flows with no client redirect."
  (-> #'page.status/status-handler
      wrap-frame-deny
      wrap-keyword-params
      wrap-params
      wrap-cookies))

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
      wrap-frame-deny
      wrap-keyword-params
      wrap-params
      wrap-cookies))

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

(def onboard
  "POST /oauth/onboard — mint a one-time onboarding claim link (confidential client whose principal administers the user)."
  (-> #'onboarding/onboard-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def claim
  "GET /oauth/claim?token=... — the onboarding claim page."
  (-> #'onboarding/claim-page-handler
      wrap-frame-deny
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def claim-password
  "POST /oauth/claim/password — bootstrap method #1 (password) as a claim action."
  (-> #'onboarding/claim-password-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(defn account-route
  [handler]
  (-> handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

(def account
  "GET /oauth/account — the caller's own account: JSON, or the account page for a browser."
  (-> #'account/account-handler
      wrap-frame-deny
      account-route))

(def account-profile
  "POST /oauth/account/profile — the caller sets its own display name."
  (account-route #'account/profile-handler))

(def account-password
  "POST /oauth/account/password — the caller replaces its own password."
  (account-route #'account/password-handler))

(def account-reauth
  "POST /oauth/account/reauth — the caller proves presence again by password."
  (account-route #'account/reauth-handler))

(def account-unlink
  "POST /oauth/account/identities/unlink — the caller removes one of its sign-in methods."
  (account-route #'account/unlink-handler))

(def account-revoke
  "POST /oauth/account/sessions/revoke — the caller ends one of its sessions, or all but this one."
  (account-route #'account/revoke-handler))

(def onboard-complete
  "POST /oauth/onboard/complete — indirect (non-browser) onboarding completion."
  (-> #'onboarding/onboard-complete-handler
      wrap-keyword-params
      wrap-params
      wrap-cookies))

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
   "/oauth/federated/client/identities" {:get federated-client-identities}
   "/oauth/federated/client/unlink" {:post federated-client-unlink}
   ;; Account onboarding/claim — federation is one claim method among several
   "/oauth/onboard" {:post onboard}
   "/oauth/onboard/complete" {:post onboard-complete}
   "/oauth/claim" {:get claim}
   "/oauth/claim/password" {:post claim-password}
   ;; Self-service account — always the caller's own, never a user named in the request
   "/oauth/account" {:get account}
   "/oauth/account/profile" {:post account-profile}
   "/oauth/account/password" {:post account-password}
   "/oauth/account/reauth" {:post account-reauth}
   "/oauth/account/identities/unlink" {:post account-unlink}
   "/oauth/account/sessions/revoke" {:post account-revoke}
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
