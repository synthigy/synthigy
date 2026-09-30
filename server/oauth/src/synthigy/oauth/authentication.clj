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

(ns synthigy.oauth.authentication
  "Server-agnostic authentication middleware and utilities."
  (:require
   [synthigy.log :as log]
   [synthigy.dataset.id :as id]
   [synthigy.iam.context :as iam.context]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as oauth]))

(defn extract-bearer-token
  "Extract the Bearer token from the Authorization header."
  [request]
  (when-let [authorization (get-in request [:headers "authorization"])]
    (second (re-find #"Bearer\s+(.+)" authorization))))

(defn validate-token
  "Validate a bearer token by JWT signature; stateless — revocation is enforced
   at refresh, not per-request."
  [token]
  (and token
       (> (count token) 6)
       ;; unsign-data verifies signature only — expiry is enforced here
       (boolean
        (when-let [{:keys [exp]} (encryption/unsign-data token)]
          (or (nil? exp)
              (> exp (quot (System/currentTimeMillis) 1000)))))))

(def first-party-audiences
  "Both faces of a first-party token: the platform audience and the identity
   default. NEVER pass this to authorize a data-plane request — it exists for
   enrichment (telemetry/log attribution), where dropping the user off an
   identity-token request loses attribution without gaining any safety."
  #{oauth/platform-audience oauth/oidc-audience})

(defn token->user-context
  "Extract user/service context from a valid token, or nil; `:audience` is a
   string or a set of acceptable ones, defaulting to the platform audience."
  [token & {:keys [audience]}]
  (try
    (when-let [claims (encryption/unsign-data token)]
      (let [{:keys [sub xid client_id aud]} claims
            accepted (cond
                       (nil? audience) #{oauth/platform-audience}
                       (set? audience) audience
                       :else           #{audience})]
        (when (contains? accepted aud)
          (let [user-id (or xid sub)]
            (when (or (nil? client_id)
                      (:active (oauth/get-client client_id)))
              (when-let [user-ctx (and user-id (iam.context/get-user-context user-id))]
                {:principal user-ctx
                 :claims claims}))))))
    (catch Exception e
      (log/error! {:id ::token-context-failed
                   :msg "Error extracting context from token"} e)
      nil)))

(defn authenticate-request
  "Authenticate request; returns {:principal user-ctx :claims jwt-claims} or
   nil. No audience argument means the platform audience — never 'any'."
  ([request] (authenticate-request request nil))
  ([request audience]
   (let [token (extract-bearer-token request)]
     (when (validate-token token)
       (or (token->user-context token :audience audience)
           (do
             (log/warn {:id ::user-context-from-token-failed}
                       "Failed to extract user context from valid token")
             nil))))))

(defn wrap-authenticate
  "Ring middleware for OAuth token-based authentication. Adds ::principal +
   ::claims to the request."
  [handler]
  (fn [request]
    (if-let [{:keys [principal claims]} (authenticate-request request)]
      (handler (assoc request
                      ::principal principal
                      ::claims claims))
      (handler request))))

(def authenticate-interceptor
  "Pedestal interceptor for OAuth token-based authentication. Adds ::principal + ::claims to the context."
  {:name ::authenticate
   :enter
   (fn [ctx]
     (if-let [{:keys [principal claims]} (authenticate-request (:request ctx))]
       (assoc ctx
              ::principal principal
              ::claims claims)
       ctx))})

(defn assoc-iam
  "Associate IAM context {:principal :claims} into a context map."
  [ctx iam]
  (if iam
    (assoc ctx
           ::principal (:principal iam)
           ::claims (:claims iam))
    ctx))

(defn get-principal
  "Get the materialized principal map from context."
  [ctx]
  (::principal ctx))

(defn get-claims
  "Get the JWT claims map from context."
  [ctx]
  (::claims ctx))

(defn authenticated?
  "Check if context has an authenticated principal."
  [ctx]
  (some? (::principal ctx)))

(def valid-token?
  "Alias for validate-token."
  validate-token)

(def get-token-context
  "Alias for token->user-context."
  token->user-context)
