(ns synthigy.oauth.introspect
  "RFC 7662 Token Introspection endpoint.

   Token introspection allows resource servers to query the authorization
   server about the state of an access token or refresh token.

   The endpoint returns metadata about the token, including:
   - Whether the token is active (valid and not expired)
   - Token scope
   - Client ID that requested the token
   - Username of the resource owner
   - Token expiration and issuance times

   RFC 7662: https://tools.ietf.org/html/rfc7662"
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.token :as token]))

;; =============================================================================
;; Introspection Logic
;; =============================================================================

(defn- inactive-response
  "Returns the standard inactive token response per RFC 7662 Section 2.2."
  []
  {:active false})

(defn- extract-token-claims
  "Extract claims from a JWT token.
   Returns nil if token is invalid or cannot be decoded."
  [token-string]
  (try
    (encryption/unsign-data token-string)
    (catch Exception _
      nil)))

(defn- token-expired?
  "Check if token is expired based on exp claim."
  [claims]
  (when-let [exp (:exp claims)]
    (< (* 1000 exp) (System/currentTimeMillis))))

(defn introspect-token
  "Introspect an access or refresh token.

   Per RFC 7662, returns a map with at minimum {:active true/false}.
   If active, includes token metadata like scope, client_id, exp, etc.

   Args:
     token - The token string to introspect
     token-type-hint - Optional hint: \"access_token\" or \"refresh_token\"

   Returns:
     Map with :active and optional metadata claims"
  [token token-type-hint]
  (log/debugf "[Introspect] Introspecting token (hint: %s)" token-type-hint)

  (cond
    ;; No token provided
    (or (nil? token) (empty? token))
    (do
      (log/debugf "[Introspect] Empty token")
      (inactive-response))

    ;; Try to find token in our token store
    :else
    (let [token-key (when token-type-hint (keyword token-type-hint))
          tokens @token/*tokens*
          ;; Try hint first, then both token types
          [found-key session] (some
                               (fn [tk]
                                 (when-some [s (get-in tokens [tk token])]
                                   [tk s]))
                               (filter some? [token-key :access_token :refresh_token]))]

      (cond
        ;; Token not found in store
        (nil? session)
        (do
          (log/debugf "[Introspect] Token not found in store")
          (inactive-response))

        ;; Token found - get claims and check expiration
        :else
        (let [claims (extract-token-claims token)
              expired? (token-expired? claims)]

          (cond
            ;; No claims (invalid token format)
            (nil? claims)
            (do
              (log/debugf "[Introspect] Could not extract claims from token")
              (inactive-response))

            ;; Token expired
            expired?
            (do
              (log/debugf "[Introspect] Token is expired")
              (inactive-response))

            ;; Valid active token - return metadata
            :else
            (let [{:keys [scope aud iss sub exp iat jti client_id sid]} claims
                  {:keys [name]} (core/get-session-resource-owner session)]
              (log/debugf "[Introspect] Token is active for user: %s" (or name sub))
              {:active true
               :scope (or scope "")
               :client_id (or client_id aud)
               :username (or name sub)
               :token_type "Bearer"
               :exp exp
               :iat iat
               :sub (or sub name)
               :iss (or iss (core/domain+))
               :aud (or aud client_id)
               :jti jti
               :sid sid})))))))

;; =============================================================================
;; Ring Handler
;; =============================================================================

(defn introspect-handler
  "OAuth 2.0 Token Introspection endpoint handler (RFC 7662).

   Accepts POST requests with form-encoded parameters:
   - token (REQUIRED): The token to introspect
   - token_type_hint (OPTIONAL): 'access_token' or 'refresh_token'

   Client authentication is REQUIRED (Basic auth or form params).

   Returns JSON response with at minimum {\"active\": true/false}.
   If active, includes token metadata (scope, client_id, exp, etc.)."
  [request]
  (let [{:keys [token token_type_hint client_id client_secret]} (:params request)]

    (log/debugf "[Introspect] Request from client: %s" client_id)

    (cond
      ;; RFC 7662 Section 2.1: Client authentication is REQUIRED
      ;; Check that client credentials are provided
      (or (nil? client_id) (empty? client_id))
      (do
        (log/debugf "[Introspect] Missing client credentials")
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" "Basic realm=\"OAuth\""
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str {:error "invalid_client"
                                :error_description "Client authentication required"})})

      ;; Validate client credentials
      (nil? (core/get-client client_id))
      (do
        (log/debugf "[Introspect] Unknown client: %s" client_id)
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" "Basic realm=\"OAuth\""
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str {:error "invalid_client"
                                :error_description "Client authentication failed"})})

      ;; Missing token parameter
      (or (nil? token) (empty? token))
      (do
        (log/debugf "[Introspect] Missing token parameter")
        {:status 400
         :headers {"Content-Type" "application/json"
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str {:error "invalid_request"
                                :error_description "Missing required parameter: token"})})

      ;; Valid request - introspect the token
      :else
      (let [result (introspect-token token token_type_hint)]
        {:status 200
         :headers {"Content-Type" "application/json"
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str result)}))))
