(ns synthigy.server.auth
  "Server-agnostic authentication middleware and utilities.

   Provides both Ring middleware and Pedestal interceptor for OAuth
   token-based authentication. Extracts Bearer token, validates against
   active tokens registry, and loads user context from IAM.

   ## Ring Middleware

   ```clojure
   (-> handler
       (wrap-authenticate))
   ```

   ## Pedestal Interceptor

   ```clojure
   [authenticate-interceptor
    other-interceptors...]
   ```

   Both approaches bind user context to the request/context for use
   by downstream handlers/resolvers."
  (:require
   [synthigy.log :as log]
   [synthigy.dataset.id :as id]
   [synthigy.iam.context :as iam.context]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as oauth]
   [synthigy.oauth.token :as token]))

;;; ============================================================================
;;; Core Authentication Logic
;;; ============================================================================

(defn extract-bearer-token
  "Extract Bearer token from Authorization header.

   Args:
     request - Ring request map

   Returns:
     Token string or nil"
  [request]
  (when-let [authorization (get-in request [:headers "authorization"])]
    (second (re-find #"Bearer\s+(.+)" authorization))))

(defn validate-token
  "Validate token — check registry first, then verify JWT signature.

   Two validation paths (per RFC 7519 / RFC 6750):
   1. Registry lookup: for session-bound tokens (authorization_code flow)
   2. JWT signature verification: for stateless tokens (client_credentials)

   Args:
     token - Bearer token string

   Returns:
     true if valid, false otherwise"
  [token]
  (and token
       (> (count token) 6)
       (or
         ;; Path 1: Session-bound token in registry
        (contains? (get @token/*tokens* :access_token) token)
         ;; Path 2: Valid JWT signature (client_credentials, cross-instance)
        (try
          (some? (encryption/unsign-data token))
          (catch Exception _ false)))))

(defn token->user-context
  "Extract user/service context from valid token.

   Handles both user tokens (authorization_code) and service tokens
   (client_credentials). Both resolve to a user identity via IAM —
   service tokens resolve to the linked service user.

   Additionally verifies that the client_id in the token (if present)
   maps to an active client. This is the server-side validation
   equivalent to session checking for stateless tokens.

   Options:
     :audience - Expected audience. If provided, token's aud claim must match.
                 Nil means accept any audience (backward compatible).

   Args:
     token - Valid bearer token

   Returns:
     Map with :user, :roles, :groups, :claims or nil on failure"
  [token & {:keys [audience]}]
  (try
    (when-let [claims (encryption/unsign-data token)]
      (let [{:keys [sub client_id aud]
             sub-xid "sub:xid" sub-uuid "sub:uuid"} claims]
        ;; Audience validation: if caller specifies expected audience, enforce it
        (when (or (nil? audience)
                  (= audience aud))
          ;; `sub:xid` / `sub:uuid` carry the canonical id (an XID — Synthigy is
          ;; xid-native; the `:uuid` claim is a legacy alias). Use it directly;
          ;; `iam.context/get-user-context` resolves by id. Fall back to `sub`
          ;; (the user name) when no id claim was requested.
          (let [user-id (or sub-xid sub-uuid sub)]
            ;; If client_id present, verify client is still active
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
  "Authenticate request. Returns {:principal user-ctx :claims jwt-claims} or nil."
  [request]
  (let [token (extract-bearer-token request)]
    (when (validate-token token)
      (or (token->user-context token)
          (do
            (log/warn {:id ::user-context-from-token-failed}
                      "Failed to extract user context from valid token")
            nil)))))

;;; ============================================================================
;;; Ring Middleware
;;; ============================================================================

(defn wrap-authenticate
  "Ring middleware for OAuth token-based authentication. Adds ::principal + ::claims to the request."
  [handler]
  (fn [request]
    (if-let [{:keys [principal claims]} (authenticate-request request)]
      (handler (assoc request
                      ::principal principal
                      ::claims claims))
      (handler request))))

;;; ============================================================================
;;; Pedestal Interceptor
;;; ============================================================================

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

;;; ============================================================================
;;; Context Map Functions (for async-safe IAM access)
;;; ============================================================================

;; Dynamic vars are thread-local and don't propagate across async boundaries.
;; These functions provide an alternative way to pass IAM context through
;; the GraphQL context map, which travels with execution.

(defn assoc-iam
  "Associate IAM context into a map (typically GraphQL context). iam = {:principal :claims}."
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

;;; ============================================================================
;;; Convenience Aliases for WebSocket Auth
;;; ============================================================================

(def valid-token?
  "Alias for validate-token - checks if token exists in active tokens."
  validate-token)

(def get-token-context
  "Alias for token->user-context - extracts IAM context from token."
  token->user-context)
