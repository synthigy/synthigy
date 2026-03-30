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
    [clojure.tools.logging :as log]
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

   Args:
     token - Valid bearer token

   Returns:
     Map with :user, :roles, :groups, :claims or nil on failure"
  [token]
  (try
    (when-let [claims (encryption/unsign-data token)]
      (let [{:keys [sub client_id]
             sub-uuid "sub:uuid"} claims
            ;; Extract user identifier from claims
            user-id (or (when sub-uuid
                          (try
                            (java.util.UUID/fromString sub-uuid)
                            (catch Exception _ nil)))
                        sub)]
        ;; If client_id present, verify client is still active
        (when (or (nil? client_id)
                  (:active (oauth/get-client client_id)))
          (when-let [user-ctx (and user-id (iam.context/get-user-context user-id))]
            {:user (id/extract user-ctx)
             :roles (:roles user-ctx)
             :groups (:groups user-ctx)
             :claims claims}))))
    (catch Exception e
      (log/errorf e "Error extracting context from token")
      nil)))

(defn authenticate-request
  "Authenticate request and return user context.

   Full authentication flow:
   1. Extract Bearer token from Authorization header
   2. Verify token exists in active tokens registry
   3. Unsign JWT to get claims
   4. Load user context from IAM

   Args:
     request - Ring request map

   Returns:
     Map with :user, :roles, :groups or nil if not authenticated"
  [request]
  (let [token (extract-bearer-token request)]
    (when (validate-token token)
      (or (token->user-context token)
          (do
            (log/warn "Failed to extract user context from valid token")
            nil)))))

;;; ============================================================================
;;; Ring Middleware
;;; ============================================================================

(defn wrap-authenticate
  "Ring middleware for OAuth token-based authentication.

   Adds authentication context to request:
   - ::user   - User map (id, username, etc.)
   - ::roles  - Set of role keywords
   - ::groups - Set of group keywords

   If no valid token, continues without authentication (nil values).
   Individual routes are responsible for checking authorization.

   Args:
     handler - Next Ring handler

   Returns:
     Wrapped Ring handler"
  [handler]
  (fn [request]
    (if-let [{:keys [user roles groups]} (authenticate-request request)]
      (handler (assoc request
                      ::user user
                      ::roles roles
                      ::groups groups))
      (handler request))))

;;; ============================================================================
;;; Pedestal Interceptor
;;; ============================================================================

(def authenticate-interceptor
  "Pedestal interceptor for OAuth token-based authentication.

   Adds to context:
   - ::user   - User map
   - ::roles  - Set of role keywords
   - ::groups - Set of group keywords

   If no valid token, continues without authentication."
  {:name ::authenticate
   :enter
   (fn [ctx]
     (if-let [{:keys [user roles groups]} (authenticate-request (:request ctx))]
       (assoc ctx
              ::user user
              ::roles roles
              ::groups groups)
       ctx))})

;;; ============================================================================
;;; Context Map Functions (for async-safe IAM access)
;;; ============================================================================

;; Dynamic vars are thread-local and don't propagate across async boundaries.
;; These functions provide an alternative way to pass IAM context through
;; the GraphQL context map, which travels with execution.

(defn assoc-iam
  "Associate IAM context into a map (typically GraphQL context).

   Use this to pass IAM data through context for async-safe access:

   ```clojure
   (gql/context-interceptor
     (fn [request]
       (auth/assoc-iam {} (auth/authenticate-request request))))
   ```

   Args:
     ctx - Context map to augment
     iam - Map with :user, :roles, :groups (or nil)

   Returns:
     Context map with IAM data merged under namespaced keys"
  [ctx iam]
  (if iam
    (assoc ctx
           ::user (:user iam)
           ::roles (:roles iam)
           ::groups (:groups iam))
    ctx))

(defn get-user
  "Get authenticated user from context map.

   Args:
     ctx - Context map (e.g., GraphQL resolver context)

   Returns:
     User map or nil if not authenticated"
  [ctx]
  (::user ctx))

(defn get-roles
  "Get authenticated user's roles from context map.

   Args:
     ctx - Context map (e.g., GraphQL resolver context)

   Returns:
     Set of role keywords or nil"
  [ctx]
  (::roles ctx))

(defn get-groups
  "Get authenticated user's groups from context map.

   Args:
     ctx - Context map (e.g., GraphQL resolver context)

   Returns:
     Set of group keywords or nil"
  [ctx]
  (::groups ctx))

(defn authenticated?
  "Check if context has an authenticated user.

   Args:
     ctx - Context map

   Returns:
     true if user is authenticated, false otherwise"
  [ctx]
  (some? (::user ctx)))

;;; ============================================================================
;;; Convenience Aliases for WebSocket Auth
;;; ============================================================================

(def valid-token?
  "Alias for validate-token - checks if token exists in active tokens."
  validate-token)

(def get-token-context
  "Alias for token->user-context - extracts IAM context from token."
  token->user-context)
