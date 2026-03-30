(ns synthigy.oauth.token
  (:require
   [buddy.core.codecs]
   [buddy.hashers :as hashers]
   [buddy.sign.util :refer [to-timestamp]]
   [camel-snake-kebab.core :as csk]
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [nano-id.core :as nano-id]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as core
    :refer [pprint
            get-client
            session-kill-hook
            access-token-expiry
            refresh-token-expiry
            process-scope
            defscope
            sign-token]]
   [timing.core :as timing]))

(defonce ^:dynamic *tokens* (atom nil))

(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"]
  (def gen-token (nano-id/custom alphabet 50)))

(defn token-error [status code & description]
  ; (log/debugf "Returning error: %s\n%s" code (str/join "\n" description))
  {:status (if (number? status) status 400)
   :headers {"Content-Type" "application/json;charset=UTF-8"
             "Pragma" "no-cache"
             "Cache-Control" "no-store"}
   :body (json/write-str
          {:error (if (number? status) code status)
           :error_description (str/join "\n"
                                        (if (number? status) description
                                            (conj description code)))})})

(comment
  (keys @core/*sessions*)
  (token-error
   400
   "authorization_pending"
   "Evo nekog opisa"))

(defn set-session-tokens
  ([session audience tokens]
   (swap! core/*sessions* assoc-in [session :tokens audience] tokens)
   (swap! *tokens*
          (fn [current-tokens]
            (reduce-kv
             (fn [tokens token-key data]
               (log/debugf "[%s] Adding token %s %s" session token-key data)
               (assoc-in tokens [token-key data] session))
             current-tokens
             tokens)))
   nil))

(defn get-token-session
  [token-key token]
  (get-in @*tokens* [token-key token]))

(defn get-token-audience
  [token-key token]
  (let [session (get-token-session token-key token)]
    (reduce-kv
     (fn [_ audience {_token token-key}]
       (when (= token _token)
         (reduced audience)))
     nil
     (:tokens (core/get-session session)))))

(defn get-session-access-token
  ([session] (get-session-access-token session nil))
  ([session audience]
   (get-in @core/*sessions* [session :tokens audience :access_token])))

(defn get-session-refresh-token
  ([session] (get-session-refresh-token session nil))
  ([session audience]
   (get-in @core/*sessions* [session :tokens audience :refresh_token])))

(defn revoke-token
  ([token-key token]
   (let [session (get-token-session token-key token)
         audience (get-token-audience token-key token)]
     (swap! core/*sessions* update-in [session :tokens audience] dissoc token-key)
     (when token
       (swap! *tokens* update token-key dissoc token)
       (iam/publish
        :oauth.revoke/token
        {:token/key token-key
         :token/data token
         :audience audience
         :session session})))
   nil))

(defn revoke-session-tokens
  ([session]
   (doseq [audience (keys (:tokens (core/get-session session)))]
     (revoke-session-tokens session audience)))
  ([session audience]
   (let [{{tokens audience} :tokens} (core/get-session session)]
     (doseq [[token-key token] tokens]
       (revoke-token token-key token)))
   nil))

(defmethod session-kill-hook 0
  [_ session]
  (revoke-session-tokens session))

(comment
  (def data (gen-token))
  (revoke-session-tokens session)
  (def session "YXldcURYFGCaMkMKwqFQvUblGOlSGh")
  (timing/value->time (* 1000 (:exp (iam/unsign-data (sign-token session :refresh_token data)))))
  (def token (first (keys (get @*tokens* :refresh_token))))
  (def token (first (keys (get @*tokens* :access_token))))
  (time (timing/value->time (* 1000 (:exp (iam/unsign-data token)))))
  (core/expires-at token)

  (java.util.Date.)
  (timing/date)
  (iam/unsign-data token))

(defmethod sign-token :refresh_token
  [session _ data]
  (let [client (get-in @core/*sessions* [session :client])]
    (encryption/sign-data
     (hash-map :value data
               :session session
               :exp (->
                     (System/currentTimeMillis)
                     (quot 1000)
                     (+ (refresh-token-expiry client))))
     {:alg :rs256})))

(defmethod sign-token :access_token
  [_ _ data]
  (encryption/sign-data data {:alg :rs256}))

(def unsupported (core/json-error 500 "unsupported" "This feature isn't supported at the moment"))

(def client-id-missmatch
  (token-error
   "unauthorized_client"
   "Refresh token that you have provided"
   "doesn't belong to given client"))

(def owner-not-authorized
  (token-error
   "resource_owner_unauthorized"
   "Provided refresh token doesn't have active user"))

(def refresh-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token refresh requests."))

(def authorization-code-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token authorization code requests"))

(def device-code-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token device code requests"))

(def client-credentials-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token client credentials requests"))

(def cookie-session-missmatch
  (token-error
   "invalid_request"
   "You session is not provided by this server."
   "This action will be logged and processed!"))

(defmulti grant-token (fn [{:keys [grant_type]}] grant_type))

(defmethod grant-token :default [_] unsupported)

; (defn- issued-at? [token] (:at (meta token)))

(defn generate
  [{{allowed-grants "allowed-grants"} :settings
    :as client} session {:keys [audience scope client_id sub]}]
  (let [access-exp (->
                    (System/currentTimeMillis)
                    (quot 1000)
                    (+ (access-token-expiry client)))
        ;; Use explicit :sub override (client_credentials) or derive from session
        user-name (or sub
                      (:name (core/get-session-resource-owner session)))
        access-token {:session session
                      :aud audience
                      :exp access-exp
                      :iss (core/domain+)
                      :sub user-name
                      :iat (quot (System/currentTimeMillis) 1000)
                      :jti (gen-token)
                      :client_id client_id
                      :sid session
                      :scope (str/join " " scope)}
        refresh? (some #(= "refresh_token" %) allowed-grants)]
    (log/debugf "Generated access token\n%s" (pprint access-token))
    (if (pos? access-exp)
      (let [refresh-token (when (and refresh? session (contains? scope "offline_access"))
                            (log/debugf "Creating refresh token: %s" session)
                            (gen-token))
            tokens (reduce
                    (fn [tokens scope]
                      (process-scope session tokens scope))
                    (if refresh-token
                      {:access_token access-token
                       :refresh_token refresh-token}
                      {:access_token access-token})
                    scope)
            signed-tokens (reduce-kv
                           (fn [tokens token data]
                             (assoc tokens token (sign-token session token data)))
                           tokens
                           tokens)]
        (when session
          (revoke-session-tokens session audience)
          (set-session-tokens session audience signed-tokens)
          (core/set-session-audience-scope session audience scope))
        (iam/publish
         :oauth.grant/tokens
         {:tokens signed-tokens
          :session session})
        (assoc signed-tokens
               :token_type "Bearer"
               :scope (str/join " " scope)
               :expires_in (access-token-expiry client)))
      (let [tokens (reduce
                    (fn [tokens scope]
                      (process-scope session tokens scope))
                    {:access_token access-token}
                    scope)
            signed-tokens (reduce-kv
                           (fn [tokens token data]
                             (assoc tokens token (sign-token session token data)))
                           tokens
                           tokens)]
        (iam/publish
         :oauth.grant/tokens
         {:tokens signed-tokens
          :session session})
        (assoc signed-tokens
               :expires_in (access-token-expiry client)
               :scope (str/join " " scope)
               :token_type "Bearer")))))

(defmethod grant-token "refresh_token"
  [{:keys [refresh_token scope audience]
    cookie-session :idsrv/session
    :as request}]
  (if (core/expired? refresh_token)
    (do
      (core/kill-session (get-token-session :refresh_token refresh_token))
      (token-error
       400
       "invalid_request"
       "Provided token is expired!"))
    (if-let [session (get-token-session :refresh_token refresh_token)]
      (let [{{:strs [allowed-grants]} :settings
             :as client} (core/get-session-client session)
            {:keys [active]} (core/get-session-resource-owner session)
            scope (or
                   scope
                   (core/get-session-audience-scope session audience))
            audience (or
                      audience
                      (get-token-audience :refresh_token refresh_token))
            current-refresh-token (get-in
                                   (core/get-session session)
                                   [:tokens audience :refresh_token])
            grants (set allowed-grants)]
        (when current-refresh-token (revoke-token :refresh_token current-refresh-token))
        (when session (revoke-session-tokens session audience))
        (cond
          ;;
          (not (contains? grants "refresh_token"))
          (do
            (core/kill-session session)
            refresh-not-supported)
          ;;
          (not active)
          (do
            (core/kill-session session)
            owner-not-authorized)
          ;;
          ;;
          (and cookie-session (not= cookie-session session))
          cookie-session-missmatch
          ;;
          (not= refresh_token current-refresh-token)
          (token-error
           400
           "invalid_request"
           "Provided token doesn't match session refresh token"
           "Your request will be logged and processed")
          ;;
          :else
          {:status 200
           :headers {"Content-Type" "application/json;charset=UTF-8"
                     "Pragma" "no-cache"
                     "Cache-Control" "no-store"}
           :body (json/write-str (generate client session (assoc request :scope scope)))}))
      (token-error
       400
       "invalid_grant"
       "There is no valid session for refresh token that"
       "was provided"))))

(defn validate-client-credentials
  "Validates client credentials for client_credentials grant.
   Returns the client if valid, nil otherwise.
   Public clients are explicitly blocked from client_credentials grant."
  [{:keys [client_id client_secret]}]
  (when-let [client (get-client client_id)]
    (let [{client-secret :secret
           client-type :type
           {allowed-grants "allowed-grants"} :settings} client
          grants (set allowed-grants)]
      (cond
        ;; Public clients cannot use client_credentials grant
        (#{:public "public"} client-type)
        (do
          (log/debugf "[%s] Public clients cannot use client_credentials grant" client_id)
          nil)

        ;; Check if client_credentials grant is allowed
        (not (contains? grants "client_credentials"))
        (do
          (log/debugf "[%s] Client credentials grant not allowed for client" client_id)
          nil)

        ;; Check if client has a secret configured
        (and (some? client-secret) (empty? client_secret))
        (do
          (log/debugf "[%s] Client secret required but not provided" client_id)
          nil)

        ;; Validate client secret if present
        (and (some? client-secret)
             (not (hashers/check client_secret client-secret)))
        (do
          (log/debugf "[%s] Invalid client secret provided" client_id)
          nil)

        ;; Client is valid
        :else
        (do
          (log/debugf "[%s] Client credentials validated successfully" client_id)
          client)))))

(defmethod grant-token "client_credentials"
  [{:keys [client_id client_secret scope]
    :as request}]
  (log/debugf "[%s] Processing client credentials grant request" client_id)
  (if-let [client (validate-client-credentials request)]
    (let [;; Process the requested scope
          processed-scope (or scope "")

          ;; Service user has the same name as the client id
          ;; This is set as sub claim in the JWT for identity resolution
          token-request (assoc request
                          :scope processed-scope
                          :sub (:id client))]

      (try
        (let [tokens (generate client nil token-request)
              response (json/write-str tokens)]
          (log/debugf "[%s] Client credentials tokens generated successfully" client_id)
          {:status 200
           :headers {"Content-Type" "application/json;charset=UTF-8"
                     "Pragma" "no-cache"
                     "Cache-Control" "no-store"}
           :body response})
        (catch Exception e
          (log/errorf e "[%s] Error generating tokens for client credentials" client_id)
          (token-error
           500
           "server_error"
           "An error occurred while generating tokens"))))

    ;; Client validation failed
    (do
      (log/warnf "[%s] Client credentials validation failed" client_id)
      (token-error
       401
       "invalid_client"
       "Client authentication failed"))))

(defn token-endpoint
  [{{:keys [grant_type]
     :as oauth-request} :params
    :as request}]
  (log/debugf "Received token endpoint request\n%s" (pprint request))
  (binding [core/*domain* (core/original-uri request)]
    (case grant_type
      ;; Supported grant types
      ("authorization_code" "refresh_token" "urn:ietf:params:oauth:grant-type:device_code" "client_credentials")
      (grant-token oauth-request)
      ;;else
      (core/handle-request-error
       {:type "unsupported_grant_type"
        :grant_type grant_type}))))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn token-handler
  "OAuth 2.0 token endpoint handler.

   Handles all OAuth grant types:
   - authorization_code
   - refresh_token
   - client_credentials
   - device_code (urn:ietf:params:oauth:grant-type:device_code)

   Returns access_token, refresh_token (optional), and token metadata."
  [request]
  (token-endpoint request))

(defn revoke-token-handler
  "OAuth 2.0 token revocation endpoint handler (RFC 7009).

   Revokes access_token or refresh_token based on token_type_hint.

   Per RFC 7009 Section 2.2: 'The authorization server responds with
   HTTP status code 200 if the token has been revoked successfully
   or if the client submitted an invalid token.'

   Returns 200 OK for valid revocation OR unknown/invalid tokens.
   Returns 400 for missing required parameters or client auth failures."
  [request]
  (let [invalid-client (core/json-error "invalid_client" "Client ID is not valid")
        invalid-request (core/json-error "invalid_request" "Missing required parameter: token")
        {:keys [token_type_hint token] :as params} (:params request)
        ;; RFC 7009 compliant success response
        ok-response {:status 200
                     :body ""
                     :headers {"Content-Type" "application/json"
                               "Cache-Control" "no-store"
                               "Pragma" "no-cache"}}]
    (cond
      ;; Missing token parameter - RFC 7009 Section 2.1: token is REQUIRED
      (or (nil? token) (empty? token))
      (do
        (log/debugf "Missing required token parameter")
        invalid-request)

      ;; Lookup token
      :else
      (let [token-key (when token_type_hint (keyword token_type_hint))
            tokens @*tokens*
            [token-key session] (some
                                 (fn [tk]
                                   (when-some [s (get-in tokens [tk token])]
                                     [tk s]))
                                 [token-key :access_token :refresh_token])]
        (cond
          ;; Token not found - RFC 7009 requires 200 OK
          (nil? session)
          (do
            (log/debugf "Token not found (already revoked or never existed) - returning 200 per RFC 7009")
            ok-response)

          ;; Client mismatch - this is an authentication error, return 400
          (core/clients-doesnt-match? session params)
          (do
            (log/errorf "[%s] Couldn't revoke token - client mismatch" session)
            invalid-client)

          ;; Valid revocation
          :else
          (do
            (log/debugf "[%s] Revoking token %s %s" session token-key token)
            (revoke-token token-key token)
            ok-response))))))

(defn delete [tokens]
  (swap! *tokens*
         (fn [state]
           (reduce-kv
            (fn [state token-type tokens-to-delete]
              (update state token-type #(apply dissoc % tokens-to-delete)))
            state
            tokens))))

;; =============================================================================
;; Custom Scopes (access_token)
;; =============================================================================

(defscope roles [:roles]
  :token :access_token
  :description "Your assigned roles"
  :resolve (fn [session]
             (let [{:keys [roles]} (core/get-session-resource-owner session)
                   dataset-roles (dataset/search-entity
                                  :iam/user-role
                                  {(id/key) {:_in roles}}
                                  {:name nil})]
               {:roles (map (comp csk/->snake_case_keyword :name) dataset-roles)})))

(defscope groups [:groups]
  :token :access_token
  :description "Your group memberships"
  :resolve (fn [session]
             (let [{:keys [groups]} (core/get-session-resource-owner session)
                   dataset-groups (dataset/search-entity
                                   :iam/user-group
                                   {(id/key) {:_in groups}}
                                   {:name nil})]
               {:groups (map (comp csk/->snake_case_keyword :name) dataset-groups)})))

(defscope permissions [:permissions]
  :token :access_token
  :description "Your permissions"
  :resolve (fn [session]
             (let [{:keys [roles]} (core/get-session-resource-owner session)]
               {:permissions (access/roles-scopes roles)})))

(defscope super [:super]
  :token :access_token
  :description "Superuser status"
  :resolve (fn [session]
             (let [{:keys [roles]} (core/get-session-resource-owner session)]
               {:super (access/superuser? roles)})))

;; NOTE: process-scope methods for roles, groups, permissions, super
;; are auto-generated by defscope macro above

;; =============================================================================
;; Special UUID Scopes (manual - different claim keys)
;; =============================================================================

(defmethod process-scope "sub:uuid"
  [session tokens _]
  (let [resource-owner (core/get-session-resource-owner session)
        user-id (id/extract resource-owner)]
    (assoc-in tokens [:access_token "sub:uuid"] (str user-id))))

(defmethod process-scope "roles:uuid"
  [session tokens _]
  (let [{:keys [roles]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token :roles] roles)))

(defmethod process-scope "groups:uuid"
  [session tokens _]
  (let [{:keys [groups]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token :groups] groups)))

(comment
  (def tokens nil)
  (access/roles-scopes #{#uuid "8ebc60f1-8df0-48c8-a9b6-747a140df021"})
  (def session "RkJDHRzznXwlkatsVQnLWMmJHRWdyg"))
