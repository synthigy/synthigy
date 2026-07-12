(ns synthigy.oauth.core
  (:require
   [buddy.core.codecs :as codecs]
   [buddy.core.crypto :as crypto]
   [buddy.hashers :as hashers]
   [clojure.core.async :as async]
   [synthigy.json :as json]
   clojure.pprint
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [synthigy.log :as log]
   [clojure.walk :refer [keywordize-keys]]
   [nano-id.core :as nano-id]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.env :as env]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.iam.connector :as connector]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.page.error :as error-page]
   [synthigy.util :as util])
  (:import
   [java.util Base64]))

(defn pprint [data] (with-out-str (clojure.pprint/pprint data)))

(defonce ^:dynamic *resource-owners* (atom nil))
(defonce ^:dynamic *clients* (atom nil))
(defonce ^:dynamic *sessions* (atom nil))
(defonce ^:dynamic *domain* nil)

(defn domain+
  ([] (domain+ ""))
  ([path]
   (str (or env/iam-root-url *domain*) path)))

(defonce ^:dynamic *encryption-key* (nano-id/nano-id 32))
(defonce ^:dynamic *initialization-vector* (nano-id/nano-id 12))

(defn encrypt
  [data]
  (let [json-data (.getBytes (json/->json data))]
    (String.
     (codecs/bytes->b64
      (crypto/encrypt
       json-data *encryption-key* *initialization-vector*
       {:alg :aes256-gcm})))))

(defn decrypt [encrypted-data]
  (try
    (json/read-str
     (String.
      (crypto/decrypt
       (codecs/b64->bytes encrypted-data) *encryption-key* *initialization-vector*
       {:alg :aes256-gcm})))
    (catch Throwable _ nil)))

; (comment
;   (time
;    (decrypt
;     (encrypt
;      {:device-code 100
;       :user-code 200
;       :ip "a"
;       :user-agent "jfioq"}))))
;
(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"]
  (def gen-session-id (nano-id/custom alphabet 30)))

(defn short-id
  "First 6 chars of an opaque id, for log correlation. Returns nil for nil/blank
   so map keys stay omittable. Long enough to correlate rows in a query window,
   short enough that a leaked log can't replay a session."
  [s]
  (when (and (string? s) (pos? (count s)))
    (subs s 0 (min 6 (count s)))))

(let [default (util/hours 2)]
  (defn access-token-expiry
    [{{{expiry "access"} "token-expiry"} :settings}]
    (or expiry default)))

(let [default (util/days 1.5)]
  (defn refresh-token-expiry
    [{{{expiry "refresh"} "token-expiry"} :settings}]
    (long (or expiry default))))

(defn expired?
  [token]
  (try
    (let [{:keys [exp]} (encryption/unsign-data token)]
      (if (some? exp)
        (< (* 1000 exp) (System/currentTimeMillis))
        ;; If no exp claim, treat as invalid/expired
        true))
    (catch clojure.lang.ExceptionInfo ex
      (let [{:keys [cause]} (ex-data ex)]
        (if (= cause :exp)
          true
          (throw ex))))))

(defn expires-at
  [token]
  (try
    (let [{:keys [exp]} (encryption/unsign-data token)]
      (java.util.Date. (* 1000 exp)))
    (catch clojure.lang.ExceptionInfo ex
      (let [{:keys [cause]} (ex-data ex)]
        (if (= cause :exp)
          (java.util.Date. 0)
          (throw ex))))))

(defmulti sign-token (fn [_ token-key _] token-key))

(defmethod sign-token :default
  [session token-key data]
  (log/error {:id ::sign-token-no-method
              :data {:session session :token-key token-key}}
             "Couldn't sign token (no method)")
  data)

(defn get-session-client [session]
  (let [euuid (get-in @*sessions* [session :client])]
    (get @*clients* euuid)))

(defprotocol LazyOAuth
  (get-resource-owner [this]))

(extend-protocol LazyOAuth
  java.util.UUID
  (get-resource-owner [this] (get @*resource-owners* this))
  java.lang.String
  (get-resource-owner [this]
    (if-some [user-id (get-in @*resource-owners* [::name-mapping this])]
      (get-resource-owner user-id)
      (let [{name :name
             :as resource-owner} (iam/get-user-details this)
            user-id (id/extract resource-owner)]
        (swap! *resource-owners*
               (fn [resource-owners]
                 (->
                  resource-owners
                  (assoc user-id resource-owner)
                  (update ::name-mapping assoc name user-id))))
        resource-owner))))

(defn get-session-resource-owner [session]
  (let [euuid (get-in @*sessions* [session :resource-owner])]
    (get @*resource-owners* euuid)))

(defmulti process-scope (fn [_ _ scope] scope))

(defmethod process-scope :default
  [session tokens scope]
  (let [{:keys [roles]} (get-session-resource-owner session)
        user-scopes (access/roles-scopes roles)]
    (if (contains? user-scopes scope)
      (update-in tokens [:access_token :scope] (fnil conj []) scope)
      tokens)))

;; =============================================================================
;; Scope → Claims Mapping (OIDC Discovery + Resolution)
;; =============================================================================

(defmulti claims-for
  "Returns scope descriptor: {:claims [...] :resolve (fn [session] {...})}.
   If :resolve is nil, claims are looked up from user's person_info."
  identity)

(defmethod claims-for :default [_] nil)

(defmacro defscope
  "Define an OIDC scope with its claims and token target.

   Options:
     :token       - Target token(s): :id_token, :access_token, or #{:id_token :access_token}
                    Defaults to :id_token
     :resolve     - Custom resolution fn (fn [session] -> claims-map)
                    If nil, claims are looked up from person_info
     :description - Human-readable description for consent UI

   Generates both claims-for method (discovery) and process-scope method (token generation)."
  [scope-name claims & {:keys [resolve description token] :or {token :id_token}}]
  (let [scope-str (name scope-name)
        tokens (if (coll? token) token #{token})
        claims-vec (vec (map keyword claims))]
    `(do
       ;; Discovery: claims-for multimethod
       (defmethod claims-for ~scope-str [~'_]
         (hash-map :claims ~claims-vec
                   :token ~tokens
                   ~@(when resolve [:resolve resolve])
                   ~@(when description [:description description])))

       ;; Token generation: process-scope multimethod
       (defmethod process-scope ~scope-str [~'session ~'tokens ~'_]
         (let [~'claims (resolve-scope-claims ~scope-str ~'session)]
           ~(if (= 1 (count tokens))
              `(update ~'tokens ~(first tokens) merge ~'claims)
              `(-> ~'tokens
                   ~@(for [t tokens]
                       `(update ~t merge ~'claims)))))))))

(defn resolve-scope-claims
  "Resolve claims for a scope. Uses :resolve fn if provided,
   otherwise looks up claims from person_info."
  [scope session]
  (when-let [{:keys [claims resolve]} (claims-for scope)]
    (if resolve
      (resolve session)
      (select-keys (:person_info (get-session-resource-owner session))
                   claims))))

(defn all-supported-scopes []
  (vec (remove #{:default} (keys (methods claims-for)))))

(defn all-supported-claims []
  (into #{}
        (mapcat (comp :claims claims-for)
                (all-supported-scopes))))

(defn scope-info-for-consent
  "Returns scope info for consent UI: [{:scope 'email' :description '...' :claims [...]}]"
  [requested-scopes]
  (for [scope requested-scopes
        :let [{:keys [claims description]} (claims-for scope)]
        :when claims]
    {:scope scope
     :description (or description scope)
     :claims claims}))

(defn token-error [code & description]
  ; (log/debugf "Returning error: %s\n%s" code (str/join "\n" description))
  {:status 400
   :headers {"Content-Type" "application/json;charset=UTF-8"
             "Pragma" "no-cache"
             "Cache-Control" "no-store"}
   :body (json/write-str
          {:error code
           :error_description (str/join "\n" description)})})

(defn get-base-uri
  "Returns the base URI without query parameters from the given URL."
  [url]
  (when (not-empty url)
    (let [uri (java.net.URI. url)]
      (str (java.net.URI. (.getScheme uri) (.getAuthority uri) (.getPath uri) nil nil)))))

(defn localhost-redirect?
  "Returns true if the URI is a loopback redirect (localhost or 127.0.0.1).
  Per RFC 8252, loopback redirects are allowed without pre-registration."
  [url]
  (when (not-empty url)
    (try
      (let [uri (java.net.URI. url)
            host (.getHost uri)]
        (or (= host "localhost") (= host "127.0.0.1")))
      (catch Exception _ false))))

(defn validate-resource-owner
  "Verify a resource-owner's credentials by walking the connector chain
   delivered by the active CredentialsProvider (set when the database
   backend boots). Returns the user map on success, nil on any failure
   (deny). The framework JIT-creates the local user record on the first
   successful authentication via an external connector."
  [username password]
  (connector/authenticate {:username username :password password}))

(defn set-session-resource-owner
  [session {username :name
            :as resource-owner}]
  (let [user-id (id/extract resource-owner)]
    (swap! *sessions* assoc-in [session :resource-owner] user-id)
    (swap! *resource-owners*
           (fn [resource-owners]
             (->
              resource-owners
              (update user-id (fn [current]
                                (->
                                 current
                                 (merge resource-owner)
                                 (update :sessions (fnil conj #{}) session))))
              (update ::name-mapping assoc username user-id))))
    nil))

(defn remove-session-resource-owner [session]
  (let [euuid (get-in @*sessions* [session :resource-owner])]
    (swap! *sessions* update session dissoc :resource-owner)
    (when euuid
      (swap! *resource-owners*
             (fn [resource-owners]
               (let [{{:keys [sessions]} euuid
                      :as resource-owners}
                     (update-in resource-owners [euuid :sessions] (fnil disj #{}) session)]
                 (if (empty? sessions)
                   (dissoc resource-owners euuid)
                   resource-owners)))))
    nil))

(defn set-session-audience-scope
  ([session scope] (set-session-audience-scope session nil scope))
  ([session audience scope]
   (swap! *sessions* assoc-in [session :scopes audience] scope)))

(defn get-session-audience-scope
  ([session] (get-session-audience-scope session nil))
  ([session audience]
   (get-in @*sessions* [session :scopes audience])))

(defn clients-match?
  "Validates client credentials against session client.

  Args:
    session - Session ID to look up client
    params - Map with :client_id and :client_secret

  Returns:
    true if client ID matches and secret validates, false otherwise"
  [session {:keys [client_id client_secret]}]
  (let [{known-id :id
         known-secret :secret} (get-session-client session)]
    (cond
      ;; Client IDs must match
      (not= client_id known-id)
      false

      ;; If no secret is required (public client), allow
      (nil? known-secret)
      true

      ;; If secret is required but not provided, reject
      (nil? client_secret)
      false

      ;; Validate hashed secret
      :else
      (hashers/check client_secret known-secret))))

(def clients-doesnt-match? (complement clients-match?))

(defn get-client
  "Resolve an OAuth client by client_id.

   Reads the in-memory `*clients*` cache first — the same cache the
   session path already trusts via `get-session-client`, kept fresh by
   `reload-clients` (every ~5s + on client deltas). Falls back to a DB
   fetch + cache populate on miss. Without the cache read this is a DB
   round-trip on every authenticated request (it runs in the hot path
   via `synthigy.server.auth/token->user-context`)."
  [id]
  (or (some (fn [c] (when (= id (:id c)) c)) (vals @*clients*))
      (when-let [client (iam/get-client id)]
        (swap! *clients* update (id/extract client) merge client)
        client)))

(defn server-login-page-url
  "Returns the URL where the server-level custom login page is mounted, or
   nil when SYNTHIGY_LOGIN_PAGE_PATH is unset. The URL has a trailing slash
   so the browser resolves relative asset paths in the user's index.html
   under the mount prefix."
  []
  (when (seq env/login-page-path)
    "/login/"))

(defn get-client-login-url
  "Returns the login URL for a client.

   Resolution order:
     1. client[:settings]['login-page'] (validated relative path)
     2. server-level SYNTHIGY_LOGIN_PAGE_PATH (mounted at /login)
     3. built-in /oauth/login

   Security: Only relative paths (starting with '/') are allowed in the
   per-client setting. Throws an exception if an invalid URL is configured
   (absolute URLs, path traversal, etc.) to prevent credential phishing."
  [client]
  (let [custom-page (get-in client [:settings "login-page"])
        client-name (:name client)]
    (cond
      ;; No per-client page - use server fallback or built-in
      (or (nil? custom-page) (empty? custom-page))
      (or (server-login-page-url) "/oauth/login")

      ;; Valid relative path
      (and (string? custom-page)
           (str/starts-with? custom-page "/")
           (not (str/includes? custom-page "..")))
      custom-page

      ;; Invalid configuration - throw error
      :else
      (throw
       (ex-info
        (str "Client '" client-name "' has invalid login-page setting. "
             "Only relative URLs starting with '/' are allowed.")
        {:type "invalid_login_page"
         :client-name client-name
         :invalid-value custom-page})))))

(defn remove-session-client [session]
  (let [euuid (get-in @*sessions* [session :client])]
    (swap! *sessions* update session dissoc :client)
    (when euuid
      (swap! *clients* update euuid
             (fn [current]
               (update current :sessions (fnil disj #{}) session))))
    nil))

(defn get-session [id] (get @*sessions* id))
(defn set-session [id data] (swap! *sessions* assoc id data))
(defn remove-session [id]
  (let [{client :client
         resource-owner :resource-owner} (get-session id)
        client-id (when client (id/extract client))
        resource-owner-id (when resource-owner (id/extract resource-owner))]
    (swap! *sessions* dissoc id)
    ;; Remove session from resource owner
    (when resource-owner
      (swap! *resource-owners* update resource-owner-id
             (fn [current]
               (update current :sessions (fnil disj #{}) id))))
    ;; Remove session from client
    (when client
      (swap! *clients* update client-id
             (fn [current]
               (update current :sessions (fnil disj #{}) id))))
    nil))

(defn get-session-tokens [session]
  (reduce
   (fn [r tokens]
     (reduce
      (fn [r [k token]] (update r k (fnil conj []) token))
      r
      (partition 2 (interleave (keys tokens) (vals tokens)))))
   nil
   (vals (:tokens (get-session session)))))

(comment
  (def session "YiSclFaUmCFKhSRrHNshCYDKlMjQKz"))

(defn get-redirection-uris [session]
  (let [{{:strs [redirections]} :settings} (get-session-client session)]
    redirections))

(def request-errors
  (reduce-kv
   (fn [r k v]
     (assoc r k (str/join "\n" v)))
   nil
   {"invalid_request"
    ["The request is missing a required parameter, includes an"
     "invalid parameter value, includes a parameter more than"
     "once, or is otherwise malformed."]
     ;;
    "unauthorized_client"
    ["The client is not authorized to request an authorization"
     "code using this method."]
     ;;
    "access_denied"
    ["The resource owner or authorization server denied the"
     "request."]
     ;;
    "unsupported_response_type"
    ["The authorization server does not support obtaining an"
     "authorization code using this method."]
     ;;
    "invalid_scope"
    ["The requested scope is invalid, unknown, or malformed."]
     ;;
    "server_error"
    ["The authorization server encountered an unexpected"
     "condition that prevented it from fulfilling the request."]
     ;;
    "temporarily_unavailable"
    ["The authorization server is currently unable to handle"
     "the request due to a temporary overloading or maintenance"
     "of the server."]}))

(defn- render-error-page
  "Renders an OAuth error page directly (400 Bad Request).

  Used when redirect_uri is invalid/missing per OAuth 2.0 spec:
  'the authorization server MUST inform the resource owner of the error
   and MUST NOT automatically redirect the user-agent to the invalid
   redirection URI' (RFC 6749 Section 4.1.2.1)"
  ([error-type] (render-error-page error-type nil))
  ([error-type context]
   (error-page/render-error-page error-type context)))

(defn handle-request-error
  "Handles OAuth authorization errors according to RFC 6749.

  For errors where redirect_uri is invalid/missing/mismatched:
  - Returns 400 Bad Request with error page (MUST NOT redirect to client)

  For other errors:
  - Redirects to client's redirect_uri with error parameters

  Optional context map provides dynamic data for certain error types
  (e.g., client-name and invalid-value for invalid_login_page)."
  [{t :type
    session :session
    request :request
    description :description
    :as error-data}]
  (let [{:keys [state redirect_uri]} (or
                                      request
                                      (:request (get-session session)))
        base-redirect-uri (get-base-uri redirect_uri)
        ;; Extract context for error page (client-name, invalid-value, etc.)
        context (select-keys error-data [:client-name :invalid-value])]
    (when session (remove-session session))
    (case t
      ;; OAuth 2.0 spec (RFC 6749 4.1.2.1): When redirect_uri is invalid/missing,
      ;; MUST NOT redirect to client - show error page directly
      ;; Also includes configuration errors that prevent safe redirect
      ("no_redirections"
       "missing_redirect"
       "redirect_missmatch"
       "missing_response_type"
       "client_not_registered"
       "corrupt_session"
       "unsupported_grant_type"
       "invalid_login_page")
      (render-error-page t context)

      ;; For other errors with valid redirect_uri: redirect to client with error
      {:status 302
       :headers {"Location" (str base-redirect-uri "?"
                                 (codec/form-encode
                                  (cond->
                                   {:error t}
                                    description (assoc :error_description description)
                                    state (assoc :state state))))
                 "Cache-Control" "no-cache"}})))

(defn set-session-authorized-at
  [session timestamp]
  (swap! *sessions* assoc-in [session :authorized-at] timestamp))

(defn get-session-authorized-at
  [session]
  (get-in @*sessions* [session :authorized-at]))

;; =============================================================================
;; ACR/AMR (Authentication Context/Methods) - OIDC Core 1.0
;; =============================================================================

(def ^:const acr-levels
  "Standard ACR (Authentication Context Class Reference) levels.
   Higher number = stronger authentication."
  {"0" {:level 0 :name "No assurance" :methods #{}}
   "1" {:level 1 :name "Password-based" :methods #{"pwd"}}
   "2" {:level 2 :name "Multi-factor" :methods #{"pwd" "otp" "sms" "hwk" "bio"}}
   "urn:mace:incommon:iap:bronze" {:level 1 :name "Bronze" :methods #{"pwd"}}
   "urn:mace:incommon:iap:silver" {:level 2 :name "Silver" :methods #{"pwd" "otp"}}
   "urn:mace:incommon:iap:gold"   {:level 3 :name "Gold" :methods #{"pwd" "hwk"}}})

(defn set-session-amr
  "Set authentication methods references for session.

   amr is a vector of method strings used during authentication.
   Standard values: pwd, otp, sms, mfa, hwk, bio, pin, wia, kba"
  [session amr]
  (swap! *sessions* assoc-in [session :amr] (vec amr)))

(defn get-session-amr
  "Get authentication methods references for session.
   Returns vector of method strings, e.g. [\"pwd\"] or [\"pwd\" \"otp\"]"
  [session]
  (get-in @*sessions* [session :amr] ["pwd"]))  ; Default to password

(defn set-session-acr
  "Set authentication context class reference for session.

   acr is a string indicating the authentication assurance level.
   Standard values: 0, 1, 2, or URN like urn:mace:incommon:iap:silver"
  [session acr]
  (swap! *sessions* assoc-in [session :acr] acr))

(defn get-session-acr
  "Get authentication context class reference for session.
   Returns acr string, defaults to '1' (password-based)."
  [session]
  (get-in @*sessions* [session :acr] "1"))  ; Default to level 1 (password)

(defn derive-acr-from-amr
  "Derive ACR level from AMR methods.

   - Single password → '1' (basic)
   - Password + any second factor → '2' (multi-factor)"
  [amr]
  (let [methods (set amr)]
    (cond
      ;; Multi-factor: password + another factor
      (and (contains? methods "pwd")
           (some methods #{"otp" "sms" "hwk" "bio" "mfa"}))
      "2"

      ;; Hardware key alone is strong
      (contains? methods "hwk")
      "2"

      ;; Biometric alone is strong
      (contains? methods "bio")
      "2"

      ;; Password only
      (contains? methods "pwd")
      "1"

      ;; No recognized method
      :else
      "0")))

(defmulti session-kill-hook (fn [priority _] priority))

(defn kill-session
  [session]
  (let [session-data (get-session session)
        resource-owner (get-session-resource-owner session)
        {client-id :id} (get-session-client session)]
    (doseq [p (sort (keys (methods session-kill-hook)))]
      (session-kill-hook p session))
    (remove-session-resource-owner session)
    (remove-session-client session)
    (swap! *sessions* dissoc session)
    (log/info {:id ::session-killed
               :user-xid (:xid resource-owner)
               :data {:action :killed
                      :subject :session
                      :session (short-id session)
                      :client client-id
                      :flow (:flow session-data)}}
              "OAuth session killed")
    (iam/publish
     :oauth.session/killed
     {:session session
      :data session-data})))

(defn kill-sessions
  []
  (doseq [[session] @*sessions*]
    (kill-session session)))

(s/def ::authorization-code-grant #{"authorization_code"})
(s/def ::password-grant #{"password"})
(s/def ::implict-grant #{"implicit"})
(s/def ::refresh-token-grant #{"refresh_token"})
(s/def ::client-credentials-grant #{"client_credentials"})

(s/def ::grant_type
  (s/or
   :authorization-code ::authorization-code-grant
   :password ::password-grant
   :refresh-token ::refresh-token-grant
   :implicit ::implict-grant))

(defn json-error
  [status & description]
  (let [_status (if (number? status) status 400)
        [code & description] (if (number? status)
                               description
                               (concat [status] description))]
    {:status _status
     :headers {"Content-Type" "application/json;charset=UTF-8"
               "Pragma" "no-cache"
               "Cache-Control" "no-store"}
     :body (json/write-str
            {:error code
             :error_description (str/join "\n" description)})}))

;; Interceptors
(defn- decode-base64-credentials
  [data]
  (when data
    (let [decoded-bytes (.decode (Base64/getDecoder) (.getBytes data "UTF-8"))
          decoded (String. decoded-bytes)]
      (str/split decoded #":" 2))))

;; =============================================================================
;; Ring Middleware (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn wrap-basic-authorization
  "Ring middleware to parse HTTP Basic Authorization header.

   Extracts client_id and client_secret from Authorization header and
   adds them to :params for OAuth client authentication.

   Example:
     Authorization: Basic base64(client_id:client_secret)
     -> {:params {:client_id \"...\" :client_secret \"...\"}}"
  [handler]
  (fn [request]
    (let [authorization (get-in request [:headers "authorization"])]
      (if-not authorization
        (handler request)
        (let [[_ credentials] (re-find #"Basic\s+(.*)" authorization)
              [id secret] (decode-base64-credentials credentials)]
          (handler (update request :params assoc
                           :client_id id
                           :client_secret secret)))))))

;; =============================================================================
(defn original-uri
  "Construct original URI from request, handling X-Forwarded-* headers.

   Used by OAuth handlers to build absolute URIs for redirects and discovery."
  [{original-uri :uri
    {forwarded-host "x-forwarded-host"
     forwarded-proto "x-forwarded-proto"
     host "host"} :headers
    :keys [scheme]}]
  (format
   "%s://%s"
   (or forwarded-proto (name scheme))
   (or forwarded-host host original-uri)))

;; =============================================================================
;; Ring Middleware (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn wrap-keywordize-params
  "Ring middleware to keywordize parameter keys.

   Converts string keys in :params to keywords.
   Example: {\"grant_type\" \"code\"} -> {:grant_type \"code\"}"
  [handler]
  (fn [request]
    (handler (update request :params keywordize-keys))))

(defn wrap-scope->set
  "Ring middleware to convert scope string to set.

   OAuth scopes are space-separated strings that need to be converted
   to sets for processing.
   Example: \"openid profile\" -> #{\"openid\" \"profile\"}"
  [handler]
  (fn [request]
    (handler (update-in request [:params :scope]
                        (fn [scope]
                          (when scope
                            (set (str/split scope #"\s+"))))))))

(defn wrap-session-read
  "Ring middleware to read session ID from cookie.

   Reads 'idsrv.session' cookie and adds it to :params as :idsrv/session
   for OAuth flows that require session tracking."
  [handler]
  (fn [request]
    (let [idsrv-session (get-in request [:cookies "idsrv.session" :value])]
      (if (empty? idsrv-session)
        (handler request)
        (handler (assoc-in request [:params :idsrv/session] idsrv-session))))))

;; Maintenance
(defn clean-sessions
  "Function will clean all sessions that were issued
  authorization code, but code wasn't used for more
  than timeout value. Default timeout is 1 minute"
  ([] (clean-sessions (util/minutes 1)))
  ([timeout]
   (let [now (util/now)]
     (doseq [[session {:keys [authorized-at tokens]}] @*sessions*
             :let [authorized-at (when authorized-at (.getTime ^java.util.Date authorized-at))]
             :when (or
                    (nil? authorized-at)
                    (and
                     (< (+ authorized-at timeout) now)
                     (every? empty? (vals tokens))))]
       (log/debug {:id ::session-timed-out
                   :data {:session session}}
                  "Session timed out: no code or access token assigned")
       (kill-session session)))))

(defn reload-clients
  []
  (try
    (let [ids (remove nil? (map :id (vals @*clients*)))
          new-clients (iam/get-clients ids)]
      (swap! *clients*
             (fn [old-clients]
               (merge-with
                merge old-clients
                (reduce
                 (fn [r client]
                   (assoc r (id/extract client) client))
                 nil
                 new-clients)))))
    (catch Throwable ex
      (log/error! {:id ::reload-clients-failed
                   :msg "Couldn't reload clients"
                   :data {:action :reloading :subject :oauth-clients}}
                  ex))))

(def ^:private client-monitor-sub-key ::client-cache-monitor)

(defn- debounced-client-reload
  "Re-arms a 5s timer on each delta; only the latest call's timer
   actually fires `reload-clients`."
  []
  (let [latest (atom 0)]
    (fn [_env]
      (let [tick (swap! latest inc)]
        (async/go
          (async/<! (async/timeout 5000))
          (when (= tick @latest)
            (log/info {:id ::reloading-clients
                       :data {:action :reloading :subject :oauth-clients}}
                      "Reloading clients")
            (try (reload-clients)
                 (catch Throwable e
                   (log/error! {:id ::reload-clients-debounced-failed} e)))))))))

(defn monitor-client-change
  "Idempotent: registers (or re-registers) a delta subscription on the
   App entity. The maintenance agent calls this periodically; same key
   ⇒ same subscription replaces in place, no go-loop accumulation."
  []
  (when-let [app-xid (some-> (id/entity :iam/app) str)]
    (delta/subscribe! client-monitor-sub-key
                      {:entity-xids #{app-xid}}
                      (debounced-client-reload))))
