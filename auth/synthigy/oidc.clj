(ns synthigy.oidc
  (:require
   [buddy.core.codecs]
   [buddy.sign.util :refer [to-timestamp]]
   [clojure.data.json :as json]
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth :as oauth]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.core :as core
    :refer [process-scope
            defscope
            resolve-scope-claims
            all-supported-scopes
            all-supported-claims
            sign-token
            domain+
            get-session
            get-session-client
            get-session-resource-owner]]
   [synthigy.oauth.device-code :as dc]
   [synthigy.oauth.login :as login]
   [synthigy.oauth.token
    :refer [get-token-session]]
   [timing.core :as timing]))

(s/def ::iss string?)
(s/def ::sub string?)
(s/def ::aud string?)
(s/def ::exp number?)
(s/def ::iat number?)
(s/def ::auth_time number?)
(s/def ::nonce string?)
(s/def ::acr string?)
(s/def ::amr (s/coll-of string? :kind sequential?))

(comment
  (time (s/valid? ::amr ["jfioq" 100])))

(def explain-id-key
  {::iss "Issuer Identifier"
   ::sub "Subject Identifier"
   ::aud "Audience(s). Must contain client_id of relying party"
   ::exp "Expiration time"
   ::iat "issued at"
   ::auth_time "Tim ewhen the end-user authentication occured"
   ::nonce "String value used to associate a client session with an ID Token to mitigate replay attacks"
   ::acr "Authentication Context Class Reference... WTF"
   ::amr "Authentication Methods References. Array of strings that are identifiers for used methods of authentication. Maybe password + OTP"
   ::azp "Authorized Party. The party to which ID token was issued. Ignore for now"})

(s/def ::id-token (s/keys
                   :req-un [::iss ::sub ::aud ::exp ::iat]
                   :opt-un [::auth_time ::nonce ::act ::amr]))

(s/def ::display #{"page" "popup" "touch" "wap"})

(s/def ::scope set?)

(s/def ::code-response (partial = #{"code"}))
(s/def ::implicit-id-response (partial = #{"id_token"}))
(s/def ::implicit-all-response (partial = #{"id_token" "token"}))
(s/def ::hybrid-id-reponse (partial = #{"code" "id_token"}))
(s/def ::hybrid-token-response (partial = #{"code" "token"}))
(s/def ::hybrid-all-response (partial = #{"code" "id_token" "token"}))

(s/def ::authorization-code-flow ::code-response)
(s/def ::implicit-flow
  (s/or :id ::implicit-id-response
        :id+token ::implicit-all-response))
(s/def ::hybrid-flow
  (s/or :id ::hybrid-id-reponse
        :token ::hybrid-token-response
        :id+token ::hybrid-all-response))

(s/def ::flow
  (s/or
   :code ::authorization-code-flow
   :implicit ::implicit-flow
   :hybrid ::hybrid-flow))

(s/def ::response_type
  (s/or
   :code ::code-response
   :implicit-id ::implicit-id-response
   :implicit-all ::implicit-all-response
   :hybrid-id ::hybrid-id-reponse
   :hybrid-token-response ::hybrid-token-response
   :hybrid-all-response ::hybrid-all-response))

(s/def ::client_id string?)
(s/def ::redirect_uri (s/and string? not-empty))

(s/def ::prompt #{"login" "page" "popup" "none"})
(s/def ::prompt-is-none #{"none"})

(s/def ::authentication-request-keys
  (s/keys
   :req-un [::scope ::response_type ::client_id ::redirect_uri]
   :opt-un [::state ::response_mode ::nonce ::display ::prompt
            ::max_age ::ui_locales ::id_token_hint
            ::login_hint ::acr_values]))

(s/def ::open-id-scope
  (fn [{:keys [scope]}] (contains? scope "openid")))

(s/def ::authentication-request
  (s/and
   ::authentication-request-keys
   ::open-id-scope))

(comment
  (s/valid? ::display "popup")
  (s/valid? ::open-id-scope? {:scope ["ifejoq"]})
  (def request
    {:scope #{"openid"}
     :response_type #{"code"}
     :client_id "f019uj391r9231"
     :redirect_uri "http://localhost:8080/synthigy"})

  (s/explain ::code-response (:response_type request))
  (s/conform ::flow (:response_type request))
  (s/valid?
   ::authentication-request
   {:scope #{"openid"}
    :response_type #{"code"}
    :client_id "f019uj391r9231"
    :redirect_uri "http://localhost:8080/synthigy"}))

(letfn [(config []
          {:issuer (domain+)
           :authorization_endpoint (domain+ "/oauth/authorize")
           :device_authorization_endpoint (domain+ "/oauth/device")
           :token_endpoint (domain+ "/oauth/token")
           :userinfo_endpoint (domain+ "/oauth/userinfo")
           :jwks_uri (domain+ "/oauth/jwks")
           :end_session_endpoint (domain+ "/oauth/logout")
           :revocation_endpoint (domain+ "/oauth/revoke")
           ; :response_types_supported ["code" "token" "id_token"
                                       ;                            "code id_token" "token id_token"
                                       ;                            "code token id_token"]
           :response_types_supported ["urn:ietf:params:oauth:grant-type:device_code"
                                      "code"]
           :subject_types_supported ["public"]
           :token_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
           :scopes_supported ["openid" "profile" "offline_access"
                              "name" "given_name" "family_name" "nickname"
                              "email" "email_verified" "picture"
                              "created_at" "identities" "phone" "address"]})])

(defn standard-claim
  [session claim]
  (get-in
   (get-session-resource-owner session)
   [:person_info claim]))

(defn add-standard-claim
  [tokens session claim]
  (assoc-in tokens [:id_token claim] (standard-claim session claim)))

(let [default (timing/minutes 30)]
  (defn id-token-expiry
    [{{{expiry "id"} "token-expiry"} :settings}]
    (or expiry default)))

;; =============================================================================
;; OIDC Standard Scopes (RFC 5.4)
;; =============================================================================

(defscope openid [:sub :iss :aud :exp :iat :auth_time :nonce :sid :acr :amr]
  :description "Your identity"
  :resolve (fn [session]
             (let [{:keys [name]} (get-session-resource-owner session)
                   {:keys [authorized-at code]} (get-session session)
                   {:keys [nonce]} (ac/get-code-request code)
                   client (get-session-client session)
                   ;; Get authentication context (acr/amr)
                   amr (core/get-session-amr session)
                   acr (core/get-session-acr session)]
               {:iss (domain+)
                :aud (:id client)
                :sub name
                :iat (to-timestamp (timing/date))
                :exp (-> (timing/date)
                         timing/date->value
                         (+ (id-token-expiry client))
                         timing/value->date
                         to-timestamp)
                :sid session
                :auth_time authorized-at
                :nonce nonce
                ;; OIDC Core 1.0 Section 2 - acr/amr claims
                :acr acr   ; Authentication Context Class Reference
                :amr amr})))  ; Authentication Methods References

(defscope profile
  [:name :family_name :given_name :middle_name :nickname
   :preferred_username :profile :picture :website
   :gender :birthdate :zoneinfo :locale :updated_at]
  :description "Your profile information")

(defscope email [:email :email_verified]
  :description "Your email address")

(defscope phone [:phone_number :phone_number_verified]
  :description "Your phone number")

(defscope address [:address]
  :description "Your postal address")

;; NOTE: process-scope methods are auto-generated by defscope macro

(defmethod sign-token :id_token
  [session _ data]
  (let [client (get-session-client session)]
    (encryption/sign-data
     (assoc data
            :exp (-> (timing/date)
                     timing/date->value
                     (+ (id-token-expiry client))
                     timing/value->date
                     to-timestamp))
     {:alg :rs256})))

(defn get-access-token
  "Extracts access token from request per RFC 6750.

   Supports three methods (in order of preference):
   1. Authorization header: Bearer <token>
   2. Form body parameter: access_token (for POST requests)
   3. Query parameter: access_token (least secure, not recommended)

   Returns the access token string or throws exception if not found."
  [{:keys [headers params form-params] :as request}]
  (let [authorization (get headers "authorization" "")
        ;; Method 1: Authorization header (preferred)
        header-token (when (and authorization (.startsWith authorization "Bearer"))
                       (subs authorization 7))
        ;; Method 2: Form body parameter (for POST with application/x-www-form-urlencoded)
        form-token (or (:access_token form-params)
                       (get form-params "access_token"))
        ;; Method 3: Query parameter (least secure)
        query-token (or (:access_token params)
                        (get params "access_token"))]
    (or header-token
        form-token
        query-token
        (throw
         (ex-info
          "Access token not found in Authorization header, form body, or query parameters"
          {:headers headers :has-params (some? params)})))))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn- base-server-metadata
  "Returns base OAuth 2.0 server metadata shared between OIDC and OAuth endpoints."
  []
  {:issuer (domain+)
   :authorization_endpoint (domain+ "/oauth/authorize")
   :device_authorization_endpoint (domain+ "/oauth/device")
   :token_endpoint (domain+ "/oauth/token")
   :revocation_endpoint (domain+ "/oauth/revoke")
   :introspection_endpoint (domain+ "/oauth/introspect")
   :jwks_uri (domain+ "/oauth/jwks")
   ;; RFC 7636 PKCE support
   :code_challenge_methods_supported ["S256" "plain"]
   ;; Token endpoint auth methods
   :token_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
   :introspection_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
   :revocation_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
   ;; Response types and grant types
   :response_types_supported ["code"]
   :response_modes_supported ["query" "fragment"]
   :grant_types_supported ["authorization_code"
                           "refresh_token"
                           "client_credentials"
                           "urn:ietf:params:oauth:grant-type:device_code"]
   :scopes_supported (all-supported-scopes)})

(defn oauth-authorization-server-handler
  "OAuth 2.0 Authorization Server Metadata handler (RFC 8414).

   Returns OAuth server metadata at /.well-known/oauth-authorization-server.
   This is the OAuth-specific metadata endpoint (vs OIDC discovery)."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [config (base-server-metadata)]
      {:status 200
       :headers {"Content-Type" "application/json"
                 "Cache-Control" "max-age=3600"}
       :body (json/write-str config :escape-slash false)})))

(defn openid-configuration-handler
  "OpenID Connect Discovery handler.

   Returns OIDC configuration metadata (RFC 8414 + OpenID Connect Discovery 1.0).
   Provides endpoint URLs and supported features for OIDC clients."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [config (merge
                   (base-server-metadata)
                   ;; OIDC-specific fields
                   {:userinfo_endpoint (domain+ "/oauth/userinfo")
                    :end_session_endpoint (domain+ "/oauth/logout")
                    :subject_types_supported ["public"]
                    :id_token_signing_alg_values_supported ["RS256"]
                    :claims_supported (mapv name (all-supported-claims))
                    ;; ACR/AMR support (OIDC Core 1.0 Section 2)
                    :acr_values_supported ["0" "1" "2"
                                           "urn:mace:incommon:iap:bronze"
                                           "urn:mace:incommon:iap:silver"
                                           "urn:mace:incommon:iap:gold"]
                    ;; OIDC optional features
                    :claims_parameter_supported false
                    :request_parameter_supported false
                    :request_uri_parameter_supported false})]
      {:status 200
       :headers {"Content-Type" "application/json"
                 "Cache-Control" "max-age=3600"}
       :body (json/write-str config :escape-slash false)})))

(defn userinfo-handler
  "OpenID Connect UserInfo endpoint handler.

   Returns claims about the authenticated end-user.
   Requires valid access token in Authorization header (Bearer scheme)."
  [request]
  (try
    (let [authorization (get-in request [:headers "authorization"])
          access-token (if (and authorization (.startsWith authorization "Bearer"))
                         (subs authorization 7)
                         (throw
                          (ex-info
                           "Authorization header doesn't contain access token"
                           {:headers (:headers request)})))
          session (get-token-session :access_token access-token)]
      (if-not session
        ;; Token not found (revoked or invalid)
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" "Bearer error=\"invalid_token\""}
         :body (json/write-str {:error "invalid_token"
                                :error_description "Access token is invalid or has been revoked"})}
        ;; Valid session - return user info
        (let [{info :person_info
               :keys [name]} (get-session-resource-owner session)]
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body (json/write-str (-> info
                                     (assoc :sub name)
                                     ;; Add preferred_username from user name if not set
                                     (update :preferred_username #(or % name))))})))
    (catch Throwable _
      {:status 403
       :body "Not authorized"})))

(defn jwks-handler
  "JSON Web Key Set (JWKS) endpoint handler.

   Returns public keys used for verifying JWT signatures (ID tokens).
   Used by OIDC clients to validate tokens without shared secrets."
  [request]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/write-str
          {:keys
           (map
            (fn [{:keys [public]}]
              (encryption/encode-rsa-key public))
            (encryption/list-keypairs encryption/*encryption-provider*))})})

;; =============================================================================
;; Legacy Pedestal Interceptors (will be removed after full conversion)
;; =============================================================================

(defn request-error
  [code & description]
  {:status code
   :headers {"Content-Type" "text/html"}
   :body (json/write-str (str/join "\n" description))})

