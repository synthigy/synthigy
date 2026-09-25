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

(ns synthigy.oidc
  (:require
   [buddy.core.codecs]
   [buddy.sign.util :refer [to-timestamp]]
   [synthigy.json :as json]
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.context :as iam.context]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth :as oauth]
   [synthigy.oauth.core :as core
    :refer [process-scope
            defscope
            resolve-scope-claims
            user-claims
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
    :refer [get-token-session token-revoked?]]
   [synthigy.util :as util]))

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
     :redirect_uri "http://localhost:7887/synthigy"})

  (s/explain ::code-response (:response_type request))
  (s/conform ::flow (:response_type request))
  (s/valid?
   ::authentication-request
   {:scope #{"openid"}
    :response_type #{"code"}
    :client_id "f019uj391r9231"
    :redirect_uri "http://localhost:7887/synthigy"}))

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

(defn granted-claims
  "Claim keys the token's scopes actually grant (OIDC Core 5.4), read from the
   `defscope` registry so adding a scope automatically widens userinfo."
  [scope]
  (into #{}
        (comp (remove str/blank?)
              (mapcat (comp :claims core/claims-for)))
        (str/split (or scope "") #"\s+")))

(defn standard-claim
  [session claim]
  (get (user-claims (get-session-resource-owner session)) claim))

(defn add-standard-claim
  [tokens session claim]
  (assoc-in tokens [:id_token claim] (standard-claim session claim)))

;; Seconds — same unit as access/refresh-token-expiry; consumption sites
;; multiply by 1000.
(let [default (* 60 10)]
  (defn id-token-expiry
    [{{{expiry "id"} "token-expiry"} :settings}]
    (or expiry default)))

;; =============================================================================
;; OIDC Standard Scopes (RFC 5.4)
;; =============================================================================

(defscope openid [:sub :xid :iss :aud :exp :iat :auth_time :nonce :sid :acr :amr]
  :description "Your identity"
  :resolve (fn [session]
             (let [{:keys [name] :as owner} (get-session-resource-owner session)
                   {:keys [authorized-at]} (get-session session)
                   client (get-session-client session)
                   amr (core/get-session-amr session)
                   acr (core/get-session-acr session)]
               {:iss (domain+)
                :aud (:id client)
                :sub name
                :xid (id/extract owner)
                :iat (to-timestamp (java.util.Date.))
                :exp (to-timestamp
                      (java.util.Date. (+ (util/now) (* 1000 (id-token-expiry client)))))
                :sid session
                :auth_time authorized-at
                :acr acr
                :amr amr})))

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
            :exp (to-timestamp
                  (java.util.Date. (+ (util/now) (* 1000 (id-token-expiry client))))))
     {:alg :rs256})))

(defn get-access-token
  "Extract the access token from a request per RFC 6750: Authorization header,
   form body, or query param, in that preference order; throws if not found."
  [{:keys [headers params form-params] :as request}]
  (let [authorization (get headers "authorization" "")
        header-token (when (and authorization (.startsWith authorization "Bearer"))
                       (subs authorization 7))
        form-token (or (:access_token form-params)
                       (get form-params "access_token"))
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

(defn base-server-metadata
  "Base OAuth 2.0 server metadata shared between OIDC and OAuth endpoints."
  []
  {:issuer (domain+)
   :authorization_endpoint (domain+ "/oauth/authorize")
   :device_authorization_endpoint (domain+ "/oauth/device")
   :token_endpoint (domain+ "/oauth/token")
   :revocation_endpoint (domain+ "/oauth/revoke")
   :introspection_endpoint (domain+ "/oauth/introspect")
   :jwks_uri (domain+ "/oauth/jwks")
   ;; S256 only — "plain" is removed by OAuth 2.1/RFC 9700 and rejected by
   ;; wrap-pkce-validation; not advertising it keeps a compliant client from
   ;; ever offering it.
   :code_challenge_methods_supported ["S256"]
   :token_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
   :introspection_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
   :revocation_endpoint_auth_methods_supported ["client_secret_basic" "client_secret_post"]
   :response_types_supported ["code"]
   :response_modes_supported ["query" "fragment"]
   :grant_types_supported ["authorization_code"
                           "refresh_token"
                           "client_credentials"
                           "urn:ietf:params:oauth:grant-type:device_code"]
   :scopes_supported (all-supported-scopes)})

(defn oauth-authorization-server-handler
  "OAuth 2.0 Authorization Server Metadata handler (RFC 8414)."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [config (base-server-metadata)]
      {:status 200
       :headers {"Content-Type" "application/json"
                 "Cache-Control" "max-age=3600"}
       :body (json/write-str config)})))

(defn openid-configuration-handler
  "OpenID Connect Discovery handler (RFC 8414 + OpenID Connect Discovery 1.0)."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [config (merge
                   (base-server-metadata)
                   {:userinfo_endpoint (domain+ "/oauth/userinfo")
                    :end_session_endpoint (domain+ "/oauth/logout")
                    :subject_types_supported ["public"]
                    :id_token_signing_alg_values_supported ["RS256"]
                    :claims_supported (mapv name (all-supported-claims))
                    :acr_values_supported ["0" "1" "2"
                                           "urn:mace:incommon:iap:bronze"
                                           "urn:mace:incommon:iap:silver"
                                           "urn:mace:incommon:iap:gold"]
                    :claims_parameter_supported false
                    :request_parameter_supported false
                    :request_uri_parameter_supported false})]
      {:status 200
       :headers {"Content-Type" "application/json"
                 "Cache-Control" "max-age=3600"}
       :body (json/write-str config)})))

(defn userinfo-handler
  "OpenID Connect UserInfo endpoint; requires a valid Bearer access token."
  [request]
  (try
    (let [authorization (get-in request [:headers "authorization"])
          access-token (when (and authorization (.startsWith authorization "Bearer"))
                         (subs authorization 7))
          ;; Stateless: unsign-data verifies signature but skips exp by design,
          ;; so exp is checked here; token-revoked? adds the RFC 7009 check
          ;; (row-less client_credentials tokens still pass on signature alone).
          claims (some-> access-token encryption/unsign-data)
          valid? (and claims
                      (let [exp (:exp claims)]
                        (or (nil? exp)
                            (> exp (quot (System/currentTimeMillis) 1000))))
                      (not (token-revoked? :access_token access-token)))]
      (if-not valid?
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" "Bearer error=\"invalid_token\""}
         :body (json/write-str {:error "invalid_token"
                                :error_description "Access token is invalid or has been revoked"})}
        ;; OIDC Core 5.4: the response MUST be limited to the claims the
        ;; token's granted scopes cover — this used to return the whole
        ;; person-info map regardless of consent.
        (let [{:keys [name] :as user} (iam.context/get-user-details {:name (:sub claims)})
              granted (granted-claims (:scope claims))
              info (select-keys (user-claims user) granted)]
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body (json/write-str
                  (cond-> (assoc info :sub name :xid (id/extract user))
                    (contains? granted :preferred_username)
                    (update :preferred_username #(or % name))))})))
    (catch Throwable _
      {:status 403
       :body "Not authorized"})))

(defn jwks-handler
  "JSON Web Key Set endpoint — public keys for verifying ID token signatures."
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

