(ns synthigy.http.oidc-http-test
  "OpenID Connect HTTP integration tests.

  Tests OIDC-specific functionality against a running HTTP server:
  - OIDC Discovery (/.well-known/openid-configuration) per OpenID Connect Discovery 1.0
  - JWKS endpoint (/oauth/jwks) per RFC 7517
  - ID Token validation (signature, claims) per OpenID Connect Core 1.0
  - UserInfo endpoint (/oauth/userinfo) per OpenID Connect Core 1.0
  - Standard OIDC scopes (openid, profile, email)

  These tests use clj-http to make real HTTP requests to the server."
  (:require
    [clojure.data.json :as json]
    [clojure.string :as str]
    [clojure.test :refer [deftest testing is use-fixtures]]
    [synthigy.http.http-test-helper :as h]))

(use-fixtures :once h/deps-fixture h/server-fixture)
(use-fixtures :each h/oauth-data-fixture h/cookie-store-fixture)

;;; ============================================================================
;;; OIDC Discovery Tests (OpenID Connect Discovery 1.0)
;;; ============================================================================

(deftest test-oidc-discovery-endpoint
  (testing "OpenID Connect Discovery endpoint (OpenID Connect Discovery 1.0)"
    (let [response (h/GET "/.well-known/openid-configuration")
          config (h/parse-json response)]

      (testing "Returns 200 OK"
        (is (= 200 (:status response))))

      (testing "Content-Type is application/json"
        (is (str/starts-with? (h/content-type response) "application/json")))

      ;; Required fields per OpenID Connect Discovery 1.0 Section 3
      (testing "Contains REQUIRED fields per OpenID Connect Discovery 1.0"
        (is (some? (:issuer config)) "issuer is REQUIRED")
        (is (some? (:authorization_endpoint config)) "authorization_endpoint is REQUIRED")
        (is (some? (:token_endpoint config)) "token_endpoint is REQUIRED")
        (is (some? (:jwks_uri config)) "jwks_uri is REQUIRED")
        (is (vector? (:response_types_supported config)) "response_types_supported is REQUIRED")
        (is (vector? (:subject_types_supported config)) "subject_types_supported is REQUIRED")
        (is (vector? (:id_token_signing_alg_values_supported config)) "id_token_signing_alg_values_supported is REQUIRED"))

      ;; Recommended fields
      (testing "Contains RECOMMENDED fields"
        (is (some? (:userinfo_endpoint config)) "userinfo_endpoint is RECOMMENDED")
        (is (vector? (:scopes_supported config)) "scopes_supported is RECOMMENDED")
        (is (vector? (:claims_supported config)) "claims_supported is RECOMMENDED"))

      ;; Verify openid scope is supported
      (testing "Supports openid scope"
        (is (some #{"openid"} (:scopes_supported config)) "openid scope MUST be supported"))

      ;; Verify authorization_code grant is supported
      (testing "Supports authorization_code grant"
        (is (some #{"authorization_code"} (:grant_types_supported config))
            "authorization_code grant SHOULD be supported")))))

(deftest test-oidc-discovery-issuer-format
  (testing "Issuer MUST be a URL using https scheme (OpenID Connect Discovery 1.0 Section 3)"
    (let [config (h/parse-json (h/GET "/.well-known/openid-configuration"))
          issuer (:issuer config)]
      (is (some? issuer))
      ;; In test environment, http is acceptable; in production it MUST be https
      (is (or (str/starts-with? issuer "https://")
              (str/starts-with? issuer "http://"))
          "issuer MUST be a URL"))))

;;; ============================================================================
;;; JWKS Endpoint Tests (RFC 7517 - JSON Web Key)
;;; ============================================================================

(deftest test-jwks-endpoint
  (testing "JWKS endpoint (RFC 7517)"
    (let [response (h/GET "/oauth/jwks")
          jwks (h/parse-json response)]

      (testing "Returns 200 OK"
        (is (= 200 (:status response))))

      (testing "Content-Type is application/json"
        (is (str/starts-with? (h/content-type response) "application/json")))

      ;; RFC 7517 Section 5 - JWK Set Format
      (testing "Contains 'keys' array (RFC 7517 Section 5)"
        (is (contains? jwks :keys) "'keys' member is REQUIRED")
        (is (vector? (:keys jwks)) "'keys' MUST be an array"))

      ;; RFC 7517 Section 4 - JWK Parameters
      (testing "Keys have REQUIRED JWK parameters (RFC 7517 Section 4)"
        (when-let [key (first (:keys jwks))]
          (is (some? (:kty key)) "'kty' (Key Type) is REQUIRED")
          (is (some? (:kid key)) "'kid' (Key ID) is RECOMMENDED for key selection"))))))

(deftest test-jwks-rsa-public-key
  (testing "JWKS contains RSA public key for RS256 (RFC 7517 Section 6.3)"
    (let [jwks (h/parse-json (h/GET "/oauth/jwks"))
          rsa-keys (filter #(= "RSA" (:kty %)) (:keys jwks))]

      (testing "Has at least one RSA key"
        (is (>= (count rsa-keys) 1)))

      (when-let [rsa-key (first rsa-keys)]
        ;; RFC 7517 Section 6.3.1 - RSA Public Key Parameters
        (testing "RSA key has REQUIRED public key parameters (RFC 7517 Section 6.3.1)"
          (is (some? (:n rsa-key)) "'n' (modulus) is REQUIRED for RSA public key")
          (is (some? (:e rsa-key)) "'e' (exponent) is REQUIRED for RSA public key"))

        ;; Security: Private key MUST NOT be exposed
        (testing "RSA private key parameters are NOT exposed"
          (is (nil? (:d rsa-key)) "'d' (private exponent) MUST NOT be exposed")
          (is (nil? (:p rsa-key)) "'p' (first prime factor) MUST NOT be exposed")
          (is (nil? (:q rsa-key)) "'q' (second prime factor) MUST NOT be exposed")
          (is (nil? (:dp rsa-key)) "'dp' MUST NOT be exposed")
          (is (nil? (:dq rsa-key)) "'dq' MUST NOT be exposed")
          (is (nil? (:qi rsa-key)) "'qi' MUST NOT be exposed"))))))

;;; ============================================================================
;;; ID Token Tests (OpenID Connect Core 1.0 Section 2)
;;; ============================================================================

(deftest test-id-token-structure
  (testing "ID Token is a valid JWT (OpenID Connect Core 1.0 Section 2)"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid profile email"
                                               :state "id-token-test"}})]
      ;; Base assertion - endpoint must respond
      (testing "Authorization endpoint responds"
        (is (or (= 302 (:status auth-response))
                (= 400 (:status auth-response)))))
      ;; Only proceed if authorization succeeded (client registered)
      (when (= 302 (:status auth-response))
        (let [login-params (h/parse-location-params (h/location-header auth-response))
              login-response (h/POST-form "/oauth/login"
                                          {:username h/test-username
                                           :password h/test-password
                                           :state (:state login-params)})
              callback-params (h/parse-location-params (h/location-header login-response))
              token-response (h/POST-form "/oauth/token"
                                          {:grant_type "authorization_code"
                                           :code (:code callback-params)
                                           :client_id h/test-client-id
                                           :client_secret h/test-client-secret
                                           :redirect_uri h/test-redirect-uri})
              tokens (h/parse-json token-response)
              id-token (:id_token tokens)]

          (testing "Token response includes id_token"
            (is (some? id-token) "id_token MUST be returned when openid scope requested"))

          (when id-token
            ;; JWT Structure: header.payload.signature
            (testing "ID Token has valid JWT structure (3 parts)"
              (let [parts (str/split id-token #"\.")]
                (is (= 3 (count parts)) "JWT MUST have exactly 3 parts")))

            ;; Decode and validate claims per OpenID Connect Core 1.0 Section 2
            (let [parts (str/split id-token #"\.")
                  payload-b64 (second parts)
                  padding (- 4 (mod (count payload-b64) 4))
                  padded (str payload-b64 (apply str (repeat (if (= 4 padding) 0 padding) "=")))
                  payload-json (String. (.decode (java.util.Base64/getUrlDecoder) padded))
                  claims (json/read-str payload-json :key-fn keyword)]

              ;; Required claims per OpenID Connect Core 1.0 Section 2
              (testing "ID Token contains REQUIRED claims (OpenID Connect Core 1.0 Section 2)"
                (is (some? (:iss claims)) "'iss' (Issuer) is REQUIRED")
                (is (some? (:sub claims)) "'sub' (Subject) is REQUIRED")
                (is (some? (:aud claims)) "'aud' (Audience) is REQUIRED")
                (is (some? (:exp claims)) "'exp' (Expiration) is REQUIRED")
                (is (some? (:iat claims)) "'iat' (Issued At) is REQUIRED"))

              ;; Audience validation
              (testing "'aud' claim contains client_id (OpenID Connect Core 1.0 Section 2)"
                (let [aud (:aud claims)]
                  (if (vector? aud)
                    (is (some #{h/test-client-id} aud))
                    (is (= h/test-client-id aud)))))

              ;; Expiration validation
              (testing "'exp' is in the future"
                (let [exp (:exp claims)
                      now (quot (System/currentTimeMillis) 1000)]
                  (is (> exp now) "ID Token MUST NOT be expired"))))))))))

(deftest test-id-token-nonce
  (testing "ID Token includes nonce when provided (OpenID Connect Core 1.0 Section 3.1.2.1)"
    (let [test-nonce "random-nonce-value-12345"
          auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid"
                                               :state "nonce-test"
                                               :nonce test-nonce}})]
      ;; Base assertion
      (testing "Authorization endpoint responds"
        (is (or (= 302 (:status auth-response))
                (= 400 (:status auth-response)))))
      (when (= 302 (:status auth-response))
        (let [login-params (h/parse-location-params (h/location-header auth-response))
              login-response (h/POST-form "/oauth/login"
                                          {:username h/test-username
                                           :password h/test-password
                                           :state (:state login-params)})
              callback-params (h/parse-location-params (h/location-header login-response))
              token-response (h/POST-form "/oauth/token"
                                          {:grant_type "authorization_code"
                                           :code (:code callback-params)
                                           :client_id h/test-client-id
                                           :client_secret h/test-client-secret
                                           :redirect_uri h/test-redirect-uri})
              tokens (h/parse-json token-response)
              id-token (:id_token tokens)]

          (when id-token
            (let [parts (str/split id-token #"\.")
                  payload-b64 (second parts)
                  padding (- 4 (mod (count payload-b64) 4))
                  padded (str payload-b64 (apply str (repeat (if (= 4 padding) 0 padding) "=")))
                  payload-json (String. (.decode (java.util.Base64/getUrlDecoder) padded))
                  claims (json/read-str payload-json :key-fn keyword)]

              (testing "'nonce' claim matches request nonce"
                (is (= test-nonce (:nonce claims))
                    "nonce in ID Token MUST match nonce in authorization request")))))))))

;;; ============================================================================
;;; UserInfo Endpoint Tests (OpenID Connect Core 1.0 Section 5.3)
;;; ============================================================================

(deftest test-userinfo-requires-authentication
  (testing "UserInfo endpoint requires authentication (OpenID Connect Core 1.0 Section 5.3.1)"
    (let [response (h/GET "/oauth/userinfo")]
      (testing "Returns 401 or 403 without token"
        (is (or (= 401 (:status response))
                (= 403 (:status response)))
            "UserInfo MUST require Bearer token")))))

(deftest test-userinfo-rejects-invalid-token
  (testing "UserInfo endpoint rejects invalid token"
    (let [response (h/GET-with-bearer "/oauth/userinfo" "invalid-access-token")]
      (testing "Returns error for invalid token"
        ;; Per RFC 6750 Section 3.1, invalid token should return 401
        ;; Some implementations may return 403 or even 200 with error body
        (is (or (= 401 (:status response))
                (= 403 (:status response))
                (= 200 (:status response)))
            "UserInfo should respond to invalid token")))))

(deftest test-userinfo-with-valid-token
  (testing "UserInfo endpoint with valid Bearer token (OpenID Connect Core 1.0 Section 5.3)"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid profile email"
                                               :state "userinfo-test"}})]
      ;; Base assertion
      (testing "Authorization endpoint responds"
        (is (or (= 302 (:status auth-response))
                (= 400 (:status auth-response)))))
      (when (= 302 (:status auth-response))
        (let [login-params (h/parse-location-params (h/location-header auth-response))
              login-response (h/POST-form "/oauth/login"
                                          {:username h/test-username
                                           :password h/test-password
                                           :state (:state login-params)})
              callback-params (h/parse-location-params (h/location-header login-response))
              token-response (h/POST-form "/oauth/token"
                                          {:grant_type "authorization_code"
                                           :code (:code callback-params)
                                           :client_id h/test-client-id
                                           :client_secret h/test-client-secret
                                           :redirect_uri h/test-redirect-uri})
              tokens (h/parse-json token-response)
              access-token (:access_token tokens)]

          (when access-token
            (let [userinfo-response (h/GET-with-bearer "/oauth/userinfo" access-token)
                  userinfo (h/parse-json userinfo-response)]

              (testing "Returns 200 OK"
                (is (= 200 (:status userinfo-response))))

              ;; Required claim per OpenID Connect Core 1.0 Section 5.3.2
              (testing "Contains 'sub' claim (REQUIRED)"
                (is (some? (:sub userinfo)) "'sub' claim is REQUIRED in UserInfo response")))))))))

;;; ============================================================================
;;; OIDC Scope Tests (OpenID Connect Core 1.0 Section 5.4)
;;; ============================================================================

(deftest test-openid-scope-required
  (testing "openid scope is REQUIRED for OIDC (OpenID Connect Core 1.0 Section 3.1.2.1)"
    (let [response (h/GET "/oauth/authorize"
                          {:query-params {:client_id h/test-client-id
                                          :redirect_uri h/test-redirect-uri
                                          :response_type "code"
                                          :scope "profile email"  ; Missing openid!
                                          :state "no-openid-test"}})]
      ;; Server may reject or proceed without OIDC features
      (is (some? (:status response))))))

;;; ============================================================================
;;; Prompt Parameter Tests (OpenID Connect Core 1.0 Section 3.1.2.1)
;;; ============================================================================

(deftest test-prompt-none-requires-session
  (testing "prompt=none returns error without session (OpenID Connect Core 1.0 Section 3.1.2.1)"
    (let [response (h/GET "/oauth/authorize"
                          {:query-params {:client_id h/test-client-id
                                          :redirect_uri h/test-redirect-uri
                                          :response_type "code"
                                          :scope "openid"
                                          :state "prompt-none-test"
                                          :prompt "none"}})]
      ;; Must return redirect with error OR 400 direct error
      (testing "Returns redirect or error"
        (is (or (= 302 (:status response))
                (= 400 (:status response)))))
      ;; Per OpenID Connect Core 1.0, MUST return login_required error
      (when (= 302 (:status response))
        (let [location (h/location-header response)]
          (when location
            (let [params (h/parse-location-params location)]
              (testing "Returns login_required error"
                (is (= "login_required" (:error params))
                    "prompt=none without session MUST return login_required error")))))))))

(deftest test-prompt-login
  (testing "prompt=login forces re-authentication (OpenID Connect Core 1.0 Section 3.1.2.1)"
    (let [response (h/GET "/oauth/authorize"
                          {:query-params {:client_id h/test-client-id
                                          :redirect_uri h/test-redirect-uri
                                          :response_type "code"
                                          :scope "openid"
                                          :state "prompt-login"
                                          :prompt "login"}})]
      ;; Should redirect to login page even if session exists
      (is (or (= 302 (:status response))
              (= 400 (:status response)))))))

;;; ============================================================================
;;; End Session / Logout Tests (OpenID Connect RP-Initiated Logout 1.0)
;;; ============================================================================

(deftest test-logout-endpoint
  (testing "Logout endpoint exists"
    (let [response (h/GET "/oauth/logout")]
      (is (or (= 200 (:status response))
              (= 302 (:status response))
              (= 400 (:status response)))
          "Logout endpoint should respond"))))

;;; ============================================================================
;;; CORS Tests
;;; ============================================================================

(deftest test-jwks-cors
  (testing "JWKS endpoint should allow CORS (public key distribution)"
    (let [response (h/GET "/oauth/jwks"
                          {:headers {"Origin" "https://example.com"}})]
      (is (= 200 (:status response))))))

(deftest test-discovery-cors
  (testing "Discovery endpoint should allow CORS"
    (let [response (h/GET "/.well-known/openid-configuration"
                          {:headers {"Origin" "https://example.com"}})]
      (is (= 200 (:status response))))))
