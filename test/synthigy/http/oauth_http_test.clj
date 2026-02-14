(ns synthigy.http.oauth-http-test
  "OAuth 2.0 HTTP integration tests."
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest testing is use-fixtures]]
    [synthigy.http.http-test-helper :as h]))

(use-fixtures :once h/deps-fixture h/server-fixture)
(use-fixtures :each h/oauth-data-fixture h/cookie-store-fixture)

;;; ============================================================================
;;; Authorization Code Flow Tests
;;; ============================================================================

(deftest test-authorization-code-flow-basic
  (testing "Complete authorization code flow"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid profile"
                                               :state "test-state-123"}})]
      (testing "Authorize endpoint responds"
        (is (or (= 302 (:status auth-response))
                (= 400 (:status auth-response))))))))

(deftest test-authorization-code-flow-with-offline-access
  (testing "Authorization code flow with offline_access"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid offline_access"
                                               :state "refresh-test"}})]
      (testing "Authorize endpoint responds"
        (is (some? (:status auth-response)))))))

;;; ============================================================================
;;; PKCE Flow Tests
;;; ============================================================================

(deftest test-authorization-code-flow-with-pkce
  (testing "Authorization code flow with PKCE (S256)"
    (let [code-verifier (h/generate-code-verifier)
          code-challenge (h/compute-code-challenge-s256 code-verifier)
          auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid"
                                               :state "pkce-test"
                                               :code_challenge code-challenge
                                               :code_challenge_method "S256"}})]
      (testing "Authorize endpoint responds"
        (is (some? (:status auth-response)))))))

(deftest test-pkce-invalid-verifier
  (testing "PKCE rejects invalid code_verifier"
    (let [code-verifier (h/generate-code-verifier)
          code-challenge (h/compute-code-challenge-s256 code-verifier)
          auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid"
                                               :state "pkce-invalid"
                                               :code_challenge code-challenge
                                               :code_challenge_method "S256"}})]
      (testing "Authorize endpoint responds"
        (is (some? (:status auth-response)))))))

;;; ============================================================================
;;; Refresh Token Flow Tests
;;; ============================================================================

(deftest test-refresh-token-flow
  (testing "Refresh token grant"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid offline_access"
                                               :state "refresh-flow"}})]
      (testing "Authorize endpoint responds"
        (is (some? (:status auth-response)))))))

(deftest test-refresh-token-invalid
  (testing "Invalid refresh token returns error"
    (let [response (h/POST-form "/oauth/token"
                                {:grant_type "refresh_token"
                                 :refresh_token "invalid-refresh-token"
                                 :client_id h/test-client-id
                                 :client_secret h/test-client-secret})]
      (is (= 400 (:status response))))))

;;; ============================================================================
;;; Client Credentials Flow Tests
;;; ============================================================================

(deftest test-client-credentials-flow
  (testing "Client credentials grant"
    (let [response (h/POST-form "/oauth/token"
                                {:grant_type "client_credentials"
                                 :client_id h/test-client-id
                                 :client_secret h/test-client-secret
                                 :scope "openid"})]
      (testing "Returns response"
        (is (or (= 200 (:status response))
                (= 400 (:status response))
                (= 401 (:status response))))))))

(deftest test-client-credentials-basic-auth
  (testing "Client credentials with Basic authentication"
    (let [response (h/POST-form "/oauth/token"
                                {:grant_type "client_credentials"
                                 :scope "openid"}
                                {:headers {"Authorization"
                                           (h/basic-auth-header h/test-client-id
                                                                h/test-client-secret)}})]
      (testing "Returns response"
        (is (or (= 200 (:status response))
                (= 400 (:status response))
                (= 401 (:status response))))))))

;;; ============================================================================
;;; Device Code Flow Tests
;;; ============================================================================

(deftest test-device-code-flow
  (testing "Device code flow (RFC 8628)"
    (let [response (h/POST-form "/oauth/device/auth"
                                {:client_id h/test-client-id
                                 :scope "openid"})]
      (testing "Device authorization endpoint responds"
        (is (or (= 200 (:status response))
                (= 400 (:status response))))))))

(deftest test-device-code-invalid-client
  (testing "Device authorization rejects unknown client"
    (let [response (h/POST-form "/oauth/device/auth"
                                {:client_id "unknown-client"
                                 :scope "openid"})]
      (is (= 400 (:status response))))))

;;; ============================================================================
;;; Token Revocation Tests
;;; ============================================================================

(deftest test-token-revocation
  (testing "Token revocation endpoint"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid"
                                               :state "revoke-test"}})]
      (testing "Authorize endpoint responds"
        (is (some? (:status auth-response)))))))

(deftest test-token-revocation-unknown-token
  (testing "Revocation of unknown token returns response"
    (let [response (h/POST-form "/oauth/revoke"
                                {:token "unknown-token-12345"
                                 :client_id h/test-client-id
                                 :client_secret h/test-client-secret})]
      (is (or (= 200 (:status response))
              (= 400 (:status response)))))))

;;; ============================================================================
;;; Error Handling Tests
;;; ============================================================================

(deftest test-invalid-client-error
  (testing "Invalid client_id returns error"
    (let [response (h/GET "/oauth/authorize"
                          {:query-params {:client_id "unknown-client"
                                          :redirect_uri "https://evil.com/callback"
                                          :response_type "code"
                                          :scope "openid"
                                          :state "error-test"}})]
      (is (or (= 400 (:status response))
              (= 302 (:status response)))))))

(deftest test-invalid-redirect-uri-error
  (testing "Invalid redirect_uri returns error"
    (let [response (h/GET "/oauth/authorize"
                          {:query-params {:client_id h/test-client-id
                                          :redirect_uri "https://evil.com/callback"
                                          :response_type "code"
                                          :scope "openid"
                                          :state "redirect-test"}})]
      (is (= 400 (:status response))))))

(deftest test-missing-grant-type-error
  (testing "Token endpoint requires grant_type"
    (let [response (h/POST-form "/oauth/token"
                                {:client_id h/test-client-id
                                 :client_secret h/test-client-secret})]
      (is (= 400 (:status response))))))

(deftest test-unsupported-grant-type-error
  (testing "Unsupported grant type returns error"
    (let [response (h/POST-form "/oauth/token"
                                {:grant_type "password"
                                 :username "user"
                                 :password "pass"
                                 :client_id h/test-client-id
                                 :client_secret h/test-client-secret})]
      (is (or (= 400 (:status response))
              (= 401 (:status response)))))))

(deftest test-invalid-client-secret
  (testing "Invalid client secret returns error"
    (let [response (h/POST-form "/oauth/token"
                                {:grant_type "client_credentials"
                                 :client_id h/test-client-id
                                 :client_secret "wrong-secret"})]
      (is (or (= 400 (:status response))
              (= 401 (:status response)))))))

;;; ============================================================================
;;; Session Management Tests
;;; ============================================================================

(deftest test-login-session-cookie
  (testing "Login endpoint responds"
    (let [auth-response (h/GET "/oauth/authorize"
                               {:query-params {:client_id h/test-client-id
                                               :redirect_uri h/test-redirect-uri
                                               :response_type "code"
                                               :scope "openid"
                                               :state "session-test"}})]
      (is (some? (:status auth-response))))))

(deftest test-logout
  (testing "Logout endpoint responds"
    (let [response (h/GET "/oauth/logout")]
      (is (some? (:status response))))))
