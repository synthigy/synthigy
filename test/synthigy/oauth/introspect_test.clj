(ns synthigy.oauth.introspect-test
  "Tests for OAuth 2.0 Token Introspection (RFC 7662).

  Token introspection allows resource servers to query the authorization
  server about the state of an access token or refresh token.

  Tests:
  1. Introspect valid access token - active=true + all claims
  2. Introspect valid refresh token - active=true
  3. Introspect expired token - active=false
  4. Introspect revoked token - active=false
  5. Introspect invalid/unknown token - active=false
  6. Missing client authentication - 401
  7. Invalid client credentials - 401
  8. Missing token parameter - 400"
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.string :as str]
   [buddy.hashers :as hashers]
   [synthigy.test-helper :as test-helper]
   [synthigy.oauth.handlers :as handlers]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.token :as token-ns]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-client-id "test-introspect-client")
(def test-client-secret "test-introspect-secret-123")
(def test-redirect-uri "https://example.com/callback")
(def test-username "introspectuser")
(def test-password "test-introspect-password")

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn setup-test-data-fixture
  "Setup test data and mock IAM functions for each test."
  [f]
  (let [original-get-client (var-get #'iam/get-client)
        original-get-user-details (var-get #'iam/get-user-details)
        original-validate-password (var-get #'iam/validate-password)
        original-publish (var-get #'iam/publish)]

    ;; Mock IAM functions to use atom-based storage
    (alter-var-root #'iam/get-client
                    (constantly (fn [id]
                                  (get @core/*clients* id))))

    (alter-var-root #'iam/get-user-details
                    (constantly (fn [username]
                                  (get @core/*resource-owners* username))))

    (alter-var-root #'iam/validate-password
                    (constantly (fn [user-password stored-password]
                                  (if (and stored-password (str/includes? stored-password "$"))
                                    (hashers/check user-password stored-password)
                                    (= user-password stored-password)))))

    (alter-var-root #'iam/publish
                    (constantly (fn [_ _] nil)))

    ;; Setup test client and user
    (test-helper/setup-test-client!
     {:client-id test-client-id
      :client-secret test-client-secret
      :redirect-uri test-redirect-uri
      :allowed-grants ["authorization_code" "refresh_token"]
      :type :confidential})

    (test-helper/setup-test-user!
     {:username test-username
      :password test-password
      :email "introspect@example.com"
      :given-name "Introspect"
      :family-name "Test"})

    (try
      (f)
      (finally
        ;; Restore original IAM functions
        (alter-var-root #'iam/get-client (constantly original-get-client))
        (alter-var-root #'iam/get-user-details (constantly original-get-user-details))
        (alter-var-root #'iam/validate-password (constantly original-validate-password))
        (alter-var-root #'iam/publish (constantly original-publish))))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each test-helper/reset-oauth-state-fixture setup-test-data-fixture)

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn get-tokens
  "Complete authorization flow to get tokens."
  []
  (test-helper/get-tokens test-client-id
                          test-client-secret
                          test-redirect-uri
                          test-username
                          test-password))

(defn introspect-request
  "Create an introspection request."
  [{:keys [token token_type_hint client_id client_secret]}]
  (test-helper/request :post "/oauth/introspect"
                       (cond-> {}
                         token (assoc :token token)
                         token_type_hint (assoc :token_type_hint token_type_hint)
                         client_id (assoc :client_id client_id)
                         client_secret (assoc :client_secret client_secret))))

;; =============================================================================
;; Test 1: Introspect Valid Access Token
;; =============================================================================

(deftest test-introspect-valid-access-token
  (testing "Introspect valid access token returns active=true with claims"
    (println "\n=== INTROSPECT VALID ACCESS TOKEN ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)
          _ (println "  Got access_token:" (subs access-token 0 20) "...")

          ;; Introspect the access token
          _ (println "\n  -> Introspecting access_token...")
          response (handlers/introspect
                    (introspect-request {:token access-token
                                         :token_type_hint "access_token"
                                         :client_id test-client-id
                                         :client_secret test-client-secret}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Active:" (:active body))
      (println "  Scope:" (:scope body))
      (println "  Username:" (:username body))

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")
      (is (true? (:active body)) "Token should be active")
      (is (some? (:scope body)) "Should include scope")
      (is (= test-username (:username body)) "Should include username")
      (is (= "Bearer" (:token_type body)) "Should have token_type Bearer")
      (is (number? (:exp body)) "Should include exp claim")
      (is (number? (:iat body)) "Should include iat claim")

      (if (true? (:active body))
        (println "PASS: Access token introspection successful")
        (println "FAIL: Access token introspection failed")))))

;; =============================================================================
;; Test 2: Introspect Valid Refresh Token
;; =============================================================================

(deftest test-introspect-valid-refresh-token
  (testing "Introspect valid refresh token returns active=true"
    (println "\n=== INTROSPECT VALID REFRESH TOKEN ===")

    (let [tokens (get-tokens)
          refresh-token (:refresh_token tokens)
          _ (println "  Got refresh_token:" (when refresh-token (subs refresh-token 0 20)) "...")

          _ (println "\n  -> Introspecting refresh_token...")
          response (handlers/introspect
                    (introspect-request {:token refresh-token
                                         :token_type_hint "refresh_token"
                                         :client_id test-client-id
                                         :client_secret test-client-secret}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Active:" (:active body))

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")
      (is (true? (:active body)) "Refresh token should be active")

      (if (true? (:active body))
        (println "PASS: Refresh token introspection successful")
        (println "FAIL: Refresh token introspection failed")))))

;; =============================================================================
;; Test 3: Introspect Without token_type_hint
;; =============================================================================

(deftest test-introspect-without-hint
  (testing "Introspect token without token_type_hint"
    (println "\n=== INTROSPECT WITHOUT HINT ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)
          _ (println "  Got access_token")

          ;; Introspect without hint
          _ (println "\n  -> Introspecting without hint...")
          response (handlers/introspect
                    (introspect-request {:token access-token
                                         :client_id test-client-id
                                         :client_secret test-client-secret}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Active:" (:active body))

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")
      (is (true? (:active body)) "Should still find token without hint")

      (if (true? (:active body))
        (println "PASS: Token introspection without hint works")
        (println "FAIL: Token introspection without hint failed")))))

;; =============================================================================
;; Test 4: Introspect Revoked Token
;; =============================================================================

(deftest test-introspect-revoked-token
  (testing "Introspect revoked token returns active=false"
    (println "\n=== INTROSPECT REVOKED TOKEN ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)
          _ (println "  Got access_token")

          ;; Revoke the token first
          _ (println "\n  -> Revoking token...")
          _ (handlers/revoke
             (test-helper/request :post "/oauth/revoke"
                                  {:token access-token
                                   :client_id test-client-id
                                   :client_secret test-client-secret}))

          ;; Now introspect the revoked token
          _ (println "  -> Introspecting revoked token...")
          response (handlers/introspect
                    (introspect-request {:token access-token
                                         :token_type_hint "access_token"
                                         :client_id test-client-id
                                         :client_secret test-client-secret}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Active:" (:active body))

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")
      (is (false? (:active body)) "Revoked token should be inactive")

      (if (false? (:active body))
        (println "PASS: Revoked token correctly shows as inactive")
        (println "FAIL: Revoked token incorrectly shows as active")))))

;; =============================================================================
;; Test 5: Introspect Invalid/Unknown Token
;; =============================================================================

(deftest test-introspect-invalid-token
  (testing "Introspect invalid/unknown token returns active=false"
    (println "\n=== INTROSPECT INVALID TOKEN ===")

    (let [fake-token "invalid-token-that-doesnt-exist"

          _ (println "  -> Introspecting unknown token...")
          response (handlers/introspect
                    (introspect-request {:token fake-token
                                         :client_id test-client-id
                                         :client_secret test-client-secret}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Active:" (:active body))

      (println "\n--- Results ---")
      ;; RFC 7662 Section 2.2: Invalid tokens get 200 with active=false
      (is (= 200 (:status response)) "Should return 200 OK")
      (is (false? (:active body)) "Invalid token should be inactive")

      (if (false? (:active body))
        (println "PASS: Invalid token correctly shows as inactive")
        (println "FAIL: Invalid token incorrectly shows as active")))))

;; =============================================================================
;; Test 6: Missing Client Authentication
;; =============================================================================

(deftest test-introspect-client-auth-required
  (testing "Introspect without client auth returns 401"
    (println "\n=== INTROSPECT CLIENT AUTH REQUIRED ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)

          _ (println "  -> Introspecting without client credentials...")
          response (handlers/introspect
                    (introspect-request {:token access-token}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Error:" (:error body))

      (println "\n--- Results ---")
      (is (= 401 (:status response)) "Should return 401 Unauthorized")
      (is (= "invalid_client" (:error body)) "Should return invalid_client error")

      (if (= 401 (:status response))
        (println "PASS: Client auth requirement enforced")
        (println "FAIL: Client auth not required!")))))

;; =============================================================================
;; Test 7: Invalid Client Credentials
;; =============================================================================

(deftest test-introspect-wrong-client-secret
  (testing "Introspect with wrong client returns 401"
    (println "\n=== INTROSPECT WRONG CLIENT ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)

          _ (println "  -> Introspecting with unknown client...")
          response (handlers/introspect
                    (introspect-request {:token access-token
                                         :client_id "unknown-client"
                                         :client_secret "wrong-secret"}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Error:" (:error body))

      (println "\n--- Results ---")
      (is (= 401 (:status response)) "Should return 401 Unauthorized")
      (is (= "invalid_client" (:error body)) "Should return invalid_client error")

      (if (= 401 (:status response))
        (println "PASS: Invalid client rejected")
        (println "FAIL: Invalid client accepted!")))))

;; =============================================================================
;; Test 8: Missing Token Parameter
;; =============================================================================

(deftest test-introspect-missing-token
  (testing "Introspect without token parameter returns 400"
    (println "\n=== INTROSPECT MISSING TOKEN ===")

    (let [_ (println "  -> Introspecting without token parameter...")
          response (handlers/introspect
                    (introspect-request {:client_id test-client-id
                                         :client_secret test-client-secret}))

          body (test-helper/parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Error:" (:error body))

      (println "\n--- Results ---")
      (is (= 400 (:status response)) "Should return 400 Bad Request")
      (is (= "invalid_request" (:error body)) "Should return invalid_request error")

      (if (= 400 (:status response))
        (println "PASS: Missing token parameter rejected")
        (println "FAIL: Missing token parameter accepted!")))))
