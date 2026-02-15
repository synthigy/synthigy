(ns synthigy.oauth.token-revocation-test
  "Tests for OAuth 2.0 Token Revocation (RFC 7009).

  Token revocation allows clients to notify the authorization server
  that a previously obtained access or refresh token is no longer needed.

  Tests:
  1. Revoke access token (happy path)
  2. Revoke refresh token (happy path)
  3. Revoke with token_type_hint
  4. Invalid token (not found)
  5. Client mismatch
  6. Token already revoked (idempotent)
  7. Missing token parameter"
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.string :as str]
   [buddy.hashers :as hashers]
   [synthigy.test-helper :as test-helper]
   [synthigy.oauth.handlers :as handlers]
   [synthigy.oauth.core :as core]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-client-id "test-revoke-client")
(def test-client-secret "test-revoke-secret-123")
(def test-redirect-uri "https://example.com/callback")
(def test-username "revokeuser")
(def test-password "test-revoke-password")

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn setup-test-data-fixture
  "Setup test data and mock IAM functions for each test."
  [f]
  ;; Note: Encryption is initialized by system-fixture

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
                                  ;; Handle both hashed (users) and plaintext (clients for testing)
                                  (if (and stored-password (str/includes? stored-password "$"))
                                    (hashers/check user-password stored-password)  ; Hashed
                                    (= user-password stored-password)))))  ; Plaintext for testing

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
      :email "revoke@example.com"
      :given-name "Revoke"
      :family-name "Test"})

    (try
      (f)
      (finally
        ;; Restore original IAM functions
        (alter-var-root #'iam/get-client (constantly original-get-client))
        (alter-var-root #'iam/get-user-details (constantly original-get-user-details))
        (alter-var-root #'iam/validate-password (constantly original-validate-password))
        (alter-var-root #'iam/publish (constantly original-publish))))))

;; Use OAuth state reset + test data setup for each test (no database needed)
(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each test-helper/reset-oauth-state-fixture setup-test-data-fixture)

;; =============================================================================
;; Helper: Get Access and Refresh Tokens
;; =============================================================================

(defn get-tokens
  "Complete authorization flow to get tokens (wrapper around shared helper)."
  []
  (test-helper/get-tokens test-client-id
                          test-client-secret
                          test-redirect-uri
                          test-username
                          test-password))

;; =============================================================================
;; Test 1: Revoke Access Token (Happy Path)
;; =============================================================================

(deftest test-revoke-access-token
  (testing "Revoke access token successfully"
    (println "\n=== REVOKE ACCESS TOKEN ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)
          _ (println "  Got access_token:" (subs access-token 0 20) "...")

          ;; Revoke the access token
          _ (println "\n  → Revoking access_token...")
          revoke-response (handlers/revoke
                           (test-helper/request :post "/oauth/revoke"
                                                {:token access-token
                                                 :token_type_hint "access_token"
                                                 :client_id test-client-id
                                                 :client_secret test-client-secret}))]

      (println "  Revoke response status:" (:status revoke-response))

      (println "\n--- Results ---")
      (is (= 200 (:status revoke-response)) "Should return 200 OK")

      (if (= 200 (:status revoke-response))
        (println "✅ PASS: Access token revoked successfully")
        (println "❌ FAIL: Failed to revoke access token")))))

;; =============================================================================
;; Test 2: Revoke Refresh Token (Happy Path)
;; =============================================================================

(deftest test-revoke-refresh-token
  (testing "Revoke refresh token successfully"
    (println "\n=== REVOKE REFRESH TOKEN ===")

    (let [tokens (get-tokens)
          refresh-token (:refresh_token tokens)
          _ (println "  Got refresh_token:" (when refresh-token (subs refresh-token 0 20)) "...")

          ;; Revoke the refresh token
          _ (println "\n  → Revoking refresh_token...")
          revoke-response (handlers/revoke
                           (test-helper/request :post "/oauth/revoke"
                                                {:token refresh-token
                                                 :token_type_hint "refresh_token"
                                                 :client_id test-client-id
                                                 :client_secret test-client-secret}))]

      (println "  Revoke response status:" (:status revoke-response))

      (println "\n--- Results ---")
      (is (= 200 (:status revoke-response)) "Should return 200 OK")

      (if (= 200 (:status revoke-response))
        (println "✅ PASS: Refresh token revoked successfully")
        (println "❌ FAIL: Failed to revoke refresh token")))))

;; =============================================================================
;; Test 3: Revoke Without token_type_hint
;; =============================================================================

(deftest test-revoke-without-hint
  (testing "Revoke token without token_type_hint"
    (println "\n=== REVOKE WITHOUT HINT ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)
          _ (println "  Got access_token")

          ;; Revoke without token_type_hint (server should auto-detect)
          _ (println "\n  → Revoking token without hint...")
          revoke-response (handlers/revoke
                           (test-helper/request :post "/oauth/revoke"
                                                {:token access-token
                                                 :client_id test-client-id
                                                 :client_secret test-client-secret}))]

      (println "  Revoke response status:" (:status revoke-response))

      (println "\n--- Results ---")
      (is (= 200 (:status revoke-response)) "Should return 200 OK")

      (if (= 200 (:status revoke-response))
        (println "✅ PASS: Token revoked without hint")
        (println "❌ FAIL: Failed to revoke without hint")))))

;; =============================================================================
;; Test 4: Invalid/Unknown Token (RFC 7009 Section 2.2)
;; =============================================================================

(deftest test-revoke-invalid-token
  (testing "Revoke invalid/unknown token returns 200 per RFC 7009"
    (println "\n=== REVOKE INVALID TOKEN (RFC 7009) ===")

    (let [fake-token "invalid-token-that-doesnt-exist"

          ;; Try to revoke invalid token
          _ (println "  → Trying to revoke unknown token...")
          revoke-response (handlers/revoke
                           (test-helper/request :post "/oauth/revoke"
                                                {:token fake-token
                                                 :client_id test-client-id
                                                 :client_secret test-client-secret}))]

      (println "  Revoke response status:" (:status revoke-response))

      (println "\n--- Results ---")
      ;; RFC 7009 Section 2.2: "The authorization server responds with HTTP status
      ;; code 200 if the token has been revoked successfully or if the client
      ;; submitted an invalid token."
      (is (= 200 (:status revoke-response))
          "RFC 7009: Unknown/invalid tokens should return 200 OK")

      (if (= 200 (:status revoke-response))
        (println "✅ PASS: RFC 7009 compliant - unknown token returns 200")
        (println "❌ FAIL: Not RFC 7009 compliant - should return 200 for unknown tokens")))))

;; =============================================================================
;; Test 5: Client Mismatch
;; =============================================================================

(deftest test-revoke-client-mismatch
  (testing "Revoke token with wrong client credentials"
    (println "\n=== REVOKE CLIENT MISMATCH ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)
          _ (println "  Got access_token from client:" test-client-id)

          ;; Try to revoke with different client credentials
          _ (println "  → Trying to revoke with wrong client...")
          revoke-response (handlers/revoke
                           (test-helper/request :post "/oauth/revoke"
                                                {:token access-token
                                                 :client_id "wrong-client-id"
                                                 :client_secret "wrong-secret"}))

          error-data (test-helper/parse-json-body revoke-response)]

      (println "  Revoke response status:" (:status revoke-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 400 (:status revoke-response)) "Should return 400")
      (is (= "invalid_client" (:error error-data)) "Should return invalid_client error")

      (if (= 400 (:status revoke-response))
        (println "✅ PASS: Client mismatch detected")
        (println "❌ FAIL: Client mismatch not detected!")))))

;; =============================================================================
;; Test 6: Token Already Revoked - Idempotent (RFC 7009)
;; =============================================================================

(deftest test-revoke-already-revoked
  (testing "Revoke already revoked token is idempotent per RFC 7009"
    (println "\n=== REVOKE ALREADY REVOKED TOKEN (RFC 7009) ===")

    (let [tokens (get-tokens)
          access-token (:access_token tokens)

          ;; First revocation
          _ (println "  → First revocation...")
          revoke1-response (handlers/revoke
                            (test-helper/request :post "/oauth/revoke"
                                                 {:token access-token
                                                  :client_id test-client-id
                                                  :client_secret test-client-secret}))

          _ (println "  First revoke status:" (:status revoke1-response))

          ;; Second revocation (token already revoked)
          _ (println "\n  → Second revocation (already revoked)...")
          revoke2-response (handlers/revoke
                            (test-helper/request :post "/oauth/revoke"
                                                 {:token access-token
                                                  :client_id test-client-id
                                                  :client_secret test-client-secret}))]

      (println "  Second revoke status:" (:status revoke2-response))

      (println "\n--- Results ---")
      (is (= 200 (:status revoke1-response)) "First revocation should succeed")
      ;; RFC 7009 Section 2.2: Returns 200 for already-revoked tokens
      ;; (they are now "invalid" tokens, which still get 200)
      (is (= 200 (:status revoke2-response))
          "RFC 7009: Already revoked tokens should return 200 OK")

      (if (and (= 200 (:status revoke1-response))
               (= 200 (:status revoke2-response)))
        (println "✅ PASS: Token revocation is idempotent (RFC 7009 compliant)")
        (println "❌ FAIL: Idempotency issue")))))

;; =============================================================================
;; Test 7: Missing Token Parameter (RFC 7009 Section 2.1)
;; =============================================================================

(deftest test-revoke-missing-token
  (testing "Revoke with missing token parameter returns 400"
    (println "\n=== REVOKE MISSING TOKEN (RFC 7009) ===")

    (let [;; Try to revoke without token parameter
          _ (println "  → Trying to revoke without token parameter...")
          revoke-response (handlers/revoke
                           (test-helper/request :post "/oauth/revoke"
                                                {:client_id test-client-id
                                                 :client_secret test-client-secret}))]

      (println "  Revoke response status:" (:status revoke-response))

      (println "\n--- Results ---")
      ;; RFC 7009 Section 2.1: "token" parameter is REQUIRED
      ;; Missing required parameter should return 400 invalid_request
      (is (= 400 (:status revoke-response))
          "RFC 7009: Missing token parameter must return 400")

      (when (= 400 (:status revoke-response))
        (let [error-data (test-helper/parse-json-body revoke-response)]
          (println "  Error:" (:error error-data))
          (is (= "invalid_request" (:error error-data))
              "Should return invalid_request error")))

      (if (= 400 (:status revoke-response))
        (println "✅ PASS: Missing token parameter rejected")
        (println "❌ FAIL: Missing token parameter accepted!")))))
