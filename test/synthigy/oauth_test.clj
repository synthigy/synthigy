(ns synthigy.oauth-test
  "Tests for OAuth 2.0 functionality.

  Tests cover:
  - OAuth client management
  - Authorization code flow
  - Device code flow
  - Token generation and validation
  - Session management
  - Token revocation
  - PKCE (Proof Key for Code Exchange)"
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.db :as db]
    [synthigy.iam.encryption :as encryption]
    [synthigy.oauth.authorization-code :as auth-code]
    [synthigy.oauth.core :as oauth-core]
    [synthigy.oauth.device-code :as device-code]
    [synthigy.oauth.token :as token]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]))

;;; ============================================================================
;;; Test Data Counters
;;; ============================================================================

;; Counters for cycling through test data IDs
(def ^:private test-client-counter (atom 0))
(def ^:private test-user-counter (atom 0))

;; Available test data keys
(def ^:private test-client-keys
  [:test/oauth-client-1 :test/oauth-client-2 :test/oauth-client-3 :test/oauth-client-4
   :test/oauth-client-5 :test/oauth-client-6 :test/oauth-client-7])

(def ^:private test-user-keys
  [:test/oauth-user-1 :test/oauth-user-2 :test/oauth-user-3 :test/oauth-user-4
   :test/oauth-user-5])

(defn next-client-id
  "Get the next client ID from test data."
  []
  (let [idx (swap! test-client-counter inc)
        data-key (nth test-client-keys (mod idx (count test-client-keys)))]
    (id/data data-key)))

(defn next-user-id
  "Get the next user ID from test data."
  []
  (let [idx (swap! test-user-counter inc)
        data-key (nth test-user-keys (mod idx (count test-user-keys)))]
    (id/data data-key)))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

;; Use the comprehensive test helper that properly initializes and shuts down
;; all services, preventing tests from hanging on unclosed async resources
(use-fixtures :once test-helper/system-fixture)

;;; ============================================================================
;;; OAuth Client Tests
;;; ============================================================================

(deftest test-get-client
  (testing "Get OAuth client"
    (let [client-id (str "test-oauth-client-" (next-client-id))
          _ (dataset/sync-entity :iam/app
                                 {:id client-id
                                  :type :confidential
                                  :settings {:redirections ["http://localhost:3000/callback"]}})
          client (oauth-core/get-client client-id)]
      (is (map? client) "Should return client map")
      (is (= client-id (:id client)) "Should have correct client ID")
      (is (id/extract client) "Should have UUID"))))

(deftest test-client-cached
  (testing "Client should be cached after first retrieval"
    (let [client-id (str "test-cache-client-" (next-client-id))
          _ (dataset/sync-entity :iam/app
                                 {:id client-id
                                  :type :public
                                  :settings {}})
          ;; First call loads from DB
          client1 (oauth-core/get-client client-id)
          ;; Second call should use cache
          client2 (oauth-core/get-client client-id)]
      (is (= client1 client2) "Cached client should match original"))))

(deftest test-client-confidential
  (testing "Confidential OAuth client"
    (let [client-id (str "test-confidential-" (next-client-id))
          client-secret "secret-12345"
          _ (dataset/sync-entity :iam/app
                                 {:id client-id
                                  :secret client-secret
                                  :type :confidential
                                  :settings {}})
          client (oauth-core/get-client client-id)]
      (is (= :confidential (:type client)) "Should be confidential type")
      (is (:secret client) "Should have secret"))))

(deftest test-client-public
  (testing "Public OAuth client (no secret)"
    (let [client-id (str "test-public-" (next-client-id))
          _ (dataset/sync-entity :iam/app
                                 {:id client-id
                                  :type :public
                                  :settings {}})
          client (oauth-core/get-client client-id)]
      (is (= :public (:type client)) "Should be public type"))))

;;; ============================================================================
;;; Session Management Tests
;;; ============================================================================

(deftest test-create-session
  (testing "Create OAuth session"
    (let [session-id (oauth-core/gen-session-id)
          session-data {:flow "authorization_code"
                        :client (id/data :test/oauth-client-1)
                        :last-active (java.util.Date.)}]
      (oauth-core/set-session session-id session-data)
      (let [retrieved (oauth-core/get-session session-id)]
        (is (map? retrieved) "Should retrieve session")
        (is (= "authorization_code" (:flow retrieved)) "Should have correct flow")
        (is (:last-active retrieved) "Should have last-active timestamp")))))

(deftest test-remove-session
  (testing "Remove OAuth session"
    (let [session-id (oauth-core/gen-session-id)
          session-data {:flow "device_code"}]
      (oauth-core/set-session session-id session-data)
      (is (oauth-core/get-session session-id) "Session should exist")
      (oauth-core/remove-session session-id)
      (is (nil? (oauth-core/get-session session-id)) "Session should be removed"))))

(deftest test-session-id-format
  (testing "Session ID format"
    (let [session-id (oauth-core/gen-session-id)]
      (is (string? session-id) "Should be a string")
      (is (= 30 (count session-id)) "Should be 30 characters long")
      (is (re-matches #"[A-Za-z]+" session-id) "Should only contain letters"))))

(deftest test-multiple-sessions
  (testing "Multiple concurrent sessions"
    (let [session1 (oauth-core/gen-session-id)
          session2 (oauth-core/gen-session-id)
          session3 (oauth-core/gen-session-id)]
      (oauth-core/set-session session1 {:flow "auth_code"
                                        :user 1})
      (oauth-core/set-session session2 {:flow "device"
                                        :user 2})
      (oauth-core/set-session session3 {:flow "auth_code"
                                        :user 3})
      (is (oauth-core/get-session session1) "Session 1 should exist")
      (is (oauth-core/get-session session2) "Session 2 should exist")
      (is (oauth-core/get-session session3) "Session 3 should exist"))))

;;; ============================================================================
;;; Resource Owner Tests
;;; ============================================================================

(deftest test-validate-resource-owner
  (testing "Validate resource owner credentials"
    (let [username (str "test-resource-owner-" (next-client-id))
          password "ValidPassword123"
          _ (dataset/sync-entity :iam/user
                                 {:name username
                                  :password password
                                  :active true
                                  :type :PERSON})
          validated (oauth-core/validate-resource-owner username password)]
      (is (map? validated) "Should return user map for valid credentials")
      (is (= username (:name validated)) "Should have correct username")
      (is (nil? (:password validated)) "Should not include password in result"))))

(deftest test-validate-wrong-password
  (testing "Reject invalid password"
    (let [username (str "test-wrong-pass-" (next-client-id))
          correct-password "Correct123"
          wrong-password "Wrong456"
          _ (dataset/sync-entity :iam/user
                                 {:name username
                                  :password correct-password
                                  :active true
                                  :type :PERSON})
          validated (oauth-core/validate-resource-owner username wrong-password)]
      (is (nil? validated) "Should return nil for invalid password"))))

(deftest test-validate-inactive-user
  (testing "Reject inactive user"
    (let [username (str "test-inactive-owner-" (next-client-id))
          password "Password123"
          _ (dataset/sync-entity :iam/user
                                 {:name username
                                  :password password
                                  :active false
                                  :type :PERSON})
          validated (oauth-core/validate-resource-owner username password)]
      (is (nil? validated) "Should return nil for inactive user"))))

(deftest test-validate-nonexistent-user
  (testing "Reject non-existent user"
    (let [validated (oauth-core/validate-resource-owner
                      "nonexistent-user-12345"
                      "password")]
      (is (nil? validated) "Should return nil for non-existent user"))))

;;; ============================================================================
;;; Authorization Code Tests
;;; ============================================================================

(deftest test-generate-authorization-code
  (testing "Generate authorization code"
    (let [code (auth-code/gen-authorization-code)]
      (is (string? code) "Should be a string")
      (is (= 30 (count code)) "Should be 30 characters")
      (is (re-matches #"[A-Za-z]+" code) "Should only contain letters"))))

(deftest test-store-authorization-code
  (testing "Store and retrieve authorization code"
    (let [code (auth-code/gen-authorization-code)
          client-uuid (id/data :test/oauth-client-2)
          request {:response_type #{"code"}
                   :client_id "test-client"
                   :redirect_uri "http://localhost:3000/callback"
                   :scope #{:profile :email}}]
      (swap! auth-code/*authorization-codes* assoc code
             {:client client-uuid
              :request request
              :created-on (System/currentTimeMillis)})
      (let [stored (get @auth-code/*authorization-codes* code)]
        (is (map? stored) "Should retrieve stored code data")
        (is (= client-uuid (:client stored)) "Should have correct client")
        (is (= request (:request stored)) "Should preserve request")))))

(deftest test-mark-code-issued
  (testing "Mark authorization code as issued"
    (let [code (auth-code/gen-authorization-code)
          session-id (oauth-core/gen-session-id)]
      (swap! auth-code/*authorization-codes* assoc code {:issued? false})
      (auth-code/mark-code-issued session-id code)
      (let [stored (get @auth-code/*authorization-codes* code)]
        (is (true? (:issued? stored)) "Code should be marked as issued")
        (is (= session-id (:session stored)) "Should reference session")))))

(deftest test-delete-authorization-code
  (testing "Delete authorization code"
    (let [code (auth-code/gen-authorization-code)]
      (swap! auth-code/*authorization-codes* assoc code {:data "test"})
      (is (get @auth-code/*authorization-codes* code) "Code should exist")
      (auth-code/delete code)
      (is (nil? (get @auth-code/*authorization-codes* code)) "Code should be deleted"))))

;;; ============================================================================
;;; Device Code Tests
;;; ============================================================================

(deftest test-generate-device-code
  (testing "Generate device code"
    (let [code (device-code/gen-device-code)]
      (is (string? code) "Should be a string")
      (is (= 40 (count code)) "Should be 40 characters")
      (is (re-matches #"[A-Z]+" code) "Should only contain uppercase letters"))))

(deftest test-generate-user-code
  (testing "Generate user code"
    (let [code (device-code/gen-user-code)]
      (is (string? code) "Should be a string")
      (is (= 9 (count code)) "Should be 9 characters (XXXX-XXXX format)")
      (is (re-matches #"[A-Z]{4}-[A-Z]{4}" code) "Should match XXXX-XXXX format"))))

(deftest test-store-device-code
  (testing "Store device code data"
    (let [device-code (device-code/gen-device-code)
          user-code (device-code/gen-user-code)
          expires-at (+ (System/currentTimeMillis) 600000) ; 10 minutes
          client-uuid (id/data :test/oauth-client-3)]
      (swap! device-code/*device-codes* assoc device-code
             {:user-code user-code
              :expires-at expires-at
              :client client-uuid
              :request {:scope #{:profile}}})
      (let [stored (get @device-code/*device-codes* device-code)]
        (is (map? stored) "Should retrieve stored device code")
        (is (= user-code (:user-code stored)) "Should have correct user code")
        (is (= expires-at (:expires-at stored)) "Should have expiration")
        (is (= client-uuid (:client stored)) "Should have client UUID")))))

(deftest test-user-code-uniqueness
  (testing "User codes should be unique"
    (let [codes (repeatedly 100 device-code/gen-user-code)
          unique-codes (set codes)]
      (is (= (count codes) (count unique-codes))
          "All generated user codes should be unique"))))

;;; ============================================================================
;;; Token Expiry Tests
;;; ============================================================================

(deftest test-access-token-expiry
  (testing "Calculate access token expiry"
    (let [client {:settings {"token-expiry" {"access" 7200}}}
          expiry (oauth-core/access-token-expiry client)]
      (is (number? expiry) "Should return a number")
      (is (= 7200 expiry) "Should use client-specified expiry"))))

(deftest test-access-token-expiry-default
  (testing "Default access token expiry"
    (let [client {:settings {}}
          expiry (oauth-core/access-token-expiry client)]
      (is (number? expiry) "Should return a number")
      ;; Default is 2 hours = 7200000 milliseconds
      (is (pos? expiry) "Should have positive default"))))

(deftest test-refresh-token-expiry
  (testing "Calculate refresh token expiry"
    (let [client {:settings {"token-expiry" {"refresh" 86400}}}
          expiry (oauth-core/refresh-token-expiry client)]
      (is (number? expiry) "Should return a number")
      (is (= 86400 expiry) "Should use client-specified expiry"))))

(deftest test-token-expired
  (testing "Check if token is expired"
    ;; Create a token that expires immediately
    (let [payload {:exp (quot (System/currentTimeMillis) 1000)} ; Current time
          token (encryption/sign-data payload)]
      ;; Wait a moment to ensure it's expired
      (Thread/sleep 1000)
      (is (oauth-core/expired? token) "Token should be expired"))))

(deftest test-token-not-expired
  (testing "Check if fresh token is not expired"
    ;; Create a token that expires in 1 hour
    (let [future-exp (+ (quot (System/currentTimeMillis) 1000) 3600)
          payload {:user-id 123
                   :exp future-exp}
          token (encryption/sign-data payload)]
      (is (not (oauth-core/expired? token)) "Fresh token should not be expired"))))

;;; ============================================================================
;;; PKCE Tests
;;; ============================================================================

(deftest test-pkce-code-challenge-s256
  (testing "Generate PKCE code challenge with S256"
    (let [verifier "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
          challenge (synthigy.oauth/generate-code-challenge verifier "S256")]
      (is (string? challenge) "Should return a string")
      (is (not= verifier challenge) "Challenge should differ from verifier")
      (is (not (re-find #"=" challenge)) "Should not contain padding"))))

(deftest test-pkce-code-challenge-plain
  (testing "Generate PKCE code challenge with plain"
    (let [verifier "test-verifier-plain"
          challenge (synthigy.oauth/generate-code-challenge verifier "plain")]
      (is (= verifier challenge) "Plain challenge should equal verifier"))))

;;; ============================================================================
;;; Session Resource Owner Tests
;;; ============================================================================

(deftest test-set-session-resource-owner
  (testing "Set resource owner for session"
    (let [session-id (oauth-core/gen-session-id)
          username (str "test-session-owner-" (next-client-id))
          user (dataset/sync-entity :iam/user
                                    {:name username
                                     :active true
                                     :type :PERSON})]
      (oauth-core/set-session-resource-owner session-id user)
      (let [session (oauth-core/get-session session-id)]
        (is (= (id/extract user) (:resource-owner session))
            "Session should reference user UUID")))))

(deftest test-remove-session-resource-owner
  (testing "Remove resource owner from session"
    (let [session-id (oauth-core/gen-session-id)
          user (dataset/sync-entity :iam/user
                                    {:name (str "test-remove-owner-" (next-client-id))
                                     :active true
                                     :type :PERSON})]
      (oauth-core/set-session session-id {:resource-owner (id/extract user)})
      (oauth-core/set-session-resource-owner session-id user)
      (oauth-core/remove-session-resource-owner session-id)
      (let [session (oauth-core/get-session session-id)]
        (is (nil? (:resource-owner session))
            "Resource owner should be removed from session")))))

;;; ============================================================================
;;; Scope Management Tests
;;; ============================================================================

(deftest test-set-session-scope
  (testing "Set scope for session"
    (let [session-id (oauth-core/gen-session-id)
          scope #{:profile :email :openid}]
      (oauth-core/set-session-audience-scope session-id scope)
      (let [retrieved-scope (oauth-core/get-session-audience-scope session-id)]
        (is (= scope retrieved-scope) "Should retrieve same scope")))))

(deftest test-set-session-scope-with-audience
  (testing "Set scope with audience"
    (let [session-id (oauth-core/gen-session-id)
          audience "https://api.example.com"
          scope #{:read :write}]
      (oauth-core/set-session-audience-scope session-id audience scope)
      (let [retrieved-scope (oauth-core/get-session-audience-scope session-id audience)]
        (is (= scope retrieved-scope) "Should retrieve scope for audience")))))

;;; ============================================================================
;;; Client Matching Tests
;;; ============================================================================

(deftest test-clients-match-with-secret
  (testing "Client credentials matching with secret"
    (let [session-id (oauth-core/gen-session-id)
          client-id "test-match-client"
          client-secret "secret-123"
          client-uuid (id/data :test/oauth-client-4)
          _ (swap! oauth-core/*clients* assoc client-uuid
                   {:id client-id
                    :secret client-secret})
          _ (oauth-core/set-session session-id {:client client-uuid})
          matches? (oauth-core/clients-match? session-id
                                              {:client_id client-id
                                               :client_secret client-secret})]
      (is (true? matches?) "Clients should match with correct ID and secret"))))

(deftest test-clients-dont-match-wrong-secret
  (testing "Client credentials don't match with wrong secret"
    (let [session-id (oauth-core/gen-session-id)
          client-id "test-nomatch-client"
          correct-secret "correct-secret"
          wrong-secret "wrong-secret"
          client-uuid (id/data :test/oauth-client-5)
          _ (swap! oauth-core/*clients* assoc client-uuid
                   {:id client-id
                    :secret correct-secret})
          _ (oauth-core/set-session session-id {:client client-uuid})
          matches? (oauth-core/clients-match? session-id
                                              {:client_id client-id
                                               :client_secret wrong-secret})]
      (is (false? matches?) "Clients should not match with wrong secret"))))

;;; ============================================================================
;;; Encryption Tests (Session Data)
;;; ============================================================================

(deftest test-encrypt-decrypt-session-data
  (testing "Encrypt and decrypt session data"
    (let [data {:device-code "ABC123"
                :user-code "WXYZ"
                :ip "192.168.1.1"
                :user-agent "Mozilla/5.0"}
          encrypted (oauth-core/encrypt data)
          decrypted (oauth-core/decrypt encrypted)]
      (is (string? encrypted) "Encrypted data should be a string")
      (is (map? decrypted) "Decrypted data should be a map")
      (is (= data decrypted) "Decrypted data should match original"))))

(deftest test-decrypt-invalid-data
  (testing "Decrypt invalid data returns nil"
    (let [result (oauth-core/decrypt "invalid-encrypted-data")]
      (is (nil? result) "Should return nil for invalid encrypted data"))))

;;; ============================================================================
;;; Edge Cases
;;; ============================================================================

(deftest test-get-nonexistent-session
  (testing "Get non-existent session"
    (let [session (oauth-core/get-session "nonexistent-session-id")]
      (is (nil? session) "Should return nil for non-existent session"))))

(deftest test-clean-sessions
  (testing "Clean expired sessions"
    ;; This just verifies the function runs without errors
    (is (nil? (oauth-core/clean-sessions)) "clean-sessions should complete")))

(deftest test-empty-scope-set
  (testing "Session with empty scope set"
    (let [session-id (oauth-core/gen-session-id)]
      (oauth-core/set-session-audience-scope session-id #{})
      (is (= #{} (oauth-core/get-session-audience-scope session-id))
          "Should handle empty scope set"))))
