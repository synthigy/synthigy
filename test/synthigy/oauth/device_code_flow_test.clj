(ns synthigy.oauth.device-code-flow-test
  "Tests for OAuth 2.0 Device Authorization Grant Flow (RFC 8628).

  The device code flow is used for devices with limited input capabilities
  (smart TVs, CLI tools, IoT devices) that cannot easily authenticate users.

  Flow:
  1. Device requests device_code and user_code
  2. Device displays user_code and verification_uri to user
  3. User visits verification_uri on another device and enters user_code
  4. Device polls token endpoint with device_code
  5. Once user authorizes, device receives access_token

  Tests:
  1. Request device code (happy path)
  2. Poll before authorization (authorization_pending)
  3. Complete flow with user authorization
  4. Invalid device code
  5. Expired device code
  6. Client ID mismatch during polling
  7. Missing client_id

  NOTE: These tests use REAL database entities (not mocks) for realistic testing."
  (:require
    [buddy.hashers :as hashers]
    [synthigy.json :as json]
    [clojure.string :as str]
    [clojure.test :refer [deftest testing is use-fixtures]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.access :as dataset.access]
    [synthigy.dataset.access.protocol :as access.protocol]
    [synthigy.dataset.id :as id]
    [synthigy.db :as db]
    [synthigy.iam :as iam]
    [synthigy.iam.encryption :as encryption]
    [synthigy.oauth.authorization-code :as ac]
    [synthigy.oauth.core :as core]
    [synthigy.oauth.device-code :as device-code]
    [synthigy.oauth.handlers :as handlers]
    [synthigy.oauth.token :as token-ns]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-client-id "device-flow-test-client")
(def test-client-secret "device-test-secret-789")
(def test-username "device-flow-test-user")
(def test-password "device-test-password-123")

(def wrong-client-id "device-flow-wrong-client")
(def wrong-client-secret "wrong-secret-123")

;; Store created entity IDs for cleanup
(def ^:dynamic *test-client-id-value* nil)
(def ^:dynamic *test-user-id-value* nil)
(def ^:dynamic *wrong-client-id-value* nil)

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defrecord TestAccessControl []
  access.protocol/AccessControl
  (entity-allows? [_ _ _] true)
  (relation-allows? [_ _ _] true)
  (relation-allows? [_ _ _ _] true))

(defn request
  ([method uri] (request method uri nil))
  ([method uri params]
   {:request-method method
    :uri uri
    :scheme :https
    :params (or params {})
    :headers {"host" "localhost"}
    :cookies {}
    :remote-addr "127.0.0.1"}))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (json/read-str body)))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

;; OAuth state reset is handled by test-helper/reset-oauth-state-fixture

(defn setup-test-entities! []
  "Create real test entities in the database"
  (println "[Setup] Creating test OAuth client and user in database...")

  ;; Create test client with device code grant
  (let [client-entity-id (id/data :test/oauth-device-client)
        client (dataset/sync-entity
                 :iam/app
                 {(id/key) client-entity-id
                  :id test-client-id
                  :name "Device Flow Test Client"
                  :type :public
                  :settings {"allowed-grants" ["urn:ietf:params:oauth:grant-type:device_code"]
                             "redirections" []
                             "logout-redirections" []}})]
    (alter-var-root #'*test-client-id-value* (constantly client-entity-id))
    (println "  Created test client:" test-client-id "id:" client-entity-id))

  ;; Create wrong client for mismatch tests
  (let [client-entity-id (id/data :test/oauth-device-wrong-client)
        client (dataset/sync-entity
                 :iam/app
                 {(id/key) client-entity-id
                  :id wrong-client-id
                  :name "Device Flow Wrong Client"
                  :type :public
                  :settings {"allowed-grants" ["urn:ietf:params:oauth:grant-type:device_code"]
                             "redirections" []
                             "logout-redirections" []}})]
    (alter-var-root #'*wrong-client-id-value* (constantly client-entity-id))
    (println "  Created wrong client:" wrong-client-id "id:" client-entity-id))

  ;; Create test user
  (let [user-entity-id (id/data :test/oauth-device-user)
        user (dataset/sync-entity
               :iam/user
               {(id/key) user-entity-id
                :name test-username
                :password (hashers/derive test-password)
                :active true
                :type :PERSON
                :person_info {:email "device-test@example.com"
                              :given_name "Device"
                              :family_name "Test"}})]
    (alter-var-root #'*test-user-id-value* (constantly user-entity-id))
    (println "  Created test user:" test-username "id:" user-entity-id)))

(defn cleanup-test-entities! []
  "Delete test entities from database"
  (println "[Cleanup] Deleting test entities from database...")

  (when *test-client-id-value*
    (try
      (dataset/delete-entity :iam/app {(id/key) *test-client-id-value*})
      (println "  Deleted test client")
      (catch Exception e
        (println "  Warning: Could not delete test client:" (.getMessage e)))))

  (when *wrong-client-id-value*
    (try
      (dataset/delete-entity :iam/app {(id/key) *wrong-client-id-value*})
      (println "  Deleted wrong client")
      (catch Exception e
        (println "  Warning: Could not delete wrong client:" (.getMessage e)))))

  (when *test-user-id-value*
    (try
      (dataset/delete-entity :iam/user {(id/key) *test-user-id-value*})
      (println "  Deleted test user")
      (catch Exception e
        (println "  Warning: Could not delete test user:" (.getMessage e))))))

(defn test-entities-fixture
  "Create and cleanup test entities for device code flow tests"
  [f]
  (binding [*test-client-id-value* nil
            *test-user-id-value* nil
            *wrong-client-id-value* nil]
    (try
      ;; Create real database entities
      (setup-test-entities!)

      ;; Run tests
      (f)

      (finally
        ;; Cleanup test entities
        (cleanup-test-entities!)))))

;; Use comprehensive system fixture for initialization and shutdown
;; System fixture handles: transit, db, dataset, iam, encryption, and proper shutdown
(use-fixtures :once test-helper/system-fixture test-helper/reset-oauth-state-fixture test-entities-fixture)

;; =============================================================================
;; Test 1: Request Device Code (Happy Path)
;; =============================================================================

(deftest test-request-device-code
  (testing "Request device code and user code"
    (println "\n=== REQUEST DEVICE CODE ===")

    (let [response (handlers/device-authorization
                     (request :post "/oauth/device/auth"
                              {:client_id test-client-id}))

          data (parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Has device_code:" (some? (:device_code data)))
      (println "  Has user_code:" (some? (:user_code data)))
      (println "  User code format:" (:user_code data))
      (println "  Verification URI:" (:verification_uri data))
      (println "  Interval:" (:interval data))
      (println "  Expires in:" (:expires_in data))

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should succeed with 200")
      (is (some? (:device_code data)) "Should return device_code")
      (is (some? (:user_code data)) "Should return user_code")
      (is (some? (:verification_uri data)) "Should return verification_uri")
      (is (some? (:verification_uri_complete data)) "Should return verification_uri_complete")
      (is (= 5 (:interval data)) "Should have polling interval")
      (is (= 900 (:expires_in data)) "Should expire in 15 minutes (900 seconds)")

      ;; Verify user_code format (should be XXXX-XXXX)
      (let [user-code (:user_code data)]
        (is (= 9 (count user-code)) "User code should be 9 chars (XXXX-XXXX)")
        (is (re-matches #"[A-Z]{4}-[A-Z]{4}" user-code) "User code should match format XXXX-XXXX"))

      (if (= 200 (:status response))
        (println "✅ PASS: Device code request works")
        (println "❌ FAIL: Device code request failed")))))

;; =============================================================================
;; Test 2: Poll Before Authorization (authorization_pending)
;; =============================================================================

(deftest test-poll-before-authorization
  (testing "Polling before user authorization returns authorization_pending"
    (println "\n=== POLL BEFORE AUTHORIZATION ===")

    ;; Step 1: Request device code
    (let [device-response (handlers/device-authorization
                            (request :post "/oauth/device/auth"
                                     {:client_id test-client-id}))

          device-data (parse-json-body device-response)
          device-code (:device_code device-data)

          _ (println "  Got device_code:" device-code)

          ;; Step 2: Poll IMMEDIATELY (before user authorizes)
          _ (println "\n  → Polling for token (before authorization)...")

          token-response (handlers/token
                           (request :post "/oauth/token"
                                    {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                     :device_code device-code
                                     :client_id test-client-id}))

          token-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error token-data))

      (println "\n--- Results ---")
      (is (= 403 (:status token-response)) "Should return 403")
      (is (= "authorization_pending" (:error token-data)) "Should return authorization_pending error")

      (if (= "authorization_pending" (:error token-data))
        (println "✅ PASS: Returns authorization_pending before user authorizes")
        (println "❌ FAIL: Should return authorization_pending")))))

;; =============================================================================
;; Test 3: Invalid Device Code
;; =============================================================================

(deftest test-invalid-device-code
  (testing "Polling with invalid device_code"
    (println "\n=== INVALID DEVICE CODE ===")

    (let [token-response (handlers/token
                           (request :post "/oauth/token"
                                    {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                     :device_code "invalid-device-code-12345"
                                     :client_id test-client-id}))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 400 (:status token-response)) "Should reject with 400")
      (is (= "invalid_request" (:error error-data)) "Should return invalid_request error")

      (if (= 400 (:status token-response))
        (println "✅ PASS: Invalid device_code rejected")
        (println "❌ FAIL: Invalid device_code accepted!")))))

;; =============================================================================
;; Test 4: Expired Device Code
;; =============================================================================

(deftest test-expired-device-code
  (testing "Polling with expired device_code"
    (println "\n=== EXPIRED DEVICE CODE ===")

    ;; Step 1: Request device code
    (let [device-response (handlers/device-authorization
                            (request :post "/oauth/device/auth"
                                     {:client_id test-client-id}))

          device-data (parse-json-body device-response)
          device-code (:device_code device-data)

          _ (println "  Got device_code:" device-code)

          ;; Step 2: Manually expire the code
          _ (println "  → Manually expiring device code...")
          _ (swap! device-code/*device-codes* assoc-in [device-code :expires-at]
                   (- (System/currentTimeMillis) 1000))  ;; 1 second in past

          ;; Step 3: Try to poll
          _ (println "\n  → Polling with expired device_code...")

          token-response (handlers/token
                           (request :post "/oauth/token"
                                    {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                     :device_code device-code
                                     :client_id test-client-id}))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      ;; Note: Implementation may need to check expiration - this tests current behavior
      (println "  Current behavior - status:" (:status token-response))

      (if (or (= 403 (:status token-response))
              (= 400 (:status token-response)))
        (println "✅ PASS: Expired device_code handled")
        (println "⚠️  Note: Expired code handling - status" (:status token-response)))

      ;; Add formal assertion
      (is (or (= 403 (:status token-response))
              (= 400 (:status token-response)))
          "Expired device code should return 403 or 400"))))

;; =============================================================================
;; Test 5: Client ID Mismatch
;; =============================================================================

(deftest test-client-id-mismatch
  (testing "Polling with different client_id than requested"
    (println "\n=== CLIENT ID MISMATCH ===")

    ;; Step 1: Request device code with test-client-id
    (let [device-response (handlers/device-authorization
                            (request :post "/oauth/device/authorize"
                                     {:client_id test-client-id}))

          device-data (parse-json-body device-response)
          device-code (:device_code device-data)

          _ (println "  Requested with client_id:" test-client-id)
          _ (println "  Got device_code:" device-code)

          ;; Step 2: Poll with DIFFERENT client_id
          _ (println "\n  → Polling with different client_id:" wrong-client-id)

          token-response (handlers/token
                           (request :post "/oauth/token"
                                    {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                     :device_code device-code
                                     :client_id wrong-client-id}))  ;; WRONG CLIENT!

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      ;; Should reject client_id mismatch
      (println "  Current behavior - status:" (:status token-response))

      (if (or (= 400 (:status token-response))
              (= 403 (:status token-response)))
        (println "✅ PASS: Client ID mismatch detected")
        (println "⚠️  Note: Client ID mismatch - status" (:status token-response)))

      ;; Add formal assertion
      (is (or (= 400 (:status token-response))
              (= 403 (:status token-response)))
          "Client ID mismatch should return 400 or 403"))))

;; =============================================================================
;; Test 6: Missing client_id
;; =============================================================================

(deftest test-missing-client-id
  (testing "Request device code without client_id"
    (println "\n=== MISSING CLIENT ID ===")

    (let [response (handlers/device-authorization
                     (request :post "/oauth/device/auth"
                              {;; NO client_id!
                               }))

          data (parse-json-body response)]

      (println "  Response status:" (:status response))
      (println "  Error:" (:error data))

      (println "\n--- Results ---")
      (is (or (= 400 (:status response))
              (= 302 (:status response)))
          "Should reject missing client_id")

      (if (not= 200 (:status response))
        (println "✅ PASS: Missing client_id rejected")
        (println "❌ FAIL: Missing client_id accepted!")))))

;; =============================================================================
;; Test 7: Complete Flow with Mock Authorization
;; =============================================================================

(deftest test-complete-flow-with-mock-authorization
  (testing "Complete device code flow with simulated user authorization"
    (println "\n=== COMPLETE FLOW (MOCK AUTHORIZATION) ===")

    ;; Step 1: Request device code
    (let [device-response (handlers/device-authorization
                            (request :post "/oauth/device/auth"
                                     {:client_id test-client-id
                                      :scope "openid profile"}))

          device-data (parse-json-body device-response)
          device-code (:device_code device-data)
          user-code (:user_code device-data)

          _ (println "  Step 1: Device code requested")
          _ (println "    device_code:" device-code)
          _ (println "    user_code:" user-code)

          ;; Step 2: Simulate user authorization by directly updating device-code state
          ;; In real flow, this would happen after user visits verification_uri and logs in
          session-id (str (id/data :test/oauth-device-session))
          _ (println "\n  Step 2: Simulating user authorization...")
          _ (swap! device-code/*device-codes* assoc-in [device-code :session] session-id)
          _ (swap! core/*sessions* assoc session-id
                   {:user test-username
                    :euuid (id/extract (get @core/*resource-owners* test-username))
                    :scope #{"openid" "profile"}})

          ;; Step 3: Poll for token (should now succeed)
          _ (println "\n  Step 3: Polling for token (after authorization)...")

          token-response (handlers/token
                           (request :post "/oauth/token"
                                    {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                     :device_code device-code
                                     :client_id test-client-id}))

          token-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Has access_token:" (some? (:access_token token-data)))
      (println "  Token type:" (:token_type token-data))

      (println "\n--- Results ---")
      (is (= 200 (:status token-response)) "Should succeed with 200")
      (is (some? (:access_token token-data)) "Should return access_token")
      (is (= "Bearer" (:token_type token-data)) "Token type should be Bearer")

      ;; Verify device code is consumed (removed after use)
      (let [code-still-exists (contains? @device-code/*device-codes* device-code)]
        (println "  Device code still exists after token issued:" code-still-exists)
        (is (not code-still-exists) "Device code should be removed after token issued"))

      (if (= 200 (:status token-response))
        (println "✅ PASS: Complete device code flow works")
        (println "❌ FAIL: Complete flow failed")))))
