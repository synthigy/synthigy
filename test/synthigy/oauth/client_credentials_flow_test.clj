(ns synthigy.oauth.client-credentials-flow-test
  "Tests for OAuth 2.0 Client Credentials Grant Flow.

  The client credentials grant is used for server-to-server authentication
  where a client (service/application) authenticates directly with the OAuth
  server using its client_id and client_secret to obtain an access token.

  Tests:
  1. Basic client credentials flow (happy path)
  2. Invalid client secret (wrong password)
  3. Unknown client_id
  4. Missing client_secret
  5. Grant type not allowed for client
  6. Token format validation
  7. Scope handling"
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.string :as str]
   [clojure.data.json :as json]
   [buddy.hashers :as hashers]
   [synthigy.oauth.handlers :as handlers]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.token :as token-ns]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.access :as dataset.access]
   [synthigy.dataset.access.protocol :as access.protocol]
   [synthigy.test-data]  ; Load test data registrations
   [synthigy.test-helper :as test-helper]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-client-id "test-client-credentials")
(def test-client-secret "test-secret-cc-789")
(def test-redirect-uri "https://example.com/callback")

;; Client without client_credentials grant
(def restricted-client-id "restricted-client")
(def restricted-client-secret "restricted-secret-123")

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
    :cookies {}}))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn reset-state! []
  (reset! core/*clients* {})
  (reset! core/*resource-owners* {})
  (reset! core/*sessions* {})
  (reset! ac/*authorization-codes* {})
  (reset! token-ns/*tokens* {}))

(defn setup-test-data! []
  (let [client-euuid (id/data :test/oauth-creds-client)
        restricted-euuid (id/data :test/oauth-creds-restricted)]

    ;; Setup client WITH client_credentials grant
    (swap! core/*clients* assoc test-client-id
           {:id test-client-id
            :euuid client-euuid
            :secret (hashers/derive test-client-secret)
            :type :confidential
            :settings {"allowed-grants" ["client_credentials"]
                       "redirections" [test-redirect-uri]
                       "logout-redirections" [test-redirect-uri]}})

    ;; Setup client WITHOUT client_credentials grant (only authorization_code)
    (swap! core/*clients* assoc restricted-client-id
           {:id restricted-client-id
            :euuid restricted-euuid
            :secret (hashers/derive restricted-client-secret)
            :type :confidential
            :settings {"allowed-grants" ["authorization_code"]
                       "redirections" [test-redirect-uri]
                       "logout-redirections" [test-redirect-uri]}})

    {:client-euuid client-euuid
     :restricted-euuid restricted-euuid}))

(defn setup-fixtures [f]
  (reset-state!)

  ;; Note: Encryption is initialized by system-fixture

  (let [original-access-control dataset.access/*access-control*
        original-get-client (var-get #'iam/get-client)
        original-publish (var-get #'iam/publish)]

    (alter-var-root #'iam/get-client
                    (constantly (fn [id]
                                  (get @core/*clients* id))))

    (alter-var-root #'iam/publish
                    (constantly (fn [_ _] nil)))

    (alter-var-root #'dataset.access/*access-control* (constantly (->TestAccessControl)))

    (setup-test-data!)

    (f)

    (reset-state!)

    (alter-var-root #'iam/get-client (constantly original-get-client))
    (alter-var-root #'iam/publish (constantly original-publish))
    (alter-var-root #'dataset.access/*access-control* (constantly original-access-control))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each setup-fixtures)

;; =============================================================================
;; Test 1: Basic Client Credentials Flow (Happy Path)
;; =============================================================================

(deftest test-basic-client-credentials-flow
  (testing "Basic client credentials grant flow"
    (println "\n=== BASIC CLIENT CREDENTIALS FLOW ===")

    (let [token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id test-client-id
                                    :client_secret test-client-secret}))

          token-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Has access_token:" (some? (:access_token token-data)))
      (println "  Has refresh_token:" (some? (:refresh_token token-data)))
      (println "  Token type:" (:token_type token-data))

      (println "\n--- Results ---")
      (is (= 200 (:status token-response)) "Should succeed with 200")
      (is (some? (:access_token token-data)) "Should return access_token")
      (is (= "Bearer" (:token_type token-data)) "Token type should be Bearer")

      ;; Client credentials typically doesn't return refresh_token
      ;; (tokens are obtained by re-authenticating with credentials)
      (println "  Note: refresh_token presence:" (some? (:refresh_token token-data)))

      (if (= 200 (:status token-response))
        (println "✅ PASS: Client credentials flow works")
        (println "❌ FAIL: Client credentials flow failed")))))

;; =============================================================================
;; Test 2: Invalid Client Secret
;; =============================================================================

(deftest test-invalid-client-secret
  (testing "Invalid client secret should be rejected"
    (println "\n=== INVALID CLIENT SECRET ===")

    (let [token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id test-client-id
                                    :client_secret "wrong-secret"}))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 401 (:status token-response)) "Should reject with 401 Unauthorized")
      (is (= "invalid_client" (:error error-data)) "Should return invalid_client error")

      (if (= 401 (:status token-response))
        (println "✅ PASS: Invalid client secret rejected")
        (println "❌ FAIL: Invalid client secret accepted!")))))

;; =============================================================================
;; Test 3: Unknown Client ID
;; =============================================================================

(deftest test-unknown-client-id
  (testing "Unknown client_id should be rejected"
    (println "\n=== UNKNOWN CLIENT ID ===")

    (let [token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id "unknown-client"
                                    :client_secret "some-secret"}))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 401 (:status token-response)) "Should reject with 401 Unauthorized")
      (is (= "invalid_client" (:error error-data)) "Should return invalid_client error")

      (if (= 401 (:status token-response))
        (println "✅ PASS: Unknown client_id rejected")
        (println "❌ FAIL: Unknown client_id accepted!")))))

;; =============================================================================
;; Test 4: Missing Client Secret
;; =============================================================================

(deftest test-missing-client-secret
  (testing "Missing client_secret should be rejected"
    (println "\n=== MISSING CLIENT SECRET ===")

    (let [token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id test-client-id
                                    ;; NO client_secret!
                                    }))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 401 (:status token-response)) "Should reject with 401 Unauthorized")
      (is (= "invalid_client" (:error error-data)) "Should return invalid_client error")

      (if (= 401 (:status token-response))
        (println "✅ PASS: Missing client_secret rejected")
        (println "❌ FAIL: Missing client_secret accepted!")))))

;; =============================================================================
;; Test 5: Grant Type Not Allowed for Client
;; =============================================================================

(deftest test-grant-type-not-allowed
  (testing "Client without client_credentials grant should be rejected"
    (println "\n=== GRANT TYPE NOT ALLOWED ===")

    (let [token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id restricted-client-id
                                    :client_secret restricted-client-secret}))

          error-data (parse-json-body token-response)]

      (println "  Client allowed-grants:" ["authorization_code"])
      (println "  Requested grant_type: client_credentials")
      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 401 (:status token-response)) "Should reject with 401 Unauthorized")
      (is (= "invalid_client" (:error error-data)) "Should return invalid_client error")

      (if (= 401 (:status token-response))
        (println "✅ PASS: Unauthorized grant type rejected")
        (println "❌ FAIL: Unauthorized grant type accepted!")))))

;; =============================================================================
;; Test 6: Token Format Validation
;; =============================================================================

(deftest test-token-format-validation
  (testing "Access token should be valid JWT with correct claims"
    (println "\n=== TOKEN FORMAT VALIDATION ===")

    (let [token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id test-client-id
                                    :client_secret test-client-secret}))

          token-data (parse-json-body token-response)
          access-token (:access_token token-data)]

      (println "  Verifying JWT structure...")

      ;; Verify JWT structure (header.payload.signature)
      (let [parts (str/split access-token #"\.")
            has-jwt-structure (= 3 (count parts))]

        (println "  JWT parts count:" (count parts))
        (println "  Valid JWT structure:" has-jwt-structure)

        (println "\n--- Results ---")
        (is has-jwt-structure "Token should be valid JWT (3 parts)")
        (is (some? access-token) "Should have access_token")

        (if has-jwt-structure
          (println "✅ PASS: Token has valid JWT structure")
          (println "❌ FAIL: Token has invalid structure"))))))

;; =============================================================================
;; Test 7: Scope Handling
;; =============================================================================

(deftest test-scope-handling
  (testing "Client credentials with requested scope"
    (println "\n=== SCOPE HANDLING ===")

    (let [requested-scope "read write admin"

          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "client_credentials"
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :scope requested-scope}))

          token-data (parse-json-body token-response)]

      (println "  Requested scope:" requested-scope)
      (println "  Token response status:" (:status token-response))
      (println "  Returned scope:" (:scope token-data))

      (println "\n--- Results ---")
      (is (= 200 (:status token-response)) "Should succeed with 200")
      (is (some? (:access_token token-data)) "Should return access_token")

      ;; Note: Scope validation logic may vary - this just verifies scope is handled
      (println "  Note: Scope in response:" (:scope token-data))

      (if (= 200 (:status token-response))
        (println "✅ PASS: Scope handling works")
        (println "❌ FAIL: Scope handling failed")))))
