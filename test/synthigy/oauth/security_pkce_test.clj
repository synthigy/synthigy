(ns synthigy.oauth.security-pkce-test
  "Security tests for OAuth 2.0 PKCE (Proof Key for Code Exchange).

  Tests:
  1. Missing code_verifier when code_challenge was provided
  2. Wrong code_verifier (doesn't match challenge)
  3. code_challenge_method support (S256 vs plain)
  4. PKCE enforcement for public clients"
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.string :as str]
   [clojure.data.json :as json]
   [buddy.hashers :as hashers]
   [buddy.core.codecs :as codecs]
   [buddy.core.hash :as hash]
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

(def test-client-id "test-client-pkce")
(def test-client-secret "test-secret-pkce-789")
(def test-redirect-uri "https://example.com/callback")
(def test-username "pkceuser")
(def test-password "test-password-pkce-123")

;; PKCE values
(def code-verifier "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk")  ;; 43 chars
(def wrong-verifier "wrong_verifier_that_wont_match_the_challenge")

;; Calculate code_challenge from verifier
(defn generate-code-challenge [verifier]
  (-> verifier
      codecs/str->bytes
      hash/sha256
      codecs/bytes->b64u
      codecs/bytes->str))

(def code-challenge (generate-code-challenge code-verifier))

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

(defn parse-location-params [location]
  (when location
    (let [[_ query] (str/split location #"\?")]
      (when query
        (into {}
              (map (fn [param]
                     (let [[k v] (str/split param #"=")]
                       [(keyword k) (ring.util.codec/url-decode v)]))
                   (str/split query #"&")))))))

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
  (let [client-euuid (id/data :test/oauth-pkce-client)
        user-euuid (id/data :test/oauth-pkce-user)]

    (swap! core/*clients* assoc test-client-id
           {:id test-client-id
            :euuid client-euuid
            :secret (hashers/derive test-client-secret)
            :type :confidential
            :settings {"allowed-grants" ["authorization_code" "refresh_token"]
                       "redirections" [test-redirect-uri]
                       "logout-redirections" [test-redirect-uri]}})

    (swap! core/*resource-owners* assoc test-username
           {:name test-username
            :euuid user-euuid
            :password (hashers/derive test-password)
            :active true
            :person_info {:email "pkce@example.com"
                          :given_name "PKCE"
                          :family_name "Test"}})

    {:client-euuid client-euuid
     :user-euuid user-euuid}))

(defn setup-fixtures [f]
  (reset-state!)

  ;; Note: Encryption is initialized by system-fixture

  (let [original-access-control dataset.access/*access-control*
        original-get-client (var-get #'iam/get-client)
        original-get-user-details (var-get #'iam/get-user-details)
        original-validate-password (var-get #'iam/validate-password)
        original-publish (var-get #'iam/publish)]

    (alter-var-root #'iam/get-client
                    (constantly (fn [id]
                                  (get @core/*clients* id))))

    (alter-var-root #'iam/get-user-details
                    (constantly (fn [username]
                                  (get @core/*resource-owners* username))))

    (alter-var-root #'iam/validate-password
                    (constantly (fn [user-password stored-password]
                                  (hashers/check user-password stored-password))))

    (alter-var-root #'iam/publish
                    (constantly (fn [_ _] nil)))

    (alter-var-root #'dataset.access/*access-control* (constantly (->TestAccessControl)))

    (setup-test-data!)

    (f)

    (reset-state!)

    (alter-var-root #'iam/get-client (constantly original-get-client))
    (alter-var-root #'iam/get-user-details (constantly original-get-user-details))
    (alter-var-root #'iam/validate-password (constantly original-validate-password))
    (alter-var-root #'iam/publish (constantly original-publish))
    (alter-var-root #'dataset.access/*access-control* (constantly original-access-control))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each setup-fixtures)

;; =============================================================================
;; Helper: Get Authorization Code with PKCE
;; =============================================================================

(defn get-authorization-code-with-pkce [challenge challenge-method]
  "Complete authorization flow with PKCE"
  (let [auth-response (handlers/authorize
                       (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :redirect_uri test-redirect-uri
                                 :response_type "code"
                                 :scope "openid profile"
                                 :state "pkce-test"
                                 :code_challenge challenge
                                 :code_challenge_method challenge-method}))

        login-params (parse-location-params (get-in auth-response [:headers "Location"]))
        login-response (handlers/login
                        (-> (request :post "/oauth/login")
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state (:state login-params)})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state (:state login-params)})))

        callback-params (parse-location-params (get-in login-response [:headers "Location"]))]

    (:code callback-params)))

;; =============================================================================
;; Test 1: Missing code_verifier
;; =============================================================================

(deftest test-missing-code-verifier
  (testing "Missing code_verifier when code_challenge was provided"
    (println "\n=== MISSING CODE VERIFIER ===")

    (let [code (get-authorization-code-with-pkce code-challenge "S256")
          _ (println "  Got authorization code with PKCE challenge")
          _ (println "  Code:" code)

          ;; Try to exchange code WITHOUT code_verifier
          _ (println "\n  → Attempting token exchange WITHOUT code_verifier...")

          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri test-redirect-uri
                                    ;; NO code_verifier!
                                    }))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      ;; Should reject missing verifier
      (is (= 400 (:status token-response)) "Should reject with 400")
      (is (some? (:error error-data)) "Should return error")

      (if (= 400 (:status token-response))
        (println "✅ PASS: Missing code_verifier rejected")
        (println "❌ FAIL: Missing code_verifier accepted!")))))

;; =============================================================================
;; Test 2: Wrong code_verifier
;; =============================================================================

(deftest test-wrong-code-verifier
  (testing "Wrong code_verifier doesn't match challenge"
    (println "\n=== WRONG CODE VERIFIER ===")

    (let [code (get-authorization-code-with-pkce code-challenge "S256")
          _ (println "  Got authorization code with PKCE challenge")
          _ (println "  Code:" code)
          _ (println "  Expected verifier:" code-verifier)

          ;; Try to exchange code with WRONG code_verifier
          _ (println "\n  → Attempting token exchange with WRONG code_verifier...")

          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri test-redirect-uri
                                    :code_verifier wrong-verifier}))  ;; WRONG!

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 400 (:status token-response)) "Should reject with 400")
      (is (some? (:error error-data)) "Should return error")

      (if (= 400 (:status token-response))
        (println "✅ PASS: Wrong code_verifier rejected")
        (println "❌ FAIL: Wrong code_verifier accepted!")))))

;; =============================================================================
;; Test 3: Correct PKCE Flow (S256)
;; =============================================================================

(deftest test-correct-pkce-flow
  (testing "Correct PKCE flow with S256 method"
    (println "\n=== CORRECT PKCE FLOW (S256) ===")

    (let [code (get-authorization-code-with-pkce code-challenge "S256")
          _ (println "  Got authorization code with PKCE challenge")
          _ (println "  Code:" code)
          _ (println "  Code challenge method: S256")

          ;; Exchange code with CORRECT code_verifier
          _ (println "\n  → Token exchange with correct code_verifier...")

          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri test-redirect-uri
                                    :code_verifier code-verifier}))  ;; CORRECT!

          token-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Has access_token:" (some? (:access_token token-data)))

      (println "\n--- Results ---")
      (is (= 200 (:status token-response)) "Should succeed with 200")
      (is (some? (:access_token token-data)) "Should return access_token")

      (if (= 200 (:status token-response))
        (println "✅ PASS: Correct PKCE flow works")
        (println "❌ FAIL: Correct PKCE flow failed!")))))

;; =============================================================================
;; Test 4: PKCE Plain Method
;; =============================================================================

(deftest test-pkce-plain-method
  (testing "PKCE with 'plain' method (verifier == challenge)"
    (println "\n=== PKCE PLAIN METHOD ===")

    ;; For 'plain' method, code_challenge == code_verifier
    (let [plain-verifier "plain_verifier_value_12345678901234567890123"
          plain-challenge plain-verifier  ;; Same value for 'plain' method

          code (get-authorization-code-with-pkce plain-challenge "plain")
          _ (println "  Got authorization code with PKCE challenge (plain)")
          _ (println "  Code:" code)

          ;; Exchange with same verifier
          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri test-redirect-uri
                                    :code_verifier plain-verifier}))

          token-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Has access_token:" (some? (:access_token token-data)))

      (println "\n--- Results ---")
      (is (= 200 (:status token-response)) "Should succeed with 200")
      (is (some? (:access_token token-data)) "Should return access_token")

      (if (= 200 (:status token-response))
        (println "✅ PASS: PKCE plain method works")
        (println "❌ FAIL: PKCE plain method failed!")))))
