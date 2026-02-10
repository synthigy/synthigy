(ns synthigy.oauth.security-code-expiration-test
  "Security tests for OAuth 2.0 authorization code expiration.

  Tests:
  1. Expired authorization codes are rejected
  2. Code expiry timing (5 minutes default)
  3. Expired codes cannot be used for token exchange"
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
   [synthigy.test-helper :as test-helper]
   [timing.core :as timing]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-client-id "test-client-expiry")
(def test-client-secret "test-secret-expiry-789")
(def test-redirect-uri "https://example.com/callback")
(def test-username "expiryuser")
(def test-password "test-password-expiry-123")

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
  (let [client-euuid (id/data :test/oauth-expire-client)
        user-euuid (id/data :test/oauth-expire-user)]

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
            :person_info {:email "expiry@example.com"
                          :given_name "Expiry"
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
;; Helper: Get Authorization Code
;; =============================================================================

(defn get-authorization-code []
  "Complete authorization flow to get a code"
  (let [auth-response (handlers/authorize
                       (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :redirect_uri test-redirect-uri
                                 :response_type "code"
                                 :scope "openid profile"
                                 :state "expiry-test"}))

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
;; Test 1: Expired Authorization Code
;; =============================================================================

(deftest test-expired-authorization-code
  (testing "Expired authorization code is rejected"
    (println "\n=== EXPIRED AUTHORIZATION CODE ===")

    ;; Get a fresh code
    (let [code (get-authorization-code)
          _ (println "  Got authorization code:" code)

          ;; Manually expire the code by setting expires-at in the past
          _ (println "  → Manually expiring the code...")
          _ (swap! ac/*authorization-codes* assoc-in [code :expires-at]
                   (- (System/currentTimeMillis) 1000))  ;; 1 second in the past

          ;; Check the expires-at value
          expires-at (get-in @ac/*authorization-codes* [code :expires-at])
          _ (println "  Code expires-at:" expires-at)
          _ (println "  Current time:" (System/currentTimeMillis))
          _ (println "  Expired:" (< expires-at (System/currentTimeMillis)))

          ;; Try to use the expired code
          _ (println "\n  → Attempting to exchange expired code...")

          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri test-redirect-uri}))

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))
      (println "  Error description:" (:error_description error-data))

      (println "\n--- Results ---")
      (is (= 400 (:status token-response)) "Should reject with 400")
      (is (some? (:error error-data)) "Should return error")

      (if (= 400 (:status token-response))
        (println "✅ PASS: Expired authorization code rejected")
        (println "❌ FAIL: Expired authorization code accepted!")))))

;; =============================================================================
;; Test 2: Code Expiry Timing
;; =============================================================================

(deftest test-code-expiry-timing
  (testing "Authorization codes expire after 5 minutes"
    (println "\n=== CODE EXPIRY TIMING ===")

    (let [code (get-authorization-code)
          _ (println "  Got authorization code:" code)

          ;; Check when the code was marked as issued
          code-data (get @ac/*authorization-codes* code)
          expires-at (:expires-at code-data)
          created-on (:created-on code-data)

          _ (println "  Code created-on:" created-on)
          _ (println "  Code expires-at:" expires-at)

          ;; Calculate expiry duration
          expiry-duration-ms (when (and expires-at created-on)
                               (- expires-at created-on))
          expiry-duration-min (when expiry-duration-ms
                                (/ expiry-duration-ms 1000 60))]

      (println "  Expiry duration:" expiry-duration-min "minutes")

      (println "\n--- Results ---")
      ;; OAuth 2.0 recommends short-lived codes (typically 5-10 minutes)
      (is (some? expires-at) "Code should have expires-at timestamp")
      (is (some? expiry-duration-min) "Should calculate expiry duration")

      (when expiry-duration-min
        (is (>= expiry-duration-min 4) "Code should live at least 4 minutes")
        (is (<= expiry-duration-min 10) "Code should expire within 10 minutes")

        (if (and (>= expiry-duration-min 4) (<= expiry-duration-min 10))
          (println "✅ PASS: Code expiry timing is secure")
          (println "⚠️  WARNING: Code expiry duration is" expiry-duration-min "minutes"))))))
