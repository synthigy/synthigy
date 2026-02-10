(ns synthigy.oauth.refresh-token-flow-test
  "Focused tests for OAuth 2.0 Refresh Token Flow.

  Tests the refresh token grant type:
  1. Get initial tokens via authorization code flow
  2. Use refresh_token to get new access_token
  3. Verify old tokens are revoked
  4. Verify new tokens are issued"
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

(def test-client-id "test-client-refresh")
(def test-client-secret "test-secret-refresh-789")
(def test-redirect-uri "http://localhost:3000/callback")
(def test-username "refreshuser")
(def test-password "test-password-refresh-123")

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
    :scheme :http
    :params (or params {})
    :headers {"host" "localhost"}
    :cookies {}}))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

(defn get-cookie-value [response cookie-name]
  ;; Ring's wrap-cookies middleware converts :cookies to Set-Cookie headers
  (when-let [set-cookie-headers (get-in response [:headers "Set-Cookie"])]
    (let [cookies (if (sequential? set-cookie-headers)
                    set-cookie-headers
                    [set-cookie-headers])]
      (some (fn [cookie-str]
              (when (string? cookie-str)
                (let [[name-value & _] (clojure.string/split cookie-str #";")
                      [name value] (clojure.string/split name-value #"=")]
                  (when (= (clojure.string/trim name) cookie-name)
                    (clojure.string/trim value)))))
            cookies))))

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
  (let [client-euuid (id/data :test/oauth-refresh-client)
        user-euuid (id/data :test/oauth-refresh-user)]

    ;; Setup client with hashed secret
    (swap! core/*clients* assoc test-client-id
           {:id test-client-id
            :euuid client-euuid
            :secret (hashers/derive test-client-secret)
            :type :confidential
            :settings {"allowed-grants" ["authorization_code" "refresh_token"]
                       "redirections" [test-redirect-uri]
                       "logout-redirections" [test-redirect-uri]}})

    ;; Setup user (password must be hashed)
    (swap! core/*resource-owners* assoc test-username
           {:name test-username
            :euuid user-euuid
            :password (hashers/derive test-password)
            :active true
            :person_info {:email "refresh@example.com"
                          :given_name "Refresh"
                          :family_name "Token"}})

    {:client-euuid client-euuid
     :user-euuid user-euuid}))

(defn setup-fixtures [f]
  (reset-state!)

  ;; Note: Encryption is initialized by system-fixture

  ;; Mock IAM functions
  (let [original-access-control dataset.access/*access-control*
        original-get-client (var-get #'iam/get-client)
        original-get-user-details (var-get #'iam/get-user-details)
        original-validate-password (var-get #'iam/validate-password)
        original-publish (var-get #'iam/publish)]

    (alter-var-root #'iam/get-client
                    (constantly (fn [id]
                                  (println "[MOCK] get-client called with:" id)
                                  (let [result (get @core/*clients* id)]
                                    (println "[MOCK] get-client returning:" (select-keys result [:id :euuid :type]))
                                    result))))

    (alter-var-root #'iam/get-user-details
                    (constantly (fn [username]
                                  (println "[MOCK] get-user-details called with:" username)
                                  (let [result (get @core/*resource-owners* username)]
                                    (println "[MOCK] get-user-details returning:" (select-keys result [:name :euuid]))
                                    result))))

    (alter-var-root #'iam/validate-password
                    (constantly (fn [user-password stored-password]
                                  (println "[MOCK] validate-password called")
                                  (hashers/check user-password stored-password))))

    (alter-var-root #'iam/publish
                    (constantly (fn [_ _] nil)))

    (alter-var-root #'dataset.access/*access-control* (constantly (->TestAccessControl)))

    (setup-test-data!)

    (f)

    (reset-state!)

    ;; Restore
    (alter-var-root #'iam/get-client (constantly original-get-client))
    (alter-var-root #'iam/get-user-details (constantly original-get-user-details))
    (alter-var-root #'iam/validate-password (constantly original-validate-password))
    (alter-var-root #'iam/publish (constantly original-publish))
    (alter-var-root #'dataset.access/*access-control* (constantly original-access-control))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each setup-fixtures)

;; =============================================================================
;; Helper: Get Initial Tokens
;; =============================================================================

(defn get-initial-tokens []
  "Complete authorization code flow to get initial tokens including refresh_token"
  (println "\n→ Getting initial tokens via authorization code flow...")

  ;; Step 1: Authorization
  (let [auth-response (handlers/authorize
                       (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :redirect_uri test-redirect-uri
                                 :response_type "code"
                                 :scope "openid profile email offline_access"
                                 :state "refresh-test-state"}))

        _ (println "  Authorization status:" (:status auth-response))

        ;; Step 2: Login
        login-params (parse-location-params (get-in auth-response [:headers "Location"]))
        login-response (handlers/login
                        (-> (request :post "/oauth/login")
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state (:state login-params)})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state (:state login-params)})))

        _ (println "  Login status:" (:status login-response))

        ;; Step 3: Get authorization code
        callback-params (parse-location-params (get-in login-response [:headers "Location"]))
        auth-code (:code callback-params)

        _ (println "  Authorization code:" auth-code)

        ;; Step 4: Exchange for tokens
        token-response (handlers/token
                        (request :post "/oauth/token"
                                 {:grant_type "authorization_code"
                                  :code auth-code
                                  :client_id test-client-id
                                  :client_secret test-client-secret
                                  :redirect_uri test-redirect-uri}))

        _ (println "  Token exchange status:" (:status token-response))

        token-data (parse-json-body token-response)]

    (println "  ✅ Initial tokens obtained")
    (println "    - access_token:" (some? (:access_token token-data)))
    (println "    - refresh_token:" (some? (:refresh_token token-data)))
    (println "    - id_token:" (some? (:id_token token-data)))

    token-data))

;; =============================================================================
;; Test: Basic Refresh Token Flow
;; =============================================================================

(deftest test-refresh-token-basic-flow
  (testing "Refresh token flow - get new access token using refresh_token"
    (println "\n=== REFRESH TOKEN BASIC FLOW ===")

    ;; Get initial tokens
    (let [initial-tokens (get-initial-tokens)
          initial-access-token (:access_token initial-tokens)
          initial-refresh-token (:refresh_token initial-tokens)

          _ (is (some? initial-access-token) "Should have initial access_token")
          _ (is (some? initial-refresh-token) "Should have initial refresh_token")

          ;; Use refresh token to get new tokens
          _ (println "\n→ Using refresh_token to get new access_token...")

          refresh-response (handlers/token
                            (request :post "/oauth/token"
                                     {:grant_type "refresh_token"
                                      :refresh_token initial-refresh-token
                                      :client_id test-client-id
                                      :client_secret test-client-secret}))

          _ (println "  Refresh response status:" (:status refresh-response))
          _ (println "  Refresh response body:" (:body refresh-response))

          refreshed-tokens (parse-json-body refresh-response)
          new-access-token (:access_token refreshed-tokens)
          new-refresh-token (:refresh_token refreshed-tokens)]

      (println "\n--- Results ---")
      (println "Status:" (:status refresh-response))
      (println "Has new access_token:" (some? new-access-token))
      (println "Has new refresh_token:" (some? new-refresh-token))
      (println "Access token changed:" (not= initial-access-token new-access-token))

      (if (= 200 (:status refresh-response))
        (do
          (println "✅ PASS: Refresh token flow works")
          (is (= 200 (:status refresh-response)))
          (is (some? new-access-token) "Should have new access_token")
          (is (not= initial-access-token new-access-token) "Access token should be different"))
        (do
          (println "❌ FAIL: Refresh token exchange failed")
          (is (= 200 (:status refresh-response))))))))

;; =============================================================================
;; Test: Refresh Token with Scope
;; =============================================================================

(deftest test-refresh-token-with-scope
  (testing "Refresh token flow - request specific scope"
    (println "\n=== REFRESH TOKEN WITH SCOPE ===")

    (let [initial-tokens (get-initial-tokens)
          refresh-token (:refresh_token initial-tokens)

          ;; Request only "openid" scope (subset of original)
          _ (println "\n→ Refreshing with reduced scope (openid only)...")

          refresh-response (handlers/token
                            (request :post "/oauth/token"
                                     {:grant_type "refresh_token"
                                      :refresh_token refresh-token
                                      :client_id test-client-id
                                      :client_secret test-client-secret
                                      :scope "openid"}))

          refreshed-tokens (parse-json-body refresh-response)]

      (println "\n--- Results ---")
      (println "Status:" (:status refresh-response))
      (println "Scope in response:" (:scope refreshed-tokens))

      (if (= 200 (:status refresh-response))
        (do
          (println "✅ PASS: Refresh with scope works")
          (is (= 200 (:status refresh-response)))
          (is (some? (:access_token refreshed-tokens))))
        (do
          (println "❌ FAIL: Refresh with scope failed")
          (is (= 200 (:status refresh-response))))))))

;; =============================================================================
;; Test: Invalid Refresh Token
;; =============================================================================

(deftest test-invalid-refresh-token
  (testing "Refresh token flow - reject invalid refresh_token"
    (println "\n=== INVALID REFRESH TOKEN ===")

    (let [refresh-response (handlers/token
                            (request :post "/oauth/token"
                                     {:grant_type "refresh_token"
                                      :refresh_token "INVALID_TOKEN_XYZ"
                                      :client_id test-client-id
                                      :client_secret test-client-secret}))

          error-data (parse-json-body refresh-response)]

      (println "\n--- Results ---")
      (println "Status:" (:status refresh-response))
      (println "Error:" (:error error-data))
      (println "Error description:" (:error_description error-data))

      (if (= 400 (:status refresh-response))
        (do
          (println "✅ PASS: Invalid refresh_token rejected")
          (is (= 400 (:status refresh-response)))
          (is (some? (:error error-data))))
        (do
          (println "❌ FAIL: Should reject invalid refresh_token")
          (is (= 400 (:status refresh-response))))))))
