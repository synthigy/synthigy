(ns synthigy.oauth.authorization-code-flow-test
  "Focused tests for OAuth 2.0 Authorization Code Flow.

  Tests each step of the flow in isolation:
  1. Client requests authorization
  2. User redirected to login
  3. User authenticates
  4. Authorization code issued
  5. Client exchanges code for tokens"
  (:require
   [buddy.hashers :as hashers]
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.test :refer [deftest testing is use-fixtures]]
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

(def test-client-id "test-client-auth-code")
(def test-client-secret "test-secret-auth-code")
(def test-redirect-uri "http://localhost:3000/callback")
(def test-username "authcodeuser")
(def test-password "authcodepass123")

;; =============================================================================
;; Mock Access Control
;; =============================================================================

(defrecord TestAccessControl []
  access.protocol/AccessControl
  (entity-allows? [_ _ _] true)
  (relation-allows? [_ _ _] true)
  (relation-allows? [_ _ _ _] true)
  (scope-allowed? [_ _] true)
  (roles-allowed? [_ _] true)
  (superuser? [_] true)
  (get-user [_] nil)
  (get-roles [_] #{})
  (get-groups [_] #{}))

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn request
  "Create a Ring request map"
  [method uri & [params]]
  {:request-method method
   :uri uri
   :scheme :http
   :params (or params {})
   :headers {"host" "localhost"}
   :cookies {}})

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

(defn get-cookie-value [response cookie-name]
  ;; Ring's wrap-cookies middleware converts :cookies to Set-Cookie headers
  ;; We need to parse the Set-Cookie header to extract the cookie value
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
  (let [client-euuid (id/data :test/oauth-authcode-client)
        user-euuid (id/data :test/oauth-authcode-user)]

    ;; Setup client with hashed secret
    (swap! core/*clients* assoc test-client-id
           {:id test-client-id
            :euuid client-euuid
            :secret (hashers/derive test-client-secret)
            :type :confidential
            :settings {"allowed-grants" ["authorization_code" "refresh_token"]
                       "redirections" [test-redirect-uri]  ; ← Fixed: was "redirect-uris"
                       "logout-redirections" [test-redirect-uri]}})

    ;; Setup user (password must be hashed for validate-password to work)
    (swap! core/*resource-owners* assoc test-username
           {:name test-username
            :euuid user-euuid
            :password (hashers/derive test-password)  ; Hash the password
            :active true  ; REQUIRED: validate-resource-owner checks this
            :person_info {:email "authcode@example.com"
                          :given_name "Auth"
                          :family_name "Code"}})

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
;; Step 1: Authorization Request
;; =============================================================================

(deftest test-step-1-authorization-request
  (testing "Step 1: Client requests authorization"
    (println "\n=== STEP 1: Authorization Request ===")

    (let [request (request :get "/oauth/authorize"
                           {:client_id test-client-id
                            :redirect_uri test-redirect-uri
                            :response_type "code"
                            :scope "openid profile offline_access"
                            :state "client-state-123"})

          _ (println "Request:" (select-keys request [:params :uri]))

          response (handlers/authorize request)

          _ (println "Response status:" (:status response))
          _ (println "Response headers:" (:headers response))]

      (println "\n--- Results ---")
      (println "Status:" (:status response))

      (if (= 302 (:status response))
        (let [location (get-in response [:headers "Location"])]
          (println "Redirect to:" location)

          ;; Should redirect to login
          (if (str/includes? location "/oauth/login")
            (do
              (println "✅ PASS: Redirects to login page")
              (is (= 302 (:status response)))
              (is (str/includes? location "/oauth/login")))
            (do
              (println "❌ FAIL: Redirects to error page")
              (println "Location:" location)
              (is false "Should redirect to /oauth/login"))))
        (do
          (println "❌ FAIL: Unexpected status")
          (is (= 302 (:status response))))))))

;; =============================================================================
;; Step 2: Login Page
;; =============================================================================

(deftest test-step-2-login-page-get
  (testing "Step 2: User visits login page"
    (println "\n=== STEP 2: Login Page GET ===")

    ;; First get authorization redirect
    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri test-redirect-uri
                                   :response_type "code"
                                   :scope "openid"
                                   :state "test-state"}))

          location (get-in auth-response [:headers "Location"])
          _ (println "Auth redirects to:" location)]

      (if (str/includes? location "/oauth/login")
        (let [login-params (parse-location-params location)
              encrypted-state (:state login-params)

              _ (println "Login page params:" (keys login-params))
              _ (println "Has encrypted state:" (some? encrypted-state))

              ;; Request login page
              login-get-request (request :get "/oauth/login"
                                         {:state encrypted-state})
              login-response (handlers/login login-get-request)

              _ (println "Login GET status:" (:status login-response))]

          (println "\n--- Results ---")
          (if (= 200 (:status login-response))
            (do
              (println "✅ PASS: Login page returns 200")
              (is (= 200 (:status login-response)))
              (is (= "text/html" (get-in login-response [:headers "Content-Type"]))))
            (do
              (println "❌ FAIL: Login page status:" (:status login-response))
              (is (= 200 (:status login-response))))))
        (println "⚠️ SKIP: Authorization didn't redirect to login")))))

;; =============================================================================
;; Step 3: User Login POST
;; =============================================================================

(deftest test-step-3-user-login-post
  (testing "Step 3: User submits credentials"
    (println "\n=== STEP 3: Login POST ===")

    ;; First get authorization redirect
    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri test-redirect-uri
                                   :response_type "code"
                                   :scope "openid profile offline_access"
                                   :state "test-state-123"}))

          location (get-in auth-response [:headers "Location"])
          _ (println "Auth redirects to:" location)]

      (if (str/includes? location "/oauth/login")
        (let [login-params (parse-location-params location)
              encrypted-state (:state login-params)

              _ (println "Encrypted state received:" (some? encrypted-state))

              ;; Decrypt and inspect state
              _ (when encrypted-state
                  (try
                    (let [decrypted (core/decrypt encrypted-state)]
                      (println "Decrypted state:" decrypted))
                    (catch Exception e
                      (println "⚠️ Could not decrypt state:" (.getMessage e)))))

              ;; POST credentials
              login-post-request (-> (request :post "/oauth/login")
                                     (assoc :params {:username test-username
                                                     :password test-password
                                                     :state encrypted-state})
                                     (assoc :form-params {:username test-username
                                                          :password test-password
                                                          :state encrypted-state}))

              _ (println "POSTing credentials for user:" test-username)

              ;; DEBUG: Check what's in authorization codes
              _ (let [decrypted (core/decrypt encrypted-state)
                      code (:authorization-code decrypted)
                      stored-code (get @ac/*authorization-codes* code)]
                  (println "Authorization code from state:" code)
                  (println "Stored authorization code data:" stored-code)
                  (println "Request in stored code:" (:request stored-code))
                  (println "response_type:" (get-in stored-code [:request :response_type]))
                  (println "response_type type:" (type (get-in stored-code [:request :response_type]))))

              login-response (handlers/login login-post-request)

              _ (println "Login POST status:" (:status login-response))
              _ (println "Login POST location:" (get-in login-response [:headers "Location"]))
              _ (println "Login POST cookies in response:" (:cookies login-response))
              _ (println "Login POST Set-Cookie header:" (get-in login-response [:headers "Set-Cookie"]))
              _ (println "Login POST all headers:" (:headers login-response))
              _ (println "Login POST response keys:" (keys login-response))]

          (println "\n--- Results ---")
          (if (= 302 (:status login-response))
            (let [callback-location (get-in login-response [:headers "Location"])
                  session-cookie (get-cookie-value login-response "idsrv.session")]

              (println "Redirects to:" callback-location)
              (println "Session cookie:" (some? session-cookie))

              (if (str/includes? callback-location test-redirect-uri)
                (do
                  (println "✅ PASS: Redirects to client callback")
                  (is (= 302 (:status login-response)))
                  (is (str/includes? callback-location test-redirect-uri))
                  (is (some? session-cookie) "Should set session cookie"))
                (do
                  (println "❌ FAIL: Redirects to:" callback-location)
                  (is false "Should redirect to client callback"))))
            (do
              (println "❌ FAIL: Expected 302 redirect, got:" (:status login-response))
              (is (= 302 (:status login-response))))))
        (println "⚠️ SKIP: Authorization didn't redirect to login")))))

;; =============================================================================
;; Step 4: Authorization Code Issued
;; =============================================================================

(deftest test-step-4-authorization-code-issued
  (testing "Step 4: Authorization code is issued to client"
    (println "\n=== STEP 4: Authorization Code Issued ===")

    ;; Complete flow up to code issuance
    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri test-redirect-uri
                                   :response_type "code"
                                   :scope "openid"
                                   :state "test-state"}))

          login-params (parse-location-params (get-in auth-response [:headers "Location"]))
          encrypted-state (:state login-params)

          login-response (handlers/login
                          (-> (request :post "/oauth/login")
                              (assoc :params {:username test-username
                                              :password test-password
                                              :state encrypted-state})
                              (assoc :form-params {:username test-username
                                                   :password test-password
                                                   :state encrypted-state})))

          callback-location (get-in login-response [:headers "Location"])
          _ (println "Callback location:" callback-location)]

      (if (and callback-location (str/includes? callback-location test-redirect-uri))
        (let [callback-params (parse-location-params callback-location)
              auth-code (:code callback-params)
              returned-state (:state callback-params)]

          (println "\n--- Results ---")
          (println "Authorization code:" auth-code)
          (println "State matches:" (= "test-state" returned-state))

          (if (some? auth-code)
            (do
              (println "✅ PASS: Authorization code issued")
              (is (some? auth-code) "Should have authorization code")
              (is (= "test-state" returned-state) "State should match"))
            (do
              (println "❌ FAIL: No authorization code in callback")
              (println "Callback params:" callback-params)
              (is false "Should have authorization code"))))
        (println "⚠️ SKIP: Login didn't redirect to callback")))))

;; =============================================================================
;; Step 5: Token Exchange
;; =============================================================================

(deftest test-step-5-token-exchange
  (testing "Step 5: Client exchanges code for tokens"
    (println "\n=== STEP 5: Token Exchange ===")

    ;; Complete flow to get authorization code
    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri test-redirect-uri
                                   :response_type "code"
                                   :scope "openid profile email offline_access"
                                   :state "test-state"}))

          login-params (parse-location-params (get-in auth-response [:headers "Location"]))
          login-response (handlers/login
                          (-> (request :post "/oauth/login")
                              (assoc :params {:username test-username
                                              :password test-password
                                              :state (:state login-params)})
                              (assoc :form-params {:username test-username
                                                   :password test-password
                                                   :state (:state login-params)})))

          callback-params (parse-location-params (get-in login-response [:headers "Location"]))
          auth-code (:code callback-params)
          session-cookie (get-cookie-value login-response "idsrv.session")

          _ (println "Got auth code:" auth-code)
          _ (println "Got session cookie:" (some? session-cookie))]

      (if (some? auth-code)
        (let [;; Exchange code for tokens
              token-request (-> (request :post "/oauth/token")
                                (assoc :params {:grant_type "authorization_code"
                                                :code auth-code
                                                :client_id test-client-id
                                                :client_secret test-client-secret
                                                :redirect_uri test-redirect-uri})
                                (assoc :cookies (when session-cookie
                                                  {"idsrv.session" {:value session-cookie}})))

              _ (println "Token request params:" (:params token-request))

              token-response (handlers/token token-request)

              _ (println "Token response status:" (:status token-response))
              _ (println "Token response body:" (:body token-response))

              token-data (parse-json-body token-response)]

          (println "\n--- Results ---")
          (println "Status:" (:status token-response))
          (println "Has access_token:" (some? (:access_token token-data)))
          (println "Has refresh_token:" (some? (:refresh_token token-data)))
          (println "Has id_token:" (some? (:id_token token-data)))
          (println "Token type:" (:token_type token-data))

          (if (= 200 (:status token-response))
            (do
              (println "✅ PASS: Tokens issued successfully")
              (is (= 200 (:status token-response)))
              (is (some? (:access_token token-data)))
              (is (some? (:refresh_token token-data)))
              (is (some? (:id_token token-data)))
              (is (= "Bearer" (:token_type token-data))))
            (do
              (println "❌ FAIL: Token exchange failed")
              (println "Response body:" (:body token-response))
              (is (= 200 (:status token-response))))))
        (println "⚠️ SKIP: No authorization code to exchange")))))

;; =============================================================================
;; Complete Flow Test
;; =============================================================================

(deftest test-complete-authorization-code-flow
  (testing "Complete Authorization Code Flow (end-to-end)"
    (println "\n=== COMPLETE AUTHORIZATION CODE FLOW ===")
    (println "Testing: authorize → login → code → token\n")

    ;; Step 1: Authorization
    (println "→ Step 1: Client requests authorization")
    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri test-redirect-uri
                                   :response_type "code"
                                   :scope "openid profile email offline_access"
                                   :state "e2e-state"}))

          auth-status (:status auth-response)
          _ (println "  Status:" auth-status)]

      (is (= 302 auth-status) "Should redirect")

      (when (= 302 auth-status)
        ;; Step 2: Login
        (println "→ Step 2: User logs in")
        (let [login-params (parse-location-params (get-in auth-response [:headers "Location"]))
              login-response (handlers/login
                              (-> (request :post "/oauth/login")
                                  (assoc :params {:username test-username
                                                  :password test-password
                                                  :state (:state login-params)})
                                  (assoc :form-params {:username test-username
                                                       :password test-password
                                                       :state (:state login-params)})))
              login-status (:status login-response)
              _ (println "  Status:" login-status)]

          (is (= 302 login-status) "Should redirect after login")

          (when (= 302 login-status)
            ;; Step 3: Code issued
            (println "→ Step 3: Authorization code issued")
            (let [callback-params (parse-location-params (get-in login-response [:headers "Location"]))
                  auth-code (:code callback-params)
                  session-cookie (get-cookie-value login-response "idsrv.session")
                  _ (println "  Code:" auth-code)
                  _ (println "  Session:" (some? session-cookie))]

              (is (some? auth-code) "Should have authorization code")
              (is (some? session-cookie) "Should have session cookie")

              (when auth-code
                ;; Step 4: Token exchange
                (println "→ Step 4: Exchange code for tokens")
                (let [token-response (handlers/token
                                      (-> (request :post "/oauth/token")
                                          (assoc :params {:grant_type "authorization_code"
                                                          :code auth-code
                                                          :client_id test-client-id
                                                          :client_secret test-client-secret
                                                          :redirect_uri test-redirect-uri})
                                          (assoc :cookies (when session-cookie
                                                            {"idsrv.session" {:value session-cookie}}))))
                      token-data (parse-json-body token-response)
                      _ (println "  Status:" (:status token-response))
                      _ (println "  Token type:" (:token_type token-data))]

                  (is (= 200 (:status token-response)) "Should return 200 OK")
                  (is (= "Bearer" (:token_type token-data)) "Should be Bearer token")
                  (is (some? (:access_token token-data)) "Should have access token")
                  (is (some? (:refresh_token token-data)) "Should have refresh token")
                  (is (some? (:id_token token-data)) "Should have ID token")

                  (if (and (= 200 (:status token-response))
                           (some? (:access_token token-data)))
                    (println "\n✅ SUCCESS: Complete flow works!")
                    (println "\n❌ FAILURE: Token exchange failed")))))))))))

;; =============================================================================
;; Test: Authorization Code Reuse Prevention
;; =============================================================================

(deftest test-authorization-code-reuse
  (testing "Authorization code can only be used once"
    (println "\n=== AUTHORIZATION CODE REUSE TEST ===")

    ;; Step 1: Get authorization code
    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri test-redirect-uri
                                   :response_type "code"
                                   :scope "openid profile"
                                   :state "reuse-test-state"}))

          _ (println "  Authorization status:" (:status auth-response))

          login-params (parse-location-params (get-in auth-response [:headers "Location"]))
          login-response (handlers/login
                          (-> (request :post "/oauth/login")
                              (assoc :params {:username test-username
                                              :password test-password
                                              :state (:state login-params)})
                              (assoc :form-params {:username test-username
                                                   :password test-password
                                                   :state (:state login-params)})))

          callback-params (parse-location-params (get-in login-response [:headers "Location"]))
          auth-code (:code callback-params)

          _ (println "  Authorization code:" auth-code)

          ;; Step 2: Use code FIRST time (should work)
          first-token-response (handlers/token
                                (request :post "/oauth/token"
                                         {:grant_type "authorization_code"
                                          :code auth-code
                                          :client_id test-client-id
                                          :client_secret test-client-secret
                                          :redirect_uri test-redirect-uri}))

          _ (println "  First token exchange status:" (:status first-token-response))

          first-tokens (parse-json-body first-token-response)

          ;; Step 3: Try to use code SECOND time (should fail)
          _ (println "\n  → Attempting to reuse authorization code...")

          second-token-response (handlers/token
                                 (request :post "/oauth/token"
                                          {:grant_type "authorization_code"
                                           :code auth-code
                                           :client_id test-client-id
                                           :client_secret test-client-secret
                                           :redirect_uri test-redirect-uri}))

          _ (println "  Second token exchange status:" (:status second-token-response))

          second-response-body (parse-json-body second-token-response)]

      (println "\n--- Results ---")
      (println "First use status:" (:status first-token-response))
      (println "First use got tokens:" (some? (:access_token first-tokens)))
      (println "Second use status:" (:status second-token-response))
      (println "Second use error:" (:error second-response-body))

      ;; Assertions
      (is (= 200 (:status first-token-response)) "First use should succeed")
      (is (some? (:access_token first-tokens)) "First use should return access_token")

      (is (= 400 (:status second-token-response)) "Second use should fail with 400")
      (is (some? (:error second-response-body)) "Second use should return error")

      (if (= 400 (:status second-token-response))
        (println "✅ PASS: Authorization code reuse blocked")
        (println "❌ FAIL: Authorization code was reused!")))))
