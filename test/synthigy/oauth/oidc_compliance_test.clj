(ns synthigy.oauth.oidc-compliance-test
  "OIDC Compliance tests for OAuth 2.0/2.1 and OpenID Connect features.

  Tests:
  1. prompt parameter (none, login, consent, select_account)
  2. max_age parameter enforcement
  3. Refresh token rotation
  4. Bearer token extraction (header, form, query per RFC 6750)
  5. iss parameter in authorization response (RFC 9207)
  6. response_mode parameter (query, fragment, form_post)"
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.string :as str]
   [synthigy.json :as json]
   [buddy.hashers :as hashers]
   [patcho.lifecycle :as lifecycle]
   [synthigy.oauth.handlers :as handlers]
   [synthigy.oauth.core :as core]
   [synthigy.oauth :as oauth]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.token :as token-ns]
   [synthigy.oidc :as oidc]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]
   [synthigy.dataset.access :as dataset.access]
   [synthigy.dataset.access.protocol :as access.protocol]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-client-id "test-client-oidc-compliance")
(def test-client-secret "test-secret-oidc-compliance")
(def test-redirect-uri "https://example.com/callback")
(def test-username "oidcuser")
(def test-password "test-password-oidc-123")

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
    (json/read-str body)))

(defn parse-location-params [location]
  (when location
    (let [[_ query] (str/split location #"\?")]
      (when query
        (into {}
              (map (fn [param]
                     (let [[k v] (str/split param #"=" 2)]
                       [(keyword k) (when (not-empty v) (ring.util.codec/url-decode v))]))
                   (str/split query #"&")))))))

(defn parse-fragment-params [location]
  (when location
    (let [[_ fragment] (str/split location #"#")]
      (when fragment
        (into {}
              (map (fn [param]
                     (let [[k v] (str/split param #"=" 2)]
                       [(keyword k) (when (not-empty v) (ring.util.codec/url-decode v))]))
                   (str/split fragment #"&")))))))

(defn get-cookie-value [response cookie-name]
  (when-let [set-cookie-headers (get-in response [:headers "Set-Cookie"])]
    (let [cookies (if (sequential? set-cookie-headers)
                    set-cookie-headers
                    [set-cookie-headers])]
      (some (fn [cookie-str]
              (when (string? cookie-str)
                (let [[name-value & _] (str/split cookie-str #";")
                      [name value] (str/split name-value #"=")]
                  (when (= (str/trim name) cookie-name)
                    (str/trim value)))))
            cookies))))

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
  (let [client-euuid (java.util.UUID/randomUUID)
        user-euuid (java.util.UUID/randomUUID)]

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
            :person_info {:email "oidc@example.com"
                          :given_name "OIDC"
                          :family_name "Compliance"}})

    {:client-euuid client-euuid
     :user-euuid user-euuid}))

(defn setup-fixtures [f]
  (reset-state!)

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

;; Simple fixture that only initializes encryption without database
(defn encryption-fixture [f]
  ;; Initialize encryption provider manually without lifecycle
  (when-not encryption/*encryption-provider*
    (alter-var-root #'encryption/*encryption-provider*
                    (constantly (encryption/->RSAEncryptionProvider))))
  ;; Generate a keypair if none exists
  (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
    (encryption/rotate-keypair encryption/*encryption-provider*))
  (f))

(use-fixtures :once encryption-fixture)
(use-fixtures :each setup-fixtures)

;; =============================================================================
;; Helper: Complete authorization flow
;; =============================================================================

(defn complete-authorization-flow
  "Complete the full authorization code flow and return tokens.
   Options:
   - :scope - OAuth scopes to request
   - :extra-auth-params - Additional params for authorization request"
  ([] (complete-authorization-flow {}))
  ([{:keys [scope extra-auth-params]
     :or {scope "openid profile offline_access"}}]
   (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  (merge
                                    {:client_id test-client-id
                                     :redirect_uri test-redirect-uri
                                     :response_type "code"
                                     :scope scope
                                     :state "test-state"}
                                    extra-auth-params)))

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

         token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code auth-code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri test-redirect-uri}))]

     {:auth-response auth-response
      :login-response login-response
      :token-response token-response
      :tokens (parse-json-body token-response)
      :auth-code auth-code
      :session-cookie session-cookie
      :callback-params callback-params})))

;; =============================================================================
;; Test 1: prompt=none without session (login_required)
;; =============================================================================

(deftest test-prompt-none-without-session
  (testing "prompt=none without existing session returns login_required"
    (println "\n=== PROMPT=NONE WITHOUT SESSION ===")

    (let [auth-response (handlers/authorize
                          (request :get "/oauth/authorize"
                                   {:client_id test-client-id
                                    :redirect_uri test-redirect-uri
                                    :response_type "code"
                                    :scope "openid"
                                    :state "prompt-none-test"
                                    :prompt "none"}))

          location (get-in auth-response [:headers "Location"])
          _ (println "  Response status:" (:status auth-response))
          _ (println "  Location:" location)]

      (println "\n--- Results ---")
      ;; Should redirect back to client with error
      (is (= 302 (:status auth-response)) "Should redirect")

      (when location
        (let [params (parse-location-params location)]
          (println "  Error:" (:error params))
          (println "  Error description:" (:error_description params))

          (is (= "login_required" (:error params))
              "Should return login_required error")
          (is (= "prompt-none-test" (:state params))
              "Should preserve state")

          (if (= "login_required" (:error params))
            (println "✅ PASS: prompt=none without session returns login_required")
            (println "❌ FAIL: Expected login_required error")))))))

;; =============================================================================
;; Test 2: prompt=login forces re-authentication
;; =============================================================================

(deftest test-prompt-login-forces-reauth
  (testing "prompt=login forces re-authentication even with existing session"
    (println "\n=== PROMPT=LOGIN FORCES RE-AUTH ===")

    ;; First, establish a session
    (let [first-flow (complete-authorization-flow)
          session-cookie (:session-cookie first-flow)
          _ (println "  First flow session:" (some? session-cookie))]

      ;; Now request with prompt=login - should clear session and redirect to login
      (let [auth-response (handlers/authorize
                            (-> (request :get "/oauth/authorize"
                                         {:client_id test-client-id
                                          :redirect_uri test-redirect-uri
                                          :response_type "code"
                                          :scope "openid"
                                          :state "prompt-login-test"
                                          :prompt "login"})
                                (assoc-in [:cookies "idsrv.session"] {:value session-cookie})))

            location (get-in auth-response [:headers "Location"])
            _ (println "  Auth response status:" (:status auth-response))
            _ (println "  Location:" location)

            ;; Check if session cookie is cleared
            set-cookie (get-in auth-response [:headers "Set-Cookie"])
            _ (println "  Set-Cookie header:" set-cookie)]

        (println "\n--- Results ---")
        (is (= 302 (:status auth-response)) "Should redirect")

        (when location
          (is (str/includes? location "/oauth/login")
              "Should redirect to login page (not directly to callback)")

          (if (str/includes? location "/oauth/login")
            (println "✅ PASS: prompt=login redirects to login page")
            (println "❌ FAIL: prompt=login should redirect to login")))))))

;; =============================================================================
;; Test 3: prompt=consent redirects to consent flow
;; =============================================================================

(deftest test-prompt-consent
  (testing "prompt=consent triggers consent flow"
    (println "\n=== PROMPT=CONSENT ===")

    (let [auth-response (handlers/authorize
                          (request :get "/oauth/authorize"
                                   {:client_id test-client-id
                                    :redirect_uri test-redirect-uri
                                    :response_type "code"
                                    :scope "openid profile"
                                    :state "prompt-consent-test"
                                    :prompt "consent"}))

          location (get-in auth-response [:headers "Location"])
          _ (println "  Response status:" (:status auth-response))
          _ (println "  Location:" location)]

      (println "\n--- Results ---")
      (is (= 302 (:status auth-response)) "Should redirect")

      (when location
        ;; Should redirect to login which will show consent
        (is (str/includes? location "/oauth/login")
            "Should redirect to login/consent flow")

        (if (str/includes? location "/oauth/login")
          (println "✅ PASS: prompt=consent triggers consent flow")
          (println "❌ FAIL: prompt=consent should trigger consent"))))))

;; =============================================================================
;; Test 4: prompt=select_account triggers account selection
;; =============================================================================

(deftest test-prompt-select-account
  (testing "prompt=select_account triggers account selection"
    (println "\n=== PROMPT=SELECT_ACCOUNT ===")

    (let [auth-response (handlers/authorize
                          (request :get "/oauth/authorize"
                                   {:client_id test-client-id
                                    :redirect_uri test-redirect-uri
                                    :response_type "code"
                                    :scope "openid"
                                    :state "prompt-select-account-test"
                                    :prompt "select_account"}))

          location (get-in auth-response [:headers "Location"])
          _ (println "  Response status:" (:status auth-response))
          _ (println "  Location:" location)]

      (println "\n--- Results ---")
      (is (= 302 (:status auth-response)) "Should redirect")

      (when location
        ;; Should redirect to login for account selection
        (is (str/includes? location "/oauth/login")
            "Should redirect to login for account selection")

        (if (str/includes? location "/oauth/login")
          (println "✅ PASS: prompt=select_account triggers account selection")
          (println "❌ FAIL: prompt=select_account should trigger account selection"))))))

;; =============================================================================
;; Test 5: max_age with stale session requires re-authentication
;; =============================================================================

(deftest test-max-age-stale-session
  (testing "max_age with stale session returns login_required for prompt=none"
    (println "\n=== MAX_AGE WITH STALE SESSION ===")

    ;; First, establish a session
    (let [first-flow (complete-authorization-flow)
          session-cookie (:session-cookie first-flow)
          session-id session-cookie
          _ (println "  First flow session:" (some? session-cookie))]

      ;; Manually set authorized-at to be old (more than 1 second ago)
      (when session-id
        (core/set-session-authorized-at session-id
                                         (java.util.Date. (- (System/currentTimeMillis) 60000))))

      ;; Now request with max_age=1 and prompt=none - session is 60s old
      (let [auth-response (handlers/authorize
                            (-> (request :get "/oauth/authorize"
                                         {:client_id test-client-id
                                          :redirect_uri test-redirect-uri
                                          :response_type "code"
                                          :scope "openid"
                                          :state "max-age-test"
                                          :max_age "1"  ; Only 1 second
                                          :prompt "none"})
                                (assoc-in [:cookies "idsrv.session"] {:value session-cookie})))

            location (get-in auth-response [:headers "Location"])
            _ (println "  Auth response status:" (:status auth-response))
            _ (println "  Location:" location)]

        (println "\n--- Results ---")
        (is (= 302 (:status auth-response)) "Should redirect")

        (when location
          (let [params (parse-location-params location)]
            (println "  Error:" (:error params))

            (is (= "login_required" (:error params))
                "Should return login_required when session is older than max_age")

            (if (= "login_required" (:error params))
              (println "✅ PASS: max_age enforcement works")
              (println "❌ FAIL: max_age should trigger login_required"))))))))

;; =============================================================================
;; Test 6: max_age with fresh session allows silent auth
;; =============================================================================

(deftest test-max-age-fresh-session
  (testing "max_age with fresh session allows silent authentication"
    (println "\n=== MAX_AGE WITH FRESH SESSION ===")

    ;; First, establish a fresh session
    (let [first-flow (complete-authorization-flow)
          session-cookie (:session-cookie first-flow)
          _ (println "  First flow session:" (some? session-cookie))]

      ;; Request with max_age=3600 and prompt=none - session is fresh
      (let [auth-response (handlers/authorize
                            (-> (request :get "/oauth/authorize"
                                         {:client_id test-client-id
                                          :redirect_uri test-redirect-uri
                                          :response_type "code"
                                          :scope "openid"
                                          :state "max-age-fresh-test"
                                          :max_age "3600"  ; 1 hour - session is fresh
                                          :prompt "none"})
                                (assoc-in [:cookies "idsrv.session"] {:value session-cookie})))

            location (get-in auth-response [:headers "Location"])
            _ (println "  Auth response status:" (:status auth-response))
            _ (println "  Location:" location)]

        (println "\n--- Results ---")
        (is (= 302 (:status auth-response)) "Should redirect")

        (when location
          (let [params (parse-location-params location)]
            (println "  Code:" (some? (:code params)))
            (println "  Error:" (:error params))

            ;; Should get a code, not an error
            (is (some? (:code params))
                "Should return authorization code for fresh session")
            (is (nil? (:error params))
                "Should not return error for fresh session")

            (if (some? (:code params))
              (println "✅ PASS: max_age with fresh session allows silent auth")
              (println "❌ FAIL: Fresh session should allow silent auth"))))))))

;; =============================================================================
;; Test 7: Refresh token rotation
;; =============================================================================

(deftest test-refresh-token-rotation
  (testing "Refresh token rotation issues new refresh token and revokes old one"
    (println "\n=== REFRESH TOKEN ROTATION ===")

    ;; Get initial tokens
    (let [{:keys [tokens]} (complete-authorization-flow)
          initial-refresh-token (:refresh_token tokens)
          initial-access-token (:access_token tokens)
          _ (println "  Initial refresh_token:" (some? initial-refresh-token))
          _ (println "  Initial access_token:" (some? initial-access-token))]

      (is (some? initial-refresh-token) "Should have initial refresh token")

      (when initial-refresh-token
        ;; Use refresh token
        (let [refresh-response (handlers/token
                                 (request :post "/oauth/token"
                                          {:grant_type "refresh_token"
                                           :refresh_token initial-refresh-token
                                           :client_id test-client-id
                                           :client_secret test-client-secret}))

              refreshed-tokens (parse-json-body refresh-response)
              new-refresh-token (:refresh_token refreshed-tokens)
              new-access-token (:access_token refreshed-tokens)
              _ (println "  Refresh response status:" (:status refresh-response))
              _ (println "  New refresh_token:" (some? new-refresh-token))
              _ (println "  New access_token:" (some? new-access-token))]

          (println "\n--- Results ---")
          (is (= 200 (:status refresh-response)) "Refresh should succeed")
          (is (some? new-refresh-token) "Should return new refresh token")
          (is (some? new-access-token) "Should return new access token")
          (is (not= initial-access-token new-access-token)
              "Access token should be different")

          ;; Per OAuth 2.1, refresh token should be rotated (new one issued)
          ;; This test verifies that a new refresh token is issued
          (when new-refresh-token
            (println "  Refresh token changed:" (not= initial-refresh-token new-refresh-token))

            ;; Try to use OLD refresh token - should fail
            (let [reuse-response (handlers/token
                                   (request :post "/oauth/token"
                                            {:grant_type "refresh_token"
                                             :refresh_token initial-refresh-token
                                             :client_id test-client-id
                                             :client_secret test-client-secret}))
                  reuse-error (parse-json-body reuse-response)
                  _ (println "  Old refresh token reuse status:" (:status reuse-response))
                  _ (println "  Old refresh token reuse error:" (:error reuse-error))]

              (is (= 400 (:status reuse-response))
                  "Old refresh token should be revoked")

              (if (= 400 (:status reuse-response))
                (println "✅ PASS: Old refresh token is revoked after rotation")
                (println "❌ FAIL: Old refresh token should be revoked")))))))))

;; =============================================================================
;; Test 8: Bearer token in Authorization header (RFC 6750)
;; =============================================================================

(deftest test-bearer-token-authorization-header
  (testing "Bearer token extraction from Authorization header"
    (println "\n=== BEARER TOKEN IN AUTHORIZATION HEADER ===")

    (let [{:keys [tokens]} (complete-authorization-flow)
          access-token (:access_token tokens)
          _ (println "  Access token:" (some? access-token))]

      (is (some? access-token) "Should have access token")

      (when access-token
        (let [userinfo-response (handlers/userinfo
                                  {:request-method :get
                                   :uri "/oauth/userinfo"
                                   :scheme :https
                                   :headers {"host" "localhost"
                                             "authorization" (str "Bearer " access-token)}
                                   :params {}
                                   :cookies {}})

              userinfo (parse-json-body userinfo-response)
              _ (println "  UserInfo status:" (:status userinfo-response))
              _ (println "  UserInfo sub:" (:sub userinfo))]

          (println "\n--- Results ---")
          (is (= 200 (:status userinfo-response))
              "Should accept Bearer token in Authorization header")
          (is (= test-username (:sub userinfo))
              "Should return correct user")

          (if (= 200 (:status userinfo-response))
            (println "✅ PASS: Bearer token in Authorization header works")
            (println "❌ FAIL: Bearer token in Authorization header failed")))))))

;; =============================================================================
;; Test 9: Bearer token in form body (RFC 6750)
;; =============================================================================

(deftest test-bearer-token-form-body
  (testing "Bearer token extraction from form body (access_token parameter)"
    (println "\n=== BEARER TOKEN IN FORM BODY ===")

    (let [{:keys [tokens]} (complete-authorization-flow)
          access-token (:access_token tokens)
          _ (println "  Access token:" (some? access-token))]

      (is (some? access-token) "Should have access token")

      (when access-token
        ;; Test get-access-token function directly
        (let [test-request {:headers {}
                            :form-params {:access_token access-token}
                            :params {}}
              extracted-token (oidc/get-access-token test-request)
              _ (println "  Extracted token from form body:" (some? extracted-token))]

          (println "\n--- Results ---")
          (is (= access-token extracted-token)
              "Should extract token from form body")

          (if (= access-token extracted-token)
            (println "✅ PASS: Bearer token extraction from form body works")
            (println "❌ FAIL: Bearer token extraction from form body failed")))))))

;; =============================================================================
;; Test 10: Bearer token in query parameter (RFC 6750)
;; =============================================================================

(deftest test-bearer-token-query-param
  (testing "Bearer token extraction from query parameter"
    (println "\n=== BEARER TOKEN IN QUERY PARAMETER ===")

    (let [{:keys [tokens]} (complete-authorization-flow)
          access-token (:access_token tokens)
          _ (println "  Access token:" (some? access-token))]

      (is (some? access-token) "Should have access token")

      (when access-token
        ;; Test get-access-token function directly
        (let [test-request {:headers {}
                            :form-params {}
                            :params {:access_token access-token}}
              extracted-token (oidc/get-access-token test-request)
              _ (println "  Extracted token from query:" (some? extracted-token))]

          (println "\n--- Results ---")
          (is (= access-token extracted-token)
              "Should extract token from query parameter")

          (if (= access-token extracted-token)
            (println "✅ PASS: Bearer token extraction from query parameter works")
            (println "❌ FAIL: Bearer token extraction from query parameter failed")))))))

;; =============================================================================
;; Test 11: RFC 9207 - iss parameter in authorization response
;; =============================================================================

(deftest test-iss-parameter-in-authorization-response
  (testing "Authorization response includes 'iss' parameter per RFC 9207"
    (println "\n=== RFC 9207 ISS PARAMETER ===")

    (let [{:keys [callback-params]} (complete-authorization-flow)
          iss (:iss callback-params)
          code (:code callback-params)
          _ (println "  Authorization code:" (some? code))
          _ (println "  Issuer (iss):" iss)]

      (println "\n--- Results ---")
      (is (some? code) "Should have authorization code")
      (is (some? iss) "Should have 'iss' parameter in authorization response")

      (when iss
        (is (str/starts-with? iss "http")
            "Issuer should be a URL"))

      (if (some? iss)
        (println "✅ PASS: Authorization response includes 'iss' parameter")
        (println "❌ FAIL: Authorization response missing 'iss' parameter")))))

;; =============================================================================
;; Test 12: response_mode=query (default)
;; =============================================================================

(deftest test-response-mode-query
  (testing "response_mode=query returns parameters in query string"
    (println "\n=== RESPONSE_MODE=QUERY ===")

    (let [auth-response (handlers/authorize
                          (request :get "/oauth/authorize"
                                   {:client_id test-client-id
                                    :redirect_uri test-redirect-uri
                                    :response_type "code"
                                    :scope "openid"
                                    :state "response-mode-query-test"
                                    :response_mode "query"}))

          login-params (parse-location-params (get-in auth-response [:headers "Location"]))
          login-response (handlers/login
                           (-> (request :post "/oauth/login")
                               (assoc :params {:username test-username
                                               :password test-password
                                               :state (:state login-params)})
                               (assoc :form-params {:username test-username
                                                    :password test-password
                                                    :state (:state login-params)})))

          location (get-in login-response [:headers "Location"])
          _ (println "  Response status:" (:status login-response))
          _ (println "  Location:" location)]

      (println "\n--- Results ---")
      (is (= 302 (:status login-response)) "Should redirect")

      (when location
        (is (str/includes? location "?")
            "Should have query string")
        (is (not (str/includes? location "#"))
            "Should not have fragment")

        (let [params (parse-location-params location)]
          (is (some? (:code params)) "Should have code in query"))

        (if (str/includes? location "?")
          (println "✅ PASS: response_mode=query returns params in query string")
          (println "❌ FAIL: response_mode=query should use query string"))))))

;; =============================================================================
;; Test 13: response_mode=fragment
;; =============================================================================

(deftest test-response-mode-fragment
  (testing "response_mode=fragment returns parameters in URL fragment"
    (println "\n=== RESPONSE_MODE=FRAGMENT ===")

    (let [auth-response (handlers/authorize
                          (request :get "/oauth/authorize"
                                   {:client_id test-client-id
                                    :redirect_uri test-redirect-uri
                                    :response_type "code"
                                    :scope "openid"
                                    :state "response-mode-fragment-test"
                                    :response_mode "fragment"}))

          login-params (parse-location-params (get-in auth-response [:headers "Location"]))
          login-response (handlers/login
                           (-> (request :post "/oauth/login")
                               (assoc :params {:username test-username
                                               :password test-password
                                               :state (:state login-params)})
                               (assoc :form-params {:username test-username
                                                    :password test-password
                                                    :state (:state login-params)})))

          location (get-in login-response [:headers "Location"])
          _ (println "  Response status:" (:status login-response))
          _ (println "  Location:" location)]

      (println "\n--- Results ---")
      (is (= 302 (:status login-response)) "Should redirect")

      (when location
        (is (str/includes? location "#")
            "Should have fragment")

        (let [params (parse-fragment-params location)]
          (is (some? (:code params)) "Should have code in fragment"))

        (if (str/includes? location "#")
          (println "✅ PASS: response_mode=fragment returns params in fragment")
          (println "❌ FAIL: response_mode=fragment should use fragment"))))))

;; =============================================================================
;; Test 14: response_mode=form_post
;; =============================================================================

(deftest test-response-mode-form-post
  (testing "response_mode=form_post returns auto-submitting HTML form"
    (println "\n=== RESPONSE_MODE=FORM_POST ===")

    (let [auth-response (handlers/authorize
                          (request :get "/oauth/authorize"
                                   {:client_id test-client-id
                                    :redirect_uri test-redirect-uri
                                    :response_type "code"
                                    :scope "openid"
                                    :state "response-mode-form-post-test"
                                    :response_mode "form_post"}))

          login-params (parse-location-params (get-in auth-response [:headers "Location"]))
          login-response (handlers/login
                           (-> (request :post "/oauth/login")
                               (assoc :params {:username test-username
                                               :password test-password
                                               :state (:state login-params)})
                               (assoc :form-params {:username test-username
                                                    :password test-password
                                                    :state (:state login-params)})))

          body (:body login-response)
          content-type (get-in login-response [:headers "Content-Type"])
          _ (println "  Response status:" (:status login-response))
          _ (println "  Content-Type:" content-type)]

      (println "\n--- Results ---")
      (is (= 200 (:status login-response))
          "Should return 200 OK (not redirect)")
      (is (str/includes? (or content-type "") "text/html")
          "Should return HTML content")

      (when body
        (println "  Has form:" (str/includes? body "<form"))
        (println "  Has hidden inputs:" (str/includes? body "type=\"hidden\""))
        (println "  Has onload submit:" (str/includes? body "onload"))

        (is (str/includes? body "<form")
            "Should contain a form element")
        (is (str/includes? body "type=\"hidden\"")
            "Should contain hidden inputs")
        (is (str/includes? body test-redirect-uri)
            "Form action should be redirect_uri")
        (is (str/includes? body "onload")
            "Should auto-submit on load"))

      (if (and (= 200 (:status login-response))
               body
               (str/includes? body "<form"))
        (println "✅ PASS: response_mode=form_post returns auto-submitting form")
        (println "❌ FAIL: response_mode=form_post should return HTML form")))))

;; =============================================================================
;; Test 15: OAuth Authorization Server Metadata (RFC 8414)
;; =============================================================================

(deftest test-oauth-authorization-server-metadata
  (testing "OAuth Authorization Server Metadata endpoint (RFC 8414)"
    (println "\n=== RFC 8414 OAUTH METADATA ===")

    (let [response (handlers/oauth-authorization-server
                     {:request-method :get
                      :uri "/.well-known/oauth-authorization-server"
                      :scheme :https
                      :headers {"host" "localhost"}
                      :params {}})

          metadata (parse-json-body response)
          _ (println "  Response status:" (:status response))
          _ (println "  Content-Type:" (get-in response [:headers "Content-Type"]))]

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")

      (when metadata
        (println "  Has issuer:" (some? (:issuer metadata)))
        (println "  Has token_endpoint:" (some? (:token_endpoint metadata)))
        (println "  Has authorization_endpoint:" (some? (:authorization_endpoint metadata)))
        (println "  Has code_challenge_methods_supported:" (some? (:code_challenge_methods_supported metadata)))

        (is (some? (:issuer metadata)) "Should have issuer")
        (is (some? (:token_endpoint metadata)) "Should have token_endpoint")
        (is (some? (:authorization_endpoint metadata)) "Should have authorization_endpoint")
        (is (some? (:code_challenge_methods_supported metadata))
            "Should have code_challenge_methods_supported")
        (is (some #{"S256"} (:code_challenge_methods_supported metadata))
            "Should support S256 code challenge method"))

      (if (and (= 200 (:status response))
               (some? (:issuer metadata)))
        (println "✅ PASS: OAuth Authorization Server Metadata endpoint works")
        (println "❌ FAIL: OAuth metadata endpoint failed")))))

;; =============================================================================
;; Test 16: OIDC Discovery with code_challenge_methods_supported
;; =============================================================================

(deftest test-oidc-discovery-pkce-methods
  (testing "OIDC Discovery includes code_challenge_methods_supported"
    (println "\n=== OIDC DISCOVERY PKCE METHODS ===")

    (let [response (handlers/openid-configuration
                     {:request-method :get
                      :uri "/.well-known/openid-configuration"
                      :scheme :https
                      :headers {"host" "localhost"}
                      :params {}})

          metadata (parse-json-body response)
          _ (println "  Response status:" (:status response))]

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")

      (when metadata
        (let [pkce-methods (:code_challenge_methods_supported metadata)]
          (println "  code_challenge_methods_supported:" pkce-methods)

          (is (some? pkce-methods)
              "Should have code_challenge_methods_supported")
          (is (some #{"S256"} pkce-methods)
              "Should support S256")
          (is (some #{"plain"} pkce-methods)
              "Should support plain (for compatibility)")))

      (if (some? (:code_challenge_methods_supported metadata))
        (println "✅ PASS: OIDC Discovery includes PKCE methods")
        (println "❌ FAIL: OIDC Discovery missing PKCE methods")))))

;; =============================================================================
;; Test 17: ACR/AMR in ID token
;; =============================================================================

(deftest test-acr-amr-in-id-token
  (testing "ID token includes acr and amr claims"
    (println "\n=== ACR/AMR IN ID TOKEN ===")

    (let [{:keys [tokens]} (complete-authorization-flow)
          id-token (:id_token tokens)
          _ (println "  ID token present:" (some? id-token))]

      (is (some? id-token) "Should have id_token")

      (when id-token
        ;; Decode the JWT (just the payload, we don't need to verify here)
        (let [parts (clojure.string/split id-token #"\.")
              payload-b64 (second parts)
              ;; Add padding if needed
              padded (str payload-b64 (apply str (repeat (mod (- 4 (mod (count payload-b64) 4)) 4) "=")))
              payload-json (String. (.decode (java.util.Base64/getUrlDecoder) padded))
              payload (json/read-str payload-json)
              _ (println "  acr:" (:acr payload))
              _ (println "  amr:" (:amr payload))]

          (println "\n--- Results ---")
          (is (some? (:acr payload)) "ID token should have 'acr' claim")
          (is (some? (:amr payload)) "ID token should have 'amr' claim")
          (is (= "1" (:acr payload)) "acr should be '1' for password auth")
          (is (= ["pwd"] (:amr payload)) "amr should be ['pwd'] for password auth")

          (if (and (= "1" (:acr payload)) (= ["pwd"] (:amr payload)))
            (println "✅ PASS: ID token includes correct acr/amr claims")
            (println "❌ FAIL: ID token missing or incorrect acr/amr claims")))))))

;; =============================================================================
;; Test 18: ACR values in OIDC Discovery
;; =============================================================================

(deftest test-acr-values-in-discovery
  (testing "OIDC Discovery includes acr_values_supported"
    (println "\n=== ACR VALUES IN OIDC DISCOVERY ===")

    (let [response (handlers/openid-configuration
                     {:request-method :get
                      :uri "/.well-known/openid-configuration"
                      :scheme :https
                      :headers {"host" "localhost"}
                      :params {}})

          metadata (parse-json-body response)
          acr-values (:acr_values_supported metadata)
          _ (println "  acr_values_supported:" acr-values)]

      (println "\n--- Results ---")
      (is (= 200 (:status response)) "Should return 200 OK")
      (is (some? acr-values) "Should have acr_values_supported")

      (when acr-values
        (is (some #{"1"} acr-values) "Should support acr level '1'")
        (is (some #{"2"} acr-values) "Should support acr level '2'"))

      (if (some? acr-values)
        (println "✅ PASS: OIDC Discovery includes acr_values_supported")
        (println "❌ FAIL: OIDC Discovery missing acr_values_supported")))))
