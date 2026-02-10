(ns synthigy.oauth.integration-test
  "Integration tests for OAuth/OIDC Ring handlers.

  Tests complete OAuth flows end-to-end:
  - Authorization code flow (with/without PKCE)
  - Refresh token flow
  - Client credentials flow
  - Device code flow
  - OIDC endpoints (UserInfo, JWKS, Discovery)
  - Login/logout with session cookies
  - Token revocation"
  (:require
   [buddy.hashers :as hashers]
   [clojure.data.json :as json]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [synthigy.oauth.handlers :as handlers]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.device-code :as dc]
   [synthigy.oauth.token :as token-ns]
   [synthigy.iam :as iam]
   [synthigy.iam.encryption :as encryption]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.access :as dataset.access]
   [synthigy.dataset.access.protocol :as access.protocol]
   [synthigy.test-data]  ; Load test data registrations
   [synthigy.test-helper :as test-helper]))

;; =============================================================================
;; Mock Access Control (allows all operations for testing)
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
;; Request Building Helpers (replaces ring-mock)
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

;; =============================================================================
;; Test Fixtures and Helpers
;; =============================================================================

(def test-client-id "test-client-123")
(def test-client-secret "test-secret-456")
(def test-redirect-uri "http://localhost:3000/callback")
(def test-username "testuser")
(def test-password "testpass123")

(defn reset-state!
  "Reset all OAuth state atoms for testing"
  []
  (reset! core/*clients* {})
  (reset! core/*resource-owners* {})
  (reset! core/*sessions* {})
  (reset! ac/*authorization-codes* {})
  (reset! dc/*device-codes* {})
  (reset! token-ns/*tokens* {}))

(defn setup-public-client!
  "Create a public OAuth client (no secret required).

  Used for device code flow tests and other public client scenarios.

  Returns the client euuid (needed for session setup)."
  []
  (let [client-euuid (id/data :test/oauth-integ-client-1)]
    (swap! core/*clients* assoc client-euuid
           {:id test-client-id
            :euuid client-euuid
            :secret nil  ;; Public client - no secret
            :type :public
            :settings {"allowed-grants" ["authorization_code"
                                         "refresh_token"
                                         "client_credentials"
                                         "urn:ietf:params:oauth:grant-type:device_code"]
                       "redirections" [test-redirect-uri]
                       "logout-redirections" [test-redirect-uri]}})
    client-euuid))

(defn setup-confidential-client!
  "Create a confidential OAuth client (with secret).

  Used for authorization code flow tests that need to validate client secrets.

  Returns the client euuid (needed for session setup)."
  []
  (let [client-euuid (id/data :test/oauth-integ-client-2)]
    (swap! core/*clients* assoc client-euuid
           {:id test-client-id
            :euuid client-euuid
            :secret (hashers/derive test-client-secret)  ;; Hash secret for validation
            :type :confidential
            :settings {"allowed-grants" ["authorization_code"
                                         "refresh_token"
                                         "client_credentials"
                                         "urn:ietf:params:oauth:grant-type:device_code"]
                       "redirections" [test-redirect-uri]
                       "logout-redirections" [test-redirect-uri]}})
    client-euuid))

;; Alias for backward compatibility - defaults to public client
(def setup-test-client! setup-public-client!)

(defn setup-test-user!
  "Create a test resource owner"
  []
  (swap! core/*resource-owners* assoc test-username
         {:name test-username
          :euuid (id/data :test/oauth-integ-user-1)
          :active true  ;; Required for validation
          :password test-password  ;; Using plain password for testing
          :person_info {:email "test@example.com"
                        :given_name "Test"
                        :family_name "User"}}))

(defn setup-fixtures
  "Setup function for tests"
  [f]
  (reset-state!)
  ;; Note: Encryption is initialized by system-fixture
  ;; Install mock access control that allows all operations
  (let [original-access-control dataset.access/*access-control*
        original-get-client (var-get #'iam/get-client)
        original-get-user-details (var-get #'iam/get-user-details)
        original-validate-password (var-get #'iam/validate-password)
        original-publish (var-get #'iam/publish)]
    ;; Mock IAM functions to return test data
    ;; NOTE: *clients* atom is keyed by euuid, but get-client takes client ID
    (alter-var-root #'iam/get-client
                    (constantly (fn [id]
                                  (some (fn [[_euuid client]]
                                          (when (= (:id client) id) client))
                                        @core/*clients*))))
    (alter-var-root #'iam/get-user-details
                    (constantly (fn [username]
                                  (get @core/*resource-owners* username))))
    ;; Mock password validation - supports both plain and hashed passwords
    (alter-var-root #'iam/validate-password
                    (constantly (fn [user-password stored-password]
                                  (let [result (cond
                                                ;; No stored password - always valid (public client)
                                                 (nil? stored-password) true
                                                ;; No user password provided but secret required - invalid
                                                 (nil? user-password) false
                                                ;; Hashed password - use hashers/check
                                                 (re-matches #"^\$2[aby]\$.*|^bcrypt\+.*" stored-password)
                                                 (hashers/check user-password stored-password)
                                                ;; Plain password - direct comparison
                                                 :else (= user-password stored-password))]
                                    result))))
    ;; Mock iam/publish to be a no-op (avoid async channel errors)
    (alter-var-root #'iam/publish
                    (constantly (fn [_ _] nil)))
    (alter-var-root #'dataset.access/*access-control* (constantly (->TestAccessControl)))
    ;; Don't setup clients here - let individual tests setup the client type they need
    ;; (setup-test-client!)  ;; Removed - causes conflicts when tests setup their own clients
    (setup-test-user!)
    (f)
    (reset-state!)
    ;; Restore original functions
    (alter-var-root #'iam/get-client (constantly original-get-client))
    (alter-var-root #'iam/get-user-details (constantly original-get-user-details))
    (alter-var-root #'iam/validate-password (constantly original-validate-password))
    (alter-var-root #'iam/publish (constantly original-publish))
    (alter-var-root #'dataset.access/*access-control* (constantly original-access-control))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each setup-fixtures)

(defn parse-json-body
  "Parse JSON response body"
  [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

(defn get-cookie-value
  "Extract cookie value from response.

  Note: wrap-cookies middleware converts :cookies to Set-Cookie header,
  so we need to parse the header instead of looking at :cookies key."
  [response cookie-name]
  (let [set-cookie-headers (get-in response [:headers "Set-Cookie"])
        cookie-pattern (re-pattern (str cookie-name "=([^;]+)"))]
    (when set-cookie-headers
      (let [cookies (if (string? set-cookie-headers)
                      [set-cookie-headers]
                      set-cookie-headers)
            matching-cookie (first (filter #(re-find cookie-pattern %) cookies))]
        (when matching-cookie
          (second (re-find cookie-pattern matching-cookie)))))))

(defn parse-query-params
  "Parse query parameters from Location header"
  [location]
  (when location
    (let [[_ query] (clojure.string/split location #"\?")]
      (when query
        (into {}
              (map (fn [param]
                     (let [[k v] (clojure.string/split param #"=")]
                       [(keyword k) (ring.util.codec/url-decode v)]))
                   (clojure.string/split query #"&")))))))

;; =============================================================================
;; Authorization Code Flow Tests
;; =============================================================================

(deftest test-authorization-code-flow-basic
  (testing "Complete authorization code flow without PKCE"
    ;; Use confidential client for authorization code flow with secret validation
    (setup-confidential-client!)
    (let [;; Step 1: Authorization request
          auth-request (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :redirect_uri test-redirect-uri
                                 :response_type "code"
                                 :scope "openid profile offline_access"
                                 :state "random-state-123"})
          auth-response (handlers/authorize auth-request)]

      ;; Should redirect to login
      (is (= 302 (:status auth-response)))
      (is (clojure.string/includes? (get-in auth-response [:headers "Location"]) "/oauth/login"))

      ;; Extract state from redirect
      (let [login-params (parse-query-params (get-in auth-response [:headers "Location"]))
            encrypted-state (:state login-params)

            ;; Step 2: Login POST
            login-request (-> (request :post "/oauth/login/index.html")
                              (assoc :params {:username test-username
                                              :password test-password
                                              :state encrypted-state})
                              (assoc :form-params {:username test-username
                                                   :password test-password
                                                   :state encrypted-state}))
            login-response (handlers/login login-request)]

        ;; Should redirect back to client with code
        (is (= 302 (:status login-response)))
        (is (clojure.string/includes? (get-in login-response [:headers "Location"]) test-redirect-uri))

        ;; Should set session cookie
        (is (some? (get-cookie-value login-response "idsrv.session")))

        ;; Extract authorization code
        (let [callback-params (parse-query-params (get-in login-response [:headers "Location"]))
              auth-code (:code callback-params)
              session-cookie (get-cookie-value login-response "idsrv.session")]

          (is (some? auth-code))
          (is (= "random-state-123" (:state callback-params)))

          ;; Step 3: Token exchange
          (let [token-request (-> (request :post "/oauth/token")
                                  (assoc :params {:grant_type "authorization_code"
                                                  :code auth-code
                                                  :client_id test-client-id
                                                  :client_secret test-client-secret
                                                  :redirect_uri test-redirect-uri})
                                  (assoc :cookies {"idsrv.session" {:value session-cookie}}))
                token-response (handlers/token token-request)
                token-data (parse-json-body token-response)]

            ;; Debug token response
            (when (not= 200 (:status token-response))
              (println "\n=== TOKEN EXCHANGE FAILED ===")
              (println "Status:" (:status token-response))
              (println "Body:" (:body token-response))
              (println "Token data:" token-data))

            ;; Should return access token
            (is (= 200 (:status token-response)))
            (is (some? (:access_token token-data)))
            (is (some? (:refresh_token token-data)))
            (is (some? (:id_token token-data))) ;; OpenID Connect
            (is (= "Bearer" (:token_type token-data)))
            (is (pos? (:expires_in token-data)))))))))

(deftest test-authorization-code-flow-with-pkce
  (testing "Complete authorization code flow with PKCE"
    ;; Use confidential client
    (setup-confidential-client!)
    (let [;; Generate PKCE parameters
          code-verifier "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
          code-challenge "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
          code-challenge-method "S256"

          ;; Step 1: Authorization request with PKCE
          auth-request (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :redirect_uri test-redirect-uri
                                 :response_type "code"
                                 :scope "openid"
                                 :code_challenge code-challenge
                                 :code_challenge_method code-challenge-method})
          auth-response (handlers/authorize auth-request)]

      ;; Should redirect to login
      (is (= 302 (:status auth-response)))

      (let [login-params (parse-query-params (get-in auth-response [:headers "Location"]))
            encrypted-state (:state login-params)

            ;; Step 2: Login
            login-request (-> (request :post "/oauth/login/index.html")
                              (assoc :params {:username test-username
                                              :password test-password
                                              :state encrypted-state})
                              (assoc :form-params {:username test-username
                                                   :password test-password
                                                   :state encrypted-state}))
            login-response (handlers/login login-request)
            callback-params (parse-query-params (get-in login-response [:headers "Location"]))
            auth-code (:code callback-params)
            session-cookie (get-cookie-value login-response "idsrv.session")]

        ;; Step 3: Token exchange with code_verifier
        (let [token-request (-> (request :post "/oauth/token")
                                (assoc :params {:grant_type "authorization_code"
                                                :code auth-code
                                                :client_id test-client-id
                                                :client_secret test-client-secret
                                                :redirect_uri test-redirect-uri
                                                :code_verifier code-verifier})
                                (assoc :cookies {"idsrv.session" {:value session-cookie}}))
              token-response (handlers/token token-request)
              token-data (parse-json-body token-response)]

          ;; PKCE validation should pass
          (is (= 200 (:status token-response)))
          (is (some? (:access_token token-data))))

        ;; Step 4: Try with WRONG code_verifier (should fail)
        (let [token-request (-> (request :post "/oauth/token")
                                (assoc :params {:grant_type "authorization_code"
                                                :code auth-code
                                                :client_id test-client-id
                                                :client_secret test-client-secret
                                                :redirect_uri test-redirect-uri
                                                :code_verifier "wrong-verifier"})
                                (assoc :cookies {"idsrv.session" {:value session-cookie}}))
              token-response (handlers/token token-request)]

          ;; PKCE validation should fail
          (is (not= 200 (:status token-response))))))))

;; =============================================================================
;; Refresh Token Flow Tests
;; =============================================================================

(deftest test-refresh-token-flow
  (testing "Refresh token flow"
    ;; First get tokens via authorization code flow
    (let [;; ... (abbreviated: do full authorization code flow)
          ;; Setup confidential client
          client-euuid (setup-confidential-client!)

          ;; For this test, we'll manually create a session and tokens
          user-euuid (id/data :test/oauth-integ-user-2)
          ;; Add resource owner to *resource-owners* atom with :active true
          _ (swap! core/*resource-owners* assoc user-euuid
                   {:name test-username
                    :euuid user-euuid
                    :active true
                    :password test-password})
          session-id (core/gen-session-id)
          _ (core/set-session session-id {:flow "authorization_code"
                                          :client client-euuid  ;; Use euuid!
                                          :last-active (java.util.Date.)})
          _ (core/set-session-resource-owner session-id {:name test-username
                                                         :euuid user-euuid})

          ;; Generate initial tokens (need offline_access for refresh token)
          tokens (token-ns/generate (get @core/*clients* client-euuid)  ;; Use euuid!
                                    session-id
                                    {:scope #{"openid" "profile" "offline_access"}})
          refresh-token (:refresh_token tokens)

          ;; Step 1: Use refresh token to get new access token
          refresh-request (-> (request :post "/oauth/token")
                              (assoc :params {:grant_type "refresh_token"
                                              :refresh_token refresh-token
                                              :client_id test-client-id
                                              :client_secret test-client-secret}))
          refresh-response (handlers/token refresh-request)
          new-tokens (parse-json-body refresh-response)]

      ;; Should return new tokens
      (is (= 200 (:status refresh-response)))
      (is (some? (:access_token new-tokens)))
      (is (some? (:refresh_token new-tokens)))
      (is (= "Bearer" (:token_type new-tokens))))))

;; =============================================================================
;; Device Code Flow Tests
;; =============================================================================

(deftest test-device-code-flow
  (testing "Complete device code flow"
    ;; Setup public client for device code flow
    (setup-public-client!)
    (let [;; Step 1: Device authorization request
          device-auth-request (request :post "/oauth/device/auth"
                                       {:client_id test-client-id
                                        :scope "openid profile"})
          device-auth-response (handlers/device-authorization device-auth-request)
          device-data (parse-json-body device-auth-response)]

      ;; Should return device_code and user_code
      (is (= 200 (:status device-auth-response)))
      (is (some? (:device_code device-data)))
      (is (some? (:user_code device-data)))
      (is (some? (:verification_uri device-data)))
      (is (= 5 (:interval device-data)))
      (is (= 900 (:expires_in device-data)))

      (let [device-code (:device_code device-data)
            user-code (:user_code device-data)

            ;; Step 2: User visits activation page (verification_uri_complete)
            activation-get-request (-> (request :get "/oauth/device/activate")
                                       (assoc :query-params {:user_code user-code}))
            activation-get-response (handlers/device-activation activation-get-request)]

        ;; Should show activation form
        (is (= 200 (:status activation-get-response)))
        (is (clojure.string/includes? (:body activation-get-response) user-code))

        ;; Step 3: User confirms (this would redirect to login in real flow)
        ;; For testing, we'll manually authorize the device
        (swap! dc/*device-codes* update device-code
               (fn [data]
                 (let [session-id (core/gen-session-id)]
                   (core/set-session session-id {:flow "device_code"
                                                 :code device-code
                                                 :client test-client-id})
                   (core/set-session-resource-owner session-id {:name test-username
                                                                :euuid (id/data :test/oauth-integ-user-3)})
                   (assoc data :session session-id))))

        ;; Step 4: Device polls for token
        (let [token-request (-> (request :post "/oauth/token")
                                (assoc :params {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                                :device_code device-code
                                                :client_id test-client-id}))
              token-response (handlers/token token-request)
              token-data (parse-json-body token-response)]

          ;; Should return access token
          (is (= 200 (:status token-response)))
          (is (some? (:access_token token-data)))
          (is (some? (:id_token token-data))))))))

;; =============================================================================
;; Login/Logout Tests
;; =============================================================================

(deftest test-login-session-cookie
  (testing "Login sets session cookie with correct attributes"
    (let [;; Setup confidential client
          client-euuid (setup-confidential-client!)
          client (get @core/*clients* client-euuid)

          ;; Setup test user
          _ (setup-test-user!)

          ;; Create an authorization code manually
          code (ac/gen-authorization-code)
          _ (swap! ac/*authorization-codes* assoc code
                   {:client client-euuid
                    :request {:response_type #{"code"}
                              :redirect_uri test-redirect-uri
                              :scope #{"openid"}}})
          encrypted-state (core/encrypt {:authorization-code code
                                         :flow "authorization_code"})

          ;; Login POST
          login-request (-> (request :post "/oauth/login/index.html")
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state encrypted-state})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state encrypted-state}))
          login-response (handlers/login login-request)
          ;; wrap-cookies middleware converts :cookies to Set-Cookie header(s)
          set-cookie-headers (get-in login-response [:headers "Set-Cookie"])
          ;; Get the session cookie string (may be a seq)
          session-cookie-str (if (string? set-cookie-headers)
                               set-cookie-headers
                               (first (filter #(re-find #"idsrv\.session=" %) set-cookie-headers)))]

      ;; Should redirect with authorization code
      (is (= 302 (:status login-response)))

      ;; Should set session cookie in Set-Cookie header
      ;; Format: "idsrv.session=<value>; Path=/; HttpOnly; Secure; SameSite=None; Expires=Session"
      (is (some? session-cookie-str) "Session cookie should be set")
      (is (re-find #"idsrv\.session=" session-cookie-str))
      (is (re-find #"Path=/" session-cookie-str))
      (is (re-find #"HttpOnly" session-cookie-str))
      (is (re-find #"Secure" session-cookie-str))
      (is (re-find #"SameSite=None" session-cookie-str))
      (is (re-find #"Expires=Session" session-cookie-str)))))

(deftest test-logout-removes-cookie
  (testing "Logout removes session cookie"
    (let [;; Setup confidential client
          client-euuid (setup-confidential-client!)

          ;; Create a session (must use client euuid, not client-id string!)
          session-id (core/gen-session-id)
          _ (core/set-session session-id {:flow "authorization_code"
                                          :code "test-code"
                                          :client client-euuid})  ;; Use euuid!
          _ (core/set-session-resource-owner session-id {:name test-username})

          ;; Logout request
          logout-request (-> (request :post "/oauth/logout")
                             (assoc :params {:idsrv/session session-id
                                             :post_logout_redirect_uri test-redirect-uri}))
          logout-response (handlers/logout logout-request)
          ;; Parse cookie from Set-Cookie header (not :cookies key)
          set-cookie-header (get-in logout-response [:headers "Set-Cookie"])
          cookie-str (if (string? set-cookie-header)
                       set-cookie-header
                       (first set-cookie-header))]

      ;; Should redirect
      (is (= 302 (:status logout-response)))

      ;; Should remove cookie (empty value, max-age=0)
      (is (some? cookie-str) "Set-Cookie header should be present")
      (is (re-find #"idsrv\.session=;" cookie-str) "Cookie value should be empty")
      (is (re-find #"Max-Age=0" cookie-str) "Max-Age should be 0")
      (is (re-find #"Path=/" cookie-str) "Path should be /"))))

;; =============================================================================
;; Token Revocation Tests
;; =============================================================================

(deftest test-token-revocation
  (testing "Token revocation"
    (let [;; Setup confidential client
          client-euuid (setup-confidential-client!)

          ;; Create session and tokens
          session-id (core/gen-session-id)
          _ (core/set-session session-id {:flow "authorization_code"
                                          :client client-euuid})  ;; Use euuid!
          _ (core/set-session-resource-owner session-id {:name test-username
                                                         :euuid (id/data :test/oauth-integ-user-2)})
          tokens (token-ns/generate (get @core/*clients* client-euuid)  ;; Use euuid!
                                    session-id
                                    {:scope #{"openid"}})
          access-token (:access_token tokens)

          ;; Revoke access token
          revoke-request (-> (request :post "/oauth/revoke")
                             (assoc :params {:token access-token
                                             :token_type_hint "access_token"
                                             :client_id test-client-id
                                             :client_secret test-client-secret}))
          revoke-response (handlers/revoke revoke-request)]

      ;; Should succeed
      (is (= 200 (:status revoke-response)))

      ;; Token should be removed from store
      (is (nil? (token-ns/get-token-session :access_token access-token))))))

;; =============================================================================
;; OIDC Endpoint Tests
;; =============================================================================

(deftest test-openid-discovery
  (testing "OpenID Connect Discovery endpoint"
    (let [request (request :get "/.well-known/openid-configuration")
          response (handlers/openid-configuration request)
          config (parse-json-body response)]

      ;; Should return configuration
      (is (= 200 (:status response)))
      (is (some? (:issuer config)))
      (is (some? (:authorization_endpoint config)))
      (is (some? (:token_endpoint config)))
      (is (some? (:userinfo_endpoint config)))
      (is (some? (:jwks_uri config)))
      (is (vector? (:response_types_supported config)))
      (is (vector? (:scopes_supported config))))))

(deftest test-jwks-endpoint
  (testing "JWKS endpoint returns public keys"
    (let [request (request :get "/oauth/jwks")
          response (handlers/jwks request)
          jwks (parse-json-body response)]

      ;; Should return keys
      (is (= 200 (:status response)))
      (is (contains? jwks :keys))
      (is (vector? (:keys jwks))))))

(deftest test-userinfo-endpoint
  (testing "UserInfo endpoint with valid token"
    (let [;; Create session and tokens
          session-id (core/gen-session-id)
          user-euuid (id/data :test/oauth-integ-user-1)
          _ (core/set-session session-id {:flow "authorization_code"
                                          :client test-client-id})
          _ (core/set-session-resource-owner session-id {:name test-username
                                                         :euuid user-euuid
                                                         :person_info {:email "test@example.com"
                                                                       :given_name "Test"
                                                                       :family_name "User"}})
          tokens (token-ns/generate (get @core/*clients* test-client-id)
                                    session-id
                                    {:scope #{"openid" "profile" "email"}})
          access-token (:access_token tokens)

          ;; UserInfo request
          userinfo-request (-> (request :get "/oauth/userinfo")
                               (assoc :headers {"authorization" (str "Bearer " access-token)}))
          userinfo-response (handlers/userinfo userinfo-request)
          userinfo (parse-json-body userinfo-response)]

      ;; Should return user claims
      (is (= 200 (:status userinfo-response)))
      (is (= test-username (:sub userinfo)))
      (is (= "test@example.com" (:email userinfo)))
      (is (= "Test" (:given_name userinfo)))
      (is (= "User" (:family_name userinfo))))))

;; =============================================================================
;; Error Handling Tests
;; =============================================================================

(deftest test-invalid-client-credentials
  (testing "Token request with invalid client secret"
    ;; Setup confidential client to test secret validation
    (setup-confidential-client!)
    (let [token-request (-> (request :post "/oauth/token")
                            (assoc :params {:grant_type "client_credentials"
                                            :client_id test-client-id
                                            :client_secret "wrong-secret"}))
          token-response (handlers/token token-request)
          token-data (parse-json-body token-response)]

      ;; Debug output
      (when (= 200 (:status token-response))
        (println "\n=== WRONG SECRET ACCEPTED ===")
        (println "Status:" (:status token-response))
        (println "Body:" token-data))

      ;; Should return error
      (is (not= 200 (:status token-response))))))

(deftest test-expired-authorization-code
  (testing "Token request with expired authorization code"
    ;; Create an expired code (manually set expiration in past)
    (let [code (ac/gen-authorization-code)
          _ (swap! ac/*authorization-codes* assoc code
                   {:client (id/data :test/oauth-integ-client-3)
                    :created-on (- (System/currentTimeMillis) 1000000) ;; Old
                    :request {:response_type #{"code"}
                              :redirect_uri test-redirect-uri}})

          token-request (-> (request :post "/oauth/token")
                            (assoc :params {:grant_type "authorization_code"
                                            :code code
                                            :client_id test-client-id
                                            :client_secret test-client-secret
                                            :redirect_uri test-redirect-uri}))
          token-response (handlers/token token-request)]

      ;; Should fail (code doesn't exist or is invalid)
      (is (not= 200 (:status token-response))))))

(deftest test-missing-redirect-uri
  (testing "Authorization request without redirect_uri"
    (let [auth-request (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :response_type "code"})
          auth-response (handlers/authorize auth-request)]

      ;; Should return error
      (is (not= 302 (:status auth-response))))))

;; =============================================================================
;; Security-Check Tests (Device Code Flow Security Validation)
;; =============================================================================

(deftest test-device-code-security-check-ip-mismatch
  (testing "Device code login rejects IP address mismatch"
    ;; Setup public client
    (setup-public-client!)
    (let [;; Step 1: Device authorization from IP 192.168.1.100
          device-auth-request (-> (request :post "/oauth/device/auth"
                                           {:client_id test-client-id
                                            :scope "openid profile"})
                                  (assoc :remote-addr "192.168.1.100"))
          device-auth-response (handlers/device-authorization device-auth-request)
          device-data (parse-json-body device-auth-response)
          device-code (:device_code device-data)
          user-code (:user_code device-data)

          ;; Step 2: User visits activation page from SAME IP
          activation-get-request (-> (request :get "/oauth/device/activate"
                                              {:user_code user-code})
                                     (assoc :remote-addr "192.168.1.100")
                                     (assoc :headers {"host" "localhost"
                                                      "user-agent" "Mozilla/5.0"}))
          activation-get-response (handlers/device-activation activation-get-request)]

      ;; Should show activation form
      (is (= 200 (:status activation-get-response)))

      ;; Extract challenge from activation form (parse HTML to get encrypted state)
      ;; For testing, we'll manually create the encrypted state
      (let [encrypted-state (core/encrypt {:device-code device-code
                                           :user-code user-code
                                           :ip "192.168.1.100"
                                           :user-agent "Mozilla/5.0"
                                           :flow "device_code"
                                           :challenge "test-challenge-123"})

            ;; Store challenge in device-codes atom
            _ (swap! dc/*device-codes* update device-code
                     (fn [data]
                       (assoc-in data [:challenges "test-challenge-123"]
                                 {:ip "192.168.1.100"
                                  :user-agent "Mozilla/5.0"})))

            ;; Step 3: User attempts login from DIFFERENT IP (ATTACK!)
            login-request (-> (request :post "/oauth/login")
                              (assoc :remote-addr "192.168.1.200") ; ← DIFFERENT IP
                              (assoc :headers {"host" "localhost"
                                               "user-agent" "Mozilla/5.0"})
                              (assoc :params {:username test-username
                                              :password test-password
                                              :state encrypted-state})
                              (assoc :form-params {:username test-username
                                                   :password test-password
                                                   :state encrypted-state}))
            login-response (handlers/login login-request)]

        ;; Should reject with IP address error
        (is (= 302 (:status login-response)))
        (let [location (get-in login-response [:headers "Location"])]
          (is (clojure.string/includes? location "/oauth/status"))
          (is (clojure.string/includes? location "error=ip_address")))))))

(deftest test-device-code-security-check-user-agent-mismatch
  (testing "Device code login rejects user agent mismatch"
    ;; Setup public client
    (setup-public-client!)
    (let [;; Setup device code
          device-auth-request (-> (request :post "/oauth/device/auth"
                                           {:client_id test-client-id
                                            :scope "openid"})
                                  (assoc :remote-addr "192.168.1.100"))
          device-auth-response (handlers/device-authorization device-auth-request)
          device-data (parse-json-body device-auth-response)
          device-code (:device_code device-data)
          user-code (:user_code device-data)

          ;; Create encrypted state with original user agent
          encrypted-state (core/encrypt {:device-code device-code
                                         :user-code user-code
                                         :ip "192.168.1.100"
                                         :user-agent "Mozilla/5.0 (Original Browser)"
                                         :flow "device_code"
                                         :challenge "test-challenge-456"})

          ;; Store challenge
          _ (swap! dc/*device-codes* update device-code
                   (fn [data]
                     (assoc-in data [:challenges "test-challenge-456"]
                               {:ip "192.168.1.100"
                                :user-agent "Mozilla/5.0 (Original Browser)"})))

          ;; Attempt login from DIFFERENT user agent (ATTACK!)
          login-request (-> (request :post "/oauth/login")
                            (assoc :remote-addr "192.168.1.100") ; Same IP
                            (assoc :headers {"host" "localhost"
                                             "user-agent" "Chrome/91.0 (Different Browser)"}) ; ← DIFFERENT UA
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state encrypted-state})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state encrypted-state}))
          login-response (handlers/login login-request)]

      ;; Should reject with user_agent error
      (is (= 302 (:status login-response)))
      (let [location (get-in login-response [:headers "Location"])]
        (is (clojure.string/includes? location "/oauth/status"))
        (is (clojure.string/includes? location "error=user_agent"))))))

(deftest test-device-code-security-check-invalid-challenge
  (testing "Device code login rejects invalid challenge"
    ;; Setup public client
    (setup-public-client!)
    (let [;; Setup device code
          device-auth-request (-> (request :post "/oauth/device/auth"
                                           {:client_id test-client-id
                                            :scope "openid"})
                                  (assoc :remote-addr "192.168.1.100"))
          device-auth-response (handlers/device-authorization device-auth-request)
          device-data (parse-json-body device-auth-response)
          device-code (:device_code device-data)
          user-code (:user_code device-data)

          ;; Create encrypted state with challenge that doesn't exist in atom
          encrypted-state (core/encrypt {:device-code device-code
                                         :user-code user-code
                                         :ip "192.168.1.100"
                                         :user-agent "Mozilla/5.0"
                                         :flow "device_code"
                                         :challenge "INVALID-CHALLENGE-999"}) ; ← Not in atom

          ;; DON'T store this challenge (simulating replay attack or expired challenge)

          ;; Attempt login with invalid challenge
          login-request (-> (request :post "/oauth/login")
                            (assoc :remote-addr "192.168.1.100")
                            (assoc :headers {"host" "localhost"
                                             "user-agent" "Mozilla/5.0"})
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state encrypted-state})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state encrypted-state}))
          login-response (handlers/login login-request)]

      ;; Should reject with challenge error
      (is (= 302 (:status login-response)))
      (let [location (get-in login-response [:headers "Location"])]
        (is (clojure.string/includes? location "/oauth/status"))
        (is (clojure.string/includes? location "error=challenge"))))))

(deftest test-device-code-security-check-passes-valid-data
  (testing "Device code login succeeds with matching IP, user agent, and valid challenge"
    ;; Setup public client
    (setup-public-client!)
    (let [;; Setup device code
          device-auth-request (-> (request :post "/oauth/device/auth"
                                           {:client_id test-client-id
                                            :scope "openid"})
                                  (assoc :remote-addr "192.168.1.100"))
          device-auth-response (handlers/device-authorization device-auth-request)
          _ (when (not= 200 (:status device-auth-response))
              (println "\n=== DEVICE AUTH FAILED ===")
              (println "Status:" (:status device-auth-response))
              (println "Headers:" (:headers device-auth-response))
              (println "Body:" (:body device-auth-response)))
          device-data (parse-json-body device-auth-response)
          device-code (:device_code device-data)
          user-code (:user_code device-data)

          ;; Create encrypted state with valid data
          encrypted-state (core/encrypt {:device-code device-code
                                         :user-code user-code
                                         :ip "192.168.1.100"
                                         :user-agent "Mozilla/5.0 Consistent Browser"
                                         :flow "device_code"
                                         :challenge "valid-challenge-789"})

          ;; Store valid challenge
          _ (swap! dc/*device-codes* update device-code
                   (fn [data]
                     (assoc-in data [:challenges "valid-challenge-789"]
                               {:ip "192.168.1.100"
                                :user-agent "Mozilla/5.0 Consistent Browser"})))

          ;; Attempt login with MATCHING IP, user agent, and valid challenge
          login-request (-> (request :post "/oauth/login")
                            (assoc :remote-addr "192.168.1.100") ; ← Same IP
                            (assoc :headers {"host" "localhost"
                                             "user-agent" "Mozilla/5.0 Consistent Browser"}) ; ← Same UA
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state encrypted-state})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state encrypted-state}))
          login-response (handlers/login login-request)]

      ;; Should succeed and redirect to success status
      (is (= 302 (:status login-response)))
      (let [location (get-in login-response [:headers "Location"])]
        (is (clojure.string/includes? location "/oauth/status"))
        (is (clojure.string/includes? location "value=success"))
        (is (not (clojure.string/includes? location "error=")))))))

(deftest test-authorization-code-flow-no-security-check
  (testing "Authorization code flow does NOT apply security-check (only device code)"
    (let [;; Create authorization code
          code (ac/gen-authorization-code)
          client (get @core/*clients* test-client-id)
          _ (swap! ac/*authorization-codes* assoc code
                   {:client (:euuid client)
                    :request {:response_type #{"code"}
                              :redirect_uri test-redirect-uri
                              :scope #{"openid"}}})
          encrypted-state (core/encrypt {:authorization-code code
                                         :flow "authorization_code"
                                         ;; Note: NO ip/user-agent/challenge for auth code flow
                                         })

          ;; Login from one IP
          login-request (-> (request :post "/oauth/login")
                            (assoc :remote-addr "192.168.1.100")
                            (assoc :headers {"host" "localhost"
                                             "user-agent" "Browser A"})
                            (assoc :params {:username test-username
                                            :password test-password
                                            :state encrypted-state})
                            (assoc :form-params {:username test-username
                                                 :password test-password
                                                 :state encrypted-state}))
          login-response (handlers/login login-request)]

      ;; Should succeed - authorization_code flow doesn't check IP/UA
      (is (= 302 (:status login-response)))
      (is (clojure.string/includes? (get-in login-response [:headers "Location"]) test-redirect-uri))
      (is (some? (get-cookie-value login-response "idsrv.session"))))))
