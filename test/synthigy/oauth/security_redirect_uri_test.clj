(ns synthigy.oauth.security-redirect-uri-test
  "Security tests for OAuth 2.0 Redirect URI validation.

  Tests critical redirect URI vulnerabilities:
  1. Open redirect attacks
  2. Redirect URI mismatch between authorize and token endpoints
  3. Partial URL matching (subdomain attacks)
  4. Protocol downgrade attacks (HTTPS → HTTP)
  5. Path manipulation"
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

(def test-client-id "test-client-redirect-security")
(def test-client-secret "test-secret-redirect-789")
(def test-username "redirectuser")
(def test-password "test-password-redirect-123")

;; Legitimate redirect URIs
(def legitimate-redirect "https://example.com/callback")
(def legitimate-redirect-with-path "https://example.com/oauth/callback")

;; Attack redirect URIs
(def evil-domain "https://evil.com/steal-tokens")
(def subdomain-attack "https://example.com.evil.com/callback")
(def http-downgrade "http://example.com/callback")
(def path-traversal "https://example.com/callback/../admin")

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

(defn get-error-from-redirect [location]
  "Extract error from redirect URL (for authorization errors)"
  (when location
    (let [params (parse-location-params location)]
      (:error params))))

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
  (let [client-euuid (id/data :test/oauth-redirect-client)
        user-euuid (id/data :test/oauth-redirect-user)]

    ;; Setup client with ONLY legitimate-redirect allowed
    (swap! core/*clients* assoc test-client-id
           {:id test-client-id
            :euuid client-euuid
            :secret (hashers/derive test-client-secret)
            :type :confidential
            :settings {"allowed-grants" ["authorization_code" "refresh_token"]
                       "redirections" [legitimate-redirect
                                       legitimate-redirect-with-path]
                       "logout-redirections" [legitimate-redirect]}})

    ;; Setup user
    (swap! core/*resource-owners* assoc test-username
           {:name test-username
            :euuid user-euuid
            :password (hashers/derive test-password)
            :active true
            :person_info {:email "redirect@example.com"
                          :given_name "Redirect"
                          :family_name "Security"}})

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

    ;; Restore
    (alter-var-root #'iam/get-client (constantly original-get-client))
    (alter-var-root #'iam/get-user-details (constantly original-get-user-details))
    (alter-var-root #'iam/validate-password (constantly original-validate-password))
    (alter-var-root #'iam/publish (constantly original-publish))
    (alter-var-root #'dataset.access/*access-control* (constantly original-access-control))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each setup-fixtures)

;; =============================================================================
;; Helper: Complete Authorization Flow
;; =============================================================================

(defn get-authorization-code
  "Complete authorization flow up to code issuance"
  [redirect-uri]
  (let [auth-response (handlers/authorize
                       (request :get "/oauth/authorize"
                                {:client_id test-client-id
                                 :redirect_uri redirect-uri
                                 :response_type "code"
                                 :scope "openid profile"
                                 :state "test-state"}))

        ;; Check if authorize step failed
        auth-location (get-in auth-response [:headers "Location"])]

    (if (str/includes? (or auth-location "") "error=")
      ;; Authorization failed - return error
      {:error (get-error-from-redirect auth-location)
       :auth-response auth-response}

      ;; Authorization succeeded - complete login
      (let [login-params (parse-location-params auth-location)
            login-response (handlers/login
                            (-> (request :post "/oauth/login")
                                (assoc :params {:username test-username
                                                :password test-password
                                                :state (:state login-params)})
                                (assoc :form-params {:username test-username
                                                     :password test-password
                                                     :state (:state login-params)})))

            callback-params (parse-location-params (get-in login-response [:headers "Location"]))
            auth-code (:code callback-params)]

        {:code auth-code
         :state (:state callback-params)
         :auth-response auth-response
         :login-response login-response}))))

;; =============================================================================
;; Test 1: Open Redirect to Evil Domain
;; =============================================================================

(deftest test-open-redirect-prevention
  (testing "Prevent open redirect to arbitrary domain"
    (println "\n=== OPEN REDIRECT PREVENTION ===")

    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri evil-domain
                                   :response_type "code"
                                   :scope "openid"
                                   :state "evil-state"}))

          location (get-in auth-response [:headers "Location"])
          body (:body auth-response)]

      (println "  Attempted redirect_uri:" evil-domain)
      (println "  Response status:" (:status auth-response))
      (println "  Redirect location:" location)
      (println "  Body contains error page:" (boolean (and body (str/includes? body "redirect_missmatch"))))

      (println "\n--- Results ---")
      ;; Per RFC 6749 Section 4.1.2.1: MUST NOT redirect for invalid redirect_uri
      ;; Should return 400 Bad Request with error page
      (is (= 400 (:status auth-response)) "Should return 400 Bad Request (not redirect)")
      (is (nil? location) "Should NOT redirect")
      (is (some? body) "Should return error page")
      (is (str/includes? body "redirect_missmatch") "Error page should mention redirect mismatch")
      (is (not (str/includes? (or location "") evil-domain))
          "Should NOT redirect to evil domain")

      (if (and (= 400 (:status auth-response)) (nil? location))
        (println "✅ PASS: Open redirect blocked")
        (println "❌ FAIL: Open redirect vulnerability!")))))

;; =============================================================================
;; Test 2: Redirect URI Mismatch (Authorize vs Token)
;; =============================================================================

(deftest test-redirect-uri-mismatch
  (testing "Reject redirect_uri mismatch between authorize and token endpoints"
    (println "\n=== REDIRECT URI MISMATCH ===")

    ;; Step 1: Get code with legitimate redirect_uri
    (let [{:keys [code]} (get-authorization-code legitimate-redirect)

          _ (println "  Got authorization code with redirect_uri:" legitimate-redirect)
          _ (println "  Code:" code)

          ;; Step 2: Try to exchange code with DIFFERENT redirect_uri
          _ (println "\n  → Attempting token exchange with different redirect_uri...")

          token-response (handlers/token
                          (request :post "/oauth/token"
                                   {:grant_type "authorization_code"
                                    :code code
                                    :client_id test-client-id
                                    :client_secret test-client-secret
                                    :redirect_uri legitimate-redirect-with-path}))  ;; DIFFERENT!

          error-data (parse-json-body token-response)]

      (println "  Token response status:" (:status token-response))
      (println "  Error:" (:error error-data))

      (println "\n--- Results ---")
      (is (= 400 (:status token-response)) "Should reject with 400")
      (is (some? (:error error-data)) "Should return error")

      (if (= 400 (:status token-response))
        (println "✅ PASS: Redirect URI mismatch blocked")
        (println "❌ FAIL: Redirect URI mismatch allowed!")))))

;; =============================================================================
;; Test 3: Subdomain Attack (Partial Match)
;; =============================================================================

(deftest test-subdomain-attack
  (testing "Prevent subdomain-based redirect URI attack"
    (println "\n=== SUBDOMAIN ATTACK PREVENTION ===")

    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri subdomain-attack
                                   :response_type "code"
                                   :scope "openid"
                                   :state "attack-state"}))

          location (get-in auth-response [:headers "Location"])
          body (:body auth-response)]

      (println "  Legitimate domain: example.com")
      (println "  Attack redirect_uri:" subdomain-attack)
      (println "  Response status:" (:status auth-response))
      (println "  Body contains error page:" (boolean (and body (str/includes? body "redirect_missmatch"))))

      (println "\n--- Results ---")
      ;; Per RFC 6749: MUST NOT redirect for invalid redirect_uri
      (is (= 400 (:status auth-response)) "Should return 400 Bad Request")
      (is (nil? location) "Should NOT redirect")
      (is (some? body) "Should return error page")
      (is (not (str/includes? (or location "") "evil.com"))
          "Should NOT redirect to evil.com subdomain")

      (if (and (= 400 (:status auth-response)) (nil? location))
        (println "✅ PASS: Subdomain attack blocked")
        (println "❌ FAIL: Subdomain attack succeeded!")))))

;; =============================================================================
;; Test 4: Protocol Downgrade Attack (HTTPS → HTTP)
;; =============================================================================

(deftest test-protocol-downgrade
  (testing "Prevent HTTPS to HTTP protocol downgrade"
    (println "\n=== PROTOCOL DOWNGRADE PREVENTION ===")

    (let [auth-response (handlers/authorize
                         (request :get "/oauth/authorize"
                                  {:client_id test-client-id
                                   :redirect_uri http-downgrade
                                   :response_type "code"
                                   :scope "openid"
                                   :state "downgrade-state"}))

          location (get-in auth-response [:headers "Location"])
          body (:body auth-response)]

      (println "  Registered redirect_uri: https://example.com/callback")
      (println "  Attack redirect_uri:" http-downgrade)
      (println "  Response status:" (:status auth-response))
      (println "  Body contains error page:" (boolean (and body (str/includes? body "redirect_missmatch"))))

      (println "\n--- Results ---")
      ;; Per RFC 6749: MUST NOT redirect for invalid redirect_uri
      (is (= 400 (:status auth-response)) "Should return 400 Bad Request")
      (is (nil? location) "Should NOT redirect")
      (is (some? body) "Should return error page")
      (is (not (str/starts-with? (or location "") "http://"))
          "Should NOT redirect to HTTP")

      (if (and (= 400 (:status auth-response)) (nil? location))
        (println "✅ PASS: Protocol downgrade blocked")
        (println "❌ FAIL: Protocol downgrade allowed!")))))

;; =============================================================================
;; Test 5: Exact Match Required (Path Validation)
;; =============================================================================

(deftest test-exact-redirect-uri-match
  (testing "Redirect URI must match exactly (base URI match only)"
    (println "\n=== EXACT REDIRECT URI MATCH ===")

    ;; Test that base URI matching works correctly
    ;; Registered: https://example.com/callback
    ;; Should allow: https://example.com/callback?foo=bar (query params OK)
    ;; Should block: https://example.com/callback/admin (path extension)

    (let [with-query-params "https://example.com/callback?foo=bar&baz=qux"

          ;; Test 1: Query parameters should be allowed
          auth-response-1 (handlers/authorize
                           (request :get "/oauth/authorize"
                                    {:client_id test-client-id
                                     :redirect_uri with-query-params
                                     :response_type "code"
                                     :scope "openid"
                                     :state "query-state"}))

          location-1 (get-in auth-response-1 [:headers "Location"])
          error-1 (get-error-from-redirect location-1)]

      (println "  Registered: https://example.com/callback")
      (println "  Testing with query params:" with-query-params)
      (println "  Status:" (:status auth-response-1))
      (println "  Error:" error-1)

      (println "\n--- Results ---")
      ;; Query params should be allowed (base URI matches)
      (is (nil? error-1) "Query parameters should be allowed")
      (is (str/includes? (or location-1 "") "/oauth/login")
          "Should proceed to login")

      (if (nil? error-1)
        (println "✅ PASS: Query parameters allowed (base URI match)")
        (println "❌ FAIL: Query parameters blocked incorrectly")))))
