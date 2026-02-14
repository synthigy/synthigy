(ns synthigy.oauth-integration-test
  "Integration tests for OAuth 2.0 and OIDC endpoints.

   Tests the full OAuth authorization code flow against a running server.
   Requires the server to be running on localhost:8080."
  (:require
    [clojure.data.json :as json]
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [synthigy.dataset :as dataset]
    [synthigy.iam :as iam]
    [synthigy.test-helper :as helper])
  (:import
    [java.io BufferedReader InputStreamReader OutputStreamWriter]
    [java.net HttpURLConnection URL]))

;;; ============================================================================
;;; Configuration
;;; ============================================================================

(def base-url "http://localhost:8080")

(def test-client
  {:id "oauth-test-client"
   :name "OAuth Integration Test Client"
   :type :confidential
   :secret "oauth-test-secret"
   :settings {:allowed-grants ["authorization_code" "refresh_token"]
              :redirections ["http://localhost:9999/callback"]
              :logout-redirections ["http://localhost:9999/logout"]}})

(def test-user
  {:name "oauth-test-user"
   :password "oauth-test-pass"
   :active true
   :person_info {:email "oauth-test@example.com"
                 :given_name "OAuth"
                 :family_name "TestUser"}})

;;; ============================================================================
;;; HTTP Helpers (using Java's HttpURLConnection)
;;; ============================================================================

(defn form-encode
  "URL encode form params."
  [params]
  (str/join "&"
            (map (fn [[k v]]
                   (str (java.net.URLEncoder/encode (name k) "UTF-8")
                        "="
                        (java.net.URLEncoder/encode (str v) "UTF-8")))
                 params)))

(defn read-response-body
  "Read response body from connection."
  [^HttpURLConnection conn]
  (try
    (let [stream (try (.getInputStream conn)
                      (catch Exception _ (.getErrorStream conn)))]
      (when stream
        (with-open [reader (BufferedReader. (InputStreamReader. stream "UTF-8"))]
          (str/join "\n" (line-seq reader)))))
    (catch Exception _ nil)))

(defn http-request
  "Make HTTP request. Returns {:status int :headers map :body string :location string}"
  [{:keys [method url headers body]}]
  (let [conn ^HttpURLConnection (.openConnection (URL. url))]
    (try
      (.setRequestMethod conn (or method "GET"))
      (.setInstanceFollowRedirects conn false)
      (.setConnectTimeout conn 5000)
      (.setReadTimeout conn 5000)

      ;; Set headers
      (doseq [[k v] headers]
        (.setRequestProperty conn k v))

      ;; Write body if present
      (when body
        (.setDoOutput conn true)
        (with-open [writer (OutputStreamWriter. (.getOutputStream conn) "UTF-8")]
          (.write writer ^String body)
          (.flush writer)))

      ;; Get response
      (let [status (.getResponseCode conn)
            location (.getHeaderField conn "Location")
            response-body (read-response-body conn)]
        {:status status
         :location location
         :body response-body
         :headers (into {} (for [[k v] (.getHeaderFields conn)
                                 :when k]
                             [k (first v)]))})
      (finally
        (.disconnect conn)))))

(defn GET [url & [opts]]
  (http-request (merge {:method "GET" :url url} opts)))

(defn POST [url & [opts]]
  (http-request (merge {:method "POST" :url url} opts)))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (try
      (json/read-str body :key-fn keyword)
      (catch Exception _ nil))))

;;; ============================================================================
;;; Fixtures
;;; ============================================================================

(defn setup-oauth-test-data
  "Create test client and user for OAuth tests."
  []
  ;; Create test client
  (iam/add-client test-client)
  ;; Create test user (plain text password - Synthigy hashes it)
  (dataset/sync-entity :iam/user test-user))

(defn cleanup-oauth-test-data
  "Remove test client and user."
  []
  (try
    (iam/remove-client {:id (:id test-client)})
    (catch Exception _))
  (try
    (dataset/delete-entity :iam/user {:name (:name test-user)})
    (catch Exception _)))

(defn oauth-fixture [f]
  (helper/initialize-system!)
  (cleanup-oauth-test-data)
  (setup-oauth-test-data)
  (try
    (f)
    (finally
      (cleanup-oauth-test-data))))

(use-fixtures :once oauth-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn parse-location-params
  "Parse query parameters from Location header URL."
  [location]
  (when-let [query-start (str/index-of location "?")]
    (let [query (subs location (inc query-start))]
      (into {}
            (map (fn [param]
                   (let [[k v] (str/split param #"=" 2)]
                     [(keyword k) (java.net.URLDecoder/decode (or v "") "UTF-8")]))
                 (str/split query #"&"))))))

;;; ============================================================================
;;; OIDC Discovery Tests
;;; ============================================================================

(deftest test-oidc-discovery
  (testing "OIDC discovery endpoint returns valid configuration"
    (let [response (GET (str base-url "/.well-known/openid-configuration"))
          config (parse-json-body response)]
      (is (= 200 (:status response)))
      (is (= base-url (:issuer config)))
      (is (str/ends-with? (:authorization_endpoint config) "/oauth/authorize"))
      (is (str/ends-with? (:token_endpoint config) "/oauth/token"))
      (is (str/ends-with? (:userinfo_endpoint config) "/oauth/userinfo"))
      (is (str/ends-with? (:jwks_uri config) "/oauth/jwks"))
      (is (contains? (set (:scopes_supported config)) "openid"))
      (is (contains? (set (:response_types_supported config)) "code")))))

(deftest test-jwks-endpoint
  (testing "JWKS endpoint returns valid keys"
    (let [response (GET (str base-url "/oauth/jwks"))
          jwks (parse-json-body response)]
      (is (= 200 (:status response)))
      (is (vector? (:keys jwks)))
      (is (pos? (count (:keys jwks))))
      ;; Each key should have required fields
      (doseq [key (:keys jwks)]
        (is (= "RSA" (:kty key)))
        (is (some? (:n key)))
        (is (some? (:e key)))
        (is (some? (:kid key)))))))

;;; ============================================================================
;;; Authorization Code Flow Tests
;;; ============================================================================

(deftest test-authorization-redirect-to-login
  (testing "Authorization endpoint redirects to login"
    (let [response (GET (str base-url "/oauth/authorize"
                             "?client_id=" (:id test-client)
                             "&redirect_uri=" (java.net.URLEncoder/encode "http://localhost:9999/callback" "UTF-8")
                             "&response_type=code"
                             "&scope=openid%20profile%20email"
                             "&state=test-state"))]
      (is (= 302 (:status response)))
      (is (str/includes? (or (:location response) "") "/oauth/login")))))

(deftest test-full-authorization-code-flow
  (testing "Complete authorization code flow"
    ;; Step 1: Request authorization
    (let [auth-response (GET (str base-url "/oauth/authorize"
                                  "?client_id=" (:id test-client)
                                  "&redirect_uri=" (java.net.URLEncoder/encode "http://localhost:9999/callback" "UTF-8")
                                  "&response_type=code"
                                  "&scope=openid%20profile%20email"
                                  "&state=integration-test"))
          login-location (:location auth-response)
          login-params (parse-location-params login-location)
          state-token (:state login-params)]

      (is (= 302 (:status auth-response)) "Should redirect to login")
      (is (some? state-token) "Should have state token")

      ;; Step 2: Submit login credentials
      (let [login-response (POST (str base-url "/oauth/login")
                                 {:headers {"Content-Type" "application/x-www-form-urlencoded"}
                                  :body (form-encode {:username (:name test-user)
                                                      :password (:password test-user)
                                                      :state state-token})})
            callback-location (:location login-response)
            callback-params (parse-location-params callback-location)]

        (is (= 302 (:status login-response))
            (str "Should redirect to callback. Got status: " (:status login-response)))
        (is (str/starts-with? (or callback-location "") "http://localhost:9999/callback")
            (str "Should redirect to registered callback URI. Got: " callback-location))
        (is (= "integration-test" (:state callback-params)) "State should match")
        (is (some? (:code callback-params)) "Should have authorization code")

        ;; Step 3: Exchange code for tokens
        (when-let [code (:code callback-params)]
          (let [token-response (POST (str base-url "/oauth/token")
                                     {:headers {"Content-Type" "application/x-www-form-urlencoded"}
                                      :body (form-encode {:grant_type "authorization_code"
                                                          :code code
                                                          :client_id (:id test-client)
                                                          :client_secret (:secret test-client)
                                                          :redirect_uri "http://localhost:9999/callback"})})
                tokens (parse-json-body token-response)]

            (is (= 200 (:status token-response))
                (str "Token exchange should succeed. Got: " (:status token-response) " - " (:body token-response)))
            (is (some? (:access_token tokens)) "Should have access token")
            (is (some? (:id_token tokens)) "Should have ID token (OIDC)")
            (is (= "Bearer" (:token_type tokens)) "Token type should be Bearer")
            (is (pos? (or (:expires_in tokens) 0)) "Should have expiration")

            ;; Step 4: Use access token to get userinfo
            (when (:access_token tokens)
              (let [userinfo-response (GET (str base-url "/oauth/userinfo")
                                           {:headers {"Authorization" (str "Bearer " (:access_token tokens))}})
                    userinfo (parse-json-body userinfo-response)]

                (is (= 200 (:status userinfo-response)) "Userinfo should succeed")
                (is (= (:name test-user) (:preferred_username userinfo))
                    "Username should match")
                (is (= (get-in test-user [:person_info :email]) (:email userinfo))
                    "Email should match")))))))))

;;; ============================================================================
;;; Error Cases
;;; ============================================================================

(deftest test-invalid-client
  (testing "Authorization with invalid client returns error"
    (let [response (GET (str base-url "/oauth/authorize"
                             "?client_id=nonexistent-client"
                             "&redirect_uri=" (java.net.URLEncoder/encode "http://localhost:9999/callback" "UTF-8")
                             "&response_type=code"
                             "&scope=openid"
                             "&state=test"))]
      (is (= 400 (:status response))))))

(deftest test-invalid-redirect-uri
  (testing "Authorization with unregistered redirect URI returns error"
    (let [response (GET (str base-url "/oauth/authorize"
                             "?client_id=" (:id test-client)
                             "&redirect_uri=" (java.net.URLEncoder/encode "http://evil.com/callback" "UTF-8")
                             "&response_type=code"
                             "&scope=openid"
                             "&state=test"))]
      (is (= 400 (:status response))))))

(deftest test-wrong-credentials
  (testing "Login with wrong password shows error"
    ;; First get the state token
    (let [auth-response (GET (str base-url "/oauth/authorize"
                                  "?client_id=" (:id test-client)
                                  "&redirect_uri=" (java.net.URLEncoder/encode "http://localhost:9999/callback" "UTF-8")
                                  "&response_type=code"
                                  "&scope=openid"
                                  "&state=test"))
          state-token (:state (parse-location-params (:location auth-response)))

          ;; Submit wrong password
          login-response (POST (str base-url "/oauth/login")
                               {:headers {"Content-Type" "application/x-www-form-urlencoded"}
                                :body (form-encode {:username (:name test-user)
                                                    :password "wrong-password"
                                                    :state state-token})})]

      ;; Should return login page with error (400) not redirect
      (is (= 400 (:status login-response))))))

;;; ============================================================================
;;; Token Revocation
;;; ============================================================================

(deftest test-token-revocation
  (testing "Token revocation endpoint"
    ;; First get a token through the full flow
    (let [auth-response (GET (str base-url "/oauth/authorize"
                                  "?client_id=" (:id test-client)
                                  "&redirect_uri=" (java.net.URLEncoder/encode "http://localhost:9999/callback" "UTF-8")
                                  "&response_type=code"
                                  "&scope=openid"
                                  "&state=revoke-test"))
          state-token (:state (parse-location-params (:location auth-response)))

          login-response (POST (str base-url "/oauth/login")
                               {:headers {"Content-Type" "application/x-www-form-urlencoded"}
                                :body (form-encode {:username (:name test-user)
                                                    :password (:password test-user)
                                                    :state state-token})})
          code (:code (parse-location-params (:location login-response)))

          token-response (POST (str base-url "/oauth/token")
                               {:headers {"Content-Type" "application/x-www-form-urlencoded"}
                                :body (form-encode {:grant_type "authorization_code"
                                                    :code code
                                                    :client_id (:id test-client)
                                                    :client_secret (:secret test-client)
                                                    :redirect_uri "http://localhost:9999/callback"})})
          access-token (:access_token (parse-json-body token-response))]

      (when access-token
        ;; Verify token works before revocation
        (let [before-revoke (GET (str base-url "/oauth/userinfo")
                                 {:headers {"Authorization" (str "Bearer " access-token)}})]
          (is (= 200 (:status before-revoke)) "Token should work before revocation"))

        ;; Revoke the token
        (let [revoke-response (POST (str base-url "/oauth/revoke")
                                    {:headers {"Content-Type" "application/x-www-form-urlencoded"}
                                     :body (form-encode {:token access-token
                                                         :client_id (:id test-client)
                                                         :client_secret (:secret test-client)})})]
          (is (#{200 204} (:status revoke-response)) "Revocation should succeed"))

        ;; Verify token no longer works
        (let [after-revoke (GET (str base-url "/oauth/userinfo")
                                {:headers {"Authorization" (str "Bearer " access-token)}})]
          (is (= 401 (:status after-revoke)) "Token should be rejected after revocation"))))))

(comment
  ;; Run tests from REPL
  (require '[clojure.test :refer [run-tests]])
  (run-tests 'synthigy.oauth-integration-test)

  ;; Run individual test
  (test-oidc-discovery)
  (test-jwks-endpoint)
  (test-full-authorization-code-flow))
