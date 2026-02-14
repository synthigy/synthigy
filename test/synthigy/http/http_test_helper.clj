(ns synthigy.http.http-test-helper
  "HTTP test utilities for OAuth/OIDC integration tests.

  Provides:
  - Server fixtures (deps-fixture, server-fixture)
  - clj-http wrapper functions
  - Cookie jar management for session tests
  - Response parsing helpers
  - Test client/user setup via direct IAM calls

  Usage:
    (use-fixtures :once deps-fixture server-fixture)
    (use-fixtures :each oauth-data-fixture)

  The fixture hierarchy ensures proper startup order:
  1. deps-fixture (:once) - starts server dependencies via lifecycle
  2. server-fixture (:once) - starts HTTP server on test port
  3. oauth-data-fixture (:each) - sets up test client + user per test"
  (:require
    [clj-http.client :as http]
    [clj-http.cookies :as cookies]
    [clojure.data.json :as json]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [ring.util.codec :as codec]
    [synthigy.server :as server]
    [synthigy.test-helper :as test-helper]))

;;; ============================================================================
;;; Test Configuration
;;; ============================================================================

(def test-port 18080)
(def test-host "localhost")
(def base-url (str "http://" test-host ":" test-port))

;;; ============================================================================
;;; HTTP Client Helpers
;;; ============================================================================

(defn make-cookie-store
  "Creates a new cookie store for session management."
  []
  (cookies/cookie-store))

(def ^:dynamic *cookie-store*
  "Dynamic var for cookie store. Bind this to share cookies across requests."
  nil)

(defn with-cookie-store
  "Execute f with a fresh cookie store bound."
  [f]
  (binding [*cookie-store* (make-cookie-store)]
    (f)))

(defn http-opts
  "Default HTTP options for test requests.

  Args:
    extra-opts - Additional options to merge

  Returns:
    HTTP options map with sensible defaults"
  ([] (http-opts {}))
  ([extra-opts]
   (merge {:throw-exceptions false
           :follow-redirects false  ; Handle redirects manually for OAuth
           :cookie-store *cookie-store*}
          extra-opts)))

;;; ============================================================================
;;; URL Building
;;; ============================================================================

(defn url
  "Build full URL for test endpoint.

  Examples:
    (url \"/oauth/authorize\")
    ;; => \"http://localhost:18080/oauth/authorize\""
  [path]
  (str base-url path))

(defn url-with-params
  "Build URL with query parameters.

  Examples:
    (url-with-params \"/oauth/authorize\" {:client_id \"foo\" :state \"bar\"})
    ;; => \"http://localhost:18080/oauth/authorize?client_id=foo&state=bar\""
  [path params]
  (if (empty? params)
    (url path)
    (str (url path) "?"
         (str/join "&"
                   (map (fn [[k v]]
                          (str (name k) "=" (codec/url-encode (str v))))
                        params)))))

;;; ============================================================================
;;; Response Parsing
;;; ============================================================================

(defn parse-json
  "Parse JSON response body to Clojure map."
  [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

(defn parse-location-params
  "Parse query parameters from Location header.

  Handles both full URLs and path-only locations.

  Examples:
    (parse-location-params \"https://example.com/callback?code=ABC&state=XYZ\")
    ;; => {:code \"ABC\" :state \"XYZ\"}"
  [location]
  (when location
    (when-let [query-start (.indexOf location "?")]
      (when (>= query-start 0)
        (let [query (subs location (inc query-start))]
          (into {}
                (map (fn [param]
                       (let [[k v] (str/split param #"=" 2)]
                         [(keyword k) (when v (codec/url-decode v))]))
                     (str/split query #"&"))))))))

(defn location-header
  "Extract Location header from response."
  [response]
  (get-in response [:headers "Location"]))

(defn content-type
  "Extract Content-Type header from response."
  [response]
  (get-in response [:headers "Content-Type"]))

;;; ============================================================================
;;; HTTP Request Wrappers
;;; ============================================================================

(defn GET
  "Perform GET request with standard options.

  Args:
    path - URL path (will be prefixed with base-url)
    opts - Optional additional HTTP options

  Returns:
    Response map"
  ([path] (GET path {}))
  ([path opts]
   (http/get (url path) (http-opts opts))))

(defn POST
  "Perform POST request with standard options.

  Args:
    path - URL path (will be prefixed with base-url)
    opts - Optional additional HTTP options

  Returns:
    Response map"
  ([path] (POST path {}))
  ([path opts]
   (http/post (url path) (http-opts opts))))

(defn POST-form
  "Perform POST request with form-encoded body.

  Args:
    path - URL path
    form-params - Map of form parameters
    opts - Optional additional HTTP options

  Returns:
    Response map"
  ([path form-params] (POST-form path form-params {}))
  ([path form-params opts]
   (POST path (merge {:form-params form-params} opts))))

(defn POST-json
  "Perform POST request with JSON body.

  Args:
    path - URL path
    body - Map to serialize as JSON
    opts - Optional additional HTTP options

  Returns:
    Response map"
  ([path body] (POST-json path body {}))
  ([path body opts]
   (POST path (merge {:headers {"Content-Type" "application/json"}
                      :body (json/write-str body)}
                     opts))))

(defn GET-with-bearer
  "Perform GET request with Bearer token authentication.

  Args:
    path - URL path
    token - Bearer token string
    opts - Optional additional HTTP options

  Returns:
    Response map"
  ([path token] (GET-with-bearer path token {}))
  ([path token opts]
   (GET path (merge {:headers {"Authorization" (str "Bearer " token)}} opts))))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn deps-fixture
  "Fixture to initialize the test system.

  Initializes core modules (database, dataset, IAM, OAuth) via
  test-helper/initialize-system!. This is the same initialization
  used by other test suites.

  Use as :once fixture before server-fixture."
  [f]
  (log/info "[HTTP-TEST] Initializing test system...")

  ;; Initialize system (database, dataset, IAM, OAuth core)
  ;; This matches what test-helper/system-fixture does
  (test-helper/initialize-system!)

  (log/info "[HTTP-TEST] Test system ready")
  (try
    (f)
    (finally
      (log/info "[HTTP-TEST] Test system cleanup (no-op)"))))

(defn server-fixture
  "Fixture to start HTTP server on test port.

  Must be used AFTER deps-fixture.

  Use as :once fixture."
  [f]
  (log/infof "[HTTP-TEST] Starting HTTP server on %s:%s" test-host test-port)

  ;; Start server with custom test port
  (server/start {:host test-host
                 :port test-port
                 :spa-root nil  ; No SPA for tests
                 :info {:test true :version "test-1.0"}})

  ;; Give server time to bind
  (Thread/sleep 500)

  (try
    ;; Verify server is responding
    (let [response (http/get (url "/info") {:throw-exceptions false})]
      (when-not (= 200 (:status response))
        (throw (ex-info "Server failed to start" {:response response}))))

    (log/info "[HTTP-TEST] Server ready, running tests...")
    (f)

    (finally
      (log/info "[HTTP-TEST] Stopping HTTP server...")
      (server/stop)
      (log/info "[HTTP-TEST] Server stopped"))))

(defn oauth-data-fixture
  "Fixture to set up test OAuth client and user.

  Resets OAuth state and creates fresh test data for each test.

  Use as :each fixture."
  [f]
  ;; Reset OAuth state
  (test-helper/reset-oauth-state!)

  ;; Setup test client and user
  (test-helper/setup-test-client!
    {:client-id "test-client"
     :client-secret "test-secret"
     :redirect-uri "https://example.com/callback"
     :allowed-grants ["authorization_code" "refresh_token"
                      "client_credentials" "urn:ietf:params:oauth:grant-type:device_code"]
     :type "confidential"})

  (test-helper/setup-test-user!
    {:username "testuser"
     :password "testpass123"
     :email "test@example.com"
     :given-name "Test"
     :family-name "User"})

  (try
    (f)
    (finally
      (test-helper/reset-oauth-state!))))

(defn cookie-store-fixture
  "Fixture that binds a fresh cookie store for each test.

  Use as :each fixture when session persistence is needed."
  [f]
  (with-cookie-store f))

;;; ============================================================================
;;; Combined Fixture Helpers
;;; ============================================================================

(defn all-fixtures
  "Compose all fixtures for HTTP tests.

  Usage:
    (use-fixtures :once (all-fixtures))"
  []
  (fn [f]
    (deps-fixture
      (fn []
        (server-fixture f)))))

;;; ============================================================================
;;; OAuth Flow Helpers
;;; ============================================================================

(defn follow-redirect
  "Follow a redirect response.

  Args:
    response - HTTP response with 302 status

  Returns:
    Response from redirect target"
  [response]
  (when-let [location (location-header response)]
    (if (str/starts-with? location "http")
      ;; External redirect (callback URL) - don't follow
      {:redirected-to location
       :params (parse-location-params location)}
      ;; Internal redirect - follow it
      (GET location))))

(defn basic-auth-header
  "Generate Basic auth header value.

  Args:
    client-id - OAuth client ID
    client-secret - OAuth client secret

  Returns:
    Authorization header value string"
  [client-id client-secret]
  (let [credentials (str client-id ":" client-secret)
        encoded (.encodeToString (java.util.Base64/getEncoder)
                                 (.getBytes credentials "UTF-8"))]
    (str "Basic " encoded)))

(defn extract-authorization-code
  "Extract authorization code from redirect response.

  Args:
    response - Response from authorize endpoint (302 redirect)

  Returns:
    Authorization code string or nil"
  [response]
  (when (= 302 (:status response))
    (when-let [location (location-header response)]
      (:code (parse-location-params location)))))

;;; ============================================================================
;;; PKCE Helpers
;;; ============================================================================

(defn generate-code-verifier
  "Generate a random PKCE code verifier (43-128 chars).

  Returns:
    String suitable for use as code_verifier"
  []
  (let [chars "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        length (+ 43 (rand-int 85))]  ; 43-128 chars
    (apply str (repeatedly length #(rand-nth chars)))))

(defn compute-code-challenge-s256
  "Compute S256 code challenge from verifier.

  Args:
    code-verifier - The PKCE code verifier

  Returns:
    Base64url-encoded SHA256 hash"
  [code-verifier]
  (let [digest (java.security.MessageDigest/getInstance "SHA-256")
        hash (.digest digest (.getBytes code-verifier "ASCII"))
        encoder (.withoutPadding (java.util.Base64/getUrlEncoder))
        encoded (.encodeToString encoder hash)]
    encoded))

;;; ============================================================================
;;; Test Data Constants
;;; ============================================================================

(def test-client-id "test-client")
(def test-client-secret "test-secret")
(def test-redirect-uri "https://example.com/callback")
(def test-username "testuser")
(def test-password "testpass123")
