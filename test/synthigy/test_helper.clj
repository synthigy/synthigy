(ns synthigy.test-helper
  "Shared test fixtures for all Synthigy tests.

  This namespace provides comprehensive system initialization and cleanup
  to prevent tests from hanging due to unclosed async resources.

  Uses patcho.lifecycle directly for module management.
  Backend implementation (Postgres, SQLite, etc.) is loaded dynamically
  when synthigy.core is required (based on environment variables).

  Usage:
    (use-fixtures :once test-helper/system-fixture)
    (use-fixtures :each test-helper/reset-state-fixture)"
  (:require
    [buddy.hashers :as hashers]
    [synthigy.json :as json]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [patcho.lifecycle :as lifecycle]
    [ring.util.codec :as codec]
    [synthigy.core :as core]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.db :as db]
    [synthigy.iam.context :as context]
    [synthigy.iam.encryption :as encryption]
    [synthigy.oauth.authorization-code :as ac]
    [synthigy.oauth.core :as oauth-core]
    [synthigy.oauth.device-code :as device-code]
    [synthigy.oauth.handlers :as handlers]
    [synthigy.oauth.token :as token-ns]
    [synthigy.test-data]))  ; Load test data registrations

;;; ============================================================================
;;; System State Management
;;; ============================================================================

;; Use def instead of defonce to allow re-initialization across test runs
(def ^:private ^:dynamic *system-initialized?* (atom false))

(defn initialize-system!
  "Initialize the complete Synthigy system for testing.

  Uses patcho.lifecycle to start modules:
  - :synthigy/dataset (→ :synthigy/database → :synthigy/transit)
  - :synthigy/iam
  - :synthigy.iam/encryption (for JWT signing tests)
  - :synthigy.dataset/encryption (for data encryption)

  This is called ONCE at the start of the test suite.
  Safe to call multiple times (idempotent).

  Auto-generates and saves encryption keys to .env if not present."
  []
  (when-not @*system-initialized?*
    (log/info "\n========================================")
    (log/info "Initializing Synthigy Test Suite")
    (log/info "========================================\n")
    (comment
      (lifecycle/print-system-report)
      (lifecycle/start! :synthigy.iam/encryption))

    (try
      ;; Start core modules using lifecycle
      (lifecycle/start!
        :synthigy/iam :synthigy.iam/encryption
        :synthigy/audit :synthigy/oauth)

      ;; Ensure dataset encryption is initialized (auto-generates and saves to .env)
      (let [{:keys [initialized? generated?]} (dataset-encryption/ensure-initialized!)]
        (when generated?
          (log/info "[TEST] Generated new dataset encryption master key (saved to .env)"))
        (when-not initialized?
          (log/warn "[TEST] Dataset encryption not initialized - some tests may fail")))

      (log/info "\n========================================")
      (log/info "Synthigy Test Suite Ready!")
      (log/info "========================================\n")

      (reset! *system-initialized?* true)

      (catch Exception e
        (log/errorf e "Failed to initialize test system")
        (throw e))))

  ;; Verify providers are set and ensure keypairs exist
  (when-not encryption/*encryption-provider*
    (log/warn "[TEST] Encryption provider not set - starting encryption module...")
    (lifecycle/start! :synthigy.iam/encryption))

  (when-not context/*user-context-provider*
    (log/warn "[TEST] User context provider not set - starting context...")
    (context/start))

  ;; Ensure encryption keypairs exist (even if system was previously initialized)
  (when (and encryption/*encryption-provider*
             (empty? (encryption/list-keypairs encryption/*encryption-provider*)))
    (log/info "[TEST] Generating encryption keypair...")
    (encryption/rotate-keypair encryption/*encryption-provider*)))

(defn shutdown-system!
  "Shutdown the Synthigy system and clean up all resources.

  NOTE: Currently a no-op. The system stays running across all test suites
  in the same JVM process. This avoids issues with:
  - HikariPool being closed but *db* still referencing it
  - Module registrations being lost
  - State inconsistencies between test suites

  The system will be cleaned up when the JVM exits."
  []
  ;; Intentionally empty - system stays running across test suites
  nil)

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn system-fixture
  "Use this as a :once fixture to initialize the system.

  This starts the complete Synthigy system and ensures proper cleanup
  after all tests complete.

  Example:
    (use-fixtures :once test-helper/system-fixture)"
  [f]
  (initialize-system!)
  (try
    (f)
    (finally
      (shutdown-system!))))

(defn reset-oauth-state!
  "Reset OAuth state atoms (sessions, codes, tokens).

  This should be called before/after each test to ensure isolation."
  []
  (reset! oauth-core/*clients* {})
  (reset! oauth-core/*resource-owners* {})
  (reset! oauth-core/*sessions* {})
  (reset! ac/*authorization-codes* {})
  (reset! device-code/*device-codes* {})
  (reset! token-ns/*tokens* {}))

(defn reset-oauth-state-fixture
  "Use this as an :each fixture to reset OAuth state between tests.

  Example:
    (use-fixtures :each test-helper/reset-oauth-state-fixture)"
  [f]
  (reset-oauth-state!)
  (f)
  (reset-oauth-state!))

;;; ============================================================================
;;; Utility Functions
;;; ============================================================================

(defn system-initialized?
  "Check if the test system is initialized."
  []
  @*system-initialized?*)

;;; ============================================================================
;;; OAuth Test Helpers
;;; ============================================================================

(defn setup-test-client!
  "Setup a test OAuth client in the *clients* atom.

  Returns the client map with ID key.

  NOTE: For testing with mocks, the secret can be stored in plaintext.
  The clients-match? function supports both plaintext and hashed secrets
  (bcrypt), so you can use either depending on your test needs."
  [{:keys [client-id client-secret redirect-uri allowed-grants type]
    :or {type "confidential"
         allowed-grants ["authorization_code" "refresh_token"]}}]
  (let [client-entity-id (id/data :test/helper-client)]
    (swap! oauth-core/*clients* assoc client-id
           {:id client-id
            (id/key) client-entity-id
            :secret client-secret  ; Store plaintext for testing (mocks only)
            :type type
            :settings {"allowed-grants" allowed-grants
                       "redirections" [redirect-uri]
                       "logout-redirections" [redirect-uri]}})
    {:id client-id
     (id/key) client-entity-id
     :secret client-secret}))

(defn setup-test-user!
  "Setup a test user in the *resource-owners* atom.

  Returns the user map with ID key."
  [{:keys [username password email given-name family-name]
    :or {email "test@example.com"
         given-name "Test"
         family-name "User"}}]
  (let [user-entity-id (id/data :test/helper-user)]
    (swap! oauth-core/*resource-owners* assoc username
           {:name username
            (id/key) user-entity-id
            :password (hashers/derive password)
            :active true
            :person_info {:email email
                          :given_name given-name
                          :family_name family-name}})
    {:username username
     (id/key) user-entity-id
     :password password}))

;;; ============================================================================
;;; Request/Response Helpers
;;; ============================================================================

(defn request
  "Create a Ring request map for testing.

  Example:
    (request :get \"/oauth/authorize\" {:client_id \"foo\"})"
  ([method uri] (request method uri nil))
  ([method uri params]
   {:request-method method
    :uri uri
    :scheme :https
    :params (or params {})
    :headers {"host" "localhost"}
    :cookies {}}))

(defn parse-json-body
  "Parse JSON response body to Clojure map."
  [response]
  (when-let [body (:body response)]
    (json/read-str body)))

(defn parse-location-params
  "Parse query parameters from Location header.

  Handles both full URLs and path-only locations.

  Example:
    (parse-location-params \"https://example.com/callback?code=ABC&state=XYZ\")
    ;; => {:code \"ABC\" :state \"XYZ\"}
    (parse-location-params \"/callback?code=ABC&state=XYZ\")
    ;; => {:code \"ABC\" :state \"XYZ\"}"
  [location]
  (when location
    ;; Find the last ? to handle both full URLs and paths
    (when-let [query-start (.indexOf location "?")]
      (when (>= query-start 0)
        (let [query (subs location (inc query-start))]
          (into {}
                (map (fn [param]
                       (let [[k v] (str/split param #"=" 2)]
                         ;; Use keyword with explicit name to avoid namespace interpretation
                         [(keyword (str k)) (when v (codec/url-decode v))]))
                     (str/split query #"&"))))))))

;;; ============================================================================
;;; Common OAuth Test Scenarios
;;; ============================================================================

(defn get-authorization-code
  "Complete authorization flow and return authorization code.

  This is a common scenario needed by many tests."
  [client-id redirect-uri username password]
  (let [;; Step 1: Request authorization
        auth-response (handlers/authorize
                        (request :get "/oauth/authorize"
                                 {:client_id client-id
                                  :redirect_uri redirect-uri
                                  :response_type "code"
                                  :scope "openid profile offline_access"
                                  :state "test"}))

        auth-location (get-in auth-response [:headers "Location"])
        login-params (parse-location-params auth-location)

        ;; Step 2: Login
        login-response (handlers/login
                         (-> (request :post "/oauth/login")
                             (assoc :params {:username username
                                             :password password
                                             :state (:state login-params)})
                             (assoc :form-params {:username username
                                                  :password password
                                                  :state (:state login-params)})))

        login-location (get-in login-response [:headers "Location"])
        callback-params (parse-location-params login-location)]

    (:code callback-params)))

(defn get-tokens
  "Complete full authorization flow and return tokens.

  Returns map with :access_token and :refresh_token."
  [client-id client-secret redirect-uri username password]
  (let [code (get-authorization-code client-id redirect-uri username password)

        ;; Step 3: Exchange code for tokens
        token-response (handlers/token
                         (request :post "/oauth/token"
                                  {:grant_type "authorization_code"
                                   :code code
                                   :client_id client-id
                                   :client_secret client-secret
                                   :redirect_uri redirect-uri}))

        token-data (parse-json-body token-response)]

    token-data))
