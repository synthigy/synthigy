(ns synthigy.oauth.client-grant-test
  "Tests for client_credentials grant with audience and scope filtering.

  Validates:
  1. Scope intersection: role_scopes ∩ api_scopes ∩ requested_scopes
  2. Audience validation: client must be linked to requested API
  3. Token claims: clean JWT without session pollution
  4. Unauthorized scope filtering
  5. Service user resolution"
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.string :as str]
   [buddy.hashers :as hashers]
   [synthigy.json :as json]
   [synthigy.dataset.core :as dataset.core]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.data :refer [*SYNTHIGY*]]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access :refer [*user*]]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.handlers :as handlers]
   [synthigy.oauth.core :as oauth-core]
   [synthigy.oauth.token :as token-ns]
   [synthigy.server.auth :as auth]
   [synthigy.test-data]
   [synthigy.test-helper :as test-helper]))

;; =============================================================================
;; Constants
;; =============================================================================

(def ^:private test-client-id "test-client-grant")
(def ^:private test-client-secret "test-cg-secret-42")
(def ^:private test-api-audience "test-robotics")

;; =============================================================================
;; Test Data Setup & Cleanup
;; =============================================================================

(defn- create-test-data!
  "Creates API, scopes, role, client, and service user in the database.
   Returns a map of created entities for reference."
  []
  (binding [*user* (:_eid *SYNTHIGY*)]
    ;; 1. Create API with scopes
    (let [api (dataset/sync-entity
               :iam/api
               {:euuid (id/data :test/client-grant-api)
                :name "Test Robotics API"
                :audience test-api-audience
                :description "Test API for client grant tests"
                :scopes [{:name "test:enqueue"
                          :description "Enqueue tasks"}
                         {:name "test:status"
                          :description "View task status"}
                         {:name "test:admin"
                          :description "Admin operations"}]})
          ;; Fetch back scopes with IDs
          api-with-scopes (dataset/get-entity
                           :iam/api
                           {(id/key) (id/extract api)}
                           {:scopes [{:selections {(id/key) nil :name nil}}]})
          scope-ids (mapv #(hash-map (id/key) (id/extract %))
                          (:scopes api-with-scopes))
          enqueue-scope (first (filter #(= "test:enqueue" (:name %))
                                       (:scopes api-with-scopes)))
          status-scope (first (filter #(= "test:status" (:name %))
                                      (:scopes api-with-scopes)))]

      ;; 2. Create role linked to only enqueue + status scopes (NOT admin)
      (let [role (dataset/sync-entity
                  :iam/user-role
                  {:euuid (id/data :test/client-grant-role)
                   :name "Test: Client Grant Operator"
                   :active true
                   :scopes [{(id/key) (id/extract enqueue-scope)}
                            {(id/key) (id/extract status-scope)}]})]

        ;; 3. Create confidential client linked to the API
        (let [client (dataset/sync-entity
                      :iam/app
                      {:euuid (id/data :test/client-grant-client)
                       :id test-client-id
                       :name "Test Client Grant Service"
                       :type :confidential
                       :active true
                       :secret test-client-secret
                       :settings {"allowed-grants" ["client_credentials"]}
                       :apis [{(id/key) (id/extract api)}]})]

          ;; 4. Create service user with the role
          (let [service-user (dataset/sync-entity
                              :iam/user
                              {:euuid (id/data :test/client-grant-service-user)
                               :name test-client-id
                               :type :SERVICE
                               :active true
                               :roles [{(id/key) (id/extract role)}]})]

            ;; 5. Reload scope cache so new role scopes are visible
            (access/load-scopes)

            {:api api
             :role role
             :client client
             :service-user service-user}))))))

(defn- cleanup-test-data!
  "Removes all test entities from the database."
  []
  (binding [*user* (:_eid *SYNTHIGY*)]
    ;; Delete in reverse dependency order
    (doseq [[entity euuid-key]
            [[:iam/user :test/client-grant-service-user]
             [:iam/app :test/client-grant-client]
             [:iam/user-role :test/client-grant-role]
             [:iam/api :test/client-grant-api]]]
      (try
        (dataset/delete-entity entity {(id/key) (id/data euuid-key)})
        (catch Exception _)))
    ;; Reload scope cache after cleanup
    (access/load-scopes)))

;; =============================================================================
;; Fixtures
;; =============================================================================

(use-fixtures :once test-helper/system-fixture)

(defn client-grant-fixture [f]
  (test-helper/reset-oauth-state!)
  (create-test-data!)
  (try
    (f)
    (finally
      (cleanup-test-data!)
      (test-helper/reset-oauth-state!))))

(use-fixtures :each client-grant-fixture)

;; =============================================================================
;; Helpers
;; =============================================================================

(defn- token-request
  "Build a Ring request for the token endpoint."
  [params]
  {:request-method :post
   :uri "/oauth/token"
   :scheme :https
   :params (merge {:grant_type "client_credentials"
                   :client_id test-client-id
                   :client_secret test-client-secret}
                  params)
   :headers {"host" "localhost"}
   :cookies {}})

(defn- parse-body [response]
  (some-> response :body (json/read-str)))

(defn- decode-token-claims
  "Decode JWT access token claims without verification."
  [jwt]
  (let [[_ payload] (str/split jwt #"\.")
        decoded (String. (.decode (java.util.Base64/getUrlDecoder) payload))]
    (json/read-str decoded)))

;; =============================================================================
;; Tests
;; =============================================================================

(deftest test-audience-scope-intersection
  (testing "Token scopes are intersection of role_scopes ∩ api_scopes ∩ requested"
    (let [response (handlers/token
                    (token-request {:scope "test:enqueue"
                                   :audience test-api-audience}))
          body (parse-body response)]
      (is (= 200 (:status response)))
      (is (= "test:enqueue" (:scope body)))
      (is (= "Bearer" (:token_type body))))))

(deftest test-multiple-scopes-granted
  (testing "Multiple valid scopes are granted when role and API allow"
    (let [response (handlers/token
                    (token-request {:scope "test:enqueue test:status"
                                   :audience test-api-audience}))
          body (parse-body response)
          granted (set (str/split (:scope body) #" "))]
      (is (= 200 (:status response)))
      (is (contains? granted "test:enqueue"))
      (is (contains? granted "test:status")))))

(deftest test-unauthorized-scope-filtered
  (testing "Scope not in role is filtered out"
    (let [response (handlers/token
                    (token-request {:scope "test:admin"
                                   :audience test-api-audience}))
          body (parse-body response)]
      (is (= 200 (:status response)))
      (is (= "" (:scope body))
          "test:admin should be filtered — role only grants enqueue and status"))))

(deftest test-partial-scope-filtering
  (testing "Mix of valid and invalid scopes — only valid ones granted"
    (let [response (handlers/token
                    (token-request {:scope "test:enqueue test:admin"
                                   :audience test-api-audience}))
          body (parse-body response)
          granted (set (str/split (:scope body) #" "))]
      (is (= 200 (:status response)))
      (is (contains? granted "test:enqueue"))
      (is (not (contains? granted "test:admin"))))))

(deftest test-unauthorized-audience
  (testing "Client not linked to requested audience returns 403"
    (let [response (handlers/token
                    (token-request {:scope "test:enqueue"
                                   :audience "unknown-api"}))
          body (parse-body response)]
      (is (= 403 (:status response)))
      (is (= "invalid_target" (:error body))))))

(deftest test-token-claims-no-session
  (testing "Client credentials token has no session/sid claims"
    (let [response (handlers/token
                    (token-request {:scope "test:enqueue"
                                   :audience test-api-audience}))
          body (parse-body response)
          claims (decode-token-claims (:access_token body))]
      (is (= test-api-audience (:aud claims)))
      (is (= test-client-id (:sub claims)))
      (is (= test-client-id (:client_id claims)))
      (is (nil? (:session claims)) "No session in client credentials token")
      (is (nil? (:sid claims)) "No sid in client credentials token"))))

(deftest test-no-audience-returns-all-role-scopes
  (testing "Without audience, token gets all role scopes (no API filtering)"
    (let [response (handlers/token
                    (token-request {}))
          body (parse-body response)
          granted (when-not (str/blank? (:scope body))
                    (set (str/split (:scope body) #" ")))]
      (is (= 200 (:status response)))
      (is (contains? granted "test:enqueue"))
      (is (contains? granted "test:status"))
      (is (not (contains? granted "test:admin"))
          "test:admin not in role scopes regardless of API"))))

(deftest test-invalid-client-secret
  (testing "Wrong secret is rejected"
    (let [response (handlers/token
                    (token-request {:client_secret "wrong-secret"
                                   :audience test-api-audience}))
          body (parse-body response)]
      (is (= 401 (:status response)))
      (is (= "invalid_client" (:error body))))))

(deftest test-audience-validation-on-receive
  (testing "Receiving service validates audience claim"
    (let [response (handlers/token
                    (token-request {:scope "test:enqueue"
                                   :audience test-api-audience}))
          body (parse-body response)
          access-token (:access_token body)]
      ;; Matching audience — returns user context
      (is (some? (auth/token->user-context access-token :audience test-api-audience))
          "Correct audience should resolve user context")
      ;; Mismatched audience — returns nil
      (is (nil? (auth/token->user-context access-token :audience "wrong-api"))
          "Wrong audience should reject token")
      ;; No audience check — backward compatible
      (is (some? (auth/token->user-context access-token))
          "No audience param should accept any token"))))

(deftest test-add-client-hashes-secret
  (testing "add-client returns raw secret, DB stores bcrypt hash"
    (let [client-id "test-add-client-hash"
          client (binding [*user* (:_eid *SYNTHIGY*)]
                   (iam/add-client {:id client-id
                                    :name "Test Add Client Hash"
                                    :type :confidential
                                    :settings {"allowed-grants" ["client_credentials"]}}))
          raw-secret (:secret client)]
      (try
        ;; Raw secret is returned (not hashed)
        (is (some? raw-secret) "Should return a secret")
        (is (not (str/starts-with? raw-secret "bcrypt"))
            "Returned secret should be raw, not hashed")
        ;; DB stores hashed version
        (let [stored (binding [*user* (:_eid *SYNTHIGY*)]
                       (dataset/get-entity :iam/app {:id client-id} {:secret nil}))]
          (is (str/starts-with? (:secret stored) "bcrypt")
              "Stored secret should be bcrypt hash")
          ;; hashers/check validates raw against stored hash
          (is (hashers/check raw-secret (:secret stored))
              "Raw secret should verify against stored hash"))
        ;; Token grant works with raw secret
        (let [response (handlers/token
                        (token-request {:client_id client-id
                                        :client_secret raw-secret}))]
          (is (= 200 (:status response))
              "Token grant should succeed with raw secret"))
        (finally
          (binding [*user* (:_eid *SYNTHIGY*)]
            (dataset/delete-entity :iam/user {:name client-id})
            (dataset/delete-entity :iam/app {:id client-id})))))))
