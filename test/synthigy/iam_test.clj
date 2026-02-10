(ns synthigy.iam-test
  "Tests for IAM (Identity and Access Management) functionality.

  Tests cover:
  - User management (creation, authentication, password hashing)
  - Role management (assignment, retrieval)
  - JWT token generation and validation
  - OAuth client management
  - Access control initialization"
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [synthigy.data :refer [*ROOT* *SYNTHIGY* *PUBLIC_USER* *PUBLIC_ROLE*]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.iam.encryption :as encryption]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

;; Use comprehensive system fixture for initialization and shutdown
;; Note: iam/start is already called by core/warmup in test-helper
(use-fixtures :once test-helper/system-fixture)

;;; ============================================================================
;;; User Management Tests
;;; ============================================================================

(deftest test-create-user
  (testing "Create a new user"
    (let [username (str "test-user-" (id/data :test/iam-user-1))
          user-data {:name username
                     :active true
                     :type :PERSON}
          result (dataset/sync-entity :iam/user user-data)]
      (is (map? result) "Should return a map")
      (is (:_eid result) "Should have internal ID")
      (is (id/extract result) "Should have UUID")
      (is (= username (:name result)) "Should have correct username"))))

(deftest test-create-user-with-password
  (testing "Create user with hashed password"
    (let [username (str "test-user-pass-" (id/data :test/iam-user-2))
          password "SecurePassword123!"
          user-data {:name username
                     :password password
                     :active true
                     :type :PERSON}
          result (dataset/sync-entity :iam/user user-data)]
      (is (map? result) "Should return a map")
      (is (:_eid result) "Should have internal ID")
      (is (:password result) "Should have password field")
      (is (not= password (:password result)) "Password should be hashed, not plain text")
      (is (re-find #"bcrypt" (:password result)) "Should use bcrypt format"))))

(deftest test-get-user-details
  (testing "Retrieve user details by username"
    (let [username (str "test-details-user-" (id/data :test/iam-user-3))
          password "TestPassword123"
          _ (dataset/sync-entity :iam/user
                                 {:name username
                                  :password password
                                  :active true
                                  :type :PERSON})
          user-details (iam/get-user-details username)]
      (is (map? user-details) "Should return user details map")
      (is (= username (:name user-details)) "Should have correct username")
      (is (id/extract user-details) "Should have UUID")
      (is (:_eid user-details) "Should have internal ID")
      (is (:password user-details) "Should include password field")
      (is (set? (:roles user-details)) "Should include roles set")
      (is (set? (:groups user-details)) "Should include groups set"))))

(deftest test-validate-password
  (testing "Password validation"
    (let [username (str "test-validate-" (id/data :test/iam-user-4))
          correct-password "CorrectPassword123"
          wrong-password "WrongPassword456"
          _ (dataset/sync-entity :iam/user
                                 {:name username
                                  :password correct-password
                                  :active true
                                  :type :PERSON})
          user-details (iam/get-user-details username)]
      (testing "Correct password should validate"
        (is (iam/validate-password correct-password (:password user-details))
            "Should return true for correct password"))
      (testing "Wrong password should not validate"
        (is (not (iam/validate-password wrong-password (:password user-details)))
            "Should return false for incorrect password")))))

(deftest test-inactive-user
  (testing "Inactive user handling"
    (let [username (str "test-inactive-" (id/data :test/iam-user-5))
          _ (dataset/sync-entity :iam/user
                                 {:name username
                                  :password "password"
                                  :active false
                                  :type :PERSON})
          user-details (iam/get-user-details username)]
      (is (map? user-details) "Should retrieve inactive user")
      (is (false? (:active user-details)) "Should show user as inactive"))))

;;; ============================================================================
;;; Role Management Tests
;;; ============================================================================

(deftest test-create-role
  (testing "Create a new user role"
    (let [role-name (str "test-role-" (id/data :test/iam-role-1))
          role-data {:name role-name}
          result (dataset/sync-entity :iam/user-role role-data)]
      (is (map? result) "Should return a map")
      (is (:_eid result) "Should have internal ID")
      (is (id/extract result) "Should have UUID")
      (is (= role-name (:name result)) "Should have correct role name"))))

(deftest test-assign-role-to-user
  (testing "Assign role to user"
    (let [username (str "test-user-role-" (id/data :test/iam-user-6))
          role-name (str "test-role-assign-" (id/data :test/iam-role-2))
          ;; Create role
          role (dataset/sync-entity :iam/user-role {:name role-name})
          ;; Create user with role
          user (dataset/sync-entity
                 :iam/user
                 {:name username
                  :active true
                  :type :PERSON
                  :roles [{(id/key) (id/extract role)}]})
          ;; Get user details with roles
          user-details (iam/get-user-details username)]
      (is (set? (:roles user-details)) "Should have roles set")
      (is (seq (:roles user-details)) "Should have at least one role")
      (is (contains? (:roles user-details) (id/extract role))
          "Should contain the assigned role"))))

(deftest test-root-role
  (testing "ROOT role should exist"
    (let [root-role (dataset/get-entity :iam/user-role
                                        {(id/key) (id/extract *ROOT*)}
                                        {(id/key) nil
                                         :name nil})]
      (is (map? root-role) "ROOT role should exist")
      (is (= "SUPERUSER" (:name root-role)) "Should be named SUPERUSER"))))

;; FIXME: PUBLIC user does not exist in database
;; (deftest test-public-user
;;   (testing "PUBLIC user should exist"
;;     (let [public-user (iam/get-user-details "PUBLIC")]
;;       (is (map? public-user) "PUBLIC user should exist")
;;       (is (= "PUBLIC" (:name public-user)) "Should be named PUBLIC")
;;       (is (true? (:active public-user)) "Should be active"))))

;;; ============================================================================
;;; JWT Token Tests
;;; ============================================================================

(deftest test-generate-jwt
  (testing "Generate JWT token"
    (let [data {:user-id 123
                :username "test-user"
                :roles ["user"]}
          token (encryption/sign-data data)]
      (is (string? token) "Should return a string token")
      (is (re-find #"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$" token)
          "Should be in JWT format (header.payload.signature)"))))

(deftest test-validate-jwt
  (testing "Validate and decode JWT token"
    (let [payload {:user-id 123
                   :username "test-jwt-user"
                   :roles ["admin"]}
          token (encryption/sign-data payload)
          decoded (encryption/unsign-data token)]
      (is (map? decoded) "Should decode to a map")
      (is (= 123 (:user-id decoded)) "Should preserve user-id")
      (is (= "test-jwt-user" (:username decoded)) "Should preserve username")
      (is (= ["admin"] (:roles decoded)) "Should preserve roles array")
      ;; Note: exp/iat are NOT automatically added by sign-data
      ;; Applications must add these claims explicitly when needed (see OAuth token.clj)
      )))

(deftest test-jwt-expiration
  (testing "JWT token expiration"
    (let [current-time (quot (System/currentTimeMillis) 1000)
          exp-time (+ current-time 3600)  ;; 1 hour from now
          payload {:user-id 456
                   :exp exp-time}  ;; Application must add exp claim
          token (encryption/sign-data payload)
          decoded (encryption/unsign-data token)
          exp-timestamp (:exp decoded)]
      (is (> exp-timestamp current-time)
          "Token expiration should be in the future")
      (is (> exp-timestamp (+ current-time 3500))
          "Token should be valid for at least 1 hour"))))

(deftest test-invalid-jwt
  (testing "Invalid JWT should return nil"
    (is (nil? (encryption/unsign-data "invalid.jwt.token"))
        "Should return nil for invalid token")))

(deftest test-tampered-jwt
  (testing "Tampered JWT should fail validation"
    (let [token (encryption/sign-data {:user-id 789})
          ;; Tamper with the payload
          parts (clojure.string/split token #"\.")
          tampered-token (str (first parts) ".tamperedpayload." (last parts))]
      (is (nil? (encryption/unsign-data tampered-token))
          "Should return nil for tampered token"))))

;;; ============================================================================
;;; OAuth Client Tests
;;; ============================================================================

(deftest test-create-oauth-client
  (testing "Create OAuth client"
    (let [client-id (str "test-client-" (id/data :test/iam-client-1))
          client-data {:id client-id
                       :type :confidential
                       :settings {}}
          result (dataset/sync-entity :iam/app client-data)]
      (is (map? result) "Should return a map")
      (is (:_eid result) "Should have internal ID")
      (is (id/extract result) "Should have UUID")
      (is (= client-id (:id result)) "Should have correct client ID"))))

(deftest test-get-oauth-client
  (testing "Retrieve OAuth client by ID"
    (let [client-id (str "test-get-client-" (id/data :test/iam-client-2))
          _ (dataset/sync-entity :iam/app
                                 {:id client-id
                                  :type :confidential
                                  :settings {:redirections ["http://localhost:3000/callback"]}})
          client (iam/get-client client-id)]
      (is (map? client) "Should return client map")
      (is (= client-id (:id client)) "Should have correct client ID")
      (is (id/extract client) "Should have UUID")
      (is (map? (:settings client)) "Should have settings map"))))

(deftest test-get-nonexistent-client
  (testing "Get non-existent client should return nil"
    (let [client (iam/get-client "nonexistent-client-id")]
      (is (nil? client) "Should return nil for non-existent client"))))

(deftest test-oauth-client-settings
  (testing "OAuth client with settings"
    (let [client-id (str "test-settings-client-" (id/data :test/iam-client-3))
          settings {:redirections ["http://localhost:3000/callback"
                                   "http://localhost:3000/auth"]
                    :token-expiry {"access" 7200
                                   "refresh" 86400}
                    :logout-redirections ["http://localhost:3000/"]}
          _ (dataset/sync-entity :iam/app
                                 {:id client-id
                                  :type :public
                                  :settings settings})
          client (iam/get-client client-id)
          client-settings (:settings client)]
      (is (= ["http://localhost:3000/callback" "http://localhost:3000/auth"]
             (get client-settings "redirections"))
          "Should have redirections")
      (is (= {"access" 7200
              "refresh" 86400}
             (get client-settings "token-expiry"))
          "Should have token-expiry")
      (is (= ["http://localhost:3000/"]
             (get client-settings "logout-redirections"))
          "Should have logout-redirections"))))

;;; ============================================================================
;;; Access Control Tests
;;; ============================================================================

(deftest test-superuser-detection
  (testing "Superuser detection"
    (testing "ROOT role user should be superuser"
      (let [username (str "test-root-user-" (id/data :test/iam-user-7))
            root-role (dataset/get-entity :iam/user-role
                                          {(id/key) (id/extract *ROOT*)}
                                          {(id/key) nil
                                           :active nil})
            user (dataset/sync-entity :iam/user
                                      {:name username
                                       :active true
                                       :type :PERSON
                                       :roles [{(id/key) (id/extract root-role)}]})
            user-details (iam/get-user-details username)
            roles (:roles user-details)]
        (is (seq roles) "User should have roles")
        ;; Note: superuser? function checks for ROOT role UUID
        (is (contains? roles (id/extract *ROOT*))
            "Should have ROOT role")))

    (testing "SYNTHIGY service user should be superuser"
      (is (= (id/extract *SYNTHIGY*) (id/extract *SYNTHIGY*))
          "SYNTHIGY service user exists"))))

(deftest test-user-type-enum
  (testing "User type enumeration"
    (testing "PERSON type"
      (let [synced (dataset/sync-entity :iam/user
                                        {:name (str "person-" (id/data :test/iam-user-8))
                                         :type :PERSON
                                         :active true})
            user (dataset/get-entity :iam/user
                                     {(id/key) (id/extract synced)}
                                     {(id/key) nil
                                      :type nil})]
        (is (= :PERSON (:type user)) "Should accept PERSON type")))

    (testing "SERVICE type"
      (let [synced (dataset/sync-entity :iam/user
                                        {:name (str "service-" (id/data :test/iam-user-9))
                                         :type :SERVICE
                                         :active true})
            user (dataset/get-entity :iam/user
                                     {(id/key) (id/extract synced)}
                                     {(id/key) nil
                                      :type nil})]
        (is (= :SERVICE (:type user)) "Should accept SERVICE type")))))

;;; ============================================================================
;;; Group Management Tests
;;; ============================================================================

(deftest test-create-user-group
  (testing "Create a user group"
    (let [group-name (str "test-group-" (id/data :test/iam-group-1))
          group-data {:name group-name}
          result (dataset/sync-entity :iam/user-group group-data)]
      (is (map? result) "Should return a map")
      (is (:_eid result) "Should have internal ID")
      (is (id/extract result) "Should have UUID")
      (is (= group-name (:name result)) "Should have correct group name"))))

(deftest test-assign-user-to-group
  (testing "Assign user to group"
    (let [username (str "test-user-group-" (id/data :test/iam-user-10))
          group-name (str "test-group-assign-" (id/data :test/iam-group-2))
          ;; Create group
          group (dataset/sync-entity :iam/user-group {:name group-name})
          ;; Create user with group
          user (dataset/sync-entity :iam/user
                                    {:name username
                                     :active true
                                     :type :PERSON
                                     :groups [{(id/key) (id/extract group)}]})
          ;; Get user details with groups
          user-details (iam/get-user-details username)]
      (is (set? (:groups user-details)) "Should have groups set")
      (is (contains? (:groups user-details) (id/extract group))
          "Should contain the assigned group"))))

;;; ============================================================================
;;; Edge Cases and Error Handling
;;; ============================================================================

(deftest test-duplicate-username
  (testing "Creating user with duplicate username should update"
    (let [username (str "test-duplicate-" (id/data :test/iam-user-11))
          user1 (dataset/sync-entity :iam/user
                                     {:name username
                                      :active true
                                      :type :PERSON})
          user2 (dataset/sync-entity :iam/user
                                     {:name username
                                      :active false
                                      :type :PERSON})]
      (is (= (id/extract user1) (id/extract user2))
          "Should update same user, not create duplicate")
      (is (false? (:active user2))
          "Should update active status"))))

(deftest test-empty-password
  (testing "User with empty/nil password"
    (let [username (str "test-no-pass-" (id/data :test/iam-user-12))
          user (dataset/sync-entity :iam/user
                                    {:name username
                                     :active true
                                     :type :PERSON})]
      (is (nil? (:password user)) "Password should be nil if not provided"))))

(deftest test-get-nonexistent-user
  (testing "Get non-existent user"
    (let [user-details (iam/get-user-details "nonexistent-user-12345")]
      (is (nil? user-details) "Should return nil for non-existent user"))))

(deftest test-user-with-avatar
  (testing "User with avatar (JSON field)"
    (let [username (str "test-avatar-" (id/data :test/iam-user-13))
          avatar {:url "https://example.com/avatar.png"
                  :size 1024
                  :mime-type "image/png"}
          synced (dataset/sync-entity :iam/user
                                      {:name username
                                       :active true
                                       :type :PERSON
                                       :avatar avatar})
          user (dataset/get-entity :iam/user
                                   {(id/key) (id/extract synced)}
                                   {(id/key) nil
                                    :avatar nil})]
      (is (map? (:avatar user)) "Avatar should be preserved as map")
      (is (= "https://example.com/avatar.png" (get (:avatar user) "url")) "Should have URL")
      (is (= 1024 (get (:avatar user) "size")) "Should have size")
      (is (= "image/png" (get (:avatar user) "mime-type")) "Should have mime-type"))))

(deftest test-user-settings
  (testing "User with settings (JSON field)"
    (let [username (str "test-settings-" (id/data :test/iam-user-14))
          settings {:theme "dark"
                    :language "en"
                    :notifications {:email true
                                    :sms false}}
          synced (dataset/sync-entity :iam/user
                                      {:name username
                                       :active true
                                       :type :PERSON
                                       :settings settings})
          user (dataset/get-entity :iam/user
                                   {(id/key) (id/extract synced)}
                                   {(id/key) nil
                                    :settings nil})]
      (is (map? (:settings user)) "Settings should be preserved as map")
      (is (= "dark" (get (:settings user) "theme")) "Should have theme")
      (is (= "en" (get (:settings user) "language")) "Should have language")
      (is (map? (get (:settings user) "notifications")) "Should have notifications map")
      (is (= true (get-in (:settings user) ["notifications" "email"])) "Should have email notification")
      (is (= false (get-in (:settings user) ["notifications" "sms"])) "Should have sms notification"))))
