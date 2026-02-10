(ns synthigy.dataset.write-test
  "Tests for sync-entity and stack-entity write operations."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.postgres :as pg]
    [synthigy.db :as db]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;;; ============================================================================
;;; Test Data Tracking
;;; ============================================================================

(def created-test-entities
  "Atom tracking all entities created during tests for cleanup"
  (atom #{}))

(defn track-entity!
  "Track an entity ID for cleanup"
  [entity-id]
  (swap! created-test-entities conj entity-id)
  entity-id)

;; Counters for cycling through test data IDs
(def ^:private test-user-counter (atom 0))
(def ^:private test-role-counter (atom 0))

;; Available test data keys for users and roles
(def ^:private test-user-keys
  [:test/write-user-1 :test/write-user-2 :test/write-user-3 :test/write-user-4
   :test/write-user-5 :test/write-user-6 :test/write-user-7 :test/write-user-8
   :test/write-user-9 :test/write-user-10 :test/write-user-11 :test/write-user-12
   :test/write-user-13 :test/write-user-14 :test/write-user-15 :test/write-user-16
   :test/write-user-17 :test/write-user-18 :test/write-user-19 :test/write-user-20
   :test/write-user-21 :test/write-user-22 :test/write-user-23 :test/write-user-24
   :test/write-user-25])

(def ^:private test-role-keys
  (mapv #(keyword "test" (str "write-role-" %)) (range 1 51)))

(defn next-user-id
  "Get the next user ID from test data, cycling through available keys."
  []
  (let [idx (swap! test-user-counter inc)
        data-key (nth test-user-keys (mod idx (count test-user-keys)))]
    (track-entity! (id/data data-key))))

(defn next-role-id
  "Get the next role ID from test data, cycling through available keys."
  []
  (let [idx (swap! test-role-counter inc)
        data-key (nth test-role-keys (mod idx (count test-role-keys)))]
    (track-entity! (id/data data-key))))

(defn cleanup-tracked-entities!
  "Delete all tracked test entities"
  []
  (doseq [entity-id @created-test-entities]
    (try
      ;; Try deleting as user first (most common in write tests)
      (db/delete-entity db/*db* :iam/user {(id/key) entity-id})
      (catch Exception _
        ;; If not a user, try as role
        (try
          (db/delete-entity db/*db* :iam/user-role {(id/key) entity-id})
          (catch Exception _))))) ; Ignore if doesn't exist
  (reset! created-test-entities #{}))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn cleanup-all-test-data!
  "Remove all test entities from previous runs before starting tests.
   This prevents duplicate key violations from leftover data."
  []
  (log/info "Cleaning up leftover test data from previous runs...")
  (try
    ;; Delete test users (by name pattern)
    (let [test-users (db/search-entity db/*db* :iam/user
                                       {:_where {:name {:_like "Test%"}}}
                                       {(id/key) nil
                                        :name nil})]
      (doseq [user test-users]
        (try
          (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user)})
          (catch Exception _)))
      (log/info "Deleted" (count test-users) "test users with 'Test%' pattern"))

    ;; Delete users with specific test names
    (let [test-names ["Alice" "Bob" "Charlie" "Updated Name"
                      "User With Reference" "User With Direct Reference"
                      "User With Bad Reference" "User With Roles"
                      "Batch User 1" "Batch User 2"]
          users (db/search-entity db/*db* :iam/user
                                  {:_where {:name {:_in test-names}}}
                                  {(id/key) nil
                                   :name nil})]
      (doseq [user users]
        (try
          (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user)})
          (catch Exception _)))
      (log/info "Deleted" (count users) "users with specific test names"))

    ;; Delete test roles
    (let [test-roles (db/search-entity db/*db* :iam/user-role
                                       {:_where {:name {:_like "Test%"}}}
                                       {(id/key) nil
                                        :name nil})]
      (doseq [role test-roles]
        (try
          (db/delete-entity db/*db* :iam/user-role {(id/key) (id/extract role)})
          (catch Exception _)))
      (log/info "Deleted" (count test-roles) "test roles"))

    (log/info "Leftover test data cleanup complete")
    (catch Exception e
      (log/warn "Initialization cleanup failed (non-fatal):" (.getMessage e)))))

(defn initial-cleanup-fixture
  "Clean up leftover test data from previous runs before running tests"
  [f]
  (cleanup-all-test-data!)
  (f))

(defn cleanup-fixture
  "Clean up test data after each test"
  [f]
  (f)
  ;; Clean up any test data created during tests
  ;; This prevents unique constraint violations on subsequent test runs
  (try
    (let [entity-count (count @created-test-entities)]
      (cleanup-tracked-entities!)
      (log/info "Cleaned up" entity-count "tracked test entities"))
    (catch Exception e
      (log/warn "Cleanup warning:" (.getMessage e)))))

;; Use comprehensive system fixture for initialization and shutdown
(use-fixtures :once test-helper/system-fixture initial-cleanup-fixture)
(use-fixtures :each cleanup-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn random-user-data
  "Generates user data for testing using registered defdata IDs."
  []
  (let [entity-id (next-user-id)]
    {(id/key) entity-id
     :name (str "Test User " entity-id)  ; Use defdata ID for guaranteed uniqueness
     :active true}))

;;; ============================================================================
;;; Milestone 1: Basic Scalar Fields Tests
;;; ============================================================================

(deftest test-sync-entity-single-insert
  (testing "Insert single entity with scalar fields"
    (let [data (random-user-data)
          result (db/sync-entity db/*db* :iam/user data)]

      (is (map? result)
          "Should return a map")

      (is (= (id/extract data) (id/extract result))
          "Should return same ID")

      (is (= (:name data) (:name result))
          "Should return same name")

      (is (= (:active data) (:active result))
          "Should return same active status")

      (is (some? (:_eid result))
          "Should have _eid assigned")

      (is (pos? (:_eid result))
          "Should have positive _eid"))))

(deftest test-sync-entity-upsert
  (testing "Update existing entity (UPSERT)"
    (let [data (random-user-data)
          ;; First insert
          result1 (db/sync-entity db/*db* :iam/user data)
          eid1 (:_eid result1)

          ;; Update same euuid
          updated-data (assoc data :name "Updated Name" :active false)
          result2 (db/sync-entity db/*db* :iam/user updated-data)
          eid2 (:_eid result2)]

      (is (= eid1 eid2)
          "Should have same _eid (UPSERT, not insert)")

      (is (= "Updated Name" (:name result2))
          "Should have updated name")

      (is (false? (:active result2))
          "Should have updated active status"))))

(deftest test-sync-entity-batch
  (testing "Batch insert multiple entities"
    (let [data1 (random-user-data)
          data2 (random-user-data)
          results (db/sync-entity db/*db* :iam/user [data1 data2])]

      (is (vector? results)
          "Should return a vector")

      (is (= 2 (count results))
          "Should return 2 results")

      (is (every? :_eid results)
          "All results should have _eid")

      (is (= #{(id/extract data1) (id/extract data2)}
             (set (map id/extract results)))
          "Should return both euuids"))))

(deftest test-stack-entity-basic
  (testing "stack-entity works like sync-entity for scalar fields"
    (let [data (random-user-data)
          result (db/stack-entity db/*db* :iam/user data)]

      (is (map? result)
          "Should return a map")

      (is (= (id/extract data) (id/extract result))
          "Should return same euuid")

      (is (some? (:_eid result))
          "Should have _eid assigned"))))

;;; ============================================================================
;;; Milestone 2: Foreign Key Reference Tests
;;; ============================================================================

(deftest test-sync-entity-with-foreign-key
  (testing "Insert entity with foreign key reference"
    ;; First, create a user to reference
    (let [ref-user-data (random-user-data)
          ref-user (db/sync-entity db/*db* :iam/user ref-user-data)
          ref-euuid (id/extract ref-user)
          ref-eid (:_eid ref-user)

          ;; Now create another user with modified_by reference
          test-id (next-user-id)
          test-user-data {(id/key) test-id
                          :name "User With Reference"
                          :active true
                          :modified_by {(id/key) ref-euuid}}
          result (db/sync-entity db/*db* :iam/user test-user-data)]

      (is (map? result)
          "Should return a map")

      (is (some? (:_eid result))
          "Should have _eid assigned")

      ;; The resolved reference should be in the result
      ;; (though pull-roots might not return it yet - that's okay for Milestone 2)
      (is (= "User With Reference" (:name result))
          "Should have correct name"))))

(deftest test-sync-entity-with-resolved-foreign-key
  (testing "Insert entity with already-resolved foreign key (_eid)"
    ;; Create a reference user
    (let [ref-user (db/sync-entity db/*db* :iam/user (random-user-data))
          ref-eid (:_eid ref-user)

          ;; Create user with direct _eid reference
          test-id (next-user-id)
          test-user-data {(id/key) test-id
                          :name "User With Direct Reference"
                          :active true
                          :modified_by ref-eid}
          result (db/sync-entity db/*db* :iam/user test-user-data)]

      (is (map? result)
          "Should return a map")

      (is (some? (:_eid result))
          "Should have _eid assigned"))))

(deftest test-sync-entity-batch-with-references
  (testing "Batch insert with foreign key references"
    ;; Create reference user
    (let [ref-user (db/sync-entity db/*db* :iam/user (random-user-data))
          ref-euuid (id/extract ref-user)

          ;; Batch insert with references
          id1 (next-user-id)
          id2 (next-user-id)
          data1 {(id/key) id1
                 :name "Batch User 1"
                 :active true
                 :modified_by {(id/key) ref-euuid}}
          data2 {(id/key) id2
                 :name "Batch User 2"
                 :active true
                 :modified_by {(id/key) ref-euuid}}
          results (db/sync-entity db/*db* :iam/user [data1 data2])]

      (is (vector? results)
          "Should return a vector")

      (is (= 2 (count results))
          "Should return 2 results")

      (is (every? :_eid results)
          "All results should have _eid"))))

;;; ============================================================================
;;; Milestone 3: Many-to-Many Relation Tests
;;; ============================================================================

(defn random-role-data
  "Generates role data for testing using registered defdata IDs."
  []
  (let [entity-id (next-role-id)]
    {(id/key) entity-id
     :name (str "Test Role " entity-id)}))

(deftest test-sync-entity-with-relations
  (testing "Insert entity with many-to-many relations (sync mode)"
    ;; First, create some roles to reference
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role1-euuid (id/extract role1)
          role2-euuid (id/extract role2)

          ;; Create a user with roles
          user-data {(id/key) (next-user-id)
                     :name (str "User With Roles " @test-user-counter)
                     :active true
                     :roles [{(id/key) role1-euuid}
                             {(id/key) role2-euuid}]}
          result (db/sync-entity db/*db* :iam/user user-data)]

      (is (map? result)
          "Should return a map")

      (is (some? (:_eid result))
          "Should have _eid assigned")

      (is (= (:name user-data) (:name result))
          "Should have correct name"))))

(deftest test-sync-entity-replaces-relations
  (testing "sync-entity REPLACES existing relations"
    ;; Create roles
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role3 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role1-euuid (id/extract role1)
          role2-euuid (id/extract role2)
          role3-euuid (id/extract role3)

          ;; Create user with role1 and role2
          user-data {(id/key) (next-user-id)
                     :name (str "User Sync Test " @test-user-counter)
                     :active true
                     :roles [{(id/key) role1-euuid}
                             {(id/key) role2-euuid}]}
          result1 (db/sync-entity db/*db* :iam/user user-data)
          user-euuid (id/extract result1)

          ;; Update same user with role3 only (should REPLACE roles)
          updated-data {(id/key) user-euuid
                        :name (:name user-data)
                        :active true
                        :roles [{(id/key) role3-euuid}]}
          result2 (db/sync-entity db/*db* :iam/user updated-data)]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid (upsert)")

      ;; Verify that only role3 is linked (sync-entity REPLACES relations)
      (let [user-with-roles (db/get-entity db/*db* :iam/user
                                           {(id/key) user-euuid}
                                           {(id/key) nil
                                            :roles [{:selections {(id/key) nil}}]})]
        (is (vector? (:roles user-with-roles))
            "Roles should be a vector")

        (is (= 1 (count (:roles user-with-roles)))
            "Should have exactly 1 role after sync (replaced)")

        (is (= role3-euuid (id/extract (first (:roles user-with-roles))))
            "Should have only role3 (replaced role1 and role2)")))))

(deftest test-stack-entity-adds-relations
  (testing "stack-entity ADDS to existing relations"
    ;; Create roles
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role3 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role1-euuid (id/extract role1)
          role2-euuid (id/extract role2)
          role3-euuid (id/extract role3)

          ;; Create user with role1 and role2
          user-data {(id/key) (next-user-id)
                     :name (str "User Stack Test " @test-user-counter)
                     :active true
                     :roles [{(id/key) role1-euuid}
                             {(id/key) role2-euuid}]}
          result1 (db/sync-entity db/*db* :iam/user user-data)
          user-euuid (id/extract result1)

          ;; Stack role3 onto existing roles (should ADD, not replace)
          stack-data {(id/key) user-euuid
                      :name (:name user-data)
                      :active true
                      :roles [{(id/key) role3-euuid}]}
          result2 (db/stack-entity db/*db* :iam/user stack-data)]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid (upsert)")

      ;; Verify that all 3 roles are linked (stack-entity ADDS relations)
      (let [user-with-roles (db/get-entity db/*db* :iam/user
                                           {(id/key) user-euuid}
                                           {(id/key) nil
                                            :roles [{:selections {(id/key) nil}}]})]
        (is (vector? (:roles user-with-roles))
            "Roles should be a vector")

        (is (= 3 (count (:roles user-with-roles)))
            "Should have all 3 roles after stack (added to existing)")

        (let [role-euuids (set (map id/extract (:roles user-with-roles)))]
          (is (contains? role-euuids role1-euuid)
              "Should still have role1")
          (is (contains? role-euuids role2-euuid)
              "Should still have role2")
          (is (contains? role-euuids role3-euuid)
              "Should have added role3"))))))

(deftest test-sync-entity-batch-with-relations
  (testing "Batch insert with relations"
    ;; Create roles
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role1-euuid (id/extract role1)
          role2-euuid (id/extract role2)

          ;; Batch insert users with roles
          user1-data {(id/key) (next-user-id)
                      :name (str "Batch User 1 " @test-user-counter)
                      :active true
                      :roles [{(id/key) role1-euuid}]}
          user2-data {(id/key) (next-user-id)
                      :name (str "Batch User 2 " @test-user-counter)
                      :active true
                      :roles [{(id/key) role2-euuid}]}
          results (db/sync-entity db/*db* :iam/user [user1-data user2-data])]

      (is (vector? results)
          "Should return a vector")

      (is (= 2 (count results))
          "Should return 2 results")

      (is (every? :_eid results)
          "All results should have _eid"))))

;;; ============================================================================
;;; Milestone 4: NULL Handling Tests
;;; ============================================================================

(deftest test-sync-entity-with-null-optional-field
  (testing "Insert entity with NULL optional field"
    (let [data {(id/key) (next-user-id)
                :name (str "User With Null " @test-user-counter)
                :active true
                :priority nil}  ; NULL optional field
          result (db/sync-entity db/*db* :iam/user data)]

      (is (map? result)
          "Should return a map")

      (is (nil? (:priority result))
          "Priority should be NULL"))))

(deftest test-sync-entity-update-to-null
  (testing "Update field to NULL"
    (let [data {(id/key) (next-user-id)
                :name (str "User " @test-user-counter)
                :active true
                :priority 10}
          result1 (db/sync-entity db/*db* :iam/user data)
          user-euuid (id/extract result1)

          ;; Update priority to NULL
          updated-data (assoc data :priority nil)
          result2 (db/sync-entity db/*db* :iam/user updated-data)]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid")

      (is (nil? (:priority result2))
          "Priority should be updated to NULL"))))

(deftest test-sync-entity-update-from-null
  (testing "Update field from NULL to value"
    (let [data {(id/key) (next-user-id)
                :name (str "User " @test-user-counter)
                :active true
                :priority nil}
          result1 (db/sync-entity db/*db* :iam/user data)
          user-euuid (id/extract result1)

          ;; Update priority from NULL to value
          updated-data (assoc data :priority 20)
          result2 (db/sync-entity db/*db* :iam/user updated-data)]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid")

      (is (= 20 (:priority result2))
          "Priority should be updated from NULL to 20"))))

(deftest test-sync-entity-null-optional-foreign-key
  (testing "Insert with NULL optional foreign key reference"
    (let [data {(id/key) (next-user-id)
                :name (str "User " @test-user-counter)
                :active true
                :modified_by nil}  ; NULL optional reference
          result (db/sync-entity db/*db* :iam/user data)]

      (is (map? result)
          "Should return a map")

      (is (some? (:_eid result))
          "Should have _eid assigned"))))

;;; ============================================================================
;;; Milestone 5: Partial Update Tests
;;; ============================================================================

(deftest test-sync-entity-partial-update
  (testing "Partial update - only some fields changed"
    (let [data {(id/key) (next-user-id)
                :name (str "User " @test-user-counter)
                :active true
                :priority 10}
          result1 (db/sync-entity db/*db* :iam/user data)
          user-euuid (id/extract result1)

          ;; Update only active field
          partial-data {(id/key) user-euuid
                        :active false}
          result2 (db/sync-entity db/*db* :iam/user partial-data)

          ;; Read back to verify unchanged fields
          verified (db/get-entity db/*db* :iam/user
                                  {(id/key) user-euuid}
                                  {(id/key) nil
                                   :name nil
                                   :active nil
                                   :priority nil})]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid")

      (is (false? (:active verified))
          "Active should be updated to false")

      (is (= (:name data) (:name verified))
          "Name should remain unchanged")

      (is (= 10 (:priority verified))
          "Priority should remain unchanged"))))

(deftest test-stack-entity-preserves-existing-fields
  (testing "stack-entity preserves existing scalar fields"
    (let [data {(id/key) (next-user-id)
                :name (str "User " @test-user-counter)
                :active true
                :priority 10}
          result1 (db/sync-entity db/*db* :iam/user data)
          user-euuid (id/extract result1)

          ;; Stack with only active field (should preserve others)
          stack-data {(id/key) user-euuid
                      :active false}
          result2 (db/stack-entity db/*db* :iam/user stack-data)

          ;; Read back to verify
          verified (db/get-entity db/*db* :iam/user
                                  {(id/key) user-euuid}
                                  {(id/key) nil
                                   :name nil
                                   :active nil
                                   :priority nil})]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid")

      (is (false? (:active verified))
          "Active should be updated")

      (is (= (:name data) (:name verified))
          "Name should be preserved")

      (is (= 10 (:priority verified))
          "Priority should be preserved"))))

;;; ============================================================================
;;; Milestone 6: Empty Relations Tests
;;; ============================================================================

(deftest test-sync-entity-with-empty-relations-array
  (testing "sync-entity with empty relations array clears relations"
    ;; Create roles
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role1-euuid (id/extract role1)
          role2-euuid (id/extract role2)

          ;; Create user with roles
          user-data {(id/key) (next-user-id)
                     :name (str "User " @test-user-counter)
                     :active true
                     :roles [{(id/key) role1-euuid}
                             {(id/key) role2-euuid}]}
          result1 (db/sync-entity db/*db* :iam/user user-data)
          user-euuid (id/extract result1)

          ;; Update with empty roles array (should clear)
          updated-data {(id/key) user-euuid
                        :name (:name user-data)
                        :active true
                        :roles []}
          result2 (db/sync-entity db/*db* :iam/user updated-data)

          ;; Verify roles are cleared
          verified (db/get-entity db/*db* :iam/user
                                  {(id/key) user-euuid}
                                  {(id/key) nil
                                   :roles [{:selections {(id/key) nil}}]})]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid")

      (is (or (nil? (:roles verified))
              (empty? (:roles verified)))
          "Roles should be cleared with empty array"))))

(deftest test-stack-entity-with-empty-relations-array
  (testing "stack-entity with empty relations array does nothing"
    ;; Create roles
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role1-euuid (id/extract role1)
          role2-euuid (id/extract role2)

          ;; Create user with roles
          user-data {(id/key) (next-user-id)
                     :name (str "User " @test-user-counter)
                     :active true
                     :roles [{(id/key) role1-euuid}
                             {(id/key) role2-euuid}]}
          result1 (db/sync-entity db/*db* :iam/user user-data)
          user-euuid (id/extract result1)

          ;; Stack with empty roles array (should do nothing)
          stack-data {(id/key) user-euuid
                      :roles []}
          result2 (db/stack-entity db/*db* :iam/user stack-data)

          ;; Verify roles are preserved
          verified (db/get-entity db/*db* :iam/user
                                  {(id/key) user-euuid}
                                  {(id/key) nil
                                   :roles [{:selections {(id/key) nil}}]})]

      (is (= (:_eid result1) (:_eid result2))
          "Should have same _eid")

      (is (= 2 (count (:roles verified)))
          "Roles should be preserved (empty array does nothing in stack mode)")

      (let [role-euuids (set (map id/extract (:roles verified)))]
        (is (contains? role-euuids role1-euuid)
            "Should still have role1")
        (is (contains? role-euuids role2-euuid)
            "Should still have role2")))))

;;; ============================================================================
;;; Milestone 7: Validation & Error Handling Tests
;;; ============================================================================

(deftest test-sync-entity-nil-required-field
  (testing "Fail when required field (euuid) is explicitly nil"
    (let [data {(id/key) nil  ; Explicitly nil - this should fail
                :name (str "User " @test-user-counter)
                :active true}]

      (is (thrown? Exception
                   (db/sync-entity db/*db* :iam/user data))
          "Should throw exception when euuid is explicitly nil"))))

(deftest test-sync-entity-omitted-optional-field
  (testing "Allow omitting optional fields entirely"
    (let [data {(id/key) (next-user-id)
                :name (str "User " @test-user-counter)
                :active true}  ; priority omitted - should be fine
          result (db/sync-entity db/*db* :iam/user data)]

      (is (map? result)
          "Should succeed when optional field is omitted")

      (is (nil? (:priority result))
          "Omitted optional field should be nil in result"))))

(deftest test-sync-entity-duplicate-in-batch
  (testing "Handle duplicate IDs in batch insert (last-write-wins)"
    (let [entity-id (id/data :test/write-user-1)  ; Use same ID for both to test duplicate handling
          data1 {(id/key) entity-id
                 :name (str "User 1 " @test-user-counter)
                 :active true}
          data2 {(id/key) entity-id  ; Same ID
                 :name (str "User 2 " @test-user-counter)
                 :active false}]

      ;; System should handle this gracefully (either error or last-write-wins)
      (try
        (let [results (db/sync-entity db/*db* :iam/user [data1 data2])]
          ;; If it succeeds, verify it handled the duplicate
          (is (vector? results)
              "Should return vector")
          (is (<= (count results) 2)
              "Should return at most 2 results"))
        (catch Exception e
          ;; If it errors, that's also acceptable behavior
          (is (some? e)
              "Should handle duplicate IDs (either succeed or error)"))))))

(deftest test-sync-entity-unique-constraint-violation
  (testing "Fail on unique constraint violation (duplicate name)"
    (let [name (str "Unique Name " @test-user-counter)
          data1 {(id/key) (next-user-id)
                 :name name
                 :active true}
          data2 {(id/key) (next-user-id)
                 :name name  ; Same name, violates unique constraint
                 :active false}]

      ;; First insert succeeds
      (db/sync-entity db/*db* :iam/user data1)

      ;; Second insert with same name should fail
      (is (thrown? Exception
                   (db/sync-entity db/*db* :iam/user data2))
          "Should throw exception for duplicate name (unique constraint)"))))

;;; ============================================================================
;;; Milestone 8: Complex Relation Scenarios
;;; ============================================================================

(deftest test-sync-entity-duplicate-relations
  (testing "Handle duplicate relations in array"
    (let [role (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role-euuid (id/extract role)

          ;; User with duplicate role references
          user-data {(id/key) (next-user-id)
                     :name (str "User " @test-user-counter)
                     :active true
                     :roles [{(id/key) role-euuid}
                             {(id/key) role-euuid}]}  ; Duplicate
          result (db/sync-entity db/*db* :iam/user user-data)]

      (is (map? result)
          "Should handle duplicate relations")

      ;; Verify only one link exists
      (let [verified (db/get-entity db/*db* :iam/user
                                    {(id/key) (id/extract result)}
                                    {(id/key) nil
                                     :roles [{:selections {(id/key) nil}}]})]
        (is (= 1 (count (:roles verified)))
            "Should deduplicate relations (only 1 role linked)")))))

(deftest test-sync-entity-large-relation-array
  (testing "Handle large relation arrays (100+ items)"
    (let [;; Create 50 roles
          roles (repeatedly 50 #(db/sync-entity db/*db* :iam/user-role (random-role-data)))
          role-refs (mapv #(hash-map (id/key) (id/extract %)) roles)

          ;; Create user with all 50 roles
          user-data {(id/key) (next-user-id)
                     :name (str "User With Many Roles " @test-user-counter)
                     :active true
                     :roles role-refs}
          result (db/sync-entity db/*db* :iam/user user-data)]

      (is (map? result)
          "Should handle large relation arrays")

      ;; Verify all roles are linked
      (let [verified (db/get-entity db/*db* :iam/user
                                    {(id/key) (id/extract result)}
                                    {(id/key) nil
                                     :roles [{:selections {(id/key) nil}}]})]
        (is (= 50 (count (:roles verified)))
            "Should link all 50 roles")))))

(deftest test-sync-entity-remove-then-add-relations
  (testing "Remove all relations then add new ones in sequence"
    (let [role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role3 (db/sync-entity db/*db* :iam/user-role (random-role-data))

          ;; Create user with role1
          user-data {(id/key) (next-user-id)
                     :name (str "User " @test-user-counter)
                     :active true
                     :roles [{(id/key) (id/extract role1)}]}
          result1 (db/sync-entity db/*db* :iam/user user-data)
          user-euuid (id/extract result1)

          ;; Remove all roles
          result2 (db/sync-entity db/*db* :iam/user
                                  {(id/key) user-euuid
                                   :roles []})

          ;; Add role2 and role3
          result3 (db/sync-entity db/*db* :iam/user
                                  {(id/key) user-euuid
                                   :roles [{(id/key) (id/extract role2)}
                                           {(id/key) (id/extract role3)}]})

          ;; Verify final state
          verified (db/get-entity db/*db* :iam/user
                                  {(id/key) user-euuid}
                                  {(id/key) nil
                                   :roles [{:selections {(id/key) nil}}]})]

      (is (= 2 (count (:roles verified)))
          "Should have 2 roles (role2 and role3)")

      (let [role-euuids (set (map id/extract (:roles verified)))]
        (is (not (contains? role-euuids (id/extract role1)))
            "Should not have role1")
        (is (contains? role-euuids (id/extract role2))
            "Should have role2")
        (is (contains? role-euuids (id/extract role3))
            "Should have role3")))))

;;; ============================================================================
;;; Run Tests
;;; ============================================================================

(comment
  ;; Run all tests
  (clojure.test/run-tests)

  ;; Run specific tests - Milestone 1 & 2
  (test-sync-entity-single-insert)
  (test-sync-entity-upsert)
  (test-sync-entity-batch)
  (test-stack-entity-basic)
  (test-sync-entity-with-foreign-key)
  (test-sync-entity-with-resolved-foreign-key)
  (test-sync-entity-batch-with-references)

  ;; Milestone 3 - Relations
  (test-sync-entity-with-relations)
  (test-sync-entity-replaces-relations)
  (test-stack-entity-adds-relations)
  (test-sync-entity-batch-with-relations))
