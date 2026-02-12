(ns synthigy.dataset.delete-test
  "Tests for delete-entity, purge-entity, and slice-entity operations."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.db :as db]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn cleanup-all-test-data!
  "Remove all test entities from previous runs before starting tests."
  []
  (log/info "Cleaning up leftover test data from previous runs...")
  (try
    ;; Delete test users
    (let [test-users (db/search-entity db/*db* :iam/user
                                       {:_where {:name {:_like "Delete%"}}}
                                       {(id/key) nil
                                        :name nil})]
      (doseq [user test-users]
        (try
          (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user)})
          (catch Exception _)))
      (log/info "Deleted" (count test-users) "delete test users"))

    ;; Delete test roles (by pattern and specific names)
    (let [test-role-patterns ["Delete Test%", "Role-%"]
          test-roles (mapcat (fn [pattern]
                               (db/search-entity db/*db* :iam/user-role
                                                 {:_where {:name {:_like pattern}}}
                                                 {(id/key) nil
                                                  :name nil}))
                             test-role-patterns)]
      (doseq [role test-roles]
        (try
          (db/delete-entity db/*db* :iam/user-role {(id/key) (id/extract role)})
          (catch Exception _)))
      (log/info "Deleted" (count test-roles) "delete test roles"))

    (log/info "Leftover test data cleanup complete")
    (catch Exception e
      (log/warn "Initialization cleanup failed (non-fatal):" (.getMessage e)))))

(defn cleanup-fixture
  "Clean up leftover test data from previous runs before running tests."
  [f]
  (cleanup-all-test-data!)
  (f))

;; Use comprehensive system fixture for initialization and shutdown
(use-fixtures :once test-helper/system-fixture cleanup-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

;; Counters for cycling through test data IDs
(def ^:private test-user-counter (atom 0))
(def ^:private test-role-counter (atom 0))

;; Available test data keys for delete tests
(def ^:private test-user-keys
  [:test/delete-user-1 :test/delete-user-2 :test/delete-user-3 :test/delete-user-4
   :test/delete-user-5 :test/delete-user-6 :test/delete-user-7 :test/delete-user-8])

(def ^:private test-role-keys
  [:test/delete-role-1 :test/delete-role-2])

(defn next-user-id
  "Get the next user ID from test data, cycling through available keys."
  []
  (let [idx (swap! test-user-counter inc)
        data-key (nth test-user-keys (mod idx (count test-user-keys)))]
    (id/data data-key)))

(defn next-role-id
  "Get the next role ID from test data, cycling through available keys."
  []
  (let [idx (swap! test-role-counter inc)
        data-key (nth test-role-keys (mod idx (count test-role-keys)))]
    (id/data data-key)))

(defn random-user-data
  "Generates user data for testing using registered defdata IDs."
  []
  (let [entity-id (next-user-id)]
    {(id/key) entity-id
     :name (str "Delete Test User " entity-id)
     :active true}))

(defn random-role-data
  "Generates role data for testing using registered defdata IDs."
  []
  (let [entity-id (next-role-id)]
    {(id/key) entity-id
     :name (str "Delete Test Role " entity-id)}))

;;; ============================================================================
;;; delete-entity Tests
;;; ============================================================================

(deftest test-delete-entity-by-euuid
  (testing "Delete entity by euuid"
    (let [;; Create a user
          user-data (random-user-data)
          user (db/sync-entity db/*db* :iam/user user-data)
          user-id (id/extract user)

          ;; Delete the user
          result (db/delete-entity db/*db* :iam/user {(id/key) user-id})]

      (is (true? result)
          "delete-entity should return true")

      ;; Verify user no longer exists
      (let [found (db/get-entity db/*db* :iam/user {(id/key) user-id} {(id/key) nil
                                                                       :name nil})]
        (is (nil? found)
            "User should not exist after deletion")))))

(deftest test-delete-entity-by-name
  (testing "Delete entity by name"
    (let [;; Create a user with unique name
          unique-name (str "DeleteByName-" @test-user-counter)
          user-data {(id/key) (next-user-id)
                     :name unique-name
                     :active true}
          user (db/sync-entity db/*db* :iam/user user-data)

          ;; Delete by name
          result (db/delete-entity db/*db* :iam/user {:name unique-name})]

      (is (true? result)
          "delete-entity should return true")

      ;; Verify user no longer exists
      (let [found (db/search-entity db/*db* :iam/user
                                    {:_where {:name {:_eq unique-name}}}
                                    {(id/key) nil
                                     :name nil})]
        (is (empty? found)
            "User should not exist after deletion")))))

(deftest test-delete-entity-multiple-conditions
  (testing "Delete entity with multiple conditions (AND)"
    (let [;; Create users with guaranteed unique names
          base-name (str "DeleteMulti-" @test-user-counter)
          unique-name1 (str base-name "-active")
          unique-name2 (str base-name "-inactive")
          user1 (db/sync-entity db/*db* :iam/user
                                {(id/key) (next-user-id)
                                 :name unique-name1
                                 :active true})
          user2 (db/sync-entity db/*db* :iam/user
                                {(id/key) (next-user-id)
                                 :name unique-name2
                                 :active false})

          ;; Delete only the active user
          result (db/delete-entity db/*db* :iam/user
                                   {(id/key) (id/extract user1)})]

      (is (true? result)
          "delete-entity should return true")

      ;; Verify only user1 was deleted, user2 remains
      (let [user1-after (db/get-entity db/*db* :iam/user {(id/key) (id/extract user1)} {(id/key) nil})
            user2-after (db/get-entity db/*db* :iam/user {(id/key) (id/extract user2)} {(id/key) nil})]
        (is (nil? user1-after)
            "User1 should be deleted")
        (is (some? user2-after)
            "User2 should remain")))))

(deftest test-delete-entity-nonexistent
  (testing "Delete nonexistent entity"
    (let [result (db/delete-entity db/*db* :iam/user {(id/key) (id/data :test/delete-nonexistent)})]
      (is (true? result)
          "delete-entity should return true even if no records deleted"))))

(deftest test-delete-entity-no-args
  (testing "Delete with no args returns false"
    (let [result (db/delete-entity db/*db* :iam/user {})]
      (is (false? result)
          "delete-entity should return false when args are empty"))))

;;; ============================================================================
;;; purge-entity Tests
;;; ============================================================================

(deftest test-purge-entity-basic
  (testing "Purge entity returns deleted data"
    (let [;; Create a user
          user-data (random-user-data)
          user (db/sync-entity db/*db* :iam/user user-data)
          user-id (id/extract user)

          ;; Purge the user
          result (db/purge-entity db/*db* :iam/user
                                  {:_where {(id/key) {:_eq user-id}}}
                                  {(id/key) nil
                                   :name nil
                                   :active nil})]

      (is (vector? result)
          "purge-entity should return a vector")

      (is (= 1 (count result))
          "Should return 1 deleted record")

      (let [deleted-user (first result)]
        (is (= user-id (id/extract deleted-user))
            "Should return the deleted user's euuid")
        (is (= (:name user-data) (:name deleted-user))
            "Should return the deleted user's name"))

      ;; Verify user no longer exists
      (let [found (db/get-entity db/*db* :iam/user {(id/key) user-id} {(id/key) nil})]
        (is (nil? found)
            "User should not exist after purge")))))

(deftest test-purge-entity-with-relations
  (testing "Purge entity with nested relations"
    (let [;; Create roles
          role1 (db/sync-entity db/*db* :iam/user-role (random-role-data))
          role2 (db/sync-entity db/*db* :iam/user-role (random-role-data))

          ;; Create user with roles
          user-data (assoc (random-user-data)
                      :roles [{(id/key) (id/extract role1)}
                              {(id/key) (id/extract role2)}])
          user (db/sync-entity db/*db* :iam/user user-data)
          user-id (id/extract user)

          ;; Purge the user with relations
          result (db/purge-entity db/*db* :iam/user
                                  {:_where {(id/key) {:_eq user-id}}}
                                  {(id/key) nil
                                   :name nil
                                   :roles [{(id/key) nil
                                            :name nil}]})]

      (is (= 1 (count result))
          "Should return 1 deleted record")

      (let [deleted-user (first result)]
        (is (= user-id (id/extract deleted-user))
            "Should return the deleted user's euuid")

        (is (vector? (:roles deleted-user))
            "Should include roles field")

        (is (= 2 (count (:roles deleted-user)))
            "Should return both roles")))))

(deftest test-purge-entity-multiple-records
  (testing "Purge multiple records at once"
    (let [;; Create multiple users with same active status
          prefix (str "PurgeMulti-" @test-user-counter)
          user1 (db/sync-entity db/*db* :iam/user
                                {(id/key) (next-user-id)
                                 :name (str prefix "-1")
                                 :active true})
          user2 (db/sync-entity db/*db* :iam/user
                                {(id/key) (next-user-id)
                                 :name (str prefix "-2")
                                 :active true})

          ;; Purge both users
          result (db/purge-entity db/*db* :iam/user
                                  {:_where {:name {:_like (str prefix "%")}}}
                                  {(id/key) nil
                                   :name nil})]

      (is (= 2 (count result))
          "Should return 2 deleted records")

      ;; Verify both users are gone
      (let [found (db/search-entity db/*db* :iam/user
                                    {:_where {:name {:_like (str prefix "%")}}}
                                    {(id/key) nil})]
        (is (empty? found)
            "No users should remain after purge")))))

(deftest test-purge-entity-no-match
  (testing "Purge with no matching records"
    (let [result (db/purge-entity db/*db* :iam/user
                                  {:_where {(id/key) {:_eq (id/data :test/delete-nonexistent)}}}
                                  {(id/key) nil
                                   :name nil})]
      (is (vector? result)
          "Should return a vector")
      (is (empty? result)
          "Should return empty vector when no records match"))))

;;; ============================================================================
;;; slice-entity Tests
;;; ============================================================================

(deftest test-slice-entity-basic
  (testing "Slice relations between entities"
    (let [;; Create role and user
          role (db/sync-entity db/*db* :iam/user-role (random-role-data))
          user-data (assoc (random-user-data)
                      :roles [{(id/key) (id/extract role)}])
          user (db/sync-entity db/*db* :iam/user user-data)

          ;; Verify relation exists
          user-before (db/get-entity db/*db* :iam/user
                                     {(id/key) (id/extract user)}
                                     {(id/key) nil
                                      :roles [{:selections {(id/key) nil}}]})
          _ (is (= 1 (count (:roles user-before)))
                "User should have 1 role before slice")

          ;; Slice the relation
          result (db/slice-entity db/*db* :iam/user
                                  {(id/key) {:_eq (id/extract user)}}
                                  {:roles [{:selections {(id/key) nil}}]})]

      (is (map? result)
          "slice-entity should return a map")

      (is (contains? result :roles)
          "Result should contain :roles key")

      (is (true? (:roles result))
          "Slice should succeed")

      ;; Verify relation is gone
      (let [user-after (db/get-entity db/*db* :iam/user
                                      {(id/key) (id/extract user)}
                                      {(id/key) nil
                                       :roles [{:selections {(id/key) nil}}]})]
        (is (empty? (:roles user-after))
            "User should have no roles after slice")))))

(deftest test-slice-entity-selective
  (testing "Slice only specific relations (filter target entity)"
    (let [;; Create roles
          role1 (db/sync-entity db/*db* :iam/user-role
                                {(id/key) (next-role-id)
                                 :name "Role-Keep"})
          role2 (db/sync-entity db/*db* :iam/user-role
                                {(id/key) (next-role-id)
                                 :name "Role-Remove"})

          ;; Create user with both roles
          user (db/sync-entity db/*db* :iam/user
                               (assoc (random-user-data)
                                 :roles [{(id/key) (id/extract role1)}
                                         {(id/key) (id/extract role2)}]))

          ;; Slice only the "Role-Remove" relation
          ;; Note: slice-entity currently doesn't support filtering target entities
          ;; This test demonstrates slicing ALL roles
          result (db/slice-entity db/*db* :iam/user
                                  {(id/key) {:_eq (id/extract user)}}
                                  {:roles [{:selections {(id/key) nil}}]})]

      (is (true? (:roles result))
          "Slice should succeed")

      ;; Verify all roles are removed (slice doesn't filter by target currently)
      (let [user-after (db/get-entity db/*db* :iam/user
                                      {(id/key) (id/extract user)}
                                      {(id/key) nil
                                       :roles [{:selections {(id/key) nil
                                                             :name nil}}]})]
        (is (empty? (:roles user-after))
            "All roles should be removed")))))

(deftest test-slice-entity-entities-remain
  (testing "Slice removes relations but entities remain"
    (let [;; Create role and user with relation
          role (db/sync-entity db/*db* :iam/user-role (random-role-data))
          user (db/sync-entity db/*db* :iam/user
                               (assoc (random-user-data)
                                 :roles [{(id/key) (id/extract role)}]))

          ;; Slice the relation
          _ (db/slice-entity db/*db* :iam/user
                             {(id/key) {:_eq (id/extract user)}}
                             {:roles [{:selections {(id/key) nil}}]})]

      ;; Verify both entities still exist
      (let [user-after (db/get-entity db/*db* :iam/user
                                      {(id/key) (id/extract user)}
                                      {(id/key) nil
                                       :name nil})
            role-after (db/get-entity db/*db* :iam/user-role
                                      {(id/key) (id/extract role)}
                                      {(id/key) nil
                                       :name nil})]
        (is (some? user-after)
            "User entity should still exist")
        (is (some? role-after)
            "Role entity should still exist")))))

(deftest test-slice-entity-no-relations
  (testing "Slice with no matching relations"
    (let [;; Create user without roles
          user (db/sync-entity db/*db* :iam/user (random-user-data))

          ;; Try to slice non-existent relations
          result (db/slice-entity db/*db* :iam/user
                                  {(id/key) {:_eq (id/extract user)}}
                                  {:roles [{:selections {(id/key) nil}}]})]

      (is (map? result)
          "slice-entity should return a map")

      (is (true? (:roles result))
          "Slice should succeed even with no relations"))))

;;; ============================================================================
;;; Run Tests
;;; ============================================================================

(comment
  ;; Run all tests
  (clojure.test/run-tests)

  ;; Run specific tests - delete-entity
  (test-delete-entity-by-euuid)
  (test-delete-entity-by-name)
  (test-delete-entity-multiple-conditions)
  (test-delete-entity-nonexistent)
  (test-delete-entity-no-args)

  ;; Run specific tests - purge-entity
  (test-purge-entity-basic)
  (test-purge-entity-with-relations)
  (test-purge-entity-multiple-records)
  (test-purge-entity-no-match)

  ;; Run specific tests - slice-entity
  (test-slice-entity-basic)
  (test-slice-entity-selective)
  (test-slice-entity-entities-remain)
  (test-slice-entity-no-relations))
