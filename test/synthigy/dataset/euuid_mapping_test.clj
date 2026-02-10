(ns synthigy.dataset.euuid-mapping-test
  "Tests for order-independent EUUID mapping in store-entity-records.

  Tests the refactored store-entity-records function that generates EUUIDs
  in the application and uses dual mapping (euuid + constraints) for
  order-independent reconstruction.

  NOTE: User entity uses 'name' as the unique constraint, not 'email'."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [synthigy.db :as db]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   ;; Backend loaded dynamically via test-helper → synthigy.core → require-backend-impl!
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.test-data]  ; Load test data registrations
   [synthigy.test-helper :as test-helper]))

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

(defn cleanup-tracked-entities!
  "Delete all tracked test entities"
  []
  (doseq [entity-id @created-test-entities]
    (try
      (db/delete-entity db/*db* :iam/user {(id/key) entity-id})
      (catch Exception _))) ; Ignore if doesn't exist
  (reset! created-test-entities #{}))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(def test-user-names
  "Names of test users - used for cleanup"
  ["EUUID Test User 1" "EUUID Test User 2" "EUUID Test User 3"
   "User With Provided EUUID" "User Updated Via Name"
   "Batch Test User 1" "Batch Test User 2" "Batch Test User 3"
   "NULL Constraint Test 1" "NULL Constraint Test 2"
   "Mixed 1 - Provided" "Mixed 2 - Generated" "Mixed 3 - Provided"
   "Order Test 1" "Order Test 2" "Order Test 3"
   "Complex Test User" "Complex Test User - Updated"
   "Complex Batch 1" "Complex Batch 2"
   "Complex Batch 1 - Updated"
   "Initial Name" "Update 1" "Update 2" "Final Update"])

(defn cleanup-test-users!
  "Remove all test users created during ID mapping tests.
   Cleans up by NAME which is the unique constraint."
  []
  (log/info "Cleaning up ID mapping test users...")
  (try
    (let [users (db/search-entity db/*db* :iam/user
                                  {:_where {:name {:_in test-user-names}}}
                                  {(id/key) nil :name nil})]
      (doseq [user users]
        (try
          (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user)})
          (catch Exception _)))
      (log/info "Deleted" (count users) "ID mapping test users"))
    ;; Also clean up performance test users by name pattern
    (try
      (let [perf-users (db/search-entity db/*db* :iam/user
                                         {:_where {:name {:_like "Perf Test User %"}}}
                                         {(id/key) nil})]
        (doseq [user perf-users]
          (try
            (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user)})
            (catch Exception _)))
        (when (seq perf-users)
          (log/info "Deleted" (count perf-users) "performance test users")))
      (catch Exception _))
    (catch Exception e
      (log/warn "Cleanup failed (non-fatal):" (.getMessage e)))))

(defn cleanup-fixture
  "Clean up test data before AND after each test"
  [f]
  ;; Clean before test to ensure fresh state
  (cleanup-tracked-entities!)
  (cleanup-test-users!)
  (try
    (f)
    (finally
      (cleanup-tracked-entities!)
      (cleanup-test-users!))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each cleanup-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn get-user-by-id
  "Retrieves a user by ID"
  [entity-id]
  (db/get-entity db/*db* :iam/user {(id/key) entity-id}
                 {(id/key) nil :name nil}))

(defn get-user-by-name
  "Retrieves a user by name (unique constraint)"
  [name]
  (first (db/search-entity db/*db* :iam/user
                           {:_where {:name {:_eq name}}}
                           {(id/key) nil :name nil})))

;;; ============================================================================
;;; Case 1: User Provides EUUID
;;; ============================================================================

(deftest test-user-provided-id
  (testing "User provides ID - should be preserved"
    (let [my-id (id/data :test/mapping-id-1)
          _ (track-entity! my-id)
          result (db/stack-entity db/*db* :iam/user
                                  [{:name "User With Provided EUUID"
                                    (id/key) my-id}])
          created-user (first result)]

      (is (some? created-user) "User should be created")
      (is (= my-id (id/extract created-user))
          "Provided ID should be preserved")
      (is (= "User With Provided EUUID" (:name created-user)))

      ;; Verify in database
      (let [db-user (get-user-by-id my-id)]
        (is (some? db-user) "User should exist in database")
        (is (= my-id (id/extract db-user))
            "ID in database should match provided ID")))))

;;; ============================================================================
;;; Case 2: New Record Without EUUID
;;; ============================================================================

(deftest test-new-record-generates-id
  (testing "New record without ID - should generate one"
    (let [result (db/stack-entity db/*db* :iam/user
                                  [{:name "EUUID Test User 1"}])
          created-user (first result)
          entity-id (id/extract created-user)]

      (is (some? created-user) "User should be created")
      (is (some? entity-id) "ID should be generated")
      (track-entity! entity-id)

      ;; Verify in database
      (let [db-user (get-user-by-id entity-id)]
        (is (some? db-user) "User should exist in database")
        (is (= entity-id (id/extract db-user))
            "ID in database should match generated ID")))))

(deftest test-batch-without-id
  (testing "Batch insert without IDs - all should get generated"
    (let [result (db/stack-entity db/*db* :iam/user
                                  [{:name "Batch Test User 1"}
                                   {:name "Batch Test User 2"}
                                   {:name "Batch Test User 3"}])
          entity-ids (map id/extract result)]

      (is (= 3 (count result)) "All users should be created")
      (is (every? some? entity-ids) "All IDs should be generated")
      (is (= 3 (count (distinct entity-ids))) "All IDs should be unique")

      ;; Track for cleanup
      (doseq [entity-id entity-ids]
        (track-entity! entity-id))

      ;; Verify all in database
      (doseq [entity-id entity-ids]
        (let [db-user (get-user-by-id entity-id)]
          (is (some? db-user) "Each user should exist in database")
          (is (= entity-id (id/extract db-user))))))))

;;; ============================================================================
;;; Case 3: Update via Constraint Without EUUID
;;; ============================================================================

(deftest test-update-via-constraint
  (testing "Update via constraint without ID - should preserve existing ID"
    ;; First create a user
    (let [initial-result (db/stack-entity db/*db* :iam/user
                                          [{:name "User Updated Via Name"}])
          initial-user (first initial-result)
          original-id (id/extract initial-user)]

      (is (some? original-id) "Original ID should exist")
      (track-entity! original-id)

      ;; Now update via name constraint by providing same name with additional data
      ;; Since name is the constraint, updating same name should preserve ID
      (let [update-result (db/stack-entity db/*db* :iam/user
                                           [{:name "User Updated Via Name"
                                             :active true}])
            updated-user (first update-result)]

        (is (some? updated-user) "Update should succeed")
        (is (= original-id (id/extract updated-user))
            "ID should remain the same after update")

        ;; Verify in database
        (let [db-user (get-user-by-name "User Updated Via Name")]
          (is (some? db-user) "User should exist in database")
          (is (= original-id (id/extract db-user))
              "ID in database should be unchanged"))))))

(deftest test-multiple-updates-via-constraint
  (testing "Multiple updates via constraint - ID should remain stable"
    ;; Create initial user
    (let [initial-result (db/stack-entity db/*db* :iam/user
                                          [{:name "Initial Name"}])
          original-id (id/extract (first initial-result))]

      (track-entity! original-id)

      ;; Multiple updates changing name (new name = new record since name is constraint)
      ;; To test ID stability, we keep the same name
      (db/stack-entity db/*db* :iam/user
                       [{:name "Initial Name"
                         :active true}])

      (db/stack-entity db/*db* :iam/user
                       [{:name "Initial Name"
                         :active false}])

      (let [final-result (db/stack-entity db/*db* :iam/user
                                          [{:name "Initial Name"
                                            :active true}])
            final-user (first final-result)]

        (is (= original-id (id/extract final-user))
            "ID should remain unchanged across multiple updates")

        ;; Verify in database
        (let [db-user (get-user-by-name "Initial Name")]
          (is (= original-id (id/extract db-user))
              "ID in database should be unchanged"))))))

;;; ============================================================================
;;; Edge Case: Mixed Batch (Some with EUUID, Some Without)
;;; ============================================================================

(deftest test-mixed-batch-with-and-without-id
  (testing "Batch with mixed ID provision - should handle both"
    (let [provided-id-1 (id/data :test/mapping-id-2)
          provided-id-2 (id/data :test/mapping-id-3)
          result (db/stack-entity db/*db* :iam/user
                                  [{:name "Mixed 1 - Provided"
                                    (id/key) provided-id-1}
                                   {:name "Mixed 2 - Generated"}
                                   {:name "Mixed 3 - Provided"
                                    (id/key) provided-id-2}])
          entity-ids (map id/extract result)]

      (is (= 3 (count result)) "All users should be created")

      ;; Track for cleanup
      (doseq [entity-id entity-ids]
        (track-entity! entity-id))

      ;; Check first user (provided)
      (is (= provided-id-1 (nth entity-ids 0))
          "First ID should be the provided one")

      ;; Check second user (generated)
      (is (some? (nth entity-ids 1))
          "Second ID should be generated")

      ;; Check third user (provided)
      (is (= provided-id-2 (nth entity-ids 2))
          "Third ID should be the provided one")

      ;; All should be unique
      (is (= 3 (count (distinct entity-ids)))
          "All IDs should be unique"))))

;;; ============================================================================
;;; Edge Case: Order Independence
;;; ============================================================================

(deftest test-order-independence
  (testing "Mapping should work regardless of database return order"
    ;; This test verifies that even if the database returns results in
    ;; a different order than insertion, the mapping still works correctly.

    (let [id-1 (id/data :test/mapping-id-4)
          id-2 (id/data :test/mapping-id-5)
          id-3 (id/data :test/mapping-id-6)
          result (db/stack-entity db/*db* :iam/user
                                  [{:name "Order Test 1"
                                    (id/key) id-1}
                                   {:name "Order Test 2"
                                    (id/key) id-2}
                                   {:name "Order Test 3"
                                    (id/key) id-3}])
          result-map (group-by id/extract result)]

      ;; Track for cleanup
      (doseq [entity-id [id-1 id-2 id-3]]
        (track-entity! entity-id))

      ;; Verify each user by ID (not by position)
      (is (= "Order Test 1" (:name (first (get result-map id-1))))
          "User 1 should have correct name regardless of return order")
      (is (= "Order Test 2" (:name (first (get result-map id-2))))
          "User 2 should have correct name regardless of return order")
      (is (= "Order Test 3" (:name (first (get result-map id-3))))
          "User 3 should have correct name regardless of return order"))))

;;; ============================================================================
;;; Edge Case: Empty Rows
;;; ============================================================================

(deftest test-empty-rows
  (testing "Empty rows array should be handled gracefully"
    (let [result (db/stack-entity db/*db* :iam/user [])]
      (is (empty? result) "Result should be empty")
      (is (vector? result) "Result should be a vector"))))

;;; ============================================================================
;;; Edge Case: NULL Constraint Values
;;; ============================================================================

(deftest test-null-constraint-values
  (testing "Rows with unique names should still map via ID"
    (let [result (db/stack-entity db/*db* :iam/user
                                  [{:name "NULL Constraint Test 1"}
                                   {:name "NULL Constraint Test 2"}])
          entity-ids (map id/extract result)]

      (is (= 2 (count result)) "Both users should be created")
      (is (every? some? entity-ids) "Both IDs should be generated")
      (is (= 2 (count (distinct entity-ids))) "IDs should be unique")

      ;; Track for cleanup
      (doseq [entity-id entity-ids]
        (track-entity! entity-id)))))

;;; ============================================================================
;;; Integration Test: Complex Scenario
;;; ============================================================================

(deftest test-complex-scenario
  (testing "Complex scenario with create, update, and batch operations"
    ;; Step 1: Create a user with provided ID
    (let [my-id (id/data :test/mapping-id-7)
          _ (track-entity! my-id)
          create-result (db/stack-entity db/*db* :iam/user
                                         [{:name "Complex Test User"
                                           (id/key) my-id}])]

      (is (= my-id (id/extract (first create-result))))

      ;; Step 2: Update via constraint (same name, without ID in request)
      (let [update-result (db/stack-entity db/*db* :iam/user
                                           [{:name "Complex Test User"
                                             :active true}])]

        (is (= my-id (id/extract (first update-result)))
            "ID should remain unchanged after update")

        ;; Step 3: Batch insert new users (without ID)
        (let [batch-result (db/stack-entity db/*db* :iam/user
                                            [{:name "Complex Batch 1"}
                                             {:name "Complex Batch 2"}])
              batch-ids (map id/extract batch-result)]

          (is (= 2 (count batch-result)))
          (is (every? some? batch-ids))

          ;; Track batch for cleanup
          (doseq [entity-id batch-ids]
            (track-entity! entity-id))

          ;; Step 4: Update one of the batch users via constraint
          (let [first-batch-id (first batch-ids)
                update-batch-result (db/stack-entity db/*db* :iam/user
                                                     [{:name "Complex Batch 1"
                                                       :active true}])]

            (is (= first-batch-id (id/extract (first update-batch-result)))
                "Batch user ID should remain unchanged after update"))

          ;; Step 5: Verify all users in database
          (is (some? (get-user-by-id my-id)))
          (doseq [entity-id batch-ids]
            (is (some? (get-user-by-id entity-id)))))))))

;;; ============================================================================
;;; Performance Test: Large Batch
;;; ============================================================================

(deftest ^:performance test-large-batch-performance
  (testing "Large batch insert should complete in reasonable time"
    (let [n 100  ;; 100 users
          users (mapv (fn [i]
                        {:name (str "Perf Test User " i)})
                      (range n))
          start (System/currentTimeMillis)
          result (db/stack-entity db/*db* :iam/user users)
          duration (- (System/currentTimeMillis) start)
          entity-ids (map id/extract result)]

      ;; Track for cleanup
      (doseq [entity-id entity-ids]
        (track-entity! entity-id))

      (is (= n (count result))
          (format "All %d users should be created" n))
      (is (= n (count (distinct entity-ids)))
          "All IDs should be unique")

      ;; Should complete in under 5 seconds for 100 users
      (is (< duration 5000)
          (format "Should complete in < 5s (took %dms for %d users, %.2fms per user)"
                  duration n (/ duration (double n))))

      (log/infof "Performance: %d users in %dms (%.2fms per user)"
                 n duration (/ duration (double n))))))
