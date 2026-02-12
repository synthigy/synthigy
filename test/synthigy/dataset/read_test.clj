(ns synthigy.dataset.read-test
  "Tests for get-entity and search-entity read operations.

  These tests verify direct protocol-level read operations without going
  through the GraphQL layer. Tests are organized by functionality:

  - Milestone 1: Basic reads (get-entity, search-entity)
  - Milestone 2: Filtering (WHERE clauses with _eq, _neq, _in, _and, _or, IS NULL)
  - Milestone 3: Additional operators (_not_in, _like, _ilike)
  - Milestone 4: Ordering and pagination (ORDER BY, LIMIT, OFFSET)
  - Milestone 5: Nested relations (single-level, multi-level)
  - Milestone 6: Distinct queries (_distinct)

  Note: _count and _agg are GraphQL-level features not available at the protocol level."
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
;;; Test Data Management
;;; ============================================================================

(def test-namespace "read-test")

;;; ============================================================================
;;; Test Data IDs - Provider-agnostic via id/data
;;; ============================================================================

(def test-user-keys
  "Maps short keys to registered test data keys for backwards compatibility."
  {:alice     :test/user-alice
   :bob       :test/user-bob
   :charlie   :test/user-charlie
   :null-name :test/user-null-name})

(defn test-user-id
  "Get test user ID by short key. Returns UUID or XID based on provider."
  [k]
  (id/data (get test-user-keys k)))

(def test-role-keys
  "Maps short keys to registered test role data keys."
  {:admin  :test/role-admin
   :viewer :test/role-viewer})

(defn test-role-id
  "Get test role ID by short key. Returns UUID or XID based on provider."
  [k]
  (id/data (get test-role-keys k)))

(defn cleanup-test-data!
  "Remove all test data created during tests"
  []
  (log/info "Cleaning up test data...")
  (try
    ;; Delete test users by predefined keys
    (doseq [[_short-key data-key] test-user-keys]
      (try
        (db/delete-entity db/*db* :iam/user {(id/key) (id/data data-key)})
        (catch Exception _))) ; Ignore if doesn't exist

    ;; Delete test roles by predefined keys
    (doseq [[_short-key data-key] test-role-keys]
      (try
        (db/delete-entity db/*db* :iam/user-role {(id/key) (id/data data-key)})
        (catch Exception _)))

    ;; Also clean up any orphaned "Test%" roles from other test files (e.g., write_test.clj)
    ;; This prevents cross-test-file pollution when tests are interrupted
    (let [orphan-roles (db/search-entity db/*db* :iam/user-role
                                         {:_where {:name {:_like "Test%"}}}
                                         {(id/key) nil})]
      (doseq [role orphan-roles]
        (try
          (db/delete-entity db/*db* :iam/user-role {(id/key) (id/extract role)})
          (catch Exception _)))
      (when (seq orphan-roles)
        (log/info "Cleaned up" (count orphan-roles) "orphaned test roles")))

    (log/info "Test data cleanup complete")
    (catch Exception e
      (log/warn "Test cleanup failed (non-fatal):" (.getMessage e)))))

(defn create-test-user!
  "Create a test user with predictable ID.
   Explicitly clears roles to prevent stale associations from previous test runs."
  ([key-name name active]
   (create-test-user! key-name name active nil))
  ([key-name name active priority]
   (db/sync-entity db/*db* :iam/user
                   (cond-> {(id/key) (test-user-id key-name)
                            :name name
                            :active active
                            :roles []}  ; Clear any stale role associations
                     priority (assoc :priority priority)))))

(defn create-test-role!
  "Create a test role with predictable ID"
  [key-name name]
  (db/sync-entity db/*db* :iam/user-role
                  {(id/key) (test-role-id key-name)
                   :name name}))

(defn assign-role-to-user!
  "Assign a role to a user (appends to existing roles)"
  [user-key role-key]
  (let [user-id (test-user-id user-key)
        role-id (test-role-id role-key)
        existing-user (db/get-entity db/*db* :iam/user
                                     {(id/key) user-id}
                                     {(id/key) nil
                                      :roles [{:selections {(id/key) nil}}]})
        existing-roles (or (:roles existing-user) [])
        all-roles (conj existing-roles {(id/key) role-id})]
    (db/sync-entity db/*db* :iam/user
                    {(id/key) user-id
                     :roles all-roles})))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn cleanup-fixture
  "Clean up leftover test data from previous runs before running tests"
  [f]
  (cleanup-test-data!)
  (f))

(defn data-cleanup-fixture
  "Clean up test data before and after each test"
  [f]
  (cleanup-test-data!)  ; Clean before test
  (f)
  (cleanup-test-data!)) ; Clean after test

;; Use comprehensive system fixture for initialization and shutdown
(use-fixtures :once test-helper/system-fixture cleanup-fixture)
(use-fixtures :each data-cleanup-fixture)

;;; ============================================================================
;;; Milestone 1: Basic Read Operations
;;; ============================================================================

(deftest test-search-entity-all
  (testing "Search for all users without filters"
    ;; Create test data
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" false)

    (let [results (db/search-entity db/*db* :iam/user {} {(id/key) nil
                                                          :name nil})]

      (is (vector? results)
          "Should return a vector")

      (is (>= (count results) 2)
          "Should return at least our 2 test users")

      (let [first-user (first results)]
        (is (contains? first-user (id/key))
            "User should have euuid field")

        (is (contains? first-user :name)
            "User should have name field")

        (is (some? (id/extract first-user))
            "ID should be generated")))))

(deftest test-search-entity-with-limit
  (testing "Search with LIMIT"
    ;; Create 3 test users
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" true)
    (create-test-user! :charlie "Charlie" true)

    (let [limit 2
          results (db/search-entity db/*db* :iam/user
                                    {:_where {:_or [{:name {:_like "Bob%"}}
                                                    {:name {:_like "Alice%"}}
                                                    {:name {:_like "Charlie%"}}]}
                                     :_limit limit}
                                    {(id/key) nil
                                     :name nil})]

      (is (vector? results)
          "Should return a vector")

      (is (<= (count results) limit)
          (str "Should return at most " limit " users")))))

(deftest test-search-entity-with-offset
  (testing "Search with OFFSET"
    ;; Create ordered test data
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" true)
    (create-test-user! :charlie "Charlie" true)

    (let [all-results (db/search-entity db/*db* :iam/user
                                        {:_where {(id/key) {:_in [(test-user-id :alice)
                                                                  (test-user-id :bob)
                                                                  (test-user-id :charlie)]}}
                                         :_order_by {:name :asc}}
                                        {(id/key) nil
                                         :name nil})
          offset-results (db/search-entity db/*db* :iam/user
                                           {:_where {(id/key) {:_in [(test-user-id :alice)
                                                                     (test-user-id :bob)
                                                                     (test-user-id :charlie)]}}
                                            :_offset 1
                                            :_order_by {:name :asc}}
                                           {(id/key) nil
                                            :name nil})]

      (is (= 3 (count all-results))
          "Should have 3 test users")

      (is (= 2 (count offset-results))
          "Offset should skip 1 record")

      (is (= (id/extract (nth all-results 1))
             (id/extract (first offset-results)))
          "Offset should skip first N records"))))

(deftest test-get-entity-by-euuid
  (testing "Get single entity by euuid"
    (create-test-user! :alice "Alice" true)

    (let [result (db/get-entity db/*db* :iam/user
                                {(id/key) (test-user-id :alice)}
                                {(id/key) nil
                                 :name nil
                                 :active nil})]

      (is (map? result)
          "get-entity should return a single map")

      (is (= (test-user-id :alice) (id/extract result))
          "Should return the requested user")

      (is (= "Alice" (:name result))
          "Should have correct name")

      (is (true? (:active result))
          "Should have correct active status"))))

(deftest test-get-entity-nonexistent
  (testing "Get entity with non-existent ID"
    (let [fake-id (id/data :test/nonexistent-entity)
          result (db/get-entity db/*db* :iam/user
                                {(id/key) fake-id}
                                {(id/key) nil
                                 :name nil})]

      (is (nil? result)
          "Should return nil for non-existent entity"))))

;;; ============================================================================
;;; Milestone 2: Filtering with WHERE Clauses
;;; ============================================================================

(deftest test-search-with-equality-filter
  (testing "Search with WHERE equality filter"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" false)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:active {:_eq true}
                                              (id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :bob)]}}}
                                    {(id/key) nil
                                     :name nil
                                     :active nil})]

      (is (= 1 (count results))
          "Should return only active user")

      (is (= "Alice" (:name (first results)))
          "Should return Alice"))))

(deftest test-search-with-not-equal-filter
  (testing "Search with WHERE not-equal filter"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" false)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:active {:_neq true}
                                              (id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :bob)]}}}
                                    {(id/key) nil
                                     :name nil
                                     :active nil})]

      (is (= 1 (count results))
          "Should return only inactive user")

      (is (= "Bob" (:name (first results)))
          "Should return Bob"))))

(deftest test-search-with-is-null-filter
  (testing "Search with WHERE IS NULL filter ({:_eq nil})"
    ;; Create user with null name
    (db/sync-entity db/*db* :iam/user
                    {(id/key) (test-user-id :null-name)
                     :active true})

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:name {:_eq nil}
                                              (id/key) {:_eq (test-user-id :null-name)}}}
                                    {(id/key) nil
                                     :name nil})]

      (is (= 1 (count results))
          "Should return user with null name")

      (is (nil? (:name (first results)))
          "Returned user should have null name"))))

(deftest test-search-with-is-not-null-filter
  (testing "Search with WHERE IS NOT NULL filter ({:_neq nil})"
    (create-test-user! :alice "Alice" true)
    (db/sync-entity db/*db* :iam/user
                    {(id/key) (test-user-id :null-name)
                     :active true})

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:name {:_neq nil}
                                              (id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :null-name)]}}}
                                    {(id/key) nil
                                     :name nil})]

      (is (= 1 (count results))
          "Should return only user with non-null name")

      (is (= "Alice" (:name (first results)))
          "Should return Alice"))))

(deftest test-search-with-and-filter
  (testing "Search with WHERE AND compound filter"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" false)
    (db/sync-entity db/*db* :iam/user
                    {(id/key) (test-user-id :null-name)
                     :active true})

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:_and [{:active {:_eq true}}
                                                     {:name {:_neq nil}}]
                                              (id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :bob)
                                                              (test-user-id :null-name)]}}}
                                    {(id/key) nil
                                     :name nil
                                     :active nil})]

      (is (= 1 (count results))
          "Should return only active user with non-null name")

      (is (= "Alice" (:name (first results)))
          "Should return Alice"))))

(deftest test-search-with-or-filter
  (testing "Search with WHERE OR compound filter"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" false)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:_or [{:name {:_eq "Alice"}}
                                                    {:name {:_eq "Bob"}}]}}
                                    {(id/key) nil
                                     :name nil
                                     :active nil})]

      (is (= 2 (count results))
          "Should return both Alice and Bob")

      (let [names (set (map :name results))]
        (is (= #{"Alice" "Bob"} names)
            "Should have both names")))))

;;; ============================================================================
;;; Milestone 3: Additional Query Operators
;;; ============================================================================

(deftest test-search-with-not-in-filter
  (testing "Search with WHERE _not_in filter"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" false)
    (create-test-user! :charlie "Charlie" true)

    (let [excluded-uuids [(test-user-id :alice) (test-user-id :bob)]
          results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_not_in excluded-uuids}
                                              :name {:_in ["Alice" "Bob" "Charlie"]}}}
                                    {(id/key) nil
                                     :name nil})]

      (is (= 1 (count results))
          "Should return only Charlie")

      (is (= "Charlie" (:name (first results)))
          "Should exclude Alice and Bob"))))

(deftest test-search-with-like-filter
  (testing "Search with WHERE _like filter (pattern matching)"
    (create-test-user! :alice "Alice Anderson" true)
    (create-test-user! :bob "Bob Builder" false)
    (create-test-user! :charlie "Alice Cooper" true)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:name {:_like "Alice%"}}}
                                    {(id/key) nil
                                     :name nil})]

      (is (>= (count results) 2)
          "Should return at least Alice Anderson and Alice Cooper")

      (let [names (set (map :name results))]
        (is (contains? names "Alice Anderson")
            "Should include Alice Anderson")
        (is (contains? names "Alice Cooper")
            "Should include Alice Cooper")
        (is (not (contains? names "Bob Builder"))
            "Should not include Bob Builder")))))

(deftest test-search-with-ilike-filter
  (testing "Search with WHERE _ilike filter (case-insensitive pattern matching)"
    (create-test-user! :alice "Alice Anderson" true)
    (create-test-user! :bob "Bob Builder" false)
    (create-test-user! :charlie "ALICE Cooper" true)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {:name {:_ilike "alice%"}}}
                                    {(id/key) nil
                                     :name nil})]

      (is (>= (count results) 2)
          "Should return both 'Alice Anderson' and 'ALICE Cooper' (case-insensitive)")

      (let [names (set (map :name results))]
        (is (contains? names "Alice Anderson")
            "Should include Alice Anderson")
        (is (contains? names "ALICE Cooper")
            "Should include ALICE Cooper (case-insensitive match)")
        (is (not (contains? names "Bob Builder"))
            "Should not include Bob Builder")))))

;;; ============================================================================
;;; Milestone 4: Ordering and Pagination
;;; ============================================================================

(deftest test-search-with-order-by-asc
  (testing "Search with ORDER BY ascending"
    (create-test-user! :charlie "Charlie" true)
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" true)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :bob)
                                                              (test-user-id :charlie)]}}
                                     :_order_by {:name :asc}}
                                    {(id/key) nil
                                     :name nil})]

      (is (= 3 (count results))
          "Should return all 3 users")

      (is (= ["Alice" "Bob" "Charlie"] (mapv :name results))
          "Should be sorted alphabetically"))))

(deftest test-search-with-order-by-desc
  (testing "Search with ORDER BY descending"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" true)
    (create-test-user! :charlie "Charlie" true)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :bob)
                                                              (test-user-id :charlie)]}}
                                     :_order_by {:name :desc}}
                                    {(id/key) nil
                                     :name nil})]

      (is (= 3 (count results))
          "Should return all 3 users")

      (is (= ["Charlie" "Bob" "Alice"] (mapv :name results))
          "Should be sorted reverse alphabetically"))))

(deftest test-search-pagination-consistency
  (testing "Pagination should return consistent, non-overlapping results"
    (create-test-user! :alice "Alice" true)
    (create-test-user! :bob "Bob" true)
    (create-test-user! :charlie "Charlie" true)

    (let [page-1 (db/search-entity db/*db* :iam/user
                                   {:_where {(id/key) {:_in [(test-user-id :alice)
                                                             (test-user-id :bob)
                                                             (test-user-id :charlie)]}}
                                    :_limit 2
                                    :_offset 0
                                    :_order_by {:name :asc}}
                                   {(id/key) nil
                                    :name nil})
          page-2 (db/search-entity db/*db* :iam/user
                                   {:_where {(id/key) {:_in [(test-user-id :alice)
                                                             (test-user-id :bob)
                                                             (test-user-id :charlie)]}}
                                    :_limit 2
                                    :_offset 2
                                    :_order_by {:name :asc}}
                                   {(id/key) nil
                                    :name nil})]

      (is (= 2 (count page-1))
          "Page 1 should have 2 results")

      (is (= 1 (count page-2))
          "Page 2 should have 1 result")

      (is (= ["Alice" "Bob"] (mapv :name page-1))
          "Page 1 should have Alice and Bob")

      (is (= ["Charlie"] (mapv :name page-2))
          "Page 2 should have Charlie")

      (let [page-1-ids (set (map id/extract page-1))
            page-2-ids (set (map id/extract page-2))]
        (is (empty? (clojure.set/intersection page-1-ids page-2-ids))
            "Pages should not have overlapping records")))))

;;; ============================================================================
;;; Milestone 4: Nested Relations
;;; ============================================================================

(deftest test-search-with-single-level-nested-relation
  (testing "Search with single-level nested relation (User -> Roles)"
    ;; Create test roles
    (create-test-role! :admin "Admin")
    (create-test-role! :viewer "Viewer")

    ;; Create users with roles
    (create-test-user! :alice "Alice" true)
    (assign-role-to-user! :alice :admin)

    (create-test-user! :bob "Bob" true)
    (assign-role-to-user! :bob :viewer)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_eq (test-user-id :alice)}}}
                                    {(id/key) nil
                                     :name nil
                                     :roles [{:selections
                                              {(id/key) nil
                                               :name nil}}]})]

      (is (= 1 (count results))
          "Should return Alice")

      (let [alice (first results)]
        (is (contains? alice :roles)
            "User should have roles field")

        (is (vector? (:roles alice))
            "Roles should be a vector")

        (is (= 1 (count (:roles alice)))
            "Alice should have 1 role")

        (is (= "Admin" (:name (first (:roles alice))))
            "Alice's role should be Admin")))))

(deftest test-search-with-multi-level-nested-relation
  (testing "Search with multi-level nested relation (User -> Roles -> Users)"
    ;; Create admin role
    (create-test-role! :admin "Admin")

    ;; Create users and assign to admin role
    (create-test-user! :alice "Alice" true)
    (assign-role-to-user! :alice :admin)

    (create-test-user! :bob "Bob" true)
    (assign-role-to-user! :bob :admin)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_eq (test-user-id :alice)}}}
                                    {(id/key) nil
                                     :name nil
                                     :roles [{:selections {(id/key) nil
                                                           :name nil
                                                           :users [{:selections {(id/key) nil
                                                                                 :name nil}}]}}]})]

      (is (= 1 (count results))
          "Should return Alice")

      (let [alice (first results)
            roles (:roles alice)]
        (is (vector? roles)
            "Roles should be a vector")

        (is (= 1 (count roles))
            "Alice should have 1 role")

        (let [admin-role (first roles)
              role-users (:users admin-role)]
          (is (= "Admin" (:name admin-role))
              "Role should be Admin")

          (is (vector? role-users)
              "Role users should be a vector")

          (is (= 2 (count role-users))
              "Admin role should have 2 users (Alice and Bob)")

          (let [user-names (set (map :name role-users))]
            (is (= #{"Alice" "Bob"} user-names)
                "Admin role should have Alice and Bob")))))))

(deftest test-search-with-empty-nested-relations
  (testing "Handle empty nested relations gracefully"
    ;; Create user without roles
    (cleanup-test-data!)
    (create-test-user! :alice "Alice" true)

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_eq (test-user-id :alice)}}}
                                    {(id/key) nil
                                     :name nil
                                     :roles [{:selections {(id/key) nil
                                                           :name nil}}]})]

      (is (= 1 (count results))
          "Should return Alice")

      (let [alice (first results)]

        (is (nil? (:roles alice))
            "Roles should be nil since we didn't assign any")))))

(deftest test-get-entity-with-nested-relations
  (testing "get-entity should support nested relations"
    ;; Create role and user with role
    (create-test-role! :admin "Admin")
    (create-test-user! :alice "Alice" true)
    (assign-role-to-user! :alice :admin)

    (let [result (db/get-entity db/*db* :iam/user
                                {(id/key) (test-user-id :alice)}
                                {(id/key) nil
                                 :name nil
                                 :roles [{:selections {(id/key) nil
                                                       :name nil}}]})]

      (is (map? result)
          "get-entity should return a single map")

      (is (= "Alice" (:name result))
          "Should return Alice")

      (is (contains? result :roles)
          "Result should include nested roles")

      (is (vector? (:roles result))
          "Roles should be a vector")

      (is (= 1 (count (:roles result)))
          "Alice should have 1 role")

      (is (= "Admin" (:name (first (:roles result))))
          "Alice's role should be Admin"))))

;;; ============================================================================
;;; Milestone 6: Aggregation and Counting (GraphQL-level features)
;;; ============================================================================

(deftest test-search-with-count-on-relations
  (testing "Search with _count to count related entities"
    ;; Create roles
    (create-test-role! :admin "Admin")
    (create-test-role! :viewer "Viewer")

    ;; Create user with multiple roles
    (create-test-user! :alice "Alice" true)
    (assign-role-to-user! :alice :admin)
    (assign-role-to-user! :alice :viewer)

    ;; Create user with one role
    (create-test-user! :bob "Bob" true)
    (assign-role-to-user! :bob :admin)

    ;; Create user with no roles
    (create-test-user! :charlie "Charlie" true)

    (let [results (db/search-entity
                    db/*db* :iam/user
                    {:_where {(id/key) {:_in [(test-user-id :alice)
                                              (test-user-id :bob)
                                              (test-user-id :charlie)]}}}
                    {(id/key) nil
                     :_count [{:selections
                               {:roles nil}}]
                     :name nil})]

      (is (= 3 (count results))
          "Should return all 3 users")

      (let [alice (first (filter #(= "Alice" (:name %)) results))
            bob (first (filter #(= "Bob" (:name %)) results))
            charlie (first (filter #(= "Charlie" (:name %)) results))]

        (is (= 2 (get-in alice [:_count :roles]))
            "Alice should have 2 roles")

        (is (= 1 (get-in bob [:_count :roles]))
            "Bob should have 1 role")

        (is (= 0 (get-in charlie [:_count :roles]))
            "Charlie should have 0 roles")))))

(deftest test-search-with-agg-on-relations
  (testing "Search with _agg to aggregate numeric fields on relations"
    ;; Create roles
    (create-test-role! :admin "Admin")
    (create-test-role! :viewer "Viewer")

    ;; Create users with priorities and assign to admin role
    (create-test-user! :alice "Alice" true 10)
    (assign-role-to-user! :alice :admin)

    (create-test-user! :bob "Bob" true 20)
    (assign-role-to-user! :bob :admin)

    (create-test-user! :charlie "Charlie" true 30)
    (assign-role-to-user! :charlie :admin)

    ;; Query roles and aggregate user priorities
    ;; TODO: User will provide correct selection syntax for _agg
    (let [results (db/search-entity
                    db/*db* :iam/user-role
                    {:_where {(id/key) {:_eq (test-role-id :admin)}}}
                    {(id/key) nil
                     :name nil
                     :_count [{:selections
                               {:users nil}}]
                     :_agg [{:selections
                             {:users
                              [{:selections
                                {:priority [{:selections
                                             {:sum nil
                                              :max nil
                                              :min nil
                                              :avg nil}}]}}]}}]
                     :users [{:selections {:priority nil}}]})]

      (is (= 1 (count results))
          "Should return admin role")

      (let [admin (first results)]
        (is (some? admin)
            "Admin role should be in results")

        (is (= 3 (get-in admin [:_count :users]))
            "Admin role should have 3 users")

        ;; Verify aggregation results for priorities [10, 20, 30]
        (is (= 60M (get-in admin [:_agg :users :sum :priority]))
            "Sum of priorities should be 60")

        ;; Note: Only :sum is currently implemented
        ;; Expected for complete implementation:
        ;; :max -> 30, :min -> 10, :avg -> 20.0
        ))))

(deftest test-count-alongside-nested-relation-pulls
  (testing "Count works alongside nested relation pulls (verify they match)"
    ;; Create roles
    (create-test-role! :admin "Admin")
    (create-test-role! :viewer "Viewer")

    ;; Create user with multiple roles
    (create-test-user! :alice "Alice" true)
    (assign-role-to-user! :alice :admin)
    (assign-role-to-user! :alice :viewer)

    ;; Query with both nested relations AND count
    (let [results (db/search-entity
                    db/*db* :iam/user
                    {:_where {(id/key) {:_eq (test-user-id :alice)}}}
                    {(id/key) nil
                     :name nil
                     :roles [{:selections {(id/key) nil
                                           :name nil}}]
                     :_count [{:selections {:roles nil}}]})]

      (is (= 1 (count results))
          "Should return 1 user")

      (let [alice (first results)]
        ;; Check nested relations are pulled
        (is (vector? (:roles alice))
            "Roles should be a vector")

        (is (= 2 (count (:roles alice)))
            "Should pull 2 roles")

        ;; Check count matches pulled relations
        (is (= 2 (get-in alice [:_count :roles]))
            "Count should match number of pulled roles")

        ;; Verify role data is actually pulled
        (let [role-names (set (map :name (:roles alice)))]
          (is (= #{"Admin" "Viewer"} role-names)
              "Should pull complete role data"))))))

;;; ============================================================================
;;; Milestone 7: Distinct Queries
;;; ============================================================================

(deftest test-search-with-distinct
  (testing "Search with _distinct to get unique values by attribute"
    ;; Create users with duplicate active status (to test _distinct)
    (create-test-user! :alice "Alice" true)      ; active=true
    (create-test-user! :bob "Bob" true)          ; active=true (duplicate)
    (create-test-user! :charlie "Charlie" false) ; active=false

    (let [results (db/search-entity db/*db* :iam/user
                                    {:_where {(id/key) {:_in [(test-user-id :alice)
                                                              (test-user-id :bob)
                                                              (test-user-id :charlie)]}}
                                     :_distinct {:attributes [:active]}}
                                    {(id/key) nil
                                     :active nil})]

      (is (= 2 (count results))
          "Should return 2 distinct active values (true and false)")

      (let [active-values (set (map :active results))]
        (is (= #{true false} active-values)
            "Should have true and false as distinct active values")))))

;;; ============================================================================
;;; Run Tests
;;; ============================================================================

(comment
  ;; Run all tests
  (clojure.test/run-tests)

  ;; Run specific tests - Milestone 1
  (test-search-entity-all)
  (test-search-entity-with-limit)
  (test-search-entity-with-offset)
  (test-get-entity-by-euuid)
  (test-get-entity-nonexistent)

  ;; Milestone 2 - Filtering
  (test-search-with-equality-filter)
  (test-search-with-not-equal-filter)
  (test-search-with-is-null-filter)
  (test-search-with-is-not-null-filter)
  (test-search-with-and-filter)
  (test-search-with-or-filter)

  ;; Milestone 3 - Additional Query Operators
  (test-search-with-not-in-filter)
  (test-search-with-like-filter)
  (test-search-with-ilike-filter)

  ;; Milestone 4 - Ordering and Pagination
  (test-search-with-order-by-asc)
  (test-search-with-order-by-desc)
  (test-search-pagination-consistency)

  ;; Milestone 5 - Nested Relations
  (test-search-with-single-level-nested-relation)
  (test-search-with-multi-level-nested-relation)
  (test-search-with-empty-nested-relations)
  (test-get-entity-with-nested-relations)

  ;; Milestone 6 - Aggregation and Counting
  (test-search-with-count-on-relations)

  ;; Milestone 7 - Distinct Queries
  (test-search-with-distinct)

  ;; Manual cleanup if needed
  (cleanup-test-data!))
