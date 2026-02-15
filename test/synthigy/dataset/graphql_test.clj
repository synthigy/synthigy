(ns synthigy.dataset.graphql-test
  "GraphQL query tests using real Lacinia schema execution.

  These tests verify the complete end-to-end GraphQL query flow:
  1. GraphQL query string → Lacinia parsing
  2. Lacinia execution → Resolver functions
  3. Resolver functions → SQL generation
  4. SQL execution → Results
  5. Results → GraphQL response"
  (:require
    [clojure.core.async :as async]
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia :as lacinia]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.graphql :as dataset-graphql]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.lacinia :as lac]
    [synthigy.dataset.operations] ; Ensure protocol implementations are loaded
    [synthigy.lacinia]
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn setup-schema-fixture
  "Generates and compiles the GraphQL schema after system initialization.

  This fixture runs once after test-helper/system-fixture to set up the schema cache."
  [f]
  (log/info "Setting up GraphQL schema for tests...")

  (def model (dataset/deployed-model))
  ;; Get current deployed model (already loaded by core/warmup in test-helper)
  (let [model (dataset/deployed-model)]
    (if (and model (instance? synthigy.dataset.core.ERDModel model))
      (do
        (log/infof "Using deployed model with %d entities" (count (core/get-entities model)))

        ;; Generate and deploy Lacinia GraphQL schema
        (let [gql-schema (lac/generate-lacinia-schema model)]
          (log/infof "Generated GraphQL schema with %d queries, %d mutations"
                     (count (get-in gql-schema [:objects :Query :fields]))
                     (count (get-in gql-schema [:objects :Mutation :fields])))
          (def gql-schema gql-schema)
          ;; Register dataset schema as a shard in the global schema
          (synthigy.lacinia/add-shard :synthigy.dataset/schema gql-schema))

        (log/info "GraphQL schema setup complete"))
      (log/warn "No deployed model found - skipping GraphQL schema generation")))

  ;; Run tests
  (f))

;; Use comprehensive system fixture for initialization and shutdown
;; Then set up GraphQL schema
(use-fixtures :once test-helper/system-fixture setup-schema-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn execute-query
  "Executes a GraphQL query string against the deployed schema.

  Args:
    query-str - GraphQL query string
    variables - Optional map of variables
    context - Optional execution context

  Returns:
    Result map with :data and :errors keys"
  ([query-str]
   (execute-query query-str nil nil))
  ([query-str variables]
   (execute-query query-str variables nil))
  ([query-str variables context]
   (let [schema @synthigy.lacinia/compiled]
     (when-not schema
       (throw (ex-info "No compiled schema available" {})))
     (lacinia/execute schema query-str variables context))))

(defn successful?
  "Checks if a GraphQL result is successful (no errors)."
  [result]
  (nil? (:errors result)))

(defn get-data
  "Extracts :data from GraphQL result, throwing if there are errors."
  [result]
  (when-let [errors (:errors result)]
    (throw (ex-info "GraphQL query returned errors"
                    {:errors errors
                     :result result})))
  (:data result))

;;; ============================================================================
;;; Basic Query Tests
;;; ============================================================================

(deftest test-simple-user-query
  (testing "Simple user search query"
    (let [query (str "
query {
  searchUser(_limit: 5) {
    " (id/field) "
    name
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Query should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        (is (<= (count users) 5)
            "Should return at most 5 users")

        (when (seq users)
          (let [first-user (first users)]
            (is (contains? first-user (id/key))
                "User should have euuid field")
            (is (contains? first-user :name)
                "User should have name field")))))))

(deftest test-filtered-user-query
  (testing "User search with WHERE filter"
    (let [query (str "
query {
  searchUser(
    _where: { active: { _boolean: TRUE } }
    _limit: 3
  ) {
    " (id/field) "
    name
    active
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Filtered query should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        (when (seq users)
          (doseq [user users]
            (is (true? (:active user))
                "All returned users should be active")))))))

#_(deftest test-ordered-user-query
    (testing "User search with ORDER BY"
      (let [query (str "
query {
  searchUser(
    _order_by: { name: asc }
    _limit: 10
  ) {
    " (id/field) "
    name
  }
}")
            result (execute-query query)]

        (is (successful? result)
            "Ordered query should execute without errors")

        (let [data (get-data result)
              users (:searchUser data)
              names (mapv :name users)]

          (is (vector? users)
              "searchUser should return a vector")

          (when (> (count names) 1)
          ;; SQL ORDER BY is case-sensitive by default
            (is (= names (sort names))
                "Users should be sorted by name in ascending order"))))))

(deftest test-get-single-user
  (testing "Get single user by euuid"
    ;; First, get a user euuid
    (let [search-result (execute-query (str "
query {
  searchUser(_limit: 1) {
    " (id/field) "
    name
  }
}"))
          users (:searchUser (get-data search-result))]

      (when (seq users)
        (let [user-euuid (str (id/extract (first users)))
              ;; Build query string properly without escaping issues
              query (str "query { getUser(" (id/field) ": \"" user-euuid "\") { " (id/field) " name } }")
              result (execute-query query)]

          (is (successful? result)
              "getUser query should execute without errors")

          (let [data (get-data result)
                user (:getUser data)]

            (is (map? user)
                "getUser should return a single user object")

            (is (= user-euuid (str (id/extract user)))
                "Returned user should have the requested euuid")))))))

(deftest test-complex-where-query
  (testing "Complex WHERE with AND/OR conditions"
    (let [query (str "
query {
  searchUser(
    _where: {
      _and: [
        { active: { _boolean: TRUE } }
        { name: { _neq: null } }
      ]
    }
    _limit: 5
  ) {
    " (id/field) "
    name
    active
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Complex WHERE query should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        (when (seq users)
          (doseq [user users]
            (is (true? (:active user))
                "All users should be active")
            (is (some? (:name user))
                "All users should have a name")))))))

(deftest test-pagination
  (testing "Pagination with limit and offset"
    (let [;; Get first page
          page1-query (str "
query {
  searchUser(_limit: 2, _offset: 0, _order_by: { name: asc }) {
    " (id/field) "
    name
  }
}")
          ;; Get second page
          page2-query (str "
query {
  searchUser(_limit: 2, _offset: 2, _order_by: { name: asc }) {
    " (id/field) "
    name
  }
}")
          page1-result (execute-query page1-query)
          page2-result (execute-query page2-query)]

      (is (successful? page1-result) "Page 1 query should succeed")
      (is (successful? page2-result) "Page 2 query should succeed")

      (let [page1-users (:searchUser (get-data page1-result))
            page2-users (:searchUser (get-data page2-result))]

        (is (<= (count page1-users) 2) "Page 1 should have at most 2 users")
        (is (<= (count page2-users) 2) "Page 2 should have at most 2 users")

        ;; Verify pages don't overlap
        (when (and (seq page1-users) (seq page2-users))
          (let [page1-ids (set (map id/extract page1-users))
                page2-ids (set (map id/extract page2-users))]
            (is (empty? (clojure.set/intersection page1-ids page2-ids))
                "Pages should not contain overlapping users")))))))

;;; ============================================================================
;;; Nested Relations Tests
;;; ============================================================================

(deftest test-single-level-nested-relation
  (testing "Single-level nested relation (User → Roles)"
    (let [query (str "
query {
  searchUser(_limit: 1) {
    " (id/field) "
    name
    roles {
      " (id/field) "
      name
    }
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Single-level nested query should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        (when (seq users)
          (let [user (first users)]
            (is (contains? user (id/key))
                "User should have euuid field")
            (is (contains? user :name)
                "User should have name field")
            (is (contains? user :roles)
                "User should have roles field")

            (when-let [roles (:roles user)]
              (is (vector? roles)
                  "Roles should be a vector")
              (when (seq roles)
                (let [role (first roles)]
                  (is (contains? role (id/key))
                      "Role should have euuid field")
                  (is (contains? role :name)
                      "Role should have name field"))))))))))

(deftest test-two-level-nested-relation
  (testing "Two-level nested relation (User → Roles → Users)"
    (let [query (str "
query {
  searchUser(_limit: 1) {
    " (id/field) "
    name
    roles {
      " (id/field) "
      name
      users {
        " (id/field) "
        name
      }
    }
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Two-level nested query should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        (when (seq users)
          (let [user (first users)]
            (is (contains? user :roles)
                "User should have roles field")

            (when-let [roles (:roles user)]
              (when (seq roles)
                (let [role (first roles)]
                  (is (contains? role :users)
                      "Role should have users field")

                  (when-let [role-users (:users role)]
                    (is (vector? role-users)
                        "Role users should be a vector")
                    (when (seq role-users)
                      (let [role-user (first role-users)]
                        (is (contains? role-user (id/key))
                            "Role user should have euuid field")
                        (is (contains? role-user :name)
                            "Role user should have name field")))))))))))))

(deftest test-multiple-relations-same-level
  (testing "Multiple relations at same level (User → Roles + Projects if exists)"
    (let [query (str "
query {
  searchUser(_limit: 1) {
    " (id/field) "
    name
    roles {
      " (id/field) "
      name
    }
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Multiple relations query should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        (when (seq users)
          (let [user (first users)]
            (is (contains? user :roles)
                "User should have roles field")

            ;; Verify each relation is independent
            (when-let [roles (:roles user)]
              (is (vector? roles)
                  "Roles should be a vector"))))))))

(deftest test-nested-relation-with-filters
  (testing "Nested relation with WHERE filters"
    (let [query (str "
query {
  searchUser(
    _where: { active: { _boolean: TRUE } }
    _limit: 1
  ) {
    " (id/field) "
    name
    active
    roles {
      " (id/field) "
      name
    }
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Nested query with filters should execute without errors")

      (let [data (get-data result)
            users (:searchUser data)]

        (when (seq users)
          (let [user (first users)]
            (is (true? (:active user))
                "User should be active")
            (is (contains? user :roles)
                "User should have roles field")))))))

(deftest test-empty-nested-relations
  (testing "Handle users with no nested relations gracefully"
    (let [query (str "
query {
  searchUser(_limit: 10) {
    " (id/field) "
    name
    roles {
      " (id/field) "
      name
    }
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Query should execute even if some users have no roles")

      (let [data (get-data result)
            users (:searchUser data)]

        (is (vector? users)
            "searchUser should return a vector")

        ;; All users should have roles field, even if empty
        (doseq [user users]
          (is (contains? user :roles)
              "Every user should have roles field (even if empty vector)"))))))

;;; ============================================================================
;;; Schema Introspection Tests
;;; ============================================================================

(deftest test-schema-introspection
  (testing "GraphQL schema introspection"
    (let [query "
query {
  __schema {
    queryType {
      name
    }
    mutationType {
      name
    }
  }
}"
          result (execute-query query)]

      (is (successful? result)
          "Introspection query should execute without errors")

      (let [data (get-data result)
            schema (:__schema data)]

        (is (= "Query" (get-in schema [:queryType :name]))
            "Query type should be named Query")

        (is (= "Mutation" (get-in schema [:mutationType :name]))
            "Mutation type should be named Mutation")))))

(deftest test-type-introspection
  (testing "Type introspection for User entity"
    (let [query "
query {
  __type(name: \"User\") {
    name
    kind
    fields {
      name
      type {
        name
        kind
      }
    }
  }
}"
          result (execute-query query)]

      (is (successful? result)
          "Type introspection query should execute without errors")

      (let [data (get-data result)
            user-type (:__type data)]

        (is (= "User" (:name user-type))
            "Type should be named User")

        (is (= :OBJECT (:kind user-type))
            "User should be an OBJECT type")

        (let [fields (:fields user-type)
              field-names (set (map :name fields))]

          (is (contains? field-names (id/field))
              "User type should have ID field")

          (is (contains? field-names "name")
              "User type should have name field"))))))

;;; ============================================================================
;;; Error Handling Tests
;;; ============================================================================

(deftest test-invalid-field-error
  (testing "Query with invalid field should return error"
    (let [query (str "
query {
  searchUser(limit: 1) {
    " (id/field) "
    invalidField
  }
}")
          result (execute-query query)]

      (is (not (successful? result))
          "Query with invalid field should return errors")

      (is (seq (:errors result))
          "Should have at least one error"))))

(deftest test-invalid-argument-type
  (testing "Query with wrong argument type should return error"
    (let [query (str "
query {
  searchUser(limit: \\\"not-a-number\\\") {
    " (id/field) "
  }
}")
          result (execute-query query)]

      (is (not (successful? result))
          "Query with invalid argument type should return errors"))))

;;; ============================================================================
;;; Test Data Helpers
;;; ============================================================================

(defn- unique-name
  "Generates a unique test name with prefix."
  [prefix]
  (str prefix "-" (subs (str (random-uuid)) 0 8)))

(defn- create-test-user!
  "Creates a test user and returns the result data."
  [name-val]
  (let [mutation (str "mutation { syncUser(data: { name: \"" name-val "\", active: true }) { " (id/field) " name active } }")
        result (execute-query mutation)]
    (when (successful? result)
      (:syncUser (get-data result)))))

(defn- delete-test-user!
  "Deletes a test user by ID."
  [user-id]
  (execute-query (str "mutation { deleteUser(" (id/field) ": \"" user-id "\") }")))

;;; ============================================================================
;;; Mutation Tests
;;; ============================================================================

(deftest test-sync-mutation-create
  (testing "syncUser creates new user"
    (let [test-name (unique-name "test-sync")
          mutation (str "mutation { syncUser(data: { name: \"" test-name "\", active: true, priority: 5 }) { " (id/field) " name active priority } }")
          result (execute-query mutation)]

      (is (successful? result)
          "syncUser mutation should execute without errors")

      (let [user (:syncUser (get-data result))]
        (is (some? (id/extract user))
            "Created user should have an ID")
        (is (= test-name (:name user))
            "User name should match input")
        (is (true? (:active user))
            "User should be active")
        (is (= 5 (:priority user))
            "User priority should be 5")

        ;; Cleanup
        (delete-test-user! (str (id/extract user)))))))

(deftest test-sync-mutation-update
  (testing "syncUser updates existing user"
    (let [test-name (unique-name "test-sync-update")
          ;; Create user first
          created (create-test-user! test-name)
          _ (is (some? created) "User should be created")]

      (when created
        (let [user-id (str (id/extract created))
              ;; Update with same ID
              update-mutation (str "mutation { syncUser(data: { " (id/field) ": \"" user-id "\", name: \"" test-name "-updated\", active: false }) { " (id/field) " name active } }")
              update-result (execute-query update-mutation)]

          (is (successful? update-result)
              "syncUser update should succeed")

          (let [updated-user (:syncUser (get-data update-result))]
            (is (= user-id (str (id/extract updated-user)))
                "ID should remain the same")
            (is (= (str test-name "-updated") (:name updated-user))
                "Name should be updated")
            (is (false? (:active updated-user))
                "Active should be updated to false"))

          ;; Cleanup
          (delete-test-user! user-id))))))

(deftest test-stack-mutation
  (testing "stackUser creates new user"
    (let [test-name (unique-name "test-stack")
          mutation (str "mutation { stackUser(data: { name: \"" test-name "\", active: true }) { " (id/field) " name active } }")
          result (execute-query mutation)]

      (is (successful? result)
          "stackUser mutation should execute without errors")

      (let [user (:stackUser (get-data result))]
        (is (some? (id/extract user))
            "Created user should have an ID")
        (is (= test-name (:name user))
            "User name should match input")

        ;; Cleanup
        (delete-test-user! (str (id/extract user)))))))

(deftest test-batch-sync-mutation
  (testing "syncUserList creates multiple users"
    (let [names [(unique-name "batch-1") (unique-name "batch-2") (unique-name "batch-3")]
          data-str (str/join ", " (map #(str "{ name: \"" % "\", active: true }") names))
          mutation (str "mutation { syncUserList(data: [" data-str "]) { " (id/field) " name } }")
          result (execute-query mutation)]

      (is (successful? result)
          "syncUserList mutation should execute without errors")

      (let [users (:syncUserList (get-data result))]
        (is (= 3 (count users))
            "Should create 3 users")
        (is (= (set names) (set (map :name users)))
            "User names should match input")

        ;; Cleanup
        (doseq [user users]
          (delete-test-user! (str (id/extract user))))))))

(deftest test-delete-mutation
  (testing "deleteUser soft deletes user"
    (let [test-name (unique-name "test-delete")
          created (create-test-user! test-name)
          _ (is (some? created) "User should be created")]

      (when created
        (let [user-id (str (id/extract created))
              mutation (str "mutation { deleteUser(" (id/field) ": \"" user-id "\") }")
              result (execute-query mutation)]

          (is (successful? result)
              "deleteUser mutation should execute without errors")

          (is (true? (:deleteUser (get-data result)))
              "deleteUser should return true")

          ;; Verify user no longer appears in normal search
          (let [search (execute-query (str "query { getUser(" (id/field) ": \"" user-id "\") { " (id/field) " } }"))]
            (is (nil? (:getUser (get-data search)))
                "Deleted user should not be found")))))))

(deftest test-purge-mutation
  (testing "purgeUser hard deletes matching users"
    (let [pattern (unique-name "purge-test")
          ;; Create 3 users with pattern
          names [(str pattern "-a") (str pattern "-b") (str pattern "-c")]
          _ (doseq [n names] (create-test-user! n))
          ;; Purge by name pattern
          mutation (str "mutation { purgeUser(_where: { name: { _like: \"" pattern "%\" } }) { " (id/field) " name } }")
          result (execute-query mutation)]

      (is (successful? result)
          "purgeUser mutation should execute without errors")

      (let [purged (:purgeUser (get-data result))]
        (is (>= (count purged) 3)
            "Should purge at least 3 users")))))

(deftest test-slice-mutation
  (testing "sliceUser deletes relations matching filter"
    (let [pattern (unique-name "slice-test")
          ;; Create users with pattern
          names [(str pattern "-1") (str pattern "-2")]
          created-users (doall (map create-test-user! names))
          _ (is (every? some? created-users) "Users should be created")]

      (when (every? some? created-users)
        (let [;; Slice - slice returns relation deletion status
              mutation (str "mutation { sliceUser(_where: { name: { _like: \"" pattern "%\" } }) { roles groups } }")
              result (execute-query mutation)]

          (is (successful? result)
              "sliceUser mutation should execute without errors")

          (let [sliced (:sliceUser (get-data result))]
            (is (map? sliced)
                "sliceUser should return a map"))

          ;; Cleanup created users
          (doseq [user created-users]
            (delete-test-user! (str (id/extract user)))))))))

;;; ============================================================================
;;; Aggregation Tests
;;; ============================================================================

(deftest test-count-aggregation
  (testing "aggregateUser returns count"
    (let [query "query { aggregateUser { count } }"
          result (execute-query query)]

      (is (successful? result)
          "Count aggregation should execute without errors")

      (let [agg (:aggregateUser (get-data result))]
        (is (integer? (:count agg))
            "Count should be an integer")
        (is (>= (:count agg) 0)
            "Count should be non-negative")))))

(deftest test-count-aggregation-with-filter
  (testing "aggregateUser with _where filter"
    (let [query "query { aggregateUser(_where: { active: { _boolean: TRUE } }) { count } }"
          result (execute-query query)]

      (is (successful? result)
          "Filtered count should execute without errors")

      (let [agg (:aggregateUser (get-data result))]
        (is (integer? (:count agg))
            "Filtered count should be an integer")))))

(deftest test-numeric-aggregations
  (testing "aggregateUser returns numeric aggregations for priority field"
    (let [query "query { aggregateUser { count priority { min max sum avg } } }"
          result (execute-query query)]

      (is (successful? result)
          "Numeric aggregations should execute without errors")

      (let [agg (:aggregateUser (get-data result))
            priority (:priority agg)]
        (is (map? priority)
            "Priority aggregations should be a map")
        (is (contains? priority :min)
            "Should contain min")
        (is (contains? priority :max)
            "Should contain max")
        (is (contains? priority :sum)
            "Should contain sum")
        (is (contains? priority :avg)
            "Should contain avg")))))

(deftest test-numeric-aggregations-with-filter
  (testing "aggregateUser numeric aggregations with filter"
    (let [query "query { aggregateUser(_where: { active: { _boolean: TRUE } }) { count priority { min max } } }"
          result (execute-query query)]

      (is (successful? result)
          "Filtered numeric aggregations should execute without errors")

      (let [agg (:aggregateUser (get-data result))]
        (is (integer? (:count agg))
            "Count should be present")
        (is (map? (:priority agg))
            "Priority aggregations should be present")))))

(deftest test-nested-count
  (testing "User _count returns relation counts"
    (let [query (str "query { searchUser(_limit: 3) { " (id/field) " name _count { roles } } }")
          result (execute-query query)]

      (is (successful? result)
          "_count field should execute without errors")

      (let [users (:searchUser (get-data result))]
        (when (seq users)
          (doseq [user users]
            (is (map? (:_count user))
                "User should have _count field")
            (is (integer? (get-in user [:_count :roles]))
                "_count roles should be an integer")))))))

(deftest test-nested-count-multiple-relations
  (testing "User _count with multiple relations"
    (let [query (str "query { searchUser(_limit: 1) { " (id/field) " _count { roles groups } } }")
          result (execute-query query)]

      (is (successful? result)
          "_count with multiple relations should execute without errors")

      (when-let [user (first (:searchUser (get-data result)))]
        (is (map? (:_count user))
            "User should have _count field")
        (when (contains? (:_count user) :roles)
          (is (integer? (get-in user [:_count :roles]))
              "_count roles should be an integer"))
        (when (contains? (:_count user) :groups)
          (is (integer? (get-in user [:_count :groups]))
              "_count groups should be an integer"))))))

;;; ============================================================================
;;; Advanced Query Features Tests
;;; ============================================================================

(deftest test-distinct-query
  (testing "searchUser with _distinct executes successfully"
    (let [query (str "query { searchUser(_distinct: { attributes: [active] }, _limit: 10) { " (id/field) " active } }")
          result (execute-query query)]

      (is (successful? result)
          "_distinct query should execute without errors")

      (let [users (:searchUser (get-data result))]
        (is (vector? users)
            "Should return a vector")
        (is (<= (count users) 10)
            "Should respect limit")))))

(deftest test-join-type-left
  (testing "Nested relation with explicit LEFT join"
    (let [query (str "query { searchUser(_limit: 1) { " (id/field) " name roles(_join: LEFT) { " (id/field) " name } } }")
          result (execute-query query)]

      (is (successful? result)
          "Query with LEFT join should execute without errors")

      (let [users (:searchUser (get-data result))]
        (when (seq users)
          (let [user (first users)]
            (is (contains? user :roles)
                "User should have roles field even with LEFT join")))))))

(deftest test-join-type-inner
  (testing "Nested relation with explicit INNER join"
    (let [query (str "query { searchUser(_limit: 5) { " (id/field) " name roles(_join: INNER) { " (id/field) " name } } }")
          result (execute-query query)]

      (is (successful? result)
          "Query with INNER join should execute without errors")

      (let [users (:searchUser (get-data result))]
        (is (vector? users)
            "Should return a vector")
        ;; Just verify query executes - INNER join behavior depends on data
        (is (every? #(contains? % :roles) users)
            "All users should have roles field")))))

(deftest test-combined-features
  (testing "Query combining multiple advanced features"
    (let [query (str "query {
  searchUser(
    _where: { active: { _boolean: TRUE } }
    _order_by: { name: asc }
    _limit: 5
  ) {
    " (id/field) "
    name
    active
    roles(_limit: 2) {
      " (id/field) "
      name
    }
    _count {
      roles
    }
  }
}")
          result (execute-query query)]

      (is (successful? result)
          "Combined query should execute without errors")

      (let [users (:searchUser (get-data result))]
        (is (vector? users)
            "Should return a vector")
        (doseq [user users]
          (is (true? (:active user))
              "All users should be active")
          (is (contains? user :roles)
              "All users should have roles field")
          (is (contains? user :_count)
              "All users should have _count field"))))))

;;; ============================================================================
;;; Subscription Tests
;;; ============================================================================

(deftest test-subscription-on-deploy-initial
  (testing "refreshedGlobalDataset sends initial model on subscribe"
    (let [received (atom [])
          upstream (fn [data] (swap! received conj data))
          context {:username "test-user"}
          ;; Call the streamer directly
          cleanup-fn (dataset-graphql/on-deploy context {} upstream)]

      ;; Wait briefly for async initial send
      (Thread/sleep 100)

      (is (fn? cleanup-fn)
          "on-deploy should return a cleanup function")

      (is (= 1 (count @received))
          "Should receive exactly one initial message")

      (let [initial-msg (first @received)]
        (is (= "Global" (:name initial-msg))
            "Initial message should have name 'Global'")
        (is (some? (:model initial-msg))
            "Initial message should include the model"))

      ;; Cleanup
      (cleanup-fn))))

(deftest test-subscription-on-deploy-publish
  (testing "refreshedGlobalDataset receives published events"
    (let [received (atom [])
          upstream (fn [data] (swap! received conj data))
          context {:username "test-user"}
          cleanup-fn (dataset-graphql/on-deploy context {} upstream)]

      ;; Wait for initial message
      (Thread/sleep 100)
      (reset! received []) ; Clear initial message

      ;; Publish an event
      (let [test-model {:test "model"}
            test-event {:topic :refreshedGlobalDataset
                        :data {:name "TestDeploy" :model test-model}}]
        (async/put! dataset/subscription test-event)

        ;; Wait for event to propagate
        (Thread/sleep 100)

        (is (= 1 (count @received))
            "Should receive the published event")

        (when (seq @received)
          (let [event (first @received)]
            (is (= "TestDeploy" (:name event))
                "Should receive event with correct name")
            (is (= test-model (:model event))
                "Should receive event with correct model"))))

      ;; Cleanup
      (cleanup-fn))))

(deftest test-subscription-on-deploy-cleanup
  (testing "refreshedGlobalDataset cleanup stops receiving events"
    (let [received (atom [])
          upstream (fn [data] (swap! received conj data))
          context {:username "test-user"}
          cleanup-fn (dataset-graphql/on-deploy context {} upstream)]

      ;; Wait for initial message
      (Thread/sleep 100)
      (reset! received [])

      ;; Call cleanup
      (cleanup-fn)

      ;; Publish an event after cleanup
      (let [test-event {:topic :refreshedGlobalDataset
                        :data {:name "AfterCleanup" :model {}}}]
        (async/put! dataset/subscription test-event)

        ;; Wait
        (Thread/sleep 100)

        (is (empty? @received)
            "Should not receive events after cleanup")))))

(deftest test-subscription-multiple-subscribers
  (testing "refreshedGlobalDataset supports multiple concurrent subscribers"
    (let [received-1 (atom [])
          received-2 (atom [])
          upstream-1 (fn [data] (swap! received-1 conj data))
          upstream-2 (fn [data] (swap! received-2 conj data))
          context {:username "test-user"}
          cleanup-1 (dataset-graphql/on-deploy context {} upstream-1)
          cleanup-2 (dataset-graphql/on-deploy context {} upstream-2)]

      ;; Wait for initial messages
      (Thread/sleep 100)

      (is (= 1 (count @received-1))
          "Subscriber 1 should receive initial message")
      (is (= 1 (count @received-2))
          "Subscriber 2 should receive initial message")

      ;; Clear and publish
      (reset! received-1 [])
      (reset! received-2 [])

      (let [test-event {:topic :refreshedGlobalDataset
                        :data {:name "MultiTest" :model {}}}]
        (async/put! dataset/subscription test-event)
        (Thread/sleep 100)

        (is (= 1 (count @received-1))
            "Subscriber 1 should receive published event")
        (is (= 1 (count @received-2))
            "Subscriber 2 should receive published event"))

      ;; Cleanup one subscriber
      (cleanup-1)
      (reset! received-1 [])
      (reset! received-2 [])

      (let [test-event {:topic :refreshedGlobalDataset
                        :data {:name "AfterPartialCleanup" :model {}}}]
        (async/put! dataset/subscription test-event)
        (Thread/sleep 100)

        (is (empty? @received-1)
            "Cleaned up subscriber should not receive events")
        (is (= 1 (count @received-2))
            "Active subscriber should still receive events"))

      ;; Cleanup remaining subscriber
      (cleanup-2))))

;;; ============================================================================
;;; Performance Tests (Optional)
;;; ============================================================================

(deftest ^:performance test-query-performance
  (testing "Query execution performance"
    (let [query (str "
query {
  searchUser(_limit: 100) {
    " (id/field) "
    name
    active
  }
}")
          start (System/nanoTime)
          result (execute-query query)
          end (System/nanoTime)
          duration-ms (/ (- end start) 1000000.0)]

      (is (successful? result)
          "Performance test query should succeed")

      (log/infof "Query executed in %.2f ms" duration-ms)

      (is (< duration-ms 1000)
          "Query should complete in less than 1 second"))))

;;; ============================================================================
;;; Run Tests
;;; ============================================================================

(comment
  ;; Run all tests
  (clojure.test/run-tests)

  ;; Run specific test - Basic queries
  (test-simple-user-query)
  (test-filtered-user-query)
  (test-ordered-user-query)
  (test-get-single-user) ; Currently skipped - get-entity not implemented
  (test-complex-where-query)
  (test-pagination)

  ;; Nested relation tests
  (test-single-level-nested-relation)
  (test-two-level-nested-relation)
  (test-multiple-relations-same-level)
  (test-nested-relation-with-filters)
  (test-empty-nested-relations)

  ;; Schema introspection
  (test-schema-introspection)
  (test-type-introspection)

  ;; Mutation tests
  (test-sync-mutation-create)
  (test-sync-mutation-update)
  (test-stack-mutation)
  (test-batch-sync-mutation)
  (test-delete-mutation)
  (test-purge-mutation)
  (test-slice-mutation)

  ;; Aggregation tests
  (test-count-aggregation)
  (test-count-aggregation-with-filter)
  (test-numeric-aggregations)
  (test-numeric-aggregations-with-filter)
  (test-nested-count)
  (test-nested-count-multiple-relations)

  ;; Advanced query features
  (test-distinct-query)
  (test-join-type-left)
  (test-join-type-inner)
  (test-combined-features)

  ;; Subscription tests
  (test-subscription-on-deploy-initial)
  (test-subscription-on-deploy-publish)
  (test-subscription-on-deploy-cleanup)
  (test-subscription-multiple-subscribers)

  ;; Performance tests
  (test-query-performance))
