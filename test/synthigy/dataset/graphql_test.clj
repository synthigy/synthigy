(ns synthigy.dataset.graphql-test
  "GraphQL query tests using real Lacinia schema execution.

  These tests verify the complete end-to-end GraphQL query flow:
  1. GraphQL query string → Lacinia parsing
  2. Lacinia execution → Resolver functions
  3. Resolver functions → SQL generation
  4. SQL execution → Results
  5. Results → GraphQL response"
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia :as lacinia]
    [synthigy.dataset :as dataset]
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

  ;; Performance tests
  (test-query-performance))
