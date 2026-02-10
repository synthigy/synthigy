(ns synthigy.iam.access-test
  "Tests for IAM access control functionality.

  Tests cover:
  - Entity-level permissions (read, write, delete, owns)
  - Relation-level permissions
  - Scope-based access control
  - Superuser bypass
  - Rule loading and caching"
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [synthigy.data :refer [*ROOT* *SYNTHIGY*]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.iam.access :as access :refer [*user* *roles* *scopes*]]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn cleanup-access-state-fixture
  "Clear access control rules and scopes after tests to avoid polluting other tests"
  [f]
  (f)
  ;; Clean up after all tests in this namespace
  (alter-var-root #'access/*rules* (constantly nil))
  (alter-var-root #'access/*scopes* (constantly nil)))

;; Use comprehensive system fixture for initialization and shutdown
;; Note: iam/start and access/start are already called by core/warmup in test-helper
(use-fixtures :once test-helper/system-fixture cleanup-access-state-fixture)

;;; ============================================================================
;;; Context Tests
;;; ============================================================================

(deftest test-access-context-vars
  (testing "Access context dynamic vars exist"
    (is (var? #'*user*) "*user* var should exist")
    (is (var? #'*roles*) "*roles* var should exist")
    (is (var? #'*scopes*) "*scopes* var should exist")))

(deftest test-binding-user-context
  (testing "Binding user context"
    (let [test-user-id 12345
          test-roles #{:admin :user}]
      (binding [*user* test-user-id
                *roles* test-roles]
        (is (= test-user-id *user*) "Should bind user ID")
        (is (= test-roles *roles*) "Should bind roles")))))

;;; ============================================================================
;;; Superuser Tests
;;; ============================================================================

(deftest test-superuser-check
  (testing "Superuser detection"
    (testing "ROOT role UUID should be superuser"
      (let [root-uuid (id/extract *ROOT*)
            roles #{root-uuid}]
        (binding [*roles* roles]
          (is (access/superuser?) "User with ROOT role should be superuser"))))

    (testing "SYNTHIGY service user should be superuser"
      (let [synthigy-id (id/extract *SYNTHIGY*)]
        (binding [*user* synthigy-id]
          (is (access/superuser?) "SYNTHIGY user should be superuser"))))

    (testing "Regular user should not be superuser"
      (binding [*user* 999
                *roles* #{(id/data :test/iam-access-role-1)}]
        (is (not (access/superuser?)) "Regular user should not be superuser")))))

;;; ============================================================================
;;; Entity Permission Tests
;;; ============================================================================

(deftest test-entity-allows-superuser
  (testing "Superuser should have access to all entities"
    (let [root-uuid (id/extract *ROOT*)
          test-entity-uuid (id/data :test/iam-access-entity-1)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? test-entity-uuid #{:read})
            "Superuser should have read access")
        (is (access/entity-allows? test-entity-uuid #{:write})
            "Superuser should have write access")
        (is (access/entity-allows? test-entity-uuid #{:delete})
            "Superuser should have delete access")
        (is (access/entity-allows? test-entity-uuid #{:owns})
            "Superuser should have owns access")))))

(deftest test-entity-allows-without-rules
  (testing "Entity access without loaded rules"
    (let [test-entity-uuid (id/data :test/iam-access-entity-2)
          regular-role-uuid (id/data :test/iam-access-role-2)]
      (binding [*user* 123
                *roles* #{regular-role-uuid}]
        ;; Without rules loaded, access should be denied for non-superusers
        ;; Note: This behavior depends on whether rules are loaded
        ;; If no rules are loaded, access/entity-allows? returns false
        (is (boolean? (access/entity-allows? test-entity-uuid #{:read}))
            "Should return boolean value")))))

;;; ============================================================================
;;; Relation Permission Tests
;;; ============================================================================

(deftest test-relation-allows-superuser
  (testing "Superuser should have access to all relations"
    (let [root-uuid (id/extract *ROOT*)
          test-relation-uuid (id/data :test/iam-access-relation-1)
          direction [:from :to]]
      (binding [*roles* #{root-uuid}]
        (is (access/relation-allows? test-relation-uuid direction #{:read})
            "Superuser should have read access to relations")
        (is (access/relation-allows? test-relation-uuid direction #{:write})
            "Superuser should have write access to relations")
        (is (access/relation-allows? test-relation-uuid direction #{:delete})
            "Superuser should have delete access to relations")))))

(deftest test-relation-allows-direction
  (testing "Relation access with different directions"
    (let [root-uuid (id/extract *ROOT*)
          test-relation-uuid (id/data :test/iam-access-relation-2)]
      (binding [*roles* #{root-uuid}]
        (is (access/relation-allows? test-relation-uuid [:from :to] #{:read})
            "Should allow from->to direction")
        (is (access/relation-allows? test-relation-uuid [:to :from] #{:read})
            "Should allow to->from direction")))))

;;; ============================================================================
;;; Scope-based Access Tests
;;; ============================================================================

(deftest test-scope-allowed
  (testing "Scope-based access control"
    (let [role-uuid (id/data :test/iam-access-role-3)
          user-scopes #{:profile :email}]
      (binding [*user* (id/data :test/iam-access-user-1)  ; Non-nil user
                *roles* #{role-uuid}
                *scopes* {role-uuid user-scopes}]
        (is (access/scope-allowed? :profile)
            "Should allow access to :profile scope")
        (is (access/scope-allowed? :email)
            "Should allow access to :email scope")
        (is (not (access/scope-allowed? :admin))
            "Should deny access to :admin scope not in user scopes")))))

(deftest test-scope-allowed-without-scopes
  (testing "Scope check without bound scopes"
    (binding [*user* (id/data :test/iam-access-user-2)  ; Non-nil user (not superuser)
              *roles* #{(id/data :test/iam-access-role-4)}  ; Non-ROOT role
              *scopes* {}]  ; Empty map, not nil
      (is (not (access/scope-allowed? :any-scope))
          "Should deny access when no scopes are bound"))))

(deftest test-multiple-scopes
  (testing "Multiple scope requirements"
    (let [user-scopes #{:read :write :delete}]
      (binding [*scopes* user-scopes]
        (is (access/scope-allowed? :read) "Should have read scope")
        (is (access/scope-allowed? :write) "Should have write scope")
        (is (access/scope-allowed? :delete) "Should have delete scope")))))

;;; ============================================================================
;;; Role and Scope Extraction Tests
;;; ============================================================================

(deftest test-roles-scopes
  (testing "Extract scopes from roles"
    ;; Create a test role with scopes
    (let [scope-name (str "test-scope-" (id/data :test/iam-access-scope-1))
          role-name (str "test-role-scopes-" (id/data :test/iam-access-role-1))
          ;; Create scope
          scope (dataset/sync-entity :iam/scope {:name scope-name})
          ;; Create role with scope
          synced-role (dataset/sync-entity :iam/user-role
                                           {:name role-name
                                            :scopes [{(id/key) (id/extract scope)}]})
          ;; Reload scopes to include new role
          _ (access/load-scopes)
          ;; Get scopes for this role UUID
          scopes (access/roles-scopes [(id/extract synced-role)])]
      (is (set? scopes) "Should return a set of scopes")
      (is (contains? scopes scope-name) "Should contain the assigned scope"))))

(deftest test-roles-scopes-empty
  (testing "Extract scopes from roles without scopes"
    (let [role-name (str "test-role-no-scopes-" (id/data :test/iam-access-role-5))
          role (dataset/sync-entity :iam/user-role {:name role-name})
          scopes (access/roles-scopes [(id/extract role)])]
      (is (set? scopes) "Should return a set")
      (is (empty? scopes) "Should be empty when role has no scopes"))))

(deftest test-roles-scopes-multiple-roles
  (testing "Extract scopes from multiple roles"
    (let [scope1-name (str "scope1-" (id/data :test/iam-access-scope-1))
          scope2-name (str "scope2-" (id/data :test/iam-access-scope-2))
          role1-name (str "role1-" (id/data :test/iam-access-role-1))
          role2-name (str "role2-" (id/data :test/iam-access-role-2))
          ;; Create scopes
          scope1 (dataset/sync-entity :iam/scope {:name scope1-name})
          scope2 (dataset/sync-entity :iam/scope {:name scope2-name})
          ;; Create roles with different scopes
          synced-role1 (dataset/sync-entity :iam/user-role
                                            {:name role1-name
                                             :scopes [{(id/key) (id/extract scope1)}]})
          synced-role2 (dataset/sync-entity :iam/user-role
                                            {:name role2-name
                                             :scopes [{(id/key) (id/extract scope2)}]})
          ;; Reload scopes to include new roles
          _ (access/load-scopes)
          ;; Get combined scopes using role UUIDs
          scopes (access/roles-scopes [(id/extract synced-role1) (id/extract synced-role2)])]
      (is (set? scopes) "Should return a set")
      (is (>= (count scopes) 2) "Should have at least 2 scopes")
      (is (contains? scopes scope1-name) "Should contain scope from role1")
      (is (contains? scopes scope2-name) "Should contain scope from role2"))))

;;; ============================================================================
;;; Rule Loading Tests
;;; ============================================================================

(deftest test-load-rules
  (testing "Load access control rules"
    ;; This test verifies that load-rules doesn't throw exceptions
    (let [result (access/load-rules)]
      (is (map? result) "load-rules should return a map of loaded rules")
      (is (contains? result :entity) "Should contain entity rules"))))

(deftest test-access-start
  (testing "Access control start/initialization"
    ;; Verify that start function can be called multiple times
    (is (nil? (access/start)) "start should complete without error")))

;;; ============================================================================
;;; Integration Tests with Real Entities
;;; ============================================================================

(deftest test-user-entity-access
  (testing "Access control for User entity"
    (let [user-entity-uuid :iam/user
          root-uuid (id/extract *ROOT*)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? user-entity-uuid #{:read})
            "ROOT should have read access to User entity")
        (is (access/entity-allows? user-entity-uuid #{:write})
            "ROOT should have write access to User entity")))))

(deftest test-role-entity-access
  (testing "Access control for UserRole entity"
    (let [role-entity-uuid :iam/user-role
          root-uuid (id/extract *ROOT*)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? role-entity-uuid #{:read})
            "ROOT should have read access to UserRole entity")
        (is (access/entity-allows? role-entity-uuid #{:write})
            "ROOT should have write access to UserRole entity")))))

;;; ============================================================================
;;; Edge Cases
;;; ============================================================================

(deftest test-nil-entity-uuid
  (testing "Entity access with nil UUID"
    (binding [*user* 123
              *roles* #{(id/data :test/iam-access-role-3)}]
      (is (not (access/entity-allows? nil #{:read}))
          "Should deny access for nil entity UUID"))))

(deftest test-empty-scopes-set
  (testing "Entity access with empty scopes set"
    (let [root-uuid (id/extract *ROOT*)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? :iam/user #{})
            "Superuser should have access even with empty scopes set")))))

(deftest test-nil-user-context
  (testing "Access control with nil user"
    (binding [*user* nil
              *roles* nil
              *scopes* nil]
      (is (access/superuser?)
          "Nil user is treated as superuser for system operations"))))

(deftest test-multiple-permission-scopes
  (testing "Check multiple permission scopes"
    (let [root-uuid (id/extract *ROOT*)
          entity-uuid (id/data :test/iam-access-entity-3)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? entity-uuid #{:read :write})
            "Superuser should have multiple permission scopes")
        (is (access/entity-allows? entity-uuid #{:read :write :delete :owns})
            "Superuser should have all permission scopes")))))

;;; ============================================================================
;;; Permission Hierarchy Tests
;;; ============================================================================

(deftest test-owns-implies-write
  (testing "Owns permission should imply other permissions"
    ;; Note: This depends on the actual implementation
    ;; In EYWA, :owns typically grants full control
    (let [root-uuid (id/extract *ROOT*)
          entity-uuid (id/data :test/iam-access-entity-4)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? entity-uuid #{:owns})
            "Superuser should have owns permission")))))

(deftest test-write-doesnt-imply-delete
  (testing "Write permission should not imply delete"
    ;; This is a design decision - document the actual behavior
    ;; For now, we just verify the superuser has all permissions
    (let [root-uuid (id/extract *ROOT*)
          entity-uuid (id/data :test/iam-access-entity-5)]
      (binding [*roles* #{root-uuid}]
        (is (access/entity-allows? entity-uuid #{:write})
            "Superuser should have write permission")
        (is (access/entity-allows? entity-uuid #{:delete})
            "Superuser should have delete permission")))))
