(ns synthigy.iam.context-test
  "Tests for user context provider pattern."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.tools.logging :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.db :as db]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as context]
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
  "Track an entity UUID for cleanup"
  [euuid]
  (swap! created-test-entities conj euuid)
  euuid)

(defn cleanup-tracked-entities!
  "Delete all tracked test entities"
  []
  (doseq [euuid @created-test-entities]
    (try
      (db/delete-entity db/*db* :iam/user {(id/key) euuid})
      (catch Exception _)))  ; Ignore if doesn't exist
  (reset! created-test-entities #{}))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn cleanup-fixture
  "Clean up test data after each test"
  [f]
  (f)
  (try
    (let [entity-count (count @created-test-entities)]
      (cleanup-tracked-entities!)
      ;; Clear global provider cache to ensure test isolation
      (when context/*user-context-provider*
        (context/clear-cache context/*user-context-provider*))
      (when (pos? entity-count)
        (log/info "Cleaned up" entity-count "tracked test entities")))
    (catch Exception e
      (log/warn "Cleanup warning:" (.getMessage e)))))

;; Use comprehensive system fixture for initialization and shutdown
(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each cleanup-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

;; Counter for assigning different test data keys
(def ^:private test-user-counter (atom 0))

(defn random-user-data
  "Generates user data for testing using registered defdata IDs.
   Uses a counter to cycle through available test user IDs."
  []
  (let [idx (swap! test-user-counter inc)
        data-key (case (mod idx 3)
                   0 :test/iam-context-user-1
                   1 :test/iam-context-user-2
                   2 :test/iam-context-user-3)
        entity-id (id/data data-key)]
    (track-entity! entity-id)
    {(id/key) entity-id
     :name (str "Context Test User " idx)
     :active true}))

;;; ============================================================================
;;; AtomUserContextProvider Tests (Unit - No DB)
;;; ============================================================================

(deftest test-atom-provider-lookup-by-uuid
  (testing "AtomUserContextProvider lookup by UUID"
    (let [user-uuid (id/data :test/iam-context-user-1)
          user-data {:_eid 1
                     (id/key) user-uuid
                     :name "test-user"
                     :active true
                     :roles #{(id/data :test/iam-context-role-1)}
                     :groups #{}}
          provider (context/->AtomUserContextProvider
                    (atom {user-uuid user-data}))]

      (is (= user-data (context/lookup-user provider user-uuid))
          "Should find user by UUID"))))

(deftest test-atom-provider-lookup-by-username
  (testing "AtomUserContextProvider lookup by username"
    (let [user-uuid (id/data :test/iam-context-user-2)
          user-data {:_eid 1
                     (id/key) user-uuid
                     :name "test-user"
                     :active true
                     :roles #{}
                     :groups #{}}
          provider (context/->AtomUserContextProvider
                    (atom {user-uuid user-data}))]

      (is (= user-data (context/lookup-user provider "test-user"))
          "Should find user by username"))))

(deftest test-atom-provider-lookup-by-eid
  (testing "AtomUserContextProvider lookup by _eid"
    (let [user-uuid (id/data :test/iam-context-user-3)
          user-data {:_eid 123
                     (id/key) user-uuid
                     :name "test-user"
                     :active true
                     :roles #{}
                     :groups #{}}
          provider (context/->AtomUserContextProvider
                    (atom {user-uuid user-data}))]

      (is (= user-data (context/lookup-user provider 123))
          "Should find user by _eid"))))

(deftest test-atom-provider-lookup-not-found
  (testing "AtomUserContextProvider returns nil when user not found"
    (let [provider (context/->AtomUserContextProvider (atom {}))
          ;; Use a different ID that's not in the provider
          nonexistent-uuid (id/data :test/delete-nonexistent)]

      (is (nil? (context/lookup-user provider nonexistent-uuid))
          "Should return nil for non-existent UUID")

      (is (nil? (context/lookup-user provider "nonexistent"))
          "Should return nil for non-existent username")

      (is (nil? (context/lookup-user provider 999))
          "Should return nil for non-existent _eid"))))

;;; ============================================================================
;;; CachedUserContextProvider Tests (Integration - With DB)
;;; ============================================================================

(deftest test-cached-provider-lookup-by-username-db-fallback
  (testing "CachedUserContextProvider loads from DB on cache miss (username)"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)

          ;; Create provider with empty cache
          provider (context/->CachedUserContextProvider)]

      ;; First lookup - should hit DB and cache
      (let [result (context/lookup-user provider (:name user-data))]
        (is (some? result) "Should find user from DB")
        (is (= (id/extract user-data) (id/extract result)) "Should have correct UUID")
        (is (set? (:roles result)) "Should have roles as set")
        (is (set? (:groups result)) "Should have groups as set"))

      ;; Second lookup - should hit cache
      (let [result (context/lookup-user provider (:name user-data))]
        (is (some? result) "Should find user from cache")
        (is (= (id/extract user-data) (id/extract result)) "Should have correct UUID from cache")))))

(deftest test-cached-provider-lookup-by-uuid-db-fallback
  (testing "CachedUserContextProvider loads from DB on cache miss (UUID)"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)

          ;; Create provider with empty cache
          provider (context/->CachedUserContextProvider)]

      ;; Lookup by UUID - should hit DB and cache
      (let [result (context/lookup-user provider (id/extract user-data))]
        (is (some? result) "Should find user from DB")
        (is (= (id/extract user-data) (id/extract result)) "Should have correct UUID")))))

(deftest test-cached-provider-lookup-by-eid-db-fallback
  (testing "CachedUserContextProvider loads from DB on cache miss (_eid)"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)
          user-eid (:_eid created-user)

          ;; Create provider with empty cache
          provider (context/->CachedUserContextProvider)]

      ;; Lookup by _eid - should hit DB and cache
      (let [result (context/lookup-user provider user-eid)]
        (is (some? result) "Should find user from DB")
        (is (= (id/extract user-data) (id/extract result)) "Should have correct UUID")
        (is (= user-eid (:_eid result)) "Should have correct _eid")))))

(deftest test-cached-provider-multi-index-cache
  (testing "CachedUserContextProvider maintains multiple cache indexes"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)
          user-eid (:_eid created-user)

          ;; Create provider with empty cache
          provider (context/->CachedUserContextProvider)]

      ;; First lookup by username - populates all indexes
      (context/lookup-user provider (:name user-data))

      ;; Should now be able to lookup by UUID without DB hit
      (let [result-by-uuid (context/lookup-user provider (id/extract user-data))]
        (is (some? result-by-uuid) "Should find from cache by UUID"))

      ;; Should also be able to lookup by _eid without DB hit
      (let [result-by-eid (context/lookup-user provider user-eid)]
        (is (some? result-by-eid) "Should find from cache by _eid")))))

(deftest test-cached-provider-invalidate-user
  (testing "CachedUserContextProvider invalidates user from cache"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)

          ;; Create provider with empty cache
          provider (context/->CachedUserContextProvider)]

      ;; First lookup - should cache the user
      (let [result1 (context/lookup-user provider (:name user-data))]
        (is (some? result1) "Should find user from DB"))

      ;; Invalidate by UUID
      (context/invalidate-user provider (id/extract user-data))

      ;; Second lookup - should still work (re-fetch from DB)
      (let [result2 (context/lookup-user provider (:name user-data))]
        (is (some? result2) "Should re-fetch user from DB after invalidation")
        (is (= (id/extract user-data) (id/extract result2)) "Should have correct UUID")))))

(deftest test-cached-provider-clear-cache
  (testing "CachedUserContextProvider clears all cached users"
    (let [user1-data (random-user-data)
          user2-data (random-user-data)
          ;; Create users in DB
          _ (dataset/sync-entity :iam/user user1-data)
          _ (dataset/sync-entity :iam/user user2-data)

          ;; Create provider and populate cache
          provider (context/->CachedUserContextProvider)]

      ;; Populate cache with both users
      (let [result1 (context/lookup-user provider (:name user1-data))
            result2 (context/lookup-user provider (:name user2-data))]
        (is (some? result1) "Should find first user")
        (is (some? result2) "Should find second user"))

      ;; Clear cache
      (context/clear-cache provider)

      ;; Users should still be found (re-fetched from DB)
      (let [result1-after (context/lookup-user provider (:name user1-data))
            result2-after (context/lookup-user provider (:name user2-data))]
        (is (some? result1-after) "Should re-fetch first user from DB")
        (is (some? result2-after) "Should re-fetch second user from DB")))))

;;; ============================================================================
;;; Provider Management Tests
;;; ============================================================================

(deftest test-set-user-context-provider
  (testing "set-user-context-provider! sets global provider"
    (let [original-provider context/*user-context-provider*
          test-provider (context/->AtomUserContextProvider (atom {}))]
      (try
        ;; Set provider
        (context/set-user-context-provider! test-provider)

        ;; Verify it's set
        (is (= test-provider context/*user-context-provider*)
            "Global provider should be set")
        (finally
          ;; Restore original provider
          (context/set-user-context-provider! original-provider))))))

(deftest test-with-user-context-provider
  (testing "with-user-context-provider temporarily overrides provider"
    (let [saved-provider context/*user-context-provider*
          original-provider (context/->AtomUserContextProvider (atom {:original true}))
          temp-provider (context/->AtomUserContextProvider (atom {:temp true}))]
      (try
        ;; Set original provider for this test
        (context/set-user-context-provider! original-provider)

        ;; Temporarily override
        (context/with-user-context-provider temp-provider
          (is (= temp-provider context/*user-context-provider*)
              "Provider should be temporarily overridden"))

        ;; Verify original is restored (after with-user-context-provider scope)
        (is (= original-provider context/*user-context-provider*)
            "Original provider should be restored")
        (finally
          ;; Restore the originally saved provider
          (context/set-user-context-provider! saved-provider))))))

;;; ============================================================================
;;; High-Level API Tests
;;; ============================================================================

(deftest test-get-user-context-no-provider
  (testing "get-user-context throws when no provider configured"
    (binding [context/*user-context-provider* nil]
      (is (thrown? IllegalStateException
                   (context/get-user-context "any-identifier"))
          "Should throw when no provider configured"))))

(deftest test-get-user-context-with-provider
  (testing "get-user-context delegates to provider"
    (let [user-uuid (id/data :test/iam-context-user-4)
          user-data {:_eid 1
                     (id/key) user-uuid
                     :name "test-user"
                     :roles #{}
                     :groups #{}}
          provider (context/->AtomUserContextProvider
                    (atom {user-uuid user-data}))]

      (context/with-user-context-provider provider
        (is (= user-data (context/get-user-context user-uuid))
            "Should return user from provider")))))

(deftest test-with-user-context-binds-access-vars
  (testing "with-user-context binds access control vars"
    (let [user-uuid (id/data :test/iam-context-user-5)
          role-uuid (id/data :test/iam-context-role-2)
          group-uuid (id/data :test/iam-context-group-2)
          user-data {:_eid 1
                     (id/key) user-uuid
                     :name "test-user"
                     :active true
                     :roles #{role-uuid}
                     :groups #{group-uuid}}
          provider (context/->AtomUserContextProvider
                    (atom {user-uuid user-data}))]

      (context/with-user-context-provider provider
        (context/with-user-context user-uuid
          ;; Verify vars are bound
          (is (= user-data access/*user*)
              "access/*user* should be bound to user data")
          (is (= #{role-uuid} access/*roles*)
              "access/*roles* should be bound to role set")
          (is (= #{group-uuid} access/*groups*)
              "access/*groups* should be bound to group set"))))))

(deftest test-with-user-context-throws-when-user-not-found
  (testing "with-user-context throws when user not found"
    (let [provider (context/->AtomUserContextProvider (atom {}))]

      (context/with-user-context-provider provider
        (is (thrown? IllegalArgumentException
                     (context/with-user-context "nonexistent-user"
                       (println "Should not reach here")))
            "Should throw when user not found")))))

;;; ============================================================================
;;; Integration Workflow Tests
;;; ============================================================================

(deftest test-full-workflow-with-db
  (testing "Full workflow: create user, lookup, invalidate, re-lookup"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)
          ;; Create a fresh provider for this test
          provider (context/->CachedUserContextProvider)]

      ;; Use with-user-context-provider to ensure test isolation
      (context/with-user-context-provider provider
        ;; Lookup user - should hit DB
        (let [result1 (context/get-user-context (:name user-data))]
          (is (some? result1) "Should find user from DB")
          (is (= (id/extract user-data) (id/extract result1))))

        ;; Invalidate cache
        (context/invalidate-user provider (id/extract user-data))

        ;; Lookup again - should hit DB again
        (let [result2 (context/get-user-context (id/extract user-data))]
          (is (some? result2) "Should find user from DB after invalidation")
          (is (= (id/extract user-data) (id/extract result2))))))))

(deftest test-with-user-context-integration
  (testing "with-user-context integration with real DB user"
    (let [user-data (random-user-data)
          ;; Create user in DB
          created-user (dataset/sync-entity :iam/user user-data)
          ;; Create a fresh provider for this test
          provider (context/->CachedUserContextProvider)]

      ;; Use with-user-context-provider to ensure test isolation
      (context/with-user-context-provider provider
        ;; Use with-user-context to bind and execute
        (context/with-user-context (:name user-data)
          ;; Inside this scope, access vars should be bound
          (is (some? access/*user*) "User should be bound")
          (is (= (id/extract user-data) (id/extract access/*user*)) "Should have correct user")
          (is (set? access/*roles*) "Roles should be a set")
          (is (set? access/*groups*) "Groups should be a set"))))))
