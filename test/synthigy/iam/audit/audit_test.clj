(ns synthigy.iam.audit.audit-test
  "Tests for audit enhancement (modified_by, modified_on).

  These tests verify audit behavior through the abstraction layer
  and work on any database backend (Postgres, SQLite, etc.)."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [synthigy.dataset.id :as id]
    [synthigy.db :as db]
    [synthigy.iam.context :as context]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]))

;;; ============================================================================
;;; Fixed Test Data
;;; ============================================================================

(def test-audit-user-keys
  "Mapping of test user roles to their registered test data keys."
  {:modifier-user :test/audit-modifier
   :creator-user :test/audit-creator-pg
   :test-user-1 :test/audit-user-1
   :test-user-2 :test/audit-user-2
   :updater-user :test/audit-updater-pg})

(defn test-audit-user-id
  "Get the ID for a test audit user based on current provider."
  [k]
  (id/data (get test-audit-user-keys k)))

(defn test-audit-user-data
  "Generate user data map for a test audit user."
  [k]
  (let [k-name (name k)
        ;; Strip "test-" prefix if present to avoid "audit-test-test-user-1"
        suffix (if (clojure.string/starts-with? k-name "test-")
                 (subs k-name 5)
                 k-name)]
    {(id/key) (test-audit-user-id k)
     :name (str "audit-test-" suffix)
     :active true}))

(defn test-users
  "Fixed test users - created and cleaned up for each test run.
  Uses provider-agnostic IDs from test_data.clj."
  []
  {:modifier-user (test-audit-user-data :modifier-user)
   :creator-user (test-audit-user-data :creator-user)
   :test-user-1 (test-audit-user-data :test-user-1)
   :test-user-2 (test-audit-user-data :test-user-2)
   :updater-user (test-audit-user-data :updater-user)})

(defn cleanup-test-users!
  "Delete all fixed test users - by ID and by name pattern.
   The name pattern cleanup handles orphaned users from previous runs
   with different ID formats (e.g., after XID migration)."
  []
  ;; First, delete by ID (for users created in this run)
  (doseq [[_ user-data] (test-users)]
    (try
      (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user-data)})
      (catch Exception _)))  ; Ignore if doesn't exist
  ;; Then, delete by name pattern (for orphaned users with old IDs)
  (try
    (let [orphaned-users (db/search-entity db/*db* :iam/user
                                           {:_where {:name {:_like "audit-test-%"}}}
                                           {(id/key) nil})]
      (doseq [user orphaned-users]
        (try
          (db/delete-entity db/*db* :iam/user {(id/key) (id/extract user)})
          (catch Exception _))))
    (catch Exception _)))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn cleanup-fixture
  "Clean up test data before and after each test."
  [f]
  ;; Clean up before test to ensure clean state
  (try
    (cleanup-test-users!)
    (when context/*user-context-provider*
      (context/clear-cache context/*user-context-provider*))
    (catch Exception e
      (log/warn "Pre-test cleanup warning:" (.getMessage e))))
  (f)
  ;; Clean up after test
  (try
    (cleanup-test-users!)
    (when context/*user-context-provider*
      (context/clear-cache context/*user-context-provider*))
    (catch Exception e
      (log/warn "Post-test cleanup warning:" (.getMessage e)))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each cleanup-fixture)

;;; ============================================================================
;;; Selection Helpers
;;; ============================================================================

(def audit-selection-with-relation
  "Selection for audit fields with modified_by as RELATION (returns user object)"
  {(id/key) nil
   :name nil
   :_eid nil
   :active nil
   :modified_on nil
   :modified_by [{:selections {(id/key) nil
                               :name nil
                               :_eid nil}}]})

;;; ============================================================================
;;; Audit Behavior Tests
;;; ============================================================================

(deftest test-audit-on-insert-without-user-context
  (testing "Insert entity without user context sets modified_on but not modified_by"
    (let [user-data (:test-user-1 (test-users))
          _ (db/sync-entity db/*db* :iam/user user-data)
          result (db/get-entity db/*db* :iam/user
                                {(id/key) (id/extract user-data)}
                                audit-selection-with-relation)]

      (is (some? result)
          "Should read user back")

      (is (= (id/extract user-data) (id/extract result))
          "Should return same ID")

      (is (some? (:modified_on result))
          "modified_on should be set automatically by database")

      (is (nil? (:modified_by result))
          "modified_by should be nil when no user context"))))

(deftest test-audit-on-insert-with-user-context
  (testing "Insert entity with user context populates modified_by"
    (let [creator-data (:creator-user (test-users))
          _ (db/sync-entity db/*db* :iam/user creator-data)
          creator (db/get-entity db/*db* :iam/user
                                 {(id/key) (id/extract creator-data)}
                                 {:_eid nil
                                  (id/key) nil})]

      (is (some? (:_eid creator))
          "Creator user should have _eid")

      (context/with-user-context (id/extract creator-data)
        (let [new-user-data (:test-user-2 (test-users))
              _ (db/sync-entity db/*db* :iam/user new-user-data)
              result (db/get-entity db/*db* :iam/user
                                    {(id/key) (id/extract new-user-data)}
                                    audit-selection-with-relation)]

          (is (= (id/extract new-user-data) (id/extract result))
              "Should return same ID")

          (is (some? (:modified_on result))
              "modified_on should be set")

          (is (= (id/extract creator-data) (id/extract (:modified_by result)))
              "modified_by should be set to current user's ID"))))))

(deftest test-audit-on-update
  (testing "Update entity updates modified_on and modified_by"
    (let [modifier-data (:modifier-user (test-users))
          _ (db/sync-entity db/*db* :iam/user modifier-data)
          initial (db/get-entity db/*db* :iam/user
                                 {(id/key) (id/extract modifier-data)}
                                 audit-selection-with-relation)
          initial-modified-on (:modified_on initial)]

      (is (some? initial-modified-on)
          "Initial modified_on should be set")

      (is (nil? (:modified_by initial))
          "Initial modified_by should be nil (no context)")

      ;; Wait a moment to ensure timestamp changes
      (Thread/sleep 100)

      (let [updater-data (:updater-user (test-users))
            _ (db/sync-entity db/*db* :iam/user updater-data)
            updater (db/get-entity db/*db* :iam/user
                                   {(id/key) (id/extract updater-data)}
                                   {:_eid nil
                                    (id/key) nil})
            update-id (id/extract updater)]

        (context/with-user-context (id/extract updater-data)
          (let [_ (db/sync-entity db/*db* :iam/user
                                  (assoc modifier-data :active false))
                updated (db/get-entity db/*db* :iam/user
                                       {(id/key) (id/extract modifier-data)}
                                       audit-selection-with-relation)]

            (is (= (id/extract modifier-data) (id/extract updated))
                "Should update same entity")

            (is (false? (:active updated))
                "Should update active field")

            (is (some? (:modified_on updated))
                "modified_on should still be set")

            (is (= update-id (id/extract (:modified_by updated)))
                "modified_by should be set to updater's ID")))))))

(deftest test-audit-read-with-selection
  (testing "Read entity with explicit selection includes audit fields"
    (let [creator-data (:creator-user (test-users))
          _ (db/sync-entity db/*db* :iam/user creator-data)]

      (context/with-user-context (id/extract creator-data)
        (let [user-data (:test-user-1 (test-users))
              _ (db/sync-entity db/*db* :iam/user user-data)
              read-user (db/get-entity db/*db* :iam/user
                                       {(id/key) (id/extract user-data)}
                                       audit-selection-with-relation)]

          (is (some? read-user)
              "Should read user")

          (is (= (id/extract user-data) (id/extract read-user))
              "Should read correct user")

          (is (some? (:modified_on read-user))
              "Should return modified_on field")

          (is (= (id/extract creator-data) (id/extract (:modified_by read-user)))
              "modified_by should contain creator user object with matching ID"))))))

(deftest test-audit-modified-by-reference
  (testing "Read entity with modified_by as reference to user entity"
    (let [creator-data (:creator-user (test-users))
          _ (db/sync-entity db/*db* :iam/user creator-data)]

      (context/with-user-context (id/extract creator-data)
        (let [user-data (:test-user-1 (test-users))
              _ (db/sync-entity db/*db* :iam/user user-data)
              read-user (db/get-entity db/*db* :iam/user
                                       {(id/key) (id/extract user-data)}
                                       audit-selection-with-relation)]

          (is (some? read-user)
              "Should read user")

          (is (map? (:modified_by read-user))
              "modified_by should be a user object map")

          (is (= (id/extract creator-data) (id/extract (:modified_by read-user)))
              "modified_by should contain creator's ID")

          (is (= (:name creator-data) (:name (:modified_by read-user)))
              "modified_by should contain creator's name"))))))
