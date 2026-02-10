(ns synthigy.iam.audit.created-fields-test
  "Tests for created_by and created_on audit fields.

  These tests verify audit behavior through the abstraction layer
  and work on any database backend (Postgres, SQLite, etc.)."
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.db :as db]
    [synthigy.iam.context :as context]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]))

;;; ============================================================================
;;; Test Data (Provider-agnostic via id/data)
;;; ============================================================================

(def test-user-keys
  "Maps short keys to registered test data keys and static data."
  {:creator-user {:data-key :test/audit-creator
                  :name "created-fields-creator"
                  :active true}
   :updater-user {:data-key :test/audit-updater
                  :name "created-fields-updater"
                  :active true}
   :test-user    {:data-key :test/audit-test-user
                  :name "created-fields-test"
                  :active true}})

(defn test-user-data
  "Get test user data map by short key. Returns data with provider-agnostic ID."
  [k]
  (let [{:keys [data-key name active]} (get test-user-keys k)]
    {(id/key) (id/data data-key)
     :name name
     :active active}))

;; For backwards compatibility with existing test code
(def test-users
  "Dynamic map that returns test user data with provider-agnostic IDs.
   Note: Access via (test-user-data key) is preferred."
  (reify clojure.lang.ILookup
    (valAt [_ k] (test-user-data k))
    (valAt [_ k not-found] (or (test-user-data k) not-found))))

(defn cleanup-test-users!
  "Delete all test users"
  []
  (doseq [[_short-key {:keys [data-key]}] test-user-keys]
    (try
      (db/delete-entity db/*db* :iam/user {(id/key) (id/data data-key)})
      (catch Exception _))))

;;; ============================================================================
;;; Fixtures
;;; ============================================================================

(defn cleanup-fixture
  "Clean up test data before and after each test."
  [f]
  (try
    (cleanup-test-users!)
    (when context/*user-context-provider*
      (context/clear-cache context/*user-context-provider*))
    (catch Exception e
      (log/warn "Pre-test cleanup warning:" (.getMessage e))))
  (f)
  (try
    (cleanup-test-users!)
    (when context/*user-context-provider*
      (context/clear-cache context/*user-context-provider*))
    (catch Exception e
      (log/warn "Post-test cleanup warning:" (.getMessage e)))))

(use-fixtures :once test-helper/system-fixture)
(use-fixtures :each cleanup-fixture)

;;; ============================================================================
;;; Tests
;;; ============================================================================

(deftest test-migration-completes
  (testing "Audit migration completes successfully"
    ;; Run migration (idempotent)
    (patch/level! :synthigy.iam/audit)

    ;; Verify version is set (exact version depends on DB backend)
    (let [version (patch/read-version patch/*version-store* :synthigy.iam/audit)]
      (is (some? version)
          "Audit version should be set after migration"))))

(deftest test-created-fields-on-insert
  (testing "INSERT sets both created_by and created_on"
    ;; Create creator user
    (let [creator-data (:creator-user test-users)
          _ (db/sync-entity db/*db* :iam/user creator-data)
          creator (db/get-entity db/*db* :iam/user
                                 {(id/key) (id/extract creator-data)}
                                 {:_eid nil
                                  (id/key) nil})]

      ;; Create test user with creator context
      (context/with-user-context (id/extract creator-data)
        (let [test-data (:test-user test-users)
              _ (db/sync-entity db/*db* :iam/user test-data)
              result (db/get-entity db/*db* :iam/user
                                    {(id/key) (id/extract test-data)}
                                    {(id/key) nil
                                     :name nil
                                     :created_by [{:selections {(id/key) nil
                                                                :_eid nil}}]
                                     :created_on nil
                                     :modified_by [{:selections {(id/key) nil
                                                                 :_eid nil}}]
                                     :modified_on nil})]

          (is (some? (:created_by result))
              "created_by should be set")

          (is (some? (:created_on result))
              "created_on should be set")

          (is (= (id/extract creator-data) (id/extract (:created_by result)))
              "created_by should reference creator user")

          (is (= (id/extract creator-data) (id/extract (:modified_by result)))
              "modified_by should also reference creator user on INSERT"))))))

(deftest test-created-fields-preserved-on-update
  (testing "UPDATE preserves created_by and created_on"
    ;; Create creator user
    (let [creator-data (:creator-user test-users)
          _ (db/sync-entity db/*db* :iam/user creator-data)]

      ;; Create test user with creator context
      (context/with-user-context (id/extract creator-data)
        (let [test-data (:test-user test-users)
              _ (db/sync-entity db/*db* :iam/user test-data)
              initial (dataset/get-entity
                        :iam/user
                        {(id/key) (id/extract test-data)}
                        {:created_by [{:selections {(id/key) nil}}]
                         :created_on nil
                         :modified_by [{:selections {(id/key) nil}}]
                         :modified_on nil})
              initial-created-on (:created_on initial)]

          ;; Wait to ensure timestamp difference
          (Thread/sleep 100)

          ;; Create updater user and update with their context
          (let [updater-data (:updater-user test-users)
                _ (db/sync-entity db/*db* :iam/user updater-data)]

            (context/with-user-context (id/extract updater-data)
              (db/sync-entity db/*db* :iam/user
                              (assoc test-data :name "updated-name"))

              (let [updated (db/get-entity db/*db* :iam/user
                                           {(id/key) (id/extract test-data)}
                                           {:name nil
                                            :created_by [{:selections {(id/key) nil}}]
                                            :created_on nil
                                            :modified_by [{:selections {(id/key) nil}}]
                                            :modified_on nil})]

                (is (= (id/extract creator-data) (id/extract (:created_by updated)))
                    "created_by should remain creator user (preserved)")

                (is (= initial-created-on (:created_on updated))
                    "created_on should remain unchanged (immutable)")

                (is (= (id/extract updater-data) (id/extract (:modified_by updated)))
                    "modified_by should be updater user")

                (is (not= (:modified_on initial) (:modified_on updated))
                    "modified_on should be updated to new timestamp")))))))))

(deftest test-created-on-immutability-via-sync
  (testing "created_on cannot be changed via sync-entity"
    ;; Create test user
    (let [creator-data (:creator-user test-users)
          _ (db/sync-entity db/*db* :iam/user creator-data)]

      (context/with-user-context (id/extract creator-data)
        (let [test-data (:test-user test-users)
              _ (db/sync-entity db/*db* :iam/user test-data)
              initial (db/get-entity db/*db* :iam/user
                                     {(id/key) (id/extract test-data)}
                                     {:created_on nil})
              initial-created-on (:created_on initial)]

          ;; Try to change created_on via sync-entity
          (db/sync-entity db/*db* :iam/user
                          (assoc test-data :created_on "2020-01-01T00:00:00"))

          ;; Verify created_on was preserved
          (let [after-attempt (db/get-entity db/*db* :iam/user
                                             {(id/key) (id/extract test-data)}
                                             {:created_on nil})]

            (is (= initial-created-on (:created_on after-attempt))
                "created_on should remain unchanged (trigger protected)")))))))
