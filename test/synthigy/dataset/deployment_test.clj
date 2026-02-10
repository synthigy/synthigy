(ns synthigy.dataset.deployment-test
  "Tests for dataset deployment and recall functionality.

  Based on EYWA's test_datasets.clj test scenarios.

  ## TEST STATUS (Updated: 2026-01-15)

  ✅ Scenario 1: Shared entities with recall! - IMPLEMENTED
  ✅ Scenario 2: destroy! with shared entities - IMPLEMENTED (NEW!)
  ✅ Scenario 3: destroy! deletes all versions - IMPLEMENTED (NEW!)
  ✅ Scenarios 9.1-9.9: Schema evolution tests - IMPLEMENTED

  ## KNOWN ISSUES

  1. **Test Isolation**: Tests interfere when run together due to shared database state.
     Recommend running with cleanup between scenarios or using unique UUIDs per test.

  2. **Entity Renaming During Rollback**: Renaming entities (Product→Item→Product) causes
     relation table column rename failures (testitem_id can't be renamed when table doesn't exist).

  3. **destroy! Behavior**: May not fully deactivate entities when claimed by multiple datasets.

  ## EYWA Test Scenarios Reference (from /Users/robi/dev/EYWA/core/test/dataset/test_datasets.clj)

  The following scenarios should be implemented for comprehensive deployment testing:

  ✅ 1. Shared entities between datasets (using recall!)
     - Deploy A (Product, Category)
     - Deploy B (Product[shared], Order)
     - Verify Product is claimed by both datasets
     - Recall A
     - Verify Product remains (still claimed by B)
     - Verify Category is dropped (exclusive to A)
     STATUS: IMPLEMENTED (scenario-1-shared-entities)

  ✅ 2. destroy! with shared entities
     - Deploy A (Product, Category)
     - Deploy B (Product[shared], Order)
     - Destroy A → verify Product remains (claimed by B), Category drops
     - Destroy B → verify Product drops (no more claims)
     STATUS: IMPLEMENTED (scenario-2-destroy-with-shared-entities)

  ✅ 3. destroy! deletes entire dataset with all versions
     - Deploy A v1, v2, v3
     - Destroy entire dataset A
     - Verify all versions deleted, entities dropped if unclaimed
     STATUS: IMPLEMENTED (scenario-3-destroy-all-versions)

  🚧 4. Name conflicts with different UUIDs
     - Deploy A with entity named 'Item' (UUID-A)
     - Attempt to deploy B with entity named 'Item' (UUID-B, different UUID)
     - Verify conflict is detected and deployment fails or renames

  🚧 5. Reactivation of previously inactive entities
     - Deploy A with entity X
     - Recall A (marks X as inactive)
     - Redeploy A
     - Verify X is reactivated (active=true)

  🚧 6. Last-deployed-wins for shared entities
     - Deploy A with shared entity E (attributes: name, price)
     - Deploy B with shared entity E (attributes: name, description)
     - Verify E has attributes from both A and B
     - Deploy C with shared entity E (attributes: name, quantity)
     - Verify E has union of all attributes

  🚧 7. Multiple datasets sharing multiple entities
     - Deploy A (E1, E2)
     - Deploy B (E1, E3)
     - Deploy C (E2, E3)
     - Verify all entities claimed correctly
     - Recall B
     - Verify E1 still claimed by A, E3 still claimed by C

  🚧 8. Entity renaming across deployments
     - Deploy A with entity 'OldName' (UUID-X)
     - Redeploy A with entity 'NewName' (UUID-X, same UUID)
     - Verify entity is renamed, data preserved

  🚧 9. Recall scenarios
     - Deploy A, B, C with overlapping entities
     - Recall in different orders (A→B→C, C→B→A)
     - Verify claims are properly removed in all cases

  🚧 10. Rollback scenarios
     - Deploy A v1
     - Deploy A v2 (breaking changes)
     - Rollback to A v1
     - Verify schema reverted correctly

  ✅ 11. Iterative development lifecycle (Schema Evolution)
     - Deploy A v1 (basic entity)
     - Insert test data
     - Deploy A v2 (add attributes)
     - Verify data persists, new attributes available
     - Deploy A v3 (add relations)
     - Verify data and attributes persist
     STATUS: IMPLEMENTED (scenarios 9.1-9.9)

  Reference date: 2026-01-15"
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [clojure.tools.logging :as log]
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming :as naming]
    [synthigy.dataset.sql.protocol :as proto]
    [synthigy.dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]
    [synthigy.db :as db]
    [synthigy.test-data]  ; Load test data registrations
    [synthigy.test-helper :as test-helper]
    [synthigy.transit]))

;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

;;; Forward declarations
(declare get-test-dataset-tables-and-types)

(defn cleanup-all-test-data!
  "Remove all test datasets from previous runs using destroy! API.
  This properly removes them from both database and __deploy_history."
  []
  (log/info "Cleaning up leftover test datasets from previous runs...")
  (try
    ;; Destroy test datasets using the proper API
    ;; This ensures __deploy_history is updated correctly
    (let [test-dataset-keys [:test/dataset-a  ; TestA
                             :test/dataset-b  ; TestB
                             :test/dataset-c  ; TestC
                             :test/dataset-d]] ; TestD

      (doseq [dataset-key test-dataset-keys
              :let [dataset-id (id/data dataset-key)]]
        (try
          ;; Query dataset with versions first
          (let [dataset (db/get-entity db/*db* :dataset/dataset
                                       {(id/key) dataset-id}
                                       {(id/key) nil
                                        :name nil
                                        :versions [{:selections {(id/key) nil
                                                                 :name nil}}]})]
            (when dataset
              (log/info (str "Destroying test dataset: " (:name dataset) " (" dataset-id ")"))
              (core/destroy! db/*db* dataset)
              (log/info (str "  ✓ Destroyed dataset: " (:name dataset)))))
          (catch Exception e
            ;; Ignore if dataset doesn't exist
            (log/debug (str "  Dataset " dataset-id " not found: " (.getMessage e)))))))

    ;; Delete test entities with names starting with "Test"
    (let [test-entities (db/search-entity db/*db* :iam/user
                                          {:_where {:name {:_like "Test%"}}}
                                          {(id/key) nil
                                           :name nil})]
      (doseq [entity test-entities]
        (try
          (db/delete-entity db/*db* :iam/user {(id/key) (id/extract entity)})
          (catch Exception _)))
      (when (pos? (count test-entities))
        (log/info (str "Deleted " (count test-entities) " test entities"))))

    ;; FALLBACK: Drop any orphaned test tables not associated with datasets
    ;; This catches tables left from crashed tests or incomplete cleanups
    (log/info "Pattern-based cleanup: finding orphaned test tables...")
    (try
      (let [test-tables (proto/list-tables-like db/*db* "test%")
            system-pattern #"^(dataset|entity|attribute|relation|user|__)"
            safe-tables (remove #(re-find system-pattern (:tablename %)) test-tables)]
        (when (seq safe-tables)
          (log/info (str "Found " (count safe-tables) " orphaned test tables to drop..."))
          (doseq [table safe-tables]
            (let [table-name (:tablename table)]
              (try
                (proto/drop-table! db/*db* table-name)
                (log/info (str "  ✓ Dropped orphaned table: " table-name))
                (catch Exception e
                  (log/warn (str "  Could not drop table " table-name ":") (.getMessage e))))))))
      (catch Exception e
        (log/warn "Pattern-based cleanup failed:" (.getMessage e))))

    ;; Drop orphaned enum types
    (log/info "Dropping orphaned test enum types...")
    (try
      (let [test-types (proto/list-types-like db/*db* "test%")]
        (when (seq test-types)
          (log/info (str "Found " (count test-types) " orphaned test enum types..."))
          (doseq [type-row test-types]
            (let [type-name (:typname type-row)]
              (try
                (proto/drop-type! db/*db* type-name)
                (log/info (str "  ✓ Dropped orphaned type: " type-name))
                (catch Exception e
                  (log/warn (str "  Could not drop type " type-name ":") (.getMessage e))))))))
      (catch Exception e
        (log/warn "Type cleanup failed:" (.getMessage e))))

    (log/info "Leftover test data cleanup complete")
    (catch Exception e
      (log/error "Cleanup failed:" (.getMessage e))
      (throw e))))

(defn force-cleanup-all-test-tables!
  "EMERGENCY CLEANUP: Brutally drop all test-related database objects.
  Use this when tests fail and leave the database in a bad state.
  This bypasses all claim logic and directly drops everything."
  []
  (log/warn "⚠️  FORCE CLEANUP: Dropping all test database objects...")
  (try
    ;; Use model-based approach to get exact table and type names
    (let [{:keys [entity-tables relation-tables enum-types]} (get-test-dataset-tables-and-types)]

      ;; 1. Drop all relation tables
      (log/info (str "Force dropping " (count relation-tables) " relation tables..."))
      (doseq [table relation-tables]
        (try
          (proto/drop-table! db/*db* table)
          (log/info (str "  ✓ Force dropped: " table))
          (catch Exception e
            (log/error (str "  ✗ Could not drop " table ": ") (.getMessage e)))))

      ;; 2. Drop all entity tables
      (log/info (str "Force dropping " (count entity-tables) " entity tables..."))
      (doseq [table entity-tables]
        (try
          (proto/drop-table! db/*db* table)
          (log/info (str "  ✓ Force dropped: " table))
          (catch Exception e
            (log/error (str "  ✗ Could not drop " table ": ") (.getMessage e)))))

      ;; 3. Drop all enum types
      (log/info (str "Force dropping " (count enum-types) " enum types..."))
      (doseq [type-name enum-types]
        (try
          (proto/drop-type! db/*db* type-name)
          (log/info (str "  ✓ Force dropped type: " type-name))
          (catch Exception e
            (log/error (str "  ✗ Could not drop type " type-name ": ") (.getMessage e))))))

    ;; 4. Truncate __deploy_history completely
    (log/info "Truncating __deploy_history...")
    (try
      (proto/truncate-table! db/*db* "__deploy_history")
      (log/info "  ✓ __deploy_history truncated (will be rebuilt on restart)")
      (catch Exception e
        (log/error "Could not truncate __deploy_history:" (.getMessage e))))

    ;; 5. Force reload model
    (log/info "Force reloading model...")
    (try
      (lifecycle/stop! :synthigy/dataset)
      (lifecycle/start! :synthigy/dataset)
      (log/info "  ✓ Model force reloaded")
      (catch Exception e
        (log/error "Could not reload model:" (.getMessage e))))

    (log/warn "✓ FORCE CLEANUP COMPLETE")
    (catch Exception e
      (log/error "FORCE CLEANUP FAILED:" (.getMessage e))
      (throw e))))

(defn cleanup-fixture
  "Clean up test datasets BEFORE and AFTER tests.

  BEFORE: Ensures clean slate in case previous run crashed.
  AFTER: Cleans up what we just created."
  [f]
  ;; BEFORE: Clean up any leftover state from previous runs
  (println "\n==========================================================")
  (println "PRE-CLEANUP: Destroying leftover test datasets...")
  (println "==========================================================\n")
  (try
    (cleanup-all-test-data!)
    (println "\n==========================================================")
    (println "PRE-CLEANUP: Complete - ready to run tests")
    (println "==========================================================\n")
    (catch Exception e
      (println "PRE-CLEANUP WARNING:" (.getMessage e))
      (println "Continuing with tests anyway...\n")))

  ;; RUN TESTS
  (try
    (f)
    (finally
      ;; AFTER: Always cleanup, even if tests fail
      (println "\n==========================================================")
      (println "POST-CLEANUP: Destroying test datasets...")
      (println "==========================================================\n")
      (try
        (cleanup-all-test-data!)
        (println "\n==========================================================")
        (println "POST-CLEANUP: Complete")
        (println "==========================================================\n")
        (catch Exception e
          (println "POST-CLEANUP ERROR:" (.getMessage e)))))))

;; Use comprehensive system fixture for initialization and shutdown
;; cleanup-fixture runs AFTER system starts (needs db/*db* to be available)
(use-fixtures :once test-helper/system-fixture cleanup-fixture)

;; Test dataset constructors
;; Using helpers from synthigy.dataset.test-helpers (imported above)
(defn create-test-dataset-a []
  "Dataset A: TestProduct and TestCategory entities (exclusive to A)"
  (let [model (core/map->ERDModel {})

        ;; TestProduct entity (unique name to avoid conflicts)
        product-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                         :name "TestProduct"})
                           (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                                :name "Price"
                                                :type "float"}))

        ;; TestCategory entity (unique name to avoid conflicts)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; Build model
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity category-entity))

        ;; Add relation
        model-with-relations (core/add-relation
                               model-with-entities
                               (make-relation
                                 {(id/key) (id/data :test/product-category-rel)
                                  :from (id/data :test/product-entity)
                                  :to (id/data :test/category-entity)
                                  :from-label "product"
                                  :to-label "category"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v1)
     :name "Test Dataset A v1"
     :version "1.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-b []
  "Dataset B: TestProduct (SHARED with A) and TestOrder (exclusive to B)"
  (let [model (core/map->ERDModel {})

        ;; TestProduct entity - SAME ID as Dataset A (shared)
        product-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                         :name "TestProduct"})
                           (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                                :name "Price"
                                                :type "float"})
                           (add-test-attribute {(id/key) (id/data :test/product-description-attr)
                                                :name "Description"
                                                :type "string"}))

        ;; TestOrder entity - exclusive to B
        order-entity (-> (make-entity {(id/key) (id/data :test/order-entity)
                                       :name "TestOrder"})
                         (add-test-attribute {(id/key) (id/data :test/order-number-attr)
                                              :name "OrderNumber"
                                              :type "string"
                                              :constraint "mandatory"}))

        ;; Build model
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity order-entity))

        ;; Add relation
        model-with-relations (core/add-relation
                               model-with-entities
                               (make-relation
                                 {(id/key) (id/data :test/order-product-rel)
                                  :from (id/data :test/order-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "order"
                                  :to-label "product"
                                  :cardinality "m2m"}))]

    {(id/key) (id/data :test/dataset-b-v1)
     :name "Test Dataset B v1"
     :version "1.0.0"
     :dataset {(id/key) (id/data :test/dataset-b)
               :name "TestB"}
     :model model-with-relations}))

(defn create-test-dataset-c []
  "Dataset C: TestProduct (SHARED with A) with different attributes - SKU instead of Price"
  (let [model (core/map->ERDModel {})

        ;; TestProduct entity - SAME ID as Dataset A (shared), different attributes
        product-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                         :name "TestProduct"})
                           (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {(id/key) (id/data :test/product-sku-attr)
                                                :name "SKU"
                                                :type "string"}))

        model-with-entities (core/add-entity model product-entity)]

    {(id/key) (id/data :test/dataset-c-v1)
     :name "Test Dataset C v1"
     :version "1.0.0"
     :dataset {(id/key) (id/data :test/dataset-c)
               :name "TestC"}
     :model model-with-entities}))

(defn create-test-dataset-d []
  "Dataset D: TestProduct and TestCategory (BOTH SHARED with A) + TestSupplier (exclusive to D)"
  (let [model (core/map->ERDModel {})

        ;; Shared entities (same IDs as A)
        product-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                         :name "TestProduct"})
                           (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                                :name "Price"
                                                :type "float"}))

        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; Exclusive to D
        supplier-entity (-> (make-entity {(id/key) (id/data :test/supplier-entity)
                                          :name "TestSupplier"})
                            (add-test-attribute {(id/key) (id/data :test/supplier-name-attr)
                                                 :name "Name"
                                                 :type "string"}))

        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity category-entity)
                                (core/add-entity supplier-entity))

        ;; Add relation (D-specific, exclusive)
        model-with-relations (core/add-relation
                               model-with-entities
                               (make-relation
                                 {(id/key) (id/data :test/product-supplier-rel)
                                  :from (id/data :test/product-entity)
                                  :to (id/data :test/supplier-entity)
                                  :from-label "product"
                                  :to-label "supplier"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-d-v1)
     :name "Test Dataset D v1"
     :version "1.0.0"
     :dataset {(id/key) (id/data :test/dataset-d)
               :name "TestD"}
     :model model-with-relations}))

(defn create-test-dataset-a-v2 []
  "Dataset A v2: Renamed TestProduct to TestItem (same ID, different name)"
  (let [model (core/map->ERDModel {})

        ;; Renamed entity (same ID, new name and added Description attribute)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"}))

        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity))

        model-with-relations (core/add-relation
                               model-with-entities
                               (make-relation
                                 {(id/key) (id/data :test/product-category-rel)
                                  :from (id/data :test/product-entity)
                                  :to (id/data :test/category-entity)
                                  :from-label "item"
                                  :to-label "category"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v2)
     :name "Test Dataset A v2"
     :version "2.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v3 []
  "Dataset A v3: Add TestComment entity with relation to TestItem"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity (same as v2)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"}))

        ;; TestCategory entity (same as v2)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; NEW: TestComment entity
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-rating-attr)
                                                :name "author_name"
                                                :type "string"}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Existing relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; NEW: Comment → Item relation
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v3)
     :name "Test Dataset A v3"
     :version "3.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v4 []
  "Dataset A v4: Add status enum attribute to TestItem"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity with status enum attribute
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        ;; NEW: status enum attribute
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "published"}]}}))

        ;; TestCategory entity (same as v3)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; TestComment entity (same as v3)
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-rating-attr)
                                                :name "author_name"
                                                :type "string"}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Item → Category relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; Comment → Item relation
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v4)
     :name "Test Dataset A v4"
     :version "4.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v5 []
  "Dataset A v5: Add 'archived' value to status enum"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity with expanded status enum (draft, published, archived)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        ;; Expanded status enum - added "archived"
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "published"}
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity (same as v4)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; TestComment entity (same as v4)
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-rating-attr)
                                                :name "author_name"
                                                :type "string"}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Item → Category relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; Comment → Item relation
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v5)
     :name "Test Dataset A v5"
     :version "5.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v6 []
  "Dataset A v6: Rename enum value 'published' → 'active'"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity with renamed enum value (draft, active, archived)
        ;; NOTE: "published" UUID is reused but name changed to "active"
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        ;; Renamed enum value: published → active (same UUID!)
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "active"}  ;; RENAMED from "published"
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity (same as v5)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; TestComment entity (same as v5)
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-rating-attr)
                                                :name "author_name"
                                                :type "string"}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Item → Category relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; Comment → Item relation
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v6)
     :name "Test Dataset A v6"
     :version "6.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v7 []
  "Dataset A v7: Add priority enum attribute to TestComment"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity (same as v6)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "active"}
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity (same as v6)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; TestComment entity with NEW priority enum attribute
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-rating-attr)
                                                :name "author_name"
                                                :type "string"})
                           ;; NEW: priority enum attribute
                           (add-test-attribute {(id/key) (id/data :test/comment-priority-attr)
                                                :name "priority"
                                                :type "enum"
                                                :configuration {:values [{(id/key) (id/data :test/priority-low)
                                                                          :name "low"}
                                                                         {(id/key) (id/data :test/priority-medium)
                                                                          :name "medium"}
                                                                         {(id/key) (id/data :test/priority-high)
                                                                          :name "high"}]}}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Item → Category relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; Comment → Item relation
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v7)
     :name "Test Dataset A v7"
     :version "7.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v8 []
  "Dataset A v8: Change TestCategory.Name type from string to json"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity (same as v7)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "active"}
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity with Name changed to json type
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "json"  ;; CHANGED from "string" to "json"
                                                 :constraint "mandatory"}))

        ;; TestComment entity (same as v7)
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-rating-attr)
                                                :name "author_name"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-priority-attr)
                                                :name "priority"
                                                :type "enum"
                                                :configuration {:values [{(id/key) (id/data :test/priority-low)
                                                                          :name "low"}
                                                                         {(id/key) (id/data :test/priority-medium)
                                                                          :name "medium"}
                                                                         {(id/key) (id/data :test/priority-high)
                                                                          :name "high"}]}}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Item → Category relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; Comment → Item relation
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v8)
     :name "Test Dataset A v8"
     :version "8.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v9 []
  "Dataset A v9: Remove TestComment.author_name attribute"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity (same as v8)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "active"}
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity (same as v8)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "json"
                                                 :constraint "mandatory"}))

        ;; TestComment entity - REMOVED author_name attribute
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           ;; author_name attribute REMOVED (was euuid (id/data :test/comment-rating-attr))
                           (add-test-attribute {(id/key) (id/data :test/comment-priority-attr)
                                                :name "priority"
                                                :type "enum"
                                                :configuration {:values [{(id/key) (id/data :test/priority-low)
                                                                          :name "low"}
                                                                         {(id/key) (id/data :test/priority-medium)
                                                                          :name "medium"}
                                                                         {(id/key) (id/data :test/priority-high)
                                                                          :name "high"}]}}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Item → Category relation
        model-with-item-category (core/add-relation
                                   model-with-entities
                                   (make-relation
                                     {(id/key) (id/data :test/product-category-rel)
                                      :from (id/data :test/product-entity)
                                      :to (id/data :test/category-entity)
                                      :from-label "item"
                                      :to-label "category"
                                      :cardinality "m2o"}))

        ;; Comment → Item relation (same as v8)
        model-with-relations (core/add-relation
                               model-with-item-category
                               (make-relation
                                 {(id/key) (id/data :test/comment-item-rel)
                                  :from (id/data :test/comment-entity)
                                  :to (id/data :test/product-entity)
                                  :from-label "comment"
                                  :to-label "item"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v9)
     :name "Test Dataset A v9"
     :version "9.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v10 []
  "Dataset A v10: Remove TestComment→TestItem relation"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity (same as v9)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "active"}
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity (same as v9)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "json"
                                                 :constraint "mandatory"}))

        ;; TestComment entity (same as v9)
        comment-entity (-> (make-entity {(id/key) (id/data :test/comment-entity)
                                         :name "TestComment"})
                           (add-test-attribute {(id/key) (id/data :test/comment-text-attr)
                                                :name "content"
                                                :type "string"})
                           (add-test-attribute {(id/key) (id/data :test/comment-priority-attr)
                                                :name "priority"
                                                :type "enum"
                                                :configuration {:values [{(id/key) (id/data :test/priority-low)
                                                                          :name "low"}
                                                                         {(id/key) (id/data :test/priority-medium)
                                                                          :name "medium"}
                                                                         {(id/key) (id/data :test/priority-high)
                                                                          :name "high"}]}}))

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity)
                                (core/add-entity comment-entity))

        ;; Only Item → Category relation (Comment → Item relation REMOVED)
        model-with-relations (core/add-relation
                               model-with-entities
                               (make-relation
                                 {(id/key) (id/data :test/product-category-rel)
                                  :from (id/data :test/product-entity)
                                  :to (id/data :test/category-entity)
                                  :from-label "item"
                                  :to-label "category"
                                  :cardinality "m2o"}))]
        ;; Comment → Item relation REMOVED (was euuid (id/data :test/comment-item-rel))

    {(id/key) (id/data :test/dataset-a-v10)
     :name "Test Dataset A v10"
     :version "10.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-a-v11 []
  "Dataset A v11: Remove TestComment entity entirely"
  (let [model (core/map->ERDModel {})

        ;; TestItem entity (same as v10)
        item-entity (-> (make-entity {(id/key) (id/data :test/product-entity)
                                      :name "TestItem"})
                        (add-test-attribute {(id/key) (id/data :test/product-name-attr)
                                             :name "Name"
                                             :type "string"
                                             :constraint "mandatory"})
                        (add-test-attribute {(id/key) (id/data :test/product-price-attr)
                                             :name "Price"
                                             :type "float"})
                        (add-test-attribute {(id/key) (id/data :test/item-description-attr)
                                             :name "Description"
                                             :type "string"})
                        (add-test-attribute {(id/key) (id/data :test/item-status-attr)
                                             :name "status"
                                             :type "enum"
                                             :configuration {:values [{(id/key) (id/data :test/status-draft)
                                                                       :name "draft"}
                                                                      {(id/key) (id/data :test/status-published)
                                                                       :name "active"}
                                                                      {(id/key) (id/data :test/status-archived)
                                                                       :name "archived"}]}}))

        ;; TestCategory entity (same as v10)
        category-entity (-> (make-entity {(id/key) (id/data :test/category-entity)
                                          :name "TestCategory"})
                            (add-test-attribute {(id/key) (id/data :test/category-name-attr)
                                                 :name "Name"
                                                 :type "json"
                                                 :constraint "mandatory"}))

        ;; TestComment entity REMOVED entirely

        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity))
                                ;; comment-entity NOT added

        ;; Only Item → Category relation
        model-with-relations (core/add-relation
                               model-with-entities
                               (make-relation
                                 {(id/key) (id/data :test/product-category-rel)
                                  :from (id/data :test/product-entity)
                                  :to (id/data :test/category-entity)
                                  :from-label "item"
                                  :to-label "category"
                                  :cardinality "m2o"}))]

    {(id/key) (id/data :test/dataset-a-v11)
     :name "Test Dataset A v11"
     :version "11.0.0"
     :dataset {(id/key) (id/data :test/dataset-a)
               :name "TestA"}
     :model model-with-relations}))

;;; ============================================================================
;;; Helper Functions for Cleanup
;;; ============================================================================

(defn get-test-dataset-tables-and-types
  "Extract all table names and enum type names from test datasets only.

  SAFETY GUARANTEE: Only returns tables from test datasets (TestA, TestB, TestC, TestD).
  NEVER touches core dataset meta-model tables (dataset, dataset_version, entity, etc.)

  Returns a map with :entity-tables, :relation-tables, and :enum-types."
  []
  (try
    ;; CRITICAL: Only create fresh test datasets - NEVER query deployed model
    ;; This ensures we only drop test tables, not system tables
    (let [fresh-datasets (try
                           [(create-test-dataset-a)
                            (create-test-dataset-b)
                            (create-test-dataset-c)
                            (create-test-dataset-d)]
                           (catch Exception e
                             (log/warn "Could not create fresh datasets:" (.getMessage e))
                             []))

          fresh-models (map :model fresh-datasets)

          ;; Get entities from test datasets only (TestProduct, TestItem, TestCategory, etc.)
          all-entities (into #{} (mapcat core/get-entities fresh-models))

          ;; Get relations from test datasets only
          all-relations (into #{} (mapcat core/get-relations fresh-models))

          ;; Get entity table names using sql.naming (testproduct, testitem, testcategory, etc.)
          entity-tables (set (map naming/entity->table-name all-entities))

          ;; Get relation table names using sql.naming (test_testproduct_testcategory, etc.)
          relation-tables (set (map naming/relation->table-name all-relations))

          ;; Get enum type names from entity attributes (testitem_status, testcomment_priority, etc.)
          enum-types (set
                       (for [entity all-entities
                             attr (:attributes entity)
                             :when (= "enum" (:type attr))
                             :let [table-name (naming/entity->table-name entity)
                                   attr-name (:name attr)
                                   enum-name (naming/normalize-name (str table-name " " attr-name))]]
                         enum-name))

          ;; SAFETY CHECK: Verify we're only targeting test tables
          system-table-pattern #"^(dataset|entity|attribute|relation|user|__)"
          unsafe-tables (filter #(re-find system-table-pattern %) entity-tables)]

      (when (seq unsafe-tables)
        (throw (ex-info "SAFETY VIOLATION: Attempted to cleanup system tables!"
                        {:unsafe-tables unsafe-tables
                         :all-entity-tables entity-tables})))

      (log/info (str "Cleanup will target: "
                     (count entity-tables) " entity tables, "
                     (count relation-tables) " relation tables, "
                     (count enum-types) " enum types"))
      (log/debug (str "Entity tables: " entity-tables))
      (log/debug (str "Relation tables: " relation-tables))
      (log/debug (str "Enum types: " enum-types))

      {:entity-tables entity-tables
       :relation-tables relation-tables
       :enum-types enum-types})
    (catch Exception e
      (log/error "Could not extract test dataset tables:" (.getMessage e))
      {:entity-tables #{}
       :relation-tables #{}
       :enum-types #{}})))

;;; ============================================================================
;;; Cleanup Functions (EYWA Pattern)
;;; ============================================================================

;; Cleanup function matching EYWA pattern
(defn cleanup-test-datasets! []
  "Remove all test datasets and their data after scenarios complete."
  (println "\n\nCLEANUP")
  (println "--------")
  (println "Deleting test data...")
  (try
    ;; Step 1: Delete all test entity data from tables
    (try
      (let [datasource (:datasource db/*db*)
            test-tables ["testproduct" "testitem" "testcategory" "testcomment" "testorder" "testsupplier"]]
        (doseq [table test-tables]
          (try
            (jdbc/execute! datasource [(str "DELETE FROM " table)])
            (catch Exception _)))
        (println "  ✓ Deleted test entity data"))
      (catch Exception e
        (println (format "  Test data deletion: %s" (.getMessage e)))))

    ;; Step 2: Delete Dataset records
    (println "\nDeleting test datasets...")

    ;; Delete Dataset A
    (try
      (let [dataset (db/get-entity db/*db* :dataset/dataset
                                   {(id/key) (id/data :test/dataset-a)}
                                   {:versions [{:selections {(id/key) nil
                                                             :name nil}}]})]
        (when dataset
          (core/destroy! db/*db* dataset)
          (println "  ✓ Deleted Dataset A")))
      (catch Exception e
        (println (format "  Dataset A: %s" (.getMessage e)))))

    ;; Delete Dataset B
    (try
      (let [dataset (db/get-entity db/*db* :dataset/dataset
                                   {(id/key) (id/data :test/dataset-b)}
                                   {:versions [{:selections {(id/key) nil
                                                             :name nil}}]})]
        (when dataset
          (core/destroy! db/*db* dataset)
          (println "  ✓ Deleted Dataset B")))
      (catch Exception e
        (println (format "  Dataset B: %s" (.getMessage e)))))

    ;; Delete Dataset C
    (try
      (let [dataset (db/get-entity db/*db* :dataset/dataset
                                   {(id/key) (id/data :test/dataset-c)}
                                   {:versions [{:selections {(id/key) nil
                                                             :name nil}}]})]
        (when dataset
          (core/destroy! db/*db* dataset)
          (println "  ✓ Deleted Dataset C")))
      (catch Exception e
        (println (format "  Dataset C: %s" (.getMessage e)))))

    ;; Delete Dataset D
    (try
      (let [dataset (db/get-entity db/*db* :dataset/dataset
                                   {(id/key) (id/data :test/dataset-d)}
                                   {:versions [{:selections {(id/key) nil
                                                             :name nil}}]})]
        (when dataset
          (core/destroy! db/*db* dataset)
          (println "  ✓ Deleted Dataset D")))
      (catch Exception e
        (println (format "  Dataset D: %s" (.getMessage e)))))

    ;; Step 3: Reload deployed model
    (println "\nReloading deployed model...")
    (try
      (lifecycle/stop! :synthigy/dataset)
      (lifecycle/start! :synthigy/dataset)
      (println "  ✓ Model reloaded")
      (catch Exception e
        (println (format "  Model reload: %s" (.getMessage e)))))

    (println "\n✓ Cleanup complete")
    (catch Exception e
      (println (format "\n✗ Cleanup failed: %s" (.getMessage e))))))

;; Test scenarios (as functions to run sequentially)
(defn scenario-1-shared-entities []
  "Scenario 1: Shared entities between datasets"
  (let [dataset-a (create-test-dataset-a)
        dataset-b (create-test-dataset-b)]

    (println "\n=== Scenario 1: Shared Entities ===")
    (println "Deploying Dataset A (TestProduct, TestCategory)...")
    (core/deploy! db/*db* dataset-a)

    (let [model-after-a (dataset/deployed-model)
          product-entity (core/get-entity model-after-a (id/data :test/product-entity))]

      (println "✓ Dataset A deployed")
      (println "  Product claimed-by:" (:claimed-by product-entity))

      ;; Verify Product is claimed by dataset A
      (is (contains? (:claimed-by product-entity) (id/data :test/dataset-a-v1))
          "Product should be claimed by Dataset A")

      ;; Verify Product is active
      (is (:active product-entity) "Product should be active after deploying A"))

    (println "\nDeploying Dataset B (TestProduct[shared], TestOrder)...")
    (core/deploy! db/*db* dataset-b)

    (let [model-after-b (dataset/deployed-model)
          product-entity (core/get-entity model-after-b (id/data :test/product-entity))
          order-entity (core/get-entity model-after-b (id/data :test/order-entity))]

      (println "✓ Dataset B deployed")
      (println "  Product claimed-by:" (:claimed-by product-entity))
      (def model-after-b model-after-b)

      ;; Verify Product is claimed by BOTH datasets
      (is (contains? (:claimed-by product-entity) (id/data :test/dataset-a-v1))
          "Product should still be claimed by Dataset A")
      (is (contains? (:claimed-by product-entity) (id/data :test/dataset-b-v1))
          "Product should now also be claimed by Dataset B")
      (is (= 2 (count (:claimed-by product-entity)))
          "Product should have exactly 2 claims")

      ;; Verify both entities are active
      (is (:active product-entity) "Product should be active")
      (is (:active order-entity) "Order should be active"))

    (println "\nRecalling Dataset A...")
    (core/recall! db/*db* dataset-a)

    (let [model-after-recall (dataset/deployed-model)
          product-entity (core/get-entity model-after-recall (id/data :test/product-entity))
          category-entity (core/get-entity model-after-recall (id/data :test/category-entity))]

      (println "✓ Dataset A recalled")
      (println "  Product claimed-by:" (:claimed-by product-entity))

      ;; Verify Product is still claimed by B (not dropped!)
      (is (contains? (:claimed-by product-entity) (id/data :test/dataset-b-v1))
          "Product should still be claimed by Dataset B")
      (is (= 1 (count (:claimed-by product-entity)))
          "Product should have exactly 1 claim now")
      (is (:active product-entity) "Product should still be active (claimed by B)")

      ;; Verify Category was dropped (exclusive to A)
      (is (nil? category-entity) "Category should be nil (dropped)")

      (println "✓ Scenario 1 PASSED: Shared entities work correctly!\n"))))

(defn scenario-2-destroy-with-shared-entities []
  "Scenario 2: destroy! with shared entities - verify claim-based table retention"
  (let [dataset-a (create-test-dataset-a)
        dataset-b (create-test-dataset-b)]

    (println "\n=== Scenario 2: destroy! with Shared Entities ===")
    (println "Deploying Dataset A (TestProduct, TestCategory)...")
    (core/deploy! db/*db* dataset-a)

    (println "✓ Dataset A deployed\n")

    (println "Deploying Dataset B (TestProduct[shared], TestOrder)...")
    (core/deploy! db/*db* dataset-b)

    (let [model-after-both (dataset/deployed-model)
          product-entity (core/get-entity model-after-both (id/data :test/product-entity))]
      (println "✓ Dataset B deployed")
      (println "  Product claimed-by:" (:claimed-by product-entity))
      (is (= 2 (count (:claimed-by product-entity))) "Product should be claimed by both datasets"))

    (println "\nDestroying Dataset A...")
    (core/destroy! db/*db* (:dataset dataset-a))

    (let [model-after-destroy-a (dataset/deployed-model)
          product-entity (core/get-entity model-after-destroy-a (id/data :test/product-entity))
          category-entity (core/get-entity model-after-destroy-a (id/data :test/category-entity))
          order-entity (core/get-entity model-after-destroy-a (id/data :test/order-entity))]

      (println "✓ Dataset A destroyed")
      (println "  Product claimed-by:" (:claimed-by product-entity))

      ;; Verify Product remains (still claimed by B)
      (is (not (nil? product-entity)) "Product should still exist")
      (is (contains? (:claimed-by product-entity) (id/data :test/dataset-b-v1))
          "Product should still be claimed by Dataset B")
      (is (= 1 (count (:claimed-by product-entity)))
          "Product should have exactly 1 claim after destroying A")
      (is (:active product-entity) "Product should still be active (claimed by B)")
      (println "  ✓ Product table retained (still claimed by B)")

      ;; Verify Category was dropped (exclusive to A)
      (is (nil? category-entity) "Category should be nil (dropped with Dataset A)")
      (println "  ✓ Category table dropped (exclusive to A)")

      ;; Verify Order remains (belongs to B)
      (is (not (nil? order-entity)) "Order should still exist")
      (is (:active order-entity) "Order should be active")
      (println "  ✓ Order table retained (belongs to B)"))

    (println "\nDestroying Dataset B...")
    (core/destroy! db/*db* (:dataset dataset-b))

    (let [model-after-destroy-b (dataset/deployed-model)
          product-entity (core/get-entity model-after-destroy-b (id/data :test/product-entity))
          order-entity (core/get-entity model-after-destroy-b (id/data :test/order-entity))]

      (println "✓ Dataset B destroyed")

      ;; Verify Product is now dropped (no more claims)
      (is (nil? product-entity) "Product should be nil (no more datasets claiming it)")
      (println "  ✓ Product table dropped (no more claims)")

      ;; Verify Order is dropped (exclusive to B)
      (is (nil? order-entity) "Order should be nil (dropped with Dataset B)")
      (println "  ✓ Order table dropped (exclusive to B)"))

    (println "✓ Scenario 2 PASSED: destroy! correctly manages entity tables based on claims!\n")))

(defn scenario-3-destroy-all-versions []
  "Scenario 3: destroy! deletes entire dataset with all versions"
  (println "\n=== Scenario 3: destroy! Deletes All Versions ===")

  ;; Deploy multiple versions
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (let [dataset-v1 (db/get-entity db/*db* :dataset/dataset
                                  {(id/key) (id/data :test/dataset-a)}
                                  {:versions [{:selections {(id/key) nil
                                                            :name nil
                                                            :version nil}}]})]
    (println "  ✓ v1.0.0 deployed")
    (is (= 1 (count (:versions dataset-v1))) "Should have 1 version"))

  (println "\nDeploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (let [dataset-v2 (db/get-entity db/*db* :dataset/dataset
                                  {(id/key) (id/data :test/dataset-a)}
                                  {:versions [{:selections {(id/key) nil
                                                            :name nil
                                                            :version nil}}]})]
    (println "  ✓ v2.0.0 deployed")
    (is (= 2 (count (:versions dataset-v2))) "Should have 2 versions"))

  (println "\nDeploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (let [dataset-v3 (db/get-entity db/*db* :dataset/dataset
                                  {(id/key) (id/data :test/dataset-a)}
                                  {:versions [{:selections {(id/key) nil
                                                            :name nil
                                                            :version nil}}]})]
    (println "  ✓ v3.0.0 deployed")
    (is (= 3 (count (:versions dataset-v3))) "Should have 3 versions")
    (println "  Total versions:" (count (:versions dataset-v3))))

  ;; Verify entities exist
  (let [model-before (dataset/deployed-model)
        item-entity (core/get-entity model-before (id/data :test/product-entity))
        category-entity (core/get-entity model-before (id/data :test/category-entity))
        comment-entity (core/get-entity model-before (id/data :test/comment-entity))]
    (println "\nEntities before destroy:")
    (is (not (nil? item-entity)) "TestItem should exist")
    (is (not (nil? category-entity)) "TestCategory should exist")
    (is (not (nil? comment-entity)) "TestComment should exist")
    (println "  ✓ TestItem, TestCategory, TestComment all exist"))

  ;; Destroy entire dataset
  (println "\nDestroying entire Dataset A (all versions)...")
  (let [dataset-full (db/get-entity db/*db* :dataset/dataset
                                    {(id/key) (id/data :test/dataset-a)}
                                    {:versions [{:selections {(id/key) nil
                                                              :name nil}}]})]
    (core/destroy! db/*db* dataset-full))

  (println "✓ Dataset A destroyed\n")

  ;; Verify dataset is gone
  (let [dataset-after (db/get-entity db/*db* :dataset/dataset
                                     {(id/key) (id/data :test/dataset-a)}
                                     {:versions [{:selections {(id/key) nil
                                                               :name nil}}]})]
    (is (nil? dataset-after) "Dataset should be completely deleted")
    (println "  ✓ Dataset record deleted from database"))

  ;; Verify all entities are gone (no other datasets claiming them)
  (let [model-after (dataset/deployed-model)
        item-entity (core/get-entity model-after (id/data :test/product-entity))
        category-entity (core/get-entity model-after (id/data :test/category-entity))
        comment-entity (core/get-entity model-after (id/data :test/comment-entity))]
    (is (nil? item-entity) "TestItem should be dropped")
    (is (nil? category-entity) "TestCategory should be dropped")
    (is (nil? comment-entity) "TestComment should be dropped")
    (println "  ✓ All entity tables dropped (no claims remaining)"))

  (println "✓ Scenario 3 PASSED: destroy! deletes entire dataset with all versions!\n"))

(defn scenario-9-add-entity []
  "Scenario 9.1: Add entity (TestComment)"
  (println "\n=== Scenario 9.1: Add Entity ===")

  ;; Deploy v1
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))

  (let [model-v1 (dataset/deployed-model)]
    (println "✓ v1.0.0 deployed")
    (is (core/get-entity model-v1 (id/data :test/product-entity))
        "TestProduct exists")
    (is (core/get-entity model-v1 (id/data :test/category-entity))
        "TestCategory exists"))

  ;; Deploy v2 (rename + add attribute)
  (println "\nDeploying Dataset A v2.0.0 (rename TestProduct→TestItem, add Description)...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))

  (let [model-v2 (dataset/deployed-model)
        item-entity (core/get-entity model-v2 (id/data :test/product-entity))]
    (println "✓ v2.0.0 deployed")
    (is (= "TestItem" (:name item-entity)) "Entity renamed to TestItem")
    (is (core/get-attribute item-entity (id/data :test/item-description-attr))
        "Description attribute added"))

  ;; Deploy v3 (add TestComment entity)
  (println "\nDeploying Dataset A v3.0.0 (add TestComment entity)...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))

  (let [model-v3 (dataset/deployed-model)
        comment-entity (core/get-entity model-v3 (id/data :test/comment-entity))
        comment-relation (core/get-relation model-v3 (id/data :test/comment-item-rel))]
    (println "✓ v3.0.0 deployed")
    (is (some? comment-entity) "TestComment entity created")
    (is (= "TestComment" (:name comment-entity)) "TestComment has correct name")
    (is (core/get-attribute comment-entity (id/data :test/comment-text-attr))
        "TestComment.content exists")
    (is (core/get-attribute comment-entity (id/data :test/comment-rating-attr))
        "TestComment.author_name exists")
    (is (some? comment-relation) "TestComment→TestItem relation created")

    (println "✓ Scenario 9.1 PASSED: Entity added successfully!\n")))

(defn scenario-9-add-enum-attribute []
  "Scenario 9.2: Add enum attribute (status:enum[draft,published] to TestItem)"
  (println "\n=== Scenario 9.2: Add Enum Attribute ===")

  ;; Deploy v1
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (println "✓ v1.0.0 deployed")

  ;; Deploy v2
  (println "\nDeploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (println "✓ v2.0.0 deployed")

  ;; Deploy v3
  (println "\nDeploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (println "✓ v3.0.0 deployed")

  ;; Deploy v4 (add status enum attribute)
  (println "\nDeploying Dataset A v4.0.0 (add status enum)...")
  (core/deploy! db/*db* (create-test-dataset-a-v4))

  (let [model-v4 (dataset/deployed-model)
        item-entity (core/get-entity model-v4 (id/data :test/product-entity))
        status-attr (core/get-attribute item-entity (id/data :test/item-status-attr))]
    (println "✓ v4.0.0 deployed")
    (is (some? status-attr) "status attribute created")
    (is (= "status" (:name status-attr)) "status attribute has correct name")
    (is (= "enum" (:type status-attr)) "status type is enum")

    ;; Verify enum values in configuration
    (let [enum-values (get-in status-attr [:configuration :values])]
      (is (= 2 (count enum-values)) "status has 2 enum values")

      ;; Verify enum value names
      (let [enum-names (set (map :name enum-values))]
        (is (contains? enum-names "draft") "enum has 'draft' value")
        (is (contains? enum-names "published") "enum has 'published' value")))

    ;; TEST DATA: Write and read records with enum values
    (println "\nTesting data writes with enum values...")

    ;; Write test items with enum values
    (let [item1 (dataset/sync-entity (id/data :test/product-entity)
                                     {:Name "Test Item 1"
                                      :Price 10.0
                                      :Description "First test item"
                                      :status "draft"})
          item2 (dataset/sync-entity (id/data :test/product-entity)
                                     {:Name "Test Item 2"
                                      :Price 20.0
                                      :Description "Second test item"
                                      :status "published"})]

      (println "  ✓ Wrote 2 items with enum values")
      (is (some? item1) "Item 1 created")
      (is (some? item2) "Item 2 created")

      ;; Read items back and verify enum values
      ;; NOTE: Attribute names are normalized to lowercase in queries
      (let [retrieved-item1 (dataset/get-entity (id/data :test/product-entity)
                                                {:name "Test Item 1"}
                                                {:name nil
                                                 :status nil
                                                 :price nil
                                                 :description nil})
            retrieved-item2 (dataset/get-entity (id/data :test/product-entity)
                                                {:name "Test Item 2"}
                                                {:name nil
                                                 :status nil
                                                 :price nil
                                                 :description nil})]

        (println "  ✓ Read items back from database")
        (println "  Item 1:" retrieved-item1)
        (println "  Item 2:" retrieved-item2)

        ;; Enum values come back as keywords
        (is (= :draft (:status retrieved-item1)) "Item 1 has correct status :draft")
        (is (= :published (:status retrieved-item2)) "Item 2 has correct status :published")
        (is (= 10.0 (:price retrieved-item1)) "Item 1 has correct price")
        (is (= 20.0 (:price retrieved-item2)) "Item 2 has correct price")
        (is (= "First test item" (:description retrieved-item1)) "Item 1 has correct description")
        (is (= "Second test item" (:description retrieved-item2)) "Item 2 has correct description")

        (println "  ✓ Enum values and all attributes read correctly")))

    (println "✓ Scenario 9.2 PASSED: Enum attribute added and data working correctly!\n")))

(defn scenario-9-expand-enum []
  "Scenario 9.3: Add value to existing enum (archived to status enum)"
  (println "\n=== Scenario 9.3: Expand Enum Values ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v4
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (println "✓ v1.0.0 deployed")

  (println "\nDeploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (println "✓ v2.0.0 deployed")

  (println "\nDeploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (println "✓ v3.0.0 deployed")

  (println "\nDeploying Dataset A v4.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (println "✓ v4.0.0 deployed")


  ;; Write test data in v4 (with draft/published)
  (println "\nWriting test data with original enum values...")
  (dataset/sync-entity (id/data :test/product-entity)
                       {:Name "Old Draft Item"
                        :Price 5.0
                        :status "draft"})
  (dataset/sync-entity (id/data :test/product-entity)
                       {:Name "Old Published Item"
                        :Price 15.0
                        :status "published"})
  (println "  ✓ Wrote items with draft and published status")

  ;; Deploy v5 (add "archived" to enum)
  (println "\nDeploying Dataset A v5.0.0 (add 'archived' to status enum)...")
  (core/deploy! db/*db* (create-test-dataset-a-v5))

  (let [model-v5 (dataset/deployed-model)
        item-entity (core/get-entity model-v5 (id/data :test/product-entity))
        status-attr (core/get-attribute item-entity (id/data :test/item-status-attr))]
    (println "✓ v5.0.0 deployed")

    ;; Verify enum now has 3 values
    (let [enum-values (get-in status-attr [:configuration :values])
          enum-names (set (map :name enum-values))]
      (is (= 3 (count enum-values)) "status now has 3 enum values")
      (is (contains? enum-names "draft") "enum still has 'draft'")
      (is (contains? enum-names "published") "enum still has 'published'")
      (is (contains? enum-names "archived") "enum now has 'archived'")
      (println "  ✓ Enum expanded to 3 values"))

    ;; Verify old data still exists and is readable
    (println "\nVerifying old data preserved...")
    (let [old-draft (dataset/get-entity (id/data :test/product-entity)
                                        {:name "Old Draft Item"}
                                        {:name nil
                                         :status nil
                                         :price nil})
          old-published (dataset/get-entity (id/data :test/product-entity)
                                            {:name "Old Published Item"}
                                            {:name nil
                                             :status nil
                                             :price nil})]
      (is (= :draft (:status old-draft)) "Old draft item still has :draft status")
      (is (= :published (:status old-published)) "Old published item still has :published status")
      (is (= 5.0 (:price old-draft)) "Old draft item data preserved")
      (is (= 15.0 (:price old-published)) "Old published item data preserved")
      (println "  ✓ Old data preserved and readable"))

    ;; Write and read new data with "archived" value
    (println "\nTesting new 'archived' enum value...")
    (dataset/sync-entity (id/data :test/product-entity)
                         {:Name "Archived Item"
                          :Price 25.0
                          :Description "This item is archived"
                          :status "archived"})
    (let [archived-item (dataset/get-entity (id/data :test/product-entity)
                                            {:name "Archived Item"}
                                            {:name nil
                                             :status nil
                                             :price nil
                                             :description nil})]
      (is (= :archived (:status archived-item)) "New item has :archived status")
      (is (= 25.0 (:price archived-item)) "Archived item has correct price")
      (println "  ✓ New enum value 'archived' works correctly"))

    (println "✓ Scenario 9.3 PASSED: Enum expanded successfully with data preserved!\n")))

(defn scenario-9-rename-enum-value []
  "Scenario 9.4: Rename enum value (published → active)"
  (println "\n=== Scenario 9.4: Rename Enum Value ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v5
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (println "✓ v1.0.0 deployed")

  (println "\nDeploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (println "✓ v2.0.0 deployed")

  (println "\nDeploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (println "✓ v3.0.0 deployed")

  (println "\nDeploying Dataset A v4.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (println "✓ v4.0.0 deployed")

  (println "\nDeploying Dataset A v5.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v5))
  (println "✓ v5.0.0 deployed")

  ;; Write test data with all three enum values
  (println "\nWriting test data with draft, published, and archived...")
  (dataset/sync-entity (id/data :test/product-entity)
                       {:Name "Draft Item for Rename Test"
                        :Price 100.0
                        :status "draft"})
  (dataset/sync-entity (id/data :test/product-entity)
                       {:Name "Published Item for Rename Test"
                        :Price 200.0
                        :status "published"})
  (dataset/sync-entity (id/data :test/product-entity)
                       {:Name "Archived Item for Rename Test"
                        :Price 300.0
                        :status "archived"})
  (println "  ✓ Wrote items with draft, published, and archived status")

  ;; Verify data before rename
  (let [published-item (dataset/get-entity (id/data :test/product-entity)
                                           {:name "Published Item for Rename Test"}
                                           {:name nil
                                            :status nil
                                            :price nil})]
    (is (= :published (:status published-item)) "Before rename: item has :published status")
    (println "  ✓ Confirmed item has :published status before rename"))

  ;; Deploy v6 (rename published → active)
  (println "\nDeploying Dataset A v6.0.0 (rename 'published' → 'active')...")
  (core/deploy! db/*db* (create-test-dataset-a-v6))

  (let [model-v6 (dataset/deployed-model)
        item-entity (core/get-entity model-v6 (id/data :test/product-entity))
        status-attr (core/get-attribute item-entity (id/data :test/item-status-attr))]
    (println "✓ v6.0.0 deployed")

    ;; Verify enum values in model
    (let [enum-values (get-in status-attr [:configuration :values])
          enum-names (set (map :name enum-values))]
      (is (= 3 (count enum-values)) "status still has 3 enum values")
      (is (contains? enum-names "draft") "enum still has 'draft'")
      (is (contains? enum-names "active") "enum now has 'active'")
      (is (not (contains? enum-names "published")) "enum no longer has 'published'")
      (is (contains? enum-names "archived") "enum still has 'archived'")
      (println "  ✓ Enum model updated: published → active"))

    ;; CRITICAL: Verify data migration - "published" → "active"
    (println "\nVerifying data migration...")
    (let [draft-item (dataset/get-entity (id/data :test/product-entity)
                                         {:name "Draft Item for Rename Test"}
                                         {:name nil
                                          :status nil
                                          :price nil})
          active-item (dataset/get-entity (id/data :test/product-entity)
                                          {:name "Published Item for Rename Test"}
                                          {:name nil
                                           :status nil
                                           :price nil})
          archived-item (dataset/get-entity (id/data :test/product-entity)
                                            {:name "Archived Item for Rename Test"}
                                            {:name nil
                                             :status nil
                                             :price nil})]

      (println "  Draft item status:" (:status draft-item))
      (println "  Active item status:" (:status active-item))
      (println "  Archived item status:" (:status archived-item))

      (is (= :draft (:status draft-item)) "Draft item still has :draft status")
      (is (= :active (:status active-item)) "Published item now has :active status (DATA MIGRATED!)")
      (is (= :archived (:status archived-item)) "Archived item still has :archived status")
      (is (= 100.0 (:price draft-item)) "Draft item data preserved")
      (is (= 200.0 (:price active-item)) "Active item data preserved")
      (is (= 300.0 (:price archived-item)) "Archived item data preserved")
      (println "  ✓ Data migrated: published → active"))

    ;; Test writing new data with "active" value
    (println "\nTesting new 'active' enum value...")
    (dataset/sync-entity (id/data :test/product-entity)
                         {:Name "New Active Item"
                          :Price 250.0
                          :status "active"})
    (let [new-active (dataset/get-entity (id/data :test/product-entity)
                                         {:name "New Active Item"}
                                         {:name nil
                                          :status nil
                                          :price nil})]
      (is (= :active (:status new-active)) "New item has :active status")
      (is (= 250.0 (:price new-active)) "New active item has correct price")
      (println "  ✓ Can write new items with 'active' status"))

    (println "✓ Scenario 9.4 PASSED: Enum value renamed and data migrated!\n")))

(defn scenario-9-add-second-enum []
  "Scenario 9.5: Add enum attribute to different entity (priority to TestComment)"
  (println "\n=== Scenario 9.5: Add Second Enum (Different Entity) ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v6
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (println "✓ v1.0.0 deployed")

  (println "\nDeploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (println "✓ v2.0.0 deployed")

  (println "\nDeploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (println "✓ v3.0.0 deployed")

  (println "\nDeploying Dataset A v4.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (println "✓ v4.0.0 deployed")

  (println "\nDeploying Dataset A v5.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v5))
  (println "✓ v5.0.0 deployed")

  (println "\nDeploying Dataset A v6.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v6))
  (println "✓ v6.0.0 deployed")

  ;; Write test comment data without priority (before enum exists)
  (println "\nWriting test comment data (no priority yet)...")
  (dataset/sync-entity (id/data :test/comment-entity)
                       {:content "This is a comment without priority"
                        :author_name "User 1"})
  (dataset/sync-entity (id/data :test/comment-entity)
                       {:content "Another comment without priority"
                        :author_name "User 2"})
  (println "  ✓ Wrote 2 comments without priority attribute")

  ;; Deploy v7 (add priority enum to TestComment)
  (println "\nDeploying Dataset A v7.0.0 (add priority enum to TestComment)...")
  (core/deploy! db/*db* (create-test-dataset-a-v7))

  (let [model-v7 (dataset/deployed-model)
        comment-entity (core/get-entity model-v7 (id/data :test/comment-entity))
        priority-attr (core/get-attribute comment-entity (id/data :test/comment-priority-attr))]
    (println "✓ v7.0.0 deployed")

    ;; Verify priority enum attribute exists
    (is (some? priority-attr) "priority attribute created")
    (is (= "priority" (:name priority-attr)) "priority attribute has correct name")
    (is (= "enum" (:type priority-attr)) "priority type is enum")

    ;; Verify enum values
    (let [enum-values (get-in priority-attr [:configuration :values])
          enum-names (set (map :name enum-values))]
      (is (= 3 (count enum-values)) "priority has 3 enum values")
      (is (contains? enum-names "low") "enum has 'low'")
      (is (contains? enum-names "medium") "enum has 'medium'")
      (is (contains? enum-names "high") "enum has 'high'")
      (println "  ✓ Priority enum created with 3 values"))

    ;; Verify old comments still exist (priority will be nil)
    (println "\nVerifying old comment data preserved...")
    (let [comment1 (dataset/get-entity (id/data :test/comment-entity)
                                       {:content "This is a comment without priority"}
                                       {:content nil
                                        :author_name nil
                                        :priority nil})
          comment2 (dataset/get-entity (id/data :test/comment-entity)
                                       {:content "Another comment without priority"}
                                       {:content nil
                                        :author_name nil
                                        :priority nil})]
      (is (some? comment1) "Comment 1 still exists")
      (is (some? comment2) "Comment 2 still exists")
      (is (= "User 1" (:author_name comment1)) "Comment 1 data preserved")
      (is (= "User 2" (:author_name comment2)) "Comment 2 data preserved")
      (is (nil? (:priority comment1)) "Comment 1 has nil priority (new optional field)")
      (is (nil? (:priority comment2)) "Comment 2 has nil priority (new optional field)")
      (println "  ✓ Old comments preserved with nil priority"))

    ;; Write and read new comments with priority values
    (println "\nTesting new priority enum values...")
    (dataset/sync-entity (id/data :test/comment-entity)
                         {:content "Low priority comment"
                          :author_name "User 3"
                          :priority "low"})
    (dataset/sync-entity (id/data :test/comment-entity)
                         {:content "High priority comment"
                          :author_name "User 4"
                          :priority "high"})

    (let [low-comment (dataset/get-entity (id/data :test/comment-entity)
                                          {:content "Low priority comment"}
                                          {:content nil
                                           :author_name nil
                                           :priority nil})
          high-comment (dataset/get-entity (id/data :test/comment-entity)
                                           {:content "High priority comment"}
                                           {:content nil
                                            :author_name nil
                                            :priority nil})]
      (is (= :low (:priority low-comment)) "Low priority comment has :low")
      (is (= :high (:priority high-comment)) "High priority comment has :high")
      (is (= "User 3" (:author_name low-comment)) "Low priority comment data correct")
      (is (= "User 4" (:author_name high-comment)) "High priority comment data correct")
      (println "  ✓ Can write and read comments with priority values"))

    ;; Verify TestItem still has its status enum working
    (println "\nVerifying TestItem status enum still works...")
    (dataset/sync-entity (id/data :test/product-entity)
                         {:Name "Item with Active Status"
                          :Price 500.0
                          :status "active"})
    (let [item (dataset/get-entity (id/data :test/product-entity)
                                   {:name "Item with Active Status"}
                                   {:name nil
                                    :status nil
                                    :price nil})]
      (is (= :active (:status item)) "TestItem status enum still works")
      (is (= 500.0 (:price item)) "TestItem data correct")
      (println "  ✓ TestItem status enum unaffected"))

    (println "✓ Scenario 9.5 PASSED: Second enum on different entity works!\n")))

(defn scenario-9-change-attribute-type []
  "Scenario 9.6: Change attribute type (TestCategory.Name from string to json)"
  (println "\n=== Scenario 9.6: Change Attribute Type ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v7
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (println "✓ v1.0.0 deployed")

  (println "\nDeploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (println "✓ v2.0.0 deployed")

  (println "\nDeploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (println "✓ v3.0.0 deployed")

  (println "\nDeploying Dataset A v4.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (println "✓ v4.0.0 deployed")

  (println "\nDeploying Dataset A v5.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v5))
  (println "✓ v5.0.0 deployed")

  (println "\nDeploying Dataset A v6.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v6))
  (println "✓ v6.0.0 deployed")

  (println "\nDeploying Dataset A v7.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v7))
  (println "✓ v7.0.0 deployed")

  ;; Write TestCategory data with string values (before type change)
  (println "\nWriting TestCategory data with string Name...")
  (dataset/sync-entity (id/data :test/category-entity)
                       {:Name "Electronics"})
  (dataset/sync-entity (id/data :test/category-entity)
                       {:Name "Books"})
  (println "  ✓ Wrote 2 categories with string Name")

  ;; Verify data before type change
  (let [electronics (dataset/get-entity (id/data :test/category-entity)
                                        {:name "Electronics"}
                                        {:name nil})]
    (is (= "Electronics" (:name electronics)) "Before change: Name is string")
    (println "  ✓ Confirmed Name is string before type change"))

  ;; Deploy v8 (change Name from string to json)
  (println "\nDeploying Dataset A v8.0.0 (change Name from string to json)...")
  (core/deploy! db/*db* (create-test-dataset-a-v8))

  (let [model-v8 (dataset/deployed-model)
        category-entity (core/get-entity model-v8 (id/data :test/category-entity))
        name-attr (core/get-attribute category-entity (id/data :test/category-name-attr))]
    (println "✓ v8.0.0 deployed")

    ;; Verify attribute type changed
    (is (= "json" (:type name-attr)) "Name attribute type changed to json")
    (println "  ✓ Attribute type changed: string → json")

    ;; Verify old data still readable
    (println "\nVerifying old string data after type conversion...")
    (let [electronics (dataset/get-entity (id/data :test/category-entity)
                                          {:name "Electronics"}
                                          {:name nil})
          books (dataset/get-entity (id/data :test/category-entity)
                                    {:name "Books"}
                                    {:name nil})]
      (println "  Electronics name:" (:name electronics))
      (println "  Books name:" (:name books))
      (is (some? electronics) "Electronics category still exists")
      (is (some? books) "Books category still exists")
      ;; Old string data is now treated as json (may be string or map depending on implementation)
      (println "  ✓ Old data preserved and readable"))

    ;; Write new category data with json values
    (println "\nTesting new json attribute...")
    (dataset/sync-entity (id/data :test/category-entity)
                         {:Name {:en "Furniture"
                                 :es "Muebles"}})
    (dataset/sync-entity (id/data :test/category-entity)
                         {:Name {:en "Clothing"
                                 :es "Ropa"
                                 :fr "Vêtements"}})

    ;; Query all categories and find the JSON ones
    (let [all-categories (dataset/search-entity (id/data :test/category-entity)
                                                {}
                                                {:name nil})
          ;; JSON names should be automatically decoded as maps
          json-categories (filter #(map? (:name %)) all-categories)
          ;; JSON fields return with string keys (like EYWA)
          furniture (first (filter #(= "Furniture" (get (:name %) "en")) json-categories))
          clothing (first (filter #(= "Clothing" (get (:name %) "en")) json-categories))]

      (println "  All categories count:" (count all-categories))
      (println "  JSON categories count:" (count json-categories))
      (println "  Furniture name:" (:name furniture))
      (println "  Clothing name:" (:name clothing))

      (is (some? furniture) "Furniture category created with json Name")
      (is (some? clothing) "Clothing category created with json Name")

      ;; JSON should be automatically decoded
      (is (map? (:name furniture)) "Furniture Name is automatically decoded as map")
      (is (map? (:name clothing)) "Clothing Name is automatically decoded as map")
      ;; JSON fields return with string keys (like EYWA)
      (is (= "Furniture" (get (:name furniture) "en")) "Can access Furniture en field")
      (is (= "Muebles" (get (:name furniture) "es")) "Can access Furniture es field")
      (is (= "Clothing" (get (:name clothing) "en")) "Can access Clothing en field")
      (is (= "Ropa" (get (:name clothing) "es")) "Can access Clothing es field")
      (is (= "Vêtements" (get (:name clothing) "fr")) "Can access Clothing fr field")

      (println "  ✓ Can write and read json data"))

    (println "✓ Scenario 9.6 PASSED: Attribute type conversion works!\n")))

(defn scenario-9-remove-attribute []
  "Scenario 9.7: Remove attribute (TestComment.author_name)"
  (println "\n=== Scenario 9.7: Remove Attribute ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v8 first
  (println "Deploying Dataset A v1.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (println "✓ v1.0.0 deployed\n")

  (println "Deploying Dataset A v2.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (println "✓ v2.0.0 deployed\n")

  (println "Deploying Dataset A v3.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (println "✓ v3.0.0 deployed\n")

  (println "Deploying Dataset A v4.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (println "✓ v4.0.0 deployed\n")

  (println "Deploying Dataset A v5.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v5))
  (println "✓ v5.0.0 deployed\n")

  (println "Deploying Dataset A v6.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v6))
  (println "✓ v6.0.0 deployed\n")

  (println "Deploying Dataset A v7.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v7))
  (println "✓ v7.0.0 deployed\n")

  (println "Deploying Dataset A v8.0.0...")
  (core/deploy! db/*db* (create-test-dataset-a-v8))
  (println "✓ v8.0.0 deployed\n")

  ;; Write TestComment data with author_name
  (println "Writing TestComment data with author_name...")
  (dataset/sync-entity (id/data :test/comment-entity)
                       {:content "Great product!"
                        :author_name "Alice"
                        :priority :medium})
  (dataset/sync-entity (id/data :test/comment-entity)
                       {:content "Could be better"
                        :author_name "Bob"
                        :priority :low})
  (println "  ✓ Wrote 2 comments with author_name\n")

  ;; Deploy v9 - remove author_name attribute
  (println "Deploying Dataset A v9.0.0 (remove author_name attribute)...")
  (core/deploy! db/*db* (create-test-dataset-a-v9))

  (let [model-v9 (dataset/deployed-model)
        comment-entity (core/get-entity model-v9 (id/data :test/comment-entity))
        active-attributes (filter :active (:attributes comment-entity))
        attribute-names (map :name active-attributes)]
    (println "✓ v9.0.0 deployed")
    (is (not (some #{"author_name"} attribute-names)) "author_name attribute removed from schema")
    (println "  ✓ author_name attribute removed from schema\n"))

  ;; Verify existing data still exists (old field dropped from schema but data intact)
  (println "Verifying existing comment data...")
  (let [comments (dataset/search-entity (id/data :test/comment-entity)
                                        {}
                                        {:content nil
                                         :priority nil})]
    (println "  Comment count:" (count comments))
    (is (= 2 (count comments)) "Existing comments preserved")
    (is (every? #(contains? % :content) comments) "content field still accessible")
    (is (every? #(contains? % :priority) comments) "priority field still accessible")
    ;; author_name should NOT be returned (removed from schema)
    (is (not-any? #(contains? % :author_name) comments) "author_name field no longer in results")
    (println "  ✓ Existing comments preserved (without removed field)\n"))

  ;; Write new comment without author_name
  (println "Writing new comment without author_name...")
  (dataset/sync-entity (id/data :test/comment-entity)
                       {:content "Works fine without author_name"
                        :priority :high})
  (let [comments (dataset/search-entity (id/data :test/comment-entity)
                                        {}
                                        {:content nil
                                         :priority nil})]
    (is (= 3 (count comments)) "New comment created successfully")
    (println "  ✓ New comment created without removed attribute\n"))

  (println "✓ Scenario 9.7 PASSED: Attribute removal works!\n"))

(defn scenario-10-remove-relation []
  "Scenario 9.8: Remove relation (TestComment→TestItem)"
  (println "\n=== Scenario 9.8: Remove Relation ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v9 first
  (println "Deploying Dataset A v1-v9...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (core/deploy! db/*db* (create-test-dataset-a-v5))
  (core/deploy! db/*db* (create-test-dataset-a-v6))
  (core/deploy! db/*db* (create-test-dataset-a-v7))
  (core/deploy! db/*db* (create-test-dataset-a-v8))
  (core/deploy! db/*db* (create-test-dataset-a-v9))
  (println "✓ v1-v9 deployed\n")

  ;; Create item and comments with relation
  (println "Creating TestItem and linking TestComments...")
  (let [item (dataset/sync-entity (id/data :test/product-entity)
                                  {:Name "Laptop"
                                   :Price 999.99})]
    (dataset/sync-entity (id/data :test/comment-entity)
                         {:content "Great laptop!"
                          :priority :high
                          :item item})
    (dataset/sync-entity (id/data :test/comment-entity)
                         {:content "Fast shipping"
                          :priority :medium
                          :item item})
    (println "  ✓ Created item with 2 linked comments\n"))

  ;; Deploy v10 - remove Comment→Item relation
  (println "Deploying Dataset A v10.0.0 (remove Comment→Item relation)...")
  (core/deploy! db/*db* (create-test-dataset-a-v10))

  (let [model-v10 (dataset/deployed-model)
        comment-entity (core/get-entity model-v10 (id/data :test/comment-entity))
        all-relations (core/focus-entity-relations model-v10 comment-entity)
        active-relations (filter :active all-relations)
        relation-labels (set (map :to-label active-relations))]
    (println "✓ v10.0.0 deployed")
    (is (not (contains? relation-labels "item")) "Comment→Item relation removed from schema")
    (println "  ✓ Comment→Item relation removed from schema\n"))

  ;; Verify entities still exist independently
  (println "Verifying entities still exist...")
  (let [items (dataset/search-entity (id/data :test/product-entity) {} {:name nil})
        comments (dataset/search-entity (id/data :test/comment-entity) {} {:content nil})]
    (is (= 1 (count items)) "TestItem still exists")
    (is (= 2 (count comments)) "TestComments still exist")
    (println "  ✓ Both entities preserved after relation removal\n"))

  (println "✓ Scenario 9.8 PASSED: Relation removal works!\n"))

(defn scenario-11-remove-entity []
  "Scenario 9.9: Remove entity (TestComment entirely)"
  (println "\n=== Scenario 9.9: Remove Entity ===")

  ;; Cleanup from previous scenario
  (try
    (core/destroy! db/*db* {(id/key) (id/data :test/dataset-a) :name "TestA"})
    (catch Exception _))

  ;; Deploy v1-v10 first
  (println "Deploying Dataset A v1-v10...")
  (core/deploy! db/*db* (create-test-dataset-a))
  (core/deploy! db/*db* (create-test-dataset-a-v2))
  (core/deploy! db/*db* (create-test-dataset-a-v3))
  (core/deploy! db/*db* (create-test-dataset-a-v4))
  (core/deploy! db/*db* (create-test-dataset-a-v5))
  (core/deploy! db/*db* (create-test-dataset-a-v6))
  (core/deploy! db/*db* (create-test-dataset-a-v7))
  (core/deploy! db/*db* (create-test-dataset-a-v8))
  (core/deploy! db/*db* (create-test-dataset-a-v9))
  (core/deploy! db/*db* (create-test-dataset-a-v10))
  (println "✓ v1-v10 deployed\n")

  ;; Create some test data
  (println "Creating test data...")
  (dataset/sync-entity (id/data :test/product-entity)
                       {:Name "Laptop"
                        :Price 999.99})
  (dataset/sync-entity (id/data :test/comment-entity)
                       {:content "Will be removed"
                        :priority :low})
  (println "  ✓ Created TestItem and TestComment\n")

  ;; Deploy v11 - remove TestComment entity
  (println "Deploying Dataset A v11.0.0 (remove TestComment entity)...")
  (core/deploy! db/*db* (create-test-dataset-a-v11))

  (let [model-v11 (dataset/deployed-model)
        all-entities (core/get-entities model-v11)
        active-entities (filter :active all-entities)
        entity-names (set (map :name active-entities))]
    (println "✓ v11.0.0 deployed")
    (is (not (contains? entity-names "TestComment")) "TestComment entity removed from schema")
    (is (contains? entity-names "TestItem") "TestItem entity still exists")
    (is (contains? entity-names "TestCategory") "TestCategory entity still exists")
    (println "  ✓ TestComment entity removed from schema")
    (println "  ✓ Other entities preserved\n"))

  ;; Verify TestItem still works
  (println "Verifying remaining entities...")
  (let [items (dataset/search-entity (id/data :test/product-entity) {} {:name nil})]
    (is (= 1 (count items)) "TestItem still queryable")
    (println "  ✓ Remaining entities fully functional\n"))

  (println "✓ Scenario 9.9 PASSED: Entity removal works!\n"))

;; Main comprehensive test (matches EYWA pattern)
(deftest test-all-deployment-scenarios
  (testing "EYWA-style deployment scenarios"
    (println "\n" (apply str (repeat 60 "=")))
    (println "SYNTHIGY DEPLOYMENT TESTS (from EYWA)")
    (println (apply str (repeat 60 "=")) "\n")

    ;; Run scenario 1 - shared entities with recall!
    (scenario-1-shared-entities)

    ;; Run scenario 2 - destroy! with shared entities
    (scenario-2-destroy-with-shared-entities)

    ;; Run scenario 3 - destroy! deletes all versions
    (scenario-3-destroy-all-versions)

    ;; Run scenario 9.1 - add entity test
    (scenario-9-add-entity)

    ;; Run scenario 9.2 - add enum attribute test
    (scenario-9-add-enum-attribute)

    ;; Run scenario 9.3 - expand enum values test
    (scenario-9-expand-enum)

    ;; Run scenario 9.4 - rename enum value test
    (scenario-9-rename-enum-value)

    ;; Run scenario 9.5 - add second enum to different entity
    (scenario-9-add-second-enum)

    ;; Run scenario 9.6 - change attribute type
    (scenario-9-change-attribute-type)

    ;; Run scenario 9.7 - remove attribute
    (scenario-9-remove-attribute)

    ;; Run scenario 9.8 - remove relation
    (scenario-10-remove-relation)

    ;; Run scenario 9.9 - remove entity
    (scenario-11-remove-entity)

    ;; Cleanup after all scenarios
    (cleanup-test-datasets!)

    (println "\n" (apply str (repeat 60 "=")))
    (println "TEST SUITE COMPLETE")
    (println (apply str (repeat 60 "=")) "\n")))
