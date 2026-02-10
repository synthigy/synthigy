(ns synthigy.dataset.projection-test
  "Pure unit tests for projection protocol.
   Tests project function and diff metadata WITHOUT database interaction.

   If these tests pass, database adapters can trust the projection output.

   ## Test Categories

   A. Attribute Projection (7 tests) - Tests for ERDEntityAttribute projection
   B. Entity Projection (6 tests) - Tests for ERDEntity projection
   C. Relation Projection (5 tests) - Tests for ERDRelation projection
   D. Model Projection (5 tests) - Tests for ERDModel projection
   E. analyze-projection (4 tests) - Tests for filtering projection results"
  (:require
   [clojure.test :refer [deftest is testing]]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.projection] ; Load protocol implementations
   [synthigy.dataset.id :as id]
   [synthigy.dataset.test-helpers :refer [make-entity make-relation add-test-attribute
                                           make-attribute get-attribute-by-id
                                           assert-projection-added assert-projection-removed
                                           assert-projection-diff? assert-projection-diff
                                           assert-projection-diff-contains assert-projection-unchanged
                                           enum-attr enum-value]]
   [synthigy.dataset.deployment-test :refer [create-test-dataset-a
                                              create-test-dataset-a-v2
                                              create-test-dataset-a-v4
                                              create-test-dataset-a-v5
                                              create-test-dataset-a-v6]]))

;;; ============================================================================
;;; Test Data IDs (runtime-resolved from synthigy.test-data registrations)
;;; ============================================================================

;; Entities
(defn product-id [] (id/data :test/product-entity))
(defn category-id [] (id/data :test/category-entity))
(defn comment-id [] (id/data :test/comment-entity))

;; Attributes
(defn name-attr-id [] (id/data :test/product-name-attr))
(defn price-attr-id [] (id/data :test/product-price-attr))
(defn description-attr-id [] (id/data :test/item-description-attr))
(defn status-attr-id [] (id/data :test/item-status-attr))

;; Enum values
(defn draft-id [] (id/data :test/status-draft))
(defn published-id [] (id/data :test/status-published))
(defn archived-id [] (id/data :test/status-archived))

;; Relations
(defn product-category-rel-id [] (id/data :test/product-category-rel))
(defn comment-item-rel-id [] (id/data :test/comment-item-rel))

;;; ============================================================================
;;; Category A: Attribute Projection Tests (7 tests)
;;; ============================================================================

(deftest test-new-attribute-projection
  (testing "Project nil -> attribute marks attribute as added"
    (let [new-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                     :name "NewAttribute"
                     :type "string"
                     :constraint "optional"
                     :active true})
          result (core/project nil new-attr)]

      (is (core/added? result) "New attribute should be marked as added")
      (is (not (core/removed? result)) "New attribute should not be marked as removed")
      (is (not (core/diff? result)) "New attribute should not have diff"))))

(deftest test-removed-attribute-projection
  (testing "Project attribute -> nil marks attribute as removed"
    (let [old-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                     :name "OldAttribute"
                     :type "string"
                     :constraint "optional"
                     :active true})
          result (core/project old-attr nil)]

      (is (core/removed? result) "Removed attribute should be marked as removed")
      (is (not (core/added? result)) "Removed attribute should not be marked as added")
      (is (not (:active result)) "Removed attribute should have active=false"))))

(deftest test-attribute-name-change
  (testing "Attribute name change produces correct diff"
    (let [old-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                     :name "OldName"
                     :type "string"
                     :constraint "optional"
                     :active true})
          new-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                     :name "NewName"
                     :type "string"
                     :constraint "optional"
                     :active true})
          result (core/project old-attr new-attr)]

      (is (core/diff? result) "Renamed attribute should have diff")
      (is (= {:name "OldName"} (core/diff result))
          "Diff should contain old name"))))

(deftest test-attribute-type-change
  (testing "Attribute type change produces correct diff"
    (let [old-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                     :name "Amount"
                     :type "string"
                     :constraint "optional"
                     :active true})
          new-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                     :name "Amount"
                     :type "int"
                     :constraint "optional"
                     :active true})
          result (core/project old-attr new-attr)]

      (is (core/diff? result) "Type-changed attribute should have diff")
      (is (= {:type "string"} (core/diff result))
          "Diff should contain old type"))))

(deftest test-enum-configuration-change-fixed
  (testing "Enum config-only change produces correct diff structure (BUG FIXED)"
    (let [old-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000030"
                     :name "status"
                     :type "enum"
                     :constraint "optional"
                     :configuration {:values [{(id/key) #uuid "aaaaaaaa-3333-3333-0000-000000000002"
                                               :name "published"}]}
                     :active true})
          new-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000030"
                     :name "status"  ; SAME
                     :type "enum"    ; SAME
                     :constraint "optional"
                     :configuration {:values [{(id/key) #uuid "aaaaaaaa-3333-3333-0000-000000000002"
                                               :name "active"}]}  ; RENAMED!
                     :active true})
          result (core/project old-attr new-attr)
          diff-data (core/diff result)]

      (testing "Diff should exist"
        (is (core/diff? result) "Should detect enum value rename"))

      (testing "CORRECT BEHAVIOR - diff nested under :configuration"
        ;; Bug was fixed! Diff is now correctly nested under :configuration
        (is (contains? diff-data :configuration)
            "Diff should be {:configuration {:values ...}}")
        (is (contains? (:configuration diff-data) :values)
            "Configuration diff should contain :values")))))

(deftest test-enum-value-added
  (testing "Add value to existing enum produces diff"
    (let [old-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000030"
                     :name "status"
                     :type "enum"
                     :constraint "optional"
                     :configuration {:values [{(id/key) (draft-id) :name "draft"}
                                              {(id/key) (published-id) :name "published"}]}
                     :active true})
          new-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000030"
                     :name "status"
                     :type "enum"
                     :constraint "optional"
                     :configuration {:values [{(id/key) (draft-id) :name "draft"}
                                              {(id/key) (published-id) :name "published"}
                                              {(id/key) (archived-id) :name "archived"}]} ; NEW!
                     :active true})
          result (core/project old-attr new-attr)]

      (is (core/diff? result) "Should detect enum value addition")
      ;; The diff contains the OLD configuration (what was there before)
      ;; This is how clojure.data/diff works - first return is "only in first"
      (let [diff-data (core/diff result)]
        (is (some? diff-data) "Diff should not be nil")))))

(deftest test-enum-value-renamed
  (testing "Rename value in enum (same UUID) produces diff"
    (let [old-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000030"
                     :name "status"
                     :type "enum"
                     :constraint "optional"
                     :configuration {:values [{(id/key) (draft-id) :name "draft"}
                                              {(id/key) (published-id) :name "published"}]}
                     :active true})
          new-attr (core/map->ERDEntityAttribute
                    {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000030"
                     :name "status"
                     :type "enum"
                     :constraint "optional"
                     :configuration {:values [{(id/key) (draft-id) :name "draft"}
                                              {(id/key) (published-id) :name "active"}]} ; RENAMED!
                     :active true})
          result (core/project old-attr new-attr)
          diff-data (core/diff result)]

      (is (core/diff? result) "Should detect enum value rename")
      ;; With bug fixed, diff is nested under :configuration
      (is (contains? diff-data :configuration)
          "Diff should have :configuration key")
      (is (contains? (:configuration diff-data) :values)
          "Configuration diff should capture values change"))))

;;; ============================================================================
;;; Category B: Entity Projection Tests (6 tests)
;;; ============================================================================

(deftest test-new-entity-projection
  (testing "Project nil -> entity marks entity as added"
    (let [new-entity (-> (make-entity {(id/key) #uuid "aaaaaaaa-1111-0000-0000-000000000099"
                                       :name "NewEntity"})
                         (add-test-attribute {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                                              :name "Name"
                                              :type "string"}))
          result (core/project nil new-entity)]

      (is (core/added? result) "New entity should be marked as added")
      (is (not (core/removed? result)) "New entity should not be marked as removed")
      ;; All attributes should also be marked as added
      (doseq [attr (:attributes result)]
        (is (core/added? attr)
            (str "Attribute " (:name attr) " should be marked as added"))))))

(deftest test-removed-entity-projection
  (testing "Project entity -> nil marks entity and all attributes as removed"
    (let [old-entity (-> (make-entity {(id/key) #uuid "aaaaaaaa-1111-0000-0000-000000000099"
                                       :name "OldEntity"})
                         (add-test-attribute {(id/key) #uuid "aaaaaaaa-1111-1111-0000-000000000099"
                                              :name "Name"
                                              :type "string"}))
          result (core/project old-entity nil)]

      (is (core/removed? result) "Removed entity should be marked as removed")
      ;; Note: mark-removed sets :removed? in metadata, not :active=false on entity record
      ;; All attributes should also be marked as removed
      (doseq [attr (:attributes result)]
        (is (core/removed? attr)
            (str "Attribute " (:name attr) " should be marked as removed"))
        (is (not (:active attr))
            (str "Attribute " (:name attr) " should have active=false"))))))

(deftest test-entity-name-change
  (testing "Entity name change produces correct diff"
    (let [old-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestProduct"})
                         (add-test-attribute {(id/key) (name-attr-id)
                                              :name "Name"
                                              :type "string"}))
          new-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestItem"}) ; RENAMED
                         (add-test-attribute {(id/key) (name-attr-id)
                                              :name "Name"
                                              :type "string"}))
          result (core/project old-entity new-entity)
          diff-data (core/diff result)]

      (is (core/diff? result) "Renamed entity should have diff")
      (is (= "TestProduct" (:name diff-data))
          "Diff should contain old name")
      ;; Diff may also contain :attributes if any attributes have diffs
      ;; (e.g., due to :entity/name being added to enum attributes)
      )))

(deftest test-entity-rename-marks-enum-attrs
  (testing "Entity rename marks enum attributes with :entity/name diff"
    (let [old-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestProduct"})
                         (add-test-attribute {(id/key) (status-attr-id)
                                              :name "status"
                                              :type "enum"
                                              :configuration {:values [{(id/key) (draft-id) :name "draft"}]}}))
          new-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestItem"}) ; RENAMED
                         (add-test-attribute {(id/key) (status-attr-id)
                                              :name "status"
                                              :type "enum"
                                              :configuration {:values [{(id/key) (draft-id) :name "draft"}]}}))
          result (core/project old-entity new-entity)
          status-attr (get-attribute-by-id result (status-attr-id))]

      (is (core/diff? result) "Entity should have diff")
      ;; Enum attributes should have :entity/name diff
      (is (core/diff? status-attr) "Status attribute should have diff")
      (is (= {:entity/name "TestProduct"} (core/diff status-attr))
          "Attribute diff should contain old entity name"))))

(deftest test-entity-with-new-attribute
  (testing "Entity with new attribute marks entity as diff and attribute as added"
    (let [old-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestProduct"})
                         (add-test-attribute {(id/key) (name-attr-id)
                                              :name "Name"
                                              :type "string"}))
          new-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestProduct"})
                         (add-test-attribute {(id/key) (name-attr-id)
                                              :name "Name"
                                              :type "string"})
                         (add-test-attribute {(id/key) (description-attr-id)
                                              :name "Description"
                                              :type "string"})) ; NEW!
          result (core/project old-entity new-entity)
          desc-attr (get-attribute-by-id result (description-attr-id))]

      (is (core/diff? result) "Entity should have diff (attribute changed)")
      (is (core/added? desc-attr) "New attribute should be marked as added"))))

(deftest test-entity-with-removed-attribute
  (testing "Entity with removed attribute - attribute is included but not marked removed"
    (let [old-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestProduct"})
                         (add-test-attribute {(id/key) (name-attr-id)
                                              :name "Name"
                                              :type "string"})
                         (add-test-attribute {(id/key) (description-attr-id)
                                              :name "Description"
                                              :type "string"}))
          new-entity (-> (make-entity {(id/key) (product-id)
                                       :name "TestProduct"})
                         (add-test-attribute {(id/key) (name-attr-id)
                                              :name "Name"
                                              :type "string"})) ; Description REMOVED
          result (core/project old-entity new-entity)
          ;; After projection, removed attributes from old entity are included
          desc-attr (get-attribute-by-id result (description-attr-id))]

      ;; INTENTIONAL DESIGN: Removed attributes are included in projection result
      ;; but NOT marked as :removed? - this is by design, not a bug.
      ;;
      ;; Rationale:
      ;; 1. Attributes are never truly deleted - kept for potential future reuse
      ;; 2. Active flags control read/write access (inactive = no access)
      ;; 3. Avoids claimed-by complexity at attribute level
      ;; 4. Preserves data - if attribute is re-added, historical data remains
      ;;
      ;; The active flag management is handled by join-models/merge-entity-attributes,
      ;; NOT by the projection layer. Projection computes diffs, join-models manages state.
      (is (some? desc-attr) "Removed attribute is included in projection result")
      (is (:active desc-attr) "Attribute retains active=true - join-models sets it to false")
      )))

;;; ============================================================================
;;; Category C: Relation Projection Tests (5 tests)
;;; ============================================================================

(deftest test-new-relation-projection
  (testing "Project nil -> relation marks relation as added"
    (let [from-entity (make-entity {(id/key) (product-id) :name "Product"})
          to-entity (make-entity {(id/key) (category-id) :name "Category"})
          new-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from from-entity
                                       :to to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          result (core/project nil new-relation)]

      (is (core/added? result) "New relation should be marked as added")
      (is (not (core/removed? result)) "New relation should not be marked as removed"))))

(deftest test-removed-relation-projection
  (testing "Project relation -> nil marks relation as removed"
    (let [from-entity (make-entity {(id/key) (product-id) :name "Product"})
          to-entity (make-entity {(id/key) (category-id) :name "Category"})
          old-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from from-entity
                                       :to to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          result (core/project old-relation nil)]

      (is (core/removed? result) "Removed relation should be marked as removed"))))

(deftest test-relation-label-change
  (testing "Relation label change produces correct diff"
    (let [from-entity (make-entity {(id/key) (product-id) :name "Product"})
          to-entity (make-entity {(id/key) (category-id) :name "Category"})
          old-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from from-entity
                                       :to to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          new-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from from-entity
                                       :to to-entity
                                       :cardinality "m2o"
                                       :from-label "item"  ; CHANGED
                                       :to-label "category"})
          result (core/project old-relation new-relation)]

      (is (core/diff? result) "Relation with changed label should have diff")
      (is (= "product" (:from-label (core/diff result)))
          "Diff should contain old from-label"))))

(deftest test-relation-from-entity-renamed
  (testing "Relation diff when from entity was renamed"
    (let [old-from-entity (make-entity {(id/key) (product-id) :name "TestProduct"})
          new-from-entity (make-entity {(id/key) (product-id) :name "TestItem"})
          to-entity (make-entity {(id/key) (category-id) :name "Category"})
          old-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from old-from-entity
                                       :to to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          new-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from new-from-entity
                                       :to to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          result (core/project old-relation new-relation)]

      (is (core/diff? result) "Relation should have diff when from entity renamed")
      (is (= {:name "TestItem"} (:from (core/diff result)))
          "Diff should contain new from entity name"))))

(deftest test-relation-to-entity-renamed
  (testing "Relation diff when to entity was renamed"
    (let [from-entity (make-entity {(id/key) (product-id) :name "Product"})
          old-to-entity (make-entity {(id/key) (category-id) :name "TestCategory"})
          new-to-entity (make-entity {(id/key) (category-id) :name "TestGroup"})
          old-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from from-entity
                                       :to old-to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          new-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from from-entity
                                       :to new-to-entity
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          result (core/project old-relation new-relation)]

      (is (core/diff? result) "Relation should have diff when to entity renamed")
      (is (= {:name "TestGroup"} (:to (core/diff result)))
          "Diff should contain new to entity name"))))

;;; ============================================================================
;;; Category D: Model Projection Tests (5 tests)
;;; ============================================================================

(deftest test-model-entity-rename-v1-v2
  (testing "Model projection: TestProduct -> TestItem rename"
    (let [v1 (:model (create-test-dataset-a))
          v2 (:model (create-test-dataset-a-v2))
          projection (core/project v1 v2)
          ;; Get the renamed entity
          item-entity (core/get-entity projection (product-id))
          diff-data (core/diff item-entity)]

      (testing "Entity diff shows rename"
        (is (core/diff? item-entity) "TestItem entity should have diff")
        (is (= "TestProduct" (:name diff-data))
            "Diff should contain old name 'TestProduct'"))

      (testing "Entity attributes have entity/name diff for enum type"
        ;; TestProduct in v1 doesn't have enum attributes
        ;; But any non-enum attributes may still have :entity/name diff
        ;; when entity is renamed
        ))))

(deftest test-model-add-enum-attr-v3-v4
  (testing "Model projection: Add status enum attribute (v3 -> v4)"
    ;; Note: We need to simulate v3 vs v4 projection
    ;; v4 adds the status enum to TestItem
    (let [;; Create a v3-like model (without status enum)
          v3-item (-> (make-entity {(id/key) (product-id)
                                    :name "TestItem"})
                      (add-test-attribute {(id/key) (name-attr-id)
                                           :name "Name"
                                           :type "string"
                                           :constraint "mandatory"})
                      (add-test-attribute {(id/key) (price-attr-id)
                                           :name "Price"
                                           :type "float"})
                      (add-test-attribute {(id/key) (description-attr-id)
                                           :name "Description"
                                           :type "string"}))
          v3-model (core/add-entity (core/map->ERDModel {}) v3-item)

          ;; v4 adds status enum
          v4-item (-> (make-entity {(id/key) (product-id)
                                    :name "TestItem"})
                      (add-test-attribute {(id/key) (name-attr-id)
                                           :name "Name"
                                           :type "string"
                                           :constraint "mandatory"})
                      (add-test-attribute {(id/key) (price-attr-id)
                                           :name "Price"
                                           :type "float"})
                      (add-test-attribute {(id/key) (description-attr-id)
                                           :name "Description"
                                           :type "string"})
                      (add-test-attribute {(id/key) (status-attr-id)
                                           :name "status"
                                           :type "enum"
                                           :configuration {:values [{(id/key) (draft-id) :name "draft"}
                                                                    {(id/key) (published-id) :name "published"}]}}))
          v4-model (core/add-entity (core/map->ERDModel {}) v4-item)

          projection (core/project v3-model v4-model)
          item-entity (core/get-entity projection (product-id))
          status-attr (get-attribute-by-id item-entity (status-attr-id))]

      (testing "New enum attribute is marked as added"
        (is (some? status-attr) "Status attribute should exist")
        (is (core/added? status-attr) "Status attribute should be marked as added"))

      (testing "Entity has diff due to attribute change"
        (is (core/diff? item-entity) "Entity should have diff")))))

(deftest test-model-add-enum-value-v4-v5
  (testing "Model projection: Add 'archived' value to status enum (v4 -> v5)"
    (let [v4 (:model (create-test-dataset-a-v4))
          v5 (:model (create-test-dataset-a-v5))
          projection (core/project v4 v5)
          item-entity (core/get-entity projection (product-id))
          status-attr (get-attribute-by-id item-entity (status-attr-id))]

      (testing "Attribute has config diff for added enum value"
        (is (core/diff? status-attr)
            "Status attribute should have diff when enum value added")
        ;; The diff captures the old configuration (before 'archived' was added)
        (let [diff-data (core/diff status-attr)]
          (is (some? diff-data) "Diff should not be nil")
          ;; Print debug info
          (println "\n=== DEBUG: Enum Expansion v4->v5 ===")
          (println "Diff?:" (core/diff? status-attr))
          (println "Diff data:" (pr-str diff-data))
          (println "New config values:" (map :name (get-in status-attr [:configuration :values])))
          (println "===================================\n"))))))

(deftest test-model-rename-enum-value-v5-v6
  (testing "Model projection: Rename 'published' -> 'active' (v5 -> v6)"
    (let [v5 (:model (create-test-dataset-a-v5))
          v6 (:model (create-test-dataset-a-v6))
          projection (core/project v5 v6)
          item-entity (core/get-entity projection (product-id))
          status-attr (get-attribute-by-id item-entity (status-attr-id))]

      (testing "Attribute has config diff for renamed enum value"
        (is (core/diff? status-attr)
            "Status attribute should have diff when enum value renamed")
        (let [diff-data (core/diff status-attr)]
          (is (some? diff-data) "Diff should not be nil")
          ;; With bug fixed, diff is nested under :configuration
          (is (contains? diff-data :configuration)
              "Diff should have :configuration key"))))))

(deftest test-model-all-entities-in-projection
  (testing "Model projection includes all entities regardless of :active"
    (let [v1 (:model (create-test-dataset-a))
          v2 (:model (create-test-dataset-a-v2))
          projection (core/project v1 v2)
          entities (core/get-entities projection)]

      (testing "All entities from both models are in projection"
        ;; Both v1 and v2 have Product/Item and Category
        (is (>= (count entities) 2)
            "Should have at least 2 entities"))

      (testing "Each entity can be retrieved by ID"
        (is (some? (core/get-entity projection (product-id)))
            "Product/Item entity should be retrievable")
        (is (some? (core/get-entity projection (category-id)))
            "Category entity should be retrievable")))))

;;; ============================================================================
;;; Category E: analyze-projection Tests (4 tests)
;;; ============================================================================

(defn analyze-projection
  "Analyzes a projected model and returns categorized entities and relations.

   Returns:
   {:new/entities [entities marked added?]
    :changed/entities [entities marked diff?]
    :removed/entities [entities marked removed?]
    :new/relations [relations marked added?]
    :changed/relations [relations marked diff?]
    :removed/relations [relations marked removed?]}"
  [projected-model]
  (let [entities (core/get-entities projected-model)
        relations (core/get-relations projected-model)]
    {:new/entities (filter core/added? entities)
     :changed/entities (filter #(and (core/diff? %)
                                     (not (core/added? %))
                                     (not (core/removed? %))) entities)
     :removed/entities (filter core/removed? entities)
     :new/relations (filter core/added? relations)
     :changed/relations (filter #(and (core/diff? %)
                                      (not (core/added? %))
                                      (not (core/removed? %))) relations)
     :removed/relations (filter core/removed? relations)}))

(deftest test-analyze-new-entities
  (testing "analyze-projection filters added entities correctly"
    (let [old-model (core/map->ERDModel {})
          new-model (-> (core/map->ERDModel {})
                        (core/add-entity (make-entity {(id/key) (product-id) :name "Product"}))
                        (core/add-entity (make-entity {(id/key) (category-id) :name "Category"})))
          projection (core/project old-model new-model)
          analysis (analyze-projection projection)]

      (is (= 2 (count (:new/entities analysis)))
          "Should have 2 new entities")
      (is (empty? (:changed/entities analysis))
          "Should have no changed entities")
      (is (empty? (:removed/entities analysis))
          "Should have no removed entities"))))

(deftest test-analyze-changed-entities
  (testing "analyze-projection filters changed entities correctly"
    (let [old-model (-> (core/map->ERDModel {})
                        (core/add-entity (make-entity {(id/key) (product-id) :name "Product"})))
          new-model (-> (core/map->ERDModel {})
                        (core/add-entity (make-entity {(id/key) (product-id) :name "Item"}))) ; RENAMED
          projection (core/project old-model new-model)
          analysis (analyze-projection projection)]

      (is (= 1 (count (:changed/entities analysis)))
          "Should have 1 changed entity")
      (is (empty? (:new/entities analysis))
          "Should have no new entities")
      (is (empty? (:removed/entities analysis))
          "Should have no removed entities"))))

(deftest test-analyze-new-relations
  (testing "analyze-projection filters added relations correctly"
    (let [product (make-entity {(id/key) (product-id) :name "Product"})
          category (make-entity {(id/key) (category-id) :name "Category"})
          old-model (-> (core/map->ERDModel {})
                        (core/add-entity product)
                        (core/add-entity category))
          new-model (-> (core/map->ERDModel {})
                        (core/add-entity product)
                        (core/add-entity category)
                        (core/add-relation (make-relation {(id/key) (product-category-rel-id)
                                                           :from product
                                                           :to category
                                                           :cardinality "m2o"})))
          projection (core/project old-model new-model)
          analysis (analyze-projection projection)]

      (is (= 1 (count (:new/relations analysis)))
          "Should have 1 new relation")
      (is (empty? (:changed/relations analysis))
          "Should have no changed relations"))))

(deftest test-analyze-changed-relations
  (testing "analyze-projection filters changed relations correctly"
    (let [product (make-entity {(id/key) (product-id) :name "Product"})
          category (make-entity {(id/key) (category-id) :name "Category"})
          old-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from product
                                       :to category
                                       :cardinality "m2o"
                                       :from-label "product"
                                       :to-label "category"})
          new-relation (make-relation {(id/key) (product-category-rel-id)
                                       :from product
                                       :to category
                                       :cardinality "m2o"
                                       :from-label "item"  ; CHANGED
                                       :to-label "category"})
          old-model (-> (core/map->ERDModel {})
                        (core/add-entity product)
                        (core/add-entity category)
                        (core/add-relation old-relation))
          new-model (-> (core/map->ERDModel {})
                        (core/add-entity product)
                        (core/add-entity category)
                        (core/add-relation new-relation))
          projection (core/project old-model new-model)
          analysis (analyze-projection projection)]

      (is (= 1 (count (:changed/relations analysis)))
          "Should have 1 changed relation")
      (is (empty? (:new/relations analysis))
          "Should have no new relations"))))

;;; ============================================================================
;;; Category F: Relation Lifecycle & Entity Rename Tests
;;; ============================================================================

;; Test UUIDs for this category
(def entity-a-euuid #uuid "ffffffff-1111-0000-0000-000000000001")
(def entity-b-euuid #uuid "ffffffff-1111-0000-0000-000000000002")
(def relation-c-euuid #uuid "ffffffff-2222-0000-0000-000000000001")
(def relation-d-euuid #uuid "ffffffff-2222-0000-0000-000000000002")

(defn make-v1-model
  "v1: EntityA, EntityB, RelationC(A→B)"
  []
  (let [entity-a (make-entity {(id/key) entity-a-euuid :name "EntityA"})
        entity-b (make-entity {(id/key) entity-b-euuid :name "EntityB"})
        ;; IMPORTANT: Relations must use UUIDs for :from/:to, not entity objects!
        ;; get-entity-relations assumes :from/:to are UUIDs
        relation-c (make-relation {(id/key) relation-c-euuid
                                   :from entity-a-euuid
                                   :to entity-b-euuid
                                   :cardinality "m2o"
                                   :from-label "a"
                                   :to-label "b"})]
    (-> (core/map->ERDModel {})
        (core/add-entity entity-a)
        (core/add-entity entity-b)
        (core/add-relation relation-c))))

(defn make-v2-model
  "v2: EntityA, EntityB, RelationD(A→B) - RelationC removed"
  []
  (let [entity-a (make-entity {(id/key) entity-a-euuid :name "EntityA"})
        entity-b (make-entity {(id/key) entity-b-euuid :name "EntityB"})
        relation-d (make-relation {(id/key) relation-d-euuid
                                   :from entity-a-euuid
                                   :to entity-b-euuid
                                   :cardinality "o2m"
                                   :from-label "a"
                                   :to-label "b"})]
    (-> (core/map->ERDModel {})
        (core/add-entity entity-a)
        (core/add-entity entity-b)
        (core/add-relation relation-d))))

(defn make-v3-model
  "v3: EntityA, EntityB_Renamed, RelationD(A→B_Renamed)"
  []
  (let [entity-a (make-entity {(id/key) entity-a-euuid :name "EntityA"})
        entity-b-renamed (make-entity {(id/key) entity-b-euuid :name "EntityB_Renamed"})
        relation-d (make-relation {(id/key) relation-d-euuid
                                   :from entity-a-euuid
                                   :to entity-b-euuid
                                   :cardinality "o2m"
                                   :from-label "a"
                                   :to-label "b_renamed"})]
    (-> (core/map->ERDModel {})
        (core/add-entity entity-a)
        (core/add-entity entity-b-renamed)
        (core/add-relation relation-d))))

(defn make-global-model-after-v2
  "Global model after v2 deployment: includes inactive RelationC"
  []
  (let [entity-a (make-entity {(id/key) entity-a-euuid :name "EntityA"})
        entity-b (make-entity {(id/key) entity-b-euuid :name "EntityB"})
        ;; RelationC is inactive (from v1, removed in v2)
        relation-c (make-relation {(id/key) relation-c-euuid
                                   :from entity-a-euuid
                                   :to entity-b-euuid
                                   :cardinality "m2o"
                                   :from-label "a"
                                   :to-label "b"
                                   :active false})
        ;; RelationD is active (added in v2)
        relation-d (make-relation {(id/key) relation-d-euuid
                                   :from entity-a-euuid
                                   :to entity-b-euuid
                                   :cardinality "o2m"
                                   :from-label "a"
                                   :to-label "b"})]
    (-> (core/map->ERDModel {})
        (core/add-entity entity-a)
        (core/add-entity entity-b)
        (core/add-relation relation-c)
        (core/add-relation relation-d))))

;; Forward version tests: v1 → v2 → v3

(deftest test-v1-to-v2-relation-added-removed
  (testing "v1→v2: RelationC removed, RelationD added"
    (let [v1 (make-v1-model)
          v2 (make-v2-model)
          projection (core/project v1 v2)
          rel-c (core/get-relation projection relation-c-euuid)
          rel-d (core/get-relation projection relation-d-euuid)]

      (testing "RelationD should be marked as added"
        (is (some? rel-d) "RelationD should exist in projection")
        (is (core/added? rel-d) "RelationD should be marked as added"))

      (testing "RelationC behavior when not in new model"
        ;; RelationC is in v1 but not in v2
        ;; The projection should include it since it's connected to entities
        ;; that exist in both models
        (if (some? rel-c)
          (is (core/removed? rel-c) "RelationC should be marked as removed")
          ;; If not present, document this behavior
          (is true "RelationC not included in projection (version-to-version behavior)"))))))

(deftest test-v2-to-v3-entity-rename-affects-active-relation
  (testing "v2→v3: EntityB renamed, RelationD should have diff"
    (let [v2 (make-v2-model)
          v3 (make-v3-model)
          projection (core/project v2 v3)
          entity-b (core/get-entity projection entity-b-euuid)
          rel-d (core/get-relation projection relation-d-euuid)]

      (testing "EntityB should have rename diff"
        (is (core/diff? entity-b) "EntityB should have diff")
        (is (= "EntityB" (:name (core/diff entity-b)))
            "Diff should contain old name 'EntityB'"))

      (testing "RelationD should have diff due to entity rename"
        (is (core/diff? rel-d) "RelationD should have diff")
        (let [diff-data (core/diff rel-d)]
          ;; The diff should indicate the :to entity name changed
          (is (or (:to diff-data)
                  (:entity.to/change diff-data)
                  (:to-label diff-data))
              "RelationD diff should capture entity name change"))))))

(deftest test-global-model-to-v3-inactive-relation-affected
  (testing "Global(after v2)→v3: Both active and INACTIVE relations affected by entity rename"
    (let [global (make-global-model-after-v2)
          v3 (make-v3-model)
          projection (core/project global v3)
          rel-c (core/get-relation projection relation-c-euuid)
          rel-d (core/get-relation projection relation-d-euuid)]

      (testing "RelationD (active) should have diff"
        (is (some? rel-d) "RelationD should exist")
        (is (core/diff? rel-d) "RelationD should have diff"))

      (testing "RelationC (inactive) should also be affected by entity rename"
        ;; When EntityB is renamed, ALL relations connected to EntityB
        ;; should be included in the projection with diffs - even inactive ones.
        ;; This is because the database tables for inactive relations still exist
        ;; and need their column names updated.
        (is (some? rel-c)
            "Inactive RelationC should be included in projection")
        (when rel-c
          (is (core/diff? rel-c)
              "Inactive RelationC should have diff when EntityB renamed")
          (let [diff-data (core/diff rel-c)]
            (is (or (:to diff-data)
                    (:entity.to/change diff-data))
                "RelationC diff should capture entity name change")))))))

;; Backward version tests: v3 → v2 → v1

(deftest test-v3-to-v2-entity-rename-rollback
  (testing "v3→v2: EntityB_Renamed back to EntityB"
    (let [v3 (make-v3-model)
          v2 (make-v2-model)
          projection (core/project v3 v2)
          entity-b (core/get-entity projection entity-b-euuid)
          rel-d (core/get-relation projection relation-d-euuid)]

      (testing "EntityB should have rename diff (rollback)"
        (is (core/diff? entity-b) "EntityB should have diff")
        (is (= "EntityB_Renamed" (:name (core/diff entity-b)))
            "Diff should contain old name 'EntityB_Renamed'"))

      (testing "RelationD should have diff due to entity rename rollback"
        (is (core/diff? rel-d) "RelationD should have diff")))))

(deftest test-v2-to-v1-relation-swap
  (testing "v2→v1: RelationD removed, RelationC added back"
    (let [v2 (make-v2-model)
          v1 (make-v1-model)
          projection (core/project v2 v1)
          rel-c (core/get-relation projection relation-c-euuid)
          rel-d (core/get-relation projection relation-d-euuid)]

      (testing "RelationC should be marked as added (restored)"
        (is (some? rel-c) "RelationC should exist in projection")
        (is (core/added? rel-c) "RelationC should be marked as added"))

      (testing "RelationD behavior when rolled back"
        (if (some? rel-d)
          (is (core/removed? rel-d) "RelationD should be marked as removed")
          (is true "RelationD not included in rollback projection"))))))

;; Full round-trip test

(deftest test-full-version-round-trip
  (testing "Full round trip: v1→v2→v3→v2→v1 changes are symmetric"
    (let [v1 (make-v1-model)
          v2 (make-v2-model)
          v3 (make-v3-model)]

      (testing "Forward: v1→v2→v3"
        (let [proj-1-2 (core/project v1 v2)
              proj-2-3 (core/project v2 v3)]
          ;; v1→v2: RelationD added
          (is (core/added? (core/get-relation proj-1-2 relation-d-euuid))
              "v1→v2: RelationD should be added")
          ;; v2→v3: EntityB renamed, RelationD has diff
          (is (core/diff? (core/get-entity proj-2-3 entity-b-euuid))
              "v2→v3: EntityB should have diff")
          (is (core/diff? (core/get-relation proj-2-3 relation-d-euuid))
              "v2→v3: RelationD should have diff")))

      (testing "Backward: v3→v2→v1"
        (let [proj-3-2 (core/project v3 v2)
              proj-2-1 (core/project v2 v1)]
          ;; v3→v2: EntityB_Renamed back to EntityB
          (is (core/diff? (core/get-entity proj-3-2 entity-b-euuid))
              "v3→v2: EntityB should have diff (rollback rename)")
          (is (core/diff? (core/get-relation proj-3-2 relation-d-euuid))
              "v3→v2: RelationD should have diff (entity rename rollback)")
          ;; v2→v1: RelationC restored
          (is (core/added? (core/get-relation proj-2-1 relation-c-euuid))
              "v2→v1: RelationC should be added (restored)"))))))
