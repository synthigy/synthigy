(ns synthigy.dataset.test-helpers
  "Helper functions for dataset testing.

  Provides:
  - Entity/relation/attribute builders with sensible defaults
  - Assertion utilities for common test patterns
  - Cleanup utilities for test data management

  Based on EYWA's test_helpers.clj pattern, adapted for Synthigy."
  (:require
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [clojure.test :refer [is]]))

;;; ============================================================================
;;; Default Values
;;; ============================================================================

(def default-entity-ui
  "Default UI/presentation values for entities."
  {:width 150.0
   :height 100.0
   :position {:x 0 :y 0}})

(def default-relation-ui
  "Default UI/presentation values for relations."
  {:path {:coordinates []}})

;;; ============================================================================
;;; Entity Builders
;;; ============================================================================

(defn make-entity
  "Creates an ERDEntity with sensible defaults.

  Accepts a map with:
  - :(id/key) (required) - Entity ID (UUID or NanoID depending on provider)
  - :name (required) - Entity name
  - :type (optional) - Entity type (\"strong\", \"weak\", \"junction\"), defaults to \"strong\"
  - :width, :height, :position (optional) - UI attributes, uses defaults if not provided
  - :configuration (optional) - Configuration map
  - :active (optional) - Active flag, defaults to true
  - :claimed-by (optional) - Set of dataset version UUIDs claiming this entity

  Example:
    (make-entity {(id/key) #uuid \"...\" :name \"Product\"})
    (make-entity {(id/key) #uuid \"...\" :name \"User\" :type \"weak\"})"
  [{:keys [name type width height position configuration active claimed-by]
    :as opts}]
  (let [entity-id (id/extract opts)]
    {:pre [(some? entity-id) (some? name)]}
    (core/map->ERDEntity
     (merge
      default-entity-ui
      {(id/key) entity-id
       :name name
       :type (or type "strong")
       :attributes []
       :configuration (or configuration {})
       :clone nil
       :original nil
       :active (if (nil? active) true active)
       :claimed-by (or claimed-by #{})}
      (when width {:width width})
      (when height {:height height})
      (when position {:position position})))))

(defn make-test-entity
  "Creates a test entity with explicit ID and test name prefix.

  Args:
    entity-id - Explicit ID for the entity (UUID or NanoID)
    name - Base name for the entity (will be prefixed with 'Test')

  Returns:
    ERDEntity with given ID

  Example:
    (make-test-entity #uuid \"aaaaaaaa-1111-0000-0000-000000000001\" \"Product\")"
  [entity-id name]
  (make-entity {(id/key) entity-id
                :name (str "Test" name)}))

;;; ============================================================================
;;; Relation Builders
;;; ============================================================================

(defn make-relation
  "Creates an ERDRelation with sensible defaults.

  Accepts a map with:
  - :(id/key) (required) - Relation ID (UUID or NanoID depending on provider)
  - :from (required) - ID of from entity (or ERDEntity record)
  - :to (required) - ID of to entity (or ERDEntity record)
  - :cardinality (required) - Cardinality (\"o2o\", \"o2m\", \"m2o\", \"m2m\")
  - :from-label (optional) - Label for from side
  - :to-label (optional) - Label for to side
  - :path (optional) - Path coordinates for UI
  - :active (optional) - Active flag, defaults to true
  - :claimed-by (optional) - Set of dataset version IDs

  Example:
    (make-relation {(id/key) #uuid \"...\"
                    :from #uuid \"...\"
                    :to #uuid \"...\"
                    :cardinality \"m2o\"})"
  [{:keys [from to from-label to-label cardinality path active claimed-by]
    :as opts}]
  (let [relation-id (id/extract opts)]
    {:pre [(some? relation-id) (some? from) (some? to) (some? cardinality)]}
    (core/map->ERDRelation
     (merge
      {(id/key) relation-id
       :from from
       :to to
       :cardinality cardinality
       :active (if (nil? active) true active)
       :claimed-by (or claimed-by #{})}
      ;; Only add from-label/to-label if provided
      (when from-label {:from-label from-label})
      (when to-label {:to-label to-label})
      ;; Add default path if not provided
      (when-not path {:path {:coordinates []}})))))

(defn make-test-relation
  "Creates a test relation with explicit ID.

  Args:
    relation-id - Explicit ID for the relation (UUID or NanoID)
    from-id - ID of from entity
    to-id - ID of to entity
    cardinality - Cardinality string (\"o2o\", \"o2m\", \"m2o\", \"m2m\")

  Example:
    (make-test-relation #uuid \"...\" from-id to-id \"m2o\")"
  [relation-id from-id to-id cardinality]
  (make-relation {(id/key) relation-id
                  :from from-id
                  :to to-id
                  :cardinality cardinality}))

;;; ============================================================================
;;; Attribute Builders
;;; ============================================================================

(defn make-attribute
  "Creates an ERDEntityAttribute with sensible defaults.

  Accepts a map with:
  - :(id/key) (required) - Attribute ID (UUID or NanoID depending on provider)
  - :name (required) - Attribute name
  - :type (required) - Attribute type (\"string\", \"int\", \"float\", etc.)
  - :seq (optional) - Sequence number, will be auto-assigned if not provided
  - :constraint (optional) - Constraint (\"mandatory\", \"optional\", \"unique\"), defaults to \"optional\"
  - :configuration (optional) - Configuration map
  - :active (optional) - Active flag, defaults to true

  Example:
    (make-attribute {(id/key) #uuid \"...\" :name \"Email\" :type \"string\" :constraint \"mandatory\"})"
  [{:keys [name type seq constraint configuration active]
    :as opts}]
  (let [attr-id (id/extract opts)]
    {:pre [(some? attr-id) (some? name) (some? type)]}
    (core/map->ERDEntityAttribute
     {(id/key) attr-id
      :name name
      :type type
      :seq (or seq 0)
      :constraint (or constraint "optional")
      :configuration (or configuration {})
      :active (if (nil? active) true active)})))

(defn add-test-attribute
  "Adds an attribute to an entity using core/add-attribute.

  Args:
    entity - ERDEntity to add attribute to
    opts - Map with ID key (via id/key), :name, :type, and optionally :constraint, :configuration, :active

  Returns:
    Updated ERDEntity with attribute added

  Example:
    (-> (make-entity {...})
        (add-test-attribute {(id/key) #uuid \"...\" :name \"Name\" :type \"string\"})
        (add-test-attribute {(id/key) #uuid \"...\" :name \"Price\" :type \"float\"}))"
  [entity opts]
  (let [attr (make-attribute opts)
        ;; Auto-assign seq based on current attribute count
        attr-with-seq (assoc attr :seq (count (:attributes entity)))]
    (core/add-attribute entity attr-with-seq)))

(defn entity-with-attributes
  "Creates an entity with attributes already added.

  Args:
    entity-opts - Map for make-entity
    attribute-opts-seq - Sequence of maps for add-test-attribute

  Returns:
    ERDEntity with attributes added

  Example:
    (entity-with-attributes
      {(id/key) #uuid \"...\" :name \"Product\"}
      [{(id/key) #uuid \"...\" :name \"Name\" :type \"string\"}
       {(id/key) #uuid \"...\" :name \"Price\" :type \"float\"}])"
  [entity-opts attribute-opts-seq]
  (reduce
   (fn [entity attr-opts]
     (add-test-attribute entity attr-opts))
   (make-entity entity-opts)
   attribute-opts-seq))

;; Shorthand attribute builders
(defn string-attr
  "Creates a string attribute with the given ID and name."
  [attr-id name]
  {(id/key) attr-id :name name :type "string"})

(defn int-attr
  "Creates an integer attribute with the given ID and name."
  [attr-id name]
  {(id/key) attr-id :name name :type "int"})

(defn float-attr
  "Creates a float attribute with the given ID and name."
  [attr-id name]
  {(id/key) attr-id :name name :type "float"})

(defn boolean-attr
  "Creates a boolean attribute with the given ID and name."
  [attr-id name]
  {(id/key) attr-id :name name :type "boolean"})

(defn mandatory-attr
  "Creates a mandatory attribute of the given type."
  [attr-id name type]
  {(id/key) attr-id :name name :type type :constraint "mandatory"})

(defn unique-attr
  "Creates a unique attribute of the given type."
  [attr-id name type]
  {(id/key) attr-id :name name :type type :constraint "unique"})

;;; ============================================================================
;;; Model Builders
;;; ============================================================================

(defn empty-model
  "Creates an empty ERDModel."
  []
  (core/map->ERDModel {}))

(defn model-with-entities
  "Creates a model with the given entities.

  Args:
    entities - Sequence of ERDEntity records

  Returns:
    ERDModel with entities added"
  [entities]
  (reduce core/add-entity (empty-model) entities))

(defn model-with-relations
  "Creates a model with entities and relations.

  Args:
    entities - Sequence of ERDEntity records
    relations - Sequence of ERDRelation records

  Returns:
    ERDModel with entities and relations added"
  [entities relations]
  (reduce core/add-relation
          (model-with-entities entities)
          relations))

;;; ============================================================================
;;; Assertion Helpers
;;; ============================================================================

(defn assert-entity-exists
  "Asserts that an entity with the given ID exists in the model.

  Args:
    model - ERDModel to check
    entity-id - Entity ID to look for
    message (optional) - Custom assertion message"
  ([model entity-id]
   (assert-entity-exists model entity-id (str "Entity " entity-id " should exist in model")))
  ([model entity-id message]
   (is (some? (core/get-entity model entity-id)) message)))

(defn assert-entity-not-exists
  "Asserts that an entity with the given ID does NOT exist in the model."
  ([model entity-id]
   (assert-entity-not-exists model entity-id (str "Entity " entity-id " should NOT exist in model")))
  ([model entity-id message]
   (is (nil? (core/get-entity model entity-id)) message)))

(defn assert-relation-exists
  "Asserts that a relation with the given ID exists in the model."
  ([model relation-id]
   (assert-relation-exists model relation-id (str "Relation " relation-id " should exist in model")))
  ([model relation-id message]
   (is (some? (core/get-relation model relation-id)) message)))

(defn assert-relation-not-exists
  "Asserts that a relation with the given ID does NOT exist in the model."
  ([model relation-id]
   (assert-relation-not-exists model relation-id (str "Relation " relation-id " should NOT exist in model")))
  ([model relation-id message]
   (is (nil? (core/get-relation model relation-id)) message)))

(defn assert-active
  "Asserts that the entity/relation is active."
  ([entity-or-relation]
   (assert-active entity-or-relation "Should be active"))
  ([entity-or-relation message]
   (is (:active entity-or-relation) message)))

(defn assert-inactive
  "Asserts that the entity/relation is inactive."
  ([entity-or-relation]
   (assert-inactive entity-or-relation "Should be inactive"))
  ([entity-or-relation message]
   (is (not (:active entity-or-relation)) message)))

(defn assert-claimed-by
  "Asserts that the entity/relation is claimed by the given dataset version UUIDs.

  Args:
    entity-or-relation - ERDEntity or ERDRelation
    expected-claims - Set of UUIDs or single UUID
    message (optional) - Custom assertion message"
  ([entity-or-relation expected-claims]
   (assert-claimed-by entity-or-relation expected-claims "Claims should match"))
  ([entity-or-relation expected-claims message]
   (let [expected-set (if (set? expected-claims)
                        expected-claims
                        #{expected-claims})
         actual-claims (:claimed-by entity-or-relation)]
     (is (= expected-set actual-claims) message))))

(defn assert-claim-count
  "Asserts that the entity/relation has exactly N claims."
  ([entity-or-relation expected-count]
   (assert-claim-count entity-or-relation expected-count (str "Should have " expected-count " claims")))
  ([entity-or-relation expected-count message]
   (is (= expected-count (count (:claimed-by entity-or-relation))) message)))

(defn assert-has-claim
  "Asserts that the entity/relation is claimed by the given UUID."
  ([entity-or-relation claim-uuid]
   (assert-has-claim entity-or-relation claim-uuid (str "Should be claimed by " claim-uuid)))
  ([entity-or-relation claim-uuid message]
   (is (contains? (:claimed-by entity-or-relation) claim-uuid) message)))

(defn assert-attribute-exists
  "Asserts that an entity has an attribute with the given name."
  ([entity attr-name]
   (assert-attribute-exists entity attr-name (str "Attribute " attr-name " should exist")))
  ([entity attr-name message]
   (is (some #(= attr-name (:name %)) (:attributes entity)) message)))

(defn assert-attribute-count
  "Asserts that an entity has exactly N attributes."
  ([entity expected-count]
   (assert-attribute-count entity expected-count (str "Should have " expected-count " attributes")))
  ([entity expected-count message]
   (is (= expected-count (count (:attributes entity))) message)))

;;; ============================================================================
;;; Predicate Helpers
;;; ============================================================================

(defn claimed-by?
  "Returns true if entity/relation is claimed by the given UUID(s)."
  [entity-or-relation claim-uuids]
  (let [claims (:claimed-by entity-or-relation)
        expected (if (set? claim-uuids) claim-uuids #{claim-uuids})]
    (= expected claims)))

(defn active?
  "Returns true if entity/relation is active."
  [entity-or-relation]
  (:active entity-or-relation))

(defn inactive?
  "Returns true if entity/relation is inactive."
  [entity-or-relation]
  (not (:active entity-or-relation)))

(defn has-claim?
  "Returns true if entity/relation is claimed by the given UUID."
  [entity-or-relation claim-uuid]
  (contains? (:claimed-by entity-or-relation) claim-uuid))

(defn has-attribute?
  "Returns true if entity has an attribute with the given name."
  [entity attr-name]
  (some #(= attr-name (:name %)) (:attributes entity)))

;;; ============================================================================
;;; Projection Assertion Helpers
;;; ============================================================================

(defn get-attribute-by-id
  "Gets attribute from entity by ID."
  [entity attr-id]
  (some #(when (= attr-id (id/extract %)) %) (:attributes entity)))

(defn projection-diff-path
  "Returns value at path in element's diff, or nil."
  [element path]
  (get-in (core/diff element) path))

(defn assert-projection-added
  "Asserts that an element is marked as added in projection."
  ([element]
   (assert-projection-added element "Should be marked as added"))
  ([element msg]
   (is (core/added? element) msg)))

(defn assert-projection-removed
  "Asserts that an element is marked as removed in projection."
  ([element]
   (assert-projection-removed element "Should be marked as removed"))
  ([element msg]
   (is (core/removed? element) msg)))

(defn assert-projection-diff?
  "Asserts that an element has a diff in projection."
  ([element]
   (assert-projection-diff? element "Should have diff"))
  ([element msg]
   (is (core/diff? element) msg)))

(defn assert-projection-no-diff
  "Asserts that an element has no diff in projection."
  ([element]
   (assert-projection-no-diff element "Should not have diff"))
  ([element msg]
   (is (not (core/diff? element)) msg)))

(defn assert-projection-diff
  "Asserts that an element's diff equals the expected diff."
  ([element expected-diff]
   (assert-projection-diff element expected-diff "Diff should match expected"))
  ([element expected-diff msg]
   (is (= expected-diff (core/diff element)) msg)))

(defn assert-projection-diff-contains
  "Asserts that an element's diff contains a specific key-value pair."
  ([element key expected-value]
   (assert-projection-diff-contains element key expected-value (str "Diff should contain " key " = " expected-value)))
  ([element key expected-value msg]
   (is (= expected-value (get (core/diff element) key)) msg)))

(defn assert-projection-unchanged
  "Asserts that an element is unchanged (not added, removed, or diffed)."
  ([element]
   (assert-projection-unchanged element "Should be unchanged"))
  ([element msg]
   (is (and (not (core/added? element))
            (not (core/removed? element))
            (not (core/diff? element)))
       msg)))

;;; ============================================================================
;;; Enum Attribute Builders
;;; ============================================================================

(defn enum-attr
  "Creates an enum attribute with the given ID, name, and values.
   Values is a vector of maps with ID key and :name keys."
  [attr-id name values]
  {(id/key) attr-id
   :name name
   :type "enum"
   :configuration {:values values}})

(defn enum-value
  "Creates an enum value map with ID and name."
  [value-id name]
  {(id/key) value-id :name name})
