(ns synthigy.dataset.lacinia.audit
  "Lacinia GraphQL enhancements for audit fields.

  This namespace detects if audit implementation is available and extends
  Lacinia's SchemaEnhancement protocol accordingly.

  Architecture:
  - Lacinia depends on audit (optional)
  - Audit does NOT depend on Lacinia
  - This enables the dependency inversion principle

  The GraphQL schema is automatically enhanced with audit fields when:
  1. IAM audit implementation is loaded (e.g., synthigy.iam.audit.postgres)
  2. This namespace is required by Lacinia

  Usage:
    (require 'synthigy.dataset.lacinia.audit)  ; Auto-extends if audit available"
  (:require
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.lacinia.enhance :as enhance]))

;; ============================================================================
;; GraphQL Schema Enhancement Implementations
;; ============================================================================

(defn- get-audit-fields-impl
  "Returns GraphQL field definitions for audit columns.

  Returns:
    {:modified_by {:type 'User :description \"...\" :resolve fn}
     :modified_on {:type '(non-null Timestamp) :description \"...\" :resolve fn}}"
  [db _]
  {:modified_by
   {:type 'User  ; Symbol resolved by Lacinia (user entity is always named "User")
    :description "User who last modified this record"}

   :modified_on
   {:type '(non-null Timestamp)
    :description "Timestamp when this record was last modified"}})

(defn- get-search-operators-impl
  "Returns synthetic attributes for _where clause building.

  These attributes are used by the GraphQL query builder to construct
  WHERE clauses for filtering by audit fields.

  Returns:
    [{:name \"modified_by\" :type \"user\" :active true}
     {:name \"modified_on\" :type \"timestamp\" :active true}]"
  [db entity]
  [{:name "modified_by"
    :type "user"
    :active true}
   {:name "modified_on"
    :type "timestamp"
    :active true}])

(defn- get-order-operators-impl
  "Returns synthetic relations for _order_by clause building.

  These relations are used by the GraphQL query builder to construct
  ORDER BY clauses for sorting by audit fields.

  Returns:
    [{:to user-uuid :to-label \"modified_by\" :cardinality \"o2o\"}]"
  [db entity]
  [{:to (dataset/deployed-entity (id/entity :iam/user))
    :to-label "modified_by"
    :cardinality "o2o"}])

(defn- get-query-args-impl
  "Returns synthetic attributes for search query arguments.

  These attributes are used by the GraphQL query builder to allow
  filtering and searching by audit fields in search queries.

  Returns:
    [{:name \"modified_on\" :type \"timestamp\" :active true}
     {:name \"modified_by\" :type \"user\" :active true}]"
  [db entity]
  [{:name "modified_on"
    :type "timestamp"
    :active true}
   {:name "modified_by"
    :type "user"
    :active true}])

;; ============================================================================
;; Protocol Extension (Conditional)
;; ============================================================================

(extend-protocol enhance/SchemaEnhancement
  synthigy.db.Postgres
  (get-audit-fields [db entity]
    (get-audit-fields-impl db entity))
  (get-search-operators [db entity]
    (get-search-operators-impl db entity))
  (get-order-operators [db entity]
    (get-order-operators-impl db entity))
  (get-query-args [db entity]
    (get-query-args-impl db entity)))
