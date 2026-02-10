(ns synthigy.dataset.lacinia.enhance
  "GraphQL schema enhancement for audit and metadata fields.

  Provides protocol for injecting audit fields into Lacinia GraphQL schemas
  without polluting the domain model."
  (:import
   [synthigy.db Postgres SQLite]))

;; ============================================================================
;; Schema Enhancement Protocol
;; ============================================================================

(defprotocol SchemaEnhancement
  "GraphQL schema enhancement for audit and metadata fields.

  Allows pluggable field injection into Lacinia schemas."

  (get-audit-fields [db entity]
    "Get audit field definitions for GraphQL object types.

    Args:
      db     - Database instance
      entity - Entity record from ERD model

    Returns:
      Map of field-name -> field-definition
      {:modified_by {:type 'User :resolve ...}
       :modified_on {:type :Timestamp :resolve ...}}")

  (get-search-operators [db entity]
    "Get audit field search operators for filters.

    Returns synthetic attributes for _where clause building.

    Returns:
      Vector of attribute-like maps
      [{:name \"modified_by\" :type \"user\" :active true}
       {:name \"modified_on\" :type \"timestamp\" :active true}]")

  (get-order-operators [db entity]
    "Get audit field order-by operators for sorting.

    Returns synthetic relations for _order_by clause building.

    Returns:
      Vector of relation-like maps for ordering
      [{:to user-entity :to-label \"modified_by\" :cardinality \"o2o\"}]")

  (get-query-args [db entity]
    "Get audit field query arguments.

    Returns synthetic attributes for search query arguments.

    Returns:
      Vector of attribute-like maps
      [{:name \"modified_on\" :type \"timestamp\" :active true}]"))

;; Default no-op implementations
;; Allows dataset tests to run without IAM loaded
(extend-protocol SchemaEnhancement
  Postgres
  (get-audit-fields [_ _] {})
  (get-search-operators [_ _] [])
  (get-order-operators [_ _] [])
  (get-query-args [_ _] [])

  SQLite
  (get-audit-fields [_ _] {})
  (get-search-operators [_ _] [])
  (get-order-operators [_ _] [])
  (get-query-args [_ _] []))
