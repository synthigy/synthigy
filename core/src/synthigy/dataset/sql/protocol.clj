(ns synthigy.dataset.sql.protocol
  "Core protocols for database abstraction.

  All protocols are extended over database record types (Postgres, SQLite, MySQL, etc.).
  The database record is passed as the first argument to all protocol methods.

  ## Protocol Overview

  1. TypeCodec - Encode/decode values to database-specific types
  2. SQLDialect - Generate database-specific SQL syntax
  3. SchemaIntrospector - Query database metadata

  ## Usage

  All protocols use the database record (typically `*db*` dynamic var):

    (encode *db* \"json\" {:foo \"bar\"})
    (like-operator *db* false)
    (get-tables *db*)

  Query execution is handled by synthigy.db.sql (database-agnostic layer)
  which delegates to database-specific JDBCBackend protocol implementations.
  This allows execution to work with raw JDBC connections inside transactions.")

;;; ============================================================================
;;; Protocol 1: TypeCodec
;;; ============================================================================

(defprotocol TypeCodec
  "Encode/decode values to/from database-specific types.

  Uses type strings matching the ERD model type system:
    Scalars: 'string', 'int', 'float', 'boolean', 'timestamp', 'uuid'
    Complex: 'json', 'transit', 'encrypted', 'hashed', 'avatar'
    Enums:   Custom type names (e.g., 'user_status', 'order_state')

  Each database implements encoding/decoding according to its capabilities:
    PostgreSQL: Native types for most, PGobject for json/enums
    SQLite:     TEXT/INTEGER/REAL, special handling for boolean/json/enums
    MySQL:      Native types, special JSON handling

  Example:
    ;; PostgreSQL
    (encode postgres-db \"json\" {:x 1})
    => #<PGobject type=jsonb value=\"{\\\"x\\\":1}\">

    ;; SQLite
    (encode sqlite-db \"json\" {:x 1})
    => \"{\\\"x\\\":1}\"  ; Plain string

    (encode sqlite-db \"boolean\" true)
    => 1  ; INTEGER"

  (encode [db type value]
    "Encode Clojure value to database-specific representation.

    Args:
      db - Database record (contains connection info and capabilities)
      type - Type string from ERD model ('json', 'boolean', 'user_status', etc.)
      value - Clojure value to encode (map, keyword, boolean, etc.)

    Returns:
      Database-specific representation (may be PGobject, string, number, etc.)

    Examples:
      (encode *db* \"json\" {:foo \"bar\"})
      (encode *db* \"boolean\" true)
      (encode *db* \"user_status\" :active)
      (encode *db* \"transit\" complex-data)")

  (decode [db type value]
    "Decode database value to Clojure representation.

    Args:
      db - Database record
      type - Type string from ERD model
      value - Database value (may be PGobject, string, number, etc.)

    Returns:
      Clojure value (keyword, map, vector, boolean, etc.)

    Examples:
      (decode *db* \"json\" pgobject-or-string)
      (decode *db* \"boolean\" 1)  ; SQLite: INTEGER → boolean
      (decode *db* \"user_status\" \"active\")  ; String → keyword"))

;;; ============================================================================
;;; Protocol 2: SQLDialect
;;; ============================================================================

(defprotocol SQLDialect
  "Generate database-specific SQL syntax.

  Handles variations in SQL dialects:
  - Case-insensitive LIKE (ILIKE vs LIKE vs GLOB)
  - LIMIT/OFFSET parameter order
  - Enum type-cast placeholders
  - UPSERT excluded-row reference casing
  - Bound-parameter ceilings"

  (like-operator [db case-sensitive?]
    "Get appropriate LIKE operator for case sensitivity.

    PostgreSQL: LIKE (case-sensitive) or ILIKE (case-insensitive)
    SQLite:     LIKE (case-insensitive default) or GLOB (case-sensitive)
    MySQL:      LIKE (case sensitivity depends on collation)

    Args:
      db - Database record
      case-sensitive? - Boolean

    Returns:
      String - SQL LIKE operator

    Examples:
      (like-operator postgres-db false)
      => \"ILIKE\"

      (like-operator sqlite-db true)
      => \"GLOB\"")

  (limit-offset-clause [db limit offset]
    "Generate LIMIT/OFFSET clause for pagination.

    PostgreSQL: LIMIT ? OFFSET ?
    SQLite:     LIMIT ? OFFSET ?
    MySQL:      LIMIT ?, ? (note: different parameter order!)

    Args:
      db - Database record
      limit - Row limit integer
      offset - Row offset integer

    Returns:
      String - SQL LIMIT/OFFSET clause

    Examples:
      (limit-offset-clause postgres-db 10 20)
      => \"LIMIT 10 OFFSET 20\"

      (limit-offset-clause mysql-db 10 20)
      => \"LIMIT 20, 10\"")

  (placeholder-for-type [db field-type]
    "Generate placeholder for parameterized value with optional type cast.

    PostgreSQL: ?::typename for enums, ? otherwise
    SQLite:     Always ? (no type casting needed)

    Used when generating INSERT/UPDATE placeholders.

    Args:
      db - Database record
      field-type - Type string (e.g., \"user_status\", \"string\", \"int\")

    Returns:
      String - Placeholder expression

    Examples:
      (placeholder-for-type postgres-db \"user_status\")
      => \"?::user_status\"

      (placeholder-for-type sqlite-db \"user_status\")
      => \"?\"")

  (excluded-ref [db column]
    "Generate reference to excluded row in UPSERT.

    PostgreSQL: EXCLUDED.column
    SQLite:     excluded.column

    Case matters for some databases.

    Args:
      db - Database record
      column - Column name

    Returns:
      String - Reference expression

    Examples:
      (excluded-ref postgres-db \"name\")
      => \"EXCLUDED.name\"

      (excluded-ref sqlite-db \"name\")
      => \"excluded.name\"")

  (max-bind-params [db]
    "Hard cap on bound parameters (placeholders) in a single prepared
    statement. Used to size multi-row INSERT / IN-list chunks so bulk
    writes never overrun the limit.

    PostgreSQL: 65535 — wire-protocol Bind message counts params in Int16
    SQLite:     32766 — SQLITE_MAX_VARIABLE_NUMBER (SQLite >= 3.32)
    MySQL:      65535 — placeholder limit

    Args:
      db - Database record

    Returns:
      Long - maximum bound parameters per statement

    Examples:
      (max-bind-params postgres-db) => 65535
      (max-bind-params sqlite-db)   => 32766"))

;;; ============================================================================
;;; Protocol 3: SchemaManager
;;; ============================================================================

(defprotocol SchemaManager
  "Query and manage database schema.

  Used for:
  - Introspection: migrations, schema diffs, healthchecks, ERD model validation
  - Maintenance: cleanup operations for testing and development"

  (get-tables [db]
    "Get list of all tables in database.

    Excludes system tables (information_schema, pg_catalog, etc.).

    Returns:
      Vector of table name strings

    Example:
      (get-tables *db*)
      => [\"users\" \"posts\" \"comments\"]")

  (get-columns [db table]
    "Get columns for a table.

    Args:
      db - Database record
      table - Table name string

    Returns:
      Vector of column info maps with keys:
        :name - Column name string
        :type - SQL type string
        :nullable - Boolean
        :default - Default value or nil

    Example:
      (get-columns *db* \"users\")
      => [{:name \"id\" :type \"integer\" :nullable false :default nil}
          {:name \"name\" :type \"text\" :nullable false :default nil}
          {:name \"active\" :type \"boolean\" :nullable true :default \"true\"}]")

  (get-enums [db]
    "Get list of enum types in database.

    PostgreSQL: Returns actual enum types from system catalog
    SQLite:     Parses CHECK constraints (if possible) or returns nil
    MySQL:      Returns enum columns

    Returns:
      Vector of enum info maps with keys:
        :name - Enum type name string
        :values - Vector of allowed value strings
      Or nil if database doesn't support enums

    Example:
      (get-enums postgres-db)
      => [{:name \"user_status\" :values [\"active\" \"inactive\" \"suspended\"]}
          {:name \"order_state\" :values [\"pending\" \"processing\" \"shipped\" \"delivered\"]}]

      (get-enums sqlite-db)
      => nil")

  (table-exists? [db table]
    "Check if table exists in database.

    Args:
      db - Database record
      table - Table name string

    Returns:
      Boolean

    Examples:
      (table-exists? *db* \"users\")
      => true

      (table-exists? *db* \"nonexistent\")
      => false")

  (column-exists? [db table column]
    "Check if column exists in table.

    Args:
      db - Database record
      table - Table name string
      column - Column name string

    Returns:
      Boolean

    Examples:
      (column-exists? *db* \"users\" \"name\")
      => true

      (column-exists? *db* \"users\" \"nonexistent\")
      => false")

  ;; === Maintenance Operations ===

  (list-tables-like [db pattern]
    "List tables matching SQL LIKE pattern.

    Useful for finding test tables, temporary tables, or tables with specific prefixes.

    Args:
      db - Database record
      pattern - SQL LIKE pattern (e.g., 'test%', '%_tmp', 'data_%')

    Returns:
      Vector of maps with :tablename key

    Examples:
      (list-tables-like *db* \"test%\")
      => [{:tablename \"testitem\"} {:tablename \"testcategory\"}]

      (list-tables-like *db* \"%_backup\")
      => [{:tablename \"users_backup\"} {:tablename \"posts_backup\"}]")

  (drop-table! [db table]
    "Drop a single table with CASCADE.

    Safe operation - uses IF EXISTS, won't fail if table doesn't exist.
    Uses CASCADE to automatically drop dependent objects (foreign keys, views, etc.).

    Args:
      db - Database record
      table - Table name string

    Returns:
      Boolean - true if table was dropped, false if it didn't exist

    Examples:
      (drop-table! *db* \"testitem\")
      => true

      (drop-table! *db* \"nonexistent\")
      => false")

  (drop-tables-like! [db pattern]
    "Drop all tables matching SQL LIKE pattern.

    Useful for bulk cleanup of test tables or temporary tables.
    Each table is dropped with CASCADE.

    Args:
      db - Database record
      pattern - SQL LIKE pattern

    Returns:
      Integer - count of tables actually dropped

    Examples:
      (drop-tables-like! *db* \"test%\")
      => 5

      (drop-tables-like! *db* \"temp_%\")
      => 12")

  (truncate-table! [db table]
    "Truncate a table (remove all rows, keep structure).

    Faster than DELETE for large tables. Uses CASCADE where supported.

    PostgreSQL: TRUNCATE TABLE ... CASCADE
    SQLite: DELETE FROM ... (no native TRUNCATE)

    Args:
      db - Database record
      table - Table name string

    Examples:
      (truncate-table! *db* \"__deploy_history\")

      (truncate-table! *db* \"audit_log\")")

  (list-types-like [db pattern]
    "List custom types matching SQL LIKE pattern.

    PostgreSQL: Returns enum types and custom types
    SQLite: Returns empty vector (no custom types)
    MySQL: Returns enum columns

    Args:
      db - Database record
      pattern - SQL LIKE pattern

    Returns:
      Vector of maps with :typname key, or empty vector

    Examples:
      (list-types-like postgres-db \"test%\")
      => [{:typname \"testitem_status\"} {:typname \"testcomment_priority\"}]

      (list-types-like sqlite-db \"test%\")
      => []")

  (drop-type! [db type-name]
    "Drop a single custom type with CASCADE.

    Safe operation - uses IF EXISTS, won't fail if type doesn't exist.

    PostgreSQL: Drops enum type or custom type
    SQLite: No-op (no custom types)

    Args:
      db - Database record
      type-name - Type name string

    Returns:
      Boolean - true if type was dropped, false otherwise

    Examples:
      (drop-type! postgres-db \"user_status\")
      => true

      (drop-type! sqlite-db \"anything\")
      => false")

  (drop-types-like! [db pattern]
    "Drop all custom types matching SQL LIKE pattern.

    Useful for cleanup of test enum types or temporary types.

    PostgreSQL: Drops all matching enum/custom types with CASCADE
    SQLite: No-op (returns 0)

    Args:
      db - Database record
      pattern - SQL LIKE pattern

    Returns:
      Integer - count of types actually dropped

    Examples:
      (drop-types-like! postgres-db \"test%\")
      => 3

      (drop-types-like! sqlite-db \"test%\")
      => 0"))
