(ns synthigy.db.sql
  "Database-agnostic JDBC execution layer.

  All databases (Postgres, SQLite, MySQL) use these functions.
  Database-specific behavior is implemented via the JDBCBackend protocol,
  which is extended by database record types (Postgres, SQLite, MySQL).

  ## Usage

  All execute functions use the *db* dynamic var from synthigy.db:

    (require '[synthigy.db.sql :refer [execute! execute-one!]])
    (execute! [\"SELECT * FROM users WHERE id = ?\" user-id] :edn)
    (execute-one! [\"SELECT COUNT(*) FROM users\"] :raw)

  ## Protocol Extension

  Each database implementation extends JDBCBackend on its record type:

    (extend-type Postgres
      JDBCBackend
      (jdbc-options [db return-type] ...))

  See synthigy.db.postgres for reference implementation."
  (:require
    [next.jdbc :as jdbc]
    [synthigy.db :refer [*db*]]
    [synthigy.json :refer [data->json]]))

;;; ============================================================================
;;; JDBCBackend Protocol
;;; ============================================================================

(defprotocol JDBCBackend
  "Protocol for database-specific JDBC result set handling.

  Each database implementation (Postgres, SQLite, MySQL) extends this protocol
  to provide database-specific configuration for query execution:
  - Result set builders (handle type decoding from ResultSet)
  - Column name transformations (kebab-case, etc.)
  - Table/column quoting functions

  This is separate from TypeCodec protocol which handles encoding values
  for INSERT/UPDATE operations."

  (jdbc-options [db return-type]
    "Returns next.jdbc options map for query execution.

    Args:
      db - Database record (Postgres, SQLite, MySQL)
      return-type - One of :graphql, :edn, :raw, or nil (defaults to :raw)

    Returns:
      Map with next.jdbc options:
        :builder-fn - Result set builder function (handles type decoding)
        :table-fn - Table name quoting function
        :label-fn - Column name transformation function
        :qualifier-fn - Qualifier transformation function
        :column-fn - Column quoting function

    Example:
      (jdbc-options postgres-db :edn)
      => {:builder-fn (fn [rs rsmeta i] ...)
          :table-fn postgres-quote
          :label-fn kebab-case
          ...}"))

;;; ============================================================================
;;; Shared Utilities
;;; ============================================================================

(defn- connectable?
  "Returns true if x is a JDBC connectable (Connection, DataSource, or transacted connection map)."
  [x]
  (or (instance? javax.sql.DataSource x)
      (instance? java.sql.Connection x)
      (and (map? x) (contains? x :connection))))

(defn normalize-statement
  "Normalizes JDBC statement by converting map parameters to JSON strings.

  This is database-agnostic - all databases use JSON strings for map values
  in parameterized queries.

  Args:
    statement - JDBC statement vector [query & params]

  Returns:
    Normalized statement with maps converted to JSON

  Example:
    (normalize-statement [\"INSERT INTO users (data) VALUES (?)\" {:name \"Alice\"}])
    => [\"INSERT INTO users (data) VALUES (?)\" \"{\\\"name\\\":\\\"Alice\\\"}\"]"
  [[q & params :as statement]]
  (if (not-empty params)
    (concat [q] (map #(if (map? %) (data->json %) %) params))
    statement))

(defn get-label-fn
  "Returns the label transformation function for the given return type.

  The label-fn transforms column names and table aliases according to
  the return type's naming convention (graphql, edn, or raw).

  Args:
    return-type - One of :graphql, :edn, :raw

  Returns:
    Function that transforms string labels to keywords or strings

  Example:
    ((get-label-fn :edn) \"user_name\")
    => :user-name

    ((get-label-fn :raw) \"user_name\")
    => \"user_name\""
  [return-type]
  (get-in (jdbc-options *db* return-type) [:label-fn] identity))

;;; ============================================================================
;;; Prepared Statements
;;; ============================================================================

(defn prepare
  "Creates a prepared statement using current *db* connection.

  Prepared statements are useful for batch operations where the same SQL
  is executed multiple times with different parameters.

  Args:
    connectable - JDBC connection or datasource
    statement - SQL string or vector [sql]

  Returns:
    PreparedStatement object

  Example:
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (let [stmt (prepare conn [\"INSERT INTO users (name) VALUES (?)\"])]
        (execute-batch! stmt [[\"Alice\"] [\"Bob\"]] :edn)))"
  [connectable statement]
  (jdbc/prepare connectable statement (jdbc-options *db* :raw)))

;;; ============================================================================
;;; Execute Functions
;;; ============================================================================

(defn execute!
  "Executes JDBC statement using *db* connection or explicit connectable.

  Uses the JDBCBackend protocol to determine database-specific configuration
  for result set handling.

  Two forms:
  1. Implicit connection: (execute! statement) or (execute! statement return-type)
  2. Explicit connection: (execute! connectable statement) or (execute! connectable statement return-type)

  Args:
    connectable - Optional JDBC connection, transaction, or datasource
    statement - JDBC statement vector [query & params]
    return-type - Optional: :graphql, :edn, :raw, or nil (defaults to :raw)

  Returns:
    Vector of result maps (format depends on return-type)

  Examples:
    ;; Implicit connection (uses *db*)
    (execute! [\"SELECT * FROM users\"])
    => [{\"id\" 1 \"name\" \"Alice\"} ...]

    (execute! [\"SELECT * FROM users WHERE id = ?\" 42] :edn)
    => [{:id 1 :name \"Alice\"}]

    ;; Explicit connection (within transaction)
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (jdbc/with-transaction [tx conn]
        (execute! tx [\"INSERT INTO users (name) VALUES (?)\", \"Alice\"])
        (execute! tx [\"UPDATE users SET active = true WHERE name = ?\", \"Alice\"])))"
  ([statement]
   (execute! (:datasource *db*) statement nil))
  ([connectable-or-statement statement-or-return-type]
   (if (connectable? connectable-or-statement)
     ;; Explicit connection: (execute! conn statement)
     (execute! connectable-or-statement statement-or-return-type nil)
     ;; Implicit connection: (execute! statement return-type)
     (execute! (:datasource *db*) connectable-or-statement statement-or-return-type)))
  ([connectable statement return-type]
   (jdbc/execute!
     connectable
     (normalize-statement statement)
     (jdbc-options *db* (or return-type :raw)))))

(defn execute-one!
  "Executes JDBC statement returning single result using *db* or explicit connectable.

  Uses the JDBCBackend protocol to determine database-specific configuration
  for result set handling.

  Two forms:
  1. Implicit connection: (execute-one! statement) or (execute-one! statement return-type)
  2. Explicit connection: (execute-one! connectable statement) or (execute-one! connectable statement return-type)

  Args:
    connectable - Optional JDBC connection, transaction, or datasource
    statement - JDBC statement vector [query & params]
    return-type - Optional: :graphql, :edn, :raw, or nil (defaults to :raw)

  Returns:
    Single result map, or nil if no results (format depends on return-type)

  Examples:
    ;; Implicit connection (uses *db*)
    (execute-one! [\"SELECT * FROM users WHERE id = ?\" 42])
    => {\"id\" 42 \"name\" \"Alice\"}

    (execute-one! [\"SELECT COUNT(*) as count FROM users\"] :edn)
    => {:count 100}

    ;; Explicit connection (within transaction)
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (jdbc/with-transaction [tx conn]
        (execute-one! tx [\"SELECT * FROM users WHERE id = ?\" 42] :edn)))"
  ([statement]
   (execute-one! (:datasource *db*) statement nil))
  ([connectable-or-statement statement-or-return-type]
   (if (connectable? connectable-or-statement)
     ;; Explicit connection: (execute-one! conn statement)
     (execute-one! connectable-or-statement statement-or-return-type nil)
     ;; Implicit connection: (execute-one! statement return-type)
     (execute-one! (:datasource *db*) connectable-or-statement statement-or-return-type)))
  ([connectable statement return-type]
   (jdbc/execute-one!
     connectable
     (normalize-statement statement)
     (jdbc-options *db* (or return-type :raw)))))

(defn execute-batch!
  "Executes batch JDBC statement using *db* or explicit connectable.

  Uses the JDBCBackend protocol to determine database-specific configuration.

  Multiple forms:
  1. SQL vector (implicit): (execute-batch! [sql & param-sets] return-type)
  2. SQL vector (explicit): (execute-batch! connectable [sql & param-sets] return-type)
  3. Prepared statement: (execute-batch! prepared-stmt param-sets return-type)

  Args:
    connectable - Optional JDBC connection, transaction, or datasource
    statement - JDBC statement vector [query & param-sets] OR PreparedStatement
    param-sets-or-return-type - If statement is PreparedStatement, this is the
                                 parameter sets collection. Otherwise return-type.
    return-type - Optional: :graphql, :edn, :raw, or nil (defaults to :raw)

  Returns:
    Vector of update counts

  Examples:
    ;; SQL vector form (implicit connection)
    (execute-batch!
      [\"INSERT INTO users (name, email) VALUES (?, ?)\"
       [\"Alice\" \"alice@example.com\"]
       [\"Bob\" \"bob@example.com\"]]
      :edn)
    => [1 1]

    ;; SQL vector form (explicit connection)
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (jdbc/with-transaction [tx conn]
        (execute-batch! tx
          [\"INSERT INTO users (name) VALUES (?)\"
           [\"Alice\"]
           [\"Bob\"]]
          :edn)))

    ;; Prepared statement form
    (let [stmt (prepare conn [\"INSERT INTO users (name) VALUES (?)\"])]
      (execute-batch! stmt [[\"Alice\"] [\"Bob\"]] :edn))
    => [1 1]"
  ([statement]
   (execute-batch! statement nil))
  ([statement-or-connectable param-sets-or-statement-or-return-type]
   (cond
     ;; PreparedStatement form: (execute-batch! prepared-stmt param-sets)
     (instance? java.sql.PreparedStatement statement-or-connectable)
     (execute-batch! statement-or-connectable param-sets-or-statement-or-return-type nil)

     ;; Explicit connection: (execute-batch! conn [sql ...])
     (connectable? statement-or-connectable)
     (execute-batch! statement-or-connectable param-sets-or-statement-or-return-type nil)

     ;; Implicit connection: (execute-batch! [sql ...] return-type)
     :else
     (let [[sql & param-groups] (normalize-statement statement-or-connectable)]
       (jdbc/execute-batch!
         (:datasource *db*)
         sql
         param-groups
         (jdbc-options *db* (or param-sets-or-statement-or-return-type :raw))))))
  ([connectable-or-prepared statement-or-param-sets return-type]
   (cond
     ;; Prepared statement: (execute-batch! prepared-stmt param-sets return-type)
     (instance? java.sql.PreparedStatement connectable-or-prepared)
     (jdbc/execute-batch!
       connectable-or-prepared
       statement-or-param-sets
       (jdbc-options *db* (or return-type :raw)))

     ;; Explicit connection: (execute-batch! conn [sql ...] return-type)
     (connectable? connectable-or-prepared)
     (let [[sql & param-groups] (normalize-statement statement-or-param-sets)]
       (jdbc/execute-batch!
         connectable-or-prepared
         sql
         param-groups
         (jdbc-options *db* (or return-type :raw))))

     ;; Should not reach here
     :else
     (throw (ex-info "Invalid execute-batch! arguments"
                     {:args [connectable-or-prepared statement-or-param-sets return-type]})))))
