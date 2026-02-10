(ns synthigy.db
  "Database abstraction protocol and global state (JVM-specific).

  This namespace provides JVM-specific database initialization and lifecycle
  management, along with cross-platform protocol definitions.

  ## Backend Discovery

  Database backend type is determined from environment variables:
  - SYNTHIGY_DATABASE_TYPE (explicit: 'postgres', 'sqlite', 'mysql')
  - DB_TYPE (generic fallback)
  - Defaults to 'postgres' if not specified

  ## Dynamic Loading

  Backend namespaces are loaded on-demand during startup:
  - For 'postgres': synthigy.db.postgres, synthigy.dataset.postgres, etc.
  - For 'sqlite': synthigy.db.sqlite, synthigy.dataset.sqlite, etc.

  This keeps the core lightweight and avoids loading unused database drivers.

  ## Usage

  From application startup (synthigy.core):
    (require '[synthigy.db :as db])
    (db/start)  ; Auto-detects backend type, loads namespaces, connects
    (db/stop)   ; Disconnects and cleans up

  With explicit configuration:
    (db/start {:type \"postgres\" :host \"localhost\" :db \"mydb\"})
    (db/start {:type \"sqlite\" :path \"/tmp/test.db\"})

  ## Environment Variables

    SYNTHIGY_DATABASE_TYPE=postgres  # Backend type
    DB_TYPE=sqlite                    # Alternative variable name

    # PostgreSQL-specific (when type=postgres):
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_DB=synthigy
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=password
    HIKARI_MAX_POOL_SIZE=20

    # SQLite-specific (when type=sqlite):
    SQLITE_PATH=/var/lib/synthigy.db"
  (:require
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]))

;;; ============================================================================
;;; Global State
;;; ============================================================================

(defonce ^:dynamic *db* nil)

;;; ============================================================================
;;; Database Abstraction Protocol
;;; ============================================================================

(defprotocol ModelQueryProtocol
  (sync-entity
    [this entity-id data]
    "Sync entity takes dataset entity id and data and
    synchronizes DB with current state. This includes
    inserting/updating new records and relations as
    well as removing relations that were previously linked
    with input data and currently are not")
  (stack-entity
    [this entity-id data]
    "Stack takes dataset entity id and data to stack
    input data on top of current DB state")
  (slice-entity
    [this entity-id args selection]
    "Slice takes dataset entity id and data to slice
    current DB state based on input data effectively deleting
    relations between entities")
  (get-entity
    [this entity-id args selection]
    "Takes dataset entity id, arguments to pinpoint target row
    and selection that specifies which attributes and relations
    should be returned")
  (get-entity-tree
    [this entity-id root on selection]
    "Takes dataset entity id, root record and constructs tree based 'on'.
    Selection that specifies which attributes and relations
    should be returned")
  (search-entity
    [this entity-id args selection]
    "Takes dataset entity id, arguments to pinpoint target rows
    and selection that specifies which attributes and relations
    should be returned")
  (search-entity-tree
    [this entity-id on args selection]
    "Takes dataset entity id, arguments to pinpoint target rows
    based 'on' recursion and selection that specifies which attributes and relations
    should be returned")
  (purge-entity
    [this entity-id args selection]
    "Find all records that match arguments, delete found records
    and return deleted information based on selection input")
  (aggregate-entity
    [this entity-id args selection]
    "Takes dataset entity id, arguments and selection to return aggregated values
    for given args and selection. Possible fields in selection are:
    * count
    * max
    * min
    * avg")
  (aggregate-entity-tree
    [this entity-id on args selection]
    "Takes dataset entity id 'on' recursion with arguments
    and selection to return aggregated values for given args and selection.
    Possible fields in selection are:
    * count
    * max
    * min
    * avg")
  (delete-entity
    [this entity-id data]
    "Function takes dataset entity id and data to delete entities from
    from DB"))

;;; ============================================================================
;;; Database Records
;;; ============================================================================

(defrecord Postgres [host port user db password max-connections datasource])
(defrecord SQLite [path datasource])

;;; ============================================================================
;;; Backend Discovery
;;; ============================================================================

(defn backend-type
  "Returns database backend type from environment configuration.

  Checks environment variables in order:
  1. SYNTHIGY_DATABASE_TYPE - Explicit database type
  2. DB_TYPE - Generic database type variable
  3. Defaults to 'postgres' if neither is set

  Returns:
    String - Database backend type ('postgres', 'sqlite', 'mysql')

  Examples:
    SYNTHIGY_DATABASE_TYPE=postgres  => \"postgres\"
    DB_TYPE=sqlite                    => \"sqlite\"
    (no env vars)                     => \"postgres\""
  []
  (or (env :synthigy-database-type)
      (env :db-type)
      "sqlite"))

(defn require-backend!
  "Dynamically loads database backend namespaces based on backend type.

  This function performs dynamic namespace loading to avoid hardcoded
  dependencies on specific database implementations. It loads:
  - Database connection/lifecycle namespace (synthigy.db.TYPE)
  - Dataset operations namespace (synthigy.dataset.TYPE)
  - Query building namespace (synthigy.dataset.TYPE.query, if exists)
  - Feature patches namespace (synthigy.dataset.TYPE.patch)

  Args:
    db-type - Optional backend type string. If not provided, uses backend-type

  Returns:
    Map with loaded backend info:
      {:type \"postgres\"
       :namespaces ['synthigy.db.postgres ...]}

  Throws:
    ExceptionInfo - If db-type is not supported

  Examples:
    (require-backend!)              ; Uses env config
    (require-backend! \"postgres\") ; Explicit type"
  ([] (require-backend! (backend-type)))
  ([db-type]
   (log/infof "[DB] Loading %s backend..." db-type)
   (case db-type
     "postgres"
     (do
       (require 'synthigy.impl.postgres)
       {:type "postgres"
        :namespaces ['synthigy.impl.postgres
                     'synthigy.db.postgres
                     'synthigy.dataset.postgres
                     'synthigy.dataset.postgres.query
                     'synthigy.dataset.postgres.patch
                     'synthigy.iam.audit.postgres]})

     "sqlite"
     (do
       (require 'synthigy.impl.sqlite)
       {:type "sqlite"
        :namespaces ['synthigy.impl.sqlite
                     'synthigy.db.sqlite
                     'synthigy.dataset.sqlite
                     'synthigy.dataset.sqlite.query
                     'synthigy.dataset.sqlite.patch
                     'synthigy.iam.audit.sqlite]})

     "mysql"
     (do
       (require 'synthigy.db.mysql)
       (require 'synthigy.dataset.mysql)
       (require 'synthigy.dataset.mysql.patch)
       {:type "mysql"
        :namespaces ['synthigy.db.mysql
                     'synthigy.dataset.mysql
                     'synthigy.dataset.mysql.patch]})

     (throw (ex-info "Unsupported database backend"
                     {:type db-type
                      :supported ["postgres" "sqlite" "mysql"]
                      :hint "Set SYNTHIGY_DATABASE_TYPE environment variable"})))))
