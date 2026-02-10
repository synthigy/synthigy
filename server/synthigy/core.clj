(ns synthigy.core
  "Core foundation - backend implementation loading.

  This namespace provides the minimal foundation for Synthigy:
  - Dynamic database backend loading (Postgres, SQLite, MySQL)
  - Core namespace requires (dataset, IAM, data, db)

  Database backend implementation is loaded dynamically based on
  SYNTHIGY_DATABASE_TYPE or DB_TYPE environment variables.

  Services (OAuth, Lacinia, LDAP, Admin, HTTP servers) are NOT loaded here.
  They should be required by server implementations (synthigy.pedestal, etc.)"
  (:require
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    synthigy.data
    synthigy.dataset
    synthigy.dataset.core
    synthigy.dataset.encryption
    synthigy.dataset.enhance
    synthigy.dataset.operations
    synthigy.dataset.sql.naming
    synthigy.db
    synthigy.iam
    synthigy.iam.access
    synthigy.iam.context
    synthigy.iam.encryption
    synthigy.transit))

;;; ============================================================================
;;; Backend Implementation Loading
;;; ============================================================================

(defn- require-backend-impl!
  "Dynamically loads database backend implementation namespace.

  This triggers lifecycle registration for backend-specific modules:
  - Database connection/pooling (synthigy.db.*)
  - Dataset operations (synthigy.dataset.*)
  - Audit enhancement (synthigy.iam.audit.*)

  Backend type is determined from environment variables:
  - SYNTHIGY_DATABASE_TYPE (explicit: 'postgres', 'sqlite', 'mysql')
  - DB_TYPE (generic fallback)
  - Defaults to 'postgres' if not specified

  Returns:
    Keyword - Backend type loaded (:postgres, :sqlite, or :mysql)

  Throws:
    ExceptionInfo - If backend type is unsupported"
  []
  (let [db-type (or (env :synthigy-database-type)
                    (env :db-type)
                    "sqlite"
                    "postgres")
        impl-ns (symbol (str "synthigy.impl." db-type))]
    (log/infof "[Core] Loading %s backend implementation..." db-type)
    (try
      (require impl-ns)
      (log/infof "[Core] %s backend implementation loaded" db-type)
      (keyword db-type)
      (catch java.io.FileNotFoundException e
        (throw (ex-info (str "Unsupported database backend: " db-type)
                        {:type db-type
                         :supported ["postgres" "sqlite" "mysql"]
                         :hint "Set SYNTHIGY_DATABASE_TYPE environment variable"
                         :impl-namespace impl-ns}
                        e))))))

;; Load backend implementation at namespace initialization
(require-backend-impl!)
