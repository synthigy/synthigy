(ns synthigy.impl.sqlite
  "SQLite backend implementation loader.

  This namespace requires all SQLite-specific implementation namespaces,
  which triggers their lifecycle registration at namespace load time.

  Loaded dynamically based on SYNTHIGY_DATABASE_TYPE or DB_TYPE environment
  variables. See synthigy.core for the dynamic loading logic.

  SQLite-specific namespaces:
  - synthigy.db.sqlite - Connection management and lifecycle
  - synthigy.dataset.sqlite - Dataset deployment, protocols (TypeCodec, SQLDialect), lifecycle
  - synthigy.dataset.sqlite.query - SQL query building (CRUD operations)
  - synthigy.dataset.sqlite.patch - Schema migrations (empty for now)
  - synthigy.iam.audit.sqlite - Audit enhancement (modified_by, modified_on) [TEMPORARILY DISABLED FOR TESTING]"
  (:require
   synthigy.db.sqlite
   synthigy.dataset.sqlite
   synthigy.dataset.sqlite.query
   synthigy.dataset.sqlite.patch
   synthigy.iam.audit.sqlite))
