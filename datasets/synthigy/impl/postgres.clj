(ns synthigy.impl.postgres
  "PostgreSQL backend implementation loader.

  This namespace requires all Postgres-specific implementation namespaces,
  which triggers their lifecycle registration at namespace load time.

  Loaded dynamically based on SYNTHIGY_DATABASE_TYPE or DB_TYPE environment
  variables. See synthigy.core for the dynamic loading logic.

  Postgres-specific namespaces:
  - synthigy.db.postgres - Connection pooling and lifecycle
  - synthigy.dataset.postgres - Dataset operations (CRUD, queries)
  - synthigy.dataset.postgres.query - SQL query building
  - synthigy.dataset.postgres.patch - Schema migrations
  - synthigy.iam.audit.postgres - Audit enhancement (modified_by, modified_on)"
  (:require
   synthigy.db.postgres
   synthigy.dataset.postgres
   synthigy.dataset.postgres.query
   synthigy.dataset.postgres.patch
   synthigy.iam.audit.postgres))
