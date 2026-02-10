# Synthigy Development Environment

This directory contains the development setup that auto-initializes when you start a REPL.

## Quick Start

1. **Set environment variables** (or use defaults):
   ```bash
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   export POSTGRES_DB=synthigy
   export POSTGRES_USER=postgres
   export POSTGRES_PASSWORD=password
   ```

2. **Start the REPL**:
   ```bash
   clj -A:dev -M:repl
   ```

3. **Auto-initialization** - The `user.clj` namespace will automatically:
   - Initialize transit handlers
   - Connect to PostgreSQL
   - Load the deployed model from `__deploy_history`
   - Deploy the GraphQL schema

## Available Commands

Once the REPL starts, you have:

```clojure
(restart)         ; Reload code and restart system
(stop)            ; Stop the system
(deployed-model)  ; View the current ERD model
db/*db*           ; Access database connection
```

## What Gets Initialized

1. **Transit Handlers** (`synthigy.transit/init`)
   - Registers read/write handlers for ERDModel, ERDEntity, ERDRelation

2. **Database Connection** (`db.postgres/from-env`)
   - Creates HikariCP connection pool from environment variables
   - Sets `synthigy.db/*db*` global

3. **Deployed Model** (`pg/reload`)
   - Loads latest model from `__deploy_history` table
   - Generates runtime schema for SQL query generation
   - Sets `synthigy.dataset/*model*` global

4. **GraphQL Schema** (`lac/deploy-schema!`)
   - Generates Lacinia schema from the model
   - Compiles and caches it for query execution

## Troubleshooting

If initialization fails:

```clojure
;; Re-run manually
(init)

;; Or step-by-step:
(synthigy.transit/init)
(alter-var-root #'synthigy.db/*db* (constantly (synthigy.db.postgres/from-env)))
(synthigy.dataset.postgres/reload)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `synthigy` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `password` | Database password |
| `HIKARI_MAX_POOL_SIZE` | `20` | Connection pool size |
