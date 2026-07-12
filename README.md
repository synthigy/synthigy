# Synthigy Core

Open-source IAM and Dataset library - the foundation of Synthigy.

## Features

### Identity & Access Management (IAM)
- OAuth 2.1 + OIDC implementation
- Authorization Code Flow & Device Code Flow
- JWT token management
- Role-based access control (User → Group → Role → Permission)
- Fine-grained permissions on entities/relations/attributes
- LDAP, Azure AD, Auth0, GitHub integration

### Data Modeling & GraphQL
- Visual data modeling with automatic infrastructure generation
- Entity-Relation-Diagram (ERD) based models
- Automatic PostgreSQL schema deployment
- Automatic GraphQL CRUD API generation
- Model versioning and migration support
- Type-safe attribute validation

### Database Support
- PostgreSQL (full support)
- Database-agnostic SQL utilities
- Planned: SQLite, MySQL support

## Installation

```clojure
;; deps.edn
{:deps {io.github.synthigy/synthigy-core {:mvn/version "0.1.0"}}}
```

## Quick Start

```clojure
(require '[synthigy.dataset.core :as ds])
(require '[synthigy.db.postgres :as pg])

;; Connect to database
(def db (pg/connect {:dbname "mydb" :host "localhost"}))

;; Create a data model
(def model (ds/create-model))
(def entity (ds/create-entity {:name "User" :type "STRONG"}))
(def model' (ds/add-entity model entity))

;; Deploy to database
(ds/deploy! db model')
```

## Architecture

- **Protocols in `.cljc`**: Share contracts with ClojureScript frontend
- **Implementations in `.cljc`**: Maximum code sharing
- **DB-specific in `.clj`**: Only PostgreSQL connection code

## Documentation

See `/docs` in main repository for full documentation.

## License

TBD (Open Source)
