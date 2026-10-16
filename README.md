# Synthigy

Model-driven IAM and data platform. Define entities and relations, and Synthigy
deploys the schema, serves a CRUD API over `/data` and enforces access on every
entity, relation and attribute.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/synthigy/tooling/main/install.sh | sh
```

Windows:

```powershell
irm https://raw.githubusercontent.com/synthigy/tooling/main/install.ps1 | iex
```

This installs the `synthigy` CLI. It runs instances and connects your code to them.

## Run

```bash
synthigy up --bundle sqlite
```

The first `up` downloads a Java runtime and the engine; nothing else is needed.
The engine serves on **:7887**. The **portal** on **127.0.0.1:7888** creates the
first superuser and runs the instance; the **console** at `/console` on the engine
administers it ([Portal and console](docs/CONSOLE.md)). Without `--bundle`, `up`
opens a setup page to choose one.

[Getting started](docs/GETTING_STARTED.md) goes from here to a deployed model and a
first query.

## What is in it

**Identity & access**
- OAuth 2.1 + OIDC provider: authorization code (PKCE), client credentials,
  device code, refresh; introspection, revocation, userinfo, JWKS, discovery
- Sign-in with Google, Microsoft, GitHub, Facebook, LinkedIn, Discord and custom
  OIDC providers
- Invitation links, password connectors to your own directory, custom login pages
- Users, groups and roles with entity, attribute and row-level rules

**Data**
- ERD models, versioned; deploying creates and migrates the schema
- Reads in XSQL, where the query is the shape of the result; SQL templates for
  analytics
- Writes: `stack`, `sync`, `slice`, `delete`, `purge`
- Live queries over one SSE stream per client
- History of every audited record, and `encrypted` attributes under a master key
  in your custody

## Bundles

| `--bundle` | data | audit history | logs + traffic |
|---|---|---|---|
| `sqlite` | SQLite file | `audit.db` beside it | local `logs.db` |
| `postgres` | your Postgres | same database | local `logs.db` per node |
| `postgres-clickhouse` | your Postgres | your ClickHouse | your ClickHouse |

Synthigy never creates the Postgres database; the setup page prints the SQL to run
first. Settings: [Environment variables](docs/ENV.md).

## Documentation

[docs/README.md](docs/README.md) lists every page: modeling, the data API, XSQL,
access control, OAuth, operations, the reverse proxy, encryption, SDKs, embedding,
and the environment variable and CLI references.

Client SDKs (MIT): JavaScript/TypeScript, Python, Go, Clojure and PHP —
[SDKs](docs/SDKS.md).

## Building from source

```bash
SYNTHIGY_COMBO=engine-sqlite:audit-sqlite:log-sqlite:server-httpkit:server-console \
  SYNTHIGY_AOT=true clojure -T:build uber
synthigy up --bundle sqlite \
  --jar target/synthigy-engine-sqlite-audit-sqlite-log-sqlite-server-httpkit-server-console.jar
```

The combo names one `deps.edn` alias per source folder; the `postgres` bundle is
`engine-postgres:audit-postgres:log-sqlite:server-httpkit:server-console`. A source
build has no Tools in the console: the web components ship in released bundles only.

## Contributing

Issues and pull requests are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).
Contributors sign a one-time CLA ([CLA.md](CLA.md)).

## License

**Elastic License 2.0** (SPDX `Elastic-2.0`), see [LICENSE](LICENSE). Use, modify and
self-host it in any company, and build and sell your own products on it. Not
allowed: offering Synthigy itself to third parties as a hosted or managed service.

A **commercial license** covers hosting as a service, OEM, resale, warranty and
support: [COMMERCIAL.md](COMMERCIAL.md), r.gersak@gmail.com.

The client SDKs and the bundled web components (`tooling.js`) are MIT. Third-party
dependencies keep their own licenses: [NOTICE.md](NOTICE.md).
