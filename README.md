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

**Business Source License 1.1** (SPDX `BUSL-1.1`), see [LICENSE](LICENSE). Source-available:
free in production for individuals, non-profits, education and organisations up
to EUR 5M, including building and selling your own products on it. Development,
testing and evaluation are free for everyone. Each version becomes GPL-2.0-or-later /
Apache-2.0 four years after its release.

A **commercial license** covers organisations over EUR 5M, integrators deploying
for them, hosting Synthigy as a service, OEM, warranty and support:
[COMMERCIAL.md](COMMERCIAL.md), r.gersak@gmail.com.

The client SDKs and the bundled web components (`tooling.js`) are MIT. Third-party
dependencies keep their own licenses: [NOTICE.md](NOTICE.md).
