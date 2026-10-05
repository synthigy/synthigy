# Synthigy

Model-driven IAM and data platform. Define entities and relations, and Synthigy
deploys the schema, generates a CRUD API over the `/data` wire protocol, and
enforces fine-grained access on every entity, relation and attribute.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/synthigy/tooling/main/install.sh | sh
```

Windows:

```powershell
irm https://raw.githubusercontent.com/synthigy/tooling/main/install.ps1 | iex
```

This installs the `synthigy` CLI, one static binary in `~/.synthigy/bin`, and
adds it to your PATH. It runs instances and connects your code to them;
`synthigy upgrade` updates it.

## Run

```bash
synthigy up
```

A fresh instance has no backend yet, so `up` opens the setup page at
**http://127.0.0.1:7888/setup**: pick a bundle, enter the database connection,
Test, Launch. The boot streams into the page and ends with the form for the
first superuser.

Name the bundle to skip the page; the portal then asks for the first
superuser. The choice is saved to the instance's `.env`, so every later `up`
starts straight away:

```bash
synthigy up --bundle sqlite
```

The first `up` provisions a Temurin JRE under `~/.synthigy/jre` and downloads
the bundle, sha256-verified. No Java install, nothing else on the machine: with
`sqlite` the whole instance is one jar and three files in `~/.synthigy/db/` —
`synthigy.db`, `audit.db` and `logs.db`.

The engine serves on **:7887**, the portal on **127.0.0.1:7888**.

| command | does |
|---|---|
| `synthigy status` | engine, portal, endpoint and identity, read off disk with no network call |
| `synthigy console` | opens the portal |
| `synthigy logs -f` | follows the engine process log |
| `synthigy versions` | lists releases; `synthigy up --version <tag>` starts a specific one |
| `synthigy service install` | starts the instance at boot (systemd user unit, launchd agent) |
| `synthigy down` | stops the engine and the portal |

Engine upgrades are done from the portal.

## Portal and console

**Portal** — http://127.0.0.1:7888, served by the CLI. It binds
loopback only and that bind is the credential: there is no login. Setup,
health, database and Dataline settings, encryption key custody and rotation,
engine stop/start/restart and release upgrades, superusers, public access. On a
remote host: `ssh -L 7888:127.0.0.1:7888 <host>`.

**Console** — http://localhost:7887/console, signed in as a user. Every
user gets their profile, sessions and sign-in methods. Administrators manage
users, groups, roles, apps (OAuth clients), APIs, credential connectors and
identity providers; system operators also get encryption, topology and custom
login pages. The tools sit beside them: **Data modeling** (draw entities and
relations, deploy them), the **Data console** for XSQL reads and writes, and
the **Log cockpit** for logs and traffic.

## What is in it

**Identity & access**
- OAuth 2.1 + OIDC provider: authorization code (PKCE), client credentials,
  device code, refresh; introspection, revocation, userinfo, JWKS, discovery
- Federated sign-in — Google, Microsoft, GitHub, Facebook, LinkedIn, Discord,
  and up to three custom OIDC providers
- Invitations: your app mints a one-time link, the invited person finishes the
  account with a password or a federated sign-in
- Credential connectors: verify passwords locally or against your own system
  over a webhook — [docs/IAM_CONNECTORS.md](docs/IAM_CONNECTORS.md)
- User → Group → Role → Permission, with row-level and attribute-level rules

**Data**
- ERD-based models; deploying a model creates and migrates the schema
- Model versioning; deployed versions are immutable
- `/data` reads in XSQL, where the query is the shape of the result —
  relations, aggregates and trees included; SQL templates for analytics
- `/data` writes: `sync` (upsert), `stack` (add without replacing siblings),
  `slice` (unlink), `delete`, `purge`
- Live queries, multiplexed on one SSE stream per client
- History: any audited record as it was at a point in time, its events, diffs
  and timeline; a record's history erased on request
- `encrypted` attributes, envelope-encrypted under a master key in your custody

**Dataline** — audit history, logs and traffic: the record of what happened to
your data
- Audit history in your own database — Postgres tables beside the data, or an
  `audit.db` next to SQLite — backed up with everything else and never deleted
  automatically
- Logs and traffic in a local, size-capped `logs.db`, or in ClickHouse for
  fleet-wide search

## Bundles

One fused jar per bundle; `--bundle` picks it. The bundle is the whole choice:
there is no switch that turns audit or logs off.

| `--bundle` | data | audit history | logs + traffic | release asset |
|---|---|---|---|---|
| `sqlite` | SQLite file | `audit.db` beside it | local `logs.db` | `synthigy-sqlite-<tag>.jar` |
| `postgres` | your Postgres | tables in the same database | local `logs.db` per node | `synthigy-postgres-<tag>.jar` |
| `postgres-clickhouse` | your Postgres | your ClickHouse | your ClickHouse | `synthigy-postgres-clickhouse-<tag>.jar` |

Postgres is configured with `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`,
`POSTGRES_USER` and `POSTGRES_PASSWORD`; ClickHouse with `CLICKHOUSE_URL`,
`CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`. The setup page writes them to
`.env`. Synthigy never creates the Postgres database — the setup page prints the
`CREATE ROLE` and `CREATE DATABASE` statements to run first.

## Putting it on the internet

The engine serves plain HTTP and terminates no TLS, so a public instance sits
behind nginx, Caddy or Traefik. [docs/PROXY.md](docs/PROXY.md) has the
copy-paste configs and what the proxy has to get right: TLS (the session
cookies are `Secure`), the client address in `X-Forwarded-For`, unbuffered SSE,
and the public origin in `SYNTHIGY_IAM_ROOT_URL`.

Synthigy answers browser calls from another origin only when it knows that
origin — see [docs/CORS.md](docs/CORS.md).

## Client SDKs

Talk to `/data` from your own application — MIT, published to each language's
registry. Each SDK generates typed functions from your `.xsql` queries against
the deployed model.

| language | package |
|---|---|
| JavaScript / TypeScript | `@synthigy/sdk` (npm) |
| Python | `synthigy` (PyPI) |
| Clojure | `com.synthigy/sdk` (Clojars) |
| Go | `github.com/synthigy/go` |
| PHP | `synthigy/sdk` (Packagist) |

Give an application its own identity and run it under that identity:

```bash
synthigy iam add-client "My App" --id my-app --type confidential \
  --grant client_credentials --role "<role>" --api Synthigy --local
synthigy connect http://localhost:7887 --client-id my-app
synthigy exec -- node app.js
```

`add-client` prints the secret once and `connect` asks for it. `--role` grants
existing roles to the app's service user; `--api Synthigy` is what lets its
tokens reach `/data`. `exec` hands the program the endpoint and the app's
identity. `--local` works on the instance's own machine; for a remote one,
create the app in the engine console.

## Embedding

The engine is a library too. Clojars carries the same three bundles —
`com.synthigy/sqlite-bundle`, `postgres-bundle`, `postgres-clickhouse-bundle` —
so an app ships its own modules and the engine as one jar. That jar runs on its
own, or under `synthigy up` with the portal. See
[docs/EMBEDDED.md](docs/EMBEDDED.md).

## Documentation

[docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) walks from install to a
first query; [docs/README.md](docs/README.md) lists every page — modeling, the
data API, XSQL, access control, OAuth, operations, encryption, SDKs, and the
environment variable and CLI references.

## Building from source

```bash
SYNTHIGY_COMBO=engine-sqlite:audit-sqlite:log-sqlite:server-httpkit:server-console \
  SYNTHIGY_AOT=true clojure -T:build uber
# -> target/synthigy-engine-sqlite-audit-sqlite-log-sqlite-server-httpkit-server-console.jar
synthigy up --bundle sqlite \
  --jar target/synthigy-engine-sqlite-audit-sqlite-log-sqlite-server-httpkit-server-console.jar
```

The combo lists `deps.edn` aliases, one per source folder: an engine, an audit
store, a log store, the HTTP server and the engine console. The `postgres`
bundle is `engine-postgres:audit-postgres:log-sqlite:server-httpkit:server-console`.
The jar has no `Main-Class`; `synthigy up --jar` boots and supervises it the
way it runs a release. A source build has no tools in the engine console — the
web components (`tooling.js`) ship in the released bundles only.

## Contributing

Issues and pull requests are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).
Contributors sign a one-time CLA ([CLA.md](CLA.md)).

## License

Synthigy is licensed under the **Elastic License 2.0** (SPDX `Elastic-2.0`) —
see [LICENSE](LICENSE). It is source-available, free of charge for everyone:

- use it, modify it, self-host it, inside any company of any size;
- build your own products on it and sell them — an ERP, a SaaS, an internal
  tool with Synthigy as its data layer.

One thing is not allowed: providing Synthigy itself to third parties as a
**hosted or managed service**. Licensing and copyright notices must stay
intact, and no trademark rights are granted.

A **commercial license** covers what the Elastic License 2.0 does not — hosting
Synthigy as a service, OEM and reseller arrangements, warranty and support.
See [COMMERCIAL.md](COMMERCIAL.md); contact r.gersak@gmail.com.

The client **SDKs are MIT**, and so are the bundled web components
(`tooling.js`). Bundled third-party dependencies keep their own
licenses — see [NOTICE.md](NOTICE.md).
