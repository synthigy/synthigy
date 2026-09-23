# Synthigy

Model-driven IAM and data platform. Define entities and relations, and Synthigy
deploys the schema, generates a CRUD API over the `/data` wire protocol, and
enforces fine-grained access on every entity, relation and attribute.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/synthigy/tooling/main/install.sh | sh
synthigy up --db sqlite --observability on
```

Windows:

```powershell
irm https://raw.githubusercontent.com/synthigy/tooling/main/install.ps1 | iex
```

`up` provisions a Temurin JRE under `~/.synthigy/jre` on first run, downloads
the engine bundle and starts it: engine on **:7887**, operator console on
**:7888**. Nothing else is installed on the machine — with `--db sqlite` the
whole instance is one jar and two files on disk.

## First run

Open **http://localhost:7888**. The console walks the rest: create the first
superuser, then model your data and deploy it. The engine is already serving —
OIDC discovery at `/.well-known/openid-configuration`, the console's own login
at `/console/login`.

An identity for an application, from the CLI instead:

```bash
synthigy iam add-client "My BFF" --id my-bff --type confidential \
  --secret <secret> --grant client_credentials --role SUPERUSER --api Synthigy --local
```

`synthigy status` prints what this instance resolved without a network call,
`synthigy versions` lists releases, `synthigy up --version <tag>` pins one, and
`synthigy down` stops it.

## What is in it

**Identity & access**
- OAuth 2.1 + OIDC provider: authorization code (PKCE), client credentials,
  device code, refresh, token introspection
- Federated sign-in — Google, Microsoft, GitHub, Facebook
- User → Group → Role → Permission, with row-level and attribute-level rules

**Data modeling**
- ERD-based models; deploying a model creates and migrates the schema
- CRUD, relation traversal, aggregates and subscriptions over `/data`
- Model versioning; deployed versions are immutable

**Operations**
- Operator console: health, database and engine panels, encryption keys,
  version upgrades
- Bundled observability store — logs and metrics land in DuckDB or ClickHouse

## Bundles

One fused jar per backend pair; `--db` selects it and the observability store
follows:

| `--db` | observability | asset |
|---|---|---|
| `sqlite` | DuckDB | `synthigy-sqlite-duckdb-<tag>.jar` |
| `postgres` | ClickHouse | `synthigy-postgres-clickhouse-<tag>.jar` |

## Client SDKs

Talk to `/data` from your own application — MIT, published to each language's
registry:

| language | package |
|---|---|
| JavaScript / TypeScript | `@synthigy/sdk` (npm) |
| Python | `synthigy` (PyPI) |
| Clojure | `com.synthigy/sdk` (Clojars) |
| Go | `github.com/synthigy/go` |
| PHP | `synthigy/sdk` (Packagist) |

## Building from source

```bash
SYNTHIGY_COMBO=sqlite:httpkit:duckdb:console SYNTHIGY_AOT=true clojure -T:build uber
# -> target/synthigy-sqlite-httpkit-duckdb-console.jar
java -jar target/synthigy-sqlite-httpkit-duckdb-console.jar
```

The combo string names classpath layers, not the released asset: `httpkit` is
the HTTP server and `console` the operator UI, so both belong in every combo
even though neither appears in a release name.

## License

Synthigy is **free software** under the **GNU Affero General Public License,
version 3** — see [LICENSE](LICENSE). OSI-approved open source: no revenue
threshold, no field-of-use restriction, no features held back.

Use it, modify it, self-host it, commercially or not. The AGPL asks for
reciprocity in return: if you modify Synthigy and let users reach it **over a
network**, section 13 requires that those users can get its source.

Synthigy is also **dual-licensed**. A commercial license removes the AGPL's
reciprocity obligations — for embedding Synthigy in a proprietary product, or
offering it as a hosted service without releasing your source. See
[COMMERCIAL.md](COMMERCIAL.md); contact r.gersak@gmail.com.

The client **SDKs are MIT** — they are libraries you embed in your own
application, and calling a Synthigy server over HTTP does not make your
application a derivative work. Bundled third-party dependencies keep their own
licenses — see [NOTICE.md](NOTICE.md).
