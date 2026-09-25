# MySQL Backend — Onboarding Plan (DEFERRED)

**Status:** Possible extension. **Not scheduled** — deferred in favor of higher-priority work.
This doc captures the analysis so the port can start cold later without re-deriving it.

## TL;DR

Adding MySQL is a **medium** effort (~1.5–2.5 weeks, ~5–7k lines) that follows the existing
per-backend module pattern. It is a **reach/compatibility play, not a performance upgrade** —
this engine's shape (multi-level relational writes, `jsonb`, RLS, level-oriented joins) favors
Postgres. The two things MySQL lacks vs Postgres (`RETURNING`, `CREATE SEQUENCE`) are both
worked around with patterns already present in the codebase. No engine changes; the surface is
one branch of one write function plus the usual per-backend DDL/plug grind.

## How backends work here (recap)

A backend is a **self-contained source root** (`postgres/`, `sqlite/`, `cockroach/`) added to the
classpath by a `deps.edn` alias (`clj -M:postgres:httpkit:dev`). Selection is the alias, not an env
var. Each backend provides the same shadowed namespaces and extends the shared-core protocols onto
its own `defrecord`. Runtime wiring is the patcho lifecycle chain:
`:synthigy/database → :synthigy/dataset → :synthigy/plug → :synthigy/audit → :synthigy.iam/connector`.

Shared seams a backend implements:
- `synthigy.dataset.sql.protocol` — `TypeCodec`, `SQLDialect`, `SchemaManager`
- `synthigy.db.sql/JDBCBackend`, `synthigy.db/{ModelQueryProtocol,Translator}`
- `synthigy.dataset.core/DatasetProtocol`, `synthigy.dataset.sql.naming/SQLNameResolution`
- `synthigy.dataset.enhance/AuditEnhancement`, patcho `VersionStore`/`LifecycleStore`

## Design decisions (settled)

1. **Keep the `bigint _eid` surrogate on every backend.** The engine transforms on `_eid`, and int8
   monotonic keys beat a varchar(22) nanoid on join speed and index footprint (InnoDB copies the PK
   into every secondary index). **Do not** switch MySQL to join relations on `xid`.
2. **`_eid` stays DB-assigned** via `AUTO_INCREMENT` (the MySQL equivalent of `bigserial`).
   Cross-container-safe for free — the DB serializes id assignment. **No hi-lo / counter table** —
   that would replace a free, correct, multi-container-safe primitive with a hand-rolled contention
   point. (`AUTO_INCREMENT` exists; `CREATE SEQUENCE` does not — the sequence gap is why hi-lo was
   considered and then rejected.)
3. **Recover keys with a SELECT-back, not `RETURNING`.** MySQL has no `RETURNING`; the write splits
   into write + harvest (below). MariaDB has `RETURNING` and could keep the fused write, but only if
   `INSERT … ON DUPLICATE KEY UPDATE … RETURNING` returns `_eid` for both inserted and matched rows —
   verify before relying on it. Default plan targets **MySQL 8.0**; MariaDB is a nicer variant, not required.

## Write path — current vs MySQL

Function: `synthigy.dataset.sql.query/store-entity-records` (STEP 6, ~line 3527). Steps 1–5 and
mapping reconstruction (7+) are **unchanged**; only the execute branch differs.

**Current (PG / SQLite / CRDB)** — one statement fuses write + key harvest:
```sql
INSERT INTO "tbl" (cols...) VALUES (...),(...),...
  ON CONFLICT (constraint-keys) DO UPDATE SET ...   -- + RLS WHERE
  RETURNING _eid, xid, constraint-keys
```

**MySQL** — split into two statements per table per level:
```sql
-- 4a. write (no keys back)
INSERT INTO `tbl` (cols...) VALUES (...),(...),...
  ON DUPLICATE KEY UPDATE col = VALUES(col), ...;    -- RLS moves to a BEFORE UPDATE trigger

-- 4b. harvest _eid for the whole chunk, new AND pre-existing rows
SELECT _eid, xid, constraint-keys
  FROM `tbl`
 WHERE (constraint-keys) IN ((...),(...),...);
```
Downstream `mapping` + `link-relations` are byte-for-byte identical — 4b returns the same row shape
`[_eid, xid, constraint-keys]` the old `RETURNING` did.

Notes:
- **Harvest on `constraint-keys`, not the generated xid.** On an upsert-hit the DB row keeps its
  original xid (xid is never in the update set), so a freshly-minted xid won't match it. This is why
  `return-cols` already carries `constraint-keys` and why the `resolve-only?` branch already SELECTs
  by them (`query.clj:3538-3545`) — MySQL's 4b *is* that SELECT, always run.
- **Why the SELECT-back is unavoidable:** on upsert-hit the needed `_eid` already lives in the table.
  `getGeneratedKeys()` only reports new rows; `LAST_INSERT_ID()` returns one value not a batch;
  hi-lo can't help (existing row keeps its old `_eid`). Every path converges on "read it back."
- **No fuse alternative:** MySQL has no `RETURNING` and no `MERGE`. (Even `MERGE`, where it exists,
  doesn't return keys until PG 17 / SQL Server `OUTPUT`.) The 2-statement split is the honest path.

## Read path — skip the fused compiler

`postgres/.../fused.clj` builds the nested JSON response one-statement-per-level via
`json_build_object` + `json_agg(child ORDER BY …) FILTER (WHERE …)` + `LEFT JOIN LATERAL`. Two of
those don't port cleanly to MySQL:
- **No `ORDER BY` inside `JSON_ARRAYAGG`** — MySQL can't order elements within the aggregate; the
  fused path orders nested collections at every level. Workarounds are fragile.
- **No `FILTER (WHERE)`** — emulate with `CASE`, a rewrite.

**So don't port it.** SQLite already ships **without** a fused path (only `query.clj` + `patch.clj`)
and rides the engine's built-in fallback (`try-search`/`try-get` return `nil` → per-level pull
compiler). MySQL does the same:
- **v1:** SQLite-style non-fused reads (N pulls, one per level). Correct, shippable, proven.
- **Later, only if measured:** a degraded `mysql/fused.clj` eating the ordering workaround. Optional.

MySQL read round-trip profile == SQLite's today (more chatter than PG on deep trees, but a correct baseline).

## Files to create (mirror `sqlite/` — closest trigger/context model)

```
mysql/src/synthigy/core.clj                      -- module aggregator
mysql/src/synthigy/db/mysql.clj                  -- pool, JDBCBackend, VersionStore, LifecycleStore, Translator
mysql/src/synthigy/dataset/mysql.clj             -- DatasetProtocol, TypeCodec, SQLDialect, SchemaManager, AuditEnhancement, DDL
mysql/src/synthigy/dataset/mysql/query.clj       -- ModelQueryProtocol (non-fused reads + the 4a/4b write branch)
mysql/src/synthigy/dataset/mysql/patch.clj       -- model patch application
mysql/src/synthigy/plug/mysql.clj           -- delta queue tables, row triggers, _ctx table, polling drainer
mysql/src/synthigy/subscriptions/mysql.clj       -- :synthigy/plug lifecycle module
mysql/src/synthigy/iam/audit.clj                 -- :synthigy/audit module
mysql/src/synthigy/iam/connector/mysql.clj       -- CredentialsProvider record
```
Shared-core touch: add `(defrecord MySQL …)` in `core/src/synthigy/db.clj`, a `Translator` default,
and a `:mysql` alias in `core/deps.edn` (`mysql-connector-j` dep). No `fused.clj`, no `xid.clj`
required for v1 (like SQLite).

## Dialect deltas

| concern | Postgres | MySQL |
|---|---|---|
| surrogate key | `bigserial` | `AUTO_INCREMENT` |
| upsert | `ON CONFLICT … DO UPDATE` / `EXCLUDED.col` | `ON DUPLICATE KEY UPDATE` / `VALUES(col)` |
| key harvest | `RETURNING` (fused) | separate `SELECT … WHERE (ckeys) IN (…)` |
| RLS on update | inline `WHERE` on `ON CONFLICT` | `BEFORE UPDATE` trigger guard |
| enums | `CREATE TYPE` | inline column `ENUM(...)`, no catalog type |
| JSON | `jsonb`, `->`/`#>>` | `JSON`, `JSON_EXTRACT`/`->>'$.x'` |
| identifier quote | `"col"` | `` `col` `` |
| LIMIT | `LIMIT n OFFSET m` | `LIMIT m, n` (reversed) |
| enum cast placeholder | `?::type` | `?` (none) |
| bind-param cap | 65535 | 65535 |
| plug wake | LISTEN/NOTIFY | polling drainer (like SQLite/CRDB) |
| context propagation | session GUC | `_ctx` table (like SQLite) |
| nested-JSON read | fused compiler | skip — non-fused fallback (like SQLite) |

## Effort & expected performance

- **Effort:** ~1.5–2.5 weeks, ~5–7k lines. Bulk in `dataset/mysql.clj` + `plug/mysql.clj`.
  Genuinely new logic (not a mechanical fork): the RLS-update-as-trigger guard.
- **Writes:** +1 batched `SELECT` per table per level vs PG. Latency tax (~+10–20% on the write path,
  concentrated in round-trips, amortizes with batch size), not a scaling cliff. Reads unaffected by it.
- **Reads:** SQLite round-trip profile (more than PG's fused on deep trees) until/unless a MySQL
  fused compiler is written.
- **Overall:** roughly comparable to Postgres on trivial OLTP; Postgres ahead for this engine's
  join/JSON/RLS-heavy shape. **Do not port expecting a speedup.**

## Verify before starting

1. Confirm `INSERT … ON DUPLICATE KEY UPDATE` + a following `SELECT … WHERE (ckeys) IN (…)` inside
   one transaction returns the correct `_eid` for both inserted and matched rows under concurrent
   writers (the multi-container case).
2. If considering MariaDB instead: probe whether `INSERT … ON DUPLICATE KEY UPDATE … RETURNING`
   returns `_eid` for matched (not just inserted) rows. If yes, MariaDB keeps the fused write and the
   4a/4b split is unnecessary there.
3. Decide the RLS-update trigger shape against the existing SQLite/CRDB plug trigger patterns.

## Popularity context (why this is a reach, not a must)

Stack Overflow 2025: PostgreSQL ~55.6% (58.2% pro), MySQL ~40.5%, MariaDB ~17%. DB-Engines: MySQL #2
(declining), PostgreSQL #4 (rising), MariaDB #12. MySQL is the bigger name to claim support for;
MariaDB is the technically-closer-to-PG variant. Neither is a performance argument for this engine.
