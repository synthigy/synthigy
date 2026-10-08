# Audit, logs and traffic

**Dataline** is what Synthigy records about itself: the **history** of your
data, the engine's **logs**, and **traffic** counters. Where each lives is
decided by the bundle:

| Bundle | History | Logs and traffic |
|---|---|---|
| `sqlite` | `audit.db`, beside the database | `logs.db`, beside the database |
| `postgres` | tables in the same Postgres database | `logs.db` on each node |
| `postgres-clickhouse` | ClickHouse | ClickHouse |

History in Postgres or `audit.db` is backed up with the rest of your data. If a
part of Dataline cannot run — ClickHouse is down, a file is not writable — the
engine keeps serving, the consoles show Dataline as degraded, and the part
reconnects on its own when it can.

## History

History records every change to the entities you choose: inserts, updates,
deletes, links and unlinks, each with who made it, when, and in which request.
Switch it on per entity in the data modeling tool (the entity's Audit tab), or
for everything with `SYNTHIGY_AUDIT_ALL=true`.

Attributes classified as **secret** in the model are recorded as changed, but
their values are withheld. Other classifications — `pii`, `phi`, `internal` —
are labels for your own policies. History is never pruned automatically.

Read it with `POST /history`, or through an SDK:

| SDK method | Returns |
|---|---|
| `history.getAt(xid, at)` | the record as it was at a point in time |
| `history.diff(xid, from, to)` | what changed between two points |
| `history.events({recordXid, between})` | the change events of one record, or of all |
| `history.timeline({between, groupBy})` | events grouped by request, actor or scope |
| `history.since({cursor})` | everything after a point, oldest first — for syncing elsewhere |

History follows entity and attribute rules: you see history only for entities
you may read, without the attributes denied to you ([ACCESS.md](ACCESS.md)). A
superuser can erase the history of a single record, for example to honour a
deletion request.

## Logs

Structured logs of the running engine: level, namespace, message, data, host.
Read and filter them in the console under **Tools → Log cockpit**, or
over `POST /logs` with the `log:read` scope. Levels and per-namespace overrides
can be changed at runtime from the cockpit (`log:configure` scope), without a
restart.

Every line written while serving a request says who and from where: the
request id, the user, and the client address (`ip`, see
[PROXY.md](PROXY.md)) as columns, and in the line's `ctx` the user agent, the
OAuth client, the access token's `jti` and the first six characters of the
session id — never the whole id, which is the session cookie. One session or
token showing up from two addresses is the line to look for.

A refused bearer token leaves a `token-rejected` line at warn: the reason
(`signature`, `expired`, `revoked`, `audience`, `client`, `user`,
`inactive`), the address, and — when the signature holds — the token's `jti`,
client and user, with `expired-at` for an expired one. Changing log levels
leaves a `config-changed` line at warn, written before the change applies, so
raising the level cannot hide who raised it.

The local log file is capped by size and by age — `SQLITE_LOGS_MAX_MB` and
`SQLITE_LOGS_RETENTION_DAYS`; ClickHouse keeps logs for
`CLICKHOUSE_LOG_RETENTION_DAYS` ([ENV.md](ENV.md)).

`synthigy logs` is a different thing: the engine **process** output — boot
messages and crashes — for when the engine is not up to ask.

## Traffic

Per-minute counters: operations, subscription streams and a latency histogram,
per node, summed across nodes when read. They feed the traffic view of the log
cockpit and `POST /logs` (`traffic-stats`). No per-request log line is written
for them.
