# The data API

Every read and write of your data goes through one endpoint, `POST /data`. A
request carries one or more **operations**; the response carries one result per
operation. The [SDKs](SDKS.md) build these requests for you — this page shows
the JSON they send, which is also what you send with `curl`.

```http
POST /data
Authorization: Bearer <access token>
Content-Type: application/json

{
  "key_format": "camel",
  "operations": [
    {"op": "search", "entity": "movie",
     "args": {"_where": {"release_year": {"_gt": 1990}}, "_limit": 10},
     "selections": {"title": null, "genres": {"name": null}}}
  ]
}
```

```json
{"results": [
  {"ok": true, "data": [{"xid": "…", "title": "Dune", "genres": [{"xid": "…", "name": "Sci-Fi"}]}]}
]}
```

`results[i]` answers `operations[i]`. A failed operation does not stop the
others; it answers `{"ok": false, "error": {"code": "…", "message": "…"}}`.

Every operation runs under the caller's access rules — the entities, rows and
attributes it may not see are not there ([ACCESS.md](ACCESS.md)). A trusted
backend can run a request as one of its users with `"acting_as": "<user xid>"`
([OAUTH.md](OAUTH.md#acting-for-a-user)).

## Request

| Field | |
|---|---|
| `operations` | Required, non-empty array. |
| `key_format` | Casing of the keys in the result: `snake`, `kebab`, `camel` or `pascal`. Omitted: `snake`, as stored. Keys you send are accepted in any casing. |
| `acting_as` | Run as this user. Trusted confidential clients only. |

Bodies may be `application/json`, `application/transit+json` or
`application/edn`.

Entity names accept spaces, `_` or `-` in any letter case: `"Movie Actor"`,
`movie_actor` and `movie-actor` are the same entity. camelCase is not split —
`movieActor` is not. Attribute keys in
the result are the model's names in snake case (`"Birth Date"` → `birth_date`).

## Values

| Type | On the wire |
|---|---|
| `xid` | a string of 22 letters and digits, URL-safe |
| `string`, `enum` | a string |
| `int`, `float` | a number |
| `boolean` | `true` / `false` |
| `timestamp` | an ISO 8601 string in UTC to the second, `"2026-10-04T14:30:00Z"`. Send any RFC 3339 date-time; it is stored and returned in UTC. |
| `json`, `encrypted` | any JSON value. An `encrypted` attribute arrives and leaves as plain JSON; only the database holds ciphertext. |
| `hashed` | send the plain string; a hash is stored and the plain value is never returned |
| `transit` | a string holding Transit-encoded data |
| `user`, `group`, `role` | a reference to one IAM record, written and selected like a relation: `"owner": {"xid": "…"}` |

## Reading

### `search`

Rows matching `args`, shaped by `selections`.

```json
{"op": "search", "entity": "movie",
 "args": {"_where": {"title": {"_ilike": "%star%"}},
          "_order_by": [["release_year", "desc"]],
          "_limit": 20, "_offset": 0},
 "selections": {"title": null, "release_year": null}}
```

### `get`

One row by a unique value. `args` is a flat map, not `_where`:

```json
{"op": "get", "entity": "movie", "args": {"xid": "…"}, "selections": {"title": null}}
```

### Selections

List what you want back; nothing else is returned. A scalar is `null` (or
`true`); a relation is an object with its own selections, nested as deep as the
model goes. The record's `xid` is always included.

```json
{"title": null, "actors": {"name": null, "movies": {"title": null}}}
```

A relation with no matching rows is left out of the result rather than returned
as `[]`.

To filter, order or page a relation, or to select the same relation twice, use
the full form — a list of configs, each with `selections`, optional `args` and
an `alias`:

```json
{"email": [
  {"alias": "work", "args": {"_where": {"kind": {"_eq": "work"}}}, "selections": {"address": null}},
  {"alias": "home", "args": {"_where": {"kind": {"_eq": "home"}}}, "selections": {"address": null}}
]}
```

A relation never drops its parent: a movie with no matching actors is still
returned. Add `"_join": "inner"` to a relation's `args` to return only the
parents that have a match.

### Filters and modifiers

| Key | |
|---|---|
| `_where` | `{field: {operator: value}}` |
| `_and`, `_or` | a list of `_where` maps |
| `_not` | a `_where` map, negated |
| `_maybe` | a `_where` map on a relation that does not drop parents |
| `_order_by` | a list of `[field, "asc"\|"desc"]` pairs — always a list, so the order survives |
| `_limit`, `_offset` | paging |
| `_distinct` | a list of fields |
| `_join` | `"inner"` on a relation, see above |

Operators: `_eq`, `_neq`, `_lt`, `_le`, `_gt`, `_ge`, `_in`, `_not_in`,
`_like`, `_ilike` (case-insensitive), `_is_null`.

### Counts and aggregates

`_count` and `_agg` compute over a relation in the database, without sending
the related rows. They are easiest to write in [XSQL](XSQL.md#counts-and-aggregates):

```json
{"title": "Dune", "_count": {"actors": 7},
 "_agg": {"ratings": {"value": {"avg": 4.3}}}}
```

### Trees

For an entity related to itself (`tree` cardinality), `get-tree` returns a root
and everything below it, and `search-tree` returns the matches with every
ancestor. `on` is the label of the relation to follow. Rows come back as a
flat list; the SDKs nest them for you.

```json
{"op": "get-tree", "entity": "category", "on": "parent", "root": "<xid>",
 "selections": {"name": null}}
```

### `sql-template`

Analytical SQL that still runs under access rules. Name entity tables in
braces — `{movie}`, `{user_rating.value}` — and pass typed parameters as
`?name:type`:

```json
{"op": "sql-template",
 "template": "SELECT count(*) AS n FROM {user_rating} WHERE value >= ?min:int",
 "params": {"min": 4}}
```

A bare table name (`FROM movie`) is refused, because it would bypass access
rules.

### XSQL

The same reads written as a query document — `{"op": "xsql", "xsql": "…"}`.
See [XSQL.md](XSQL.md).

## Writing

Records are identified by `xid`. Send one to update that record, or mint one on
the client to create a record you can reference in the same batch.

| Op | |
|---|---|
| `stack` | Create or update, and **add** the relations you send. Relations already on the record stay. |
| `sync` | Create or update, and make each relation **you send** exactly what you send — members not listed are unlinked. Relations you leave out are not touched. |
| `slice` | Unlink the relations named in `selections` from the records matching `args`. |
| `delete` | Delete by identity: a unique-key map (one record) or a list of xids (up to 1000). |
| `purge` | Delete everything a search with the same `args` and `selections` would return. |

```json
{"op": "stack", "entity": "user_rating",
 "data": {"value": 5, "movie": {"xid": "…"}}}
```

`data` may be one record or a list. Nested records in `data` are created or
updated and linked in the same transaction.

`stack` and `sync` differ only in what happens to the relations you send:

```json
{"op": "sync", "entity": "movie",
 "data": {"xid": "…", "genres": [{"xid": "…"}, {"xid": "…"}]}}
```

After this `sync` the movie has exactly these two genres; any other it had is
unlinked. The same `data` under `stack` adds the two and keeps the rest. A
relation absent from `data` is untouched by both.

A one-to-one target has one owner. Linking it to a record moves it there:
the record that held it is unlinked in the same write, under `stack` and
`sync` alike. Taking it is a change to that previous owner, so your access
rules must let you see and write it. One write that gives a target two
owners is refused.

`delete` names records by identity, never by filter:

```json
{"op": "delete", "entity": "movie", "args": {"xid": "…"}}
```

`args` is a map of one unique key (one record) or a list of up to 1000 xids.
For "everything matching", use `purge` with the `args` of a search.

In one request, writes run first, in order, then the reads — so a request can
write and read the result back.

## Errors

Branch on `error.code`; messages may change.

| Code | |
|---|---|
| `UNAUTHORIZED` | No or invalid token (HTTP 401). |
| `FORBIDDEN` | Missing scope for this operation. |
| `ENTITY_FORBIDDEN`, `ATTRIBUTE_FORBIDDEN` | Your roles may not touch this entity, or write this attribute ([ACCESS.md](ACCESS.md)). |
| `UNKNOWN_ENTITY`, `UNKNOWN_ATTRIBUTE` | Not in the model, or not visible to you. Carries a `hint` when a close name exists. |
| `UNKNOWN_OPERATOR`, `BAD_ARGS_SHAPE` | A typo in `args`. |
| `UNIQUE_VIOLATION`, `NOT_NULL_VIOLATION`, `FK_VIOLATION`, `CHECK_VIOLATION`, `TYPE_CAST_FAILURE` | The database refused the write. |
| `DELETE_FORBIDDEN` | The record exists but your access rules hide it. |
| `ROW_FORBIDDEN` | The write changes a record your access rules let you see but not write. |
| `SLOT_OCCUPIED` | The link you are replacing, or the one-to-one target you are taking, belongs to a record your access rules hide. |
| `BAD_DATA_SHAPE` | `data` contradicts the model, e.g. one write gives a one-to-one target two owners. |
| `XSQL_PARSE_ERROR` | With line and column. |
| `TIMEOUT` | The statement was cancelled. |
| `INTERNAL_ERROR` | Details are in the server log only. |

## Live data

`GET /data/events` is a server-sent event stream; `POST
/data/subscription/set` says which entities and records it should carry. Events
say what changed — inserted, updated, deleted, linked, unlinked — and SDKs use
them to re-run your query, so every refresh is checked against access rules
again. In practice you use `watch` in an SDK rather than the stream directly.
The server closes each stream after
`SYNTHIGY_SERVER_SSE_MAX_LIFETIME_MS`; SDKs reconnect on their own.

## History

`POST /history` reads the audit history of a record: its state at a point in
time, the changes between two points, and who made them. See
[DATALINE.md](DATALINE.md#history).
