# Errors

A failed operation answers `{"ok": false, "error": {…}}`. Branch on `code`; the
other fields say what to change.

| Field | |
|---|---|
| `code` | Stable, for your code to branch on: the codes of [the data API](DATA.md#errors) and of [files](FILES.md#errors). |
| `message` | What was refused and what to send instead. The wording may change. |
| `hint` | The fix, often with the right shape. |
| `rule` | The rule that refused. |
| `doc` | The section below on that rule, e.g. `errors/relation_data`, read at `GET /docs/errors/relation_data`. |
| `path` | Where in the operation, e.g. `["data", "file"]`. |
| `entity`, `attribute`, `relation` | The names the refusal is about. |

## Writing

### relation_data

`BAD_DATA_SHAPE`. A to-one relation takes one record or `null`; a to-many
relation takes an array of records. A record is an object, and `{"xid": "…"}`
alone links an existing one.

```json
{"op": "stack", "entity": "document",
 "data": {"name": "Contract", "file": {"xid": "…"}, "attachments": [{"xid": "…"}, {"xid": "…"}]}}
```

A bare xid (`"file": "…"`), one record for a to-many (`"attachments": {"xid": "…"}`)
and strings in its array are refused: `relation` names the field and `hint`
shows its shape. See [Relation data](DATA.md#relation-data).

### files_read_only

`ENGINE_OWNED`. Files, folders and download links are created, changed and
deleted through `/files`. On `/data` you read them, and link your records to
them with the xid alone:

```json
{"op": "stack", "entity": "document", "data": {"xid": "…", "file": {"xid": "<file xid>"}}}
```

The message says what was refused:

| Sent to `/data` | Do instead |
|---|---|
| A `File` or `Folder` without an `xid`, or with one no file has | Upload with `POST /files?name=…` (a folder: `POST /files/folders`), then link the `xid` it returns |
| A `File` with fields besides `xid` (`name`, `content_type`, …) | Send only the `xid`; change the file with `PATCH /files/<xid>` |
| A file's `folder`, a folder's `parent` | `PATCH /files/<xid> {"folder": "<folder xid>"}`, `PATCH /files/folders/<xid> {"parent": "<folder xid>"}` |
| A folder's access lists | `POST /files/folders/<xid>/share`, or `/unshare` |
| `delete` or `purge` of files, folders or links | `DELETE /files/<xid>`; to unlink a record from a file, write the relation as `null` |

A link to a file that doesn't exist, or that you can't read, answers
`REF_NOT_FOUND` instead: check the xid, or upload the file first. See
[Linking records to files](FILES.md#linking-records-to-files).

### stamp

`ATTRIBUTE_FORBIDDEN`. A stamp attribute (who created or changed a record, and
when) is set by the engine, and this one refuses a value from the client.
Leave it out of `data` and read it back after the write. See
[Entities and attributes](MODELING.md#entities-and-attributes).

### delete_identity

`UNIQUE_KEY_REQUIRED`. `delete` names one record by a unique key, and these
`args` hold none: `hint` lists the entity's unique keys. For several records,
give a list of up to 1000 xids; for every record that matches a filter, use
`purge`.

```json
{"op": "delete", "entity": "movie", "args": {"xid": "…"}}
```

### slice_column

`INVALID_SLICE`. `slice` cuts the links of a to-many relation. A reference
attribute or a tree's parent is a column of the record: set it to `null` with
`sync`.

```json
{"op": "sync", "entity": "task", "data": {"xid": "…", "owner": null}}
```

## Reading and filtering

### reference_by_xid

`INVALID_FILTER`. A reference (an attribute of type `user`, `group` or `role`,
or a tree's parent) is compared by the referenced record's xid:

```json
{"op": "search", "entity": "task", "args": {"owner": {"_eq": "<user xid>"}}}
```

```xsql
task (owner.xid = ?owner:string)
  title
```

To filter by the referenced record's fields, nest them:
`{"owner": {"name": {"_eq": "Alice"}}}`. `_like`, `_gt` or a number against the
reference itself is refused.

### args_modifier

`BAD_ARGS_SHAPE`. A key in `args` that starts with `_` is a modifier, and this
one doesn't exist: `supported` lists the ones that do.

### args_value

`BAD_ARGS_SHAPE`. A modifier with the wrong kind of value: `_limit` and
`_offset` take a whole number, `_and` and `_or` a list of maps, `_not` and
`_maybe` a map, `_distinct` a list of attributes, `_join` `"inner"`, `"left"`
or `"none"`.

```json
{"op": "search", "entity": "task",
 "args": {"_or": [{"status": {"_eq": "open"}}, {"due": {"_lt": "2026-12-01"}}], "_limit": 20}}
```

### order_by_entry

`BAD_ORDER_BY_SHAPE`. `_order_by` takes a map of attribute to direction, or a
list of pairs when the order matters:

```json
{"op": "search", "entity": "movie", "args": {"_order_by": [["release_year", "desc"], ["title", "asc"]]}}
```

### args_where

`BAD_ARGS_SHAPE`. Field conditions go directly in `args`; `_where` was removed.

```json
{"op": "search", "entity": "movie", "args": {"title": {"_ilike": "%dune%"}, "_limit": 10}}
```

See [Filters and modifiers](DATA.md#filters-and-modifiers).

### get_args

`BAD_ARGS_SHAPE`. `get` finds one record by a unique key, so its `args` are
plain values, `{"xid": "…"}` or `{"name": "alice"}`. For conditions, use
`search`. See [`get`](DATA.md#get).

### filter_through_cut

`INVALID_FILTER`. In `slice` and `purge`, root `args` pick the parent records.
They can't also filter through the relation the same call cuts or deletes: put
that condition on the relation.

```json
{"op": "slice", "entity": "movie", "args": {"xid": "…"},
 "selections": {"tags": [{"args": {"name": {"_eq": "red"}}}]}}
```

See [Writing](DATA.md#writing).

## Operations

### operation_shape

`BAD_OP`, `INVALID_SELECTIONS` or `XSQL_PARSE_ERROR`. The operation itself is
malformed: no `op`, `selections` that isn't a map of fields, or an `xsql`
operation without its document or with `entity`, `selections` or `args`
beside it. An XSQL document travels in its own operation, which runs the
document's first operation with `params`:

```json
{"op": "xsql", "xsql": "@search recent\nmovie (release_year > ?since:int)\n  title", "params": {"since": 2020}}
```

See [XSQL](DATA.md#xsql).
