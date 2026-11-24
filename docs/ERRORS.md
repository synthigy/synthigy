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

### linked_record

`REF_NOT_FOUND`. A record with only its `xid`, or only a unique key such as
`{"name": "…"}`, links an existing record, and no record you can read matches
it. `relation` names the field it was linked under; a record at the root of
the write, or a tree parent, carries none. A record you can't read answers the
same as one that doesn't exist.

```json
{"op": "stack", "entity": "task", "data": {"title": "Review", "project": {"xid": "<project xid>"}}}
```

Link an xid you read from that entity, or create the record first and link
the xid it returns. The engine answers this when it may not create the record:
your roles can't create records of that entity, or its row rules limit which
ones you see, and always for a file or folder. Otherwise a record that doesn't
exist is created with that key and nothing else, so a write can link a record
that arrives later. A record with attributes besides its `xid` or key writes
that record, updated or created, and is refused like any other write your roles
don't allow, never with `REF_NOT_FOUND` (a file or folder:
[files_read_only](#files_read_only)). The relations sent with a record don't
count: with only its `xid` or key it is still a link. See
[Relation data](DATA.md#relation-data).

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
`REF_NOT_FOUND` instead ([linked_record](#linked_record)): check the xid, or
upload the file first. See
[Linking records to files](FILES.md#linking-records-to-files).


### apps_status

`ENGINE_OWNED`. An App Package's `Status` and `Last Error` are written by the
engine alone: it sets them while it starts the package and when the start
succeeds or fails. To deploy, set `Active` and read `Status` back:

```json
{"op": "stack", "entity": "app_package", "data": {"xid": "…", "active": true}}
```

`Status` goes `starting`, then `running`, or `failed` with the reason in
`Last Error`. A failed start leaves the package that was running before.

### apps_hostname

An App's hostnames:

| Code | Refused | Do instead |
|---|---|---|
| `HOSTNAME_INVALID` | A `name` that is not one lowercase DNS name: letters, digits, dots and dashes, no scheme, port or path | Send `app.example.com`, not `https://App.example.com/` |
| `ROW_FORBIDDEN` | Giving a hostname to an App you may not change | Ask someone in the App's owner group, or an admin |

### apps_sensitive

`SENSITIVE_STAYS`. A sensitive App Variable stays sensitive: setting
`Sensitive` back to `false` is refused, because its value has already been
hidden from everyone but the App. Set a new `Value`, or delete the variable and
create a plain one. Reads and history show a sensitive `Value` as `null` to
everyone except the App's own service user.

### apps_volume

An App Volume:

| Code | Refused | Do instead |
|---|---|---|
| `ENGINE_OWNED` | Writing `State`, `Used MB`, `Last Error` or `Delete After` | Set `Size MB` and `Mount Path`; the engine reports the rest |
| `VOLUME_REFUSED` | Sizes that add up to more than the App's `Volume Limit MB`, or an App with no limit | Shrink a volume, or raise the limit in the App's configuration |
| `VOLUME_REFUSED` | Two volumes at one `Mount Path` | Give each its own path |
| `VOLUME_REFUSED` | A path that is not absolute, has `.` or `..`, or sits under a system directory (`/etc`, `/usr`, …) | Use a path of your own, such as `/data` (the default) or `/srv/uploads` |

A resize is checked together with the App's other volumes.

### stamp

`ATTRIBUTE_FORBIDDEN`. A stamp attribute (who created or changed a record, and
when) is set by the engine, and this one refuses a value from the client.
Leave it out of `data` and read it back after the write. See
[Entities and attributes](MODELING.md#entities-and-attributes).

### delete_identity

`UNIQUE_KEY_REQUIRED`. `delete` names one record by a unique key, and this
`data` holds none: `hint` lists the entity's unique keys. For several records,
give a list of up to 1000 xids; for every record that matches a filter, use
`purge`.

```json
{"op": "delete", "entity": "movie", "data": {"xid": "…"}}
```

### delete_data

`BAD_DATA_SHAPE` or `DELETE_BATCH_LIMIT`. `delete` takes its records in
`data`, never in `args`: one record by a unique key, `{"xid": "…"}`, or a list
of xid strings.

```json
{"op": "delete", "entity": "movie", "data": ["<movie xid>", "<movie xid>"]}
```

`data` that is missing, or neither a map nor a list, is refused with
`BAD_DATA_SHAPE`, and so is a list holding anything but xid strings, such as
`{"xid": "…"}` records. More than 1000 distinct xids answer
`DELETE_BATCH_LIMIT`, with `limit`: for more, or for every record a filter
matches, use `purge` with `{"args": {"xid": {"_in": […]}}}`.

### slice_column

`INVALID_SLICE`. `slice` cuts the links of a to-many relation. A reference
attribute or a tree's parent is a column of the record: set it to `null` with
`sync`.

```json
{"op": "sync", "entity": "task", "data": {"xid": "…", "owner": null}}
```

### join_none_slice

`INVALID_JOIN`. A relation in `slice` with `"_join": "none"`, which keeps the
records that have no link: `slice` cuts links, so it would have none to cut.
`relation` names it. Leave `_join` out and narrow the links that go with the
relation's own `args`; in an XSQL `@slice`, write `->tags`, not `!tags`.

### record_lock

`RECORD_LOCKED`. Another user holds a lock on a record whose attributes this
write changes; the whole write is refused. Links, delete and purge are never
refused by a lock. `xids` names the records and
`details` gives each one's `holder` and `until`. Wait for the lock to expire,
ask the holder to `release` it, or, with scope `data:unlock`, release it
yourself. Not retryable: unlike `LOCKED`, which is a busy database, it lasts
until the lock is released or expires.

The other refusals of this rule: `LOCK_NOT_LOCKABLE`, `claim` or `release` on
an entity without a Lock attribute; `LOCK_LIMIT_REQUIRED`, `claim` without a
limit; `LOCK_LIMIT_TOO_LARGE`, a `claim` limit above what one statement can bind
(`max` gives it, about 32 000 on SQLite and 65 000 on Postgres); `LOCK_DISTINCT`, `claim` with `_distinct`; `LOCK_NO_PRINCIPAL`, `claim`
with no signed-in user to hold the lock; `LOCK_MODEL_INVALID`, a deploy whose
lock attributes break a rule (one Lock and one Lock expiry per entity, an expiry
needs a Lock, a TTL is a positive number of seconds).

```json
{"op": "claim", "entity": "job", "args": {"status": {"_eq": "queued"}, "_limit": 5},
 "selections": {"xid": null, "payload": null}}
```

## Database constraints

### unique

`UNIQUE_VIOLATION`. A unique attribute's value, or a unique group's values
together, already belong to another record: `details` names the `entity` and
the `attributes`. A write finds its record by `xid`, or without one by the
first unique key it carries in full; a clash on any other unique key is
refused, not written to the record that has the value. Send a value no other
record has, or write the record that holds it. The schema (`GET /schema`)
lists each entity's unique keys under `constraints.unique`.

A deploy that makes an attribute unique while stored records share a value is
refused the same way: the message counts them and `details.values` shows one.
Remove or merge the duplicates first, or make the attribute unique together
with another one. Deploying is the human's.

### required

`NOT_NULL_VIOLATION`. The database refused a row that leaves empty a column it
requires: `details` names the `entity` (its table) and the column
(`attributes`). No attribute is such a column, `mandatory` ones included. The
database requires a value only in a record's `xid` and in both ends of a
relation link, and the engine writes those, so this doesn't come from a value
missing in `data`: if it repeats, report it to whoever runs the instance.

### reference

`FK_VIOLATION`. The database refused a link to a record that doesn't exist.
`details` names the `entity` written and, when the write's `data` shows which,
the relation (`attributes`) and its `target_entity`, or several `candidates`.
A record linked by its xid alone is created when it doesn't exist, or refused
with `REF_NOT_FOUND` ([linked_record](#linked_record)), before the write
reaches the database. Read the linked record again, and link one that exists.

A deploy is refused the same way when stored records point at records that
don't exist: remove those references first. Deploying is the human's.

### check

`CHECK_VIOLATION`. The database refused the write with a check constraint. A
model creates none, so it was added to the database outside Synthigy; on
PostgreSQL `details.constraint` names it. Send a value it accepts, or ask
whoever runs the database.

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

### schema_attribute

`UNKNOWN_ATTRIBUTE`. A name in `selections` or `args` is neither an attribute
nor a relation of the entity. `entity` and `attribute` name them, `path` says
where (`["selections"]` or `["args"]`), and `hint`, when there is one, is the
closest name the entity has. Read the names from the schema (`GET /schema`).
A selected attribute your roles may not read is left out of the result, not
refused.

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

A read (search, get, the tree reads), slice or purge with `data` is refused:
they filter with `args` (`{"args": {"name": {"_eq": "…"}}}`); `data` is what
stack, sync and delete write. `argument` is `data`, and `hint` shows that
operation's shape. `deploy` needs its version's `dataset` with an id
(`argument` is `dataset`), and `describe`, which code generators send, its
`sources`.

### operation_arg

`MISSING_ENTITY`. The operation needs an `entity` and has none: `op` names it
and `argument` is `entity`. Every read and write does: `search`, `get`, the
tree reads, `stack`, `sync`, `delete`, `slice` and `purge`.

```json
{"op": "search", "entity": "movie", "selections": {"title": null}}
```

Entity names take spaces, `_` or `-` in any letter case. See
[Request](DATA.md#request).

## Modeling and deploying

### entity_name

`ENTITY_NAME_CONFLICT`. Each entity name maps to one table, and this deploy
gives two active entities the same one: their names match in snake case
(`Movie Actor` and `movie_actor`). `details` lists each `table` with the
entities that share it. A version that draws an entity again, instead of
bringing in the deployed one, hits this: bring it in
([One dataset per domain](MODELING.md#one-dataset-per-domain)). Otherwise
rename one of them. Deploying is the human's.

### field_key

`FIELD_KEY_CONFLICT`. An entity's attributes and its relation labels share
one set of field keys, and this deploy gives one key to two of them: an
attribute `Owner` and a relation labelled `owner`, or `Birth Date` and
`birth_date`. A relation gives each of its entities the label you traverse
from there, so the label on `User`'s side of your relation must not repeat a
`User` attribute such as `name`. `details` lists each entity and `field` with
the attributes and labels that claim it. Rename one of them. Deploying is the
human's.

### rls_guard

`RLS_GUARD_BROKEN`. A row rule guard this version declares would stop
working: it has no conditions, or one reads an attribute or relation the
deployed model would no longer have active. Such a guard fails closed, so the
deploy is refused. `details` lists each guard with its `entity`, id,
operations and the conditions left unresolved. Delete the guard, or keep what
it reads; a broken guard another dataset declares doesn't stop the deploy.
Deploying is the human's. See [Row rules](ACCESS.md#row-rules).

### system_entity

`SYSTEM_ENTITY_ALTERED`. The deploy would change a system entity or relation,
one a system dataset declares, such as `User`, `User Group` or `File`. A
version carries a brought-in entity as it was when brought in, and the last
deploy wins, so a copy that no longer matches the deployed one would change
it. A recall or destroy that would change one is refused the same way.
`details` lists each change: the entity or relation, the attribute, and the
`field` that differs or the `change`. Bring the entity in again as deployed
(Pull or Revert in the deploy view, `pull_entity` in Studio). A superuser's
deploy only warns. Deploying is the human's.

### shared_entity

`SHARED_ENTITY_OVERRIDE`, a warning only: it never stops a deploy. Datasets
are views of one model and the last deploy wins, whichever dataset it came
from. This deploy changes an entity or relation another dataset also
declares, so that dataset will see the change too. `details` lists each
affected `dataset` with its `changes`: the entity or relation, the
attribute, and the `field` with its `before` and `after` values, or the
`change`. An attribute going from `active` true to false is hidden for
everyone (its column and data stay). If the change is intended, deploy. To
keep the other dataset's version, bring the entity in again as deployed
(Pull in the deploy view, `pull_entity` in Studio) and redo only your own
edits. Deploying is the human's. System entities are reported under
[system_entity](#system_entity) instead.

### system_entity_rls

`SYSTEM_ENTITY_RLS`. Row rules on system entities come from the system
datasets. Most system entities are closed, `User` and `User Group` among them:
a version may not add a guard to one, or switch its row rules on. `File` is
open: any dataset may add guards to it, but not reuse an existing guard's id
for another guard, or switch its row rules on. `details` lists each entity
with the `guards` concerned. Put your guards on your own entities, related to
the system one; on `File`, give your guard a new id and leave the others as
they are. Deploying is the human's.

### system_mark

`SYSTEM_MARK`. Only a superuser marks a dataset, entity or relation as
system, and this version marks one the deployed model doesn't: an entity or a
relation, or the version's own model. `details` lists each, with
`{"model": true}` for the version. Leave the marks out; to use a system
entity, bring it in as deployed. Deploying is the human's.

### system_dataset

`SYSTEM_DATASET_ENGINE_OWNED`. System datasets, such as the engine's own
`Synthigy`, `OAuth` and `File System`, change only through engine upgrades: a
deploy into one, or a recall or destroy of one, is refused for anyone but a
superuser. To use their entities, such as `User` or `File`, bring them into a
dataset of your own and relate yours to them
([One dataset per domain](MODELING.md#one-dataset-per-domain)). Deploying is
the human's.

### version_name

`VERSION_NAME_TAKEN`. A version name is used once per dataset, and another
version of this dataset already has it: saving or deploying the version is
refused. `details` gives the `version` name, the `dataset` and whether the
version holding the name is `deployed`. Save under a name no version of the
dataset has; the same name in another dataset is free.

### deploy_immutable

`IMMUTABLE_VERSION_ALTERED`. A deployed version never changes. A deploy that
sends a deployed version with another name, model or dataset is refused, and
so is a write that renames one, changes its model, moves it to another
dataset, or leaves it out of its dataset's `versions` in a `sync`; `details`
says what changed. Save the change as a new version and deploy that one.
Deploying a deployed version unchanged is a rollback, not a change. Deploying
is the human's. See [Datasets and versions](MODELING.md#datasets-and-versions).

### engine_owned

`ENGINE_OWNED`, or `FORBIDDEN_OP` for a purge. The rows that describe a
deployed model change by deploying, recalling and destroying, never by a write
on `/data`:

| Sent to `/data` | Do instead |
|---|---|
| A `dataset_entity`, `dataset_entity_attribute` or `dataset_relation`: its fields, its links to versions and attributes, a `delete` | Deploy a version with the change; a role's grants reference these rows by `xid` alone |
| `deployed` or `deployed_on` on a `dataset_version` | Deploy the version, or recall it |
| `slice` of a dataset's `versions` | Recall the version, or destroy the dataset |
| `purge` that reaches a dataset, a version or one of these rows | `delete` the version to recall it, or the dataset to destroy it |

Deploying, recalling and destroying are the human's.

### model_lock

`MODEL_BUSY`. Deploys, recalls and destroys take turns, and another one is
running: `details` names it with its `action`, `dataset`, `version`, who
started it (`by`) and `since`. Send yours again when it finishes. Deploying,
recalling and destroying are the human's.
