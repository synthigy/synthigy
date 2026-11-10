# XSQL

XSQL is Synthigy's query language. You write the shape you want back; the
server applies access rules and returns exactly that shape. It compiles to the
same operations as [the data API](DATA.md), so it can do nothing the caller is
not allowed to do.

Use it inline — every SDK has `query(text, params)`, and on the wire it is
`{"op": "xsql", "xsql": "…"}` — or keep queries in `.xsql` files and generate
typed code from them ([SDKS.md](SDKS.md#code-generation)).

## A query is a shape

```
movie (release_year > ?since:int=1980, limit ?limit:int=20, order by release_year desc)
  xid
  title (ilike ?q:string="%")
  release_year
  ->genres
    name
```

- The **root** (`movie`) is an entity. Arguments in `(...)` filter, page and
  sort it: comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `ilike`,
  `in`, `not in`, `is null`, `is not null`) joined with `and` / `or`, and
  `limit`, `offset`, `order by`, `distinct`.
- Each **indented line** is a field to return. Nothing else comes back.
- A field can carry its own filter: `title (ilike ?q)`.
- `->genres` follows a **relation**; its fields are indented below it, as deep
  as the model goes.

### Layout

Keep a short argument list on one line. Once it carries more than one
condition, or paging and sorting besides a condition, give each part its own
line: conditions first, each further condition starting with `and` / `or`, then
`order by`, `limit` and `offset`, and the closing `)` on its own line.

```
user (
  name ilike ?q?
  and roles.xid in ?roles?[]
  and active = true,
  order by ?sort:order(name, roles)="name asc",
  limit ?limit:int=100,
  offset ?offset:int=0
)
  xid
  name
  roles: count(roles)
```

The same holds for a relation's arguments:

```
movie (xid = ?xid:string)
  title
  ->ratings (
    value >= 4
    and created_on > ?since:timestamp,
    order by created_on desc,
    limit 5
  )
    value
    review
```

Line breaks inside `(...)` don't change the query; the field lines below it
still follow indentation.

## Relations: left, inner and none

`->relation` keeps every parent — a movie with no genres is still returned and
simply has no `genres` key. `-relation` keeps only the parents that have a
match, so the relation scopes its parent. `!relation` keeps only the parents
with no match: `!genres (name = "Horror" or name = "Gore")` returns movies
that have neither genre. A `!relation` selects nothing, so its block holds
only filters (`name = "x"`) and nested `-`/`!` constraints. Only related rows
you can read count as a match for `-` and `!`.

## Parameters

`?name:type=default` — typed, with an optional default. Without a default the
parameter is required. `?name?:type` is optional: when the caller leaves it out, the
predicate that uses it is dropped (`title (ilike ?q?:string)` filters only when `q` is
given). An optional parameter is not allowed under `or`, where dropping one branch would
silently narrow the result.

Optional and default exclude each other: `?q?:string="%"` is an error. A default
means "filter with this value", optional means "don't filter at all". String defaults
are always double-quoted: `?q:string="%"`, not `'%'` or a bare `%`.

```
?since:int=1980
?q:string="%"
?xid:string
?ids:string[]          a list
?sort:order            a sort direction chosen by the caller
```

Types: `int`, `float`, `string`, `boolean`, `timestamp`, `uuid`, `order`.
Parameters are passed as a plain object, `{"since": 1990, "q": "%star%"}`.

## Counts and aggregates

Operations compute in the database — no related rows are sent. Each is a line
`alias: op(argument)`; the alias is required and is the result key.

```
movie (limit 20, order by votes desc)
  title
  votes: count(movie_ratings)
  cast: count(actors)
  rating: avg(movie_ratings.value)
  fans: count(movie_ratings.value >= 4)
  scores: count(distinct movie_ratings.value)
```

```json
{"title": "…", "votes": 42, "cast": 7, "rating": 4.3, "fans": 30, "scores": 5}
```

- `count(rel)` counts related rows, `count(<predicate>)` the ones that match,
  `count(distinct rel.field)` distinct values.
- `sum`, `avg`, `min`, `max` take `rel.field`. A second argument filters the
  related rows: `avg(movie_ratings.value, movie_ratings.value > 1)`. Every
  path in a filter starts with the operation's relation.
- Counts are never null (0 when there are no rows); the others are null when
  no row has a value. `min` / `max` keep the field's type.
- `order by` takes an alias (`order by votes desc`) or a related attribute
  (`order by studio.name`).
- Operations work under relations, at any level. A reference (a relation to
  one record) takes none.

A filter can reach through a relation: `user (roles.name = "Admin")` keeps the
users that have such a role, whether `roles` is selected or not. Each path
predicate asks on its own, so `roles.name = "A" and roles.active = false` can be
met by two different roles. References filter the same way, on their label:
`movie (modified_by.display_name = "Ana")`.

A relation line needs at least one field. To keep parents by their related rows,
filter instead (`roles.name = "Admin"`); to keep parents without any, use `!roles`.

## Operation files

An `.xsql` file holds named operations. A header names the verb and the
operation; the body follows.

```
@search list
movie (release_year > ?since:int=1980, limit ?limit:int=20)
  xid
  title

@get detail
movie (xid = ?xid:string)
  title
  ->actors
    name
```

| Header | |
|---|---|
| `@search <name>` | rows |
| `@get <name>` | one row |
| `@search-tree`, `@get-tree` | walk a self-relation ([trees](DATA.md#trees)) |
| `@sql-template <name>` | raw SQL, see below |
| `@purge <name>` | delete the rows the body matches |
| `@slice <name>` | unlink the relations the body names |
| `@sync`, `@stack`, `@delete` | writes; the body is a data parameter: `?records:movie[]` |
| `@batch <name>: a b c` | run several operations of the file in one request |
| `@description <text>` | documents the operation above it |
| `@namespace <name>` | groups the generated methods |
| `@returns name:type, …` | result columns of a sql-template |
| `@watch` | the generated method gets a live version; on a sql-template, list the entities whose changes re-run it |

```
@stack add_ratings
?records:user_rating[]

@delete remove_movies
?ids:movie[]

@batch dashboard: list detail
```

## SQL templates

For analytics a query shape cannot express, write SQL. Name entity tables in
braces — that is how access rules reach the query. A bare `FROM movie` is
refused.

```
@sql-template stats
@namespace dashboard
@returns total_movies:int, total_ratings:int, avg_rating:float?
WITH m AS (SELECT count(*) AS n FROM {movie}),
     r AS (SELECT count(*) AS n, avg({user_rating.value}) AS avg_value FROM {user_rating})
SELECT m.n AS total_movies, r.n AS total_ratings, r.avg_value AS avg_rating
FROM m, r
```

Use one CTE per entity rather than scalar subqueries, so each is checked on its
own. `float?` marks a column that may be null.

Text columns sort in human order (see [DATA.md](DATA.md#filters-and-modifiers)).
On SQLite, an expression over a text column — `lower(x)`, `coalesce(x, '')`,
`x || ''` — falls back to byte order; write `order by lower(x) collate human` to
keep it. PostgreSQL keeps the column's order through expressions.

A timestamp column selected as is — `{e.time}` or `{e.time} AS t` — comes back
as a timestamp on every database. On SQLite, an expression over it —
`max({e.time}) AS t` — comes back as UTC text, `2026-09-26 14:45:00.000`; parse
it on the client. PostgreSQL returns a timestamp either way.

## Editor support

The XSQL linter and language server check queries against your model offline.
They read `xsql/schema.json`, which `synthigy schema pull` writes from the
server the profile is connected to ([CLI.md](CLI.md)). `synthigy schema check`
tells you when it is out of date.
