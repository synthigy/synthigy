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

## Relations: left and inner

`->relation` keeps every parent — a movie with no genres is still returned and
simply has no `genres` key. `-relation` keeps only the parents that have a
match, so the relation scopes its parent.

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

`_count` and `_agg` compute in the database — no related rows are sent.

```
movie (limit 20)
  title
  _count
    ratings:movie_ratings
    actors:actors
  _agg
    ratings:movie_ratings
      value: avg
```

```json
{"title": "…", "_count": {"ratings": 42, "actors": 7},
 "_agg": {"ratings": {"value": {"avg": 4.3}}}}
```

`ratings:movie_ratings` aliases the relation `movie_ratings` as `ratings`.
Aggregate functions: `avg`, `sum`, `min`, `max`.

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

## Editor support

The XSQL linter and language server check queries against your model offline.
They read `xsql/schema.json`, which `synthigy schema pull` writes from the
server the profile is connected to ([CLI.md](CLI.md)). `synthigy schema check`
tells you when it is out of date.
