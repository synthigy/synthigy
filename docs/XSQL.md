# XSQL

XSQL is Synthigy's query language. You write the shape you want back; the
server applies access rules and returns exactly that shape. It compiles to the
same operations as [the data API](DATA.md), so it can do nothing the caller is
not allowed to do.

Use it inline — every SDK has `query(text, params)`, and on the wire it is
`{"op": "xsql", "xsql": "…"}` — or keep queries in `.xsql` files and generate
typed code from them ([SDKS.md](SDKS.md#code-generation)).

## A query is a shape

```xsql
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

```xsql
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

```xsql
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
given). Under `or`, an optional parameter works when leaving it out empties every side:
`name ilike ?q? or email ilike ?q?` drops the whole `or` without `q`, so a search box needs
no default. An `or` that would lose only some of its sides (`active = true or name like ?q?`)
is refused, because the side left over would silently narrow the result.

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

```xsql
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
- `sum`, `avg`, `min`, `max` take `rel.field`, one hop. A second argument
  filters the related rows: `avg(movie_ratings.value, movie_ratings.value > 1)`.
  Every path in a filter starts with the operation's relation and can go on
  through its relations: `count(movie_ratings, movie_ratings.user.active = true)`,
  `anonymous: count(movie_ratings, movie_ratings.user is null)`. A filter only
  tests each related row and never repeats one, so a sum stays right through a
  to-many relation.
- Counts are never null (0 when there are no rows); the others are null when
  no row has a value. `min` / `max` keep the field's type.
- `order by` takes an alias (`order by votes desc`) or a related attribute
  (`order by studio.name`).
- Operations work under relations, at any level. A reference (a relation to
  one record) takes none.

A filter can reach through a relation: `user (roles.name = "Admin")` keeps the
users that have such a role, whether `roles` is selected or not. Predicates on
the same relation joined by `and` or `,` describe one related row:
`roles.name = "A" and roles.active = false` keeps users with an inactive role
named A. To match two different rows, use two aliased relation lines
(`-a:roles (name = "A")`, `-i:roles (active = false)`). `or` asks each side on
its own. References filter the same way, on their label:
`movie (modified_by.display_name = "Ana")`.

A path can also ask whether there is a related row at all: `user (roles is null)`
keeps users without a role, `roles is not null` the ones with one, at any depth
(`customer (payments.invoice is null)`: customers with a payment that has no
invoice). `roles.xid is null` means the same as `roles is null`.

A relation line needs at least one field, or a nested `-`/`!` line; with only
nested lines it returns each matching row's `xid`. To keep parents by their
related rows without returning them, filter instead (`roles.name = "Admin"`); to
keep parents without any, use `!roles`.

### Coming from SQL

| In SQL | In XSQL |
|---|---|
| `LEFT JOIN r … WHERE r.id IS NULL` | `r is null` in a filter, or a `!r` line |
| `WHERE EXISTS (… r.x = 1)` | `r.x = 1` |
| `count` / `sum` per parent with `GROUP BY` | an operation line, `total: sum(r.amount, <filter>)` |
| `HAVING`, a column compared with another column, `GROUP BY` a column | `@sql-template` |
| two conditions on two different related rows | two aliased relation lines |

## Operation files

An `.xsql` file holds named operations. A header names the verb and the
operation; the body follows.

```xsql
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

```xsql
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

```xsql
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

Placeholders: `{movie}` is the entity's table, `{movie.title}` a column, and
`{movie->genres.name}` follows a relation as a LEFT JOIN, through its junction
table when it has one. `-` joins INNER, `<-` RIGHT and `=>` FULL OUTER; chains go
on, `{movie->actors->awards.name}`. Access rules apply to every entity a
placeholder reaches.

The resolver writes the FROM and the joins from the placeholders, so a query
needs neither:

```xsql
@sql-template genre_counts
@namespace dashboard
@returns title:string, genres:int
SELECT {movie.title} AS title, count({movie->genres.name}) AS genres
GROUP BY {movie.title}
```

- A placeholder brings its own table alias: `FROM {movie} m` is broken SQL.
  Write `{movie.title}`, not `m.title`.
- A relation is not a column of the table. Reach it through a placeholder,
  `{movie->studio.name}`, never a guessed `movie.studio_id`.
- `{movie->genres}` without a field is the genres table alone; read its fields as
  `{movie->genres.name}`.
- `@returns` names the SELECT's columns, in any order.

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
