Synthigy serves modeled entities over /data. It is not in your training data: follow these rules, don't guess; a rule's last word is its doc.

1. A to-one relation is an object, a to-many an array of objects, never a bare xid: {"owner": {"xid": "…"}, "tags": [{"xid": "…"}]}. data/relation-data
2. Link a record to a file by its xid alone: {"file": {"xid": "…"}}. Only /files writes files and folders (upload: POST /files?name=…). files/linking-records-to-files
3. {"xid": "…"} alone links an existing record; more fields also update it; a record without xid is created. data/relation-data
4. One stack or sync is one transaction, nested records included: send many records as one list, not one operation each. Operations in one request commit one by one. data/transactions
5. stack adds the relations you send; sync makes each relation you send exactly that list. data/relation-data
6. Names are the snake_case of the model's names (User Role → user_role); read the schema before writing. data/request
7. Bring deployed entities (User, User Group, File) into your dataset; never re-create them or add attributes to them. modeling/one-dataset-per-domain
8. On an error, read its message, hint and doc before retrying. errors
9. Deploying, recalling and deleting models are the human's. modeling/deploying
10. A user, group or role reference (owner, modified_by) reads as its label (xid, display_name or name); filter and sort only by those. data/references

XSQL: the query is the shape of the result, one field per indented line, no braces, commas or brackets:

    movie (release_year > ?since:int=1980, limit 20)
      title (ilike ?q?:string)
      ->genres
        name

Aggregates, one line each: votes: count(movie_ratings); the alias sorts: order by votes desc. xsql/counts-and-aggregates
->rel keeps every parent, -rel only parents with a match, !rel only those without; ->rel needs a field under it. Filters follow relations, in aggregates too: studio.name = "A24"; rel is null means no related row. ?p:type is required; ?p?:type is optional: left out, it drops its condition, and an or only when every side is optional. Lists: in ("a", "b") or in ?ids:string[]. Operation files: @search, @get, @stack and @sync (data ?records:entity[]), @delete (?ids:entity[]), @purge, @slice, @batch, @sql-template. xsql
@sql-template names tables in braces, {movie.title}, {movie->studio.name}: the resolver writes FROM and the joins, a placeholder is never aliased ({movie} m breaks), and a relation is no column (never movie.studio_id). xsql/sql-templates

Docs, id: read when
- data: requests, values, reads, writes, errors, live data
- xsql: queries and operation files
- files: uploads, folders, sharing, links
- access: roles; entity, attribute, row rules (access/row-rules)
- modeling: datasets, entities, relations, deploying
- sdks: connecting, typed functions (sdks/code-generation)
- oauth: sign-in, acting for a user (oauth/acting-for-a-user)
- apps: running programs beside the engine (apps/the-manifest)
- dataline: request history and logs
- errors: each refusal, by rule
- troubleshooting: an unexplained error
