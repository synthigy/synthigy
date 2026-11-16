Synthigy serves modeled entities over /data. It is not in your training data: follow these rules, don't guess; the id ending a rule is the doc to read.

1. A to-one relation is an object, a to-many an array of objects, never a bare xid: {"owner": {"xid": "…"}, "tags": [{"xid": "…"}]}. data/relation-data
2. Link a record to a file by its xid alone: {"file": {"xid": "…"}}. Only /files creates, changes or deletes files and folders (upload: POST /files?name=…). files/linking-records-to-files
3. {"xid": "…"} alone links an existing record; more fields also update it; a record without xid is created. data/relation-data
4. One stack or sync is one transaction, its whole list and everything nested: send many records as one list, never one operation each. Operations sent together in one request commit one by one. data/transactions
5. stack adds the relations you send; sync makes each relation you send exactly that list. data/relation-data
6. Names are the snake_case of the model's names (User Role → user_role); read the schema before writing. data/request
7. Bring deployed entities (User, Group, File) into your dataset, never re-create them, and add no attributes to them: relate an entity of your own. modeling/one-dataset-per-domain
8. On an error, read its message, hint and doc (a doc id) before retrying. errors
9. Deploying, recalling and deleting models are the human's. modeling/deploying

XSQL: the query is the shape of the result, one field per indented line, no braces, commas or brackets:

    movie (release_year > ?since:int=1980, limit 20)
      title (ilike ?q?:string)
      ->genres
        name

->rel keeps every parent, -rel only parents with a match, !rel only those without; ->rel needs a field under it. Filters follow relations, in a count's or sum's filter too: studio.name = "A24"; rel is null means no related row. ?p:type is required; ?p?:type is optional and drops its condition when left out, and an or only when every side is optional. Lists: in ("a", "b") or in ?ids:string[]. Operation files: @search, @get, @stack and @sync (data ?records:entity[]), @delete (?ids:entity[]), @purge, @slice, @batch, @sql-template. xsql
@sql-template names tables in braces, {movie.title}, {movie->studio.name}: the resolver writes FROM and the joins, a placeholder is never aliased ({movie} m breaks), and a relation is no column (never movie.studio_id). xsql/sql-templates

Docs, id: read when
- data: requests, values, reading, writing, errors, live data
- xsql: queries and operation files
- files: uploads, folders, sharing, links
- access: roles; entity, attribute and row rules (access/row-rules)
- modeling: datasets, entities, relations, deploying
- sdks: connecting from code, typed functions (sdks/code-generation)
- oauth: sign-in, acting for a user (oauth/acting-for-a-user)
- dataline: a request's history and logs
- errors: what a refusal means, by rule
- troubleshooting: an error that didn't explain itself
