# Modeling data

Synthigy is model-first. You draw entities and relations, deploy the model, and
Synthigy creates the tables, keeps them migrated and serves them over `/data`
with access control on every entity, relation and attribute.

## Datasets and versions

A **dataset** is one named model — `Movies`, `CRM`, `Inventory`. It has
**versions**. You edit a draft version; deploying it makes it the live model of
that dataset.

- A deployed version never changes. To change the model, create a new version,
  edit it and deploy it.
- The live model of the instance is the latest deployed version of every
  dataset, merged. Datasets can share entities (several datasets can relate to
  `User`, for example).
- Deploying migrates the database: new tables and columns are created, changed
  ones are altered.

## Entities and attributes

An **entity** becomes a table. Every record gets an `xid` — a short, URL-safe
unique id. Per entity you can also record who created a record and when
(`created_by`, `created_on`) and who changed it last and when (`modified_by`,
`modified_on`).

An **attribute** becomes a column. Types:

| Type | Holds |
|---|---|
| `string` | text |
| `int`, `float` | numbers |
| `boolean` | true / false |
| `timestamp` | a point in time, always UTC |
| `enum` | one of a fixed list of values |
| `json` | any JSON document |
| `encrypted` | JSON encrypted at rest ([ENCRYPTION.md](ENCRYPTION.md)) |
| `hashed` | a one-way hash, e.g. a password — the plain value is never stored |
| `transit` | Clojure data in Transit encoding |
| `user`, `group`, `role` | a reference to an IAM user, group or role |

Each attribute has a constraint: optional (the default), `mandatory`
(NOT NULL), `unique`, or `unique+mandatory`. Unique attributes are arranged in
unique groups in the modeler: the combination of values in one group must be
unique.

Attributes can carry **classification** tags — `pii`, `phi`, `secret`,
`internal`. Values of `secret` attributes are withheld from the audit history
([DATALINE.md](DATALINE.md)); the other tags are labels for your own policies.

**Changing a type** in a new version converts the column. Conversions inside
one family (`int` → `float`, `enum` → `string`) are safe. Others, such as
`string` → `int`, need every stored value to convert and fail the deploy if
one does not. The modeler warns before you deploy. A reference attribute cannot
change type: deactivate it and add a new one.

**Removing an attribute** from a deployed model deactivates it. The column and
its data stay in the database; the attribute disappears from the API.

## Relations

A **relation** links two entities, with a label on each side. The labels are
the names you traverse in queries: a `movie` relation labelled `actors` on one
side and `movies` on the other is read as `movie → actors` and
`person → movies`.

| Cardinality | Meaning |
|---|---|
| `o2o` | one to one: linking a target to another record moves it there |
| `o2m` / `m2o` | one to many / many to one: a child has one parent; listing it under another moves it there |
| `m2m` | many to many |
| `tree` | an entity related to itself as parent and children |

## Deploying

From the **Data modeling** tool — in the console under Tools, or as a
web component in your own page (`<synthigy-data-modeling>`, see the
[tooling repository](https://github.com/synthigy/tooling)): draw the model,
then **Deploy**. The deploy view lists what will change before anything does.

From code: export the version from the modeler and pass the file to an SDK.
The server decodes the export, so the same file deploys anywhere:

```js
const ack = await client.deploy(fs.readFileSync("movies.json", "utf8"))
// ack.dataset is the dataset's xid
await client.destroy(ack.dataset) // removes every version, table and row
```

Deploying needs the `dataset:deploy` scope and destroying `dataset:delete` —
both come with the **Dataset Developer** role. **Dataset Modeler** edits the
datasets shared with it but cannot deploy.

## Next

- Read and write the data: [DATA.md](DATA.md)
- Queries as files, with generated typed code: [XSQL.md](XSQL.md)
- Who may see what: [ACCESS.md](ACCESS.md)
