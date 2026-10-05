# Getting started

From nothing to a deployed model and your first query.

## 1. Install the CLI

```bash
curl -fsSL https://raw.githubusercontent.com/synthigy/tooling/main/install.sh | sh
```

Windows:

```powershell
irm https://raw.githubusercontent.com/synthigy/tooling/main/install.ps1 | iex
```

## 2. Start an instance

```bash
mkdir my-app && cd my-app
synthigy env init        # this folder gets its own instance in .synthigy/
synthigy up
```

The first `up` downloads a Java runtime and the engine into `~/.synthigy` and
starts the **portal**; open `http://127.0.0.1:7888`. Pick a bundle —
`sqlite` needs nothing else; `postgres` needs a database you created — and
press **Launch**. When the engine is up, the portal asks for the first
superuser.

`synthigy up --bundle sqlite` skips the choice.

The engine now serves on `http://localhost:7887`, with the **console** at
`http://localhost:7887/console`. Sign in there as the superuser. The portal
runs the instance; the console administers it ([CONSOLE.md](CONSOLE.md)).

## 3. Model your data

In the console open **Tools → Data modeling**. Create a dataset, `Movies`,
with one entity, **Movie**, and two attributes: **Title** (`string`,
mandatory) and **Release year** (`int`). Leave access control off for now: an
entity with it off is open to every signed-in caller
([ACCESS.md](ACCESS.md)). Press **Deploy**; Synthigy creates the table.

In the API the names are snake case: entity `movie`, attributes `title` and
`release_year`. Relations, the other types and versions: [MODELING.md](MODELING.md).

## 4. Let an application in

```bash
synthigy iam add-client "My App" --id my-app --type confidential \
  --grant client_credentials --role "Dataset Explorer" --api Synthigy --local
synthigy connect http://localhost:7887 --client-id my-app
```

`add-client` prints the secret once; `connect` asks for it and saves the
identity in `.synthigy/`. **Dataset Explorer** lets the application read the
model, which code generation needs. What data it may read and write is decided
by your entities' access rules — and an entity with access control off is open
to every signed-in client ([ACCESS.md](ACCESS.md)).

## 5. Write, then read

One request may carry several operations. Writes run before reads, so this
creates a movie and reads it back:

```bash
curl -s http://localhost:7887/data \
  -H "Authorization: Bearer $(synthigy token)" \
  -H 'Content-Type: application/json' \
  -d '{"operations": [
        {"op": "stack", "entity": "movie",
         "data": {"title": "Dune", "release_year": 2021}},
        {"op": "search", "entity": "movie", "args": {"_limit": 5},
         "selections": {"title": null, "release_year": null}}]}'
```

The second result is the search:

```json
{"ok": true, "data": [{"xid": "…", "title": "Dune", "release_year": 2021}]}
```

Or from code, with an SDK, run under `synthigy exec` so it finds the server and
the identity on its own ([SDKS.md](SDKS.md)):

```bash
npm install @synthigy/sdk
synthigy exec -- node app.js
```

## Next

- [DATA.md](DATA.md) — every operation of the data API
- [XSQL.md](XSQL.md) — queries as documents, typed code from them
- [SIGN_IN.md](SIGN_IN.md) — signing users in to your application
- [OPERATIONS.md](OPERATIONS.md) — running it for real: service, upgrades,
  backups
