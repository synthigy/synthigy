# SDKs

Five client SDKs talk to [the data API](DATA.md). They are thin — the server
does the work — MIT licensed, and each reads like its language.

| Language | Install | Repository |
|---|---|---|
| JavaScript / TypeScript | `npm install @synthigy/sdk` | [synthigy/js](https://github.com/synthigy/js) |
| Python | `pip install synthigy` | [synthigy/py](https://github.com/synthigy/py) |
| Go | `go get github.com/synthigy/go` | [synthigy/go](https://github.com/synthigy/go) |
| Clojure / ClojureScript | `com.synthigy/sdk` on Clojars | [synthigy/clj](https://github.com/synthigy/clj) |
| PHP | `composer require synthigy/sdk` | [synthigy/php](https://github.com/synthigy/php) |

All of them read, write, run XSQL and sql-templates, batch operations into one
request, act for users (`acting_as`) and sign users in through the browser. All
but PHP also keep queries live (`watch`). Each repository's README is that
SDK's manual.

A Clojure application can skip HTTP altogether and run the engine in-process —
see [EMBEDDED.md](EMBEDDED.md).

## Connecting

An application needs an address and an identity. For a service — an OAuth
client with a secret, created as in
[Getting started](GETTING_STARTED.md#4-let-an-application-in) — both are three
lines in `.synthigy/.env`, in the folder it runs from:

```bash
# .synthigy/.env
SYNTHIGY_ENDPOINT=https://engine.example.com
SYNTHIGY_CLIENT_ID=my-app
SYNTHIGY_CLIENT_SECRET=<secret>
```

Run the program under `synthigy exec`:

```bash
synthigy exec -- node app.js          # or python, go run, clj …
```

`exec` runs your program with that environment, and the SDK picks it up without
configuration (`connect()` with no arguments). A long-running program asks
`exec` for a fresh token when the old one expires.

A folder that only connects needs no `synthigy env init`, which sets up a local
instance. Its `.env` names an endpoint and no `SYNTHIGY_BUNDLE` or
`SYNTHIGY_JAR`, so no engine runs from it, and `synthigy status` shows the
engine as `remote`. Without `env init`, protect the secret yourself:
`chmod 600 .synthigy/.env`, and a `.synthigy/.gitignore` holding `*` keeps the
folder out of git.

`synthigy connect` is the checked way to write the same lines: it asks for the
secret and mints one token with it before saving anything. Without
`--client-id` it asks who is connecting; a person signs in through the browser,
and programs under `exec` then run as that person.

```bash
synthigy connect https://engine.example.com --client-id my-app   # the service
synthigy connect https://engine.example.com                       # you, or a service
```

`connect` saves to the nearest `.synthigy/`, here or above; with none, to
`~/.synthigy/.env`, so in a new project create the folder first, with a
`.gitignore` holding `*`. A profile holds one identity, and `connect` refuses
while it has one; `synthigy disconnect` removes it, secret included.

Outside `exec`, pass the endpoint and credentials to the SDK yourself, or set
`SYNTHIGY_ENDPOINT` and `SYNTHIGY_TOKEN` (from `synthigy token`; it is not
refreshed). The code generators also read `SYNTHIGY_CLIENT_ID` and
`SYNTHIGY_CLIENT_SECRET`.
`synthigy token` prints an access token for `curl` and scripts.

## Code generation

Write your queries as `.xsql` files ([XSQL.md](XSQL.md)) and generate typed
functions from them. The generators read your model from `xsql/schema.json`,
which they fetch from the server when it is missing.

| Language | Generate |
|---|---|
| JavaScript / TypeScript | `synthigy exec -- npx synthigy-gen xsql/ generated/` |
| Python | `synthigy exec -- python -m synthigy.codegen gen xsql/ --out ops.py` |
| Go | `//go:generate go run github.com/synthigy/go/cmd/synthigy-gen` |
| Clojure | `synthigy exec -- clj -X synthigy.gen/generate :ns-prefix myapp.ops` |
| PHP | `synthigy exec -- vendor/bin/synthigy-codegen gen xsql/ --out gen/ops.php` |

Code is generated for what the connected identity may see: connect as your
application's own client to get exactly its view of the model. Each generator
has a check mode for CI that fails when generated code is out of date, and
`synthigy schema check` tells you when the model on the server has changed.

`xsql/schema.json` is your whole data model. Commit it in a private
repository; keep it out of a public one.
