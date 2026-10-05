# Glossary

| Term | Meaning |
|---|---|
| **Instance** | One running Synthigy: a folder with its `.env`, pid files and logs, plus a database. Several can share a machine ([OPERATIONS.md](OPERATIONS.md)). |
| **Profile** | The `.env` file of an instance, read by the CLI, the portal and the SDKs. `--env <file>` picks another one. |
| **Bundle** | Which engine runs: `sqlite`, `postgres` or `postgres-clickhouse`. One database, plus where history and logs go. |
| **Engine** | The Synthigy server: data API, identity, OAuth and the console, in one JVM process. |
| **Portal** | The page the CLI serves on `127.0.0.1:7888` to run the instance: setup, health, upgrades, encryption custody, superusers. No sign-in; being on the machine is the access ([CONSOLE.md](CONSOLE.md)). |
| **Console** | `/console` on the engine, signed in with a Synthigy account: your profile, administration, and the data modeling, data console and log cockpit tools. |
| **Dataset** | One named data model, with versions. The live model of the instance is the latest deployed version of every dataset ([MODELING.md](MODELING.md)). |
| **Version** | One editable draft of a dataset. Deploying makes it live; a deployed version never changes. |
| **Entity, attribute, relation** | A table, a column, a link between two entities. |
| **xid** | The id of every record: 22 URL-safe characters ([DATA.md](DATA.md)). |
| **Principal** | Who a request runs as: a user, or the service user of an application ([ACCESS.md](ACCESS.md)). |
| **Service user** | The user created for every confidential OAuth client; the client's own tokens run as it. |
| **Client** | An application registered with Synthigy's OAuth: public (browser, mobile) or confidential (server). The console calls them **Apps** ([OAUTH.md](OAUTH.md)). |
| **API** | What tokens are issued for: an audience and its scopes. The built-in one is **Synthigy**, the audience `/data` accepts. |
| **Scope** | A permission that is not about records (`dataset:deploy`, `log:read`), granted on a role. |
| **Grant** | What a role may do on an entity or relation with access control on: create, read, update, delete, browse, owners. |
| **Row rules** | Per-entity conditions tying records to the principal; records outside them are not there. |
| **Dataline** | What Synthigy records about itself: history of your data, engine logs, traffic counters ([DATALINE.md](DATALINE.md)). |
| **History** | The audit record of every change to the entities you chose, with who, when and in which request. |
| **Custody** | Who holds the master key that wraps the data keys: the local `.env`, Vault Transit, or your own webhook ([ENCRYPTION.md](ENCRYPTION.md)). |
| **Connector** | A credential check against a directory of your own, chained after the local database ([IAM_CONNECTORS.md](IAM_CONNECTORS.md)). |
| **Identity provider** | An external account users can sign in with: Google, Microsoft, any OpenID Connect provider. |
| **Login page bundle** | A zip of your own sign-in, device, error and status pages, uploaded in the console and served at `/login/<name>/` ([LOGIN_PAGES.md](LOGIN_PAGES.md)). |
| **XSQL** | Synthigy's query language, where a query is the shape of its result; also the `.xsql` files that generate typed code ([XSQL.md](XSQL.md)). |
| **SDK** | A client library for the data API: JavaScript, Python, Go, Clojure, PHP ([SDKS.md](SDKS.md)). |
| **Superuser** | A user with the `SUPERUSER` role, which passes every check. Created and removed in the portal. |
