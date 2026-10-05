# Portal and console

Synthigy has two web surfaces for two kinds of people.

| | Portal | Console |
|---|---|---|
| For | whoever runs the instance | users and administrators of the instance |
| Address | `http://127.0.0.1:7888` — this machine only | `/console` on the engine, e.g. `https://id.example.com/console` |
| Sign-in | none: being on the machine is the access | a Synthigy account |
| Served by | the `synthigy` CLI | the engine |

## Portal

Started by `synthigy up` and opened with `synthigy console`. It binds to
127.0.0.1 only; to reach a remote instance, forward the port:
`ssh -L 7888:127.0.0.1:7888 server`.

**First run** is a setup page: pick a bundle, connect the database, choose
where audit and logs go, set the public URL, then **Launch**. Synthigy never
creates the database itself — the page shows the SQL to create it. After the
engine boots, the page asks for the first superuser.

After that it shows the running instance:

- **Health** — dataset, IAM, OAuth, encryption and Dataline, with what is wrong
  when something is.
- **Database** — what the database reports about itself: version, size,
  connections in use.
- **Dataline** — where history, logs and traffic go, and their health
  ([DATALINE.md](DATALINE.md)).
- **Encryption** — custody, master key rotation and rewrap
  ([ENCRYPTION.md](ENCRYPTION.md)).
- **Engine** — the running version, the release channel (Stable, Nightly or a
  repository of your own), upgrades, restart and the `full`/`bare` profile.
  For a local jar it shows the version the jar reports and its modules
  ([EMBEDDED.md](EMBEDDED.md)).
- **Access** — the public URL and the redirect URLs of the Synthigy tools.
- **Superusers** — add and remove them. This works when nobody can sign in to
  the console, which makes it the recovery path.
- **Effective configuration** — every setting, its value and whether it came
  from `.env` or the shell. Edits are saved to `.env` and the engine restarts.
- **Engine log** — the engine's log, live, with filters.

A database in the legacy format is detected here and converted by a guided migration.

## Console

Signed in with a Synthigy account. What you see depends on your roles:

- **Your account** — profile and password, active sessions, linked sign-in
  methods.
- **Administration** — Users, Groups, Roles, Apps, APIs, Connectors and
  Identity Providers, each shown only to roles that may manage it
  ([ACCESS.md](ACCESS.md), [OAUTH.md](OAUTH.md)).
- **System**, for superusers and members of a role named `System Operator` —
  overview, topology of the running modules, data
  encryption keys and login page bundles.
- **Tools** — **Data modeling** ([MODELING.md](MODELING.md)), the **Data
  console** for running queries and XSQL against the live model, and the **Log
  cockpit**.
