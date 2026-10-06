# Running Synthigy

## One instance, one folder

An instance is a folder: its `.env` (configuration and secrets, mode 0600),
pid files, logs and — with the `sqlite` bundle — its database files. The CLI
finds the instance the way git finds a repository: the nearest `.synthigy/`
above the current directory, else `~/.synthigy`. `SYNTHIGY_HOME` names one
explicitly.

That `.env` is the instance's **profile**: the CLI, the portal and the SDKs
all read it. `--env <file>` (or `SYNTHIGY_ENV_FILE`) runs a command under
another profile file, which is how one project folder talks to several
instances: `synthigy --env .synthigy/staging.env status`.

So several instances can live on one machine, one per project folder, each on
its own ports (`SYNTHIGY_SERVER_PORT`, `SYNTHIGY_PORTAL_PORT`). The JRE and
downloaded releases are shared in `~/.synthigy`.

```bash
synthigy env init      # create .synthigy/ here
synthigy up            # start (detaches; --join stays attached)
synthigy status        # what this instance is, from disk, no network
synthigy doctor        # every setting and where it came from, plus checks
synthigy logs -f       # engine process output: boot and crashes
synthigy down          # stop the engine and the portal
```

## Starting at boot

```bash
synthigy service install     # systemd user unit (Linux) or launchd agent (macOS)
synthigy service status
synthigy service uninstall
```

The unit runs `synthigy up --join` for this instance. No root is needed; on
Linux the CLI enables lingering so the service runs without anyone logged in.
Its output goes to the journal (`journalctl --user -u synthigy -f`), and
`synthigy logs` reads it from there. On a small cloud server, see
[AWS provisioning](AWS_PROVISIONING.md).

## Upgrades

Upgrade the engine from the portal's **Engine** panel, or pin a version with
`synthigy up --version vX.Y.Z`. The new release's schema patches run when it
boots; the panel lists what is deployed.

> Going back to an older release after a newer one has started on the
> database is not supported — the older engine does not understand the newer
> schema. Back up before upgrading, and restore the backup to go back.

The **release channel** is Stable by default. Nightly carries development
builds; a custom channel is any GitHub repository with the same release
layout.

The CLI upgrades itself with `synthigy upgrade`, and tells you when a newer
version exists.

## Backups

Back up together:

| | |
|---|---|
| Database | `pg_dump`, or `<home>/db/synthigy.db` |
| History | with Postgres it is in the same database; with SQLite, `<home>/db/audit.db` |
| `.env` | holds the master key under local custody ([ENCRYPTION.md](ENCRYPTION.md)) |

`logs.db` is optional. Copy SQLite files with the engine stopped, or online
with `sqlite3 synthigy.db ".backup backup.db"`.

Keep the master key apart from the database backups: one without the other is
useless to whoever finds it, and the database without its key is useless to
you too.

## Moving IAM configuration between instances

Roles, groups, users, OAuth clients and APIs export as JSON and import into
another instance — development to production, or into version control. Both
commands need a connection as a superuser.

```bash
synthigy connect https://dev.example.com
synthigy iam export role > roles.json
synthigy iam export client --id my-app > my-app.json

synthigy connect https://prod.example.com
synthigy iam import role roles.json --dry-run   # validate, write nothing
synthigy iam import role roles.json             # --mode sync (default) or stack
```

`sync` makes each imported record, its memberships and grants included,
exactly what the file says; `stack` adds to what is there and removes nothing.
Exports never contain credentials: user passwords and client secrets are left
out, so an imported client gets its secret with `iam add-client --secret` on
the existing client. OAuth signing keys and identity provider configuration
have no export.

## Air-gapped machines

```bash
synthigy pull --bundle postgres --version v0.2.12   # on a connected machine
```

copies the release into the cache. Move `~/.synthigy/modules` (and the JRE in
`~/.synthigy/jre`, or set `SYNTHIGY_JAVA`) to the target machine; `synthigy up`
then starts from the cache without network access. Or run a jar you built
yourself with `SYNTHIGY_JAR`.

## Corporate networks

The CLI and the engine follow `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`. Point
`SYNTHIGY_CA_BUNDLE` at a PEM file of your certificate authorities and both
trust them ([ENV.md](ENV.md#cli-and-network)).

## Containers

No image is published yet. The shape that works: the CLI is PID 1 and runs
`synthigy up --join`; the engine's JVM is the base image's (`SYNTHIGY_JAVA`),
so nothing is downloaded at start; the instance lives on a volume
(`SYNTHIGY_HOME=/data`); the engine binds every interface
(`SYNTHIGY_SERVER_HOST=0.0.0.0`) and port 7887 is published. With
`SYNTHIGY_BUNDLE` and the database settings in the environment, the engine
starts without the setup page. `synthigy pull` at build time bakes a bundle
into the image for hosts without network access.

The portal binds 127.0.0.1 inside the container and is not published. The
first start needs it, for the first superuser: run that start with
`--network host`, so the portal is at `127.0.0.1:7888` on the host. Later
starts do not need it.

## On the internet

The engine speaks plain HTTP; put it behind a reverse proxy that terminates
TLS, and set `SYNTHIGY_IAM_ROOT_URL` to the public URL. See
[PROXY.md](PROXY.md).
