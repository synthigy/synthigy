# Apps

An **App** is a program Synthigy runs beside the engine, on the same machine:
an OCI image, started as a confidential OAuth client of that engine. The
client is the App's identity; the image is what runs. You write the program,
describe it in a manifest, and `synthigy app deploy` packs, uploads and starts
it.

| Kind | Runs | Address |
|---|---|---|
| `service` | an HTTP server on `PORT` | `<name>.<engine host>`, or names an admin adds |
| `worker` | a long-running process | none |

Scheduled jobs and RPA are not Apps.

## The loop

1. **Once per machine:** `sudo synthigy service install --apps` (Linux only,
   needs Podman, passt and nftables). On a server installed with `--domain`,
   Apps are already set up. See [Running Synthigy](OPERATIONS.md).
2. **Once per App:** an admin creates its client
   ([Setting up an App](#setting-up-an-app)).
3. **In the project:** write `apps/<name>.app.json` ([The manifest](#the-manifest)).
4. **Every release:** `synthigy app deploy <name>`. It packs, uploads, starts the
   new version next to the running one, and switches over once it is healthy.
5. **Watch it:** `synthigy app logs <client-id>`.

Deploy goes to the engine the project is connected to (`synthigy status`), so
one manifest serves staging and production.

## Setting up an App

An admin, once per App:

```
synthigy iam add-client shop --id shop-app --type confidential --api Synthigy --role "App Program"
```

- `--type confidential`: only a confidential client gets a secret, and only it
  can run as an App.
- `--api Synthigy`: its tokens are accepted by `/data`.
- `--role "App Program"`: its service user may read its own packages,
  variables, hostnames and configuration. Add the roles the program needs for
  its own data, as for any service user ([ACCESS.md](ACCESS.md)).

The rest is in Studio's **Apps** tool, per App:

- **Deployers:** people (or a CI client's service user) who may deploy it. They
  also need the **App Deployer** role. A superuser can always deploy.
- **Variables:** settings and secrets the program gets at start
  ([What a running App gets](#what-a-running-app-gets)). A sensitive variable
  is readable only by the App itself, and stays sensitive.
- **Limits:** memory, CPU and volume space above the defaults (admins only).
- **Sandbox type** (admins only): `strict`, the default, or `relaxed`
  ([Sandbox type](#sandbox-type)).
- **Volumes:** persistent folders, within the volume limit.
- **Hostnames:** admins add custom domains; the first deploy writes the
  generated one.

## The project

```
myproject/
  .synthigy/            connection profile (gitignored)
  apps/
    shop.app.json       one manifest per App; the file name is its local name
  services/shop/        whatever the build needs
  dist/                 pack output (gitignored)
```

The project root is the first folder, walking up from the working directory,
that holds `apps/` and either `.synthigy/` or `.git`.

## The manifest

`apps/<name>.app.json`. `<name>` is lowercase letters, digits and `-`, at most
63. Unknown fields are an error.

```json
{
  "clientId": "shop-app",
  "clientName": "shop",
  "kind": "service",
  "rootDir": "services/shop",
  "buildCommand": "mkdir -p out && docker build --platform linux/amd64 -t shop . && docker save -o out/shop.tar shop",
  "image": "out/shop.tar",
  "healthCheckPath": "/healthz",
  "resources": { "memoryMB": 128, "cpuMillicores": 250 }
}
```

| Field | | |
|---|---|---|
| `clientId` | required | the App's client id; deploy finds the client by it |
| `clientName` | required | that client's name; deploy refuses a client with another name, so a manifest never lands on a client it does not name |
| `kind` | required | `service` or `worker` |
| `image` | required | the image archive `buildCommand` leaves, relative to `rootDir`: `docker save`, `podman save` or an OCI layout tar |
| `rootDir` | | where `buildCommand` runs, relative to the project root; default `.` |
| `buildCommand` | | run with `sh -c` (Windows: Git Bash or WSL) |
| `startCommand` | | exec form, e.g. `["/app/server", "--port", "8080"]`; replaces the image's entrypoint and command |
| `healthCheckPath` | service | must answer 2xx or 3xx before the App counts as started |
| `resources` | | `{"memoryMB", "cpuMillicores"}` the App asks for |
| `volumeMB` | | on a deploy with no volume yet, creates one of this size at `/data` |
| `description` | | free text |

Settings and secrets never go in the manifest: they are variables on the
engine, per environment. Neither do the engine, the hostname or the version.

## Building the image

The image is yours: base, libraries, updates. It must be **linux/amd64**,
with an entrypoint, a command, or a `startCommand`.

| Language | Without Docker |
|---|---|
| Go | `mkdir -p out && KO_DOCKER_REPO=app ko build --bare --platform linux/amd64 --push=false --tarball out/app.tar .` |
| Java, Clojure | Jib, `jib:buildTar` |
| anything else | `docker build --platform linux/amd64` then `docker save -o out/app.tar <tag>`, or the same with Podman |

`docker save` and ko do not create the output folder: `mkdir -p` it first.
A static site is an image with a small file server and the built files.

The App runs as user 65532 whatever the image says, on a read-only root, so
files it reads must be readable by any user and it writes only to `/tmp` or a
volume.

## Pack

```
synthigy app pack [name] [--version V]
```

Without a name it packs every manifest in `apps/`.

1. **Version:** `--version`, else `git describe --tags --always --dirty`. With
   neither, pack refuses.
2. Runs `buildCommand`, with `SOURCE_DATE_EPOCH` at the HEAD commit's time.
3. Checks the archive: exactly one linux/amd64 image, something to run. It
   warns when the image runs as root or is over 500 MB compressed.
4. Writes `dist/<name>-<version>.tar` and its `.sha256`. The same input gives
   the same bytes.

Pack needs no Docker or Podman itself; only your `buildCommand` might. CI packs
once and deploys the `.tar`.

## Deploy

```
synthigy app deploy [name | dist/<name>-<version>.tar] [--version V] [--timeout 10m]
synthigy app deploy dist/shop-1.4.0.tar --client-id shop-preview --client-name shop-preview
```

Without an argument it deploys every manifest in `apps/`.

1. Packs, or takes a `.tar` pack wrote.
2. Finds the client by `clientId` and checks `clientName`. `--client-id` and
   `--client-name` send one App's package to another client on the same engine,
   for a preview.
3. Compares with what the App already has of this version:

   | The App has | Deploy |
   |---|---|
   | this version, same image, running | nothing: "already running" |
   | this version, same image, stopped | starts it again, no upload: a rollback |
   | this version, another image | uploads it and replaces the old one |
   | nothing of this version | uploads it |

4. Uploads the archive to Files and writes a new App Package.
5. The engine starts the new package next to the running one and stops the old
   one once the new one is healthy. **A package that fails to start leaves the
   previous one running**, and deploy prints the program's last lines.
6. Waits until the package runs, fails (exit 1) or is taken over by a later
   deploy.

Ctrl-C stops the waiting; the engine carries on. The engine keeps the newest
10 packages per App. **Rollback** is deploying an older `.tar` again.

In Studio's Apps tool you can also upload a packed `.tar`, and activate or stop
a package.

## What a running App gets

**Environment:**

| Variable | |
|---|---|
| `SYNTHIGY_ENDPOINT` | the engine's URL |
| `SYNTHIGY_CLIENT_ID`, `SYNTHIGY_CLIENT_SECRET` | the App's credentials |
| `PORT` | `8080`, service only |
| `SYNTHIGY_APP_URL` | the App's own public URL, service only |
| your variables | the App's variables of type `environment` |

The SDKs read the first three, so an App connects with no configuration
([SDKS.md](SDKS.md)). Variables of type `sdk` are not in the environment: the App
reads them over `/data`. A variable change applies at the App's next start.

**The secret** belongs to the engine. When nothing of the App runs, a deploy
sets a new one, and the old one stops working everywhere. Rotating it
(`synthigy iam rotate-client`, or Studio) restarts the running App with a new
one and prints nothing.

**Network:** the internet, and the engine at `SYNTHIGY_ENDPOINT`. The host,
private address ranges and cloud metadata are blocked. Files and data are
reached only through the engine's API, with the App's token.

**Address** (service): `<client name, kebab-cased>.<engine host>`, e.g.
`shop.box.example.com`, or under the apps domain when the server has one.
Renaming the client does not move it. On a machine without the proxy it
answers on a loopback port only.

**Limits:** 256 MB memory, 500 millicores and 64 processes unless the App's
limits say more. An ask above the limits fails the deploy.

**Disk:** read-only root, 64 MB `/tmp` in memory, and its volumes.

**Health:** a service must answer within 60 s, on `healthCheckPath` with 2xx or
3xx, or on `/` with anything below 500. Redirects are not followed. A worker
must stay up for 3 s.

**Stop:** SIGTERM, then SIGKILL after 10 s.

**Crashes:** the App is restarted in place, after 10 s, doubling to 5 min,
without end. Its package stays `running`, with the crash in its Last Error
until it is up again.

## Sandbox type

| Type | The App |
|---|---|
| `strict` (default) | runs only its own program; it cannot start containers or change its system |
| `relaxed` | may run containers of its own, inside its own limits, such as a whole Synthigy whose users deploy Apps into it |

A relaxed App gets more access to the server than a strict one: its root
user (still mapped to unprivileged ids), more capabilities, setuid helpers,
fuse and tun devices, a writable cgroup and up to 1024 processes. Use it
only for images you trust. Apps inside it are strict, and a relaxed App
cannot be nested in another. It answers on one hostname, and its user ids
never overlap strict Apps'. It needs 196608 sub-ids for the Apps user, which
`sudo synthigy service install --apps` sets up; running Apps restart once
while it does.

## Logs

```
synthigy app logs shop-app
synthigy app logs shop-app --kind event --since 1h
synthigy app logs shop-app --since 2026-11-23T10:00:00Z --json
```

| Kind | |
|---|---|
| `output` | the program's stdout and stderr, at most 100 lines/s |
| `event` | starts, crashes, restarts, certificates |

Its deployers, admins and the App itself may read them. A failed start's last
20 lines are also in the package's Last Error.

## Stopping and removing

- Stop: deactivate the package, or the App's client, in Studio. Activating it
  again starts it.
- Remove: `synthigy iam remove-client <id>` stops it, and frees its hostnames.

## Running an image by hand

On a machine set up for Apps, without an engine in the loop, for trying an
image:

```
synthigy app run dist/shop-1.4.0.tar --name shop --health /healthz -e GREETING=hi
synthigy app ls
synthigy app stop shop
```

It gets no client, secret or hostname from an engine.

## When something fails

| You see | Cause |
|---|---|
| deploy prints the program's last lines and exits 1 | it crashed or failed its health check; the previous package still runs |
| `clientName` mismatch | the manifest names another client than the one with that id |
| a limit refusal | `resources` asks more than the App's limits |
| a package `running` with a Last Error | a crash being restarted, or a certificate that does not cover a hostname |
| the App gets 401 from the engine | it uses a secret from before a rotation or redeploy: read `SYNTHIGY_CLIENT_SECRET` at start, never copy it |

Error codes: [ERRORS.md](ERRORS.md).
