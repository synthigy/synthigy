# IAM Credentials Connectors

Pluggable backend for verifying user credentials during OAuth login. Operators
configure an ordered chain of connectors persisted in the database. Each
connector type has its own verification implementation — built-ins ship for
local DB and HTTP webhook; Clojure embedders extend the dispatch with their
own.

This document is the canonical reference for the feature. For why the design
landed where it did, the conversation that drove it is summarised in the
project memory entries.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Built-in connector types](#built-in-connector-types)
4. [Webhook contract](#webhook-contract)
5. [Custom connectors (Clojure)](#custom-connectors-clojure)
6. [Storage and refresh](#storage-and-refresh)
7. [Admin HTTP API](#admin-http-api)
8. [REPL helpers](#repl-helpers)
9. [Operator setup](#operator-setup)
10. [Migrating from LDAP](#migrating-from-ldap)
11. [Known gaps](#known-gaps)
12. [File reference](#file-reference)

---

## Overview

When a user submits credentials at `/oauth/login`, Synthigy walks an ordered
**chain of connectors** until one resolves the attempt. The chain lives in the
`__iam_auth_connector` table and is loaded by a database-backed
`CredentialsProvider` that the appropriate backend (Postgres or SQLite)
installs at boot.

Each connector dispatches by `:type` to a `verify-credentials` multimethod.
Adding a new auth backend is a `defmethod` (in-process Clojure) or a webhook
(out-of-process — Node, Python, anything that speaks HTTP).

```
                 ┌────────────────────────────────────────────────┐
                 │          synthigy.iam.connector                │
                 │                                                │
   /oauth/login  │  authenticate                                  │
   ──────────►   │      │                                         │
                 │      ▼                                         │
                 │  list-chain ──► [conn1, conn2, conn3, ...]     │
                 │      │                                         │
                 │      ▼                                         │
                 │  run-chain                                     │
                 │      │                                         │
                 │      ▼                                         │
                 │  verify-credentials   (dispatch on :type)      │
                 │      │                                         │
                 │      ├──► :database                            │
                 │      ├──► :webhook                             │
                 │      └──► :acme/legacy-mainframe (your impl)   │
                 └────────────────────────────────────────────────┘
                                  │
                          {:ok true :user CLAIMS}
                                  │
                                  ▼
                          ensure-local-user (JIT-create if missing)
                                  │
                                  ▼
                          {:_eid ... :euuid ...} → OAuth session
```

LDAP support has been removed (2026-04-30). Operators who need LDAP run a
small adapter service and configure it as a webhook, or write a custom
Clojure `defmethod`.

---

## Architecture

### Two-layer pluggability

| Layer | Purpose | Mechanism |
|---|---|---|
| **Connector dispatch** | What does authentication mean for this connector? | `defmulti verify-credentials` (open extension) |
| **Connector storage** | Where does the chain live, how do we refresh? | `defprotocol CredentialsProvider` (one impl per DB backend) |

These layers are independent. You can:
- Add a custom dispatch (`defmethod verify-credentials :acme/foo …`) without
  touching the storage layer.
- Implement a custom storage backend (Consul, Vault, …) without touching any
  `defmethod`.

### Chain semantics

`run-chain` walks connectors in **priority ascending** order and short-
circuits on the first definitive answer:

| Connector returns | Chain action |
|---|---|
| `{:ok true :user CLAIMS}` | **Stop, success.** Framework JIT-creates local user (if missing) and returns it. |
| `{:ok false :reason :unknown-user}` | Try next connector. |
| `nil` | Treated as `:unknown-user` (try next). |
| `{:ok false :reason :invalid-credentials}` | **Stop, deny.** Don't try the next connector — wrong password is a definitive answer. |
| `{:ok false :reason :error :error e}` | **Stop, fail-closed.** Don't fall through to the next connector — prevents an attacker from DoSing the webhook to bypass it. |
| Any other / unknown reason | Treated as `:invalid-credentials` (deny). |

### JIT user creation

After a `:ok true` response, the framework looks up the local
`:iam/user` row by `:name` (the username). If missing, it creates one via
`dataset/sync-entity` using `:name`, `:active true`, plus `:avatar` and
`:type` from the claims if present. The OAuth flow then sees a normal local
user record (`:_eid`, `:euuid`, audit fields).

This means a custom webhook backed by an external IdP can stand up users in
Synthigy on first login without anyone touching the IAM database.

### Default fallback

If no `CredentialsProvider` is installed (e.g. embedded Clojure usage where
no DB backend has been booted), `authenticate` falls back to a single
`:database` connector. This preserves Synthigy's pre-feature behaviour for
tests and trivial deployments.

---

## Built-in connector types

### `:database`

Verifies against the local `:iam/user` table. Returns the user row on
success.

```edn
{:type     :database
 :name     "Local database"
 :priority 1000
 :enabled  true}
```

No type-specific config. The default-seed row inserted by the patch is this
exact shape.

| Outcome | Returns |
|---|---|
| User not found | `{:ok false :reason :unknown-user}` (try next) |
| User exists but `:active false` | `{:ok false :reason :invalid-credentials}` |
| Password mismatch | `{:ok false :reason :invalid-credentials}` |
| Password matches | `{:ok true :user <user-row-without-password>}` |

### `:webhook`

POSTs the credentials to an external HTTP service which decides yes/no.
The "external small service" pattern that replaces LDAP for most use cases.

```edn
{:type       :webhook
 :name       "Corp AD via webhook"
 :priority   100
 :enabled    true
 :url        "https://auth.internal.corp/verify"
 :secret     "shared-secret-for-hmac"
 :timeout-ms 1500
 :domain     "corp.com"}     ; optional — see Known gaps below
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `:url` | string | — | Required. HTTPS strongly recommended. |
| `:secret` | string | — | HMAC-SHA256 key. If absent, no signature header is sent (not recommended). |
| `:timeout-ms` | int | `1500` | Both connect and read. On exceed: fail-closed. |

5xx, network errors, and non-conforming bodies all map to `:error`
(fail-closed). 200 + `{"ok":false,"reason":"unknown_user"}` falls through to
the next connector. Any other 200 + `{"ok":false}` denies the chain.

---

## Webhook contract

### Request

```http
POST /verify HTTP/1.1
Host: auth.internal.corp
Content-Type: application/json
X-Synthigy-Signature: hmac-sha256=<base64-hmac>

{"username":"alice","password":"p@ssw0rd","request_id":"<uuid>"}
```

`X-Synthigy-Signature` is `HMAC-SHA256(secret, body-bytes)` base64-encoded,
prefixed `hmac-sha256=`. Header is omitted entirely when `:secret` is absent
in the connector spec.

### Response — success

```json
{
  "ok": true,
  "user": {
    "name":         "alice",
    "email":        "alice@corp.com",
    "given_name":   "Alice",
    "family_name":  "Smith",
    "avatar":       "https://corp.com/avatars/alice.png",
    "type":         "PERSON",
    "groups":       ["staff", "engineers"],
    "roles":        ["developer"]
  }
}
```

| Field | Required | Used by JIT today? |
|---|---|---|
| `name` | ✅ | ✅ — primary identifier for lookup/JIT |
| `avatar` | optional | ✅ — passed to `sync-entity` |
| `type` | optional | ✅ — defaults to `:PERSON` |
| `email`, `given_name`, `family_name` | optional | ❌ informational; framework may auto-sync in future |
| `groups`, `roles` | optional | ❌ not auto-applied; operators wire downstream |

Any additional fields are round-tripped (kept in the result map) but ignored
by JIT.

### Response — failure

```json
{"ok": false, "reason": "unknown_user"}          // try next connector
{"ok": false, "reason": "invalid_credentials"}    // stop chain, deny
{"ok": false, "reason": "locked"}                 // → invalid_credentials
```

Any reason other than `"unknown_user"` is treated as `:invalid-credentials`.

### Verifier reference (Node.js)

```js
import crypto from 'node:crypto';

app.post('/verify', express.text({ type: 'application/json' }), (req, res) => {
  const raw = req.body;                                       // string, NOT parsed
  const expected = 'hmac-sha256=' + crypto
    .createHmac('sha256', process.env.SHARED_SECRET)
    .update(raw)
    .digest('base64');
  const got = req.get('X-Synthigy-Signature') ?? '';

  const a = Buffer.from(expected);
  const b = Buffer.from(got);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).end();
  }

  const { username, password } = JSON.parse(raw);
  // …check creds against your real backend…
  res.json({ ok: true, user: { name: username, email: 'alice@corp.com' } });
});
```

### Verifier reference (Python / FastAPI)

```python
import hmac, hashlib, base64, os
from fastapi import FastAPI, Request, HTTPException

SECRET = os.environ['SHARED_SECRET'].encode()

@app.post('/verify')
async def verify(request: Request):
    raw = await request.body()                                # bytes, NOT parsed
    expected = b'hmac-sha256=' + base64.b64encode(
        hmac.new(SECRET, raw, hashlib.sha256).digest()
    )
    got = request.headers.get('X-Synthigy-Signature', '').encode()

    if not hmac.compare_digest(expected, got):
        raise HTTPException(401)

    data = json.loads(raw)
    return {'ok': True, 'user': {'name': data['username']}}
```

### Two non-obvious rules

1. **Hash the raw body bytes**, not parsed-then-serialized JSON. Most web
   frameworks parse JSON before your handler sees it; the re-serialized
   string will not match Synthigy's HMAC. Configure your framework to keep
   the raw body (`express.text(...)` in Node, `await request.body()` in
   FastAPI).
2. **Constant-time compare** signatures, not `==`. Use
   `crypto.timingSafeEqual` (Node) / `hmac.compare_digest` (Python) /
   `crypto/subtle.ConstantTimeCompare` (Go).

---

## Custom connectors (Clojure)

Embedders add their own backend by `defmethod`-ing on `verify-credentials`:

```clojure
(ns acme.legacy-auth
  (:require [synthigy.iam.connector :as connector]))

(defmethod connector/verify-credentials :acme/legacy-mainframe
  [{:keys [endpoint timeout-ms]} {:keys [username password]}]
  (try
    (if (mainframe/check endpoint username password
                         {:timeout (or timeout-ms 2000)})
      {:ok true :user (mainframe/lookup username)}
      {:ok false :reason :invalid-credentials})
    (catch java.net.SocketTimeoutException _
      {:ok false :reason :error :error :mainframe-timeout})))
```

Then add a connector row:

```clojure
(connector/save-connector!
  {:type :acme/legacy-mainframe
   :name "Mainframe"
   :priority 50
   :endpoint "tcp://mf.internal:5040"
   :timeout-ms 2500})
```

The custom defmethod must be `require`d before any login attempt — typically
do this from your application's startup namespace. The `:type` keyword can
be anything; namespacing it (`:acme/...`) avoids collisions with built-ins
or other plugins.

---

## Storage and refresh

### Table: `__iam_auth_connector`

| Column | Postgres | SQLite | Notes |
|---|---|---|---|
| `euuid` | UUID PK | TEXT PK | UUID; also acts as a stable identifier |
| `xid` | VARCHAR(64) UNIQUE | TEXT UNIQUE | Public-facing identifier used in admin URLs |
| `name` | TEXT | TEXT | Human label |
| `type` | TEXT | TEXT | Stored as `name` of the keyword (e.g. `"webhook"`, `"acme/foo"`) |
| `priority` | INT | INTEGER | Lower runs first |
| `domain` | TEXT (nullable) | TEXT (nullable) | Email-domain filter (see Known gaps) |
| `enabled` | BOOLEAN | INTEGER (0/1) | Disabled rows are skipped by `list-chain` |
| `config` | JSONB | TEXT | Type-specific blob (`:url`, `:secret`, `:timeout-ms`, …) |
| `created_on` / `modified_on` | TIMESTAMPTZ | TEXT (ISO 8601) | Auto-populated |

Index `(enabled, priority)` matches the only query the chain runner uses.

The table is **not** part of the ERD model — it's an internal config table
managed via Patcho patches (the `__` prefix flags this convention). Side
effects:
- No automatic GraphQL CRUD generation.
- No automatic `/data` endpoint exposure.
- Permission-gating, not row-level access control.

CRUD goes through the `synthigy.iam.connector` sugar API or the
`/__admin/iam/connectors` HTTP endpoints.

### Refresh flow

The provider caches the chain in memory. Local writes invalidate
immediately. Cross-node invalidation differs by backend:

```
                                Local write
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │ save/delete-connector!│
                          │ ─ set DB row          │
                          │ ─ refresh! local cache│
                          │ ─ publish event       │
                          └──────┬────────────────┘
                                 │
        ┌────────────────────────┴────────────────────────┐
        │                                                 │
   Postgres only:                                  Single-process:
   trigger fires pg_notify         (SQLite, embedded)     │
        │                                                 │
        ▼                                                 ▼
   LISTEN thread on every node                    no further action;
   ─ catches NOTIFY                                cache stays consistent
   ─ refresh! → cache invalidated                  (only one process)
```

Postgres is multi-node-safe by design via the trigger. SQLite is single-
process by design. No external pub/sub (Redis, etc.) needed.

---

## Admin HTTP API

All endpoints live on the **admin port** (`127.0.0.1` only, port written to
`~/.synthigy/admin.port`). No OAuth required — local-loopback binding is
the trust boundary.

| Method | Path | Body / Query | Behaviour |
|---|---|---|---|
| `GET` | `/__admin/iam/connectors` | `?reveal=true` (optional) | List the chain in priority order. Secrets `***` unless `reveal`. |
| `POST` | `/__admin/iam/connectors` | connector spec | Create. Returns 201 + the persisted row (secret redacted). |
| `GET` | `/__admin/iam/connectors/:xid` | `?reveal=true` (optional) | Single row. Secret redacted unless `reveal`. |
| `PUT` | `/__admin/iam/connectors/:xid` | connector spec | Update. **If `:secret` is omitted from the body, the existing secret is preserved.** |
| `DELETE` | `/__admin/iam/connectors/:xid` | — | Remove. |
| `POST` | `/__admin/iam/connectors/refresh` | — | Force `refresh!` on the active provider. Rarely needed (Postgres NOTIFY handles cache invalidation automatically). |
| `POST` | `/__admin/iam/connectors/:xid/test` | `{username, password}` | **Dry-run.** Runs only this connector against the supplied creds and returns the raw `verify-credentials` response. No JIT, no session created. |

Every `?reveal=true` request emits a `WARN`-level log line with the connector
xid (or count for the list endpoint) so secret access is auditable.

### Examples

```bash
# Read the admin port
PORT=$(cat ~/.synthigy/admin.port)
ADMIN=http://127.0.0.1:$PORT

# Create a webhook connector
curl -s "$ADMIN/__admin/iam/connectors" \
  -H 'Content-Type: application/json' \
  -d '{"type":"webhook","name":"Corp AD","url":"https://auth.internal.corp/verify","secret":"shared-secret","priority":100}'

# List the chain (secrets redacted)
curl -s "$ADMIN/__admin/iam/connectors"

# List with secrets revealed (logged on the server)
curl -s "$ADMIN/__admin/iam/connectors?reveal=true"

# Verify the webhook actually works without going through OAuth
curl -s "$ADMIN/__admin/iam/connectors/<xid>/test" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"p@ssw0rd"}'
# → {"ok":true,"user":{"name":"alice","email":"..."}}

# Update priority without re-supplying the secret
curl -s -X PUT "$ADMIN/__admin/iam/connectors/<xid>" \
  -H 'Content-Type: application/json' \
  -d '{"type":"webhook","name":"Corp AD","url":"https://auth.internal.corp/verify","priority":50}'
```

---

## REPL helpers

`(require '[synthigy.iam.connector.dev :as dev])` to load.

| Fn | Purpose |
|---|---|
| `(dev/print-chain)` | Pretty-print the active chain as a table |
| `(dev/list-types)` | Show every connector type with a registered defmethod |
| `(dev/seed-webhook! url secret)` / `(dev/seed-webhook! url secret priority)` | Insert a webhook connector with sensible defaults |
| `(dev/seed-database!)` / `(dev/seed-database! priority)` | Insert/update the local-database connector |
| `(dev/disable! xid)` / `(dev/enable! xid)` | Toggle without deleting |
| `(dev/set-priority! xid n)` | Reorder |
| `(dev/set-domain! xid "corp.com")` | Set/clear the domain filter |
| `(dev/try-authenticate "alice" "password")` | Run the chain and pretty-print the outcome |

Sample output:

```
PRIORITY  TYPE               DOMAIN      ENABLED   NAME                XID
---------------------------------------------------------------------------------
50        :webhook                       true      Webhook auth.inte…  0731a2f7-…
1000      :database                      true      Local database      bd0f5935-…
---------------------------------------------------------------------------------
2 connector(s)
```

---

## Operator setup

Typical workflow for a new install:

```bash
# 1. Boot Synthigy with the DB backend of choice. Patches run on first start
#    and create __iam_auth_connector with a default :database connector seeded.
clj -M:postgres:pedestal:dev      # or :sqlite:pedestal:dev

# 2. Verify the chain via the admin API
PORT=$(cat ~/.synthigy/admin.port)
curl -s "http://127.0.0.1:$PORT/__admin/iam/connectors"
# → {"connectors":[{"type":"database","name":"Local database","priority":1000,...}]}

# 3. (Optional) Stand up your verifier service on its own host.
#    See "Webhook contract" → "Verifier reference" above.

# 4. Add the webhook connector. Priority < 1000 puts it before the local DB.
curl -s "http://127.0.0.1:$PORT/__admin/iam/connectors" \
  -H 'Content-Type: application/json' \
  -d '{"type":"webhook","name":"Corp AD","url":"https://auth.internal/verify","secret":"...","priority":100}'

# 5. Dry-run it before relying on it for real logins.
curl -s "http://127.0.0.1:$PORT/__admin/iam/connectors/<xid>/test" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"p@ssw0rd"}'
```

---

## Migrating from LDAP

Synthigy used to bundle LDAP/AD support via `synthigy.oidc.ldap` configured
through `LDAP_*` environment variables. As of **2026-04-30** that namespace
and all LDAP env-var handling have been removed.

Two replacement paths, in order of effort:

### 1. LDAP-bind webhook adapter (recommended)

Run a small service that does the LDAP bind on Synthigy's behalf and exposes
the standard webhook contract. ~30 lines in any language. Configure via the
admin API as a `:webhook` connector. Benefits:

- Synthigy itself stays LDAP-free.
- You can swap LDAP for any other backend later without touching Synthigy.
- The webhook can do whatever pre/post-processing your org needs (group
  mapping, audit logging, etc.) without imposing them on Synthigy.

### 2. Custom Clojure `defmethod`

Embed the UnboundID LDAP SDK (or `clj-ldap`) directly in your application
code, register a `defmethod verify-credentials :acme/ldap`, configure a
connector row of that type. Best for embedders who want zero out-of-process
hops.

The previous `synthigy.oidc.ldap`'s JIT-create-on-success behaviour is now
an automatic property of every connector — see [JIT user creation](#jit-user-creation)
above.

---

## Known gaps

These are documented because they're *real* and ship-blocking-adjacent, not
because they're nice-to-haves. Address before depending on the feature in
production.

| Gap | Why it matters | Cost to close |
|---|---|---|
| **Postgres path is unverified end-to-end.** Compiles + lints clean, but the LISTEN/NOTIFY round-trip and JSONB serialization are not exercised against a real Postgres instance. | Cache invalidation across nodes is the whole point of the trigger; if it doesn't actually work, multi-node deployments will see stale chains. | ~20 min — boot a Postgres-aliased REPL, exercise CRUD + `psql -c "\d __iam_auth_connector"`. |
| **No webhook behavioural test.** Unit tests cover chain semantics, but no test stands up an HTTP server and exercises the full HMAC + JSON round-trip. | The HMAC code path is untested at the wire level. A subtle bug here would only surface in production. | ~30 min — http-kit handler in-process, run `verify-credentials` against it. |
| **`:domain` column isn't wired into routing.** The schema column exists, the dev helper is documented, but `run-chain` doesn't filter by domain. A connector with `:domain "corp.com"` runs for every user. | Half-implemented features mislead operators. Either implement the filter or drop the column. | ~5 min — implement (4 lines in `run-chain`) or drop. |
| **Webhook secrets are plaintext at rest.** `__iam_auth_connector.config` JSONB stores `:secret` in the clear. | Real production secret-handling expects encryption at rest, especially when admins can `SELECT * FROM __iam_auth_connector`. | Separate small PR — wrap with `synthigy.iam.encryption`. |

Lower-priority items (defer until requested):

- Spec validation on save (`save-connector! {:type "garbage"}` succeeds today).
- CLI subcommands (`synthigy connector list/add/remove`) — admin curl works today.
- Per-connector log levels for debugging one webhook in isolation.
- Replay protection on webhook (timestamp + nonce).
- Frontend admin UI screen.

---

## File reference

| Path | Purpose |
|---|---|
| `auth/synthigy/iam/connector.clj` | Multimethod, protocol, sugar API, `MemoryCredentialsProvider`, default chain |
| `auth/synthigy/iam/connector/dev.clj` | REPL helpers (require manually) |
| `postgres/src/synthigy/iam/connector/postgres.clj` | Postgres provider, patch (table + trigger + seed), LISTEN thread, lifecycle module |
| `sqlite/src/synthigy/iam/connector/sqlite.clj` | SQLite provider, patch (table + seed), lifecycle module |
| `core/core/src/synthigy/admin/core.clj` | Admin HTTP handlers + router (mixed in with other admin endpoints) |
| `auth/synthigy/oauth/core.clj` | `validate-resource-owner` integration point |
| `test/synthigy/iam/connector_test.clj` | Multimethod / chain / sugar / `authenticate` unit tests |
| `test/synthigy/admin/connector_test.clj` | Admin HTTP endpoint tests (Ring-handler-direct, no server) |

Test counts (as of 2026-04-30): 15 connector + 9 admin + 14 OAuth = 38 tests,
99 assertions, all green.
