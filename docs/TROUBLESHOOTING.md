# Troubleshooting

Symptom, then cause and fix. The page in brackets has the detail.

## Starting

**`synthigy up` opens the setup page every time.** No `SYNTHIGY_BUNDLE` in the
instance's `.env`. Pick a bundle in the portal, or `synthigy up --bundle
sqlite`; the choice is saved ([OPERATIONS.md](OPERATIONS.md)).

**The database test fails on a fresh Postgres.** Synthigy never creates the
database. The setup page shows the `CREATE ROLE` / `CREATE DATABASE` SQL to
run as the Postgres administrator first ([CONSOLE.md](CONSOLE.md)).

**Port already in use.** Another instance, or a stale one, holds 7887 or 7888.
`synthigy status` says what this instance believes is running. Give each
instance its own `SYNTHIGY_SERVER_PORT` and `SYNTHIGY_PORTAL_PORT` in `.env`
([OPERATIONS.md](OPERATIONS.md)).

**The engine refuses to start after a restore or a move.** It cannot unwrap
its data keys: the master key in `.env` is not the one the database was
encrypted with, or Vault is unreachable. The portal says which. Restore the
key with the database ([ENCRYPTION.md](ENCRYPTION.md)).

**The first `up` hangs or fails while downloading.** Behind a corporate proxy
set `HTTPS_PROXY` and point `SYNTHIGY_CA_BUNDLE` at your CA file. Without
network, `synthigy pull` on a connected machine and move the cache
([OPERATIONS.md](OPERATIONS.md)).

**An older release will not start on the database.** Going back after a newer
release has run is not supported. Restore the backup taken before the upgrade
([OPERATIONS.md](OPERATIONS.md)).

## Signing in

**The login page accepts the password and comes straight back.** The instance
is reached over plain `http://` on a host other than localhost, so the browser
drops the `Secure` session cookie. Put TLS in front and set
`SYNTHIGY_IAM_ROOT_URL` ([PROXY.md](PROXY.md)).

**Links on the login page point at the wrong host or scheme.**
`SYNTHIGY_IAM_ROOT_URL` is unset behind a proxy or load balancer. Set it to
the public URL ([PROXY.md](PROXY.md)).

**Nobody can sign in to the console any more.** Superusers are managed in the
portal, which needs no sign-in: `synthigy console`, then **Superusers**
([CONSOLE.md](CONSOLE.md)).

**Password sign-in fails for everyone at once.** A login connector that errors
(unreachable, timeout) refuses the sign-in instead of falling through. Test it
with **Dry-run** under Administration → Connectors, and disable it to fall back
to the local database ([IAM_CONNECTORS.md](IAM_CONNECTORS.md)).

**`redirect_uri` mismatch.** The URI your app sends is not registered on the
client, character for character. Loopback URIs may differ in port; everything
else must match ([SIGN_IN.md](SIGN_IN.md)).

## Calling the API

**`401` / `UNAUTHORIZED`.** No token, or an expired one: access tokens last
15 minutes and the SDKs renew them on their own. For `curl`, `synthigy token`
prints a fresh one ([DATA.md](DATA.md)).

**The token is valid and every call is still refused.** The client has roles
but no API, so its tokens are not issued for the data API. Give it the
**Synthigy** API under Administration → Apps, or recreate it with `--api
Synthigy` ([SIGN_IN.md](SIGN_IN.md)).

**`FORBIDDEN`, `ENTITY_FORBIDDEN`, `ATTRIBUTE_FORBIDDEN`.** The caller's roles
lack the grant for that entity, relation or attribute. Grants live on roles,
not in the model, so they change without a deploy ([ACCESS.md](ACCESS.md)).

**A search returns fewer rows than exist, or an update changes nothing.** Row
rules on the entity hide records from the caller; hidden records are never
reported as errors ([ACCESS.md](ACCESS.md)).

**A selected attribute or relation is missing from the result.** The attribute
is denied to the caller's role, or the relation has no matching rows — those
are left out rather than returned as `[]` ([ACCESS.md](ACCESS.md),
[DATA.md](DATA.md)).

**`UNKNOWN_ENTITY` for an entity that exists.** Not deployed yet, not visible
to the caller, or spelled differently: names accept spaces, `_` and `-`, but
camelCase is not split. The error carries a `hint` when a close name exists
([DATA.md](DATA.md)).

**Browser: CORS error.** The page's origin is not known: register a redirect
URI with that origin on an OAuth client, or list the origin in
`SYNTHIGY_SERVER_ALLOWED_ORIGINS`. Changes take up to 30 seconds. Do not add
CORS headers at the proxy; Synthigy sends its own ([CORS.md](CORS.md)).

**`watch` never fires, or the console freezes behind a proxy.** The proxy
buffers the event streams. Turn buffering and compression off for
`/data/events` and `/console/live/*` ([PROXY.md](PROXY.md)).

## Modeling and code generation

**A deploy is refused because of a row rule.** A guard depends on an attribute
the new version removes. Change the rule, or keep the attribute
([ACCESS.md](ACCESS.md)).

**A type change fails the deploy.** A stored value does not convert to the new
type, for example `string` to `int`. Fix the data first, or add a new
attribute instead ([MODELING.md](MODELING.md)).

**Generated code is out of date; the linter rejects fields that exist.**
`xsql/schema.json` is older than the deployed model. `synthigy schema pull`,
then regenerate. `synthigy schema check` fails in CI when it is stale
([SDKS.md](SDKS.md)).

**Code generation does not see an attribute.** The schema is the connected
identity's view of the model. Connect as the client your application runs as;
an attribute denied to its roles is not there ([SDKS.md](SDKS.md)).
