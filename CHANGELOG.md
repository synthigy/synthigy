# Changelog

All notable changes to the Synthigy engine. Releases before 0.3.0 are
described in their [GitHub release notes](https://github.com/synthigy/synthigy/releases).

## [0.3.1] — 2026-10-19

### License
- **Synthigy is now under the Business Source License 1.1** (0.3.0 and earlier:
  Elastic License 2.0). Production use is free while the Total Finances of you
  and your affiliates (the greater of revenue and capital raised in the last
  twelve months) stay at or under EUR 5,000,000, and the same limit applies to a
  third party you install or run it for. Offering it to others as a hosted or
  managed service is not covered. The limit doesn't apply to individuals using
  it for personal, non-commercial purposes, non-profits, or accredited
  education and research. Each version becomes GPL-2.0-or-later or Apache-2.0
  four years after its release. Full terms in [LICENSE](LICENSE); other
  arrangements in [COMMERCIAL.md](COMMERCIAL.md).

### Added
- **A sign-in client for the Synthigy desktop app.** Every engine now has a
  public OAuth client, Synthigy Desktop: it signs in through the system browser
  to a loopback address on any port (RFC 8252), with PKCE.
  `/.well-known/synthigy` names it as `auth.oidc.desktop_client_id`. The desktop
  app's own origins, `tauri://localhost` and `http://tauri.localhost`, are always
  allowed browser origins.
- **Client setting `"application-type": "native"`** (OIDC client registration's
  `application_type`). A native client's redirect URIs no longer count as
  allowed browser origins; its sign-in redirects work as before. The desktop
  client carries it, so its `http://127.0.0.1` redirect does not let local web
  pages call the engine.

### Changed
- **Text sorts the way people read it.** `string` and `enum` attributes use the
  `human` collation: case-insensitive first (case breaks ties), digits compare
  as numbers, accents follow Unicode — `apple Apple Auto2 Auto11 čevapi Zebra`.
  PostgreSQL and SQLite return the same order. Equality is unchanged
  (`Auto2` ≠ `Auto02`, `apple` ≠ `Apple`), so unique constraints and lookups
  behave as before.
- **Existing databases are converted on first boot** (dataset feature 1.7.0).
  PostgreSQL rebuilds only the indexes on those columns; SQLite rewrites each
  converted column.

### Fixed
- **The modeler loads system datasets again.** Engine 0.3.0 answered the
  modeler's `synthigy-datasets` request with `INTERNAL_ERROR` every time, so
  the modeler showed an error on open and the Synthigy and OAuth datasets were
  not read only for users who aren't superusers. More generally, an operation
  whose result is a list of plain values (not records) no longer breaks the
  response.
- **Deleting a dataset version answers `true`.** A deployed version answered
  `{"update_count": 1}` and an undeployed one `null`.
- **Failed `synthigy.embedded` calls are audited with their stack trace.** The
  audit row's `error_trace` was empty.
- **`_distinct` with a field list works on SQLite.** A list from the wire or the
  Clojure API was ignored there; both engines now read the list and the form
  XSQL sends the same way.
- **SQLite `search-tree` keeps the order past the ninth sibling.** Roots and
  siblings ranked 10 and above used to sort before rank 2.
- **SQLite `_agg` min/max with a relation filter** follows the text sort order
  (it returned `Apple` before `apple`).

### Breaking
- **`synthigy.engine/execute` returns unshaped results.** Trimming to the
  selection and renaming keys to `key_format` moved to `synthigy.wire`; an
  in-process caller that wants what `/data` returns runs
  `(synthigy.wire/results (synthigy.wire/key-fn key-format) results)`. `/data`,
  the SDKs and `synthigy.embedded` return exactly what they did.
- **PostgreSQL needs ICU.** The engine refuses to boot on a server without ICU
  collations. RDS, Aurora, Neon and the official images have them.
- **SQLite runs on `com.synthigy/sqlite-jdbc`** (sqlite-jdbc with ICU built in)
  instead of `org.xerial/sqlite-jdbc`; boot refuses the stock driver. A database
  file can still be opened by other SQLite tools, but anything there that uses an
  index on its text columns (ordering, comparing, even a `count(*)` the planner
  answers from such an index) fails with `no such collation sequence: HUMAN`.
  The driver ships native libraries for Linux (glibc and musl) on x86_64 and
  arm64, macOS on Intel and Apple Silicon, and Windows on x86_64; 32-bit
  systems, Windows on ARM and FreeBSD are not covered.

## [0.3.0] — 2026-10-16

### Breaking
- **`SYNTHIGY_SERVER_TRUST_PROXY` lists your proxies.** It takes IPs or CIDRs,
  comma separated — `127.0.0.1/32,::1/128` for a proxy on the same host —
  instead of `true`. `X-Forwarded-For` is believed only from those addresses
  and is read from the right, skipping trusted hops, so a proxy may overwrite
  or append to the header. `true` is ignored with a warning: until you change
  it, every request is recorded with the proxy's address. See
  [PROXY.md](docs/PROXY.md).
- **`X-Forwarded-Host` and `X-Forwarded-Proto` follow the same list.** From
  any caller not in `SYNTHIGY_SERVER_TRUST_PROXY` they are dropped before the
  request is handled. With `SYNTHIGY_IAM_ROOT_URL` set nothing changes. Behind a
  proxy, without `TRUST_PROXY` and without `ROOT_URL`, the token issuer and
  login links now take the scheme of the connection (`http`) and the host from
  `Host` — set one of the two. A directly exposed engine still believes
  `Host`; only `SYNTHIGY_IAM_ROOT_URL` pins it.

- **UserInfo requires the `openid` scope.** A token without it — every
  `client_credentials` token — gets 403 `insufficient_scope` (OIDC Core §5.3,
  RFC 6750 §3.1); it used to return the service user's claims. `synthigy
  whoami` on a service profile needs the portal release that ships with this
  engine.

- **One system dataset: Synthigy.** The IAM dataset is renamed Synthigy and
  absorbs the Datasets meta-model; a fresh install deploys Synthigy and OAuth.
  On an engine running IAM, an upgrade deploys Synthigy and retires the
  Datasets dataset (its rows only — no table or column is dropped). The
  retire is skipped while Synthigy does not cover everything Datasets owns
  (`datasets-retire-skipped` at warn, retried every boot) and never blocks
  boot. The schema drift stamp loses its Datasets key and Synthigy's changes,
  so `synthigy schema check` and SDK codegen report the schema stale once —
  pull it again. A fresh install that starts only the dataset module gets the
  `user`, `user_group` and `user_role` tables too.
- **Synthigy and OAuth change only through engine upgrades.** For anyone but
  a superuser, a deploy, recall or destroy that targets either dataset is
  refused (`SYSTEM_DATASET_ENGINE_OWNED`), and so is one whose result would
  change a system entity or relation — a user dataset carrying a copy of
  User, User Group, User Role or an OAuth entity that differs from the one in
  force (`SYSTEM_ENTITY_ALTERED`; the error lists the changes). A superuser's
  deploy goes through and the changes are logged at warn
  (`:synthigy.engine/deploy-warning`). Such a dataset deploys again once its
  copies match the global model (Pull or Revert in the modeler's deploy
  drawer).
- **RLS on system entities comes only from Synthigy and OAuth.** Guards that
  another dataset declares on User, User Group, User Role or an OAuth entity
  are ignored, for every role — they used to apply engine-wide, in the
  console too. On upgrade such guards stop filtering, and the next deploy of
  that dataset is refused with `SYSTEM_ENTITY_RLS` until they are removed. Put
  guards on your own entities instead.
- **Dataset meta-model rows are written by the engine only.** Through `/data`:
  - Dataset Entity, Dataset Entity Attribute and Dataset Relation rows cannot
    be created, changed, linked or deleted by anyone, superuser included
    (`ENGINE_OWNED`). A role grant that names them by id keeps working.
  - A version's `deployed` and `deployed_on` are set only by deploy and
    recall. For anyone but a superuser, a deployed version cannot be changed,
    moved to another dataset, unlinked from its dataset or deleted with a
    plain delete (`IMMUTABLE_VERSION_ALTERED`); recall and destroy remove
    them.
  - A version name is used once per dataset: saving or deploying a second
    version under a taken name is refused with `VERSION_NAME_TAKEN` (the
    details say whether the holder is deployed). Duplicates already in the
    database stay; upgrades only warn about them.
  - `purge` whose selection reaches a meta-model entity is refused
    (`FORBIDDEN_OP`); `slice` across a meta-model link is refused
    (`ENGINE_OWNED`), and across Dataset → Versions for anyone but a
    superuser.
- **`deploy` needs the dataset's id.** The payload's `dataset` must carry an
  `xid` or `euuid`; a name alone is refused with `BAD_OP`. Only the version's
  ids, `name`, `configuration`, `model` and the dataset reference are written
  — anything nested under `dataset` is ignored.
- **Clojure API: IAM model names are Synthigy names.** For embedders and
  library callers: `:iam/id` is `:synthigy/id` (same xid);
  `synthigy.iam/current-version` and `register-reference-types!` are
  `synthigy.dataset/current-synthigy-version` and
  `synthigy.dataset/register-reference-types!`; the patch topic
  `:synthigy.iam/model` is `:synthigy/model`; `dataset/iam.json` is
  `dataset/synthigy.json`. Bootstrap log ids changed with them
  (`:synthigy-model-loading`, `:synthigy-model-mounted`,
  `:synthigy.iam/deploying-synthigy-model`). `retire-datasets!`,
  `mount-synthigy-schema!`, `fix-deployed-on!`, `model-ids` and
  `can-migrate-to-xid?` moved from `synthigy.dataset` to
  `synthigy.dataset.patch`, and the `:synthigy/dataset` upgrade steps with
  them.
- **Audit history is no longer tenant-scoped.** New history rows and live
  subscription events carry no tenant (`tenant: null`); `/history` no longer
  forces one, so a `tenant` a caller passes now filters — and matches only
  rows written before this release.

### Added
- **Every log line knows where its request came from.** Request-scoped lines
  carry the client address in a new, indexed `ip` column — filter with
  `{:where {:ip "198.51.100.7"}}` or `:group-by :ip` — and `ctx` carries the
  proxy's address (`peer`), the user agent and the raw `X-Forwarded-For`.
  Existing log tables gain the column on first boot.
- **Log lines name the session and the token.** A request with a bearer token
  carries the OAuth client, the token's `jti` and the first six characters of
  its session id in `ctx`; a request with only the session cookie carries the
  session. Issued access tokens are logged at info with their `jti`, so a
  token can be followed from the address it was issued to every address that
  uses it. The full session id is never logged.
- **Refused tokens are logged.** A bearer token refused on `/data`, `/logs`,
  `/history`, subscriptions, SSE, `/oauth/userinfo` or `/oauth/password`
  leaves one `token-rejected` line at warn
  with the reason — forged, expired, minted for another audience, for a
  disabled client, for a deactivated user or for a user that no longer
  exists — and the address. When the signature holds, the line also names
  the token's `jti`, client and user, and when an expired token expired.
- **Successful writes are logged.** `/data` writes log at info like reads: one
  line per operation, with its source address. The audit trail keeps the
  values; the log line says who and from where. Expect more log lines.
- **A request id from an untrusted caller is kept aside.** An `X-Request-Id`
  sent by a trusted proxy becomes the request's id; from anyone else it is
  recorded as `client-request-id` and a fresh id is issued. The response's
  `X-Request-Id` always names the id in use.
- **Every deploy logs what it changes** — the same diff the modeler's deploy
  drawer shows (`:synthigy.dataset/deploy-changes`), including the deploys a
  system upgrade runs on its own.
- **`GET /health` for load balancers and supervisors.** Unauthenticated, no
  cache: 200 `{"status":"ok"}`, 200 `"degraded"` while audit or logs lag
  behind, 503 `"unavailable"` when the database does not answer within 2 s or
  a required module is down. The body names each check. See
  [OPERATIONS.md](docs/OPERATIONS.md).
- **OIDC `login_hint` pre-fills the sign-in.** The built-in login page fills
  the username and focuses the password; a custom login page gets it as
  `?login_hint=` next to `state` (trimmed, at most 256 characters). See
  [LOGIN_PAGES.md](docs/LOGIN_PAGES.md).
- **Console: one-time sign-in link for a user.** The user page can generate a
  link, valid 24 hours, where the person sets a password or links an identity
  provider. It needs a fresh sign-in, is shown once, cancels the previous
  link, and is offered only to an admin of that user — never for an app's
  service user.
- **Custom account set-up page.** A `claim.html` in the `default` custom-page
  bundle replaces the built-in page a sign-in link opens. It receives
  `?token=`, `methods` (only when the link restricts them) and `error`
  (`password_too_short`), and posts `{token, password}` to
  `/oauth/claim/password`. A rejected password does not use up the link.

- **App URL on apps.** Where people open the app. A sign-in link made for
  an app (console picker, or `/oauth/onboard` without `return_url`) sends the
  person there after they set up their account; without an app it ends on
  the status page.
- **`deploy-check` asks before deploying.** Same scope as `deploy`
  (`dataset:deploy`); returns `{errors, warnings}` — each shaped like a `/data`
  error with `code`, `message`, `hint` and `details` — for the version given,
  without deploying it. A deploy throws the first of those errors. The
  modeler's deploy drawer calls it as you edit.
- **`synthigy-datasets` names the engine's own datasets** — the ids of
  Synthigy and OAuth, scope `dataset:load`.
- **Model subscriptions announce a running change.** Besides
  `{"action": "deploy"}`, model subscribers get `{"action": "busy", "busy":
  {action, dataset, version, by, since}}` when a deploy, recall or destroy
  starts and `{"action": "idle"}` when it ends. A client that refetches on
  every model event now refetches twice more per deploy.
- **PROXY.md: behind an AWS Application Load Balancer.** ALB settings, and
  nginx, Caddy and Traefik configs that keep the client address the ALB
  recorded instead of replacing it with the ALB's. See
  [PROXY.md](docs/PROXY.md#behind-an-aws-application-load-balancer).

### Changed
- **One model change at a time.** A deploy, recall or destroy started while
  another runs is refused at once with `MODEL_BUSY`, naming the change in
  progress, who started it and when. Try again when it finishes. The deploy
  checks run inside the same lock, against the model the change applies to.
- **Deploys refuse new name collisions with a code.** Two active entities
  whose names map to the same table were refused as `OPERATION_ERROR`; now
  `ENTITY_NAME_CONFLICT`. New: an attribute and a relation label (or two
  labels) that give an entity the same field key are refused with
  `FIELD_KEY_CONFLICT`. Only collisions the global model does not already have
  count. Renaming a deployed version through `deploy` is refused like any
  other change to it (`IMMUTABLE_VERSION_ALTERED`).
- **System models: Synthigy 0.90.1, OAuth 0.2.10.** Their copies of the shared
  entities are now identical: `Name` is unique and mandatory on User, User
  Group, User Role, Dataset and Dataset Entity, and User `Password` is
  classified secret in both. Before, OAuth's copy won on a fresh install and
  the password was not marked secret. Existing installs deploy both on
  upgrade; no `NOT NULL` is added, so rows without a name do not block it.
- **A bearer token's signature is checked once a minute, not on every
  request.** Verified claims are kept for 60 seconds per token while its
  signing key is still loaded; rotating a key away invalidates them at once.
  Expiry, audience, client and user are still checked on every request. In a
  benchmark (enso, Postgres) `/data` reads went from about 23k to 31k requests
  per second.
- **`deployed-model` and `runtime-model` return the full model to a token with
  `schema:read`**, as `schema` already did. Without it they return what the
  caller may read.
- **Facebook sign-in uses Graph API v26.0 and sends `appsecret_proof`**, so it
  works with Meta's "Require App Secret" setting on. The console's Facebook
  guide now walks through developer registration, the `email` permission and
  publishing the app.
- **Server adapters: http-kit, enso and Jetty.** `:server-enso`
  ([enso](https://github.com/mpenet/enso), virtual threads, HTTP/1.1, h2, h3)
  is new; http-kit stays the default and is the only adapter in the
  published bundles. `:server-pedestal` and
  `:server-undertow` are removed. patcho now comes from Clojars
  (`dev.gersak/patcho` 0.4.4) instead of a submodule.

- **The console is for administrators only.** Signing in, and every console
  page, needs a System Operator role or the right to change users, groups,
  roles, apps, APIs, providers or connectors. Other accounts are told to
  manage their account in their app. Profile, Sessions and Sign-in methods
  go with it.
- **Console: "Login pages" is now "Custom pages"**, at
  `/console/custom-pages` (was `/console/login-pages`). The page documents
  `claim.html` beside the other bundle files.
- **Upgrading from the Datasets meta-model is one step.** The legacy
  `:synthigy.dataset/model` 1.0.0–1.0.6 upgrades are replaced by a single
  1.0.6 step that brings the tables in line with Synthigy without writing a
  version row, then backfills `deployed_on` where it is empty.
  `dataset/dataset.json` is no longer shipped.
- **The built-in login page keeps the username after a failed sign-in** and
  lets password managers fill saved credentials (`autocomplete` was
  `new-password`). Errors show in an alert box with shorter copy ("Wrong
  username or password."); the page is restyled.
- **Custom login pages reusing `/oauth/css/login.css`** lose the `span.error`
  rule — errors are styled by `.sy-error-slot` / `.sy-error` now. A page that
  shows errors in `<span class="error">` renders them unstyled until it
  switches.
- **The docs and the console say `synthigy portal`** where they said
  `synthigy console`; the portal keeps `console` as an alias.
- **The console's bundled tools** (modeler, data console, log cockpit) are
  rebuilt: modeler notifications stay bottom-right over an open drawer, and
  the `login-hint` attribute is gone (name a demo account on the instance's
  login page instead).

### Fixed
- **Engines that start only the dataset module accept user, group and role
  attributes.** Mounting such a model failed with "Reference type 'user' is
  not registered".
- **Console create forms keep what you entered** when the create is
  rejected — links, owned rows, settings and config came back reset.
- **SQL templates cannot write.** Only the first keyword was checked, so a
  data-modifying statement inside a CTE or after it reached the database, with
  no RBAC or RLS, from any role. Every CTE body and the outer statement must
  now be a `SELECT` (or `VALUES`), and the template runs in a read-only
  transaction. A template that wrote now fails with `QUERY_NOT_SELECT` or a
  database error.
- **Deleting a dataset or version on a UUID-format instance destroys or
  recalls it.** With `SYNTHIGY_ID_FORMAT=euuid` the engine's delete hooks never
  fired: a delete of a Dataset Version was a plain row delete, with no recall
  and no checks.
- **A failed read during deploy fails the deploy.** When the deployed versions
  could not be read (lost connection, timeout), the deploy carried on as if
  none existed: every entity of every version ever deployed came back active
  and RLS was not reconciled.
- **`_order_by` on a nested relation** in the `[[field, direction]]` form,
  which XSQL's `order by` produces, failed with `INTERNAL_ERROR`.
- **Reactivating an attribute on SQLite no longer fails the deploy** with
  `duplicate column name` — the column is reused, as on Postgres.
- **Audit rows record their request.** `request_id` on audit rows was always
  empty. It now carries the request's id, which joins an audit row to the log
  lines of the request that made the change; history timelines grouped by
  request work.
- **The client cannot choose its own address.** http-kit took the first
  `X-Forwarded-For` entry from any caller. The engine now sees the connection's
  own address (http-kit 2.9.0-beta3) and decides the client address in one
  place for every server adapter; sessions and login checks use it.
- **Log filters on keys containing a dash.** Paths such as `[:data :elapsed-ms]`
  or `[:ctx :user-agent]` never matched on the SQLite, DuckDB and ClickHouse
  log stores.
- **Login events name the client.** `login-authenticated` and
  `login-credentials-rejected` logged `client: null`.
- **Turning logging down is recorded.** A log-level change was logged at info
  after it took effect, so raising the level to `error` dropped the record of
  the change itself. `config-changed` and `config-cleared` are now written at
  warn before the change applies, whatever the levels, and name the store
  level too.
- **A deactivated user's tokens stop working.** Bearer requests checked that
  the client was active but not the user, so a deactivated user's access
  token kept working until it expired. It is now refused as soon as the
  deactivation is processed.
- **Every 401 carries a `WWW-Authenticate` challenge.** `/data`, `/logs`,
  `/history`, subscriptions and SSE answered 401 without the header RFC 6750
  requires. They now send `Bearer`, or `Bearer error="invalid_token"` when a
  token was refused.
- **Only a first-party token changes a password.** `/oauth/password` took a
  token for any audience from any client — a token minted for a partner API
  could change its user's password. It now requires the platform or identity
  audience, an active client and an active user.
- **Introspection reports service tokens as active.** A valid
  `client_credentials` token introspected as `{"active": false}`, because only
  tokens with a stored session row counted. It is now active while it is
  unexpired and its client and service user are active (RFC 7662 §2.2).
- **UserInfo answers only for a live user.** `/oauth/userinfo` returned the
  claims of a deactivated user, and nulls for a user that no longer exists. Both
  are now refused with `invalid_token`. A failure on the server's side is a 500
  `server_error` and is logged; it used to be a silent 403.
