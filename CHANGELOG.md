# Changelog

All notable changes to the Synthigy engine. Releases before 0.2.14 are
described in their [GitHub release notes](https://github.com/synthigy/synthigy/releases).

## [0.2.14-SNAPSHOT] — 2026-10-09

### Breaking
- **`SYNTHIGY_SERVER_TRUST_PROXY` lists your proxies.** It takes IPs or CIDRs,
  comma separated — `127.0.0.1/32,::1/128` for a proxy on the same host —
  instead of `true`. `X-Forwarded-For` is believed only from those addresses
  and is read from the right, skipping trusted hops, so a proxy may overwrite
  or append to the header. `true` is ignored with a warning: until you change
  it, every request is recorded with the proxy's address. See
  [PROXY.md](docs/PROXY.md).

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
- **Clojure API: IAM model names are Synthigy names.** For embedders and
  library callers: `:iam/id` is `:synthigy/id` (same xid);
  `synthigy.iam/current-version` and `register-reference-types!` are
  `synthigy.dataset/current-synthigy-version` and
  `synthigy.dataset/register-reference-types!`; the patch topic
  `:synthigy.iam/model` is `:synthigy/model`; `dataset/iam.json` is
  `dataset/synthigy.json`. Bootstrap log ids changed with them
  (`:synthigy-model-loading`, `:synthigy-model-mounted`,
  `:synthigy.iam/deploying-synthigy-model`).
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

### Changed
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
- **Undertow: `/data/events` accepts `?access_token`** like the other
  adapters; a browser `EventSource` got 401.
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
