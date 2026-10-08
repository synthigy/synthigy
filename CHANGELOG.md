# Changelog

All notable changes to the Synthigy engine. Releases before 0.2.14 are
described in their [GitHub release notes](https://github.com/synthigy/synthigy/releases).

## [0.2.14-SNAPSHOT] — 2026-10-08

### Breaking
- **`SYNTHIGY_SERVER_TRUST_PROXY` lists your proxies.** It takes IPs or CIDRs,
  comma separated — `127.0.0.1/32,::1/128` for a proxy on the same host —
  instead of `true`. `X-Forwarded-For` is believed only from those addresses
  and is read from the right, skipping trusted hops, so a proxy may overwrite
  or append to the header. `true` is ignored with a warning: until you change
  it, every request is recorded with the proxy's address. See
  [PROXY.md](docs/PROXY.md).

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

### Fixed
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
