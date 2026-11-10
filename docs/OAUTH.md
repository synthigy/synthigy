# Sign-in and OAuth

Synthigy is an OAuth 2.1 and OpenID Connect provider. Your applications sign
users in through it and call `/data` with the tokens it issues. Discovery is at
`/.well-known/openid-configuration`.

Step by step, with code: [SIGN_IN.md](SIGN_IN.md).

## Applications

An application is an OAuth **client**, created in the console under
Administration → Apps or with `synthigy iam add-client` ([SIGN_IN.md](SIGN_IN.md)).

- **Public** clients run where a secret cannot be kept: browsers, mobile and
  desktop apps. They sign users in with the authorization code flow.
- **Confidential** clients run on a server and hold a secret. Each has its own
  **service user**: tokens the client gets with its own credentials run as that
  user, with the roles you give it.

A confidential client can be marked **trusted** (`--trusted`): it skips the
consent screen, and it may run `/data` requests as its users
([Acting for a user](#acting-for-a-user)).

A client lists the **redirect URIs** it may return to, matched exactly. Loopback
URIs (`http://localhost`, `http://127.0.0.1`) have two exceptions:

- a **native** app (application type `native`) may use
  any port on any of its loopback URIs (RFC 8252);
- for a web client, a loopback URI registered **without** a port
  (`http://localhost/callback`) accepts any port, but only while the engine itself
  runs on localhost. One registered with a port accepts only that port.

A native app never gets a silent `prompt=none` answer: the engine replies
`interaction_required`, because a loopback redirect can't prove which app is
asking (RFC 8252 §8.6). Its redirect origins are also the browser origins
allowed to call the API ([CORS.md](CORS.md)).

## Flows

| Grant | For |
|---|---|
| `authorization_code` | Users signing in through a browser. PKCE with `S256` is required for every client, public or confidential. |
| `client_credentials` | A backend acting as itself. |
| `refresh_token` | Renewing a user's access without signing in again. Request the `offline_access` scope to get one. |
| `urn:ietf:params:oauth:grant-type:device_code` | Devices and CLIs without a browser — `synthigy connect` uses it. The user confirms a code at `/oauth/device/activate`. |

Access tokens are signed JWTs and last **15 minutes**; refresh tokens last
**24 hours**. Both can be changed per client. A signed-in session ends after 24
hours without activity, and after 30 days in any case.

| Endpoint | |
|---|---|
| `GET /oauth/authorize` | Start a sign-in |
| `POST /oauth/token` | Exchange a code, refresh, client credentials, device code |
| `POST /oauth/device/auth` | Start a device sign-in |
| `GET /oauth/userinfo` | Claims of the signed-in user |
| `POST /oauth/introspect` | Check a token |
| `POST /oauth/revoke` | Revoke a token |
| `GET /oauth/jwks` | Public keys that verify tokens |
| `GET,POST /oauth/logout` | Sign out (without `id_token_hint` the user confirms first) |

## APIs, audiences and scopes

An **API** is what tokens are issued for: it has an **audience** and the
**scopes** that can be granted on it. The built-in API is **Synthigy**, with
audience `https://synthigy.com`; `/data` accepts tokens for that audience. Its
scopes are listed in [ACCESS.md](ACCESS.md#scopes). Define your own APIs to
issue tokens for your own services.

## Acting for a user

A backend that serves users — a BFF, a job runner — can run a `/data` request
as one of them with `"acting_as": "<user xid>"`. The user's own roles and row
rules then apply, not the backend's. This is allowed only for a
**confidential** client marked **trusted**; anything else is refused with
`PUBLIC_CLIENT_FORBIDDEN` or `NOT_TRUSTED`, and an unknown or deactivated user
with `USER_NOT_FOUND` or `USER_INACTIVE`.

## Identity providers

Add Google, Microsoft, LinkedIn, GitHub, Facebook, Discord or up to three custom
OpenID Connect providers in the console under Administration → Identity Providers;
it shows the redirect URI to register with the provider. A user can link several
sign-in methods to one account ([Your account](#your-account)).

## Your account

Every user manages their own account at `/oauth/account`, with no role needed:
display name, password, sign-in methods, and the browsers, apps and devices
signed in. A browser gets the account page; signed out, it goes through the
login page first, with a password or a linked provider. Anything else gets
JSON. Link to it from your app as "Manage account":
`/oauth/account?client_id=<your client id>&return=<a redirect URI of that client>`
adds "Back to <app>" (and `back` in the JSON). A `return` that is not one of the
client's registered redirect URIs is ignored.

| Endpoint | |
|---|---|
| `GET /oauth/account` | `profile` (`xid`, `login`, `display_name`, `has_password`), `identities`, `sessions`, `fresh` |
| `POST /oauth/account/profile` | `{"display_name"}`; empty goes back to the default |
| `POST /oauth/account/password` | `{"current_password", "new_password"}`; every session ends, this one too |
| `POST /oauth/account/reauth` | `{"password"}`: confirm it's you |
| `POST /oauth/account/identities/unlink` | `{"xid"}` of a sign-in method |
| `POST /oauth/account/sessions/revoke` | `{"xid"}` of a session, or `{"all_others": true}` |

- Every call acts on the caller; a user named in the body is ignored. The caller
  is a bearer token for the `https://synthigy.com` audience, else the browser's
  sign-in cookie.
- With the cookie, a POST must come from the engine's own pages
  (`Sec-Fetch-Site`, else `Origin`, else `Referer`); anything else gets
  `403 cross_origin`.
- Unlinking and `all_others` need a sign-in within the last 5 minutes (`fresh`),
  else `403 reauth_required`. Confirm with `/oauth/account/reauth`, or with a
  linked provider at
  `/oauth/federated/start?provider=<name>&mode=reauth&return=/oauth/account`.
- A session's handle is its `xid`; the session id itself is never shown.
- A display name is free text, not unique: at most 64 characters, with control
  and format characters (bidi overrides included) removed. The `xid` is what
  identifies a user.
- Errors: `login_required` (with `login_url`), `invalid_token`, `cross_origin`,
  `reauth_required`, `invalid_request`, `invalid_credentials`,
  `password_too_short`, `no_local_credential`, `display_name_too_long`,
  `last_login_method`, `identity_not_found`, `session_not_found`, `missing_xid`.

## See also

- Checking passwords against your own directory: [IAM_CONNECTORS.md](IAM_CONNECTORS.md)
- Your own login pages: [LOGIN_PAGES.md](LOGIN_PAGES.md)
- New accounts and invitation links: [ACCESS.md](ACCESS.md#delegated-administration)
- Public URL and TLS: [PROXY.md](PROXY.md)
