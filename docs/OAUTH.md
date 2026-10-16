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

A client lists the **redirect URIs** it may return to — exact matches, except
that a loopback URI (`http://localhost`, `http://127.0.0.1`) may use any port.
Its redirect origins are also the browser origins allowed to call the API
([CORS.md](CORS.md)).

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
| `GET,POST /oauth/logout` | Sign out |

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
sign-in methods to one account (Your account → Sign-in methods).

## See also

- Checking passwords against your own directory: [IAM_CONNECTORS.md](IAM_CONNECTORS.md)
- Your own login pages: [LOGIN_PAGES.md](LOGIN_PAGES.md)
- New accounts and invitation links: [ACCESS.md](ACCESS.md#delegated-administration)
- Public URL and TLS: [PROXY.md](PROXY.md)
