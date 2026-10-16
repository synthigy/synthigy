# Signing users in to your app

Synthigy is the identity provider; your application is an OAuth client. Two
shapes cover nearly every application:

| Your app | Client | Flow | Who calls `/data` |
|---|---|---|---|
| Browser app (SPA) | public | authorization code + PKCE, in the browser | the browser, with the user's token |
| Server with a browser front (BFF) | confidential | authorization code + PKCE, on the server | the server: as itself, or as the user with `acting_as` |

Both start with a registered client and a redirect URI. The flows and
lifetimes behind them are in [OAUTH.md](OAUTH.md).

## 1. Register the client

A browser app:

```bash
synthigy iam add-client "My SPA" --id my-spa --type public \
  --grant authorization_code --redirect http://localhost:5173/
```

A backend:

```bash
synthigy iam add-client "My BFF" --id my-bff --type confidential \
  --grant client_credentials --grant authorization_code \
  --redirect http://localhost:5174/auth/callback \
  --api Synthigy --role "Dataset Explorer" --trusted
```

Add `--local` to go through the portal of the instance on this machine instead
of a connected server. Things that bite first-timers:

- **The redirect URI must match character for character** what your app
  sends: `/auth/callback` is not `/auth/callback/`, `http` is not `https`.
  A loopback URI (`localhost`, `127.0.0.1`) may use any port. Register one
  `--redirect` per URI, production included; the console edits them later.
- **`--api Synthigy` is what makes the backend's own tokens usable.** `/data`
  checks the token's audience; a client with roles but no API gets tokens
  that `/data` refuses.
- **A backend needs both grants:** `client_credentials` to call `/data` as
  itself, `authorization_code` to sign users in.
- **`--trusted`** lets the backend run requests as its users (`acting_as`)
  and skips the consent screen.
- **The secret prints once.** Only a hash is stored. `--secret` on the
  existing client sets a new one.

## 2. A browser app

The SDK runs the whole flow in the browser; the client is public and holds no
secret. Tokens stay in memory. A reload restores the session silently, through
a hidden iframe that rides the engine's session cookie, so the iframe's page
has to call `silentCallback()` before anything else:

```ts
import { createBrowserLogin, createClient, silentCallback } from '@synthigy/sdk'

if (!silentCallback()) {
  const login = createBrowserLogin({
    endpoint: SYNTHIGY_URL,
    clientId: 'my-spa',
    redirectUri: `${location.origin}/`,   // registered with --redirect
  })
  const synthigy = createClient({ endpoint: SYNTHIGY_URL, tokenProvider: login.tokenProvider() })

  if (login.isCallback()) {
    const { returnTo } = await login.complete()   // back from the login page
    history.replaceState(null, '', returnTo)
  } else {
    await login.renew().catch(() => login.start())  // restore, else sign in
  }
  render(login.user)   // id_token claims: sub, xid, …
}
```

Every call then carries the user's token, and `/data` applies the user's roles
and row rules ([ACCESS.md](ACCESS.md)). Renewal is automatic. The origin of
the redirect URI is allowed to call the API from the browser
([CORS.md](CORS.md)).

## 3. A backend

The SDK owns the protocol; sessions, cookies and routes stay yours. Between
the start of a login and the callback, the flow has to remember one thing per
`state` (the PKCE verifier and nonce); you give the SDK a store for it, backed
by whatever holds your sessions. The in-memory store is for one process only.

```ts
const synthigy = createClient({
  endpoint: SYNTHIGY_URL, clientId: CLIENT_ID, clientSecret: CLIENT_SECRET,
  loginStore: new MemoryLoginStore(),   // dev only
})

// GET /login
const { url } = await synthigy.login.start({
  redirectUri: `${BASE_URL}/auth/callback`,
  returnTo: req.query.returnTo ?? '/',
})
res.writeHead(302, { Location: url })

// GET /auth/callback?code=…&state=…   (or ?error=… when the user cancelled)
if (error) {
  const pending = await synthigy.login.cancel(state)
  return redirect(pending?.returnTo ?? '/')
}
const { user, returnTo } = await synthigy.login.complete({
  code, state, redirectUri: `${BASE_URL}/auth/callback`,
})
const sid = mySessionStore.create({ user })   // your session
setCookie(res, sid)
redirect(returnTo)

// Every call afterwards runs as the user
await synthigy.search('movie', args, selection, { actingAs: session.user.xid })
```

Python, Go, Clojure and PHP have the same `login.start` / `login.complete`
pair; each SDK's README shows it in its language ([SDKS.md](SDKS.md)).

`user.xid` comes out of the id_token, so no lookup is needed. Passing it as
`acting_as` makes the user's own roles and row rules apply, not the backend's
([OAUTH.md](OAUTH.md#acting-for-a-user)). Never let the browser choose that
value: the backend resolves the user from its own session. A call without
`acting_as` runs as the backend's service user.

When the browser reaches Synthigy at a different URL than your server does
(containers, proxies), pass that URL as `publicEndpoint` to `login.start`.

## 4. Signing out

Your app owns its session. `GET /oauth/logout` ends Synthigy's own session, the one
silent renewal rides on. Token and session lifetimes: [OAUTH.md](OAUTH.md#flows).

`sub` in the token is the username and `xid` the user's record id. Profile claims
(`name`, `email`) come from `GET /oauth/userinfo`; the browser login merges them in
with `loadUserInfo: true`.
