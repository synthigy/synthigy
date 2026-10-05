# Custom login pages

The sign-in, device, error and status pages can be replaced with your own.
The server owns the flow; a custom page is a thin view that collects input,
preserves what the server handed it, and posts back to the right endpoint. The
console shows this reference live, from the server's own message tables, under
**System → Login pages → Page reference**. A working starter is in
[example-login-page](example-login-page/).

## Bundles

Pages ship as a **bundle**: a zip with `index.html` at its root (a zipped
folder works too), uploaded in the console under **System → Login pages**.
Uploading stores a new, inactive version; preview it, then activate. An active
bundle named `<name>` is served at `/login/<name>/`.

- **Server-wide** — a bundle named `default`. Every client without its own
  bundle signs in there, and `device.html`, `status.html` and `error.html` are
  looked up in it.
- **Per client** — the app's **Login page** setting names a bundle (lowercase
  letters, digits, dashes). An inactive or missing bundle falls back to
  `default`, then to the built-in page.

A bundle may hold up to 8 MB zipped and 50 MB unzipped, and only web assets:
html, css, js, json, images, fonts. Every page is served with
`Content-Security-Policy: connect-src 'self'; frame-ancestors 'none'`: it can
`fetch` only its own deployment and cannot be framed. A file you leave out is
replaced by the built-in page, so brand what you want and inherit the rest.

The built-in assets (`/oauth/css/*`, `/oauth/js/*`, `/oauth/images/*`) are
yours to reuse; the contract is the markup and the POST targets, not the CSS.

## The one rule: preserve `state`

Both interactive flows send the browser to your login page as

```
/login/<name>/?state=<opaque blob>
```

`state` is encrypted and tamper-proof. It carries the pending flow and its
security binding, and it **is** the CSRF and replay protection; there is no
separate token. Your page never reads it, only sends it back.

The built-in page lives at `/oauth/login`, so a form with no `action` posts
back to it and `state` rides along. A custom page lives under `/login/`, which
is GET-only, so it must:

- post **explicitly to `/oauth/login`** (`action="/oauth/login"`, or a `fetch`
  to it), and
- carry `state` **itself**: read it from this page's `?state=` and put it in a
  hidden `state` field or the `fetch` body.

## index.html — sign-in

Served at `/login/<name>/`. On GET, render your form with `?state=` copied
into a hidden field. POST to `/oauth/login` with:

| field | required |
|---|---|
| `state` | yes, the blob from your `?state=` |
| `username` | yes |
| `password` | yes |

Two response modes, chosen by the `Accept` header.

**HTML mode** (no special header): the server drives the browser. Success is a
`302` to the client's `redirect_uri` (or to `/oauth/status`) and sets the
session cookie. On bad credentials the server re-renders the **built-in** login
page, since it cannot re-render yours, so your branding is lost on error.

**JSON mode** (`Accept: application/json`, POST via `fetch` with
`credentials: 'include'` so the session cookie sticks): the server answers JSON
and you drive navigation.

Success, authorization code:

```json
{"ok": true, "redirect": "<redirect_uri>?code=…&iss=…&state=…"}
```

or, when the client asked for `response_mode=form_post`, a form to build and
submit:

```json
{"ok": true, "form_post": {"url": "<redirect_uri>", "params": {…}}}
```

Success, device code:

```json
{"ok": true, "redirect": "/oauth/status?value=success&client=…&user=…"}
```

Errors are always `400 {"ok": false, "error": "<code>"}`:

| `error` | meaning |
|---|---|
| `credentials` | wrong username or password; prompt again |
| `expired_code` | the authorization code's grace period expired |
| `already_authorized` | the device was already authorized |
| `device_code_expired` | the device code expired |
| `ip_address`, `user_agent`, `challenge` | a security binding failed |
| `broken_flow` | unrecognized flow |
| `unknown` | anything else |

On success, `window.location = redirect`.

### "Continue with …" buttons

The built-in page shows one button per active identity provider. A custom page
gets the same with two public requests, no auth and no secrets:

1. `GET /oauth/federated/providers` returns
   `[{"name": "google", "provider": "google", "label": "Google"}, …]`.
   `name` is the slug an administrator gave the provider; `label` is display
   text. An empty array means password only; render nothing.
2. Each button is a plain link to
   `/oauth/federated/start?provider=<name>&state=<state>`, with the **same**
   `state` your password form carries. The server does the rest: the provider
   redirect, the callback and the final redirect to the client.

A sign-in the person can retry (cancelled at the provider, identity not
linked, …) comes back to this page as
`?error=<code>&state=<state>`: show the message and use that `state` as usual.
The codes and their copy are in the example page's `FEDERATED` map. A page
that forwards to a provider on load must not do so when `?error` is present,
or the person bounces between your page and the provider. A failure with no
flow left to resume lands on `/oauth/status?flow=federated&error=…`.

No `state` in your URL means someone opened the page directly; there is no
flow to continue, so skip the buttons.

## error.html

Static: no credentials, no POST, no `state`. With an `error.html` in the
`default` bundle the server redirects to it instead of rendering the built-in
page, with everything the page needs in the query:

```
/login/default/error.html?error=redirect_missmatch&secure=true
/login/default/error.html?error=invalid_login_page&secure=false&client-name=acme&invalid-value=http%3A%2F%2Fevil
```

| param | meaning |
|---|---|
| `error` | the code, a key into your message copy |
| `secure` | `true`: a security error. Warn the person not to enter credentials and show no login link. |
| others | context for some errors, e.g. `client-name`, `invalid-value` |

| code | secure | built-in text |
|---|---|---|
| `client_not_registered` | yes | Unknown Application — This application is not recognized by the authorization server. Do not enter any credentials. |
| `corrupt_session` | no | Session Error — Your authorization session could not be verified. Please try again or contact support. |
| `invalid_login_page` | no | Configuration Error — This app's Login page setting is not a bundle name (context: `client-name`, `invalid-value`). |
| `missing_redirect` | no | Invalid Request — The authorization request is missing required information. Please contact the application developer. |
| `missing_response_type` | no | Invalid Request — The authorization request is incomplete. Please contact the application developer. |
| `no_redirections` | yes | Authorization Blocked — This application has no configured redirect addresses. The authorization request cannot be completed safely. |
| `redirect_missmatch` | yes | Authorization Blocked — The return address provided by this application doesn't match its configuration. Do not enter any credentials. |
| `unsupported_grant_type` | no | Unsupported Request — The requested authorization method is not supported. Please contact the application developer. |

## status.html

Static. Where a flow **ends** when there is no client to send the browser back
to: a device login finished or cancelled, a cancelled federated sign-in, an
expired code. With a `status.html` in the `default` bundle, `/oauth/status` and
`/oauth/device/status` redirect to it. Everything arrives in the query, and any
of it may be absent:

| param | meaning |
|---|---|
| `value` | `success` or `error` |
| `flow` | which flow ended: `device_code`, `login`, … |
| `error` | the code when `value=error` |
| `error_description` | human text for some errors |
| `user` | who signed in, on success |
| `client`, `client_name` | the app a device login was for: id and display name |
| `provider` | the identity provider involved, if any |

A device cancel arrives as `value=canceled` with no `error`; a cancelled login
as `value=error&error=cancelled`. `idp_<code>` is the identity provider's own
OAuth error, e.g. `idp_access_denied` when the person cancels there. *retry*
means the person can start again; *support* means an administrator is needed.

| code | kind | built-in text |
|---|---|---|
| `access_denied` | support | Access was denied. |
| `already_authorized` | retry | Someone already authenticated using this code. |
| `broken_flow` | retry | The authorization flow is broken. |
| `callback_error` | support | The identity provider returned an error. |
| `cancelled` | retry | You cancelled the sign-in. |
| `challenge` | retry | A potentially malicious challenge change was detected. |
| `claim_invalid` | retry | This invitation link is invalid or has already been used. |
| `claim_method_not_allowed` | support | This invitation doesn't allow that sign-in method. |
| `device_code_expired` | retry | The code you entered has expired. |
| `email_exists` | retry | An account with this email already exists. Sign in with your existing method, then link this identity. |
| `expired_code` | retry | Your login grace period has expired. |
| `identity_already_linked` | support | This external identity is already linked to an account. |
| `ip_address` | retry | A potentially malicious IP-address change was detected. |
| `link_reauth_required` | retry | For your security, please sign in again before linking a new login method. |
| `link_requires_login` | retry | You must be signed in to link an external account. |
| `link_session_mismatch` | support | Account-link verification failed. Please try linking again. |
| `no_code` | retry | The identity provider sent no authorization code. |
| `no_flow` | support | The sign-in request carried no flow to continue. |
| `not_linked` | retry | This external identity isn't linked to any account. |
| `provider_disabled` | support | That sign-in method is switched off. |
| `provider_unknown` | support | That sign-in method doesn't exist. |
| `provider_unsupported` | retry | Could not reach that identity provider. |
| `provision_failed` | support | We couldn't create your account. |
| `reauth_identity_mismatch` | retry | You verified with a different account than the one signed in. |
| `state_invalid` | retry | That sign-in link expired. |
| `token_invalid` | support | The identity provider's response couldn't be verified. |
| `user_agent` | retry | A potentially malicious app change was detected. |

## device.html

Static, and it answers two steps of `/oauth/device/activate`.

- **Entry** — a GET without `user_code`. With a `device.html` in the `default`
  bundle the server redirects there. Your page POSTs `user_code` to
  `/oauth/device/activate`; the server finds the code and redirects to login.
- **Confirm** — a GET with `?user_code=XXXX`, the link a device shows. The
  server sets a short-lived `device_confirm` cookie (`HttpOnly`, `Secure`,
  `SameSite=Lax`, five minutes) and redirects to
  `/login/default/device.html?user_code=XXXX`. Your page shows the code from
  the URL and POSTs `action=confirm` or `action=cancel` to
  `/oauth/device/activate`, with an explicit form action. The browser sends
  the cookie; the server validates it. Confirm redirects to login with
  `state`; cancel to the device status page; no cookie is a `400`.

```html
<form method="post" action="/oauth/device/activate">
  <p>Confirm code: <b id="code"></b></p>
  <button name="action" value="confirm">Confirm</button>
  <button name="action" value="cancel">Cancel</button>
</form>
<script>document.getElementById('code').textContent =
  new URLSearchParams(location.search).get('user_code');</script>
```

The cookie is `SameSite=Lax`, so the browser withholds it from any cross-site
POST; no form token is needed. The explicit Confirm button is the anti-phishing
step the device flow requires.

## Summary

| page | what reaches it |
|---|---|
| `index.html` | `?state=` in the URL; results as a redirect or as JSON |
| `status.html` | `?value=`, `?error=` and context in the URL; nothing to post |
| `error.html` | `?error=`, `?secure=` and context in the URL; nothing to post |
| `device.html`, entry | nothing; it posts `user_code` |
| `device.html`, confirm | the `device_confirm` cookie, set on the activation GET; it posts `action` |

Preserve what you are handed, post back to the right endpoint, and everything
else is yours to style.
