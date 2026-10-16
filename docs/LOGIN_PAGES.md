# Custom login pages

The sign-in, device, error and status pages can be your own. The server owns the
flow; your page collects input, keeps what it was handed and posts back. A working
starter: [example-login-page](example-login-page/). Every error and status code with
its default text: the console, **System → Login pages → Page reference**.

## Bundles

A bundle is a zip with `index.html` at its root, uploaded in the console under
**System → Login pages**. An upload is inactive until you preview and activate it.
Bundle `<name>` is served at `/login/<name>/`.

- `default` serves every client without its own bundle, and holds the `device.html`,
  `status.html` and `error.html` pages.
- An app's **Login page** setting names its own bundle. Inactive or missing falls
  back to `default`, then to the built-in page.

Limits: 8 MB zipped, 50 MB unzipped, web assets only. Pages are served with
`Content-Security-Policy: connect-src 'self'; frame-ancestors 'none'`. A file you
leave out falls back to the built-in page. The built-in assets (`/oauth/css/*`,
`/oauth/js/*`, `/oauth/images/*`) are yours to reuse.

## The one rule: send `state` back

The browser arrives at `/login/<name>/?state=<opaque blob>`. `state` is encrypted and
is the CSRF and replay protection. Copy it into a hidden field and post it, unchanged,
**explicitly to `/oauth/login`** — `/login/` itself is GET-only.

## index.html — sign-in

POST `state`, `username` and `password` to `/oauth/login`.

When the app passed an OIDC `login_hint`, the URL carries it too
(`?state=…&login_hint=…`, trimmed, at most 256 characters): use it to pre-fill the
username. It's only a hint from the app, so let the person change it.

- **Plain form post:** the server redirects to the app on success. On wrong
  credentials it shows the **built-in** page, so your branding is lost.
- **`fetch` with `Accept: application/json`** and `credentials: 'include'`: you stay
  on your page and drive navigation.

```json
{"ok": true, "redirect": "<redirect_uri>?code=…&iss=…&state=…"}
{"ok": true, "form_post": {"url": "<redirect_uri>", "params": {…}}}
{"ok": false, "error": "credentials"}
```

On `redirect`, set `window.location`; on `form_post`, build and submit that form.
Errors are `400` with a code: `credentials` (prompt again), `expired_code`,
`already_authorized`, `device_code_expired`, `ip_address`, `user_agent`,
`challenge`, `broken_flow`, `unknown`.

**"Continue with …" buttons.** `GET /oauth/federated/providers` lists the active
identity providers (`name`, `label`). Each button is a link to
`/oauth/federated/start?provider=<name>&state=<state>`, with the same `state`. No
`state` in your URL means nobody is signing in: show no buttons.

## error.html and status.html

Static pages, both in the `default` bundle. The server redirects to them with
everything in the query.

- **error.html** — requests the server must not redirect back to the app.
  `?error=<code>` and `?secure=true|false`; on `secure=true`, warn the person not to
  enter credentials and show no login link. Some codes add context such as
  `client-name`.
- **status.html** — where a flow ends with no app to return to: a device login, a
  cancelled sign-in, an expired code. `?value=success|error|canceled`, plus `error`,
  `flow`, `user`, `client_name` and `provider` when they apply.

## device.html

Answers `/oauth/device/activate` twice:

- **Entry**, no code in the URL: POST `user_code` to `/oauth/device/activate`.
- **Confirm**, `?user_code=XXXX`: show the code and POST `action=confirm` or
  `action=cancel` to `/oauth/device/activate`. The browser carries a short-lived
  `SameSite=Lax` cookie that the server set; no token in the page.

```html
<form method="post" action="/oauth/device/activate">
  <p>Confirm code: <b id="code"></b></p>
  <button name="action" value="confirm">Confirm</button>
  <button name="action" value="cancel">Cancel</button>
</form>
<script>document.getElementById('code').textContent =
  new URLSearchParams(location.search).get('user_code');</script>
```
