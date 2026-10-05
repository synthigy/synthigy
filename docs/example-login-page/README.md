# Example custom login-page bundle

A working starter for a custom OAuth page bundle. Zip this folder and upload it
in the console under **System → Login pages**:

```bash
zip -r login-page.zip example-login-page -x '*.md'
```

Name the bundle `default` to replace the built-in pages for every client, or
anything else and pick it per app (the app's **Login page** setting). Uploading
stores an inactive version — preview it, then activate. Served at
`/login/<name>/`. Any file present here is used; anything missing falls back to
the built-in page. See [LOGIN_PAGES.md](../LOGIN_PAGES.md) for the full contract.

| file         | route it answers            | what it does |
|--------------|-----------------------------|--------------|
| `index.html` | `/login/<name>/` (login)           | posts `username`/`password` **+ the `state` blob** to `/oauth/login` (JSON mode, so errors stay on-page); renders federated "Continue with X" buttons from `/oauth/federated/providers` |
| `error.html` | `/login/default/error.html`       | renders the message from `?error=` / `?secure=` |
| `status.html`| `/login/default/status.html` | final outcome when there's no app to return to — every `?error=` code mapped |
| `device.html`| `/login/default/device.html`       | entry (type the code → `user_code`) and confirm (cookie carries state, POST `action`) — same PIN boxes as the built-in |

These pages **reuse the built-in assets** (`/oauth/css/*`, `/oauth/js/*`,
`/oauth/images/favicon.png`) so they work and match out of the box. To rebrand,
swap the `<link rel="stylesheet">` for your own — the contract is the markup and
the POST targets, not the CSS.

**The one rule:** the server owns the flow; these pages only preserve what the
server hands them —
- login: the opaque `?state=` → hidden field → POST to `/oauth/login`
- device-confirm: the `device_confirm` cookie (the browser sends it; the page
  posts only `action`)
- error: read `?error=` and display

Break that round-trip and the flow breaks. Everything else is yours to style.
