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
| `index.html` | `/login/<name>/` (login)           | posts `username`/`password` **+ the `state` blob** to `/oauth/login` (JSON mode, so errors stay on-page); renders federated "Continue with X" buttons from `/oauth/federated/providers`; a cancel link (`/oauth/login?state=…&cancel=1`) ends the flow |
| `error.html` | `/login/default/error.html`       | renders the error from `?error=` / `?secure=` (+ `client-name`, `redirect-uri`, `invalid-value`) |
| `status.html`| `/login/default/status.html` | final outcome when there's no app to return to — every `?error=` code mapped |
| `files.html` | `/login/default/files.html`       | a browser opened a private or missing file link: `?error=unauthorized` + `uri` |
| `account.html` | `/login/default/account.html`   | the signed-in user's own account: display name, password, sign-in methods, sessions — all through `fetch` to `/oauth/account`; signed out, it follows `login_url` |
| `device.html`| `/login/default/device.html`       | entry (type the code → `user_code`) and confirm (cookie carries state, POST `action`) — same segmented code boxes as the built-in |

## The look

These pages **reuse the built-in assets** (`/oauth/css/*`, `/oauth/js/*`,
`/oauth/images/favicon.png`), so they look like the built-in pages out of the box:
the same markup and class names, with your brand in place of Synthigy's.

- **Shell** — `.sy-shell` holds a 36px brand bar (`.sy-bar`), the stage with the
  card (`.sy-stage > .sy-card`) and a status line (`.sy-status`). The bar starts
  with your brand: the examples show an `Acme` wordmark (`.acme-mark`, styled in
  each page's `<style>` block) where the built-in pages show the Synthigy lockup.
  `.sy-bar-chip` on the right names the app, when the page knows it.
- **Card** — `.sy-card-head` (optional `.sy-tile` icon, `.sy-card-title`,
  `.sy-card-sub`), `.sy-card-body`, `.sy-card-foot`. `.sy-card--wide` for
  prose-heavy pages such as the error page.
- **Fields** — `.sy-field` > `.sy-label-row` (`.sy-label`, optional `.sy-tag`) +
  `.sy-control` > `.sy-input`. Buttons: one `.sy-btn--primary` per card,
  `.sy-btn--secondary` for the rest, each with a `.sy-btn-label` and a `.sy-spin`.
- **Messages never move the card.** On a form page the line under the title is a
  fixed-height voice row (`.sy-voice`): it shows the sub text, and a message
  replaces it in place. Write messages with
  `SynthigyPage.message('voice', text, tone)` (tone `error`, `warn`, `info`,
  `success`), clear with `SynthigyPage.message('voice', '')`.
- **Starfield** — `/oauth/js/starfield.js` draws the sky behind the page. Drop the
  script tag for a plain background.

`/oauth/js/shell.js` wires the shared behaviour by attribute:

| hook | does |
|------|------|
| `window.SynthigyPage.message(el, text, tone)` | message into a voice row (element or id) |
| `window.SynthigyPage.busy(form, on)` | spinner on the form's submit button, button keeps its size |
| `form[data-sy-busy]` | busy state on a plain (non-fetch) submit |
| `[data-sy-host]` | filled with `location.host` |
| `[data-sy-reveal="<input id>"]` | password show / hide toggle |
| `[data-sy-caps="<input id>"]` | tag shown while Caps Lock is on |

To rebrand, change the `.acme-mark` wordmark, or swap the
`<link rel="stylesheet">` for your own — the contract is the markup and the POST
targets, not the CSS.

The "Continue with X" buttons arrive after a fetch, so `index.html` keeps the
card transparent until they are in (1.5 s at most) and it never grows in front
of the person. If you know your providers, write the links into the markup and
show the card right away.

## The one rule

The server owns the flow; these pages only preserve what the server hands them —
- login: the opaque `?state=` → hidden field → POST to `/oauth/login`
- device-confirm: the `device_confirm` cookie (the browser sends it; the page
  posts only `action`)
- error: read `?error=` and display

Break that round-trip and the flow breaks. Everything else is yours to style.
