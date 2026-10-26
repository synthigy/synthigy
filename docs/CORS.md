# Browser origins (CORS)

A web app on another origin — an SPA using the SDK's browser login, the
Synthigy tools embedded in your site — calls Synthigy straight from the
browser. Synthigy answers those calls only for origins it knows:

- the origin of every **redirect URI** registered on an active OAuth client.
  Registering `https://app.example.com/callback` allows
  `https://app.example.com`;
- for a loopback redirect registered without a port
  (`http://localhost/callback`), **any port** on the same scheme and host, while
  the engine itself runs on localhost: `http://localhost:3001` works too. With a
  port (`http://localhost:8000`) only that origin is allowed. `127.0.0.1` is a
  different host and needs its own registration;
- the desktop app's webview origins, `tauri://localhost` and
  `http://tauri.localhost`, always;
- anything listed in `SYNTHIGY_SERVER_ALLOWED_ORIGINS` (comma-separated), for
  pages that read public data without an OAuth client of their own:

```bash
SYNTHIGY_SERVER_ALLOWED_ORIGINS=https://status.example.com,https://docs.example.com
```

Other origins get no CORS headers, so the browser refuses to read the
response.

Cross-origin calls never carry cookies: Synthigy does not send
`Access-Control-Allow-Credentials`, so a fetch with `credentials: 'include'`
from another origin is refused. Send the access token in the `Authorization`
header instead.

Server-to-server calls (a backend using an SDK) are not affected —
CORS is a browser rule. Discovery (`/.well-known/*`) stays readable from any
origin.

Changes to a client's redirect URIs take effect within 30 seconds.

Without the OAuth module (a server running without IAM) every origin is
allowed, as there are no credentials to protect.
