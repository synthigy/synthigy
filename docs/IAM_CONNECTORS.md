# Login connectors

When someone signs in with a username and password, Synthigy asks a **chain of
connectors** whether the credentials are good. The chain starts with one
connector, **Local database**, which checks the password stored on the user.
Add connectors to check credentials somewhere else — a corporate directory, a
legacy user database, an LDAP server behind a small adapter.

Sign-in with an external identity provider (Google, Microsoft, any OpenID
Connect provider) is separate — see [OAUTH.md](OAUTH.md#identity-providers).

## The chain

Connectors run in **priority** order, lowest first. Each answers one of:

| Answer | The chain |
|---|---|
| valid | stops: the user is signed in |
| unknown user | asks the next connector |
| invalid credentials | stops: sign-in is refused |
| error (unreachable, timeout, bad response) | stops: sign-in is refused |

An error never falls through to the next connector, so making a connector
unreachable cannot be used to get around it.

When a connector accepts a user that does not exist in Synthigy yet, Synthigy
creates the user on the spot (active, with the name the connector returned).
Give the new user roles and groups afterwards, in the console or over `/data`.

## Managing connectors

In the console under **Administration → Connectors**. A new connector is
created disabled: configure it, use **Dry-run** to test a username and password
against that connector alone, then enable it.

Connectors are IAM records like users and roles, so they can also be managed
over `/data` (entity `auth_connector`). **Local database** cannot be deleted — every account without an external
identity depends on it.

## Webhook connector

The built-in way to plug in anything: Synthigy POSTs the credentials to a URL
you run, and your service answers.

| Setting | |
|---|---|
| Webhook URL | Your endpoint. Use HTTPS — it receives passwords. |
| Signing secret | When set, every request is signed (see below). |
| Timeout (ms) | Default 1500. A timeout is an error, which refuses the sign-in. |

Request:

```http
POST /verify
Content-Type: application/json
X-Synthigy-Signature: hmac-sha256=<base64 HMAC-SHA256 of the body>

{"username": "alice", "password": "…", "request_id": "…"}
```

Answers:

```json
{"ok": true, "user": {"name": "alice"}}
{"ok": false, "reason": "unknown_user"}
{"ok": false, "reason": "invalid_credentials"}
```

`user.name` is required: it is the Synthigy username the sign-in resolves to.
Any other `reason` refuses the sign-in, as do a non-200 status and a body that
is not one of these shapes.

Verify the signature over the **raw** request body, before parsing it — a
re-serialized body will not match — and compare in constant time. In Node:

```js
import crypto from "node:crypto"

app.post("/verify", express.text({ type: "application/json" }), (req, res) => {
  const expected = "hmac-sha256=" +
    crypto.createHmac("sha256", process.env.SECRET).update(req.body).digest("base64")
  const got = req.get("X-Synthigy-Signature") ?? ""
  if (expected.length !== got.length ||
      !crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(got))) {
    return res.status(401).end()
  }
  const { username, password } = JSON.parse(req.body)
  // check username and password against your directory
  res.json({ ok: true, user: { name: username } })
})
```


**LDAP and Active Directory** have no built-in connector: run a small webhook
that binds against the directory and answers as above.

## Your own connector type (embedded)

When Synthigy is embedded in a Clojure application ([EMBEDDED.md](EMBEDDED.md)),
a connector type can be plain code:

```clojure
(ns acme.auth
  (:require [synthigy.iam.connector :as connector]))

(defmethod connector/verify-credentials :mainframe
  [{:keys [endpoint]} {:keys [username password]}]
  (try
    (if (mainframe/check endpoint username password)
      {:ok true :user {:name username}}
      {:ok false :reason :invalid-credentials})
    (catch java.io.IOException e
      {:ok false :reason :error :error e})))
```

Load the namespace before the first sign-in (list it in `SYNTHIGY_REQUIRE`
under the portal) and add a connector of that type; its other settings arrive
as the first argument:

```clojure
(connector/save-connector!
  {:type :mainframe :name "Mainframe" :priority 50 :endpoint "tcp://mf.internal:5040"})
```

Use a type name without a namespace.
