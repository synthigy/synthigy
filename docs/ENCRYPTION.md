# Encryption

Attributes of type `encrypted` are encrypted before they reach the database,
and so are Synthigy's own secrets, such as token signing keys. Reading them
over `/data` returns plain values to callers allowed to read them; the database
and its backups only ever hold ciphertext.

## How it works

- **Data keys** encrypt the values (AES-256-GCM). They are stored in the
  database, in the `__deks` table — but only wrapped.
- The **master key** wraps the data keys. It is never stored in the database.
  Who holds it is the **custody**:

| Custody | The master key lives | Configure with |
|---|---|---|
| Local | in the instance's `.env` | `SYNTHIGY_ENCRYPTION_MASTER_KEY` |
| Vault Transit | in HashiCorp Vault — it never leaves Vault | `SYNTHIGY_VAULT_*` |
| Webhook | in your own KMS or HSM, behind an HTTP endpoint | `SYNTHIGY_ENCRYPTION_WEBHOOK_*` |

See [ENV.md](ENV.md#encryption) for every setting. When Vault or a webhook is
configured, it is used and the local key is not. When none is, a fresh
instance started with `synthigy up` gets a generated local key in its `.env`.

If the engine cannot unwrap its data keys — wrong master key, Vault
unreachable — it refuses to start, and the portal says why. It never
runs with data it cannot read.

> **Back up the master key separately from the database.** A database backup
> without its master key is unreadable, by design.

## Managing keys

In the **portal** (`synthigy console`), the Encryption panel:

- **Change custody** — move from local to Vault or a webhook, or back. **Test
  connection** round-trips throwaway data through the new provider before
  anything changes. Data keys are rewrapped under the new custody and the
  engine restarts.
- **Rotate the master key** (local custody) — a new key is generated, every
  data key is rewrapped under it and the key is saved to `.env`. You never
  handle key material.
- **Rewrap** (Vault) — after rotating the key inside Vault, rewrap the data
  keys under its newest version. The panel shows when they lag behind.

In the **console**, System → Encryption lists the data keys. **Rotate**
starts a new data key for new writes. Existing values are not rewritten: each
keeps the key it was written with and moves to the new key the next time it
is written. Old data keys are kept so everything stays readable.

A database restored from another instance needs that instance's master key.
The setup page of a new instance detects encrypted data in the database it is
pointed at and asks for that key before starting the engine.

## Webhook custody

Synthigy calls your endpoint to wrap and unwrap data keys:

```http
POST <SYNTHIGY_ENCRYPTION_WEBHOOK_URL>
X-Synthigy-Signature: hmac-sha256=<base64 HMAC-SHA256 of the body>

{"op": "wrap",   "dek": "<base64>", "request_id": "…"}  →  {"wrapped": "<opaque>"}
{"op": "unwrap", "wrapped": "<opaque>", "request_id": "…"}  →  {"dek": "<base64>"}
```

The signature header is sent when `SYNTHIGY_ENCRYPTION_WEBHOOK_SECRET` is set.
`wrapped` is stored as is and handed back on unwrap; its format is yours. Any
error or timeout makes the call fail, and the engine does not start without
its keys.
