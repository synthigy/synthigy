# Running Synthigy behind a reverse proxy

Synthigy serves plain HTTP on one port (`SYNTHIGY_SERVER_PORT`, default 7887).
It terminates no TLS and does no virtual hosting, so any internet-facing
instance sits behind nginx, Caddy, Traefik or a cloud load balancer.

This page is what that proxy has to do. Copy-paste configs are at the bottom.

## TLS is not optional

Session and login cookies are minted `Secure` + `SameSite=None`. A browser
discards those over plain `http://` on anything but `localhost`, so an instance
published over HTTP appears to accept the password and then bounces straight
back to the login page. Terminate TLS at the proxy and keep the engine on
loopback:

```bash
SYNTHIGY_SERVER_HOST=127.0.0.1
SYNTHIGY_SERVER_PORT=7887
```

## Two engine settings

```bash
SYNTHIGY_SERVER_TRUST_PROXY=true
SYNTHIGY_IAM_ROOT_URL=https://synthigy.example.com
```

**`SYNTHIGY_SERVER_TRUST_PROXY`** makes Synthigy read the client address from
`X-Forwarded-For` rather than from the connection. Set it whenever a proxy is
in front; without it, sessions and login events on a Jetty/Undertow deployment
are all recorded against the proxy's own address.

On the default http-kit adapter the flag changes nothing by itself — http-kit
resolves `X-Forwarded-For` inside `getRemoteAddr()` before Ring ever sees the
request, and it does so unconditionally, with no trust setting of its own. Set
it anyway: it states the deployment's intent and it is what makes the behavior
the same across adapters.

**`SYNTHIGY_IAM_ROOT_URL`** is the public origin. Unset, the OAuth issuer,
redirect URIs and login links are derived per request from `X-Forwarded-Host`
and `X-Forwarded-Proto` — fine on a laptop, wrong in front of a load balancer,
and attacker-steerable if the proxy passes through what the client sent. The
operator console flags this until it is set.

## The proxy is what makes X-Forwarded-For trustworthy

Both Synthigy and http-kit take the **first** element of `X-Forwarded-For` as
the client address. Nothing checks who wrote it. An engine reachable directly
from the internet therefore records whatever address the caller claims — so
the proxy is not just a TLS terminator here, it is the only thing that makes
the recorded client address mean anything. Two rules follow.

**Never expose the engine port directly** once a proxy is in front. Bind it to
loopback, or firewall it; a request that reaches 7887 around the proxy carries
its own idea of who sent it.

**Have the proxy overwrite the header, not append to it.** The idiomatic nginx
value `$proxy_add_x_forwarded_for` *appends* to whatever the client sent, which
leaves the client's own value in first position — exactly the slot that is
read:

| proxy   | directive                                           |
|---------|-----------------------------------------------------|
| nginx   | `proxy_set_header X-Forwarded-For $remote_addr;`     |
| Caddy   | `header_up X-Forwarded-For {remote_host}`            |
| Traefik | default — leave `forwardedHeaders.trustedIPs` unset  |

Forward `X-Forwarded-Proto` and `X-Forwarded-Host` too; the OAuth layer builds
absolute URLs from them whenever the root URL is unset.

If a second proxy or a cloud load balancer sits in front, only the outermost
hop can establish the client address. Have that hop write the header and the
inner one pass it through unchanged.

## Streaming endpoints

| path              | shape                                        |
|-------------------|----------------------------------------------|
| `/data/events`    | SSE, keepalive every 20s, open indefinitely  |
| `/console/live/*` | SSE, heartbeat every 15s, recycled every 90s |

Both need response buffering and compression **off** on those paths and a read
timeout well above the keepalive interval. A buffering proxy holds events until
its buffer fills — which looks like a frozen console and a subscription that
never fires, not like a proxy problem. There is no WebSocket endpoint, so no
upgrade handling is needed.

## Everything else

- **Give the instance its own hostname.** Every route is an absolute path
  (`/data`, `/oauth/*`, `/console/*`, `/.well-known/*`). Pass the URI through
  unrewritten — no `StripPrefix`, no mounting under a subpath.
- **Do not add CORS at the proxy.** Synthigy emits its own `Access-Control-*`
  and `Vary: Origin`; a second copy makes browsers reject the response.
- **Raise the body limit.** `/data` writes, model deploys and `/iam/import`
  arrive as a single POST body. 64m is a workable ceiling; nginx's 1m default
  is not.
- **Never proxy port 7888.** That is the portal daemon — the operator/admin
  surface — and it defends itself by rejecting any connection whose peer is
  not loopback. A reverse proxy on the same host connects *from* 127.0.0.1, so
  the check passes and the admin surface is published to the internet. Reach it
  with `ssh -L 7888:127.0.0.1:7888 <host>` instead. The engine's own console at
  `/console` on 7887 is a normal route and is proxied like everything else.

## What this buys you

An in-flight login is pinned to the client address, so a stolen authorization
state replayed from somewhere else is rejected. That check is only worth
something when the address cannot be chosen by the caller — which is to say,
when the proxy in front is writing `X-Forwarded-For` and the engine port is not
reachable around it. Configured as above, it holds; on a directly exposed
engine, an attacker sets both sides of the comparison and it does not.

---

## nginx

`proxy_pass http://synthigy;` names the `upstream` block, and `http://` is the
protocol *to the engine* — plain HTTP, since it terminates no TLS. Write it
with **no path after the host**: a bare `proxy_pass http://synthigy;` forwards
the request URI untouched, while adding even a slash
(`proxy_pass http://synthigy/;`) makes nginx strip the matched `location`
prefix and rewrite the path. Synthigy's routes are absolute, so a rewrite means
`/console/live/system` arrives as `/live/system` and 404s.

```nginx
upstream synthigy {
    server 127.0.0.1:7887;
    keepalive 32;
}

server {
    listen 80;
    listen [::]:80;
    server_name synthigy.example.com;
    return 301 https://$host$request_uri;
}

server {
    # nginx 1.25.1+ prefers `listen 443 ssl;` plus a separate `http2 on;`, and
    # warns that this form is deprecated. This form is the one that also works
    # on 1.14–1.24, where `http2 on;` is an unknown directive and nginx refuses
    # to start.
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name synthigy.example.com;

    ssl_certificate     /etc/letsencrypt/live/synthigy.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/synthigy.example.com/privkey.pem;

    client_max_body_size 64m;

    proxy_http_version 1.1;
    # Required by `keepalive` in the upstream block: without it the client's
    # own Connection header is forwarded and the pool is never reused.
    proxy_set_header Connection        "";
    proxy_set_header Host              $host;
    proxy_set_header X-Real-IP         $remote_addr;
    # $remote_addr, NOT $proxy_add_x_forwarded_for — see above.
    proxy_set_header X-Forwarded-For   $remote_addr;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Host  $host;
    proxy_set_header X-Forwarded-Port  $server_port;

    location / {
        proxy_pass http://synthigy;
        proxy_read_timeout 75s;
    }

    location /console/assets/ {
        proxy_pass http://synthigy;
        proxy_cache_valid 200 1h;
    }

    location = /data/events {
        proxy_pass http://synthigy;
        proxy_buffering off;
        proxy_cache off;
        gzip off;
        proxy_read_timeout 1h;
        proxy_send_timeout 1h;
    }

    location ^~ /console/live/ {
        proxy_pass http://synthigy;
        proxy_buffering off;
        proxy_cache off;
        gzip off;
        proxy_read_timeout 5m;
        proxy_send_timeout 5m;
    }
}
```

## Caddy

```caddyfile
synthigy.example.com {
	@sse  path /data/events /console/live/*
	@rest not path /data/events /console/live/*

	encode @rest zstd gzip

	reverse_proxy @sse 127.0.0.1:7887 {
		header_up X-Forwarded-For {remote_host}
		flush_interval -1
		transport http {
			read_timeout 0
			compression off
		}
	}

	reverse_proxy 127.0.0.1:7887 {
		header_up X-Forwarded-For {remote_host}
	}
}
```

Caddy obtains the certificate itself. `{remote_host}` replaces the
client-supplied `X-Forwarded-For` rather than appending to it.

## Traefik v3

Static (`traefik.yml`) — with no `trustedIPs`, Traefik drops a client-supplied
`X-Forwarded-For` and writes the real peer address itself, which is what
Synthigy needs. Add `trustedIPs` only for a load balancer in front of Traefik.

```yaml
entryPoints:
  web:
    address: ":80"
    http:
      redirections:
        entryPoint:
          to: websecure
          scheme: https
  websecure:
    address: ":443"
    forwardedHeaders:
      insecure: false

certificatesResolvers:
  le:
    acme:
      email: ops@example.com
      storage: /letsencrypt/acme.json
      httpChallenge:
        entryPoint: web

providers:
  file:
    filename: /etc/traefik/dynamic.yml
    watch: true
```

Dynamic (`dynamic.yml`) — one router for the whole host, no path rewriting:

```yaml
http:
  routers:
    synthigy:
      rule: "Host(`synthigy.example.com`)"
      entryPoints:
        - websecure
      service: synthigy
      tls:
        certResolver: le

  services:
    synthigy:
      loadBalancer:
        servers:
          - url: "http://127.0.0.1:7887"
        passHostHeader: true
        responseForwarding:
          flushInterval: "100ms"
```

As Docker labels instead of the file provider:

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.synthigy.rule=Host(`synthigy.example.com`)"
  - "traefik.http.routers.synthigy.entrypoints=websecure"
  - "traefik.http.routers.synthigy.tls.certresolver=le"
  - "traefik.http.services.synthigy.loadbalancer.server.port=7887"
  - "traefik.http.services.synthigy.loadbalancer.responseforwarding.flushinterval=100ms"
```
