# Running Synthigy behind a reverse proxy

Synthigy serves plain HTTP on one port (`SYNTHIGY_SERVER_PORT`, default 7887).
It terminates no TLS and does no virtual hosting, so an internet-facing
instance sits behind nginx, Caddy, Traefik or a cloud load balancer. This page
is what that proxy has to do; copy-paste configs are at the bottom.

## TLS is required

Session and login cookies are `Secure`, so a browser drops them over plain
`http://` on anything but `localhost`: the login page accepts the password and
comes straight back to itself. Terminate TLS at the proxy and keep the engine
on loopback:

```bash
SYNTHIGY_SERVER_HOST=127.0.0.1
SYNTHIGY_SERVER_PORT=7887
```

## Two engine settings

```bash
SYNTHIGY_IAM_ROOT_URL=https://synthigy.example.com
SYNTHIGY_SERVER_TRUST_PROXY=127.0.0.1/32,::1/128
```

**`SYNTHIGY_IAM_ROOT_URL`** is the public origin: the token issuer and the
base of every login and redirect link. Unset, they are derived per request
from `X-Forwarded-Host` and `X-Forwarded-Proto` — fine on a laptop, wrong
behind a load balancer, and steerable by whoever can set those headers. The
portal flags it until it is set.

**`SYNTHIGY_SERVER_TRUST_PROXY`** lists your proxies as IPs or CIDRs, comma
separated. Only a request that arrives from one of them has its
`X-Forwarded-For` believed; from anyone else the header is ignored and the
connection's own address is the client. A proxy on the same host connects from
loopback, hence `127.0.0.1/32,::1/128`; in a container network, name the
proxy's subnet. Unset, the header is never believed.

## The client address

Synthigy reads `X-Forwarded-For` from the right: it skips every hop that is one
of your proxies and takes the first one that is not. Whatever a client writes
into the header itself sits to the left of what your proxy adds, so it is never
reached — the proxy may overwrite the header (`$remote_addr`) or append to it
(`$proxy_add_x_forwarded_for`), both are safe. Behind a load balancer *and* a
proxy, list both hops.

That address is the request's `ip` on every log line it produces (`peer` — the
proxy — and the raw `forwarded_for` sit in the line's `ctx`), it is recorded on
the OAuth session, and an in-flight login is pinned to it, so a stolen login
state replayed from elsewhere is rejected.

An `X-Request-Id` sent by one of your proxies becomes the request's id. From
anyone else it is kept as `client_request_id` and a fresh id is issued; the
response's `X-Request-Id` always names the id in use.

**Do not expose the engine port directly** once a proxy is in front. Bind it
to loopback or firewall it: a caller that goes around the proxy can no longer
choose its address, but it still skips TLS and everything else the proxy
enforces.

| proxy   | directive                                           |
|---------|-----------------------------------------------------|
| nginx   | `proxy_set_header X-Forwarded-For $remote_addr;`     |
| Caddy   | `header_up X-Forwarded-For {remote_host}`            |
| Traefik | default — leave `forwardedHeaders.trustedIPs` unset  |

Forward `X-Forwarded-Proto` and `X-Forwarded-Host` too; the OAuth layer builds
absolute URLs from them whenever the root URL is unset.

## Streaming endpoints

| path              | shape                                                                   |
|-------------------|-------------------------------------------------------------------------|
| `/data/events`    | SSE, keepalive every 20s, open until `SYNTHIGY_SERVER_SSE_MAX_LIFETIME_MS` (15 minutes by default) |
| `/console/live/*` | SSE, heartbeat every 15s, recycled every 90s                            |

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
  and `Vary: Origin`; a second copy makes browsers reject the response
  ([CORS.md](CORS.md)).
- **Raise the body limit.** `/data` writes, model deploys and `/iam/import`
  arrive as a single POST body. 64m is a workable ceiling; nginx's 1m default
  is not.
- **Never proxy port 7888.** That is the portal, and it accepts any connection
  that comes from 127.0.0.1; a reverse proxy on the same host connects from
  there, so proxying it publishes the operator surface to the internet. Reach
  it with `ssh -L 7888:127.0.0.1:7888 <host>` instead. The console at
  `/console` on 7887 is a normal route and is proxied like everything else.

---

## nginx

`proxy_pass http://synthigy;` names the `upstream` block; `http://` is the
protocol to the engine, which terminates no TLS. Write it with **no path after
the host**: a bare `proxy_pass http://synthigy;` forwards the request URI
untouched, while `proxy_pass http://synthigy/;` makes nginx strip the matched
`location` prefix, so `/console/live/system` arrives as `/live/system` and
404s.

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
