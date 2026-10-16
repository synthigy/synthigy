# Running Synthigy behind a reverse proxy

The engine serves plain HTTP on one port (`SYNTHIGY_SERVER_PORT`, default 7887), so
an internet-facing instance sits behind nginx, Caddy, Traefik or a load balancer.
Copy-paste configs are at the bottom.

## Engine settings

```bash
SYNTHIGY_SERVER_HOST=127.0.0.1                    # reachable only through the proxy
SYNTHIGY_IAM_ROOT_URL=https://synthigy.example.com
SYNTHIGY_SERVER_TRUST_PROXY=127.0.0.1/32,::1/128
```

- **TLS is required.** Session cookies are `Secure`; over plain `http://` the login
  page accepts the password and comes straight back.
- **`SYNTHIGY_IAM_ROOT_URL`** is the public origin: token issuer and base of every
  login link. Unset, it is taken from `X-Forwarded-Host`/`-Proto` per request, which
  is wrong behind a load balancer, or from `Host` when the proxy isn't trusted. The
  portal flags it until set.
- **`SYNTHIGY_SERVER_TRUST_PROXY`** lists your proxies (IPs or CIDRs). Only requests
  from them have `X-Forwarded-For`, `-Host` and `-Proto` believed; from anyone else
  those headers are dropped. Synthigy reads `X-Forwarded-For` from the
  right and skips your proxies, so a client can't fake its address. It can only
  find the address the first hop recorded: behind a load balancer, the proxy on
  the instance must keep that record, not overwrite it — see
  [Behind an AWS load balancer](#behind-an-aws-application-load-balancer). In a
  container network, list the proxy's subnet.
- An `X-Request-Id` from a trusted proxy becomes the request id.

## What the proxy must do

- **Stream without buffering** on `/data/events` and `/console/live/*` (server-sent
  events, keepalive every 15–20s): buffering and compression off, read timeout well
  above that. A buffering proxy looks like a frozen console. There are no WebSockets.
- **Give the instance its own hostname** and pass paths through unrewritten — no
  subpath mounting, no `StripPrefix`.
- **Don't add CORS headers**; Synthigy sends its own ([CORS.md](CORS.md)).
- **Raise the body limit** to about 64m for writes, deploys and IAM imports.
- **Health check:** `GET /health`, 200 while the node can serve, 503 when it can't
  ([OPERATIONS.md](OPERATIONS.md#health-check)).
- **Forward `X-Forwarded-Proto` and `X-Forwarded-Host`.**
- **Never proxy port 7888**, the portal: it trusts anything from 127.0.0.1, so a
  local proxy would publish it. Reach it with `ssh -L 7888:127.0.0.1:7888 <host>`.
- **Firewall 7887** so nothing bypasses the proxy.

---

The configs below are for a proxy that faces the internet itself. Behind a load
balancer, see [the last section](#behind-an-aws-application-load-balancer).

## nginx

Write `proxy_pass http://synthigy;` with **no path after the host**: a trailing `/`
makes nginx strip the location prefix, and routes 404.

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
    # works on nginx 1.14+; on 1.25.1+ you may use `listen 443 ssl;` + `http2 on;`
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name synthigy.example.com;

    ssl_certificate     /etc/letsencrypt/live/synthigy.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/synthigy.example.com/privkey.pem;

    client_max_body_size 64m;

    proxy_http_version 1.1;
    # needed by `keepalive` in the upstream block
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
client-supplied `X-Forwarded-For` rather than appending to it — right when Caddy
is the edge, wrong behind a load balancer.

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

## Behind an AWS Application Load Balancer

```
client → ALB (TLS) → proxy on the instance, port 80 → engine on 127.0.0.1:7887
```

Each hop sees only the machine connected to it. The ALB records the client in
`X-Forwarded-For`; the proxy on the instance sees the ALB. A proxy that overwrites
the header with its own peer, as the configs above do, makes every request look
like it came from the ALB. Make the proxy trust the ALB and take the client from
the header instead; `SYNTHIGY_SERVER_TRUST_PROXY` then stays at loopback.

**ALB**

- **X-Forwarded-For header:** Append (the default). Leave client port
  preservation off.
- Terminate TLS on the ALB and redirect HTTP to HTTPS with an ALB listener rule.
  The proxy only ever receives HTTP from the ALB, so a redirect there loops.
- Target the instance's port 80, health check `GET /health`. The default 60s idle
  timeout is above the 15–20s event-stream keepalive.
- Allow port 80 on the instance only from the ALB's security group.
- Identify the ALB by the **subnets it is attached to**: its node addresses
  change as it scales.

**Engine:** the settings at the top. `SYNTHIGY_IAM_ROOT_URL` is not optional here —
the proxy receives plain HTTP, so anything derived from the request says `http://`.

**nginx** — replaces both server blocks of the nginx config; keep its `upstream`
and `location` blocks. Needs `ngx_http_realip_module`, included in the
distribution packages.

```nginx
server {
    listen 80;
    server_name synthigy.example.com;

    # one line per subnet the ALB is attached to
    set_real_ip_from  10.0.1.0/24;
    set_real_ip_from  10.0.2.0/24;
    real_ip_header    X-Forwarded-For;
    real_ip_recursive on;

    client_max_body_size 64m;

    proxy_http_version 1.1;
    proxy_set_header Connection        "";
    proxy_set_header Host              $host;
    # $remote_addr is the client once realip has run
    proxy_set_header X-Real-IP         $remote_addr;
    proxy_set_header X-Forwarded-For   $remote_addr;
    proxy_set_header X-Forwarded-Proto $http_x_forwarded_proto;
    proxy_set_header X-Forwarded-Host  $host;
    proxy_set_header X-Forwarded-Port  $http_x_forwarded_port;

    # location blocks as in the nginx config
}
```

**Caddy 2.8+**

```caddyfile
{
	servers {
		trusted_proxies static 10.0.1.0/24 10.0.2.0/24
		trusted_proxies_strict
	}
}

http://synthigy.example.com {
	@sse  path /data/events /console/live/*
	@rest not path /data/events /console/live/*

	encode @rest zstd gzip

	reverse_proxy @sse 127.0.0.1:7887 {
		header_up X-Forwarded-For {client_ip}
		flush_interval -1
		transport http {
			read_timeout 0
			compression off
		}
	}

	reverse_proxy 127.0.0.1:7887 {
		header_up X-Forwarded-For {client_ip}
	}
}
```

`trusted_proxies_strict` makes Caddy read the header from the right; without it
Caddy takes the leftmost entry, which the client wrote. `http://` in the site
address leaves TLS to the ALB, and the ALB's `X-Forwarded-Proto: https` passes
through because the ALB is trusted.

**Traefik:** set `forwardedHeaders.trustedIPs` to the ALB subnets on the entry
point the ALB targets, and drop the `web` → `websecure` redirect (the ALB
redirects). Traefik keeps the ALB's entries and appends its own peer rather than
resolving the client, so add the ALB subnets to `SYNTHIGY_SERVER_TRUST_PROXY`
as well.

**Check it:** request log lines carry `ip`, the resolved client. Behind a
correctly configured ALB it is your public address, never a `10.x` one.
