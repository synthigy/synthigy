(ns synthigy.server.profile
  "Canonical dependency lists + docs for the two Synthigy server profiles.
   Starting an httpkit vs Jetty vs Undertow listener is backend-specific
   (each backend's server ns keeps its own start/stop + SSE mechanics);
   which modules a profile pulls in is not — every backend registers
   `:synthigy/server` / `:synthigy/bare-server` against the same deps
   here so the profiles can't drift across backends the way they had
   before this namespace existed.")

(def full-deps
  "`:synthigy/server` — full HTTP server: IAM + OAuth + audit + substrate
   + subscriptions. :synthigy/subscriptions transitively pulls
   :synthigy/iam, and :synthigy/oauth.persistence transitively pulls
   :synthigy.iam/encryption + :synthigy.iam/connector + :synthigy/oauth
   — so starting :synthigy/server boots a full IAM + OAuth surface. For
   a true data-only server without IAM, start :synthigy/bare-server."
  [:synthigy/audit
   :synthigy/substrate
   :synthigy/subscriptions
   :synthigy/oauth.persistence])

(def full-doc "Full HTTP server — /data + IAM + OAuth + SSE + admin/SPA")

(def bare-deps
  "`:synthigy/bare-server` — minimum viable Synthigy server: just the
   dataset module. No IAM, OAuth, audit, or subscriptions are pulled in
   as hard deps; the Live/History planes light up for free if the
   operator separately starts :synthigy/substrate / :synthigy/observability
   alongside it — the shared route table (`synthigy.server.routes`) gates
   each optional route on the module actually being up."
  [:synthigy/log :synthigy/dataset])

(def bare-doc "Data-only HTTP server — /data + /schema, no IAM/OAuth")
