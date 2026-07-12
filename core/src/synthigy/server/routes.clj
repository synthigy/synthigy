(ns synthigy.server.routes
  "Backend-agnostic route tables + module gating shared by every Synthigy
   HTTP adapter (httpkit/ring-jetty/undertow). SSE streaming mechanics are
   the one thing that differs per adapter (http-kit channels vs Jetty's
   StreamableResponseBody vs Undertow's RespondBody) — each backend builds
   its own `:sse-handler` (and, where it has one, `:subscribe-handler` for
   the best-effort `/subscribe` notifier path) and passes it in here.
   Everything else — URIs, methods, module gating — lives once so the
   three backends can't drift the way they had before this namespace
   existed (see project_subscriptions_bare_mode memory)."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.json :as json]
   [synthigy.oauth.handlers :as oauth]
   [synthigy.server.data :as data]
   [synthigy.server.history :as history]
   [synthigy.server.info :as info]
   [synthigy.server.subscription :as subscription]))

;;; ============================================================================
;;; Module Gate — 404s a route when its backing module isn't running.
;;; Keeps the route table identical across modes (full vs data-only); the
;;; gate decides at request time. Var-deref of `handler-var` happens per
;;; call so REPL hot-reload of the handler still takes effect.
;;; ============================================================================

(defn module-unavailable [module-key]
  {:status 404
   :headers {"Content-Type" "application/json"}
   :body (json/write-str
          {:error {:code "MODULE_NOT_LOADED"
                   :message (str "Module " module-key " is not running on this server.")}})})

(defn gated
  "Wrap `handler` so requests 404 unless `module-key` is in the running
   lifecycle. Used to keep IAM/OAuth/substrate routes registered but
   inert when the operator runs a data-only server."
  [module-key handler]
  (fn [request]
    (if (lifecycle/started? module-key)
      (handler request)
      (module-unavailable module-key))))

;;; ============================================================================
;;; Route Tables
;;; ============================================================================

(defn full-routes
  "Route table for `:synthigy/server` (the full deployment).

   Args:
     :info              - server info map for /info
     :sse-handler        - backend's /data/events Ring handler (required)
     :subscribe-handler - backend's /subscribe (notifier) Ring handler;
                          omit if the backend doesn't implement it — the
                          route is simply left out of the table."
  [{:keys [info sse-handler subscribe-handler] :or {info {}}}]
  (let [info-handler (fn [_]
                       {:status 200
                        :headers {"Content-Type" "application/json"}
                        :body (json/write-str info)})]
    (cond-> [;; Info
             ["/info" :get info-handler]

             ;; Routes use Var references (#'ns/handler) instead of bare
             ;; function values so that reloading a handler in the REPL
             ;; takes effect immediately — the router resolves the Var
             ;; per request.

             ;; OAuth 2.0 Core — gated on :synthigy/iam so a data-only
             ;; server 404s these instead of NPEing on missing oauth state.
             ["/oauth/authorize" :get (gated :synthigy/iam #'oauth/authorize)]
             ["/oauth/token" :post (gated :synthigy/iam #'oauth/token)]
             ["/oauth/login" :get (gated :synthigy/iam #'oauth/login)]
             ["/oauth/login" :post (gated :synthigy/iam #'oauth/login)]
             ["/oauth/logout" :get (gated :synthigy/iam #'oauth/logout)]
             ["/oauth/logout" :post (gated :synthigy/iam #'oauth/logout)]
             ["/oauth/revoke" :post (gated :synthigy/iam #'oauth/revoke)]
             ["/oauth/introspect" :post (gated :synthigy/iam #'oauth/introspect)]

             ;; Device Flow (RFC 8628)
             ["/oauth/device/auth" :post (gated :synthigy/iam #'oauth/device-authorization)]
             ["/oauth/device/activate" :get (gated :synthigy/iam #'oauth/device-activation)]
             ["/oauth/device/activate" :post (gated :synthigy/iam #'oauth/device-activation)]

             ;; Federated (social) login — Synthigy brokering to upstream IdPs
             ["/oauth/federated/start" :get (gated :synthigy/iam #'oauth/federated-start)]
             ["/oauth/federated/callback" :get (gated :synthigy/iam #'oauth/federated-callback)]

             ;; OpenID Connect
             ["/oauth/userinfo" :get (gated :synthigy/iam #'oauth/userinfo)]
             ["/oauth/jwks" :get (gated :synthigy/iam #'oauth/jwks)]
             ["/.well-known/openid-configuration" :get (gated :synthigy/iam #'oauth/openid-configuration)]

             ;; Public discovery — answers "is auth required?" before the
             ;; modeler can authenticate. Must remain callable when
             ;; :synthigy/iam isn't loaded.
             ["/.well-known/synthigy" :get #'info/handler]

             ;; Data API (direct dataset operations, service-to-service)
             ["/data" :post #'data/handler]

             ;; Schema introspection — GET, cacheable, Swagger-compatible
             ["/schema" :get #'data/schema-handler]

             ;; XSQL lint — schema-aware diagnostics for query DSL strings
             ["/lint" :post #'data/lint-handler]

             ;; Audit substrate query surface (5 ops). Handler self-gates
             ;; on :synthigy/observability, returning 404 when no audit
             ;; provider module is loaded.
             ["/history" :post #'history/handler]

             ;; Data Subscriptions — gated on the substrate (the drainer
             ;; that feeds delta/dispatch!), NOT on IAM. With IAM running
             ;; the handlers demand a token; without it they run against
             ;; the allow-all facade and identity-key via x-synthigy-client
             ;; (see synthigy.server.subscription/request-identity).
             ["/data/subscription/set" :post (gated :synthigy/substrate #'subscription/set-handler)]
             ["/data/subscription/status" :get (gated :synthigy/substrate #'subscription/status-handler)]
             ["/data/events" :get (gated :synthigy/substrate sse-handler)]]
      ;; Best-effort live notifications (SSE). Sub here with declared
      ;; interest; the notifier emits xid-keyed thin events. /data is the
      ;; refetch path. See subscriptions-substrate. Identity is a
      ;; per-connection nano-id, so this gates on the substrate like the
      ;; /data subscription path above, not on IAM.
      subscribe-handler (conj ["/subscribe" :get (gated :synthigy/substrate subscribe-handler)]))))

(defn bare-routes
  "Route table for `:synthigy/bare-server` — the data-only deployment.

   Only the routes that work meaningfully when nothing IAM-related is in
   the lifecycle: data queries/mutations, schema introspection, XSQL
   lint, and the public discovery doc that lets the modeler skip OIDC.

   No oauth/*, no admin/*. The Live plane (/subscribe, /data/subscription/*,
   /data/events) is gated on :synthigy/substrate, and the History plane
   (/history) on :synthigy/observability — start those modules alongside
   the bare server and they work with no IAM (allow-all access; /data
   subscription identity via x-synthigy-client; audit rows carry no actor).

   Args: same as `full-routes`."
  [{:keys [info sse-handler subscribe-handler] :or {info {}}}]
  (let [info-handler (fn [_]
                       {:status 200
                        :headers {"Content-Type" "application/json"}
                        :body (json/write-str info)})]
    (cond-> [["/info" :get info-handler]
             ["/.well-known/synthigy" :get #'info/handler]
             ["/data" :post #'data/handler]
             ["/schema" :get #'data/schema-handler]
             ["/lint" :post #'data/lint-handler]
             ["/data/subscription/set" :post (gated :synthigy/substrate #'subscription/set-handler)]
             ["/data/subscription/status" :get (gated :synthigy/substrate #'subscription/status-handler)]
             ["/data/events" :get (gated :synthigy/substrate sse-handler)]
             ["/history" :post (gated :synthigy/observability #'history/handler)]]
      subscribe-handler (conj ["/subscribe" :get (gated :synthigy/substrate subscribe-handler)]))))
