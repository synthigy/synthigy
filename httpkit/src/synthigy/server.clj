(ns synthigy.server
  "http-kit HTTP server implementation for Synthigy.

  Provides core HTTP infrastructure:
  - OAuth 2.0 + OpenID Connect endpoints
  - Data API (/data)
  - Data subscriptions (SSE)
  - Admin endpoints (/__admin)
  - Authentication middleware
  - CORS configuration
  - SPA serving

  GraphQL and Frontend are optional services managed by admin.
  See synthigy.admin.core for service orchestration.

  ## Quick Start

  ```clojure
  (require '[synthigy.server :as server])

  ;; Start with defaults (localhost:7887)
  (server/start)

  ;; Custom port
  (server/start {:port 3000})

  ;; Stop
  (server/stop)
  ```

  See individual handler functions for details."
  (:require
   [clojure.core.async :as async]
   [clojure.string :as str]
   [environ.core :refer [env]]
   [nano-id.core :refer [nano-id]]
   [org.httpkit.server :as httpkit]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.log :as log]
   [ring.middleware.keyword-params :refer [wrap-keyword-params]]
   [ring.middleware.params :refer [wrap-params]]
   [ring.util.response :as response]
   [synthigy.json :as json]
   synthigy.admin
   synthigy.core
   [synthigy.iam.access :as access]
   synthigy.oauth
   synthigy.oauth.persistence
   [synthigy.server.auth :as auth]
   [synthigy.server.data :as data]
   [synthigy.server.profile :as profile]
   [synthigy.server.routes :as routes]
   [synthigy.server.spa :as spa]
   [synthigy.server.subscription :as subscription]
   [synthigy.server.subscriptions.notifier :as notifier]))

;;; ============================================================================
;;; Simple Router (No External Dependencies)
;;; ============================================================================

(defn- match-route
  "Simple route matching. Returns handler or nil."
  [request routes]
  (let [uri (:uri request)
        method (:request-method request)]
    (some (fn [[route-uri route-method handler]]
            (when (and (= uri route-uri)
                       (or (= route-method :any)
                           (= route-method method)))
              handler))
          routes)))

(defn make-router
  "Creates a simple router from route definitions.

  Routes are vectors of [uri method handler]:
    [[\"/oauth/token\" :post oauth/token]
     [\"/info\" :get info-handler]]

  Args:
    routes - Vector of route definitions

  Returns:
    Ring handler that dispatches to matching route or returns nil"
  [routes]
  (fn [request]
    (when-let [handler (match-route request routes)]
      (handler request))))

;;; ============================================================================
;;; SSE Handler
;;; ============================================================================

(defn- make-sse-handler
  "Creates a Ring handler for SSE event streaming via http-kit.
   Authenticates, creates an event stream, and streams events to the client.

   ## Observability

   - `:synthigy.server/sse-client-connected`  — `:debug`, on open
   - `:synthigy.server/sse-event-sent`        — `:debug`, one per real
       send (skips keepalives), carries `:event-type` + `:event-bytes`
   - `:synthigy.server/sse-connection-stats`  — `:info`,  on close,
       carries `:duration-ms` + `:events-sent` + `:bytes-sent`

   Per-event debug rows are silent at the default `:info` level — bump
   `synthigy.server` to `:debug` in the cockpit settings (Phase B) when
   you want to see what's being shipped to each subscriber. The close
   summary is always present at `:info`."
  []
  (fn [request]
    (let [;; EventSource can't set headers — accept access_token query param
          request (if (get-in request [:headers "authorization"])
                    request
                    (if-let [token (get-in request [:query-params "access_token"])]
                      (assoc-in request [:headers "authorization"] (str "Bearer " token))
                      request))
          ;; Optional-IAM: token mandatory only when :synthigy/iam runs
          ;; (same contract as the /data handlers).
          iam-active? (lifecycle/started? :synthigy/iam)
          iam (when iam-active? (auth/authenticate-request request))]
      (if (and iam-active? (not iam))
        {:status 401
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})}
        (let [identity-key (subscription/request-identity request iam)
              stream-chan  (subscription/create-event-stream identity-key)
              started-ms   (System/currentTimeMillis)
              events-sent  (java.util.concurrent.atomic.AtomicLong. 0)
              bytes-sent   (java.util.concurrent.atomic.AtomicLong. 0)]
          (httpkit/as-channel request
                              {:on-open
                               (fn [ch]
                                 (log/debug {:id ::sse-client-connected
                                             :data {:identity-key identity-key}}
                                            "SSE client connected")
                                 (let [origin (get-in request [:headers "origin"])]
                                   (httpkit/send! ch
                                                  {:status 200
                                                   :headers (cond-> {"Content-Type" "text/event-stream"
                                                                     "Cache-Control" "no-cache"
                                                                     "Connection" "keep-alive"}
                                                              origin
                                                              (merge {"Access-Control-Allow-Origin" origin
                                                                      "Access-Control-Allow-Credentials" "true"}))}
                                                  false))
                                 (async/go-loop []
                                   (when (httpkit/open? ch)
                                     (let [[val port] (async/alts!
                                                       [stream-chan
                                                        (async/timeout 20000)])]
                                       (cond
                                         (and (nil? val) (= port stream-chan))
                                         (when (httpkit/open? ch)
                                           (httpkit/close ch))

                                         (not= port stream-chan)
                                         (do (httpkit/send! ch subscription/keepalive-msg false)
                                             (recur))

                                         :else
                                         (let [payload (subscription/format-sse val)
                                               n       (count payload)]
                                           (httpkit/send! ch payload false)
                                           (.incrementAndGet events-sent)
                                           (.addAndGet bytes-sent (long n))
                                           (log/debug {:id ::sse-event-sent
                                                       :data {:identity-key identity-key
                                                              :event-type   (:event val)
                                                              :event-bytes  n}}
                                                      "SSE event sent")
                                           (recur)))))))

                               :on-close
                               (fn [_ch _status]
                                 (let [total-events (.get events-sent)
                                       total-bytes  (.get bytes-sent)
                                       duration-ms  (- (System/currentTimeMillis) started-ms)]
                                   (log/info {:id ::sse-connection-stats
                                              :data {:identity-key identity-key
                                                     :duration-ms  duration-ms
                                                     :events-sent  total-events
                                                     :bytes-sent   total-bytes}}
                                             "SSE connection closed"))
                                 (subscription/destroy-event-stream identity-key stream-chan))}))))))

;;; ============================================================================
;;; /subscribe — best-effort live notifications via SSE
;;;
;;; Declared interest is passed as either:
;;;   - query params: entity_xid=ex_user&record_xids=rcx_a,rcx_b
;;;   - JSON body on initial GET (rare for SSE, but supported)
;;;
;;; The handler authenticates, parses interest, registers the connection
;;; with notifier/register!, then keeps the channel open. The drainer
;;; calls notifier/dispatch! per envelope; the notifier walks
;;; connections, matches, and emits via the per-connection `emit` fn.
;;;
;;; Request parsing (parse-declared-interest, parse-replay-cursor) lives in
;;; synthigy.server.subscriptions.notifier — shared with ring-jetty/undertow.
;;; ============================================================================

(defn- make-subscribe-handler
  "Ring handler for `/subscribe`. Establishes an SSE connection, registers
   declared interest with the notifier, holds the channel open with a
   keep-alive comment every 20s. Best-effort; reconnection is the client's
   job."
  []
  (fn [request]
    (let [;; EventSource cannot set headers — accept token via query param too.
          request (if (get-in request [:headers "authorization"])
                    request
                    (if-let [token (get-in request [:query-params "access_token"])]
                      (assoc-in request [:headers "authorization"] (str "Bearer " token))
                      request))
          iam-active? (lifecycle/started? :synthigy/iam)
          iam (when iam-active? (auth/authenticate-request request))]
      (if (and iam-active? (not iam))
        {:status 401
         :headers {"Content-Type" "application/json"}
         :body (json/->json {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})}
        (let [conn-id (nano-id 16)
              interest (notifier/parse-declared-interest request)]
          (httpkit/as-channel
           request
           {:on-open
            (fn [ch]
              (let [origin (get-in request [:headers "origin"])
                    emit   (fn [sse-str]
                             (when (httpkit/open? ch)
                               (httpkit/send! ch sse-str false)))]
                (httpkit/send! ch
                               {:status 200
                                :headers (cond-> {"Content-Type" "text/event-stream"
                                                  "Cache-Control" "no-cache"
                                                  "Connection" "keep-alive"
                                                  "X-Synthigy-Conn-Id" conn-id}
                                           origin
                                           (merge {"Access-Control-Allow-Origin" origin
                                                   "Access-Control-Allow-Credentials" "true"}))}
                               false)
                 ;; Backfill before going live: if a cursor was supplied
                 ;; (?from=<seq>, or the browser's Last-Event-ID on reconnect),
                 ;; replay the matching missed events in seq order first. No
                 ;; cursor → live-only. Filtering reuses the live matcher, so
                 ;; the client sees exactly what it would have seen live.
                (when-let [cursor (notifier/parse-replay-cursor request)]
                  (notifier/replay-and-emit! interest cursor emit {}))
                (notifier/register! conn-id interest emit)
                (log/info {:id ::subscribe-connected
                           :data {:action :started :subject :subscribe
                                  :conn-id conn-id
                                  :interest interest}}
                          "Subscribe SSE client connected")
                 ;; Keep-alive comment every 20s to defeat proxy buffering.
                (async/go-loop []
                  (async/<! (async/timeout 20000))
                  (when (httpkit/open? ch)
                    (httpkit/send! ch ": keepalive\n\n" false)
                    (recur)))))
            :on-close
            (fn [_ch _status]
              (notifier/unregister! conn-id)
              (log/info {:id ::subscribe-disconnected
                         :data {:action :stopped :subject :subscribe :conn-id conn-id}}
                        "Subscribe SSE client disconnected"))}))))))

;;; ============================================================================
;;; Default Routes — table + module gating live in synthigy.server.routes,
;;; shared with ring-jetty/undertow. This backend only supplies its own
;;; SSE transport (:sse-handler, :subscribe-handler).
;;; ============================================================================

(defn default-routes
  "Route table for the full server (`:synthigy/server`). See
   `synthigy.server.routes/full-routes` for the shared route definitions."
  [opts]
  (routes/full-routes (assoc opts
                             :sse-handler (make-sse-handler)
                             :subscribe-handler (make-subscribe-handler))))

(defn bare-routes
  "Route table for `:synthigy/bare-server`. See
   `synthigy.server.routes/bare-routes` for the shared route definitions."
  [opts]
  (routes/bare-routes (assoc opts
                             :sse-handler (make-sse-handler)
                             :subscribe-handler (make-subscribe-handler))))

;;; ============================================================================
;;; Middleware Stack
;;; ============================================================================

(defn wrap-cors
  "Simple CORS middleware allowing all origins.

  For production, configure specific origins."
  [handler]
  (fn [request]
    (let [response (handler request)
          origin (get-in request [:headers "origin"])]
      (if response
        (update response :headers merge
                {"Access-Control-Allow-Origin" (or origin "*")
                 "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                 "Access-Control-Allow-Headers" "Content-Type, Authorization"
                 "Access-Control-Allow-Credentials" "true"})
        response))))

(defn wrap-options
  "Handle OPTIONS preflight requests."
  [handler]
  (fn [request]
    (if (= :options (:request-method request))
      {:status 204
       :headers {"Access-Control-Allow-Origin" (get-in request [:headers "origin"] "*")
                 "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                 "Access-Control-Allow-Headers" "Content-Type, Authorization"
                 "Access-Control-Allow-Credentials" "true"}}
      (handler request))))

;;; ============================================================================
;;; Static Resource Serving
;;; ============================================================================

(def ^:private content-type-map
  "Map of file extensions to MIME content types."
  {"css"  "text/css"
   "js"   "application/javascript"
   "png"  "image/png"
   "jpg"  "image/jpeg"
   "jpeg" "image/jpeg"
   "gif"  "image/gif"
   "svg"  "image/svg+xml"
   "ico"  "image/x-icon"
   "woff" "font/woff"
   "woff2" "font/woff2"})

(defn- get-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (subs path (inc idx))))

(defn wrap-static-resources
  "Middleware to serve static resources from classpath.
   Handles /oauth/css/*, /oauth/js/*, /oauth/images/* paths."
  [handler]
  (fn [request]
    (let [uri (:uri request)]
      (if (and (= :get (:request-method request))
               (or (str/starts-with? uri "/oauth/css/")
                   (str/starts-with? uri "/oauth/js/")
                   (str/starts-with? uri "/oauth/images/")))
        (let [resource-path (subs uri 1)
              resp (response/resource-response resource-path)]
          (if resp
            (let [ext (get-extension uri)
                  content-type (get content-type-map ext "application/octet-stream")]
              (assoc-in resp [:headers "Content-Type"] content-type))
            (handler request)))
        (handler request)))))

(defn- auth-ctx-from-request
  "Best-effort: if the request carries a Bearer token that resolves, return
  `{:user-xid … :tenant …}` (omitting any keys whose values are nil) for
  `log/with-ctx`. Returns `{}` for unauthenticated requests. Never throws —
  auth failures fall back to empty ctx so the request still gets logged."
  [request]
  (try
    (when-let [{:keys [principal]} (auth/authenticate-request request)]
      (cond-> {}
        (:xid    principal) (assoc :user-xid (:xid    principal))
        (:tenant principal) (assoc :tenant   (:tenant principal))))
    (catch Throwable _ {})))

(defn wrap-request-id
  "Ring middleware that establishes a request-id (and, when the request
  authenticates, user-xid + tenant) for the lifetime of the request and
  emits one `::request-completed` info signal per call.

  - Uses an inbound X-Request-Id header if present (preserving upstream
    tracing), otherwise generates a fresh nano-id.
  - Resolves auth once via `auth/authenticate-request`. If a principal
    is found, `:user-xid` (and `:tenant` if present) are bound to
    `log/with-ctx` for the request's lifetime — so request-completed
    AND every signal the handler emits land with promoted `user_xid`
    in the wire row.
  - On the way out, emits `:synthigy.server/request-completed` with
    method, uri, status, and duration_ms — guarantees one row per
    request even when the handler is silent.
  - Echoes the id as the X-Request-Id response header.

  Inner handlers still call `auth/authenticate-request` themselves —
  this middleware does not pre-resolve into the request map. The
  duplicate auth call is the price for clean separation; if it becomes
  a hot-path concern we can stash the principal in the request map
  for inner handlers to pick up."
  [handler]
  (fn [request]
    (let [req-id    (or (some-> request :headers (get "x-request-id"))
                        (nano-id 10))
          started   (System/currentTimeMillis)
          auth-ctx  (auth-ctx-from-request request)
          full-ctx  (assoc auth-ctx :request-id req-id)
          response  (log/with-ctx full-ctx
                      (let [resp (handler request)]
                        (log/info
                         {:id :synthigy.server/request-completed
                          :data {:method      (some-> (:request-method request) name)
                                 :uri         (:uri request)
                                 :status      (:status resp)
                                 :duration-ms (- (System/currentTimeMillis) started)}}
                         "request completed")
                        resp))]
      (assoc-in response [:headers "X-Request-Id"] req-id))))

(defn make-handler
  "Creates the complete Ring handler with all middleware.

  Args:
    opts - Configuration:
           :routes   - Custom routes (default: default-routes)
           :spa-root - Filesystem path for SPA (optional)
           :info     - Server info map

  Returns:
    Ring handler"
  [{:keys [routes spa-root info]
    :or {info {}}}]
  (let [routes (or routes (default-routes {:info info}))
        router (make-router routes)]
    (wrap-request-id
     (spa/wrap-spa
      (-> router
          wrap-static-resources
          wrap-cors
          wrap-options
          wrap-keyword-params
          wrap-params
          data/wrap-preserve-body)
      {:root spa-root}))))

;;; ============================================================================
;;; Server Lifecycle
;;; ============================================================================

(defonce server (atom nil))

(comment
  (println @server))

(defn stop
  "Stops the http-kit server.

  Returns:
    nil"
  []
  (when-let [stop-fn @server]
    (log/info {:id ::http-stopping :data {:action :stopping :subject :http-server}} "Stopping HTTP server")
    (stop-fn :timeout 100)
    (reset! server nil))
  nil)

(defn start
  "Starts the http-kit HTTP server.

  Args:
    opts - Configuration map:
           :host     - Bind address (default: localhost or SYNTHIGY_HOST)
           :port     - Port number (default: 7887 or SYNTHIGY_PORT)
           :spa-root - SPA static files directory (default: SYNTHIGY_SERVE)
           :info     - Server info for /info endpoint
           :routes   - Custom routes (replaces defaults)

  Returns:
    nil"
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port spa-root info routes]
     :or {host (or (env :synthigy-host) "localhost")
          port (or (some-> (env :synthigy-port) Integer/parseInt) 7887)
          spa-root (env :synthigy-serve)}}]

   (stop)
   (log/info {:id ::http-starting :data {:action :starting :subject :http-server :host host :port port}}
             "Starting HTTP server")

   (let [handler (make-handler {:routes routes
                                :spa-root spa-root
                                :info info})
         stop-fn (httpkit/run-server handler {:ip host
                                              :port port})]
     (reset! server stop-fn)
     (log/info {:id ::http-started :data {:action :started :subject :http-server :host host :port port}}
               "HTTP server started")
     (when spa-root
       (log/info {:id ::spa-enabled :data {:action :enabled :subject :spa :root spa-root}}
                 "SPA static files enabled"))
     nil)))

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point for http-kit server.

  Configuration via environment variables:
    SYNTHIGY_HOST - Bind address (default: localhost)
    SYNTHIGY_PORT - Port (default: 7887)
    SYNTHIGY_SERVE - SPA static files directory"
  [& _]
  (try
    (start)
    (log/info {:id ::server-running :data {:action :ready :subject :http-server}}
              "Synthigy http-kit server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error! {:id ::server-start-failed :data {:action :starting :subject :http-server}} ex)
      (System/exit 1))))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
 :synthigy/server
 ;; Hard deps: the substrates that the full server's always-on routes
 ;; (/data, /history, /subscribe, /oauth/*) need. NOTE —
 ;; :synthigy/subscriptions transitively pulls :synthigy/iam, and
 ;; :synthigy/oauth.persistence transitively pulls :synthigy.iam/encryption
 ;; + :synthigy.iam/connector + :synthigy/oauth — so starting
 ;; :synthigy/server boots a full IAM + OAuth surface. For a true data-only
 ;; server without IAM, start :synthigy/bare-server instead (separate
 ;; registration below).
 {:depends-on profile/full-deps
  :doc profile/full-doc
  :start (fn []
           (log/info {:id ::lifecycle-starting :data {:action :starting}} "HTTP server lifecycle starting")
           (stop)
           (start)
           (log/info {:id ::lifecycle-started :data {:action :started}} "HTTP server lifecycle started"))
  :stop (fn []
          (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "HTTP server lifecycle stopping")
          (stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "HTTP server lifecycle stopped"))})

;;; ============================================================================
;;; Bare server — data-only, no IAM, no audit, no subscriptions
;;; ============================================================================

(lifecycle/register-module!
 :synthigy/bare-server
 ;; Minimum viable Synthigy server: just the dataset module. No IAM, no
 ;; OAuth, no audit substrate, no subscriptions, no SSE. Routes registered
 ;; are /data, /schema, /lint, /.well-known/synthigy, /info. The modeler
 ;; probes the discovery doc, sees auth.required=false, and runs without
 ;; a login flow.
 ;;
 ;; Operator picks one or the other — :synthigy/server and
 ;; :synthigy/bare-server are mutually exclusive (same http-kit instance,
 ;; same port). The two register-module! blocks just declare modes; the
 ;; lifecycle resolver only runs whichever the operator explicitly starts.
 {:depends-on profile/bare-deps
  :doc profile/bare-doc
  :start (fn []
           (log/info {:id ::bare-starting :data {:action :starting :subject :bare-server}}
                     "Bare HTTP server lifecycle starting")
           (stop)
           (start {:routes (bare-routes {:info (patch/available-versions :synthigy/dataset)})})
           (log/info {:id ::bare-started :data {:action :started :subject :bare-server}}
                     "Bare HTTP server lifecycle started"))
  :stop (fn []
          (log/info {:id ::bare-stopping :data {:action :stopping :subject :bare-server}}
                    "Bare HTTP server lifecycle stopping")
          (stop)
          (log/info {:id ::bare-stopped :data {:action :stopped :subject :bare-server}}
                    "Bare HTTP server lifecycle stopped"))})

(comment
  (lifecycle/start! :synthigy/server)
  (lifecycle/start! :synthigy/bare-server)
  (lifecycle/print-topology-layers)
  (lifecycle/system-report)
  (println (lifecycle/topology-layers-string))
  (start {:port 7887})
  (stop))
