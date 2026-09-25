;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server
  "Undertow HTTP server implementation for Synthigy.

   Provides core HTTP infrastructure:
   - OAuth 2.0 + OpenID Connect endpoints
   - Data API (/data)
   - Data subscriptions (SSE)
   - Authentication middleware
   - CORS configuration
   - SPA serving

   Uses ring-undertow-adapter (Luminus/Kit compatible).


   ## Quick Start

   ```clojure
   (require '[synthigy.server :as server])

   ;; Start with defaults (localhost:7887)
   (server/start)

   ;; Custom port
   (server/start {:port 3000})

   ;; Stop
   (server/stop)
   ```"
  (:require
    [synthigy.cors :as cors]
    [clojure.core.async :as async]
    [synthigy.json :as json]
    [clojure.string :as str]
    [environ.core :refer [env]]
    [nano-id.core :refer [nano-id]]
    [synthigy.log :as log]
    [synthigy.traffic :as traffic]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [ring.adapter.undertow :refer [run-undertow]]
    ring.adapter.undertow.response
    [ring.middleware.keyword-params :refer [wrap-keyword-params]]
    [ring.middleware.params :refer [wrap-params]]
    [ring.util.response :as response]
    synthigy.core
    synthigy.oauth
    synthigy.oauth.persistence
    [synthigy.oauth.authentication :as auth]
    [synthigy.server.data :as data]
    [synthigy.server.profile :as profile]
    [synthigy.server.routes :as routes]
    [synthigy.server.subscription :as subscription]))

;;; ============================================================================
;;; Simple Router
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
  "Creates a simple router from route definitions."
  [routes]
  (fn [request]
    (if-let [handler (match-route request routes)]
      (handler request)
      ;; opt-in modules (console, …) register URI prefixes at lifecycle
      ;; start; consulted per request so no server restart is needed
      (routes/extension-handler request))))

;;; ============================================================================
;;; SSE Handler
;;; ============================================================================

(defrecord SSEBody [identity-key stream-chan])

(extend-protocol ring.adapter.undertow.response/RespondBody
  SSEBody
  (respond [{:keys [identity-key stream-chan]} ^io.undertow.server.HttpServerExchange exchange]
    (if (.isInIoThread exchange)
      (.dispatch exchange ^Runnable
        (^:once fn* [] (ring.adapter.undertow.response/respond
                         (->SSEBody identity-key stream-chan) exchange)))
      (do
        (.startBlocking exchange)
        (let [out (.getOutputStream exchange)]
          (.flush out)
          (loop []
            (let [[val port] (async/<!! (async/go
                                          (async/alts!
                                            [stream-chan
                                             (async/timeout 20000)])))]
              (cond
                (and (nil? val) (= port stream-chan))
                (do (subscription/destroy-event-stream identity-key stream-chan)
                    (try (.endExchange exchange) (catch Exception _)))

                (not= port stream-chan)
                (if (try
                      (.write out (.getBytes ^String subscription/keepalive-msg "UTF-8"))
                      (.flush out)
                      true
                      (catch Exception _
                        (subscription/destroy-event-stream identity-key stream-chan)
                        false))
                  (recur)
                  (try (.endExchange exchange) (catch Exception _)))

                :else
                (if (try
                      (.write out (.getBytes ^String (subscription/format-sse val) "UTF-8"))
                      (.flush out)
                      true
                      (catch Exception _
                        (subscription/destroy-event-stream identity-key stream-chan)
                        false))
                  (recur)
                  (try (.endExchange exchange) (catch Exception _)))))))))))

(defn- make-sse-handler
  "Creates a Ring handler for SSE event streaming."
  []
  (fn [request]
    ;; Optional-IAM: token mandatory only when :synthigy/iam runs
    ;; (same contract as the /data handlers).
    (let [iam-active? (lifecycle/started? :synthigy/iam)
          iam (when iam-active? (auth/authenticate-request request))]
      (if (and iam-active? (not iam))
        {:status 401
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})}
        (let [identity-key (subscription/request-identity request iam)
              stream-chan (subscription/create-event-stream identity-key)]
          {:status 200
           :headers {"Content-Type" "text/event-stream"
                     "Cache-Control" "no-cache"
                     "Connection" "keep-alive"}
           :body (->SSEBody identity-key stream-chan)})))))

;;; ============================================================================
;;; Route Tables — table + module gating live in synthigy.server.routes,
;;; shared with httpkit/ring-jetty. This backend only supplies its own SSE
;;; transport (:sse-handler).
;;; ============================================================================

(defn default-routes
  "Route table for the full server (`:synthigy/server`). See
   `synthigy.server.routes/full-routes` for the shared route definitions."
  [opts]
  (routes/full-routes (assoc opts :sse-handler (make-sse-handler))))

(defn bare-routes
  "Route table for `:synthigy/bare-server`. See
   `synthigy.server.routes/bare-routes` for the shared route definitions."
  [opts]
  (routes/bare-routes (assoc opts :sse-handler (make-sse-handler))))

;;; ============================================================================
;;; Middleware Stack
;;; ============================================================================

;;; ============================================================================
;;; Static Resource Serving
;;; ============================================================================

(def ^:private content-type-map
  "Map of file extensions to MIME content types."
  {"css" "text/css"
   "js" "application/javascript"
   "png" "image/png"
   "jpg" "image/jpeg"
   "jpeg" "image/jpeg"
   "gif" "image/gif"
   "svg" "image/svg+xml"
   "ico" "image/x-icon"
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
        ;; Try to serve from classpath
        (let [resource-path (subs uri 1) ; remove leading /
              resp (response/resource-response resource-path)]
          (if resp
            (let [ext (get-extension uri)
                  content-type (get content-type-map ext "application/octet-stream")]
              (assoc-in resp [:headers "Content-Type"] content-type))
            (handler request)))
        (handler request)))))

(defn wrap-request-id
  "Ring middleware that establishes a request-id for the lifetime of the
  request and emits one `::request-completed` info signal per call.

  - Uses an inbound X-Request-Id header if present (preserving upstream
    tracing), otherwise generates a fresh nano-id.
  - The downstream handler runs inside `(log/with-ctx {:request-id ...})`,
    so every signal emitted during the request carries :request-id in ctx
    and lands in the promoted request_id wire column.
  - On the way out, emits `:synthigy.server/request-completed` with method,
    uri, status, and duration_ms — guarantees one row per request even when
    the handler is silent.
  - Echoes the id as the X-Request-Id response header."
  [handler]
  (fn [request]
    (let [req-id   (or (some-> request :headers (get "x-request-id"))
                       (nano-id 10))
          started  (System/currentTimeMillis)
          response (log/with-ctx {:request-id req-id}
                     (let [resp (handler request)
                           duration (- (System/currentTimeMillis) started)]
                       ;; measurement = counters; the DEBUG signal exists for
                       ;; request-id e2e correlation only (no info traffic).
                       (traffic/record! (:status resp) duration)
                       (log/debug
                         {:id :synthigy.server/request-completed
                          :data {:method      (some-> (:request-method request) name)
                                 :uri         (:uri request)
                                 :status      (:status resp)
                                 :duration-ms duration}}
                         "request completed")
                       resp))]
      (assoc-in response [:headers "X-Request-Id"] req-id))))

(defn make-handler
  "Creates the complete Ring handler with all middleware."
  [{:keys [routes info]
    :or {info {}}}]
  (let [routes (or routes (default-routes {:info info}))
        router (routes/wrap-not-found (make-router routes))]
    (-> router
        wrap-static-resources
        cors/wrap-cors
        cors/wrap-options
        wrap-keyword-params
        wrap-params
        ;; LAST in this -> means OUTERMOST, i.e. runs before wrap-params,
        ;; which consumes the body stream — without it /data answers
        ;; INVALID_BODY to every SDK
        data/wrap-preserve-body
        ;; before wrap-request-id, so the error signal is emitted while
        ;; log/with-ctx still carries the request-id
        routes/wrap-errors
        wrap-request-id)))

;;; ============================================================================
;;; Server Lifecycle
;;; ============================================================================

(defonce server (atom nil))

(defn stop
  "Stops the Undertow server."
  []
  (when-let [s @server]
    (log/info {:id ::http-stopping :data {:action :stopping :subject :http-server}} "Stopping HTTP server")
    (.stop s)
    (reset! server nil))
  nil)

(defn start
  "Starts the Undertow HTTP server."
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port info routes]
     :or {host (or (env :synthigy-server-host) "localhost")
          port (or (some-> (env :synthigy-server-port) Integer/parseInt) 7887)}}]

   (stop)
   (log/info {:id ::http-starting :data {:action :starting :subject :http-server :host host :port port}}
             "Starting HTTP server")

   (let [handler (make-handler {:routes routes :info info})
         s (run-undertow handler {:host host
                                  :port port})]
     (reset! server s)
     (log/info {:id ::http-started :data {:action :started :subject :http-server :host host :port port}}
               "HTTP server started")
     nil)))

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point for Undertow server."
  [& _]
  (try
    (start)
    (log/info {:id ::server-running :data {:action :ready :subject :http-server}}
              "Synthigy Undertow server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error! {:id ::server-start-failed :data {:action :starting :subject :http-server}} ex)
      (System/exit 1))))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/server
  {:headline true
   :doc profile/full-doc
   :depends-on profile/full-deps
   :start (fn []
            (profile/guard-mode! :synthigy/server :synthigy/bare-server)
            (log/info {:id ::lifecycle-starting :data {:action :starting}} "HTTP server lifecycle starting")
            (stop)
            (start)
            (log/info {:id ::lifecycle-started :data {:action :started}} "HTTP server lifecycle started"))
   :stop (fn []
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "HTTP server lifecycle stopping")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "HTTP server lifecycle stopped"))})

;;; ============================================================================
;;; Bare server — data-only + live subscriptions, no IAM, no audit
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/bare-server
  ;; Mutually exclusive with :synthigy/server (same Undertow instance,
  ;; same port) — the operator starts one or the other.
  {:doc profile/bare-doc
   :depends-on profile/bare-deps
   :start (fn []
            (profile/guard-mode! :synthigy/bare-server :synthigy/server)
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
  (lifecycle/print-system-report)
  (do
    (lifecycle/start! :synthigy/dataset)
    (lifecycle/start! :synthigy/server)

    (lifecycle/stop! :synthigy/server)
    (start)))
