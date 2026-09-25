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
  "ring-jetty (Jetty 12) HTTP server implementation for Synthigy.

   Provides core HTTP infrastructure:
   - OAuth 2.0 + OpenID Connect endpoints
   - Data API (/data)
   - Data subscriptions (SSE)
   - Authentication middleware
   - CORS configuration
   - SPA serving

   Uses ring/ring-jetty-adapter (Jetty 12) with the Ring 1.11+ spec
   for StreamableResponseBody and the ring.websocket protocol.


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
    [clojure.string :as str]
    [environ.core :refer [env]]
    [nano-id.core :refer [nano-id]]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [ring.adapter.jetty :as jetty]
    [ring.core.protocols :as ring-proto]
    [starfederation.datastar.clojure.adapter.ring :as d*ring]
    [ring.middleware.keyword-params :refer [wrap-keyword-params]]
    [ring.middleware.params :refer [wrap-params]]
    [ring.util.response :as response]
    [synthigy.json :as json]
    [synthigy.log :as log]
    [synthigy.traffic :as traffic]
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
  [routes]
  (fn [request]
    (if-let [handler (match-route request routes)]
      (handler request)
      ;; opt-in modules (console, …) register URI prefixes at lifecycle
      ;; start; consulted per request so no server restart is needed
      (routes/extension-handler request))))

;;; ============================================================================
;;; SSE Handler (Jetty StreamableResponseBody)
;;; ============================================================================

(defrecord SSEBody [identity-key stream-chan]
  ring-proto/StreamableResponseBody
  (write-body-to-stream [_ _response out]
    (try
      (.flush out)
      (loop []
        (let [[val port] (async/alts!! [stream-chan (async/timeout 20000)])]
          (cond
            (and (nil? val) (= port stream-chan))
            nil

            (not= port stream-chan)
            (if (try
                  (.write out (.getBytes ^String subscription/keepalive-msg "UTF-8"))
                  (.flush out)
                  true
                  (catch Exception _ false))
              (recur)
              nil)

            :else
            (if (try
                  (.write out (.getBytes ^String (subscription/format-sse val) "UTF-8"))
                  (.flush out)
                  true
                  (catch Exception _ false))
              (recur)
              nil))))
      (finally
        (subscription/destroy-event-stream identity-key stream-chan)
        (try (.close out) (catch Exception _))))))

(defn- make-sse-handler
  []
  (fn [request]
    (let [request (if (get-in request [:headers "authorization"])
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
              stream-chan (subscription/create-event-stream identity-key)]
          {:status 200
           :headers {"Content-Type" "text/event-stream"
                     "Cache-Control" "no-cache"
                     "Connection" "keep-alive"}
           :body (->SSEBody identity-key stream-chan)})))))

;;; ============================================================================
;;; Route Tables — table + module gating live in synthigy.server.routes,
;;; shared with httpkit/undertow. This backend only supplies its own SSE
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
;;; Middleware
;;; ============================================================================

;;; ============================================================================
;;; Static Resource Serving
;;; ============================================================================

(def ^:private content-type-map
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
  [{:keys [routes info]
    :or {info {}}}]
  (let [routes (or routes (default-routes {:info info}))
        router (routes/wrap-not-found (make-router routes))]
    (wrap-request-id
      ;; INSIDE wrap-request-id, never outside — the error signal must be
      ;; emitted while log/with-ctx still carries the request-id.
      (routes/wrap-errors
        (-> router
            wrap-static-resources
            cors/wrap-cors
            cors/wrap-options
            wrap-keyword-params
            wrap-params
            data/wrap-preserve-body)))))

;;; ============================================================================
;;; Server Lifecycle
;;; ============================================================================

(defonce server (atom nil))

(defn stop
  []
  (when-let [s @server]
    (log/info {:id ::http-stopping :data {:action :stopping :subject :http-server}} "Stopping HTTP server")
    (.stop s)
    ;; drop the transport with the server that supplied it
    (routes/unregister-sse-transport!)
    (reset! server nil))
  nil)

(defn start
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port info routes]
     :or {host (or (env :synthigy-server-host) "localhost")
          port (or (some-> (env :synthigy-server-port) Integer/parseInt) 7887)}}]

   (stop)
   (log/info {:id ::http-starting :data {:action :starting :subject :http-server :host host :port port}}
             "Starting HTTP server")

   (let [handler (make-handler {:routes routes :info info})
         s (jetty/run-jetty handler {:host host
                                     :port port
                                     :join? false})]
     ;; Opt-in modules register their prefix after boot, so they can't be
     ;; handed :sse-handler as an argument — they look the adapter up.
     (routes/register-sse-transport! d*ring/->sse-response)
     (reset! server s)
     (log/info {:id ::http-started :data {:action :started :subject :http-server :host host :port port}}
               "HTTP server started")
     nil)))

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  [& _]
  (try
    (start)
    (log/info {:id ::server-running :data {:action :ready :subject :http-server}}
              "Synthigy ring-jetty server running. Press Ctrl+C to stop.")
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
  ;; Mutually exclusive with :synthigy/server (same Jetty instance, same
  ;; port) — the operator starts one or the other.
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
  (lifecycle/start! :synthigy/server)
  (lifecycle/start! :synthigy/bare-server)
  (start)
  (stop))
