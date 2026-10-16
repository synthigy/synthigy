;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server
  "Undertow HTTP server adapter for Synthigy."
  (:require
    [synthigy.client-address :as client-address]
    [synthigy.cors :as cors]
    [clojure.core.async :as async]
    [synthigy.json :as json]
    [clojure.string :as str]
    [environ.core :refer [env]]
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
    synthigy.audit
    synthigy.log.store
    synthigy.oauth
    synthigy.oauth.persistence
    [synthigy.oauth.authentication :as auth]
    [synthigy.server.data :as data]
    [synthigy.server.profile :as profile]
    [synthigy.server.routes :as routes]
    [synthigy.server.subscription :as subscription]))

(defn match-route
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
  "Ring handler dispatching on exact [uri method handler] route vectors."
  [routes]
  (fn [request]
    (if-let [handler (match-route request routes)]
      (handler request)
      (routes/extension-handler request))))

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

(defn make-sse-handler
  "Ring handler streaming the identity's subscription events over Undertow."
  []
  (fn [request]
    (let [request (if (get-in request [:headers "authorization"])
                    request
                    (if-let [token (get-in request [:query-params "access_token"])]
                      (assoc-in request [:headers "authorization"] (str "Bearer " token))
                      request))
          iam-active? (lifecycle/started? :synthigy/iam)
          iam (when iam-active? (auth/authenticate-request request))]
      (if (and iam-active? (not iam))
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" (auth/challenge request)}
         :body (json/write-str {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})}
        (let [identity-key (subscription/request-identity request iam)
              stream-chan (subscription/create-event-stream identity-key)]
          {:status 200
           :headers {"Content-Type" "text/event-stream"
                     "Cache-Control" "no-cache"
                     "Connection" "keep-alive"}
           :body (->SSEBody identity-key stream-chan)})))))

(defn default-routes
  "Route table for the full server (`:synthigy/server`)."
  [opts]
  (routes/full-routes (assoc opts :sse-handler (make-sse-handler))))

(defn bare-routes
  "Route table for `:synthigy/bare-server`."
  [opts]
  (routes/bare-routes (assoc opts :sse-handler (make-sse-handler))))

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

(defn get-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (subs path (inc idx))))

(defn wrap-static-resources
  "Middleware serving /oauth/css|js|images/* from the classpath."
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
  "Ring middleware binding the request's log context and resolved client address."
  [handler]
  (fn [request]
    (let [req-ctx  (client-address/request-context request)
          request  (assoc request :remote-addr (:ip req-ctx))
          started  (System/currentTimeMillis)
          response (log/with-ctx (merge (auth/log-context request) req-ctx)
                     (let [resp (handler request)
                           duration (- (System/currentTimeMillis) started)]
                       (traffic/record! (:status resp) duration)
                       (log/debug
                         {:id :synthigy.server/request-completed
                          :data {:method      (some-> (:request-method request) name)
                                 :uri         (:uri request)
                                 :status      (:status resp)
                                 :duration-ms duration}}
                         "request completed")
                       resp))]
      (assoc-in response [:headers "X-Request-Id"] (:request-id req-ctx)))))

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
        ;; keep outermost — body readers downstream need :raw-body and a fresh stream
        data/wrap-preserve-body
        ;; must stay inside wrap-request-id — the error signal needs the request-id ctx
        routes/wrap-errors
        wrap-request-id)))

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
  "Starts the Undertow HTTP server, replacing any running one."
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

(lifecycle/register-module!
  :synthigy/bare-server
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
