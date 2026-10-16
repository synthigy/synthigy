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
  "Pedestal HTTP server adapter for Synthigy."
  (:require
    [synthigy.json :as json]
    clojure.set
    [clojure.string :as str]
    [environ.core :refer [env]]
    [io.pedestal.http :as http]
    [io.pedestal.http.content-negotiation :as conneg]
    [io.pedestal.http.cors :as cors]
    [synthigy.client-address :as client-address]
    [synthigy.cors :as origin-policy]
    [io.pedestal.interceptor :as interceptor]
    [io.pedestal.interceptor.chain :as chain]
    [io.pedestal.http.ring-middlewares :as middlewares]
    [io.pedestal.http.route :as route]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [ring.util.response :as response]
    [synthigy.log :as log]
    [synthigy.traffic :as traffic]
    synthigy.core
    synthigy.audit
    synthigy.log.store
    synthigy.oauth
    [synthigy.oauth.handlers :as oauth.handlers]
    synthigy.oauth.persistence
    [synthigy.oauth.authentication :as auth]
    [synthigy.server.data :as data]
    [synthigy.server.health :as health]
    [synthigy.server.history :as history]
    [synthigy.oauth.page.custom :as login-page]
    [synthigy.server.profile :as profile]
    [synthigy.server.routes :as shared-routes]
    [synthigy.server.subscription :as subscription]))

(def coerce-body
  "Pedestal interceptor merging a JSON request body into :params."
  {:name ::coerce-body
   :enter
   (fn [ctx]
     (let [{{content-type "content-type"} :headers
            body :body
            :as request} (:request ctx)]
       (if (and body content-type (str/starts-with? content-type "application/json"))
         ;; never slurp without replacing — data/preserve-body is the one shared implementation
         (let [request (data/preserve-body request)]
           (try
             (assoc ctx :request
                    (update request :params merge (json/read-str (:raw-body request))))
             (catch Exception e
               (log/warn {:id ::json-body-parse-failed :error e}
                         "Failed to parse JSON body")
               (assoc ctx :request request))))
         ctx)))})

(def json-response
  "Pedestal interceptor to ensure JSON response Content-Type header."
  {:name ::json-response
   :leave
   (fn [ctx]
     (assoc-in ctx [:response :headers "Content-Type"] "application/json"))})

(defn make-info-interceptor
  [info]
  {:name ::info
   :enter
   (fn [ctx]
     (assoc ctx :response
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (json/write-str (or info {}))}))})

(def supported-types
  ["text/html"
   "application/transit+json"
   "application/edn"
   "application/json"
   "text/plain"])

(def content-negotiation
  (conneg/negotiate-content supported-types))

(defn ring-handler->pedestal-interceptor
  "Pedestal interceptor running a Ring handler under the request's log context and resolved client address."
  ([handler] (ring-handler->pedestal-interceptor handler ::ring-handler))
  ([handler name]
   {:name name
    :enter
    (fn [ctx]
      (let [req-ctx  (client-address/request-context (:request ctx))
            request  (assoc (:request ctx) :remote-addr (:ip req-ctx))
            started  (System/currentTimeMillis)
            response (log/with-ctx (merge (auth/log-context request) req-ctx)
                       (let [resp (handler request)
                             duration (- (System/currentTimeMillis) started)]
                         (traffic/record! (:status resp) duration)
                         (log/debug
                           {:id :synthigy.server/request-completed
                            :data {:method      (some-> (:request-method request) clojure.core/name)
                                   :uri         (:uri request)
                                   :status      (:status resp)
                                   :duration-ms duration}}
                           "request completed")
                         resp))]
        (assoc ctx :response
               (assoc-in response [:headers "X-Request-Id"] (:request-id req-ctx)))))}))

(defn default-routes
  "Default Synthigy Pedestal routes."
  [info]
  #{["/info" :get [coerce-body content-negotiation (make-info-interceptor info)]
     :route-name ::info]

    ["/health" :get [(ring-handler->pedestal-interceptor
                      (health/handler [:synthigy/dataset :synthigy/iam]) ::health)]
     :route-name ::health]

    ["/data" :post [coerce-body
                    (ring-handler->pedestal-interceptor #'data/handler ::data-api)]
     :route-name ::data-api]

    ["/logs" :post [coerce-body
                    (ring-handler->pedestal-interceptor #'data/logs-handler ::logs-api)]
     :route-name ::logs-api]

    ["/history" :post [coerce-body
                       (ring-handler->pedestal-interceptor #'history/handler ::history-api)]
     :route-name ::history-api]

    ["/data/subscription/set" :post [coerce-body
                                     (ring-handler->pedestal-interceptor #'subscription/set-handler ::subscription-set)]
     :route-name ::subscription-set]
    ["/data/subscription/status" :get [(ring-handler->pedestal-interceptor #'subscription/status-handler ::subscription-status)]
     :route-name ::subscription-status]

    ["/schema" :get [(ring-handler->pedestal-interceptor #'data/schema-handler ::schema)]
     :route-name ::schema]

    ["/lint" :post [(ring-handler->pedestal-interceptor #'data/lint-handler ::lint)]
     :route-name ::lint]

    ["/iam/export" :get [(ring-handler->pedestal-interceptor #'data/iam-export-handler ::iam-export)]
     :route-name ::iam-export]
    ["/iam/import" :post [(ring-handler->pedestal-interceptor #'data/iam-import-handler ::iam-import)]
     :route-name ::iam-import]
    ["/iam/clients" :post [(ring-handler->pedestal-interceptor #'data/iam-add-client-handler ::iam-add-client)]
     :route-name ::iam-add-client]})

(def oauth-routes
  "OAuth 2.0 and OpenID Connect endpoint routes."
  #{["/oauth/authorize" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/authorize ::oauth-authorize)]
     :route-name ::oauth-authorize]

    ["/oauth/token" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/token ::oauth-token)]
     :route-name ::oauth-token]

    ["/oauth/status" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/oauth-status ::oauth-status)]
     :route-name ::oauth-status]

    ["/oauth/device/status" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/oauth-status ::oauth-device-status)]
     :route-name ::oauth-device-status]

    ["/oauth/login" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/login ::oauth-login)]
     :route-name ::oauth-login-get]

    ["/oauth/login" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/login ::oauth-login)]
     :route-name ::oauth-login-post]

    ["/oauth/logout" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/logout ::oauth-logout)]
     :route-name ::oauth-logout-get]

    ["/oauth/logout" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/logout ::oauth-logout)]
     :route-name ::oauth-logout-post]

    ["/oauth/revoke" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/revoke ::oauth-revoke)]
     :route-name ::oauth-revoke]

    ["/oauth/introspect" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/introspect ::oauth-introspect)]
     :route-name ::oauth-introspect]

    ["/oauth/device/auth" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/device-authorization ::oauth-device-auth)]
     :route-name ::oauth-device-auth]

    ["/oauth/device/activate" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/device-activation ::oauth-device-activate)]
     :route-name ::oauth-device-activate-get]

    ["/oauth/device/activate" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/device-activation ::oauth-device-activate)]
     :route-name ::oauth-device-activate-post]

    ["/oauth/federated/start" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-start ::oauth-federated-start)]
     :route-name ::oauth-federated-start]

    ["/oauth/federated/callback" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-callback ::oauth-federated-callback)]
     :route-name ::oauth-federated-callback]
    ["/oauth/federated/providers" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-providers ::oauth-federated-providers)]
     :route-name ::oauth-federated-providers]
    ["/oauth/federated/identities" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-identities ::oauth-federated-identities)]
     :route-name ::oauth-federated-identities]
    ["/oauth/federated/unlink" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-unlink ::oauth-federated-unlink)]
     :route-name ::oauth-federated-unlink]
    ["/oauth/federated/client/identities" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-client-identities ::oauth-federated-client-identities)]
     :route-name ::oauth-federated-client-identities]
    ["/oauth/federated/client/unlink" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/federated-client-unlink ::oauth-federated-client-unlink)]
     :route-name ::oauth-federated-client-unlink]

    ["/oauth/onboard" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/onboard ::oauth-onboard)]
     :route-name ::oauth-onboard]
    ["/oauth/onboard/complete" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/onboard-complete ::oauth-onboard-complete)]
     :route-name ::oauth-onboard-complete]
    ["/oauth/claim" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/claim ::oauth-claim)]
     :route-name ::oauth-claim]
    ["/oauth/claim/password" :post
     [(ring-handler->pedestal-interceptor #'oauth.handlers/claim-password ::oauth-claim-password)]
     :route-name ::oauth-claim-password]

    ["/oauth/userinfo" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/userinfo ::oidc-userinfo)]
     :route-name ::oidc-userinfo]

    ["/oauth/jwks" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/jwks ::oidc-jwks)]
     :route-name ::oidc-jwks]

    ["/.well-known/openid-configuration" :get
     [(ring-handler->pedestal-interceptor #'oauth.handlers/openid-configuration ::oidc-discovery)]
     :route-name ::oidc-discovery]})

(def ^:private content-type-map
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

(defn get-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (subs path (inc idx))))

(def serve-resource
  "Interceptor serving the request URI as a classpath resource."
  {:name ::serve-resource
   :enter (fn [{{:keys [uri]} :request :as ctx}]
            (let [resource-path (subs uri 1)
                  resp (response/resource-response resource-path)]
              (if resp
                (let [ext (get-extension uri)
                      content-type (get content-type-map ext "application/octet-stream")]
                  (assoc ctx :response
                         (assoc-in resp [:headers "Content-Type"] content-type)))
                (assoc ctx :response {:status 404 :body "Not found"}))))})

(def static-routes
  "Routes for static OAuth resources and the custom login page bundles at /login/<name>/*."
  #{["/oauth/css/*" :get [serve-resource] :route-name ::static-css]
    ["/oauth/js/*" :get [serve-resource] :route-name ::static-js]
    ["/oauth/images/*" :get [serve-resource] :route-name ::static-images]
    ["/login/*" :get [(ring-handler->pedestal-interceptor #'login-page/handler ::login-page)]
     :route-name ::login-page]})

(defonce server (atom nil))

(defn stop
  "Stops the Pedestal HTTP server."
  ([]
   (when-let [running-server @server]
     (log/info {:id ::http-stopping :data {:action :stopping :subject :http-server}} "Stopping HTTP server")
     (http/stop running-server)
     (reset! server nil))
   nil))

(defn start
  "Starts the Pedestal HTTP server, replacing any running one."
  ([] (start {:info (patch/available-versions :synthigy/dataset :synthigy/iam)}))
  ([{:keys [host port routes service-initializer info]
     :or {host (or (env :synthigy-server-host) "localhost")
          port (or (some-> (env :synthigy-server-port) Integer/parseInt) 7887)
          routes #{}
          service-initializer identity}}]

   (log/info {:id ::http-starting :data {:action :starting :subject :http-server :host host :port port}}
             "Starting HTTP server")
   (stop)

   (let [all-routes (route/expand-routes
                      (clojure.set/union
                        (default-routes info)
                        oauth-routes
                        static-routes
                        routes))
         router (route/router all-routes :map-tree)

         ;; must run on :enter and terminate — a :leave fill-in empties every matched response
         extensions (interceptor/interceptor
                      {:name ::prefix-extensions
                       :enter
                       (fn [ctx]
                         (if-let [resp (shared-routes/extension-handler (:request ctx))]
                           (-> ctx (assoc :response resp) chain/terminate)
                           ctx))})

         interceptors [;; never move below allow-origin — first in the vector runs last on :leave
                       (interceptor/interceptor
                        {:name ::vary-origin
                         :leave (fn [ctx]
                                  (if (get-in ctx [:request :headers "origin"])
                                    (update-in ctx [:response :headers] origin-policy/vary-origin)
                                    ctx))})
                       (cors/allow-origin {:allowed-origins origin-policy/allowed?})
                       (middlewares/content-type {:mime-types {}})
                       route/query-params
                       (route/method-param)
                       extensions
                       router]

         service-map
         {::http/type :jetty
          ::http/join? false
          ::http/host host
          ::http/port port
          ::http/interceptors interceptors}

         final-service-map (service-initializer service-map)

         _server (-> final-service-map
                     http/create-server
                     http/start)]

     (reset! server _server)
     (log/info {:id ::http-started :data {:action :started :subject :http-server :host host :port port}}
               "HTTP server started")
     nil)))

(defn -main
  "Main entry point for the Pedestal server."
  [& _]
  (try
    (start)
    (log/info {:id ::server-running :data {:action :ready :subject :http-server}}
              "Synthigy Pedestal server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error! {:id ::server-start-failed :data {:action :starting :subject :http-server}} ex)
      (System/exit 1))))

(lifecycle/register-module!
  :synthigy/server
  {:headline true
   :doc profile/full-doc
   :depends-on profile/full-deps
   :start (fn []
            (log/info {:id ::lifecycle-starting :data {:action :starting}} "HTTP server lifecycle starting")
            (stop)
            (start)
            (log/info {:id ::lifecycle-started :data {:action :started}} "HTTP server lifecycle started"))
   :stop (fn []
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "HTTP server lifecycle stopping")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "HTTP server lifecycle stopped"))})

(comment
  (lifecycle/print-topology-layers)
  (lifecycle/system-report)
  (lifecycle/print-system-report)
  (lifecycle/start! :synthigy/dataset)
  (lifecycle/start! :synthigy.iam/audit)
  (lifecycle/start! :synthigy/server))
