;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.routes
  "Backend-agnostic route tables and module gating shared by the ring HTTP adapters."
  (:require
   [clojure.string :as str]
   [patcho.lifecycle :as lifecycle]
   [synthigy.engine :as engine]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.handlers :as oauth]
   [synthigy.oauth.page.custom :as login-page]
   [synthigy.server.data :as data]
   [synthigy.server.health :as health]
   [synthigy.server.history :as history]
   [synthigy.server.info :as info]
   [synthigy.server.subscription :as subscription]))

(defn module-unavailable [module-key]
  {:status 404
   :headers {"Content-Type" "application/json"}
   :body (json/write-str
          {:error {:code "MODULE_NOT_LOADED"
                   :message (str "Module " module-key " is not running on this server.")}})})

(defn gated
  "Wrap handler so requests 404 unless module-key is in the running lifecycle."
  [module-key handler]
  (fn [request]
    (if (lifecycle/started? module-key)
      (handler request)
      (module-unavailable module-key))))

(defonce ^:private extensions
  (atom {}))

(defn register-prefix!
  "Claim prefix for handler (pass a Var so REPL reloads take effect); longest
   prefix wins at dispatch."
  [key prefix handler]
  (swap! extensions assoc key {:prefix prefix :handler handler})
  nil)

(defn unregister-prefix! [key]
  (swap! extensions dissoc key)
  nil)

(defn extension-handler
  "Dispatch request to the longest matching registered prefix, or nil."
  [request]
  (let [uri (:uri request)]
    (when-let [{:keys [handler]}
               (->> (vals @extensions)
                    (filter (fn [{:keys [prefix]}]
                              (or (= uri prefix)
                                  (str/starts-with? uri (str prefix "/")))))
                    (sort-by (comp count :prefix) >)
                    first)]
      (handler request))))

(defn wrap-not-found
  "Turn a nil response into an explicit 404; every backend wraps its router with this."
  [handler]
  (fn [request]
    (or (handler request)
        {:status 404
         :headers {"Content-Type" "application/json"}
         :body (json/write-str {:error {:code "NOT_FOUND" :message "No matching route"}})})))

(defn wrap-errors
  "Turn an uncaught handler exception into a JSON 500 and a logged signal."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Throwable ex
        (log/error! {:id ::unhandled-exception
                     :msg "Unhandled exception escaped the handler"
                     :data {:action :failed
                            :subject :http-server
                            :method (:request-method request)
                            :uri (:uri request)}}
                    ex)
        (data/json-response
         500
         {:error (engine/with-request-id
                   {:code "INTERNAL_ERROR"
                    :message "Internal server error"})})))))

(defonce ^:private sse-transport (atom nil))

(defn register-sse-transport!
  "Install the running backend's SSE adapter; last write wins."
  [f]
  (reset! sse-transport f)
  nil)

(defn unregister-sse-transport! []
  (reset! sse-transport nil)
  nil)

(defn sse-available?
  "Whether the running backend can stream; callers degrade to a static render otherwise."
  []
  (some? @sse-transport))

(defn sse-response
  "Ring response streaming via the registered backend adapter; 503s when nothing
   is registered."
  [request opts]
  (if-let [f @sse-transport]
    (f request opts)
    {:status 503
     :headers {"Content-Type" "application/json"}
     :body (json/write-str
            {:error {:code "SSE_UNAVAILABLE"
                     :message "This server backend does not provide SSE streaming."}})}))

(defn full-routes
  "Route table for :synthigy/server (the full deployment)."
  [{:keys [info sse-handler] :or {info {}}}]
  (register-prefix! ::login-pages login-page/mount-prefix (gated :synthigy/iam #'login-page/handler))
  (let [info-handler (fn [_]
                       {:status 200
                        :headers {"Content-Type" "application/json"}
                        :body (json/write-str info)})]
    [["/info" :get info-handler]

     ["/oauth/authorize" :get (gated :synthigy/iam #'oauth/authorize)]
     ["/oauth/token" :post (gated :synthigy/iam #'oauth/token)]
     ["/oauth/login" :get (gated :synthigy/iam #'oauth/login)]
     ["/oauth/login" :post (gated :synthigy/iam #'oauth/login)]
     ["/oauth/logout" :get (gated :synthigy/iam #'oauth/logout)]
     ["/oauth/logout" :post (gated :synthigy/iam #'oauth/logout)]
     ["/oauth/revoke" :post (gated :synthigy/iam #'oauth/revoke)]
     ["/oauth/introspect" :post (gated :synthigy/iam #'oauth/introspect)]

     ["/oauth/status" :get (gated :synthigy/iam #'oauth/oauth-status)]

     ["/oauth/device/auth" :post (gated :synthigy/iam #'oauth/device-authorization)]
     ["/oauth/device/activate" :get (gated :synthigy/iam #'oauth/device-activation)]
     ["/oauth/device/activate" :post (gated :synthigy/iam #'oauth/device-activation)]
     ["/oauth/device/status" :get (gated :synthigy/iam #'oauth/oauth-status)]

     ["/oauth/federated/start" :get (gated :synthigy/iam #'oauth/federated-start)]
     ["/oauth/federated/callback" :get (gated :synthigy/iam #'oauth/federated-callback)]
     ["/oauth/federated/providers" :get (gated :synthigy/iam #'oauth/federated-providers)]
     ["/oauth/federated/identities" :get (gated :synthigy/iam #'oauth/federated-identities)]
     ["/oauth/federated/unlink" :post (gated :synthigy/iam #'oauth/federated-unlink)]
     ["/oauth/federated/client/identities" :get (gated :synthigy/iam #'oauth/federated-client-identities)]
     ["/oauth/federated/client/unlink" :post (gated :synthigy/iam #'oauth/federated-client-unlink)]

     ["/oauth/onboard" :post (gated :synthigy/iam #'oauth/onboard)]
     ["/oauth/onboard/complete" :post (gated :synthigy/iam #'oauth/onboard-complete)]
     ["/oauth/claim" :get (gated :synthigy/iam #'oauth/claim)]
     ["/oauth/claim/password" :post (gated :synthigy/iam #'oauth/claim-password)]
     ["/oauth/password" :post (gated :synthigy/iam #'oauth/change-password)]

     ["/oauth/userinfo" :get (gated :synthigy/iam #'oauth/userinfo)]
     ["/oauth/jwks" :get (gated :synthigy/iam #'oauth/jwks)]
     ["/.well-known/openid-configuration" :get (gated :synthigy/iam #'oauth/openid-configuration)]

             ;; never gate — must answer pre-auth
     ["/.well-known/synthigy" :get #'info/handler]
     ["/health" :get (health/handler [:synthigy/dataset :synthigy/iam])]

     ["/data" :post #'data/handler]

     ["/logs" :post #'data/logs-handler]

     ["/schema" :get #'data/schema-handler]
     ["/lint" :post #'data/lint-handler]
     ["/compile" :post #'data/compile-handler]

             ;; IAM configuration transfer — ROOT-gated inside the handler
     ["/iam/export" :get (gated :synthigy/iam #'data/iam-export-handler)]
     ["/iam/import" :post (gated :synthigy/iam #'data/iam-import-handler)]
     ["/iam/clients" :post (gated :synthigy/iam #'data/iam-add-client-handler)]

     ["/history" :post #'history/handler]

     ["/data/subscription/set" :post (gated :synthigy/plug #'subscription/set-handler)]
     ["/data/subscription/status" :get (gated :synthigy/plug #'subscription/status-handler)]
     ["/data/events" :get (gated :synthigy/plug sse-handler)]]))

(defn bare-routes
  "Route table for :synthigy/bare-server, the data-only deployment."
  [{:keys [info sse-handler] :or {info {}}}]
  (let [info-handler (fn [_]
                       {:status 200
                        :headers {"Content-Type" "application/json"}
                        :body (json/write-str info)})]
    [["/info" :get info-handler]
     ["/.well-known/synthigy" :get #'info/handler]
     ["/health" :get (health/handler [:synthigy/dataset])]
     ["/data" :post #'data/handler]
     ["/logs" :post #'data/logs-handler]
     ["/schema" :get #'data/schema-handler]
     ["/lint" :post #'data/lint-handler]
     ["/compile" :post #'data/compile-handler]
     ["/data/subscription/set" :post (gated :synthigy/plug #'subscription/set-handler)]
     ["/data/subscription/status" :get (gated :synthigy/plug #'subscription/status-handler)]
     ["/data/events" :get (gated :synthigy/plug sse-handler)]
     ["/history" :post (gated :synthigy/audit #'history/handler)]]))
