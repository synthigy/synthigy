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

(ns synthigy.oauth.ring
  "Ring utilities replacing Pedestal interceptor dependencies, so OAuth/OIDC
   handlers work with any Ring framework. See docs/core/synthigy/oauth/ring.md."
  (:require
   [clojure.walk :as walk]))

;; =============================================================================
;; Middleware Composition
;; =============================================================================

(defn compose-middleware
  "Compose middleware left-to-right (last argument is the handler), mirroring
   Pedestal's (conj interceptors)."
  [& args]
  (if (empty? args)
    identity
    (let [handler (last args)
          middleware (butlast args)]
      (reduce (fn [h mw] (mw h))
              handler
              (reverse middleware)))))

;; =============================================================================
;; State Passing (Replaces Pedestal Context Map)
;; =============================================================================

(defn pass-state
  "Pass custom state through the request map (replaces Pedestal's ctx keys)."
  [request k v]
  (assoc request k v))

(defn get-state
  [request k]
  (get request k))

(defn update-state
  [request k f & args]
  (apply update request k f args))

;; =============================================================================
;; Parameter Utilities
;; =============================================================================

(defn keywordize-params
  [params]
  (walk/keywordize-keys params))

(defn stringify-params
  [params]
  (walk/stringify-keys params))

;; =============================================================================
;; Response Utilities
;; =============================================================================

(defn set-cookie
  ([response cookie-name value]
   (set-cookie response cookie-name value {}))
  ([response cookie-name value opts]
   (assoc-in response [:cookies cookie-name]
             (merge {:value value} opts))))

(defn delete-cookie
  "Delete a cookie by setting max-age to 0."
  [response cookie-name]
  (assoc-in response [:cookies cookie-name]
            {:value "" :max-age 0 :path "/"}))

(defn merge-cookies
  [response cookies]
  (update response :cookies merge cookies))

;; =============================================================================
;; Early Return / Short-Circuit (Replaces chain/terminate)
;; =============================================================================

(defn short-circuit?
  "Documents that returning a response map instead of calling (handler request)
   IS Ring's equivalent of Pedestal's chain/terminate."
  [response]
  (and (map? response)
       (contains? response :status)))

;; =============================================================================
;; CORS Support (Replaces Pedestal allow-origin)
;; =============================================================================

(defn wrap-cors
  "Add CORS headers to responses; :allowed-origins may be :all, a set, or a
   coll."
  [handler {:keys [allowed-origins
                   allowed-methods
                   allowed-headers
                   max-age
                   allow-credentials]
            :or {allowed-methods #{:get :post :put :delete :options}
                 allowed-headers #{"*"}
                 max-age 3600
                 allow-credentials true}}]
  (fn [request]
    (let [origin (get-in request [:headers "origin"])
          origin-allowed? (cond
                            (= :all allowed-origins) true
                            (set? allowed-origins) (contains? allowed-origins origin)
                            (coll? allowed-origins) (some #(= % origin) allowed-origins)
                            :else false)]
      (if-not origin-allowed?
        ;; Origin not allowed - proceed without CORS headers
        (handler request)
        ;; Origin allowed - add CORS headers
        (if (= :options (:request-method request))
          ;; Handle preflight request
          {:status 200
           :headers {"Access-Control-Allow-Origin" origin
                     "Access-Control-Allow-Methods" (clojure.string/join ", " (map name allowed-methods))
                     "Access-Control-Allow-Headers" (if (set? allowed-headers)
                                                      (clojure.string/join ", " allowed-headers)
                                                      (first allowed-headers))
                     "Access-Control-Max-Age" (str max-age)
                     "Access-Control-Allow-Credentials" (str allow-credentials)}}
          ;; Normal request - add CORS headers to response
          (let [response (handler request)]
            (update response :headers merge
                    {"Access-Control-Allow-Origin" origin
                     "Access-Control-Allow-Credentials" (str allow-credentials)})))))))

(comment
  ;; Example usage

  ;; Compose middleware
  (def my-handler
    (compose-middleware
     wrap-params
     wrap-cookies
     wrap-keywordize
     my-oauth-handler))

  ;; Pass state through request
  (defn wrap-add-session [handler]
    (fn [request]
      (let [session-id (generate-session-id)]
        (handler (pass-state request ::session session-id)))))

  ;; Get state in handler
  (defn my-handler [request]
    (let [session-id (get-state request ::session)]
      {:status 200
       :body (str "Session: " session-id)}))

  ;; Set cookie in response
  (defn login-handler [request]
    (let [session-id (create-session)
          response {:status 302 :headers {"Location" "/"}}]
      (set-cookie response "idsrv.session" session-id
                  {:http-only true
                   :secure true
                   :same-site :none})))

  ;; Short-circuit pattern
  (defn wrap-auth [handler]
    (fn [request]
      (if (authenticated? request)
        (handler request)  ; Continue
        {:status 401 :body "Unauthorized"}))))  ; Short-circuit
