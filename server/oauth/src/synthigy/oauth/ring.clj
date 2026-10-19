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

(ns synthigy.oauth.ring
  "Ring utilities so the OAuth/OIDC handlers work with any Ring framework."
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
