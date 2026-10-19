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

(ns synthigy.cors
  "Which browser origins get CORS headers; the oauth module narrows the default to registered clients.")


(defn allow-all [_origin] true)

(defonce ^:dynamic *origin-allowed?* allow-all)

(defn allowed?
  [origin]
  (boolean (and origin (*origin-allowed?* origin))))

(defn vary-origin
  "A response that depends on the request Origin must say so, or a cache serves one origin's answer to the next."
  [headers]
  (assoc headers "Vary" (if-let [v (get headers "Vary")] (str v ", Origin") "Origin")))

(defn headers
  [origin]
  {"Access-Control-Allow-Origin" origin
   "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
   "Access-Control-Allow-Headers" "Content-Type, Authorization"
   "Access-Control-Allow-Credentials" "true"})

(defn response-headers
  "CORS headers for a response to `origin`, merged over `base`."
  [base origin]
  (cond
    (allowed? origin) (vary-origin (merge base (headers origin)))
    origin (vary-origin base)
    :else base))

(defn wrap-cors
  [handler]
  (fn [request]
    (when-let [response (handler request)]
      (update response :headers response-headers (get-in request [:headers "origin"])))))

(defn wrap-options
  "Answer preflights; an origin outside the policy gets no CORS headers, so the browser blocks the real request."
  [handler]
  (fn [request]
    (if (= :options (:request-method request))
      {:status 204
       :headers (response-headers {} (get-in request [:headers "origin"]))}
      (handler request))))
