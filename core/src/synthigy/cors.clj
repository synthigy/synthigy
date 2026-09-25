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
