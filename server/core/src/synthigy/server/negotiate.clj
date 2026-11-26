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

(ns synthigy.server.negotiate
  "Answer format chosen from the request's Accept: transit, EDN or JSON."
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [synthigy.json :as json]
   [synthigy.transit :as transit]))

(defn accept-type [request]
  (some-> (get-in request [:headers "accept"])
          (str/split #",")
          first
          (str/split #";")
          first
          str/trim
          str/lower-case))

(defn response
  "Ring response with `body` written in the format the request's Accept names; JSON when it names none we speak."
  [request status body]
  (case (accept-type request)
    "application/transit+json"
    {:status status
     :headers {"Content-Type" "application/transit+json"}
     :body (or (transit/->transit body) "{\"~#'\":null}")}

    "application/edn"
    {:status status
     :headers {"Content-Type" "application/edn"}
     :body (pr-str body)}

    {:status status
     :headers {"Content-Type" "application/json"}
     :body (json/write-str body)}))

(defn read-body
  "The string body of a `response` read back by its Content-Type; nil when it does not parse."
  [{:keys [body headers]}]
  (when (string? body)
    (try
      (case (get headers "Content-Type")
        "application/transit+json" (transit/<-transit body)
        "application/edn" (edn/read-string body)
        (json/read-str body))
      (catch Exception _ nil))))
