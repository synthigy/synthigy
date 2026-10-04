;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.info
  "Serves synthigy.info over HTTP, unauthenticated."
  (:require
   [synthigy.info :as info]
   [synthigy.json :as json]))

(defn handler [_request]
  {:status 200
   :headers {"Content-Type" "application/json"
             "Cache-Control" "max-age=60"
             "Access-Control-Allow-Origin" "*"}
   :body (json/write-str (info/discovery-body))})
