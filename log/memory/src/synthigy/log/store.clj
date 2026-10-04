;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.log.store
  "Log-store dimension: the in-memory ring, with in-memory traffic."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.log.store.protocol :as protocol]
   [synthigy.log.store.ring :as ring]))

(defn open-store
  "An isolated store instance; the lifecycle module binds its own."
  [config]
  (ring/create-ring config))

(defn close-store! [_])

(lifecycle/register-module!
 :synthigy.log/store
 {:depends-on [:synthigy/log]
  :doc "Logs in a bounded in-memory ring; traffic stays in memory"
  :start (fn [] (alter-var-root #'protocol/*log-store* #(or % (ring/create-ring))))
  :stop  (fn [])})
