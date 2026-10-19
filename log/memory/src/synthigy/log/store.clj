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
