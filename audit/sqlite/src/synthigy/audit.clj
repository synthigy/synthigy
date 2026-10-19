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

(ns synthigy.audit
  "Audit dimension: history in its own SQLite file beside the data."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.audit.sqlite :as sqlite]))

(defn open-provider
  "An isolated provider instance (`:path`, default in-memory); the lifecycle module binds its own."
  [{:keys [path]}]
  (sqlite/open-provider (or path ":memory:")))

(defn close-provider! [p]
  (sqlite/close-provider! p))

(lifecycle/register-module!
 :synthigy/audit
 {:depends-on [:synthigy/plug]
  :doc "Audit history in audit.db beside the data database"
  :setup   sqlite/setup
  :start   sqlite/start
  :stop    sqlite/stop
  :cleanup sqlite/cleanup})
