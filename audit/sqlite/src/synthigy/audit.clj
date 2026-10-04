;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
