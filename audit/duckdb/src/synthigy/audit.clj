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
  "Audit dimension: history in its own embedded DuckDB file."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.audit.duckdb :as duckdb]))

(defn open-provider
  "An isolated provider instance (`:path`, default in-memory); the lifecycle module binds its own."
  [{:keys [path]}]
  (duckdb/open-provider (or path ":memory:")))

(defn close-provider! [p]
  (duckdb/close-provider! p))

(lifecycle/register-module!
 :synthigy/audit
 {:depends-on [:synthigy/plug]
  :doc "Audit history in audit.duckdb"
  :setup   duckdb/setup
  :start   duckdb/start
  :stop    duckdb/stop
  :cleanup duckdb/cleanup})
