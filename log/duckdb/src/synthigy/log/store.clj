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
  "Log-store dimension: logs and traffic in an embedded DuckDB file, capped by rows and age."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.log.store.duckdb :as duckdb]))

(defn open-store
  "An isolated store instance for `config`; the lifecycle module binds its own."
  [config]
  (duckdb/open-store config))

(defn close-store! [s]
  (duckdb/close-store! s))

(lifecycle/register-module!
 :synthigy.log/store
 {:depends-on [:synthigy/log]
  :doc "Logs + traffic in logs.duckdb, capped by rows and age"
  :setup   duckdb/setup
  :start   duckdb/start
  :stop    duckdb/stop
  :cleanup duckdb/cleanup})
