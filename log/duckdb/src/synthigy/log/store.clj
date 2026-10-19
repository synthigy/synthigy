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
