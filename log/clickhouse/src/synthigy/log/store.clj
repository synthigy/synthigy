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
  "Log-store dimension: logs and traffic in ClickHouse."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.clickhouse :as ch]
   [synthigy.log.store.clickhouse :as clickhouse]))

(defn open-store
  "An isolated store instance in a throwaway database; the lifecycle module binds its own."
  [config]
  (clickhouse/open-store (assoc config :database (str "synthigy_isolated_" (System/nanoTime)))))

(defn close-store! [{:keys [client config] :as s}]
  (clickhouse/close-store! s)
  (ch/exec-ddl! client config (str "DROP DATABASE IF EXISTS " (:database config))))

(lifecycle/register-module!
 :synthigy.log/store
 {:depends-on [:synthigy/log]
  :doc "Logs + traffic in ClickHouse (logs / traffic_minutes)"
  :setup   clickhouse/setup
  :start   clickhouse/start
  :stop    clickhouse/stop
  :cleanup clickhouse/cleanup})
