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
  "Audit dimension: history tables in ClickHouse."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.audit.clickhouse :as clickhouse]))

(defn open-provider
  "An isolated provider instance; the lifecycle module binds its own."
  [config]
  (clickhouse/open-provider config))

(defn close-provider! [_])

(lifecycle/register-module!
 :synthigy/audit
 {:depends-on [:synthigy/plug]
  :doc "Audit history in ClickHouse (audit_entity / audit_relation)"
  :setup   clickhouse/setup
  :start   clickhouse/start
  :stop    clickhouse/stop
  :cleanup clickhouse/cleanup})
