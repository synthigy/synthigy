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
