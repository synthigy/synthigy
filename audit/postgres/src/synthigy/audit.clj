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
  "Audit dimension: history tables in the Postgres data database."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.audit.postgres :as postgres]
   [synthigy.db :as db]))

(defn open-provider
  "An isolated provider instance over the bound database; the lifecycle module binds its own."
  [_]
  (postgres/apply-schema! (:datasource db/*db*))
  (postgres/->PostgresAudit))

(defn close-provider! [_])

(lifecycle/register-module!
 :synthigy/audit
 {:depends-on [:synthigy/plug]
  :doc "Audit history in __audit_* tables beside the data, written in the drain transaction"
  :setup   postgres/setup
  :start   postgres/start
  :stop    postgres/stop
  :cleanup postgres/cleanup})
