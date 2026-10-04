;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.iam.audit
  "Stub audit enhancement — no-op for compilation without a database backend.

   Extends `Object`, not a concrete backend record: the `:stub` alias has
   no backend on the classpath, so `synthigy.db.SQLite` is both the wrong
   type to name here and unreachable.

   `augment-schema` must return `{}` and `audit` must return `data`
   unchanged — both sit mid-pipeline (`runtime/model->schema`,
   `enhance/apply-audit`) and a nil return silently drops the schema
   fragment / the whole mutation payload."
  (:require
   [synthigy.dataset.enhance :as enhance]))

(extend-protocol enhance/AuditEnhancement
  Object
  (transform-audit [_ _ _] nil)
  (augment-schema  [_ _] {})
  (audit           [_ _ data _] data))
