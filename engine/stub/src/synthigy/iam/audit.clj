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
