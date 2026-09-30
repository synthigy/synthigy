;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

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
