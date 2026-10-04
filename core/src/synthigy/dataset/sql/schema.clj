;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.sql.schema
  "Deployed runtime-schema cache.")

(defonce ^:private _deployed-schema (atom nil))

(defn set-deployed-schema!
  "Replace the cached schema. Pass nil to clear."
  [schema]
  (reset! _deployed-schema schema))

(defn deployed-schema
  "Returns the currently cached runtime schema, or nil."
  []
  @_deployed-schema)

(defn deployed-schema-entity
  "Returns the compiled schema entry for entity-id; throws if absent."
  [entity-id]
  (if-some [entity (get @_deployed-schema entity-id)]
    entity
    (throw
     (ex-info
      (str "Entity " entity-id " not found in deployed schema")
      {:type ::entity-not-found
       :entity entity-id
       :available (keys @_deployed-schema)}))))
