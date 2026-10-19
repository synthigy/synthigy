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
