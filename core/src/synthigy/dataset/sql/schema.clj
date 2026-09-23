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
