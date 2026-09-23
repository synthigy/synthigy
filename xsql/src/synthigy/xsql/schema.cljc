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

(ns synthigy.xsql.schema
  "Adapt a committed `schema.json` (the server's `GET /schema` projection)
   into the shape `synthigy.xsql.lint`/`synthigy.xsql.complete` consume.
   Pure data transform, no I/O."
  (:require [clojure.string :as str]))

(defn normalize-type
  "ERD attribute type to the linter's five-op vocabulary. Unknown types
   pass through unchanged."
  [t]
  (let [s (some-> t str/lower-case)]
    (cond
      (nil? s)                                                       "string"
      (re-find #"bool" s)                                            "boolean"
      (re-find #"^(int|long|float|double|decimal|numeric|number)" s) "number"
      (re-find #"(timestamp|datetime|date|time)" s)                  "timestamp"
      (re-find #"enum" s)                                            "enum"
      (re-find #"(string|text|varchar|char)" s)                      "string"
      :else                                                          s)))

(defn wire->lint
  "Adapt a parsed, string-keyed `schema.json` into `{:entities {name
   {:attributes {a {:type :unique}} :relations {r {:target :cardinality}}}}}`."
  [wire]
  {:entities
   (into {}
         (map (fn [[ename edef]]
                (let [unique (into #{} (comp (filter #(= 1 (count %))) (map first))
                                   (get-in edef ["constraints" "unique"]))]
                  [ename
                   {:attributes
                    (into {} (map (fn [[a adef]]
                                    [a {:type   (normalize-type (get adef "type"))
                                        :unique (contains? unique a)}]))
                          (get edef "attributes"))
                    :relations
                    (into {} (map (fn [[r rdef]]
                                    [r {:target      (get rdef "to")
                                        :cardinality (get rdef "cardinality")}]))
                          (get edef "relations"))}])))
         (get wire "entities"))})
