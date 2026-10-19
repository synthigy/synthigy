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
   {:attributes {a {:type :unique :enum}} :relations {r {:target :cardinality}}}}}`."
  [wire]
  {:entities
   (into {}
         (map (fn [[ename edef]]
                (let [unique (into #{} (comp (filter #(= 1 (count %))) (map first))
                                   (get-in edef ["constraints" "unique"]))]
                  [ename
                   {:attributes
                    (into {} (map (fn [[a adef]]
                                    [a (cond-> {:type   (normalize-type (get adef "type"))
                                                :unique (contains? unique a)}
                                         (get adef "enum") (assoc :enum (get adef "enum")))]))
                          (get edef "attributes"))
                    :relations
                    (into {} (map (fn [[r rdef]]
                                    [r {:target      (get rdef "to")
                                        :cardinality (get rdef "cardinality")}]))
                          (get edef "relations"))}])))
         (get wire "entities"))})
