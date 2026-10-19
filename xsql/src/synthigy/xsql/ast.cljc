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

(ns synthigy.xsql.ast
  "AST data shape and dump utilities for the XSQL parser."
  (:require [clojure.string :as str]))

;; ── Constructors ──────────────────────────────────────────────────────────

(defn node
  [tag span children]
  {:node tag :span span :children (vec children)})

(defn leaf
  ([tag span]      {:node tag :span span})
  ([tag span text] {:node tag :span span :text text}))

(defn leaf? [n] (and (map? n) (contains? n :text)))
(defn container? [n] (and (map? n) (contains? n :children)))

;; ── Walker ────────────────────────────────────────────────────────────────

(defn walk
  "Pre-order depth-first walk calling `(f node)` for side effects."
  [n f]
  (f n)
  (when (container? n)
    (doseq [c (:children n)]
      (walk c f))))

(defn find-children
  [n tag]
  (filterv #(= tag (:node %)) (:children n)))

(defn find-child
  [n tag]
  (first (find-children n tag)))

;; ── Tag → CamelCase ───────────────────────────────────────────────────────

(defn tag->camel
  "Convert a kebab-case keyword tag to its CamelCase string form."
  [kw]
  (->> (str/split (name kw) #"-")
       (map str/capitalize)
       (apply str)))

;; ── S-expression dump (treeToSexpr equivalent) ────────────────────────────

(defn sexpr-leaf-string [n]
  (let [name- (tag->camel (:node n))]
    (case (:node n)
      (:identifier :arrow :dash) (str "(" name- " " (pr-str (:text n)) ")")
      (str "(" name- ")"))))

(defn ast->sexpr
  "Compact S-expression dump of an AST."
  [n]
  (if (leaf? n)
    (sexpr-leaf-string n)
    (let [name- (tag->camel (:node n))
          parts (map ast->sexpr (:children n))]
      (str "(" name-
           (when (seq parts) (str " " (str/join " " parts)))
           ")"))))

;; ── Vector-shape dump (parse.test.js shape() equivalent) ──────────────────

(defn ast->shape [n]
  (let [name- (tag->camel (:node n))]
    (if (container? n)
      (into [name-] (map ast->shape (:children n)))
      [name-])))

;; ── JSON dump (treeToJson equivalent) ─────────────────────────────────────

(defn ast->json [n]
  (let [base {:name (tag->camel (:node n))}]
    (cond
      (container? n)
      (assoc base :children (mapv ast->json (:children n)))

      (and (leaf? n)
           (#{:identifier :arrow :dash} (:node n)))
      (assoc base :text (:text n))

      :else
      base)))
