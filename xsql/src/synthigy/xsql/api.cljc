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

(ns synthigy.xsql.api
  "Public API for the XSQL query-DSL package."
  (:refer-clojure :exclude [compile])
  (:require [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]
            [synthigy.xsql.compile :as compile-impl]
            [synthigy.xsql.lint :as lint-impl]
            [synthigy.xsql.complete :as complete-impl]))

(defn parse
  "Parse an XSQL source string into a :query AST root."
  [source]
  (parser/parse source))

(defn root-entity
  "Return the root entity name of an XSQL source, or nil."
  [source]
  (some-> (parser/parse source) :root-entity :text))

(defn ast->sexpr
  "Compact S-expression dump of an AST."
  [ast-node]
  (ast/ast->sexpr ast-node))

(defn ast->json
  "JSON-shaped dump of an AST."
  [ast-node]
  (ast/ast->json ast-node))

(defn line-col
  "Map a byte offset to 1-based `{:line :col}`."
  [^String source offset]
  (let [n (count source)]
    (loop [i 0 line 1 line-start 0]
      (if (or (>= i offset) (>= i n))
        {:line line :col (- offset line-start -1)}
        (if (= \newline (.charAt source i))
          (recur (inc i) (inc line) (inc i))
          (recur (inc i) line line-start))))))

(defn compile
  "Compile XSQL source to wire JSON `{:selections {…} :args {…}?}`."
  ([source]                (compile-impl/compile source))
  ([source op]             (compile-impl/compile source op))
  ([source op params]      (compile-impl/compile source op params)))

(defn compile-ast
  "Compile a pre-parsed AST to wire JSON."
  [ast op params]
  (compile-impl/compile-ast ast op params))

(defn lint
  "Schema-aware linter; returns a vector of diagnostics."
  ([source]
   (lint-impl/lint source))
  ([source schema root-entity]
   (lint-impl/lint source schema root-entity))
  ([source schema root-entity op]
   (lint-impl/lint source schema root-entity op)))

(defn complete
  "Context-aware autocompletion for an XSQL source at an offset."
  [opts]
  (complete-impl/complete opts))
