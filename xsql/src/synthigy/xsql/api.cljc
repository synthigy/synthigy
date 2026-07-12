(ns synthigy.xsql.api
  "Public API for the XSQL query-DSL package.

   This is the only namespace external consumers should import.
   Internal modules (`tokens`, `parser`, `ast`, `compile`, `lint`,
   `complete`) are subject to refactoring without notice.

   Output shapes match the JS `@synthigy/query-dsl` package byte-for-byte
   (after EDN ↔ JSON round-trip), so the CM6 adapter in
   `query_dsl_cm.cljs` can swap in transparently."
  (:refer-clojure :exclude [compile])
  (:require [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]
            [synthigy.xsql.compile :as compile-impl]
            [synthigy.xsql.lint :as lint-impl]
            [synthigy.xsql.complete :as complete-impl]))

(defn parse
  "Parse an XSQL source string. Returns the :query AST root.
   Errors are embedded as :error nodes — partial parses still
   produce a usable tree."
  [source]
  (parser/parse source))

(defn root-entity
  "The root entity name (string) of a rooted XSQL source, or nil when the
   source has no leading entity. Cheap: parses only to read the root node."
  [source]
  (some-> (parser/parse source) :root-entity :text))

(defn ast->sexpr
  "Compact S-expression dump of an AST. Useful for snapshot tests
   and debugging."
  [ast-node]
  (ast/ast->sexpr ast-node))

(defn ast->json
  "JSON-shaped dump of an AST: `{:name :children? :text?}`. Mirrors
   the JS `treeToJson` output."
  [ast-node]
  (ast/ast->json ast-node))

(defn line-col
  "Map a byte offset to 1-based `{:line :col}` for error messages."
  [^String source offset]
  (let [n (count source)]
    (loop [i 0 line 1 line-start 0]
      (if (or (>= i offset) (>= i n))
        {:line line :col (- offset line-start -1)}
        (if (= \newline (.charAt source i))
          (recur (inc i) (inc line) (inc i))
          (recur (inc i) line line-start))))))

(defn compile
  "Compile XSQL source to wire JSON `{:selections {…} :args {…}?}`.
   Output matches the existing JS `compile()` byte-for-byte (modulo the
   named-parameter resolution feature).

   `op` is the wire op string (\"search\" / \"get\" / …). For `get`,
   root-level scalar predicates are lifted into root args (see
   XSQL.md §`get`).

   `params` is an optional `{name value}` map for resolving `?name:type[]`
   placeholders. Missing or mistyped values throw `ex-info` carrying
   `:code` \"PARAM_MISSING\" or \"PARAM_TYPE_MISMATCH\"."
  ([source]                (compile-impl/compile source))
  ([source op]             (compile-impl/compile source op))
  ([source op params]      (compile-impl/compile source op params)))

(defn lint
  "Schema-aware linter. Returns a vector of diagnostics:
   `[{:severity :error :from int :to int :message string} …]`.

   When `schema` or `root-entity` is nil, only syntax errors are
   reported — schema-driven rules stay silent.

   `op` is the wire op string. When `op` is `\"get\"`, additional
   root-scope rules apply: `_args (…)` is rejected and only `=` is
   allowed at the root."
  ([source]
   (lint-impl/lint source))
  ([source schema root-entity]
   (lint-impl/lint source schema root-entity))
  ([source schema root-entity op]
   (lint-impl/lint source schema root-entity op)))

(defn complete
  "Context-aware autocompletion. Takes a map with keys:
   `:source` `:offset` `:schema` `:root-entity` `:op` and returns
   `{:from int :to int :options [{:label :type :detail? :info? :snippet?}]}`.

   `:op` is the wire op string (\"search\" / \"get\" / …); when set,
   op-specific filters apply (e.g. `get` hides `_args` at root and
   tags unique-constrained attrs as identity targets)."
  [opts]
  (complete-impl/complete opts))
