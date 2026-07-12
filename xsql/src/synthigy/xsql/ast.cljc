(ns synthigy.xsql.ast
  "AST data shape and dump utilities for the XSQL parser.

   Nodes are plain maps. Container nodes carry `:children`; leaf
   tokens carry `:text`:

     {:node :scalar          ; container
      :span [from to]
      :children [<node>...]}

     {:node :identifier      ; leaf
      :span [from to]
      :text \"name\"}

   Naming convention: internal tags are kebab-case keywords
   (`:scalar-or-expr`). The dump formats render them as CamelCase
   strings (`\"ScalarOrExpr\"`) to match the lezer-style names used in
   the existing JS test fixtures."
  (:require [clojure.string :as str]))

;; ── Constructors ──────────────────────────────────────────────────────────

(defn node
  "Container node with the given tag, span, and children vector."
  [tag span children]
  {:node tag :span span :children (vec children)})

(defn leaf
  "Leaf node carrying text from the source."
  ([tag span]      {:node tag :span span})
  ([tag span text] {:node tag :span span :text text}))

(defn leaf? [n] (and (map? n) (contains? n :text)))
(defn container? [n] (and (map? n) (contains? n :children)))

;; ── Walker ────────────────────────────────────────────────────────────────

(defn walk
  "Pre-order depth-first walk. Calls `(f node)` on every node, then
   recurses into children. Return value of `f` is ignored."
  [n f]
  (f n)
  (when (container? n)
    (doseq [c (:children n)]
      (walk c f))))

(defn find-children
  "Return the children of `n` whose tag is `tag`."
  [n tag]
  (filterv #(= tag (:node %)) (:children n)))

(defn find-child
  "First child of `n` whose tag is `tag`, or nil."
  [n tag]
  (first (find-children n tag)))

;; ── Tag → CamelCase ───────────────────────────────────────────────────────

(defn tag->camel
  "Convert a kebab-case keyword tag to its CamelCase string form.
   `:scalar-or-expr` → \"ScalarOrExpr\"."
  [kw]
  (->> (str/split (name kw) #"-")
       (map str/capitalize)
       (apply str)))

;; ── S-expression dump (treeToSexpr equivalent) ────────────────────────────

(defn- sexpr-leaf-string [n]
  (let [name- (tag->camel (:node n))]
    (case (:node n)
      ;; Match JS treeToSexpr: only Identifier/Arrow/Dash show their text.
      (:identifier :arrow :dash) (str "(" name- " " (pr-str (:text n)) ")")
      (str "(" name- ")"))))

(defn ast->sexpr
  "Compact S-expression dump. Diff-friendly snapshot for tests.
   Mirrors the JS `treeToSexpr` output up to leaf-text rules."
  [n]
  (if (leaf? n)
    (sexpr-leaf-string n)
    (let [name- (tag->camel (:node n))
          parts (map ast->sexpr (:children n))]
      (str "(" name-
           (when (seq parts) (str " " (str/join " " parts)))
           ")"))))

;; ── Vector-shape dump (parse.test.js shape() equivalent) ──────────────────
;;
;; Each node becomes a vector starting with its CamelCase name, followed by
;; its children's shapes. This is what the JS parse tests assert on.

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
