(ns synthigy.xsql.lint
  "Schema-aware linter. Produces diagnostics
   `[{:severity :error :from int :to int :message string} …]`.

   Schema shape (Clojure-native, no JS marshalling):

     {:entities
      {\"User\" {:attributes {\"name\"   {:type \"string\"}
                              \"active\" {:type \"boolean\"}}
                 :relations  {\"roles\"  {:target \"Role\"
                                           :cardinality \"o2m\"}}}}}

   Attribute :type is one of: string, number, boolean, timestamp, enum.
   Unknown types are tolerated — operator rules fall through.

   Tier 1 rules (port of lint.js):
   - Surface parser :error nodes as syntax errors (with coalescing)
   - Unknown attribute / relation against the schema
   - Demand-fields: relations must have at least one child
   - Operator-type mismatch (e.g. `ilike` on boolean)
   - Empty `in ()` / `not in ()` lists
   - Path predicate validity (terminate at attribute, no walk past scalar)"
  (:require [clojure.string :as str]
            [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]
            [synthigy.xsql.sql-params :as sql-params]))

(declare lint-statement lint-scalar lint-relation lint-count-block
         lint-agg-block lint-args-of-parens lint-or-expr lint-arg-predicate
         lint-pred-op-against-type)

(def ^:private ops-by-type
  {"string"    #{"_eq" "_neq" "_lt" "_le" "_gt" "_ge"
                 "_like" "_ilike" "_in" "_not_in"
                 "is_null" "is_not_null"}
   "number"    #{"_eq" "_neq" "_lt" "_le" "_gt" "_ge"
                 "_in" "_not_in" "is_null" "is_not_null"}
   "boolean"   #{"_eq" "_neq" "is_null" "is_not_null"}
   "timestamp" #{"_eq" "_neq" "_lt" "_le" "_gt" "_ge"
                 "_in" "_not_in" "is_null" "is_not_null"}
   "enum"      #{"_eq" "_neq" "_in" "_not_in" "is_null" "is_not_null"}})

(def ^:private structural-recovery
  #{:newline :statement :block :relation :scalar
    :count-block :count-child :agg-block :agg-relation :root-args})

(defn- error-diag [node msg]
  {:severity :error
   :from (first (:span node))
   :to   (second (:span node))
   :message msg})

(defn- warning-diag [node msg]
  {:severity :warning
   :from (first (:span node))
   :to   (second (:span node))
   :message msg})

(defn- entity-name-of
  "Best-effort reverse lookup of entity name from an entityDef.
   Used in error messages."
  [entity-def schema]
  (or (some (fn [[name def]] (when (= def entity-def) name))
            (:entities schema))
      "entity"))

;; ── Syntax error coalescing ─────────────────────────────────────────────

(defn- last-content-offset
  "Offset just past the last non-whitespace character. Errors after
   this are 'user hasn't finished typing yet' and get suppressed."
  [^String source]
  (let [n (count source)]
    (loop [i (dec n)]
      (cond
        (< i 0) 0
        (re-matches #"\s" (subs source i (inc i))) (recur (dec i))
        :else (inc i)))))

(defn- slice-nearby [source from to max]
  (let [n (count source)
        end (min (if (> to from) to (+ from max)) n)
        nl (str/index-of source "\n" from)
        end (if (and nl (< nl end)) nl end)
        out (str/trim (subs source from end))]
    (when (pos? (count out)) out)))

(defn- describe-syntax-error
  "Heuristic message for a parser :error node, mirroring lint.js."
  [err-node ^String source parent]
  (let [from (first (:span err-node))
        to   (second (:span err-node))
        snippet (slice-nearby source from to 24)
        leading (when (< from (count source)) (subs source from (inc from)))]
    (cond
      (= "(" leading)
      "Scalars take inline predicates, not parens. Compound logic belongs in `_args (…)` or `-rel (…)`."

      (= :parens (:node parent))
      "Missing `)`"

      (= :block (:node parent))
      "Empty indented block — add a child or dedent."

      :else
      (if snippet
        (str "Syntax error near `" snippet "`")
        "Syntax error"))))

(defn- collect-syntax-errors
  "Walk the AST, emit one diagnostic per error-run. Mirrors the
   coalescing behavior in lint.js: only the first :error in a run
   is reported; suppression resets when we hit a structural-recovery
   node."
  [ast ^String source]
  (let [last-content (last-content-offset source)
        diags (volatile! [])
        suppressing? (volatile! false)]
    (letfn [(walk [n parent]
              (cond
                (= :error (:node n))
                (when (and (not @suppressing?)
                           (< (first (:span n)) last-content))
                  (vswap! diags conj
                          (assoc (error-diag n
                                             (or (:message n)
                                                 (describe-syntax-error n source parent)))
                                 :severity :error))
                  (vreset! suppressing? true))

                (structural-recovery (:node n))
                (do (vreset! suppressing? false)
                    (when (ast/container? n)
                      (run! #(walk % n) (:children n))))

                (ast/container? n)
                (run! #(walk % n) (:children n))))]
      (walk ast nil))
    @diags))

;; ── Statement dispatch ───────────────────────────────────────────────────

(defn- lint-statement [stmt-node entity-def ctx]
  (let [inner (first (:children stmt-node))]
    (case (:node inner)
      :scalar      (lint-scalar inner entity-def ctx)
      :relation    (lint-relation inner entity-def ctx)
      :count-block (lint-count-block inner entity-def ctx)
      :agg-block   (lint-agg-block inner entity-def ctx)
      nil)))

;; ── Scalar ──────────────────────────────────────────────────────────────

(defn- get-bare-identifiers
  "Return :identifier children of `parent` that are NOT inside a
   nested :alias, :join-marker, or :pred-op."
  [parent]
  (filterv #(= :identifier (:node %)) (:children parent)))

(defn- lint-scalar [scalar-node entity-def ctx]
  (let [ids (get-bare-identifiers scalar-node)]
    (when (seq ids)
      (let [field-id (first ids)
            field-name (:text field-id)
            attr-def (get-in entity-def [:attributes field-name])
            rel-def  (get-in entity-def [:relations field-name])
            ;; Detect a misplaced `:` after a valid field identifier —
            ;; user almost certainly meant a relation alias and got the
            ;; order wrong. Common form: `name: -roles` (intended
            ;; `-name:roles`) or `display: name` (scalar aliases removed).
            colon-err (some #(and (= :error (:node %))
                                  (= ":" (:text %)) %)
                            (:children scalar-node))]
        (cond
          colon-err
          (vswap! (:diags ctx) conj
                  (error-diag colon-err
                              (str "Unexpected `:` after '" field-name "'. "
                                   "For relation aliases use `-" field-name ":<relation>`. "
                                   "Scalar aliases are not supported.")))

          attr-def
          (when-let [pred-op (ast/find-child scalar-node :pred-op)]
            (lint-pred-op-against-type pred-op attr-def field-name ctx))

          rel-def
          (vswap! (:diags ctx) conj
                  (error-diag field-id
                              (str "'" field-name
                                   "' is a relation; prefix with '-' or '->' to select it")))

          :else
          (vswap! (:diags ctx) conj
                  (error-diag field-id
                              (str "Unknown attribute '" field-name
                                   "' on " (entity-name-of entity-def (:schema ctx))))))))))

;; ── Relation ────────────────────────────────────────────────────────────

(defn- has-statement-child? [block]
  (boolean (some #(= :statement (:node %)) (:children block))))

(defn- lint-relation [rel-node entity-def ctx]
  (let [ids (get-bare-identifiers rel-node)]
    (when (seq ids)
      (let [rel-id (first ids)
            rel-name (:text rel-id)
            rel-def (get-in entity-def [:relations rel-name])
            attr-def (get-in entity-def [:attributes rel-name])]
        (cond
          rel-def
          (let [target-def (get-in (:schema ctx) [:entities (:target rel-def)])
                parens (ast/find-child rel-node :parens)
                block  (ast/find-child rel-node :block)]
            (when (and parens target-def)
              (lint-args-of-parens parens target-def ctx))
            (cond
              (or (nil? block) (not (has-statement-child? block)))
              (vswap! (:diags ctx) conj
                      (error-diag rel-id
                                  (str "Relation '" rel-name
                                       "' must have at least one child selection")))

              target-def
              (doseq [c (:children block)
                      :when (= :statement (:node c))]
                (lint-statement c target-def ctx))))

          attr-def
          (vswap! (:diags ctx) conj
                  (error-diag rel-id
                              (str "'" rel-name
                                   "' is an attribute; drop the '-' prefix to select it")))

          :else
          (vswap! (:diags ctx) conj
                  (error-diag rel-id
                              (str "Unknown relation '" rel-name
                                   "' on " (entity-name-of entity-def (:schema ctx))))))))))

;; ── _count ──────────────────────────────────────────────────────────────

(defn- lint-count-block [count-node entity-def ctx]
  (doseq [child (ast/find-children count-node :count-child)]
    (let [ids (get-bare-identifiers child)]
      (when (seq ids)
        (let [rel-id (first ids)
              rel-name (:text rel-id)
              rel-def (get-in entity-def [:relations rel-name])]
          (if rel-def
            (let [target-def (get-in (:schema ctx) [:entities (:target rel-def)])
                  parens (ast/find-child child :parens)]
              (when (and parens target-def)
                (lint-args-of-parens parens target-def ctx)))
            (vswap! (:diags ctx) conj
                    (error-diag rel-id
                                (str "Unknown relation '" rel-name
                                     "' on " (entity-name-of entity-def (:schema ctx)))))))))))

;; ── _agg ────────────────────────────────────────────────────────────────

(defn- lint-agg-block [agg-node entity-def ctx]
  (doseq [agg-rel (ast/find-children agg-node :agg-relation)]
    (let [ids (get-bare-identifiers agg-rel)]
      (when (seq ids)
        (let [rel-id (first ids)
              rel-name (:text rel-id)
              rel-def (get-in entity-def [:relations rel-name])]
          (if-not rel-def
            (vswap! (:diags ctx) conj
                    (error-diag rel-id
                                (str "Unknown relation '" rel-name
                                     "' on " (entity-name-of entity-def (:schema ctx)))))
            (when-let [target-def (get-in (:schema ctx) [:entities (:target rel-def)])]
              (doseq [agg-attr (ast/find-children agg-rel :agg-attr)]
                (let [attr-id (first (filter #(= :identifier (:node %))
                                             (:children agg-attr)))
                      attr-name (:text attr-id)
                      attr-def (get-in target-def [:attributes attr-name])]
                  (cond
                    (nil? attr-def)
                    (vswap! (:diags ctx) conj
                            (error-diag attr-id
                                        (str "Unknown attribute '" attr-name
                                             "' on " (:target rel-def))))

                    (not= "number" (:type attr-def))
                    (vswap! (:diags ctx) conj
                            (error-diag attr-id
                                        (str "_agg requires a numeric attribute; '"
                                             attr-name "' is " (:type attr-def))))))))))))))

;; ── Args inside parens ──────────────────────────────────────────────────

(defn- lint-args-of-parens [parens-node entity-def ctx]
  (when-let [list (ast/find-child parens-node :arg-list)]
    (doseq [stmt (ast/find-children list :arg-stmt)]
      (let [inner (first (:children stmt))]
        (when (= :or-expr (:node inner))
          (lint-or-expr inner entity-def ctx))))))

(defn- lint-or-expr [or-node entity-def ctx]
  (doseq [a (ast/find-children or-node :and-expr)]
    (doseq [p (ast/find-children a :primary-expr)]
      (let [inner (first (:children p))]
        (case (:node inner)
          :arg-predicate (lint-arg-predicate inner entity-def ctx)
          :grouped-expr  (when-let [nested (ast/find-child inner :or-expr)]
                           (lint-or-expr nested entity-def ctx))
          nil)))))

(defn- lint-arg-predicate [pred-node entity-def ctx]
  (let [path (ast/find-child pred-node :path)
        pred-op (ast/find-child pred-node :pred-op)]
    (when path
      (let [segs (filterv #(= :identifier (:node %)) (:children path))
            schema (:schema ctx)]
        (loop [i 0
               current entity-def
               last-attr nil
               last-name nil]
          (if (>= i (count segs))
            (when (and pred-op last-attr)
              (lint-pred-op-against-type pred-op last-attr last-name ctx))
            (let [seg (nth segs i)
                  seg-name (:text seg)
                  is-last? (= i (dec (count segs)))
                  attr (get-in current [:attributes seg-name])
                  rel  (get-in current [:relations seg-name])]
              (cond
                attr
                (if is-last?
                  (recur (inc i) current attr seg-name)
                  (vswap! (:diags ctx) conj
                          (error-diag seg
                                      (str "'" seg-name
                                           "' is an attribute — path cannot continue past a scalar"))))

                rel
                (let [target (get-in schema [:entities (:target rel)])]
                  (cond
                    (nil? target) nil
                    is-last? (vswap! (:diags ctx) conj
                                     (error-diag seg
                                                 (str "'" seg-name
                                                      "' is a relation — path must terminate at an attribute")))
                    :else (recur (inc i) target last-attr last-name)))

                :else
                (vswap! (:diags ctx) conj
                        (error-diag seg
                                    (str "Unknown attribute/relation '" seg-name
                                         "' on " (entity-name-of current schema))))))))))))

;; ── Operator / type compatibility ───────────────────────────────────────

(defn- classify-pred-op
  "Return [kind op-key] for a :pred-op node, or [nil nil] if the node
   is incomplete (e.g. user typed just `is` or `not` mid-edit). Strict
   sequence matching avoids spurious lint errors during typing."
  [pred-op-node]
  (let [children (:children pred-op-node)
        first-text (-> children first :text)
        second-text (some-> children second :text)
        texts (mapv :text children)]
    (cond
      (= ["is" "null"] texts)              ["is_null" "is_null"]
      (= ["is" "not" "null"] texts)        ["is_not_null" "is_not_null"]
      (and (= "not" first-text)
           (= "in" second-text))           ["not_in" "_not_in"]
      (= "in" first-text)                  ["in" "_in"]
      (= "ilike" first-text)               ["ilike" "_ilike"]
      (= "like" first-text)                ["like" "_like"]

      :else
      (if-let [bin-op (ast/find-child pred-op-node :binary-op)]
        (let [tok (-> bin-op :children first :node)
              wire (case tok
                     :eq "_eq" :neq "_neq" :lt "_lt" :le "_le"
                     :gt "_gt" :ge "_ge" nil)]
          ["binop" wire])
        [nil nil]))))

(def ^:private op-key->human
  {"_eq" "=" "_neq" "!=" "_lt" "<" "_le" "<="
   "_gt" ">" "_ge" ">=" "_like" "like" "_ilike" "ilike"
   "_in" "in" "_not_in" "not in"
   "is_null" "is null" "is_not_null" "is not null"})

(defn- lint-pred-op-against-type
  "Check operator/type compatibility and empty-list cases."
  [pred-op-node attr-def field-name ctx]
  (let [[kind op-key] (classify-pred-op pred-op-node)]
    (when op-key
      ;; Empty list
      (when (#{"in" "not_in"} kind)
        (when-let [list-node (ast/find-child pred-op-node :list-literal)]
          (when (zero? (count (ast/find-children list-node :value)))
            (vswap! (:diags ctx) conj
                    (error-diag list-node
                                (str "Empty list for '" field-name "' — "
                                     (str/replace kind "_" " ") " () is invalid"))))))
      ;; Type compatibility
      (when-let [allowed (ops-by-type (:type attr-def))]
        (when-not (allowed op-key)
          (vswap! (:diags ctx) conj
                  (error-diag pred-op-node
                              (str "Operator '" (op-key->human op-key op-key)
                                   "' is not valid on " (:type attr-def)
                                   " attribute '" field-name "'"))))))))

;; ── Op-specific rules (multimethod) ─────────────────────────────────────
;;
;; `lint` always runs syntax + schema-aware rules (the universal
;; baseline). Per-op rules layer on top via this multimethod, dispatched
;; on the wire op string. New ops with their own root-scope quirks
;; register a `defmethod` here; nothing else needs to change.
;;
;; Method contract: takes `[op tree ctx]`, mutates `(:diags ctx)`. Return
;; value is ignored.

(defmulti op-lint-rules
  "Apply op-specific lint rules. Dispatches on the wire op string;
   `:default` is a no-op for ops that don't impose extra constraints."
  (fn [op _tree _ctx] (or op :default)))

(defmethod op-lint-rules :default [_ _ _] nil)

(defn- lint-get-root-scalar
  "Get-mode root-scalar check. A root scalar is either a bare projection
   (no predicate) or `field = value` on a unique-constrained attribute.
   Other operators (`!=`, `<`, `like`, `in`, `is null`, …) are errors at
   the root. Compound parens forms (`field(… or …)`) are errors unless
   they reduce to a single `=`. When the schema carries `:unique`
   information for at least one attribute, non-unique attrs with a `=`
   predicate at the root are also rejected."
  [scalar-node entity-def ctx]
  (let [pred-op  (ast/find-child scalar-node :pred-op)
        filter   (ast/find-child scalar-node :scalar-filter)
        field-id (some (fn [c] (when (= :identifier (:node c)) c))
                       (:children scalar-node))
        field-name (or (:text field-id) "field")
        attr-def (when (and entity-def field-name)
                   (get-in entity-def [:attributes field-name]))]
    (cond
      pred-op
      (let [[_ op-key] (classify-pred-op pred-op)]
        (cond
          (and op-key (not= "_eq" op-key))
          (vswap! (:diags ctx) conj
                  (error-diag pred-op
                              (str "`get` only accepts `=` at the root for identity. `"
                                   (op-key->human op-key op-key)
                                   "` on '" field-name "' is not allowed.")))

          ;; `=` with known attr but not unique-constrained — only flag
          ;; when the schema actually carries `:unique` info (i.e. at
          ;; least one attr on the entity has it). Avoids false-positives
          ;; in environments where uniqueness isn't propagated yet.
          (and (= "_eq" op-key)
               attr-def
               (not (:unique attr-def))
               (some (fn [[_ a]] (:unique a)) (:attributes entity-def)))
          (vswap! (:diags ctx) conj
                  (error-diag scalar-node
                              (str "`get` requires identity on a unique-constrained attribute. '"
                                   field-name "' is not unique.")))))

      filter
      (let [or-expr     (first (:children filter))
            and-exprs   (ast/find-children or-expr :scalar-and-expr)
            single-and? (= 1 (count and-exprs))
            prims       (when single-and? (ast/find-children (first and-exprs) :scalar-prim))
            single?     (and prims (= 1 (count prims)))
            sole-prim   (when single? (first prims))
            sole-op     (when sole-prim (ast/find-child sole-prim :pred-op))
            [_ ok]      (when sole-op (classify-pred-op sole-op))]
        (when-not (= "_eq" ok)
          (vswap! (:diags ctx) conj
                  (error-diag filter
                              (str "`get` only accepts `=` at the root. "
                                   "Compound or non-`=` predicates on '"
                                   field-name "' are not allowed."))))))))

(defn- attr-identity?
  "True if `field-name` `=`-matched (via `pred-op`) is a valid get identity:
   the attribute is unique (or the schema carries no unique info at all)."
  [field-name pred-op entity-def]
  (let [attr-def (when (and entity-def field-name)
                   (get-in entity-def [:attributes field-name]))]
    (when (and pred-op attr-def)
      (let [[_ op-key] (classify-pred-op pred-op)]
        (and (= "_eq" op-key)
             (or (:unique attr-def)
                 (not (some (fn [[_ a]] (:unique a)) (:attributes entity-def)))))))))

(defn- root-parens-has-identity?
  "True if the root parens (`Movie (xid = \"…\")`) carry a `=` predicate on a
   unique-constrained attribute — the root-args form of get identity."
  [tree entity-def]
  (boolean
    (when-let [parens (:root-parens tree)]
      (some
       (fn [n]
         (when (= :arg-predicate (:node n))
           (let [path     (ast/find-child n :path)
                 field-id (some #(when (= :identifier (:node %)) %) (:children path))]
             (attr-identity? (some-> field-id :text)
                             (ast/find-child n :pred-op)
                             entity-def))))
       (tree-seq :children :children parens)))))

(defn- root-statement-has-identity?
  "True if any root :statement carries a `=` predicate on a unique-
   constrained attribute. Used by the get-mode 'must have identity'
   check below."
  [tree entity-def]
  (boolean
    (some
      (fn [c]
        (when (= :statement (:node c))
          (let [inner (first (:children c))]
            (when (= :scalar (:node inner))
              (let [pred-op  (ast/find-child inner :pred-op)
                    field-id (some #(when (= :identifier (:node %)) %)
                                   (:children inner))
                    field-name (some-> field-id :text)
                    attr-def (when (and entity-def field-name)
                               (get-in entity-def [:attributes field-name]))]
                (when (and pred-op attr-def)
                  (let [[_ op-key] (classify-pred-op pred-op)]
                    (and (= "_eq" op-key)
                         (or (:unique attr-def)
                             ;; If the schema doesn't carry :unique info,
                             ;; accept any `=` — lint-get-root-scalar will
                             ;; only flag non-unique attrs when the schema
                             ;; does carry it for at least one attribute.
                             (not (some (fn [[_ a]] (:unique a))
                                        (:attributes entity-def))))))))))))
      (:children tree))))

(defmethod op-lint-rules "get"
  [_ tree ctx]
  (let [entity-def (:root-entity-def ctx)]
    (doseq [c (:children tree)]
      (case (:node c)
        :root-args
        (vswap! (:diags ctx) conj
                (error-diag c
                            "`_args (…)` is not allowed for `get`. Express identity inline as root-level scalar predicates (e.g. `xid = \"…\"`)."))

        :statement
        (let [inner (first (:children c))]
          (when (= :scalar (:node inner))
            (lint-get-root-scalar inner entity-def ctx)))

        nil))
    ;; `get` is row-identity, not 'search and take first' (XSQL.md L484).
    ;; Require at least one root scalar with `=` on a unique-constrained
    ;; attribute. Without it the server falls through to first-by-_eid,
    ;; which makes invalid queries look successful.
    (when (and entity-def
               (seq (:children tree))
               (not (root-statement-has-identity? tree entity-def))
               (not (root-parens-has-identity? tree entity-def)))
      (vswap! (:diags ctx) conj
              (assoc (error-diag tree
                                 "`get` requires identity. Add at least one root predicate like `xid = \"…\"` on a unique-constrained attribute.")
                     :from 0
                     :to   (max 1 (count (:source ctx ""))))))))

;; ── Named-parameter validation (schema-independent) ────────────────────
;;
;; Bare `?` and `?N` positional placeholders are rejected at the
;; tokenizer level (emitted as :error tokens; `collect-syntax-errors`
;; surfaces them). This pass catches the cases the parser swallowed
;; successfully but still need flagging — currently just the unknown
;; type token. Walks every `:param-ref` leaf in the AST.

(defn- collect-param-ref-errors
  [ast]
  (let [diags (volatile! [])]
    (letfn [(walk [n]
              (when (= :param-ref (:node n))
                (let [raw (:param-type-raw n)]
                  (when (and raw
                             (not (contains? sql-params/type-aliases
                                             (str/lower-case raw))))
                    (vswap! diags conj
                            (error-diag n
                                        (str "Unknown parameter type `:" raw
                                             "`. Expected one of: "
                                             (str/join ", "
                                                       (sort (keys sql-params/type-aliases)))))))))
              (when (ast/container? n)
                (run! walk (:children n))))]
      (walk ast))
    @diags))

;; ── Duplicate-sibling warnings ─────────────────────────────────────────
;;
;; XSQL has no scalar aliases (XSQL.md line 169) and bare (un-aliased)
;; relations share a response key, so duplicates at the same scope are
;; meaningless. We walk every scope (`:query` root + every `:block`) and
;; warn on the second-and-later occurrences. Aliased relations
;; (`-good:roles`) are skipped from the check — they legitimately repeat.

(defn- statement-bare-name
  "If `stmt-node` contains a bare scalar or bare relation/count-child,
   return `[:scalar|:relation identifier-node]`. Else nil. Aliased
   relations are skipped."
  [stmt-node]
  (let [inner (first (:children stmt-node))]
    (cond
      (= :scalar (:node inner))
      (when-let [id (some #(when (= :identifier (:node %)) %)
                          (:children inner))]
        [:scalar id])

      (#{:relation :count-child} (:node inner))
      (when-not (some #(= :alias (:node %)) (:children inner))
        (when-let [id (some #(when (= :identifier (:node %)) %)
                            (:children inner))]
          [:relation id])))))

(defn- lint-duplicate-siblings
  "Walk every scope (`:query` root + every `:block` node) and emit a
   warning for each duplicate bare attribute or bare relation. The
   first occurrence is unflagged; later ones get the warning."
  [tree ctx]
  (letfn [(walk [node]
            (when (ast/container? node)
              (when (#{:query :block} (:node node))
                (let [seen (volatile! {})]
                  (doseq [child (:children node)
                          :when (= :statement (:node child))
                          :let [hit (statement-bare-name child)]
                          :when hit]
                    (let [[kind id] hit
                          name (:text id)]
                      (if-let [first-pos (get @seen name)]
                        (vswap! (:diags ctx) conj
                                (warning-diag
                                  id
                                  (str "Duplicate "
                                       (clojure.core/name kind)
                                       " '" name "' at this scope "
                                       "(first listed at offset "
                                       first-pos
                                       "). "
                                       (if (= kind :relation)
                                         (str "Add an alias (e.g. `-other:" name "`) to project it again under a different key.")
                                         "XSQL has no scalar aliases; remove the duplicate."))))
                        (vswap! seen assoc name (first (:span id))))))))
              (run! walk (:children node))))]
    (walk tree)))

;; ── Public entry ────────────────────────────────────────────────────────

(defn lint
  "Lint an XSQL source against a schema and root entity.
   Returns a vector of diagnostic maps.

   `op` is the wire op string (\"search\" / \"get\" / …). When `op`
   is `\"get\"`, additional root-scope rules apply: no `_args (…)`
   block, only `=` predicates at the root."
  ([source]
   (lint source nil nil nil))
  ([source schema root-entity]
   (lint source schema root-entity nil))
  ([source schema root-entity op]
   (let [tree     (parser/parse source)
         ;; Rooted XSQL is self-describing: prefer the entity at the query
         ;; root over the (legacy) external arg. Falls back to the arg for
         ;; a bodyless source.
         root     (or (some-> (:root-entity tree) :text) root-entity)
         diags    (volatile! (vec
                              (concat (collect-syntax-errors tree source)
                                      (collect-param-ref-errors tree))))
         root-def (when (and schema root)
                    (get-in schema [:entities root]))
         ctx      {:source source
                   :schema schema
                   :root-entity-def root-def
                   :diags diags}]
     (when root-def
       (doseq [c (:children tree)]
         (case (:node c)
           :root-args
           (let [arg-list (or (some-> (ast/find-child c :parens)
                                      (ast/find-child :arg-list))
                              (ast/find-child c :arg-list))]
             (when arg-list
               (doseq [stmt (ast/find-children arg-list :arg-stmt)]
                 (let [inner (first (:children stmt))]
                   (when (= :or-expr (:node inner))
                     (lint-or-expr inner root-def ctx))))))

           :statement
           (lint-statement c root-def ctx)

           nil)))
     ;; Schema-independent rule — runs even without root-def because
     ;; duplicate detection only needs identifier text and tree shape.
     (lint-duplicate-siblings tree ctx)
     (op-lint-rules op tree ctx)
     @diags)))
