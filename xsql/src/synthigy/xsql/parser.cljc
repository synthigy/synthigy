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

(ns synthigy.xsql.parser
  "Recursive-descent parser for the XSQL DSL: tokens (synthigy.xsql.tokens)
   to AST (synthigy.xsql.ast), mirroring sdk/query-dsl/grammar/query-dsl.grammar."
  (:require [synthigy.xsql.ast :as ast]
            [synthigy.xsql.tokens :as tok]))

;; ── Token cursor ─────────────────────────────────────────────────────────

(defn make-state [tokens] {:tokens (vec tokens) :pos 0})

(defn peek-tok
  ([st]   (get (:tokens st) (:pos st)))
  ([st k] (get (:tokens st) (+ (:pos st) k))))

(defn peek-type
  ([st]   (:type (peek-tok st)))
  ([st k] (:type (peek-tok st k))))

(defn peek-text [st] (:text (peek-tok st)))

(defn advance [st] (update st :pos inc))

(defn at-eof? [st] (= :eof (peek-type st)))

(defn id-text=?
  "True if current token is an :identifier whose text matches."
  [st text]
  (and (= :identifier (peek-type st))
       (= text (peek-text st))))

(defn id-text=at?
  "True if token at offset k is an :identifier whose text matches."
  [st k text]
  (and (= :identifier (peek-type st k))
       (= text (:text (peek-tok st k)))))

;; ── Errors ───────────────────────────────────────────────────────────────

(defn error-node [tok msg]
  (let [t (or tok {:from 0 :to 0 :text ""})]
    {:node :error
     :span [(:from t) (:to t)]
     :text (or (:text t) "")
     ;; Prefer the lexer's own diagnostic message when present — more
     ;; specific than the parser's generic fallback.
     :message (or (:message t) msg)}))

(defn skip-to-newline
  "Advance until :newline / :dedent / :eof; consume the :newline if found."
  [st]
  (loop [st st]
    (case (peek-type st)
      :newline       (advance st)
      (:dedent :eof) st
      (recur (advance st)))))

;; ── Token → leaf node ────────────────────────────────────────────────────

(defn tok->leaf [t]
  (let [tag (:type t)
        sp  [(:from t) (:to t)]]
    (case tag
      (:identifier :string :number :arrow :dash
       :eq :neq :lt :le :gt :ge)
      (ast/leaf tag sp (:text t))

      ;; Preserve the lexer's :message so callers (the linter) can
      ;; surface its specific cause instead of a generic fallback.
      :error
      (cond-> (ast/leaf tag sp (:text t))
        (:message t) (assoc :message (:message t)))

      ;; Param refs carry richer metadata so the compiler doesn't have
      ;; to re-scan the text.
      :param-ref
      (cond-> (assoc (ast/leaf tag sp (:text t))
                     :param-name     (:param-name t)
                     :param-type-raw (:param-type-raw t)
                     :array?         (:array? t))
        (:optional? t)       (assoc :optional? true)
        (:param-default t)   (assoc :param-default (:param-default t))
        (:param-type-args t) (assoc :param-type-args (:param-type-args t)))

      (:newline :blank-line :indent :dedent)
      (ast/leaf tag sp (:text t)))))

(defn consume-leaf [st]
  [(tok->leaf (peek-tok st)) (advance st)])

(defn expect
  "Consume token of given type; on mismatch emit :error and don't advance."
  [st expected-type msg]
  (if (= expected-type (peek-type st))
    (consume-leaf st)
    [(error-node (peek-tok st) (or msg (str "expected " (name expected-type))))
     st]))

(defn skip-silent
  "Advance past token of given type without adding to children."
  [st t]
  (if (= t (peek-type st))
    (advance st)
    st))

;; ── Span helpers ─────────────────────────────────────────────────────────

(defn end-of-children
  "Last child's :span end, or fallback if no children."
  [children fallback]
  (if (seq children)
    (or (second (:span (last children))) fallback)
    fallback))

(defn container
  "Build a container node spanning from `start-tok`'s :from to the
   last child's end (or :from when childless)."
  [tag start-tok children]
  (let [from (:from start-tok)
        to   (end-of-children children from)]
    (ast/node tag [from to] (vec children))))

;; ── Forward declarations ─────────────────────────────────────────────────

(declare parse-statement parse-scalar parse-relation parse-block
         parse-count-block parse-count-child parse-agg-block parse-agg-relation
         parse-agg-attr parse-agg-fn parse-parens parse-arg-list parse-arg-stmt
         parse-or-expr parse-and-expr parse-primary-expr parse-grouped-expr
         parse-arg-predicate parse-path parse-pred-op parse-binary-op
         parse-value parse-list-literal parse-meta-key parse-order-spec
         parse-scalar-filter parse-scalar-or-expr parse-scalar-and-expr
         parse-scalar-prim parse-alias parse-join-marker)

;; ── Predicates ───────────────────────────────────────────────────────────

(def ^:private binary-op-types #{:eq :neq :lt :le :gt :ge})

;; Bare SQL spellings are canonical; legacy `_`-forms still parse (lint
;; nudges). Position disambiguates bare forms — see docs.
(def ^:private meta-keys #{"_limit" "_offset" "_order_by" "_distinct" "_join" "_on"})
(def ^:private bare-meta-keys #{"limit" "offset" "distinct" "join" "on"})

(defn normalize-meta-kw
  "Canonical `_`-form for either spelling (`limit` and `_limit` → \"_limit\";
   `order` → \"_order_by\"). Public — the compiler normalizes meta-key AST
   nodes through this same table so the two can't drift."
  [text]
  (case text
    "limit"    "_limit"
    "offset"   "_offset"
    "order"    "_order_by"
    "distinct" "_distinct"
    "join"     "_join"
    "on"       "_on"
    text))

(defn pred-op-starter?
  "True if current token can start a PredOp (after a path / scalar id)."
  [st]
  (or (binary-op-types (peek-type st))
      (id-text=? st "in")
      (id-text=? st "not")
      (id-text=? st "like")
      (id-text=? st "ilike")
      (id-text=? st "is")))

(defn meta-key-start?
  "True when the current token begins a meta-key arg-stmt. Legacy
   `_`-forms are unconditional; bare forms need position: `order` only
   with a following `by`, the rest only when not followed by a
   predicate operator (`limit > 10` stays a predicate)."
  [st]
  (and (= :identifier (peek-type st))
       (let [t (peek-text st)]
         (or (meta-keys t)
             (and (= "order" t) (id-text=at? st 1 "by"))
             (and (bare-meta-keys t)
                  (not (pred-op-starter? (advance st))))))))

(defn looks-like-alias?
  "An alias prefix is :identifier directly followed by :colon."
  [st]
  (and (= :identifier (peek-type st))
       (= :colon      (peek-type st 1))))

;; ═══════════════════════════════════════════════════════════════════════
;;   PRODUCTIONS
;; ═══════════════════════════════════════════════════════════════════════

;; Query → RootEntity Newline Indent Body Dedent
;; Body  → (BlankLine | RootArgs | Statement)*
;;
;; Rooted form: the entity identifier lives on the :query node under
;; :root-entity, NOT as a child, so AST shape stays identical to the
;; body alone. `compile-query` reads it to emit :entity.

(defn parse-query-body
  "Parse the body of a rooted query: (BlankLine | Statement)* until
   :dedent / :eof. Returns `[children st]`."
  [st]
  (loop [st       st
         children []]
    (let [pos-before (:pos st)]
      (cond
        ;; nil peek = past EOF: an unterminated root paren can swallow
        ;; the rest of the buffer into its arg-list. Same exit as :eof.
        (or (nil? (peek-type st)) (#{:dedent :eof} (peek-type st)))
        [children st]

        (= :blank-line (peek-type st))
        (let [[bl st'] (consume-leaf st)]
          (recur st' (conj children bl)))

        (or (#{:dash :arrow} (peek-type st))
            (= :identifier   (peek-type st)))
        (let [[n st'] (parse-statement st)]
          (recur st' (conj children n)))

        :else
        ;; Progress watchdog: force-advance if skip-to-newline made no
        ;; progress (returned at :dedent/:eof) — an editor hang otherwise.
        (let [err (error-node (peek-tok st)
                              (str "unexpected token: " (name (or (peek-type st) :eof))))
              st' (skip-to-newline (advance st))]
          (if (= pos-before (:pos st'))
            [(conj children err) (advance st')]
            (recur st' (conj children err))))))))

(defn parse-query [st]
  (let [start-tok (peek-tok st)
        start     (:from start-tok 0)
        ;; Leading blank-lines / comments before the root entity.
        [lead-blanks st]
        (loop [st st acc []]
          (if (= :blank-line (peek-type st))
            (let [[bl st'] (consume-leaf st)]
              (recur st' (conj acc bl)))
            [acc st]))
        ;; Root entity: a single identifier at col-0.
        [root-entity st]
        (if (= :identifier (peek-type st))
          (consume-leaf st)
          [nil st])
        ;; Optional root args, same arg-list grammar as a relation header.
        [root-parens st]
        (if (and root-entity (= :lparen (peek-type st)))
          (parse-parens st)
          [nil st])
        st          (skip-silent st :newline)
        [pre-blanks st]
        (loop [st st acc []]
          (if (= :blank-line (peek-type st))
            (let [[bl st'] (consume-leaf st)]
              (recur st' (conj acc bl)))
            [acc st]))
        had-indent? (= :indent (peek-type st))
        st          (skip-silent st :indent)
        [body st]   (parse-query-body st)
        st          (if had-indent? (skip-silent st :dedent) st)
        children    (-> (vec lead-blanks) (into pre-blanks) (into body))
        end         (or (:from (peek-tok st)) start)
        query       (cond-> (ast/node :query [start end] children)
                      root-entity (assoc :root-entity root-entity)
                      root-parens (assoc :root-parens root-parens))]
    [query (skip-silent st :eof)]))

;; Statement → Scalar | Relation | CountBlock | AggBlock

(defn parse-statement [st]
  (let [start (peek-tok st)
        [inner st']
        (cond
          (id-text=? st "_count")            (parse-count-block st)
          (id-text=? st "_agg")              (parse-agg-block st)
          (#{:dash :arrow} (peek-type st))   (parse-relation st)
          (= :identifier (peek-type st))     (parse-scalar st)
          :else                              [(error-node start "expected statement")
                                              (advance st)])]
    [(container :statement start [inner]) st']))

;; Scalar → Identifier (PredOp | ScalarFilter)? Newline
;; Scalar aliases were dropped from the language (relation/_count/_agg
;; aliases remain) — see docs.

(defn parse-scalar [st]
  (let [start (peek-tok st)
        children []
        [field-id st] (expect st :identifier "expected field name")
        children (conj children field-id)
        [tail st]
        (cond
          (= :lparen (peek-type st)) (parse-scalar-filter st)
          ;; Inline predicates parse through the same or/and machinery as
          ;; the parens form; a single bare predicate unwraps to a plain
          ;; :pred-op child, a joiner chain becomes :scalar-filter — same
          ;; shape either way for compile/lint/complete.
          (pred-op-starter? st)
          (let [tok (peek-tok st)
                [expr st'] (parse-scalar-or-expr st)
                and-exprs (:children expr)
                single-pred (when (= 1 (count and-exprs))
                              (let [prims (:children (first and-exprs))]
                                (when (= 1 (count prims))
                                  (ast/find-child (first prims) :pred-op))))]
            (if single-pred
              [single-pred st']
              [(container :scalar-filter tok [expr]) st']))
          :else                      [nil st])
        children (cond-> children tail (conj tail))
        [nl st] (expect st :newline "expected newline at end of scalar")
        children (conj children nl)]
    [(container :scalar start children) st]))

;; Relation → JoinMarker Alias? Identifier Parens? Newline Block?

(defn parse-relation [st]
  (let [start (peek-tok st)
        children []
        [jm st]       (parse-join-marker st)
        children      (conj children jm)
        [alias-n st]  (if (looks-like-alias? st) (parse-alias st) [nil st])
        children      (cond-> children alias-n (conj alias-n))
        [field-id st] (expect st :identifier "expected relation name")
        children      (conj children field-id)
        [parens st]   (if (= :lparen (peek-type st)) (parse-parens st) [nil st])
        children      (cond-> children parens (conj parens))
        [nl st]       (expect st :newline "expected newline after relation header")
        children      (conj children nl)
        [block st]    (if (= :indent (peek-type st)) (parse-block st) [nil st])
        children      (cond-> children block (conj block))]
    [(container :relation start children) st]))

;; Block → Indent (Statement | BlankLine)+ Dedent

(defn parse-block [st]
  (let [start (peek-tok st)
        [ind st] (expect st :indent "expected indent to open block")
        children [ind]]
    (loop [st       st
           children children]
      (cond
        (= :dedent (peek-type st))
        (let [[ded st] (consume-leaf st)]
          [(container :block start (conj children ded)) st])

        (= :eof (peek-type st))
        [(container :block start children) st]

        (= :blank-line (peek-type st))
        (let [[bl st] (consume-leaf st)]
          (recur st (conj children bl)))

        :else
        (let [[stmt st'] (parse-statement st)]
          (recur st' (conj children stmt)))))))

;; CountBlock → "_count" Newline Indent CountChild+ Dedent

(defn parse-count-block [st]
  (let [start (peek-tok st)
        [kw st]  (consume-leaf st)
        [nl st]  (expect st :newline "expected newline after _count")
        [ind st] (expect st :indent  "expected indented children for _count")
        children [kw nl ind]]
    (loop [st       st
           children children]
      (cond
        (= :dedent (peek-type st))
        (let [[ded st] (consume-leaf st)]
          [(container :count-block start (conj children ded)) st])

        (= :eof (peek-type st))
        [(container :count-block start children) st]

        (= :blank-line (peek-type st))
        (let [[bl st] (consume-leaf st)]
          (recur st (conj children bl)))

        :else
        ;; Progress watchdog: if `parse-count-child` can't advance
        ;; (mid-typing produces a token it can't consume — e.g. a
        ;; double-indent line when the user nested deeper than the
        ;; grammar allows), force one step forward to break the loop.
        ;; Without this the editor's per-keystroke parse hangs the
        ;; browser.
        (let [pos-before (:pos st)
              [c st']    (parse-count-child st)]
          (if (= pos-before (:pos st'))
            [(container :count-block start (conj children c)) (advance st')]
            (recur st' (conj children c))))))))

;; CountChild → Alias? Identifier Parens? Newline
;; `_count` children are always inner joins — a leading `-`/`->` is
;; consumed and replaced with a specific :error so the line still parses.

(defn parse-count-child [st]
  (let [start (peek-tok st)
        [marker-err st]
        (case (peek-type st)
          :dash  (let [[_ st'] (consume-leaf st)]
                   [(error-node start
                                "join markers are not allowed inside `_count` — counts are always inner; drop the `-`")
                    st'])
          :arrow (let [[_ st'] (consume-leaf st)]
                   [(error-node start
                                "join markers are not allowed inside `_count` — counts are always inner; drop the `->`")
                    st'])
          [nil st])
        children      (cond-> [] marker-err (conj marker-err))
        [alias-n st]  (if (looks-like-alias? st) (parse-alias st) [nil st])
        children      (cond-> children alias-n (conj alias-n))
        [field-id st] (expect st :identifier "expected relation name")
        children      (conj children field-id)
        [parens st]   (if (= :lparen (peek-type st)) (parse-parens st) [nil st])
        children      (cond-> children parens (conj parens))
        [nl st]       (expect st :newline "expected newline after count child")
        children      (conj children nl)]
    [(container :count-child start children) st]))

;; AggBlock → "_agg" Newline Indent AggRelation+ Dedent

(defn parse-agg-block [st]
  (let [start (peek-tok st)
        [kw st]  (consume-leaf st)
        [nl st]  (expect st :newline "expected newline after _agg")
        [ind st] (expect st :indent  "expected indented children for _agg")
        children [kw nl ind]]
    (loop [st       st
           children children]
      (cond
        (= :dedent (peek-type st))
        (let [[ded st] (consume-leaf st)]
          [(container :agg-block start (conj children ded)) st])

        (= :eof (peek-type st))
        [(container :agg-block start children) st]

        (= :blank-line (peek-type st))
        (let [[bl st] (consume-leaf st)]
          (recur st (conj children bl)))

        :else
        ;; Progress watchdog — see comment in `parse-count-block`.
        (let [pos-before (:pos st)
              [r st']    (parse-agg-relation st)]
          (if (= pos-before (:pos st'))
            [(container :agg-block start (conj children r)) (advance st')]
            (recur st' (conj children r))))))))

;; AggRelation → JoinMarker Alias? Identifier Newline Indent AggAttr+ Dedent

(defn parse-agg-relation [st]
  (let [start (peek-tok st)
        ;; `_agg` headers are always inner-joined, same as `_count` children.
        [marker-err st]
        (case (peek-type st)
          :dash  (let [[_ st'] (consume-leaf st)]
                   [(error-node start
                                "join markers are not allowed inside `_agg` — aggregates are always inner; drop the `-`")
                    st'])
          :arrow (let [[_ st'] (consume-leaf st)]
                   [(error-node start
                                "join markers are not allowed inside `_agg` — aggregates are always inner; drop the `->`")
                    st'])
          [nil st])
        children      (cond-> [] marker-err (conj marker-err))
        [alias-n st]  (if (looks-like-alias? st) (parse-alias st) [nil st])
        children      (cond-> children alias-n (conj alias-n))
        [field-id st] (expect st :identifier "expected agg-relation name")
        children      (conj children field-id)
        ;; Optional parens-filter on the agg header — symmetric with
        ;; `_count` children. `approved:approval_records(approved=true)`
        ;; restricts the source rows for every fn under this header.
        [parens st]   (if (= :lparen (peek-type st)) (parse-parens st) [nil st])
        children      (cond-> children parens (conj parens))
        [nl st]       (expect st :newline "expected newline after agg-relation header")
        children      (conj children nl)
        [ind st]      (expect st :indent  "expected indented agg attributes")
        children      (conj children ind)]
    (loop [st       st
           children children]
      (cond
        (= :dedent (peek-type st))
        (let [[ded st] (consume-leaf st)]
          [(container :agg-relation start (conj children ded)) st])

        (= :eof (peek-type st))
        [(container :agg-relation start children) st]

        :else
        ;; Progress watchdog — see comment in `parse-count-block`.
        (let [pos-before (:pos st)
              [a st']    (parse-agg-attr st)]
          (if (= pos-before (:pos st'))
            [(container :agg-relation start (conj children a)) (advance st')]
            (recur st' (conj children a))))))))

;; AggAttr → Identifier ":" AggFn ("," AggFn)* Newline

(defn parse-agg-attr [st]
  (let [start (peek-tok st)
        [attr-id st] (expect st :identifier "expected aggregate attribute name")
        st (skip-silent st :colon)
        children [attr-id]
        [first-fn st] (parse-agg-fn st)
        children (conj children first-fn)]
    (loop [st       st
           children children]
      (cond
        (= :comma (peek-type st))
        (let [st (advance st)
              [fn-node st] (parse-agg-fn st)]
          (recur st (conj children fn-node)))

        (= :newline (peek-type st))
        (let [[nl st] (consume-leaf st)]
          [(container :agg-attr start (conj children nl)) st])

        :else
        [(container :agg-attr start
                    (conj children (error-node (peek-tok st)
                                               "expected , or newline after agg fn")))
         (skip-to-newline st)]))))

;; AggFn → "avg" | "sum" | "min" | "max"

(defn parse-agg-fn [st]
  (let [start (peek-tok st)
        ok?   (and (= :identifier (peek-type st))
                   (#{"avg" "sum" "min" "max"} (peek-text st)))]
    (if ok?
      (let [[id st] (consume-leaf st)]
        [(container :agg-fn start [id]) st])
      [(error-node start "expected aggregate function (avg/sum/min/max)")
       (advance st)])))

;; Alias → Identifier ":"

(defn parse-alias [st]
  (let [start (peek-tok st)
        [id st] (consume-leaf st)
        st (skip-silent st :colon)]
    [(container :alias start [id]) st]))

;; JoinMarker → Arrow | Dash

(defn parse-join-marker [st]
  (let [start (peek-tok st)]
    (case (peek-type st)
      (:arrow :dash)
      (let [[leaf st] (consume-leaf st)]
        [(container :join-marker start [leaf]) st])
      [(error-node start "expected join marker (- or ->)") (advance st)])))

;; Parens → "(" ArgList? ")"

(defn parse-parens [st]
  (let [start (peek-tok st)
        st    (skip-silent st :lparen)
        [arg-list st]
        (if (or (= :rparen (peek-type st))
                (= :eof    (peek-type st)))
          [nil st]
          (parse-arg-list st))
        ;; Capture `)` end before consuming: an empty "()" would otherwise
        ;; get a zero-width span at "(", and completion couldn't detect
        ;; the cursor is inside the parens.
        rparen-to (when (= :rparen (peek-type st)) (:to (peek-tok st)))
        st        (skip-silent st :rparen)
        children  (filterv some? [arg-list])
        from      (:from start)
        to        (or rparen-to
                      (when (seq children) (second (:span (last children))))
                      from)]
    [(ast/node :parens [from to] children) st]))

;; ArgList → ArgStmt ("," ArgStmt)*

(defn parse-arg-list [st]
  (let [start (peek-tok st)
        [first-stmt st] (parse-arg-stmt st)
        children [first-stmt]]
    (loop [st       st
           children children]
      (cond
        (= :comma (peek-type st))
        (let [st (advance st)
              [stmt st'] (parse-arg-stmt st)]
          (recur st' (conj children stmt)))

        ;; Comma-optional: a fresh identifier (field name or `_limit`/
        ;; `_order_by`/… meta-key) after a complete arg begins the next
        ;; arg — whitespace separates args just like a comma. `_order_by`
        ;; stays variadic internally (its specs are comma-separated), so
        ;; this never over-consumes. The pos guard avoids an infinite loop
        ;; if parse-arg-stmt makes no progress.
        (= :identifier (peek-type st))
        (let [pos-before (:pos st)
              [stmt st'] (parse-arg-stmt st)]
          (if (= pos-before (:pos st'))
            [(container :arg-list start children) st]
            (recur st' (conj children stmt))))

        :else
        [(container :arg-list start children) st]))))

;; ArgStmt → OrExpr | MetaKey

(defn parse-arg-stmt [st]
  (let [start (peek-tok st)
        [inner st'] (if (meta-key-start? st)
                      (parse-meta-key st)
                      (parse-or-expr st))]
    [(container :arg-stmt start [inner]) st']))

;; OrExpr → AndExpr ("or" AndExpr)*

(defn parse-or-expr [st]
  (let [start (peek-tok st)
        [first-and st] (parse-and-expr st)
        children [first-and]]
    (loop [st       st
           children children]
      (if (id-text=? st "or")
        (let [st (advance st)
              [a st'] (parse-and-expr st)]
          (recur st' (conj children a)))
        [(container :or-expr start children) st]))))

;; AndExpr → PrimaryExpr ("and" PrimaryExpr)*

(defn parse-and-expr [st]
  (let [start (peek-tok st)
        [first-prim st] (parse-primary-expr st)
        children [first-prim]]
    (loop [st       st
           children children]
      (if (id-text=? st "and")
        (let [st (advance st)
              [p st'] (parse-primary-expr st)]
          (recur st' (conj children p)))
        [(container :and-expr start children) st]))))

;; PrimaryExpr → ArgPredicate | GroupedExpr

(defn parse-primary-expr [st]
  (let [start (peek-tok st)
        [inner st']
        (if (= :lparen (peek-type st))
          (parse-grouped-expr st)
          (parse-arg-predicate st))]
    [(container :primary-expr start [inner]) st']))

;; GroupedExpr → "(" OrExpr ")"

(defn parse-grouped-expr [st]
  (let [start (peek-tok st)
        st (skip-silent st :lparen)
        [inner st] (parse-or-expr st)
        st (skip-silent st :rparen)]
    [(container :grouped-expr start [inner]) st]))

;; ArgPredicate → Path PredOp

(defn parse-arg-predicate [st]
  (let [start (peek-tok st)
        [path st]    (parse-path st)
        [pred-op st] (parse-pred-op st)]
    [(container :arg-predicate start [path pred-op]) st]))

;; Path → Identifier ("." Identifier)*

(defn parse-path [st]
  (let [start (peek-tok st)
        [first-id st] (expect st :identifier "expected identifier in path")
        children [first-id]]
    (loop [st       st
           children children]
      (if (= :dot (peek-type st))
        (let [st (advance st)
              [id st'] (expect st :identifier "expected identifier after `.`")]
          (recur st' (conj children id)))
        [(container :path start children) st]))))

;; PredOp →
;;     BinaryOp Value
;;   | "in" (ListLiteral | ParamRef)
;;   | "not" "in" (ListLiteral | ParamRef)
;;   | "like" String
;;   | "ilike" String
;;   | "is" "null"
;;   | "is" "not" "null"

(defn parse-in-operand
  "`in ?xs[]` — a bare array param binds the whole set; wrapped in a
   ListLiteral so the compiler sees one shape. Anything else parses as
   a parenthesized list."
  [st]
  (if (= :param-ref (peek-type st))
    (let [tok    (peek-tok st)
          [p st] (consume-leaf st)]
      [(container :list-literal tok [(container :value tok [p])]) st])
    (parse-list-literal st)))

(defn parse-pred-op [st]
  (let [start (peek-tok st)]
    (cond
      ;; "is null" / "is not null"
      (id-text=? st "is")
      (let [[is-id st]   (consume-leaf st)
            children     [is-id]
            [not-id st]  (if (id-text=? st "not")
                           (consume-leaf st)
                           [nil st])
            children     (cond-> children not-id (conj not-id))
            [null-id st] (if (id-text=? st "null")
                           (consume-leaf st)
                           [(error-node (peek-tok st) "expected `null` after `is`")
                            st])
            children     (conj children null-id)]
        [(container :pred-op start children) st])

      ;; "not in (...)" — only consume the list if `in` actually follows;
      ;; eager consumption on partial input (`name not <eol>`) used to
      ;; fabricate a misleading empty-list lint error.
      (id-text=? st "not")
      (let [[not-id st] (consume-leaf st)
            children    [not-id]]
        (if (id-text=? st "in")
          (let [[in-id st] (consume-leaf st)
                children   (conj children in-id)
                [list st]  (parse-in-operand st)
                children   (conj children list)]
            [(container :pred-op start children) st])
          (let [err (error-node (peek-tok st) "expected `in` after `not`")]
            [(container :pred-op start (conj children err)) st])))

      ;; "in (...)"
      (id-text=? st "in")
      (let [[in-id st] (consume-leaf st)
            [list st]  (parse-in-operand st)]
        [(container :pred-op start [in-id list]) st])

      ;; "like" / "ilike" (String | ParamRef)
      (or (id-text=? st "like") (id-text=? st "ilike"))
      (let [[op-id st] (consume-leaf st)
            [val st]   (if (= :param-ref (peek-type st))
                         (consume-leaf st)
                         (expect st :string "expected string or named-param after like/ilike"))]
        [(container :pred-op start [op-id val]) st])

      ;; Binary op + value
      (binary-op-types (peek-type st))
      (let [[bop st]  (parse-binary-op st)
            [val st]  (parse-value st)
            ;; Missing value: move the error onto the operator so the
            ;; linter underlines `>` instead of a zero-width marker.
            val (if (= :error (:node val))
                  (assoc val :span (:span bop))
                  val)]
        [(container :pred-op start [bop val]) st])

      :else
      [(error-node start "expected predicate operator") (advance st)])))

;; BinaryOp → Eq | Neq | Lt | Le | Gt | Ge

(defn parse-binary-op [st]
  (let [start (peek-tok st)]
    (if (binary-op-types (peek-type st))
      (let [[leaf st] (consume-leaf st)]
        [(container :binary-op start [leaf]) st])
      [(error-node start "expected binary operator") (advance st)])))

;; Value → String | Number | "true" | "false" | "null" | ListLiteral |
;; Identifier | ParamRef

(defn parse-value [st]
  (let [start (peek-tok st)]
    (cond
      (= :string (peek-type st))
      (let [[s st] (consume-leaf st)] [(container :value start [s]) st])

      (= :number (peek-type st))
      (let [[n st] (consume-leaf st)] [(container :value start [n]) st])

      (= :lparen (peek-type st))
      (let [[l st] (parse-list-literal st)] [(container :value start [l]) st])

      ;; Reserved meta-key in value position: emit a zero-width error
      ;; WITHOUT advancing, so `_limit 3` still parses as the next arg.
      (and (= :identifier (peek-type st))
           (meta-keys (peek-text st)))
      [{:node :error :span [(:from start) (:from start)] :text ""
        :message "expected value after operator"} st]

      ;; true/false/null/enum.
      (= :identifier (peek-type st))
      (let [[id st] (consume-leaf st)] [(container :value start [id]) st])

      (= :param-ref (peek-type st))
      (let [[p st] (consume-leaf st)] [(container :value start [p]) st])

      :else
      [(error-node start "expected value") (advance st)])))

;; ListLiteral → "(" (Value ("," Value)*)? ")"

(defn parse-list-literal [st]
  (let [start (peek-tok st)
        st    (skip-silent st :lparen)
        children []]
    (if (= :rparen (peek-type st))
      ;; empty list
      [(container :list-literal start children) (advance st)]
      (let [[first-v st] (parse-value st)
            children     (conj children first-v)]
        (loop [st       st
               children children]
          (cond
            (= :comma (peek-type st))
            (let [st (advance st)
                  [v st'] (parse-value st)]
              (recur st' (conj children v)))

            (= :rparen (peek-type st))
            [(container :list-literal start children) (advance st)]

            :else
            [(container :list-literal start
                        (conj children (error-node (peek-tok st)
                                                   "expected , or ) in list")))
             (skip-to-newline st)]))))))

;; MetaKey →
;;     "_limit" Number
;;   | "_offset" Number
;;   | "_order_by" OrderSpec ("," OrderSpec)*
;;   | "_distinct" Identifier ("," Identifier)*

(defn consume-number-or-param-ref
  "A :number literal or a :param-ref placeholder, for `_limit`/`_offset`."
  [st label]
  (cond
    (= :number (peek-type st))    (consume-leaf st)
    (= :param-ref (peek-type st)) (consume-leaf st)
    :else [(error-node (peek-tok st)
                       (str "expected number or named-param after " label))
           st]))

(defn parse-meta-key [st]
  (let [start (peek-tok st)
        kw-text (peek-text st)
        canon (normalize-meta-kw kw-text)
        [kw-id st] (consume-leaf st)
        ;; `order by` — consume the `by` token into the node too.
        [by-id st] (if (and (= "order" kw-text) (id-text=? st "by"))
                     (consume-leaf st)
                     [nil st])
        children (cond-> [kw-id] by-id (conj by-id))]
    (case canon
      "_limit"
      (let [[n st] (consume-number-or-param-ref st "_limit")]
        [(container :meta-key start (conj children n)) st])

      "_offset"
      (let [[n st] (consume-number-or-param-ref st "_offset")]
        [(container :meta-key start (conj children n)) st])

      ;; `_join left|inner|right|full` — explicit join type. Overrides the
      ;; relation's sigil-implied default (e.g. `->rel (_join inner)`).
      "_join"
      (let [[id st] (expect st :identifier
                            "expected join type (left|inner|right|full) after _join")]
        [(container :meta-key start (conj children id)) st])

      ;; `_on <relation>` — the self-relation @search-tree/@get-tree recurse
      ;; over. Root parens of tree ops only (lint enforces placement).
      "_on"
      (let [[id st] (expect st :identifier
                            "expected a self-relation name after _on")]
        [(container :meta-key start (conj children id)) st])

      "_order_by"
      (if (= :param-ref (peek-type st))
        ;; `_order_by ?sort:order(cols…)="col dir"` — a single param-ref
        ;; replaces the whole spec list; no mixing with static specs.
        (let [[pref st] (consume-leaf st)]
          [(container :meta-key start (conj children pref)) st])
        (let [[first-spec st] (parse-order-spec st)
              children (conj children first-spec)]
        (loop [st       st
               children children]
          ;; Stop at comma-then-meta-key — `limit` starts a new meta-key,
          ;; not another sort column (same reserved-word tradeoff as SQL).
          (if (and (= :comma (peek-type st))
                   (not (meta-key-start? (advance st))))
            (let [st (advance st)
                  [s st'] (parse-order-spec st)]
              (recur st' (conj children s)))
            [(container :meta-key start children) st]))))

      "_distinct"
      (let [[id st] (expect st :identifier "expected identifier after _distinct")
            children (conj children id)]
        (loop [st       st
               children children]
          (if (= :comma (peek-type st))
            (let [st (advance st)
                  [id st'] (expect st :identifier "expected identifier after `,`")]
              (recur st' (conj children id)))
            [(container :meta-key start children) st]))))))

;; OrderSpec → Identifier ("asc" | "desc")

(defn parse-order-spec [st]
  (let [start (peek-tok st)
        [id st] (expect st :identifier "expected identifier in order-by")
        children [id]
        [dir st]
        (cond
          (or (id-text=? st "asc") (id-text=? st "desc"))
          (consume-leaf st)
          :else
          [(error-node (peek-tok st) "expected asc or desc") st])
        children (conj children dir)]
    [(container :order-spec start children) st]))

;; ── ScalarFilter family ─────────────────────────────────────────────────
;;
;; ScalarFilter  → "(" ScalarOrExpr ")"
;; ScalarOrExpr  → ScalarAndExpr ("or" ScalarAndExpr)*
;; ScalarAndExpr → ScalarPrim ("and" ScalarPrim)*
;; ScalarPrim    → PredOp | "(" ScalarOrExpr ")"

(defn parse-scalar-filter [st]
  (let [start (peek-tok st)
        st    (skip-silent st :lparen)
        [inner st] (parse-scalar-or-expr st)
        st    (skip-silent st :rparen)]
    [(container :scalar-filter start [inner]) st]))

(defn parse-scalar-or-expr [st]
  (let [start (peek-tok st)
        [first-and st] (parse-scalar-and-expr st)
        children [first-and]]
    (loop [st       st
           children children]
      (if (id-text=? st "or")
        (let [st (advance st)
              [a st'] (parse-scalar-and-expr st)]
          (recur st' (conj children a)))
        [(container :scalar-or-expr start children) st]))))

(defn parse-scalar-and-expr [st]
  (let [start (peek-tok st)
        [first-prim st] (parse-scalar-prim st)
        children [first-prim]]
    (loop [st       st
           children children]
      (if (id-text=? st "and")
        (let [st (advance st)
              [p st'] (parse-scalar-prim st)]
          (recur st' (conj children p)))
        [(container :scalar-and-expr start children) st]))))

(defn parse-scalar-prim [st]
  (let [start (peek-tok st)
        [inner st']
        (if (= :lparen (peek-type st))
          (let [st (skip-silent st :lparen)
                [n st] (parse-scalar-or-expr st)
                st (skip-silent st :rparen)]
            [n st])
          (parse-pred-op st))]
    [(container :scalar-prim start [inner]) st']))

;; ── Public entry ─────────────────────────────────────────────────────────

(defn parse
  "Parse an XSQL source string. Returns the :query AST root node.
   Errors are embedded as :error nodes in the tree; partial parses
   still produce a usable AST."
  [source]
  (let [tokens (tok/tokenize source)
        st     (make-state tokens)
        [root _st] (parse-query st)]
    root))
