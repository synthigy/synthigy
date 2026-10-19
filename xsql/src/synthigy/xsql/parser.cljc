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
  [st text]
  (and (= :identifier (peek-type st))
       (= text (peek-text st))))

(defn id-text=at?
  [st k text]
  (and (= :identifier (peek-type st k))
       (= text (:text (peek-tok st k)))))

;; ── Errors ───────────────────────────────────────────────────────────────

(defn error-node [tok msg]
  (let [t (or tok {:from 0 :to 0 :text ""})]
    {:node :error
     :span [(:from t) (:to t)]
     :text (or (:text t) "")
     :message (or (:message t) msg)}))

(defn skip-to-newline
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

      :error
      (cond-> (ast/leaf tag sp (:text t))
        (:message t) (assoc :message (:message t)))

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
  [st t]
  (if (= t (peek-type st))
    (advance st)
    st))

;; ── Span helpers ─────────────────────────────────────────────────────────

(defn end-of-children
  [children fallback]
  (if (seq children)
    (or (second (:span (last children))) fallback)
    fallback))

(defn container
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

(def ^:private meta-keys #{"_limit" "_offset" "_order_by" "_distinct" "_join" "_on"})
(def ^:private bare-meta-keys #{"limit" "offset" "distinct" "join" "on"})

(defn normalize-meta-kw
  "Canonical `_`-form of a meta-key spelling; the compiler shares this table."
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
  [st]
  (or (binary-op-types (peek-type st))
      (id-text=? st "in")
      (id-text=? st "not")
      (id-text=? st "like")
      (id-text=? st "ilike")
      (id-text=? st "is")))

(defn meta-key-start?
  "True when the current token begins a meta-key arg-stmt (bare forms disambiguated by position)."
  [st]
  (and (= :identifier (peek-type st))
       (let [t (peek-text st)]
         (or (meta-keys t)
             (and (= "order" t) (id-text=at? st 1 "by"))
             (and (bare-meta-keys t)
                  (not (pred-op-starter? (advance st))))))))

(defn looks-like-alias?
  [st]
  (and (= :identifier (peek-type st))
       (= :colon      (peek-type st 1))))

;; ═══════════════════════════════════════════════════════════════════════
;;   PRODUCTIONS
;; ═══════════════════════════════════════════════════════════════════════

(defn parse-query-body
  "Parse a rooted query's body until :dedent / :eof; returns `[children st]`."
  [st]
  (loop [st       st
         children []]
    (let [pos-before (:pos st)]
      (cond
        ;; nil peek = past EOF (unterminated root paren) — must exit here or skip-to-newline loops forever
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
        ;; Progress watchdog: force-advance on no progress — editor hang otherwise.
        (let [err (error-node (peek-tok st)
                              (str "unexpected token: " (name (or (peek-type st) :eof))))
              st' (skip-to-newline (advance st))]
          (if (= pos-before (:pos st'))
            [(conj children err) (advance st')]
            (recur st' (conj children err))))))))

(defn parse-query [st]
  (let [start-tok (peek-tok st)
        start     (:from start-tok 0)
        [lead-blanks st]
        (loop [st st acc []]
          (if (= :blank-line (peek-type st))
            (let [[bl st'] (consume-leaf st)]
              (recur st' (conj acc bl)))
            [acc st]))
        [root-entity st]
        (if (= :identifier (peek-type st))
          (consume-leaf st)
          [nil st])
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

(defn parse-scalar [st]
  (let [start (peek-tok st)
        children []
        [field-id st] (expect st :identifier "expected field name")
        children (conj children field-id)
        [tail st]
        (cond
          (= :lparen (peek-type st)) (parse-scalar-filter st)
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
        ;; Progress watchdog: force-advance on no progress — editor hang otherwise.
        (let [pos-before (:pos st)
              [c st']    (parse-count-child st)]
          (if (= pos-before (:pos st'))
            [(container :count-block start (conj children c)) (advance st')]
            (recur st' (conj children c))))))))

;; CountChild → Alias? Identifier Parens? Newline

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
        ;; Progress watchdog: force-advance on no progress — editor hang otherwise.
        (let [pos-before (:pos st)
              [r st']    (parse-agg-relation st)]
          (if (= pos-before (:pos st'))
            [(container :agg-block start (conj children r)) (advance st')]
            (recur st' (conj children r))))))))

(defn parse-agg-relation [st]
  (let [start (peek-tok st)
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
        ;; Progress watchdog: force-advance on no progress — editor hang otherwise.
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
        ;; capture `)` end before consuming — else `()` gets a zero-width span and completion misses it
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

(defn parse-in-operand
  "`in` operand: a bare array param wrapped as a ListLiteral, else a parenthesized list."
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

      ;; "not in (...)"
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

      ;; never advance on a meta-key here — `_limit 3` must still parse as the next arg
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

(defn consume-number-or-param-ref
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

      "_join"
      (let [[id st] (expect st :identifier
                            "expected join type (left|inner|right|full) after _join")]
        [(container :meta-key start (conj children id)) st])

      "_on"
      (let [[id st] (expect st :identifier
                            "expected a self-relation name after _on")]
        [(container :meta-key start (conj children id)) st])

      "_order_by"
      (if (= :param-ref (peek-type st))
        (let [[pref st] (consume-leaf st)]
          [(container :meta-key start (conj children pref)) st])
        (let [[first-spec st] (parse-order-spec st)
              children (conj children first-spec)]
        (loop [st       st
               children children]
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
  "Parse an XSQL source string to its :query AST; errors are embedded as :error nodes."
  [source]
  (let [tokens (tok/tokenize source)
        st     (make-state tokens)
        [root _st] (parse-query st)]
    root))
