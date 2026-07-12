(ns synthigy.xsql.parser
  "Recursive-descent parser for the XSQL DSL.

   Consumes the flat token vector from `synthigy.xsql.tokens`
   and produces an AST tree per `synthigy.xsql.ast`.

   Productions correspond 1:1 to rules in
   `sdk/query-dsl/grammar/query-dsl.grammar`. Each `parse-*` function
   takes a state map `{:tokens vec :pos int}` and returns
   `[ast-node new-state]`.

   Error recovery: when an `expect` fails, an `:error` node is emitted
   in place and the parser advances to the next `:newline` (or `:eof`)
   so the rest of the file still parses."
  (:require [synthigy.xsql.ast :as ast]
            [synthigy.xsql.tokens :as tok]))

;; ── Token cursor ─────────────────────────────────────────────────────────

(defn make-state [tokens] {:tokens (vec tokens) :pos 0})

(defn- peek-tok
  ([st]   (get (:tokens st) (:pos st)))
  ([st k] (get (:tokens st) (+ (:pos st) k))))

(defn- peek-type
  ([st]   (:type (peek-tok st)))
  ([st k] (:type (peek-tok st k))))

(defn- peek-text [st] (:text (peek-tok st)))

(defn- advance [st] (update st :pos inc))

(defn- at-eof? [st] (= :eof (peek-type st)))

(defn- id-text=?
  "True if current token is an :identifier whose text matches."
  [st text]
  (and (= :identifier (peek-type st))
       (= text (peek-text st))))

(defn- id-text=at?
  "True if token at offset k is an :identifier whose text matches."
  [st k text]
  (and (= :identifier (peek-type st k))
       (= text (:text (peek-tok st k)))))

;; ── Errors ───────────────────────────────────────────────────────────────

(defn- error-node [tok msg]
  (let [t (or tok {:from 0 :to 0 :text ""})]
    {:node :error
     :span [(:from t) (:to t)]
     :text (or (:text t) "")
     ;; If the underlying token came from the lexer with its own diagnostic
     ;; message (e.g. `"XSQL parameters must be named …"`), prefer that —
     ;; the linter's generic fallback (`"expected value"`) is less useful
     ;; than the cause the lexer already pinpointed.
     :message (or (:message t) msg)}))

(defn- skip-to-newline
  "Advance until :newline / :dedent / :eof; consume the :newline if found."
  [st]
  (loop [st st]
    (case (peek-type st)
      :newline       (advance st)
      (:dedent :eof) st
      (recur (advance st)))))

;; ── Token → leaf node ────────────────────────────────────────────────────

(defn- tok->leaf [t]
  (let [tag (:type t)
        sp  [(:from t) (:to t)]]
    (case tag
      (:identifier :string :number :arrow :dash
       :eq :neq :lt :le :gt :ge)
      (ast/leaf tag sp (:text t))

      ;; Preserve the lexer's :message so callers (linter) can surface the
      ;; specific cause (e.g. "XSQL parameters must be named") rather than
      ;; falling back to the parser's generic "expected value".
      :error
      (cond-> (ast/leaf tag sp (:text t))
        (:message t) (assoc :message (:message t)))

      ;; Param refs are leaves but carry richer metadata (name, type-raw,
      ;; array?) so the compiler doesn't have to re-scan the text.
      :param-ref
      (cond-> (assoc (ast/leaf tag sp (:text t))
                     :param-name     (:param-name t)
                     :param-type-raw (:param-type-raw t)
                     :array?         (:array? t))
        (:param-default t) (assoc :param-default (:param-default t)))

      (:newline :blank-line :indent :dedent)
      (ast/leaf tag sp (:text t)))))

(defn- consume-leaf [st]
  [(tok->leaf (peek-tok st)) (advance st)])

(defn- expect
  "Consume token of given type; on mismatch emit :error and don't advance."
  [st expected-type msg]
  (if (= expected-type (peek-type st))
    (consume-leaf st)
    [(error-node (peek-tok st) (or msg (str "expected " (name expected-type))))
     st]))

(defn- skip-silent
  "Advance past token of given type without adding to children."
  [st t]
  (if (= t (peek-type st))
    (advance st)
    st))

;; ── Span helpers ─────────────────────────────────────────────────────────

(defn- end-of-children
  "Last child's :span end, or fallback if no children."
  [children fallback]
  (if (seq children)
    (or (second (:span (last children))) fallback)
    fallback))

(defn- container
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
         parse-scalar-prim parse-alias parse-join-marker parse-root-args)

;; ── Predicates ───────────────────────────────────────────────────────────

(def ^:private binary-op-types #{:eq :neq :lt :le :gt :ge})

;; Reserved arg-list meta-keys. They start a fresh arg-stmt and must never
;; be consumed as a predicate value — otherwise `value > _limit 3` eats
;; `_limit` as the RHS of `>` and the rest of the parens cascade.
(def ^:private meta-keys #{"_limit" "_offset" "_order_by" "_distinct" "_join"})

(defn- pred-op-starter?
  "True if current token can start a PredOp (after a path / scalar id)."
  [st]
  (or (binary-op-types (peek-type st))
      (id-text=? st "in")
      (id-text=? st "not")
      (id-text=? st "like")
      (id-text=? st "ilike")
      (id-text=? st "is")))

(defn- looks-like-alias?
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
;; Rooted form: the outermost node is the entity; everything indented under
;; it is the selection. The entity identifier is captured on the :query node
;; under `:root-entity` (NOT as a child) so AST shape/sexpr dumps stay
;; identical to the body alone — only the entity *key* is added. The newline
;; after the entity and the body's opening :indent / closing :dedent are
;; consumed silently. `compile-query` reads `:root-entity` to emit `:entity`.

(defn- parse-query-body
  "Parse the body of a rooted query: (BlankLine | RootArgs | Statement)*
   until :dedent / :eof. Returns `[children st]`."
  [st]
  (loop [st       st
         children []]
    (let [pos-before (:pos st)]
      (cond
        (#{:dedent :eof} (peek-type st))
        [children st]

        (= :blank-line (peek-type st))
        (let [[bl st'] (consume-leaf st)]
          (recur st' (conj children bl)))

        (and (id-text=? st "_args")
             (or (= :lparen  (peek-type st 1))
                 (= :newline (peek-type st 1))))
        (let [[n st'] (parse-root-args st)]
          (recur st' (conj children n)))

        ;; Anything else valid is wrapped in a Statement.
        (or (#{:dash :arrow} (peek-type st))
            (= :identifier   (peek-type st)))
        (let [[n st'] (parse-statement st)]
          (recur st' (conj children n)))

        :else
        ;; Unrecognized token. Emit an error AND guarantee forward progress —
        ;; skip-to-newline can return at :dedent / :eof, so the watchdog
        ;; below force-advances if nothing moved.
        (let [err (error-node (peek-tok st)
                              (str "unexpected token: " (name (peek-type st))))
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
        ;; Optional root args in parens — `Movie (release_year >= 2000, _limit
        ;; 10, _order_by release_year desc)`. The root accepts the same arg-list
        ;; as a relation header; compiles to root :args.
        [root-parens st]
        (if (and root-entity (= :lparen (peek-type st)))
          (parse-parens st)
          [nil st])
        ;; Consume the newline after the entity. Blank-lines / comments may
        ;; sit between the entity and the body's opening :indent — gather them
        ;; first so the indent check below isn't fooled into thinking the body
        ;; is empty.
        st          (skip-silent st :newline)
        [pre-blanks st]
        (loop [st st acc []]
          (if (= :blank-line (peek-type st))
            (let [[bl st'] (consume-leaf st)]
              (recur st' (conj acc bl)))
            [acc st]))
        had-indent? (= :indent (peek-type st))
        st          (skip-silent st :indent)
        ;; Parse the body, then close the block.
        [body st]   (parse-query-body st)
        st          (if had-indent? (skip-silent st :dedent) st)
        children    (-> (vec lead-blanks) (into pre-blanks) (into body))
        end         (or (:from (peek-tok st)) start)
        query       (cond-> (ast/node :query [start end] children)
                      root-entity (assoc :root-entity root-entity)
                      root-parens (assoc :root-parens root-parens))]
    [query (skip-silent st :eof)]))

;; RootArgs → "_args" Parens Newline                  (parens form, single-line)
;;          | "_args" Newline Indent ArgStmt+ Dedent  (block form, multi-line)
;;
;; Block form: each indented line is one ArgStmt; siblings AND together
;; (same as comma in the parens form). Within a single line, `or` and
;; explicit `and` joiners work as today. Use grouping parens for
;; multi-line OR chains.

(defn- parse-block-arg-list
  "Parse the body of an indented args block: ArgStmt+ separated by
   newlines instead of commas. Skips blank lines. Stops at :dedent / :eof."
  [st]
  (let [start (peek-tok st)]
    (loop [st       st
           children []]
      (cond
        (#{:dedent :eof} (peek-type st))
        [(container :arg-list start children) st]

        (= :blank-line (peek-type st))
        (let [[bl st'] (consume-leaf st)]
          (recur st' (conj children bl)))

        :else
        (let [[stmt st']  (parse-arg-stmt st)
              children    (conj children stmt)
              [nl st'']   (expect st' :newline "expected newline after arg")
              children    (conj children nl)]
          (recur st'' children))))))

(defn parse-root-args [st]
  (let [start        (peek-tok st)
        [args-id st] (consume-leaf st)]
    (cond
      ;; Parens form: `_args (a = 1, b = 2)` on one line.
      (= :lparen (peek-type st))
      (let [[parens st] (parse-parens st)
            [nl st]     (expect st :newline "expected newline after _args(...)")]
        [(container :root-args start [args-id parens nl]) st])

      ;; Block form: `_args` then indented body of arg-stmts, one per line.
      (= :newline (peek-type st))
      (let [[nl st]      (consume-leaf st)
            [ind st]     (expect st :indent
                                  "expected indented args block after `_args` (or use `_args (...)` form)")
            ;; If the indent itself was missing, don't try to parse the
            ;; body — emit a clean root-args with the error and let the
            ;; rest of the query parse normally.
            [arg-list st] (if (= :error (:node ind))
                            [nil st]
                            (parse-block-arg-list st))
            [ded st]     (if arg-list
                           (expect st :dedent "expected dedent after args block")
                           [nil st])
            children     (filterv some? [args-id nl ind arg-list ded])]
        [(container :root-args start children) st])

      ;; Neither — emit an error after consuming "_args".
      :else
      (let [err (error-node (peek-tok st)
                            "expected `(...)` or indented block after `_args`")]
        [(container :root-args start [args-id err]) st]))))

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
;;
;; Scalar aliases (`display: name`) were dropped from the language —
;; they were cosmetic, broke sql.query, and confused users into writing
;; `alias:-rel` forms. Relation, _count, and _agg aliases remain.

(defn parse-scalar [st]
  (let [start (peek-tok st)
        children []
        [field-id st] (expect st :identifier "expected field name")
        children (conj children field-id)
        [tail st]
        (cond
          (= :lparen (peek-type st)) (parse-scalar-filter st)
          (pred-op-starter? st)      (parse-pred-op st)
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
;;
;; `_count` children are always inner joins on the parent's relations, so
;; the `-` / `->` join markers used elsewhere are syntactically rejected
;; here. A leading `-` or `->` is consumed and replaced with an :error
;; node carrying a specific message — keeps the rest of the line parseable
;; so the user only sees one diagnostic.

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
        ;; `_agg` headers are always inner-joined relations — same as
        ;; `_count` children. Reject leading `-` / `->` with a specific
        ;; message; consume the bad marker so the rest of the line still
        ;; parses cleanly and we surface only one diagnostic.
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
        ;; Capture ) end-position before consuming it. Without this, an
        ;; empty "()" produces a zero-width span at "(" — the cursor
        ;; between "(" and ")" falls outside the span, so completion
        ;; fails to detect the in-parens context.
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
        meta? (and (= :identifier (peek-type st))
                   (meta-keys (peek-text st)))
        [inner st'] (if meta? (parse-meta-key st) (parse-or-expr st))]
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
;;   | "in" ListLiteral
;;   | "not" "in" ListLiteral
;;   | "like" String
;;   | "ilike" String
;;   | "is" "null"
;;   | "is" "not" "null"

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

      ;; "not in (...)" — only consume the list literal if `in` actually
      ;; follows. Eagerly calling parse-list-literal on partial input
      ;; (`name not <eol>`) used to fabricate an empty list and the lint
      ;; would then complain about "Empty list — not in () is invalid",
      ;; which is misleading mid-typing.
      (id-text=? st "not")
      (let [[not-id st] (consume-leaf st)
            children    [not-id]]
        (if (id-text=? st "in")
          (let [[in-id st] (consume-leaf st)
                children   (conj children in-id)
                [list st]  (parse-list-literal st)
                children   (conj children list)]
            [(container :pred-op start children) st])
          (let [err (error-node (peek-tok st) "expected `in` after `not`")]
            [(container :pred-op start (conj children err)) st])))

      ;; "in (...)"
      (id-text=? st "in")
      (let [[in-id st] (consume-leaf st)
            [list st]  (parse-list-literal st)]
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
            ;; Missing/invalid value (e.g. `value > _limit`) → point the
            ;; error at the OPERATOR so the linter underlines `>` instead of
            ;; a zero-width marker floating in the whitespace after it.
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

;; Value → String | Number | "true" | "false" | "null" | ListLiteral | Identifier | ParamRef

(defn parse-value [st]
  (let [start (peek-tok st)]
    (cond
      (= :string (peek-type st))
      (let [[s st] (consume-leaf st)] [(container :value start [s]) st])

      (= :number (peek-type st))
      (let [[n st] (consume-leaf st)] [(container :value start [n]) st])

      (= :lparen (peek-type st))
      (let [[l st] (parse-list-literal st)] [(container :value start [l]) st])

      ;; Reserved meta-key in value position → the predicate is missing its
      ;; value (`value > _limit`). Emit a zero-width "expected value" error
      ;; WITHOUT advancing, so `_limit 3` is still parsed as the next arg and
      ;; the parens/block stay intact (no cascade to 'missing child').
      (and (= :identifier (peek-type st))
           (meta-keys (peek-text st)))
      [{:node :error :span [(:from start) (:from start)] :text ""
        :message "expected value after operator"} st]

      ;; Identifiers (true/false/null/enum).
      (= :identifier (peek-type st))
      (let [[id st] (consume-leaf st)] [(container :value start [id]) st])

      ;; Named-parameter placeholder `?name:type[]`. The lexer's error
      ;; token (bare `?`) is also accepted here so error recovery keeps
      ;; the surrounding tree intact.
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

(defn- consume-number-or-param-ref
  "Accept either a :number literal or a :param-ref placeholder as a
   value. Used by `_limit` / `_offset` where the meta-key takes a single
   numeric value position."
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
        [kw-id st] (consume-leaf st)
        children [kw-id]]
    (case kw-text
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

      "_order_by"
      (let [[first-spec st] (parse-order-spec st)
            children (conj children first-spec)]
        (loop [st       st
               children children]
          ;; Stop at comma-then-meta-key: `_order_by release_year desc, _limit 20`
          ;; — the `_limit` starts a new meta-key, not another sort column.
          (if (and (= :comma (peek-type st))
                   (not (meta-keys (peek-text (advance st)))))
            (let [st (advance st)
                  [s st'] (parse-order-spec st)]
              (recur st' (conj children s)))
            [(container :meta-key start children) st])))

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
