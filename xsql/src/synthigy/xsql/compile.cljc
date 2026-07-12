(ns synthigy.xsql.compile
  "AST → wire JSON.

   Produces `{:selections {…} :args {…}?}` matching the XSQL.md
   §Wire-compilation table. Wire output is byte-identical to the
   existing JS `compile()` (after JSON ↔ EDN round-trip) and is
   what `/data` consumes today.

   No schema knowledge here — that lives in the linter.

   ## Named parameters

   The 3-arity `compile` accepts a `params` map. Any `?name:type[]`
   reference in the AST is resolved to its literal value at compile
   time and type-checked against the declared `:type`. The wire output
   never carries the `?name` syntax — only literal values that
   downstream sql.query binds via JDBC. Missing required params and
   type mismatches throw with a clear message."
  (:refer-clojure :exclude [compile])
  (:require [clojure.string :as str]
            [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]
            [synthigy.xsql.sql-params :as sql-params]))

(declare compile-statement-into compile-scalar compile-relation
         compile-count-block compile-agg-block compile-args-of-parens
         compile-arg-list compile-arg-stmt compile-or-expr compile-and-expr
         compile-primary-expr compile-arg-predicate compile-pred-op
         compile-value compile-list-literal compile-meta-key
         compile-scalar-or compile-scalar-and compile-scalar-prim
         merge-filters-into deep-merge-args)

(def ^:private meta-keys #{:_limit :_offset :_order_by :_distinct :_join})

(defn- text-of [leaf] (:text leaf))

;; ── Named parameters — resolution during compile ────────────────────────
;;
;; `compile` rebinds `*params*` to whatever map the caller passes (or `nil`
;; when no params were supplied). `compile-value` / `compile-meta-key` look
;; here when they encounter a :param-ref leaf in the AST. The wire never
;; carries `?name` references — only literal values that downstream
;; sql.query binds via JDBC.

(def ^:dynamic *params* nil)

(defn- param-type-kw
  "Resolve a :param-ref node's raw type token to a canonical type keyword.
   Defaults to :string (matches sql-params behaviour for untyped params)."
  [param-ref-node]
  (let [raw (:param-type-raw param-ref-node)]
    (or (when raw
          (get sql-params/type-aliases (str/lower-case raw)))
        :string)))

(defn- lookup-param
  "Resolve a :param-ref leaf's value: the bound value if supplied, else the
   inline `=default` if declared, else nil (syntax-only compile) or a
   PARAM_MISSING throw (params bound but the name absent and no default)."
  [param-ref-node]
  (let [nm (:param-name param-ref-node)]
    (cond
      (and *params* (contains? *params* (keyword nm))) (get *params* (keyword nm))
      (and *params* (contains? *params* nm))           (get *params* nm)
      (:param-default param-ref-node)
      (sql-params/coerce-default (:param-default param-ref-node) (param-type-kw param-ref-node))
      (nil? *params*) nil
      :else
      (throw (ex-info (str "Missing value for parameter ?" nm)
                      {:code "PARAM_MISSING"
                       :param nm
                       :span (:span param-ref-node)})))))

(defn- resolve-param-ref
  "Resolve a :param-ref leaf to its bound literal value. Type-checks
   against the declared :type; throws on mismatch."
  [param-ref-node]
  (let [type   (param-type-kw param-ref-node)
        array? (:array? param-ref-node)
        v      (lookup-param param-ref-node)
        err    (when (some? *params*)
                 (sql-params/validate-value type array? v))]
    (when err
      (throw (ex-info
              (str "Parameter ?" (:param-name param-ref-node) " " err)
              {:code "PARAM_TYPE_MISMATCH"
               :param (:param-name param-ref-node)
               :type type
               :array? array?
               :value v
               :message err
               :span (:span param-ref-node)})))
    v))

;; ── Op-specific rewrites (multimethod) ───────────────────────────────────
;;
;; `compile-query` produces a generic wire shape; per-op rewrites layer
;; on top via this multimethod. New ops with their own compile-time
;; transforms register a `defmethod` here.

(defmulti op-compile-rewrite
  "Rewrite the compiled `{:selections … :args …?}` map for op-specific
   semantics. Dispatches on the wire op string; `:default` is identity."
  (fn [op _result] (or op :default)))

(defmethod op-compile-rewrite :default [_ result] result)

(defn- root-scalar-args-entry?
  "Detect the wire shape compile-scalar emits for `field = value` (and
   other inline scalar predicates): a single-element vector containing
   exactly one map whose only key is `:args` and whose `:args` value is
   itself a map (i.e. an operator map like `{:_eq …}`).

   Relations and `_count` / `_agg` blocks never produce this exact shape:
   relations always include `:selections` (or are empty `{}` when bare),
   and `_count` / `_agg` use plain map values, not single-args vectors.
   `is_null` / `is_not_null` produce a string `:args`, not a map — those
   stay field-level too."
  [v]
  (and (vector? v)
       (= 1 (count v))
       (let [e (first v)]
         (and (map? e)
              (= #{:args} (set (keys e)))
              (map? (:args e))))))

(defn- get-arg-value
  "Collapse a single-field predicate map to the bare value `get`
   expects. `get` is primary-key-style (`{:name \"Engineering\"}`),
   not predicate-style — the lifted scalar is `{:_eq …}` from compile,
   so we strip that wrapper. Any non-`:_eq` operator passes through
   unchanged (rare; would be a misuse, but better than silently
   discarding it)."
  [pred]
  (if (and (map? pred)
           (= [:_eq] (vec (keys pred))))
    (:_eq pred)
    pred))

(defmethod op-compile-rewrite "get"
  [_ {:keys [selections args] :as result}]
  ;; Lift every root-level scalar predicate (`[{:args {…}}]` shape) out
  ;; of selections and into root args, collapsing `{:_eq v}` to bare
  ;; `v` along the way. `get` is primary-key-style — args are flat
  ;; key-value pairs, not predicate maps. The selection keeps the bare
  ;; field as `nil`. Pre-existing args (legacy `_args (…)` form, even
  ;; though lint rejects it) are also flattened so the whole `:args`
  ;; map is uniformly bare for the backend.
  (let [[sels' lifted]
        (reduce-kv
         (fn [[s a] k v]
           (if (root-scalar-args-entry? v)
             [(assoc s k nil) (assoc a k (get-arg-value (:args (first v))))]
             [(assoc s k v) a]))
         [{} {}]
         selections)
        merged-args (->> (merge (or args {}) lifted)
                         (reduce-kv (fn [m k v] (assoc m k (get-arg-value v)))
                                    {}))]
    (assoc result
           :selections sels'
           :args merged-args)))

;; ── mergeFiltersInto ─────────────────────────────────────────────────────

(defn- merge-filters-into
  "Merge scalar-filter contributions into an existing args map as AND.
   Meta-keys stay at the top level, not inside `_and`."
  [args filters]
  (if (empty? filters)
    args
    (let [base (or args {})]
      (cond
        ;; Single filter, no existing preds → return filter directly.
        (and (= 1 (count filters))
             (empty? base))
        (first filters)

        :else
        (let [meta     (into {} (filter (fn [[k _]] (meta-keys k)) base))
              pred-only (into {} (remove (fn [[k _]] (meta-keys k)) base))
              all-parts (cond->> (vec filters)
                          (seq pred-only) (cons pred-only)
                          true vec)]
          (if (= 1 (count all-parts))
            (merge meta (first all-parts))
            (merge meta {:_and all-parts})))))))

;; ── Top-level ───────────────────────────────────────────────────────────

(defn compile-query
  "Compile a :query AST node to `{:selections {…} :args {…}?}`.

   `op` is the wire op string (\"search\" / \"get\" / …) — used to apply
   op-specific rewrites. For `get`, every root-level scalar predicate is
   lifted out of field-level args and into root args; the selection
   keeps the bare field. See XSQL.md §`get` for rationale."
  ([query-node] (compile-query query-node nil))
  ([query-node op]
   (let [entity (some-> (:root-entity query-node) :text)
         ;; Root args in parens — `Movie (release_year >= 2000, _limit 10)`.
         ;; Compiled like a relation's arg-list → seeds the root :args.
         root-parens-args (when-let [p (:root-parens query-node)]
                            (when-let [al (ast/find-child p :arg-list)]
                              (compile-arg-list al)))
         out (atom {:selections {}})
         root-filters (atom [])]
     (doseq [c (:children query-node)]
       (case (:node c)
         :root-args
         ;; Root args may live inside a Parens (single-line form) or
         ;; directly under :root-args as an :arg-list (block form).
         (let [arg-list (or (some-> (ast/find-child c :parens)
                                    (ast/find-child :arg-list))
                            (ast/find-child c :arg-list))
               args-map (if arg-list (compile-arg-list arg-list) {})]
           (swap! out assoc :args args-map))

         :statement
         (let [{:keys [out' filters]}
               (compile-statement-into c (:selections @out) @root-filters)]
           (swap! out assoc :selections out')
           (reset! root-filters filters))

         ;; Skip blank-line / error / etc.
         nil))
     (let [filters @root-filters]
       (when (seq filters)
         (swap! out update :args merge-filters-into filters)))
     ;; Seed root :args from the root parens (merged with any block `_args`
     ;; during the additive phase).
     (when (seq root-parens-args)
       (swap! out update :args #(deep-merge-args (or % {}) root-parens-args)))
     (let [result (op-compile-rewrite op @out)
           {:keys [args]} result]
       (cond-> result
         (or (nil? args) (empty? args)) (dissoc :args)
         entity (assoc :entity entity))))))

;; ── Statement dispatch ──────────────────────────────────────────────────

(defn- compile-statement-into
  "Compile one Statement, returning `{:out' :filters}`."
  [stmt-node parent filter-acc]
  (let [inner (first (:children stmt-node))]
    (case (:node inner)
      :scalar
      (compile-scalar inner parent filter-acc)

      :relation
      {:out' (compile-relation inner parent) :filters filter-acc}

      :count-block
      {:out' (assoc parent :_count (compile-count-block inner)) :filters filter-acc}

      :agg-block
      {:out' (assoc parent :_agg (compile-agg-block inner)) :filters filter-acc}

      ;; Unknown / error — leave parent alone.
      {:out' parent :filters filter-acc})))

;; ── Scalar ──────────────────────────────────────────────────────────────

(defn- scalar-parts
  "Extract {:field :pred :filter} from a :scalar AST node."
  [scalar-node]
  (let [field-id (some (fn [c] (when (= :identifier (:node c)) c))
                       (:children scalar-node))
        pred (ast/find-child scalar-node :pred-op)
        filter (ast/find-child scalar-node :scalar-filter)]
    {:field  (when field-id (:text field-id))
     :pred   pred
     :filter filter}))

(defn- scalar-colon-error?
  "True if the scalar carries an :error node whose token text is `:`.
   Indicates the user tried to write a scalar alias (`display: name`)
   or got the relation-alias order wrong (`name: -roles`). Either way,
   the scalar shouldn't end up on the wire."
  [scalar-node]
  (boolean (some #(and (= :error (:node %)) (= ":" (:text %)))
                 (:children scalar-node))))

(defn- compile-scalar
  "Returns `{:out' :filters}` — out' is the new selections map and
   filters is the (possibly extended) filter-accumulator."
  [scalar-node parent filter-acc]
  (let [{:keys [field pred filter]} (scalar-parts scalar-node)]
    (if (or (nil? field) (scalar-colon-error? scalar-node))
      ;; No field name, or alias-style mistake — drop the scalar so the
      ;; wire stays clean. Lint surfaces the underlying error separately.
      {:out' parent :filters filter-acc}
      (let [key (keyword field)]
        (cond
          ;; ScalarFilter → lift compound predicates into enclosing
          ;; _where; selection stays bare since the filter is the
          ;; parent's concern, not the column projection's.
          filter
          (let [or-expr (first (:children filter))
                compiled (compile-scalar-or or-expr field)
                filter-acc' (conj filter-acc compiled)]
            {:out' (assoc parent key nil) :filters filter-acc'})

          ;; Bare scalar with no inline predicate.
          (nil? pred)
          {:out' (assoc parent key nil) :filters filter-acc}

          ;; Scalar with inline predicate. If the same field already carries
          ;; an inline-predicate selection (the top-level "block form" — one
          ;; predicate per line on the same field), AND-combine the args
          ;; rather than clobbering the earlier predicate.
          :else
          (let [new-args (compile-pred-op pred)
                existing (get parent key)
                prev     (when (and (vector? existing) (map? (:args (first existing))))
                           (:args (first existing)))
                args     (if prev (deep-merge-args prev new-args) new-args)]
            {:out' (assoc parent key [{:args args}])
             :filters filter-acc}))))))

;; ── ScalarFilter compilation ────────────────────────────────────────────

(defn- compile-scalar-or [or-node field]
  (let [ands (ast/find-children or-node :scalar-and-expr)]
    (if (= 1 (count ands))
      (compile-scalar-and (first ands) field)
      {:_or (mapv #(compile-scalar-and % field) ands)})))

(defn- compile-scalar-and [and-node field]
  (let [prims (ast/find-children and-node :scalar-prim)]
    (if (= 1 (count prims))
      (compile-scalar-prim (first prims) field)
      {:_and (mapv #(compile-scalar-prim % field) prims)})))

(defn- compile-scalar-prim [prim-node field]
  (let [pred-op (ast/find-child prim-node :pred-op)]
    (cond
      pred-op
      {(keyword field) (compile-pred-op pred-op)}

      ;; Grouped: parenthesised ScalarOrExpr (no PredOp child)
      :else
      (let [nested (ast/find-child prim-node :scalar-or-expr)]
        (if nested
          (compile-scalar-or nested field)
          {})))))

;; ── Relation ────────────────────────────────────────────────────────────

(defn- compile-relation [rel-node parent]
  (let [join-marker (ast/find-child rel-node :join-marker)
        is-left? (= :arrow (-> join-marker :children first :node))
        alias-node (ast/find-child rel-node :alias)
        ;; Field identifier: the :identifier that is NOT inside :alias
        ;; or :join-marker. Walk children and find the first naked identifier.
        field-id (some (fn [c]
                         (when (= :identifier (:node c)) c))
                       (:children rel-node))
        field (text-of field-id)
        parens (ast/find-child rel-node :parens)
        block (ast/find-child rel-node :block)
        alias (when alias-node (-> alias-node :children first :text))

        ;; Compile nested selections (and collect child filter contributions).
        [selections child-filters]
        (if block
          (loop [stmts (filter #(= :statement (:node %)) (:children block))
                 sel  {}
                 filters []]
            (if-let [s (first stmts)]
              (let [{:keys [out' filters]} (compile-statement-into s sel filters)]
                (recur (rest stmts) out' filters))
              [sel filters]))
          [nil []])

        ;; Args from parens (relation-level filters).
        args-raw (when parens (compile-args-of-parens parens))
        meta     (into {} (filter (fn [[k _]] (meta-keys k)) (or args-raw {})))
        preds    (into {} (remove (fn [[k _]] (meta-keys k)) (or args-raw {})))

        ;; AND-merge child scalar-filters into the relation's preds.
        preds (if (seq child-filters)
                (merge-filters-into preds child-filters)
                preds)
        preds (or preds {})
        ;; Strip meta-keys from merged preds (they stayed at top by merge).
        meta  (merge meta (into {} (filter (fn [[k _]] (meta-keys k)) preds)))
        preds (into {} (remove (fn [[k _]] (meta-keys k)) preds))

        ;; Args layout — predicates are flat at the args level
        ;; (alongside meta-keys), matching the root-args shape. No more
        ;; `:_where` wrapper. Distinguished by underscore prefix:
        ;;   :_join     — "left" for `->rel`, absent for `-rel`
        ;;   :_limit / :_offset / :_order_by / :_distinct — meta
        ;;   :_or / :_and — explicit predicate combinators
        ;;   anything else — field predicate (implicit AND between siblings)
        has-preds? (seq preds)
        has-meta?  (seq meta)
        args (when (or has-preds? has-meta? is-left?)
               (cond-> (merge meta preds)
                 ;; Arrow implies LEFT, but an explicit `(_join …)` arg wins.
                 (and is-left? (not (contains? meta :_join))) (assoc :_join "left")))

        entry (cond-> {}
                alias (assoc :alias alias)
                args  (assoc :args args)
                selections (assoc :selections selections))

        wrapped (if (empty? entry) {} entry)
        ;; Append to existing vector under this key — multiple aliased
        ;; entries for the same relation conjoin instead of overwriting.
        ;; Without this, `->aktivne:groups (...)` followed by
        ;; `->neaktivne:groups (...)` would lose the first entry.
        existing (get parent (keyword field) [])]
    (assoc parent (keyword field) (conj (vec existing) wrapped))))

;; ── _count ──────────────────────────────────────────────────────────────

(defn- compile-count-block
  "Wire shape for `_count`:

     [{:selections {<relation-name> [{:alias <a> :args <preds>} ...]}}]

   Children are grouped by *relation name* (the target after `alias:`,
   or the bare identifier when no alias). Each child becomes one entry
   in the relation's vector. A bare relation with no alias and no args
   collapses to `nil`, matching the backend's `(nil [nil])`
   short-circuit in `synthigy.dataset.sql.query/selection->schema`.

   Predicate args are emitted *flat* (same shape as `->rel` args); the
   backend's count path lifts them into a SELECT-side `case when`
   directly. No `:_where` envelope — that was a temporary indirection
   wired around the old `_where → _maybe` rename, which Aggregate Hoist
   makes obsolete."
  [count-node]
  (let [grouped
        (reduce
          (fn [acc child]
            (let [alias-node (ast/find-child child :alias)
                  field-id   (some (fn [c]
                                     (when (= :identifier (:node c)) c))
                                   (:children child))
                  field      (text-of field-id)
                  alias      (when alias-node (-> alias-node :children first :text))
                  parens     (ast/find-child child :parens)
                  args-raw   (when parens (compile-args-of-parens parens))
                  args       (when (seq args-raw) args-raw)
                  entry      (cond-> nil
                               alias (assoc :alias alias)
                               args  (assoc :args args))
                  rel-key    (keyword field)]
              (update acc rel-key (fnil conj []) entry)))
          {}
          (ast/find-children count-node :count-child))]
    [{:selections grouped}]))

;; ── _agg ────────────────────────────────────────────────────────────────

(defn- compile-agg-block
  "Wire shape for `_agg` (full `_count`-pattern parity):

     [{:selections {<relation-name>
                    [{:alias?     <a>
                      :args?      <flat-preds>
                      :selections {<attr> [{:selections {<fn> nil}} ...]}}
                     ...]}}]

   - **Envelope-per-child**: every entry sits in its own envelope under
     the relation key, so the backend can group entries that target the
     same relation and produce one JOIN with multiple aggregate columns
     (Aggregate Hoist).
   - **Alias / args at the relation level**: filter the source rows
     once per entry; aggregates are computed over that filtered set.
     Predicates land *flat* on `:args` (no `:_where`/`:_maybe` wrapper —
     those are going away).
   - **Per-attribute, each fn is its own entry**: required because the
     backend's `_agg` schema-build extracts the fn via
     `(ffirst (:selections data))` (`query.clj:1653`); packing multiple
     fns under one map silently drops all but the first."
  [agg-node]
  (let [children (ast/find-children agg-node :agg-relation)
        grouped
        (reduce
          (fn [acc rel]
            (let [alias-node (ast/find-child rel :alias)
                  alias      (when alias-node (-> alias-node :children first :text))
                  field-id   (some (fn [c]
                                     (when (= :identifier (:node c)) c))
                                   (:children rel))
                  rel-name   (text-of field-id)
                  parens     (ast/find-child rel :parens)
                  args-raw   (when parens (compile-args-of-parens parens))
                  args       (when (seq args-raw) args-raw)
                  attrs      (ast/find-children rel :agg-attr)
                  sel        (reduce
                               (fn [sel-acc attr]
                                 (let [attr-id (first (filter #(= :identifier (:node %))
                                                              (:children attr)))
                                       attr-name (text-of attr-id)
                                       fns (ast/find-children attr :agg-fn)
                                       fn-entries (mapv
                                                    (fn [f]
                                                      (let [fn-text (-> f :children first :text)]
                                                        {:selections {(keyword fn-text) nil}}))
                                                    fns)]
                                   (assoc sel-acc (keyword attr-name) fn-entries)))
                               {}
                               attrs)
                  entry      (cond-> {:selections sel}
                               alias (assoc :alias alias)
                               args  (assoc :args args))
                  rel-key    (keyword rel-name)]
              (update acc rel-key (fnil conj []) entry)))
          {}
          children)]
    [{:selections grouped}]))

;; ── Args inside Parens ──────────────────────────────────────────────────

(defn- compile-args-of-parens [parens-node]
  (when parens-node
    (if-let [list (ast/find-child parens-node :arg-list)]
      (compile-arg-list list)
      {})))

(defn- deep-merge-args
  "Fold sibling arg-statements (comma- or newline-separated, implicitly AND'd)
   into one args map. Predicates on the SAME field must AND-combine, not
   clobber: two operator maps merge their keys (`{:_ge 2008}` + `{:_le 2014}`
   → `{:_ge 2008 :_le 2014}`, the idiomatic /data where shape), and nested
   paths merge recursively. Plain `merge` did last-write-wins, silently
   dropping the first predicate (the implicit-AND clobber bug). Non-map
   collisions (same op twice, vectors) keep the latter — degenerate input."
  [a b]
  (merge-with (fn [x y] (if (and (map? x) (map? y)) (deep-merge-args x y) y)) a b))

(defn- compile-arg-list [list-node]
  (reduce
    (fn [acc stmt]
      (let [inner (first (:children stmt))]
        (case (:node inner)
          :or-expr   (deep-merge-args acc (compile-or-expr inner))
          :meta-key  (deep-merge-args acc (compile-meta-key inner))
          acc)))
    {}
    (ast/find-children list-node :arg-stmt)))

(defn- compile-or-expr [or-node]
  (let [ands (ast/find-children or-node :and-expr)]
    (if (= 1 (count ands))
      (compile-and-expr (first ands))
      {:_or (mapv compile-and-expr ands)})))

(defn- compile-and-expr [and-node]
  (let [prims (ast/find-children and-node :primary-expr)]
    (if (= 1 (count prims))
      (compile-primary-expr (first prims))
      {:_and (mapv compile-primary-expr prims)})))

(defn- compile-primary-expr [prim-node]
  (let [inner (first (:children prim-node))]
    (case (:node inner)
      :arg-predicate (compile-arg-predicate inner)
      :grouped-expr  (compile-or-expr (ast/find-child inner :or-expr))
      {})))

(defn- compile-arg-predicate [pred-node]
  (let [path (ast/find-child pred-node :path)
        pred-op (ast/find-child pred-node :pred-op)
        segs (mapv :text (filter #(= :identifier (:node %)) (:children path)))
        leaf (compile-pred-op pred-op)]
    ;; Walk segments right-to-left, wrapping.
    (reduce (fn [obj seg] {(keyword seg) obj})
            leaf
            (reverse segs))))

;; ── PredOp ──────────────────────────────────────────────────────────────

(def ^:private binop->wire
  {:eq  :_eq
   :neq :_neq
   :lt  :_lt
   :le  :_le
   :gt  :_gt
   :ge  :_ge})

(defn- compile-pred-op [pred-op-node]
  (let [children (:children pred-op-node)
        first-child (first children)
        first-text (:text first-child)
        second-text (some-> children second :text)]
    (cond
      ;; "is null" / "is not null" — strict sequence match.
      ;; Partial input (just `is`, or `is not` without `null`) falls
      ;; through to {} so we don't fabricate a wire predicate.
      (= "is" first-text)
      (let [texts (mapv :text children)]
        (cond
          (= ["is" "null"] texts)        "is_null"
          (= ["is" "not" "null"] texts)  "is_not_null"
          :else                          {}))

      ;; "not in (...)" — only emit _not_in when `in` was parsed
      ;; alongside `not`. Bare `not` yields {}.
      (= "not" first-text)
      (if (= "in" second-text)
        (let [list-node (ast/find-child pred-op-node :list-literal)]
          {:_not_in (compile-list-literal list-node)})
        {})

      ;; "in (...)"
      (= "in" first-text)
      (let [list-node (ast/find-child pred-op-node :list-literal)]
        {:_in (compile-list-literal list-node)})

      ;; like / ilike (String | ParamRef)
      (or (= "like" first-text) (= "ilike" first-text))
      (let [operand (some #(when (#{:string :param-ref} (:node %)) %) children)
            value (if (= :param-ref (:node operand))
                    (resolve-param-ref operand)
                    ;; Unquote: strip surrounding quotes; handle backslash escapes.
                    (-> operand :text (subs 1) (#(subs % 0 (dec (count %))))
                        (str/replace #"\\(.)" "$1")))]
        (if (= "like" first-text)
          {:_like value}
          {:_ilike value}))

      ;; BinaryOp Value
      :else
      (let [bin-op (ast/find-child pred-op-node :binary-op)
            value (ast/find-child pred-op-node :value)]
        (if bin-op
          (let [tok (-> bin-op :children first :node)
                wire-op (binop->wire tok :_eq)]
            {wire-op (compile-value value)})
          {})))))

(defn- compile-value [value-node]
  (let [inner (first (:children value-node))]
    (case (:node inner)
      :string
      ;; Strip quotes; handle escapes.
      (let [t (:text inner)
            stripped (subs t 1 (dec (count t)))]
        (str/replace stripped #"\\(.)" "$1"))

      :number
      (let [t (:text inner)]
        (if (str/includes? t ".")
          #?(:clj (Double/parseDouble t)
             :cljs (js/parseFloat t))
          #?(:clj (Long/parseLong t)
             :cljs (js/parseInt t 10))))

      :list-literal
      (compile-list-literal inner)

      :identifier
      (case (:text inner)
        "true"  true
        "false" false
        "null"  nil
        (:text inner))         ; bare enum value

      :param-ref
      (resolve-param-ref inner)

      ;; Fallback: unknown
      nil)))

(defn- compile-list-literal [list-node]
  ;; Most entries map 1:1 to a value. The exception is an array-typed
  ;; named-parameter inside a list — `status in (?statuses:string[])` —
  ;; which splices its array into the list position so a single param
  ;; can bind a whole `_in` set. Without the splice the wire would
  ;; carry `{:_in [["a" "b"]]}` (an array inside a list), which neither
  ;; sql.query nor JDBC interprets correctly.
  (reduce
    (fn [acc v-node]
      (let [inner (first (:children v-node))]
        (if (and (= :param-ref (:node inner))
                 (:array? inner))
          (let [v (resolve-param-ref inner)]
            (cond
              (nil? v)          (conj acc nil)
              (sequential? v)   (into acc v)
              :else             (conj acc v)))
          (conj acc (compile-value v-node)))))
    []
    (ast/find-children list-node :value)))

;; ── MetaKey ─────────────────────────────────────────────────────────────

(defn- compile-meta-key [meta-node]
  (let [children (:children meta-node)
        kw (-> children first :text)]
    (case kw
      "_limit"
      (let [v-node (some #(when (#{:number :param-ref} (:node %)) %) children)
            v (case (:node v-node)
                :number    #?(:clj (Long/parseLong (:text v-node))
                              :cljs (js/parseInt (:text v-node) 10))
                :param-ref (resolve-param-ref v-node)
                nil)]
        {:_limit v})

      "_offset"
      (let [v-node (some #(when (#{:number :param-ref} (:node %)) %) children)
            v (case (:node v-node)
                :number    #?(:clj (Long/parseLong (:text v-node))
                              :cljs (js/parseInt (:text v-node) 10))
                :param-ref (resolve-param-ref v-node)
                nil)]
        {:_offset v})

      "_order_by"
      (let [specs (ast/find-children meta-node :order-spec)]
        {:_order_by
         (mapv (fn [spec]
                 (let [id (some #(when (= :identifier (:node %)) %)
                                (:children spec))
                       dir (last (filter #(and (= :identifier (:node %))
                                               (#{"asc" "desc"} (:text %)))
                                         (:children spec)))]
                   [(:text id) (:text dir)]))
               specs)})

      "_distinct"
      (let [ids (filter #(and (= :identifier (:node %))
                              (not= "_distinct" (:text %)))
                        children)]
        {:_distinct {:attributes (mapv :text ids)}})

      "_join"
      (let [v (some #(when (and (= :identifier (:node %)) (not= "_join" (:text %))) %)
                    children)]
        {:_join (:text v)})

      {})))

;; ── Public entry ────────────────────────────────────────────────────────

(defn compile
  "Compile an XSQL source string to wire JSON.
   Returns `{:selections {…} :args {…}?}` matching XSQL.md § Wire.

   `op` is the wire op string (\"search\" / \"get\" / …). Only `get`
   currently triggers any op-specific behavior (root scalar lifting).

   `params` is an optional `{name value}` map (keyword or string keys)
   used to resolve `?name:type[]` placeholders in the source. When
   omitted, placeholders compile to nil — useful for syntax-only paths
   like the linter and editor tooling. When supplied, missing required
   params or type mismatches throw an ex-info with a `:code` of
   \"PARAM_MISSING\" or \"PARAM_TYPE_MISMATCH\"."
  ([source]
   (compile source nil nil))
  ([source op]
   (compile source op nil))
  ([source op params]
   (binding [*params* params]
     (compile-query (parser/parse source) op))))
