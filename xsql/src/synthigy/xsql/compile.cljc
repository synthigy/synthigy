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

(ns synthigy.xsql.compile
  "AST → wire JSON for the XSQL query DSL."
  (:refer-clojure :exclude [compile])
  (:require [clojure.string :as str]
            [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]
            [synthigy.xsql.sql-params :as sql-params]))

(declare compile-ast
         compile-statement-into compile-scalar compile-relation
         compile-count-block compile-agg-block compile-args-of-parens
         compile-arg-list compile-arg-stmt compile-or-expr compile-and-expr
         compile-primary-expr compile-arg-predicate compile-pred-op
         compile-value compile-list-literal compile-meta-key
         compile-scalar-or compile-scalar-and compile-scalar-prim
         throw-optional-in-or
         merge-filters-into deep-merge-args)

(def ^:private meta-keys #{:_limit :_offset :_order_by :_distinct :_join :_on})

(defn text-of [leaf] (:text leaf))

;; ── Named parameters — resolution during compile ────────────────────────

(def ^:dynamic *params* nil)

(def ^:private absent
  "Sentinel for a `?name?` optional param whose value wasn't supplied — drops the enclosing predicate; distinct from {} and nil."
  ::absent)

(defn absent? [v] (= absent v))

(defn param-type-kw
  [param-ref-node]
  (let [raw (:param-type-raw param-ref-node)]
    (or (when raw
          (get sql-params/type-aliases (str/lower-case raw)))
        :string)))

(defn lookup-param
  "Resolve a :param-ref leaf's value from bound params, inline default, or the
   absent sentinel."
  [param-ref-node]
  (let [nm (:param-name param-ref-node)]
    (cond
      (and *params* (contains? *params* (keyword nm))) (get *params* (keyword nm))
      (and *params* (contains? *params* nm))           (get *params* nm)
      (:param-default param-ref-node)
      (sql-params/coerce-default (:param-default param-ref-node) (param-type-kw param-ref-node))
      (nil? *params*) nil
      (:optional? param-ref-node) absent
      :else
      (throw (ex-info (str "Missing value for parameter ?" nm)
                      {:code "PARAM_MISSING"
                       :param nm
                       :span (:span param-ref-node)})))))

(defn resolve-param-ref
  "Resolve a :param-ref leaf to its bound literal value, type-checking against
   the declared type."
  [param-ref-node]
  (let [type   (param-type-kw param-ref-node)
        array? (:array? param-ref-node)
        v      (lookup-param param-ref-node)
        err    (when (and (some? *params*) (not (absent? v)))
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

(defmulti op-compile-rewrite
  "Rewrite the compiled result map for op-specific semantics; dispatches on the
   wire op string."
  (fn [op _result] (or op :default)))

(defmethod op-compile-rewrite :default [_ result] result)

(defn root-scalar-args-entry?
  "Detect the `[{:args {…}}]` wire shape compile-scalar emits for inline scalar
   predicates."
  [v]
  (and (vector? v)
       (= 1 (count v))
       (let [e (first v)]
         (and (map? e)
              (= #{:args} (set (keys e)))
              (map? (:args e))))))

(defn get-arg-value
  "Collapse a `{:_eq v}` predicate map to the bare value `get` expects."
  [pred]
  (if (and (map? pred)
           (= [:_eq] (vec (keys pred))))
    (:_eq pred)
    pred))

(defmethod op-compile-rewrite "get"
  [_ {:keys [selections args] :as result}]
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

(defn merge-filters-into
  "Merge scalar-filter contributions into an existing args map as AND."
  [args filters]
  (if (empty? filters)
    args
    (let [base (or args {})]
      (cond
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
  "Compile a :query AST node to `{:selections {…} :args {…}?}`."
  ([query-node] (compile-query query-node nil))
  ([query-node op]
   (let [entity (some-> (:root-entity query-node) :text)
         root-parens-args (when-let [p (:root-parens query-node)]
                            (when-let [al (ast/find-child p :arg-list)]
                              (compile-arg-list al)))
         out (atom {:selections {}})
         root-filters (atom [])]
     (doseq [c (:children query-node)]
       (case (:node c)
         :statement
         (let [{:keys [out' filters]}
               (compile-statement-into c (:selections @out) @root-filters)]
           (swap! out assoc :selections out')
           (reset! root-filters filters))

         nil))
     (let [filters @root-filters]
       (when (seq filters)
         (swap! out update :args merge-filters-into filters)))
     (when (seq root-parens-args)
       (swap! out update :args #(deep-merge-args (or % {}) root-parens-args)))
     (let [result (op-compile-rewrite op @out)
           {:keys [args]} result]
       (cond-> result
         (or (nil? args) (empty? args)) (dissoc :args)
         entity (assoc :entity entity))))))

;; ── Statement dispatch ──────────────────────────────────────────────────

(defn compile-statement-into
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

      {:out' parent :filters filter-acc})))

;; ── Scalar ──────────────────────────────────────────────────────────────

(defn scalar-parts
  [scalar-node]
  (let [field-id (some (fn [c] (when (= :identifier (:node c)) c))
                       (:children scalar-node))
        pred (ast/find-child scalar-node :pred-op)
        filter (ast/find-child scalar-node :scalar-filter)]
    {:field  (when field-id (:text field-id))
     :pred   pred
     :filter filter}))

(defn scalar-colon-error?
  "True when the scalar carries a `:` error node (attempted scalar alias) and
   must stay off the wire."
  [scalar-node]
  (boolean (some #(and (= :error (:node %)) (= ":" (:text %)))
                 (:children scalar-node))))

(defn compile-scalar
  "Returns `{:out' :filters}` — updated selections plus the filter accumulator."
  [scalar-node parent filter-acc]
  (let [{:keys [field pred filter]} (scalar-parts scalar-node)]
    (if (or (nil? field) (scalar-colon-error? scalar-node))
      {:out' parent :filters filter-acc}
      (let [key (keyword field)]
        (cond
          filter
          (let [or-expr (first (:children filter))
                compiled (compile-scalar-or or-expr field)
                filter-acc' (if (absent? compiled)
                              filter-acc
                              (conj filter-acc compiled))]
            {:out' (assoc parent key nil) :filters filter-acc'})

          (nil? pred)
          {:out' (assoc parent key nil) :filters filter-acc}

          :else
          (let [new-args (compile-pred-op pred)
                existing (get parent key)
                prev     (when (and (vector? existing) (map? (:args (first existing))))
                           (:args (first existing)))]
            (if (absent? new-args)
              {:out' (if prev parent (assoc parent key nil))
               :filters filter-acc}
              (let [args (if prev (deep-merge-args prev new-args) new-args)]
                {:out' (assoc parent key [{:args args}])
                 :filters filter-acc}))))))))

;; ── ScalarFilter compilation ────────────────────────────────────────────

(defn compile-scalar-or [or-node field]
  (let [ands (ast/find-children or-node :scalar-and-expr)]
    (if (= 1 (count ands))
      (compile-scalar-and (first ands) field)
      (let [ms (mapv #(compile-scalar-and % field) ands)]
        (if (some absent? ms)
          (throw-optional-in-or or-node)
          {:_or ms})))))

(defn compile-scalar-and [and-node field]
  (let [prims (ast/find-children and-node :scalar-prim)]
    (if (= 1 (count prims))
      (compile-scalar-prim (first prims) field)
      (let [ms (remove absent? (mapv #(compile-scalar-prim % field) prims))]
        (case (count ms)
          0 absent
          1 (first ms)
          {:_and (vec ms)})))))

(defn compile-scalar-prim [prim-node field]
  (let [pred-op (ast/find-child prim-node :pred-op)]
    (cond
      pred-op
      (let [m (compile-pred-op pred-op)]
        (if (absent? m) absent {(keyword field) m}))

      :else
      (let [nested (ast/find-child prim-node :scalar-or-expr)]
        (if nested
          (compile-scalar-or nested field)
          {})))))

;; ── Relation ────────────────────────────────────────────────────────────

(defn compile-relation [rel-node parent]
  (let [join-marker (ast/find-child rel-node :join-marker)
        is-left? (= :arrow (-> join-marker :children first :node))
        alias-node (ast/find-child rel-node :alias)
        field-id (some (fn [c]
                         (when (= :identifier (:node c)) c))
                       (:children rel-node))
        field (text-of field-id)
        parens (ast/find-child rel-node :parens)
        block (ast/find-child rel-node :block)
        alias (when alias-node (-> alias-node :children first :text))

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

        args-raw (when parens (compile-args-of-parens parens))
        meta     (into {} (filter (fn [[k _]] (meta-keys k)) (or args-raw {})))
        preds    (into {} (remove (fn [[k _]] (meta-keys k)) (or args-raw {})))

        preds (if (seq child-filters)
                (merge-filters-into preds child-filters)
                preds)
        preds (or preds {})
        meta  (merge meta (into {} (filter (fn [[k _]] (meta-keys k)) preds)))
        preds (into {} (remove (fn [[k _]] (meta-keys k)) preds))

        args (cond-> (merge meta preds)
               ;; Sigil implies the join; an explicit `(_join …)` arg wins.
               (not (contains? meta :_join))
               (assoc :_join (if is-left? "left" "inner")))

        entry (cond-> {}
                alias (assoc :alias alias)
                args  (assoc :args args)
                selections (assoc :selections selections))

        wrapped (if (empty? entry) {} entry)
        existing (get parent (keyword field) [])]
    (if (and (empty? preds)
             (nil? selections)
             (not is-left?)
             parens
             (some #(and (= :param-ref (:node %)) (:optional? %)
                         (absent? (lookup-param %)))
                   (tree-seq :children :children parens)))
      ;; Every predicate evaporated via absent optional params — drop the JOIN
      ;; too.
      parent
      (assoc parent (keyword field) (conj (vec existing) wrapped)))))

;; ── _count ──────────────────────────────────────────────────────────────

(defn compile-count-block
  "Wire shape for `_count` — children grouped by relation name."
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

(defn compile-agg-block
  "Wire shape for `_agg` — envelope-per-child; each aggregate fn is its own
   entry."
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

(defn compile-args-of-parens [parens-node]
  (when parens-node
    (if-let [list (ast/find-child parens-node :arg-list)]
      (compile-arg-list list)
      {})))

(defn deep-merge-args
  "AND-fold sibling arg maps — same-field operator maps merge keys; colliding
   combinators restructure instead of clobbering."
  [a b]
  (reduce-kv
    (fn [m k v]
      (if-not (contains? m k)
        (assoc m k v)
        (let [x (get m k)]
          (cond
            (= k :_and)
            (assoc m k (into (vec x) v))

            (= k :_or)
            (-> m
                (dissoc :_or)
                (update :_and (fnil into []) [{:_or x} {:_or v}]))

            (and (map? x) (map? v))
            (assoc m k (deep-merge-args x v))

            :else (assoc m k v)))))
    a b))

(defn compile-arg-list [list-node]
  (reduce
    (fn [acc stmt]
      (let [inner (first (:children stmt))]
        (case (:node inner)
          :or-expr   (let [m (compile-or-expr inner)]
                       (if (absent? m) acc (deep-merge-args acc m)))
          :meta-key  (let [m (into {} (remove (comp absent? val))
                                   (compile-meta-key inner))]
                       (deep-merge-args acc m))
          acc)))
    {}
    (ast/find-children list-node :arg-stmt)))

(defn throw-optional-in-or
  "Absent optional under `or` is an error — dropping a disjunct silently narrows
   the result."
  [node]
  (throw (ex-info "optional `?name?` param under `or` — dropping a disjunct is ambiguous; restructure or supply the value"
                  {:code "PARAM_OPTIONAL_IN_OR" :span (:span node)})))

(defn compile-or-expr [or-node]
  (let [ands (ast/find-children or-node :and-expr)]
    (if (= 1 (count ands))
      (compile-and-expr (first ands))
      (let [ms (mapv compile-and-expr ands)]
        (if (some absent? ms)
          (throw-optional-in-or or-node)
          {:_or ms})))))

(defn compile-and-expr [and-node]
  (let [prims (ast/find-children and-node :primary-expr)]
    (if (= 1 (count prims))
      (compile-primary-expr (first prims))
      (let [ms (remove absent? (mapv compile-primary-expr prims))]
        (case (count ms)
          0 absent
          1 (first ms)
          {:_and (vec ms)})))))

(defn compile-primary-expr [prim-node]
  (let [inner (first (:children prim-node))]
    (case (:node inner)
      :arg-predicate (compile-arg-predicate inner)
      :grouped-expr  (compile-or-expr (ast/find-child inner :or-expr))
      {})))

(defn compile-arg-predicate [pred-node]
  (let [path (ast/find-child pred-node :path)
        pred-op (ast/find-child pred-node :pred-op)
        segs (mapv :text (filter #(= :identifier (:node %)) (:children path)))
        leaf (compile-pred-op pred-op)]
    (if (absent? leaf)
      absent
      (reduce (fn [obj seg] {(keyword seg) obj})
              leaf
              (reverse segs)))))

;; ── PredOp ──────────────────────────────────────────────────────────────

(def ^:private binop->wire
  {:eq  :_eq
   :neq :_neq
   :lt  :_lt
   :le  :_le
   :gt  :_gt
   :ge  :_ge})

(defn compile-pred-op [pred-op-node]
  (let [children (:children pred-op-node)
        first-child (first children)
        first-text (:text first-child)
        second-text (some-> children second :text)]
    (cond
      ;; "is null" / "is not null" — partial input falls through to {}.
      (= "is" first-text)
      (let [texts (mapv :text children)]
        (cond
          (= ["is" "null"] texts)        "is_null"
          (= ["is" "not" "null"] texts)  "is_not_null"
          :else                          {}))

      ;; "not in (...)" — bare `not` yields {}.
      (= "not" first-text)
      (if (= "in" second-text)
        (let [list-node (ast/find-child pred-op-node :list-literal)
              vs (compile-list-literal list-node)]
          (if (absent? vs) absent {:_not_in vs}))
        {})

      ;; "in (...)"
      (= "in" first-text)
      (let [list-node (ast/find-child pred-op-node :list-literal)
            vs (compile-list-literal list-node)]
        (if (absent? vs) absent {:_in vs}))

      ;; like / ilike (String | ParamRef)
      (or (= "like" first-text) (= "ilike" first-text))
      (let [operand (some #(when (#{:string :param-ref} (:node %)) %) children)
            value (if (= :param-ref (:node operand))
                    (resolve-param-ref operand)
                    (-> operand :text (subs 1) (#(subs % 0 (dec (count %))))
                        (str/replace #"\\(.)" "$1")))]
        (cond
          (absent? value)       absent
          (= "like" first-text) {:_like value}
          :else                 {:_ilike value}))

      ;; BinaryOp Value
      :else
      (let [bin-op (ast/find-child pred-op-node :binary-op)
            value (ast/find-child pred-op-node :value)]
        (if bin-op
          (let [tok (-> bin-op :children first :node)
                wire-op (binop->wire tok :_eq)
                v (compile-value value)]
            (if (absent? v) absent {wire-op v}))
          {})))))

(defn compile-value [value-node]
  (let [inner (first (:children value-node))]
    (case (:node inner)
      :string
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

      nil)))

(defn compile-list-literal [list-node]
  ;; Array-typed params splice into the list position.
  (let [{:keys [vals dropped?]}
        (reduce
          (fn [acc v-node]
            (let [inner (first (:children v-node))]
              (if (and (= :param-ref (:node inner))
                       (:array? inner))
                (let [v (resolve-param-ref inner)]
                  (cond
                    (absent? v)     (assoc acc :dropped? true)
                    (nil? v)        (update acc :vals conj nil)
                    (sequential? v) (update acc :vals into v)
                    :else           (update acc :vals conj v)))
                (let [v (compile-value v-node)]
                  (if (absent? v)
                    (assoc acc :dropped? true)
                    (update acc :vals conj v))))))
          {:vals [] :dropped? false}
          (ast/find-children list-node :value))]
    ;; A list emptied by absent optionals takes the whole predicate with it —
    ;; never fabricate `_in []`.
    (if (and dropped? (empty? vals)) absent vals)))

;; ── MetaKey ─────────────────────────────────────────────────────────────

(defn compile-meta-key [meta-node]
  (let [children (:children meta-node)
        kw (parser/normalize-meta-kw (-> children first :text))]
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
      (if-let [pref (some #(when (= :param-ref (:node %)) %)
                          (:children meta-node))]
        (let [v (lookup-param pref)]
          (if (or (nil? v) (absent? v))
            {}
            (let [pairs (sql-params/normalize-order-specs
                          v {:name (:param-name pref)
                             :span (:span pref)
                             :type-args (:param-type-args pref)})]
              {:_order_by
               (if (seq (:param-type-args pref))
                 pairs
                 (with-meta pairs
                   {:xsql/scope-to-selection {:param (:param-name pref)
                                              :span (:span pref)}}))})))
        (let [specs (ast/find-children meta-node :order-spec)]
          {:_order_by
           (mapv (fn [spec]
                   (let [id (some #(when (= :identifier (:node %)) %)
                                  (:children spec))
                         dir (last (filter #(and (= :identifier (:node %))
                                                 (#{"asc" "desc"} (:text %)))
                                           (:children spec)))]
                     [(:text id) (:text dir)]))
                 specs)}))

      "_distinct"
      (let [ids (filter #(= :identifier (:node %)) (rest children))]
        {:_distinct {:attributes (mapv :text ids)}})

      "_join"
      (let [v (some #(when (= :identifier (:node %)) %) (rest children))]
        {:_join (:text v)})

      "_on"
      (let [v (some #(when (= :identifier (:node %)) %) (rest children))]
        {:_on (:text v)})

      {})))

;; ── Selection-scoped order params (post-pass) ───────────────────────────

(defn selection-scalars [selections]
  (set (keep (fn [[k v]] (when (nil? v) (name k))) selections)))

(defn check-order-scope [{:keys [args] :as node} selections]
  (when-let [info (some-> (:_order_by args) meta :xsql/scope-to-selection)]
    (let [allowed (selection-scalars selections)]
      (doseq [[col _] (:_order_by args)]
        (when-not (allowed col)
          (throw (ex-info
                  (str "Parameter ?" (:param info) " orders by \"" col
                       "\" which is not in the selection — select the column "
                       "or widen with ?" (:param info) "(" col ", …)")
                  {:code "PARAM_TYPE_MISMATCH"
                   :param (:param info)
                   :column col
                   :allowed (vec (sort allowed))
                   :span (:span info)}))))))
  node)

(defn enforce-order-scoping
  "Apply selection-scoping to tagged order params across root and relation
   configs."
  [{:keys [selections] :as result}]
  (check-order-scope result selections)
  (letfn [(walk-sel [sel]
            (doseq [[_ v] sel]
              (when (vector? v)
                (doseq [config v]
                  (when (map? config)
                    (check-order-scope config (:selections config))
                    (walk-sel (:selections config)))))))]
    (when (map? selections)
      (walk-sel selections)))
  result)

;; ── Public entry ────────────────────────────────────────────────────────

(defn compile
  "Compile an XSQL source string to wire JSON."
  ([source]
   (compile source nil nil))
  ([source op]
   (compile source op nil))
  ([source op params]
   (compile-ast (parser/parse source) op params)))

(defn compile-ast
  "Compile a pre-parsed AST to wire JSON — the cache-friendly entry."
  [ast op params]
  (binding [*params* params]
    (enforce-order-scoping (compile-query ast op))))
