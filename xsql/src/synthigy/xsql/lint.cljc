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

(ns synthigy.xsql.lint
  "Schema-aware linter, producing `[{:severity :from :to :message} …]`."
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
    :count-block :count-child :agg-block :agg-relation})

(defn error-diag [node msg]
  {:severity :error
   :from (first (:span node))
   :to   (second (:span node))
   :message msg})

(defn warning-diag [node msg]
  {:severity :warning
   :from (first (:span node))
   :to   (second (:span node))
   :message msg})

(defn entity-name-of
  "Reverse lookup of entity name from an entity-def, for error messages."
  [entity-def schema]
  (or (some (fn [[name def]] (when (= def entity-def) name))
            (:entities schema))
      "entity"))

;; ── Syntax error coalescing ─────────────────────────────────────────────

(defn last-content-offset
  "Offset just past the last non-whitespace char — errors past here are
   'user hasn't finished typing yet' and get suppressed."
  [^String source]
  (let [n (count source)]
    (loop [i (dec n)]
      (cond
        (< i 0) 0
        (re-matches #"\s" (subs source i (inc i))) (recur (dec i))
        :else (inc i)))))

(defn slice-nearby [source from to max]
  (let [n (count source)
        end (min (if (> to from) to (+ from max)) n)
        nl (str/index-of source "\n" from)
        end (if (and nl (< nl end)) nl end)
        out (str/trim (subs source from end))]
    (when (pos? (count out)) out)))

(defn describe-syntax-error
  "Heuristic message for a parser :error node, mirroring lint.js."
  [err-node ^String source parent]
  (let [from (first (:span err-node))
        to   (second (:span err-node))
        snippet (slice-nearby source from to 24)
        leading (when (< from (count source)) (subs source from (inc from)))]
    (cond
      (= "(" leading)
      "Inside `attr(…)` write operator-first predicates joined with and/or: `xid(= \"a\" or = \"b\")`, `priority(>= 3 and <= 9)`."

      (= :parens (:node parent))
      "Missing `)`"

      (= :block (:node parent))
      "Empty indented block — add a child or dedent."

      :else
      (if snippet
        (str "Syntax error near `" snippet "`")
        "Syntax error"))))

(defn collect-syntax-errors
  "Walk the AST, emitting one diagnostic per error-run (coalesced)."
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

(defn lint-statement [stmt-node entity-def ctx]
  (let [inner (first (:children stmt-node))]
    (case (:node inner)
      :scalar      (lint-scalar inner entity-def ctx)
      :relation    (lint-relation inner entity-def ctx)
      :count-block (lint-count-block inner entity-def ctx)
      :agg-block   (lint-agg-block inner entity-def ctx)
      nil)))

;; ── Scalar ──────────────────────────────────────────────────────────────

(defn get-bare-identifiers
  "`:identifier` children of `parent` not nested in
   :alias/:join-marker/:pred-op."
  [parent]
  (filterv #(= :identifier (:node %)) (:children parent)))

(defn lint-scalar [scalar-node entity-def ctx]
  (let [ids (get-bare-identifiers scalar-node)]
    (when (seq ids)
      (let [field-id (first ids)
            field-name (:text field-id)
            attr-def (get-in entity-def [:attributes field-name])
            rel-def  (get-in entity-def [:relations field-name])
            ;; A misplaced `:` after a valid field means the user meant a
            ;; relation alias in the wrong order (`name: -roles`) or a
            ;; scalar alias (unsupported).
            colon-err (some #(and (= :error (:node %))
                                  (= ":" (:text %)) %)
                            (:children scalar-node))]
        (cond
          ;; `_args` was removed from the grammar — checked before schema
          ;; lookups so the message helps even without a loaded schema.
          (= "_args" field-name)
          (vswap! (:diags ctx) conj
                  (error-diag field-id
                              (str "`_args` was removed — put args in parens on the "
                                   "root or relation line: `entity (…)` / `-rel (…)`. "
                                   "Parens may span multiple lines.")))

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

(defn has-statement-child? [block]
  (boolean (some #(= :statement (:node %)) (:children block))))

(defn lint-redundant-alias
  "Warn when `alias:rel` equals the relation name — bare `rel` is identical."
  [node ctx]
  (when-let [alias-node (ast/find-child node :alias)]
    (let [alias-id (first (filter #(= :identifier (:node %)) (:children alias-node)))
          rel-id   (first (get-bare-identifiers node))]
      (when (and alias-id rel-id (= (:text alias-id) (:text rel-id)))
        (vswap! (:diags ctx) conj
                (warning-diag alias-id
                              (str "Redundant alias — '" (:text alias-id) ":"
                                   (:text rel-id) "' is the same as bare '"
                                   (:text rel-id) "'")))))))

(defn lint-relation [rel-node entity-def ctx]
  (lint-redundant-alias rel-node ctx)
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

(defn lint-count-block [count-node entity-def ctx]
  (doseq [child (ast/find-children count-node :count-child)]
    (lint-redundant-alias child ctx)
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

(defn lint-agg-block [agg-node entity-def ctx]
  (doseq [agg-rel (ast/find-children agg-node :agg-relation)]
    (lint-redundant-alias agg-rel ctx)
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

(defn lint-order-restriction-set
  "Validate a `?name:order(cols…)` restriction set: each column must be
   a real scalar attribute of `entity-def` (schema-dependent, so it runs
   inside the entity-tracking walk rather than the schema-blind pass)."
  [param-ref-node entity-def ctx]
  (doseq [col (:param-type-args param-ref-node)
          :when (not (contains? (:attributes entity-def) col))]
    (vswap! (:diags ctx) conj
            (error-diag param-ref-node
                        (str "Unknown order column '" col "' on "
                             (entity-name-of entity-def (:schema ctx)))))))

(defn lint-meta-key [meta-node entity-def ctx]
  (when (and entity-def (= "order" (some-> meta-node :children first :text)))
    (when-let [pref (first (filter #(= :param-ref (:node %)) (:children meta-node)))]
      (lint-order-restriction-set pref entity-def ctx))))

(defn lint-args-of-parens [parens-node entity-def ctx]
  (when-let [list (ast/find-child parens-node :arg-list)]
    (doseq [stmt (ast/find-children list :arg-stmt)]
      (let [inner (first (:children stmt))]
        (case (:node inner)
          :or-expr  (lint-or-expr inner entity-def ctx)
          :meta-key (lint-meta-key inner entity-def ctx)
          nil)))))

(defn lint-or-expr [or-node entity-def ctx]
  (doseq [a (ast/find-children or-node :and-expr)]
    (doseq [p (ast/find-children a :primary-expr)]
      (let [inner (first (:children p))]
        (case (:node inner)
          :arg-predicate (lint-arg-predicate inner entity-def ctx)
          :grouped-expr  (when-let [nested (ast/find-child inner :or-expr)]
                           (lint-or-expr nested entity-def ctx))
          nil)))))

(defn lint-arg-predicate [pred-node entity-def ctx]
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

(defn classify-pred-op
  "[kind op-key] for a :pred-op node, or [nil nil] for incomplete input
   (e.g. `is`/`not` mid-edit) via strict sequence matching."
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

(defn lint-pred-op-against-type
  "Check operator/type compatibility and empty-list cases."
  [pred-op-node attr-def field-name ctx]
  (let [[kind op-key] (classify-pred-op pred-op-node)]
    (when op-key
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
;; Per-op rules layer on top of the universal syntax/schema baseline.
;; Contract: `[op tree ctx]`, mutates `(:diags ctx)`.

(defmulti op-lint-rules
  "Apply op-specific lint rules. Dispatches on the wire op string;
   `:default` is a no-op."
  (fn [op _tree _ctx] (or op :default)))

(defmethod op-lint-rules :default [_ _ _] nil)

(defn lint-get-root-scalar
  "Get-mode root-scalar check: bare projection or `field = value` on a
   unique attr is valid; other operators, and non-`=` compound parens
   forms, are errors."
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

          ;; Only flag non-unique `=` when the schema carries :unique
          ;; info for at least one attr (avoids false positives where
          ;; uniqueness isn't propagated yet).
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

(defn attr-identity?
  "True when `field-name` `=`-matched is a valid get identity: the
   attribute is unique (or the schema carries no unique info at all)."
  [field-name pred-op entity-def]
  (let [attr-def (when (and entity-def field-name)
                   (get-in entity-def [:attributes field-name]))]
    (when (and pred-op attr-def)
      (let [[_ op-key] (classify-pred-op pred-op)]
        (and (= "_eq" op-key)
             (or (:unique attr-def)
                 (not (some (fn [[_ a]] (:unique a)) (:attributes entity-def)))))))))

(defn root-parens-has-identity?
  "True if the root parens carry a `=` predicate on a unique attribute."
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

(defn root-statement-has-identity?
  "True if any root :statement carries a `=` predicate on a unique attribute."
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
                             (not (some (fn [[_ a]] (:unique a))
                                        (:attributes entity-def))))))))))))
      (:children tree))))

(defmethod op-lint-rules "get"
  [_ tree ctx]
  (let [entity-def (:root-entity-def ctx)]
    (doseq [c (:children tree)]
      (case (:node c)
        :statement
        (let [inner (first (:children c))]
          (when (= :scalar (:node inner))
            (lint-get-root-scalar inner entity-def ctx)))

        nil))
    ;; get is row-identity, not "search and take first" — without a root
    ;; `=` on a unique attr the server falls through to first-by-_eid.
    (when (and entity-def
               (seq (:children tree))
               (not (root-statement-has-identity? tree entity-def))
               (not (root-parens-has-identity? tree entity-def)))
      (vswap! (:diags ctx) conj
              (assoc (error-diag tree
                                 "`get` requires identity. Add at least one root predicate like `xid = \"…\"` on a unique-constrained attribute.")
                     :from 0
                     :to   (max 1 (count (:source ctx ""))))))))

;; ── Tree ops (@search-tree / @get-tree) ─────────────────────────────────

(def ^:private tree-ops #{"search-tree" "get-tree"})

(defn on-meta-node?
  [n]
  (and (= :meta-key (:node n))
       (= "_on" (some-> (first (:children n)) :text))))

(defn root-on-nodes
  "All `_on` :meta-key nodes inside the root parens."
  [tree]
  (if-let [p (:root-parens tree)]
    (filterv on-meta-node? (tree-seq :children :children p))
    []))

(defn on-relation-id
  "The relation identifier of an `_on` meta-key node."
  [on-node]
  (some #(when (and (= :identifier (:node %)) (not= "_on" (:text %))) %)
        (:children on-node)))

(defn lint-on-placement
  "`_on` is only valid in the root parens of a tree op."
  [op tree ctx]
  (let [root-spans (into #{} (map :span) (root-on-nodes tree))
        all-on     (concat (filter on-meta-node? (tree-seq :children :children tree))
                           (root-on-nodes tree))]
    (doseq [n all-on]
      (cond
        (not (contains? root-spans (:span n)))
        (vswap! (:diags ctx) conj
                (error-diag n "`_on` belongs in the ROOT parens — it names the relation a tree op recurses over"))

        (and op (not (tree-ops op)))
        (vswap! (:diags ctx) conj
                (error-diag n (str "`_on` is only valid on @search-tree / @get-tree — not @" op)))))))

(defn lint-tree-on
  "Shared @search-tree/@get-tree rule: exactly one `_on <rel>` in the root
   parens, naming a self-relation of the root entity."
  [op tree ctx]
  (let [entity-def (:root-entity-def ctx)
        ons (root-on-nodes tree)]
    (cond
      (empty? ons)
      (vswap! (:diags ctx) conj
              {:severity :error
               :from 0 :to (max 1 (count (:source ctx "")))
               :message (str "`" op "` requires `_on <relation>` in the root parens — "
                             "the self-relation to recurse over.")})

      (> (count ons) 1)
      (vswap! (:diags ctx) conj
              (error-diag (second ons) "Only one `_on` per tree op"))

      :else
      (when entity-def
        (let [n         (first ons)
              rel-id    (on-relation-id n)
              rel-name  (:text rel-id)
              rel-def   (get-in entity-def [:relations rel-name])
              root-name (entity-name-of entity-def (:schema ctx))]
          (cond
            (nil? rel-def)
            (vswap! (:diags ctx) conj
                    (error-diag (or rel-id n)
                                (str "Unknown relation '" rel-name "' on " root-name)))

            (not= (:target rel-def) root-name)
            (vswap! (:diags ctx) conj
                    (error-diag (or rel-id n)
                                (str "`_on " rel-name "` must be a SELF-relation of "
                                     root-name " — it targets " (:target rel-def))))))))))

(defn root-parens-id-identity?
  "True if the root parens carry `xid = …` (or `euuid = …`) — the ID
   field, since a unique business attr isn't enough for @get-tree."
  [tree]
  (boolean
    (when-let [parens (:root-parens tree)]
      (some (fn [n]
              (when (= :arg-predicate (:node n))
                (let [path  (ast/find-child n :path)
                      field (some #(when (= :identifier (:node %)) (:text %))
                                  (:children path))
                      pred  (ast/find-child n :pred-op)]
                  (and (#{"xid" "euuid"} field)
                       pred
                       (= "_eq" (second (classify-pred-op pred)))))))
            (tree-seq :children :children parens)))))

(defmethod op-lint-rules "search-tree"
  [op tree ctx]
  (lint-tree-on op tree ctx))

(defmethod op-lint-rules "get-tree"
  [op tree ctx]
  (lint-tree-on op tree ctx)
  (when-not (root-parens-id-identity? tree)
    (vswap! (:diags ctx) conj
            {:severity :error
             :from 0 :to (max 1 (count (:source ctx "")))
             :message "`get-tree` requires the tree root's identity in the root parens: `xid = …`"})))

;; ── Named-parameter validation (schema-independent) ────────────────────
;; Bare `?`/`?N` are rejected at the tokenizer; this walk catches what
;; parsed fine but still needs flagging (unknown type, misplaced :order).

(defn order-default-diags
  "Validate an :order param's inline `=\"col dir, …\"` default (mirrors
   compile's normalize-order-specs; kept textual here so lint stays
   throw-free)."
  [n]
  (when-let [d (:param-default n)]
    (let [raw     (str/trim d)
          s       (if (and (str/starts-with? raw "\"")
                           (str/ends-with? raw "\"")
                           (> (count raw) 1))
                    (subs raw 1 (dec (count raw)))
                    raw)
          allowed (not-empty (set (:param-type-args n)))
          pieces  (remove str/blank? (map str/trim (str/split s #",")))]
      (if (empty? pieces)
        [(error-diag n "An `:order` default must name at least one column")]
        (vec (keep
              (fn [p]
                (let [[col dir & extra] (str/split p #"\s+")]
                  (cond
                    (seq extra)
                    (error-diag n (str "Malformed order spec \"" p "\" in default"))
                    (not (re-matches #"[a-z_][a-z0-9_]*" (or col "")))
                    (error-diag n (str "Invalid order column \"" col "\" in default"))
                    (and dir (not (#{"asc" "desc"} dir)))
                    (error-diag n (str "Invalid direction \"" dir "\" in default (asc|desc)"))
                    (and allowed (not (allowed col)))
                    (error-diag n (str "Default orders by \"" col
                                       "\" — outside its restriction set ("
                                       (str/join ", " (sort allowed)) ")")))))
              pieces))))))

(defn collect-param-ref-errors
  [ast]
  (let [diags (volatile! [])]
    (letfn [(walk [n in-order-by?]
              (when (= :param-ref (:node n))
                (let [raw    (some-> (:param-type-raw n) str/lower-case)
                      ;; Type is inferred from position: an untyped param
                      ;; in `order by` IS an order param.
                      order? (or (= "order" raw) (and in-order-by? (nil? raw)))]
                  (cond
                    (and raw (not= "order" raw)
                         (not (contains? sql-params/type-aliases raw)))
                    (vswap! diags conj
                            (error-diag n
                                        (str "Unknown parameter type `:" raw
                                             "`. Expected one of: "
                                             (str/join ", "
                                                       (sort (conj (set (keys sql-params/type-aliases))
                                                                   "order"))))))

                    (and (= "order" raw) (not in-order-by?))
                    (vswap! diags conj
                            (error-diag n "The `:order` parameter type is only valid in `order by` position"))

                    (and in-order-by? raw (not= "order" raw))
                    (vswap! diags conj
                            (error-diag n (str "An order-by parameter's type is inferred — drop `:"
                                               raw "` (or declare `:order`)")))

                    order?
                    (run! #(vswap! diags conj %) (order-default-diags n)))
                  (when (and (:param-type-args n) (not order?))
                    (vswap! diags conj
                            (error-diag n "A restriction set `( … )` is only valid on order parameters")))))
              (when (ast/container? n)
                (let [order-meta? (and (= :meta-key (:node n))
                                       (#{"_order_by" "order"}
                                        (some-> n :children first :text)))]
                  (run! #(walk % order-meta?) (:children n)))))]
      (walk ast false))
    @diags))

;; ── Legacy meta-key spellings ──────────────────────────────────────────
;;
;; The bare SQL spellings (`limit`, `order by`, `distinct`, `join`, `on`)
;; are canonical; the `_`-prefixed wire forms still parse. Nudge, don't
;; break — migration per repo policy is codemod + escalating lint.

(def ^:private legacy-meta-spelling
  {"_limit" "limit" "_offset" "offset" "_order_by" "order by"
   "_distinct" "distinct" "_join" "join" "_on" "on"})

(defn collect-legacy-meta-key-warnings
  [ast]
  (let [diags (volatile! [])]
    (letfn [(walk [n]
              (when (= :meta-key (:node n))
                (let [kw (some-> n :children first)]
                  (when-let [bare (legacy-meta-spelling (:text kw))]
                    (vswap! diags conj
                            (assoc (warning-diag kw
                                                 (str "`" (:text kw) "` is the wire spelling — prefer `"
                                                      bare "`"))
                                   :severity :info)))))
              (when (ast/container? n)
                (run! walk (:children n))))]
      (walk ast))
    @diags))

;; ── Duplicate-sibling warnings ─────────────────────────────────────────
;; XSQL has no scalar aliases and bare relations share a response key,
;; so duplicates at the same scope (query root or any block) are warned
;; on second-and-later occurrence; aliased relations are skipped.

(defn statement-bare-name
  "If `stmt-node` is a bare scalar or bare relation/count-child, return
   `[:scalar|:relation identifier-node]`, else nil."
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

(defn lint-duplicate-siblings
  "Warn on each duplicate bare attribute/relation at a scope; the first
   occurrence is unflagged."
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
  "Lint an XSQL source against a schema and root entity, returning a
   vector of diagnostic maps. `op` is the wire op string (\"search\" /
   \"get\" / …); `\"get\"` additionally requires root identity."
  ([source]
   (lint source nil nil nil))
  ([source schema root-entity]
   (lint source schema root-entity nil))
  ([source schema root-entity op]
   (let [tree     (parser/parse source)
         ;; Query-root entity wins over the (legacy) external arg.
         root-tok (:root-entity tree)
         root     (or (some-> root-tok :text) root-entity)
         ;; Root parens hang off :query under :root-parens, not
         ;; :children — collectors must be pointed at them explicitly.
         rp       (:root-parens tree)
         diags    (volatile! (vec
                              (concat (collect-syntax-errors tree source)
                                      (when rp (collect-syntax-errors rp source))
                                      (collect-param-ref-errors tree)
                                      (when rp (collect-param-ref-errors rp))
                                      (collect-legacy-meta-key-warnings tree)
                                      (when rp (collect-legacy-meta-key-warnings rp)))))
         root-def (when (and schema root)
                    (get-in schema [:entities root]))
         ctx      {:source source
                   :schema schema
                   :root-entity-def root-def
                   :diags diags}]
     (when (and schema root-tok (not root-def))
       (vswap! diags conj
               (error-diag root-tok
                           (str "Unknown entity '" root "' — not found in the deployed model."))))
     (when root-def
       (when-let [p (:root-parens tree)]
         (lint-args-of-parens p root-def ctx))
       (doseq [c (:children tree)]
         (case (:node c)
           :statement
           (lint-statement c root-def ctx)

           nil)))
     ;; Duplicate detection is schema-independent — runs without root-def.
     (lint-duplicate-siblings tree ctx)
     (lint-on-placement op tree ctx)
     (op-lint-rules op tree ctx)
     @diags)))
