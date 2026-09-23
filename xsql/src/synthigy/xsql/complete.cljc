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

(ns synthigy.xsql.complete
  "Context-aware autocompletion engine for XSQL."
  (:require [clojure.string :as str]
            [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]))

;; ── Constants ────────────────────────────────────────────────────────────

(def ^:private meta-keys ["limit" "offset" "order by" "distinct" "join"])
(def ^:private agg-fns ["avg" "sum" "min" "max"])

(def ^:private ops-by-type
  {"string"    ["=" "!=" "<" "<=" ">" ">=" "like" "ilike" "in" "not in" "is" "is not null"]
   "number"    ["=" "!=" "<" "<=" ">" ">=" "in" "not in" "is"]
   "boolean"   ["=" "!=" "is"]
   "timestamp" ["=" "!=" "<" "<=" ">" ">=" "in" "not in" "is"]
   "enum"      ["=" "!=" "in" "not in" "is"]})

;; ── Lexical helpers ──────────────────────────────────────────────────────

(defn char-at [^String s i]
  (when (and (>= i 0) (< i (count s)))
    (subs s i (inc i))))

(defn ws-or-tab? [c] (or (= " " c) (= "\t" c)))
(defn ident-char? [c]
  (and c (re-matches #"[A-Za-z0-9_]" c)))
(defn ident-letter? [c]
  (and c (re-matches #"[A-Za-z_]" c)))

(defn prev-non-whitespace-char [^String source offset]
  (loop [i (dec offset)]
    (cond
      (< i 0) nil
      (let [c (char-at source i)]
        (or (= c " ") (= c "\t") (= c "\n")))
      (recur (dec i))
      :else (char-at source i))))

(defn marker-before-current-word
  "Return `:arrow` when `->` immediately precedes the current word, `:dash` for
   `-`, else nil."
  [^String source from]
  (let [from (if (= ":" (char-at source (dec from)))
               ;; step back over `alias:` to the alias's own start
               (loop [i (- from 2)]
                 (if (ident-char? (char-at source i))
                   (recur (dec i))
                   (inc i)))
               from)]
    (cond
      (and (>= from 2)
           (= ">" (char-at source (dec from)))
           (= "-" (char-at source (- from 2))))
      :arrow

      (and (>= from 1)
           (= "-" (char-at source (dec from))))
      :dash

      :else nil)))

(defn prev-word-at [^String source offset]
  (let [n (count source)]
    (loop [i (dec offset)]
      (cond
        (< i 0) nil
        (ws-or-tab? (char-at source i)) (recur (dec i))
        :else
        (let [end (inc i)]
          (loop [i i]
            (if (and (>= i 0) (ident-letter? (char-at source i)))
              (recur (dec i))
              (let [start (inc i)]
                (when (< start end)
                  (subs source start end))))))))))

(defn prev-word-start [^String source offset]
  (loop [i (dec offset)]
    (cond
      (< i 0) 0
      (ws-or-tab? (char-at source i)) (recur (dec i))
      :else
      (loop [j i]
        (if (and (>= j 0) (ident-letter? (char-at source j)))
          (recur (dec j))
          (inc j))))))

(defn prev-word-before [^String source offset-before]
  (loop [i (dec offset-before)]
    (cond
      (< i 0) nil
      (ws-or-tab? (char-at source i)) (recur (dec i))
      :else
      (let [end (inc i)]
        (loop [i i]
          (if (and (>= i 0) (ident-letter? (char-at source i)))
            (recur (dec i))
            (let [start (inc i)]
              (when (< start end)
                (subs source start end)))))))))

(defn current-line-indent-text [^String source offset]
  (let [start (loop [s offset]
                (if (and (pos? s) (not= "\n" (char-at source (dec s))))
                  (recur (dec s)) s))
        n (count source)]
    (loop [i start]
      (if (and (< i n) (ws-or-tab? (char-at source i)))
        (recur (inc i))
        (- i start)))))

(defn line-start-indent [^String source pos]
  (current-line-indent-text source pos))

(defn line-start-offset
  "Offset of the first character of the line containing `offset`."
  [^String source offset]
  (loop [s offset]
    (if (and (pos? s) (not= "\n" (char-at source (dec s))))
      (recur (dec s)) s)))

(defn current-word-range [^String source offset]
  (let [n (count source)
        start (loop [s offset]
                (if (and (pos? s) (ident-char? (char-at source (dec s))))
                  (recur (dec s)) s))
        end (loop [e offset]
              (if (and (< e n) (ident-char? (char-at source e)))
                (recur (inc e)) e))]
    {:from start :to end}))

;; ── AST ancestor resolution ──────────────────────────────────────────────

(defn offset-in-span? [node offset]
  (let [[from to] (:span node)]
    (and (>= offset from) (<= offset to))))

(defn innermost-with-ancestors
  "Find the innermost AST node whose span contains `offset`, returning `[node
   ancestors]`."
  [root offset source]
  (let [descend (fn [start anc0]
                  (loop [n start anc anc0]
                    (if-let [child (and (ast/container? n)
                                        (reduce (fn [best c]
                                                  (if (offset-in-span? c offset)
                                                    (let [[from to] (:span c)
                                                          zero? (= from to)]
                                                      (cond
                                                        (nil? best)                          c
                                                        zero?                                best
                                                        (> from (first (:span best)))        c
                                                        :else                                best))
                                                    best))
                                                nil
                                                (:children n)))]
                      (recur child (conj anc n))
                      [n anc])))
        rp (:root-parens root)]
    (if (and rp (let [[from to] (:span rp)
                      closed? (= ")" (char-at source (dec to)))]
                  (and (> offset from)
                       (or (not closed?) (< offset to)))))
      (descend rp [root])
      (descend root []))))

(defn enclosing-of-tag
  [node ancestors tag]
  (or (when (= tag (:node node)) node)
      (some #(when (= tag (:node %)) %) (reverse ancestors))))

;; ── Indent-based relation context ────────────────────────────────────────

(defn relation-name-of
  [rel-node]
  (let [seen-alias (atom false)]
    (some (fn [c]
            (cond
              (= :alias (:node c)) (do (reset! seen-alias true) nil)
              (= :identifier (:node c)) (:text c)
              :else nil))
          (:children rel-node))))

(defn collect-relations-before
  "Collect relation nodes starting before `offset` as `{:name :indent :from
   :kind}` entries."
  [root source offset]
  (let [acc (volatile! [])
        rel-tags #{:relation :count-child :agg-relation}]
    (letfn [(walk [n]
              (when (ast/container? n)
                (when (and (rel-tags (:node n))
                           (< (first (:span n)) offset))
                  (vswap! acc conj
                          {:name   (relation-name-of n)
                           :indent (line-start-indent source (first (:span n)))
                           :from   (first (:span n))
                           :kind   (:node n)}))
                (run! walk (:children n))))]
      (walk root))
    @acc))

(defn enclosing-relation-target-by-indent
  "Resolve the entity context for a line at `child-indent` from the innermost
   preceding relation at smaller indent."
  [root source offset child-indent schema root-entity]
  (let [root-def (get-in schema [:entities root-entity])]
    (when root-def
      (let [rels (collect-relations-before root source offset)
            targets
            (loop [i 0
                   acc []]
              (if (>= i (count rels))
                acc
                (let [r (nth rels i)
                      parent-entity
                      (or (loop [j (dec i)]
                            (cond
                              (< j 0) nil
                              (let [p (nth rels j)]
                                (and (< (:indent p) (:indent r))
                                     (nth acc j)))
                              (nth acc j)
                              :else (recur (dec j))))
                          root-def)
                      rel-def (get-in parent-entity [:relations (:name r)])
                      target (when rel-def
                               (get-in schema [:entities (:target rel-def)]))]
                  (recur (inc i) (conj acc target)))))]
        (loop [i (dec (count rels))]
          (cond
            (< i 0) nil
            (and (< (:indent (nth rels i)) child-indent)
                 (nth targets i))
            (nth targets i)
            :else (recur (dec i))))))))

(defn enclosing-relation-kind-by-indent
  "Kind of the innermost preceding relation at strictly smaller indent, or nil."
  [root source offset child-indent schema root-entity]
  (when (get-in schema [:entities root-entity])
    (let [rels (collect-relations-before root source offset)]
      (loop [i (dec (count rels))]
        (cond
          (< i 0) nil
          (< (:indent (nth rels i)) child-indent) (:kind (nth rels i))
          :else (recur (dec i)))))))

;; ── Schema walks ─────────────────────────────────────────────────────────

(defn resolve-attr-from-path-node
  "Walk a :path AST node against the current entity, returning the final
   attr-def or nil."
  [path-node current-entity schema]
  (when current-entity
    (let [segs (filterv #(= :identifier (:node %)) (:children path-node))]
      (loop [i 0 current current-entity]
        (if (>= i (count segs))
          nil
          (let [name (:text (nth segs i))
                attr (get-in current [:attributes name])
                rel  (get-in current [:relations name])
                last? (= i (dec (count segs)))]
            (cond
              last? attr
              rel  (let [target (get-in schema [:entities (:target rel)])]
                     (if target (recur (inc i) target) nil))
              :else nil)))))))

(defn resolved-path-target
  "Parse the dotted path ending at offset and return its target entity."
  [^String source offset current-entity schema]
  (when (and current-entity schema)
    (let [start (loop [i offset]
                  (if (and (pos? i) (= "." (char-at source (dec i))))
                    (dec i) i))
          chars (loop [i start acc []]
                  (let [c (char-at source (dec i))]
                    (if (and (pos? i)
                             (or (re-matches #"[A-Za-z0-9_.]" c)))
                      (recur (dec i) (cons c acc))
                      acc)))
          path-text (apply str chars)
          segs (filter seq (str/split path-text #"\."))]
      (loop [segs segs current current-entity]
        (cond
          (empty? segs) current
          :else
          (let [name (first segs)
                rel (get-in current [:relations name])
                target (when rel (get-in schema [:entities (:target rel)]))]
            (if target
              (recur (rest segs) target)
              nil)))))))

;; ── Context detection ────────────────────────────────────────────────────

(defn block-body-indent
  "Column of a :block's first body line, via its leading :indent token."
  [block-node source]
  (when block-node
    (when-let [indent-tok (ast/find-child block-node :indent)]
      (current-line-indent-text source (first (:span indent-tok))))))

(defn agg-body-indent
  "Column of an :agg-relation body's first line (no :block wrapper)."
  [agg-node source]
  (when-let [indent-tok (ast/find-child agg-node :indent)]
    (current-line-indent-text source (first (:span indent-tok)))))

(defn entity-stack-from-ancestors
  "Push relation-target entities for ancestors whose block/parens contain
   offset; agg-relation bodies (no :block) are detected via their :indent
   child. Cursor-column hint (see docs): a body-indent past the cursor's
   column treats the relation as logically closed, so completion doesn't
   stay stuck inside a trailing blank-line block."
  [ancestors offset source schema entity-stack]
  (let [cursor-col (current-line-indent-text source offset)
        outdented? (fn [body-indent]
                     (and body-indent (< cursor-col body-indent)))]
    (reduce
      (fn [stack node]
        (cond
          (#{:relation :count-child} (:node node))
          (let [rel-name (relation-name-of node)
                parent   (peek stack)
                rel-def  (get-in parent [:relations rel-name])
                target   (when rel-def (get-in schema [:entities (:target rel-def)]))
                block    (ast/find-child node :block)
                parens   (ast/find-child node :parens)
                in-block?  (and block  (offset-in-span? block  offset))
                in-parens? (and parens (offset-in-span? parens offset))
                body-ind   (block-body-indent block source)]
            (if (and target
                     (or in-block? in-parens?)
                     (not (and in-block? (outdented? body-ind))))
              (conj stack target)
              stack))

          (= :agg-relation (:node node))
          (let [rel-name (relation-name-of node)
                parent   (peek stack)
                rel-def  (get-in parent [:relations rel-name])
                target   (when rel-def (get-in schema [:entities (:target rel-def)]))
                ind      (ast/find-child node :indent)
                in-body? (and ind (> offset (first (:span ind))))
                body-ind (agg-body-indent node source)]
            (if (and target in-body? (not (outdented? body-ind)))
              (conj stack target)
              stack))

          :else stack))
      entity-stack
      ancestors)))

(defn attr-def-for-value-position
  "Attr-def of the field being predicated when the cursor is right after
   a binary op (`=`, `!=`, `<`, …), for value-position completion."
  [resolved ancestors current-entity schema]
  (let [pred-op (enclosing-of-tag resolved ancestors :pred-op)]
    (when (and pred-op
               (some #(= :binary-op (:node %)) (:children pred-op)))
      (or
        (when-let [arg-pred (enclosing-of-tag resolved ancestors :arg-predicate)]
          (when-let [path (ast/find-child arg-pred :path)]
            (resolve-attr-from-path-node path current-entity schema)))

        (when-let [scalar (enclosing-of-tag resolved ancestors :scalar)]
          (let [field-id (some #(when (= :identifier (:node %)) %)
                               (:children scalar))]
            (when field-id
              (get-in current-entity [:attributes (:text field-id)]))))))))

(defn scope-parent-node
  "AST node whose direct children are the sibling statements at the
   cursor's scope: root `:query` at column 0, else the innermost
   ancestor `:block` whose body indent is ≤ the cursor column."
  [tree ancestors source offset]
  (let [cursor-col (current-line-indent-text source offset)
        block-anc  (->> ancestors
                        (filter #(= :block (:node %)))
                        (filter (fn [b]
                                  (when-let [bi (block-body-indent b source)]
                                    (<= bi cursor-col))))
                        last)]
    (or block-anc tree)))

(defn sibling-bare-names
  "Bare attr/relation identifiers already used at this scope, as
   `{:attrs #{names} :rels #{names}}` — see docs for why duplicates
   matter and how the in-progress statement is excluded."
  [scope-parent source offset]
  (let [attrs (volatile! #{})
        rels  (volatile! #{})
        ls    (line-start-offset source offset)
        le    (loop [i ls]
                (if (and (< i (count source)) (not= "\n" (char-at source i)))
                  (recur (inc i)) i))
        blank-cursor-line? (str/blank? (subs source ls le))]
    (letfn [(stmt-contains-cursor? [s]
              (let [[from to] (:span s)]
                (and (>= offset from) (< offset to)
                     (not blank-cursor-line?))))
            (walk-stmt [s]
              (when-not (stmt-contains-cursor? s)
                (doseq [c (:children s)]
                  (cond
                    (= :scalar (:node c))
                    (when-let [id (some #(when (= :identifier (:node %)) %)
                                        (:children c))]
                      (vswap! attrs conj (:text id)))

                    (#{:relation :count-child} (:node c))
                    (when-not (some #(= :alias (:node %)) (:children c))
                      (when-let [id (some #(when (= :identifier (:node %)) %)
                                          (:children c))]
                        (vswap! rels conj (:text id))))))))]
      (run! walk-stmt
            (filter #(= :statement (:node %))
                    (:children scope-parent))))
    {:attrs @attrs :rels @rels}))

(defn unaliased-child-names
  "Bare (unaliased) child names of a `_count`/`_agg` body — its
   :count-child/:agg-relation nodes sit directly under the block, so
   `sibling-bare-names` never sees them."
  [block-node child-tag offset]
  (into #{}
        (keep (fn [c]
                (let [[from to] (:span c)]
                  (when (and (= child-tag (:node c))
                             (not (and (>= offset from) (< offset to)))
                             (not (some #(= :alias (:node %)) (:children c))))
                    (some #(when (= :identifier (:node %)) (:text %))
                          (:children c))))))
        (:children block-node)))

(defn agg-attr-names-of
  "Attr names already aggregated in an :agg-relation body (`value: avg`),
   excluding the line the cursor is on (the one being typed)."
  [agg-rel-node offset]
  (into #{}
        (keep (fn [c]
                (let [[from to] (:span c)]
                  (when (and (= :agg-attr (:node c))
                             (not (and (>= offset from) (< offset to))))
                    (some #(when (= :identifier (:node %)) (:text %))
                          (:children c))))))
        (:children agg-rel-node)))

(defn agg-fn-names-of
  "Fn names already picked on an :agg-attr line (`value: avg, sum`)."
  [agg-attr-node]
  (into #{}
        (keep (fn [c]
                (when (= :agg-fn (:node c))
                  (some #(when (= :identifier (:node %)) (:text %))
                        (:children c)))))
        (:children agg-attr-node)))

(defn last-leaf-of
  "Rightmost leaf of a node's subtree."
  [n]
  (if-let [cs (seq (:children n))]
    (recur (last cs))
    n))

(defn open-paren-depth
  "Unclosed-paren depth before `offset` (string-literal parens count too)."
  [^String source offset]
  (loop [i 0 d 0]
    (if (>= i offset)
      d
      (let [c (char-at source i)]
        (recur (inc i) (cond (= c "(") (inc d)
                             (= c ")") (max 0 (dec d))
                             :else d))))))

(defn current-arg-text
  "Text from the nearest `,` / `(` boundary before `offset` up to it."
  [^String source offset]
  (loop [i (dec offset)]
    (if (< i 0)
      (subs source 0 offset)
      (let [c (char-at source i)]
        (if (or (= c ",") (= c "("))
          (subs source (inc i) offset)
          (recur (dec i)))))))

(defn innermost-open-paren
  "Position of the innermost unmatched `(` before `offset`, or nil."
  [^String source offset]
  (loop [i 0 stack []]
    (if (>= i offset)
      (peek stack)
      (let [c (char-at source i)]
        (recur (inc i) (cond (= c "(") (conj stack i)
                             (= c ")") (if (seq stack) (pop stack) stack)
                             :else stack))))))

(defn complete-predicate-stmt?
  "True when an :arg-stmt ends in a literal value or a null test — a
   meta-key stmt (`_limit 100`) or a half-typed predicate is not."
  [stmt]
  (and (nil? (ast/find-child stmt :meta-key))
       (let [leaf (last-leaf-of stmt)]
         (or (#{:number :string :param-ref} (:node leaf))
             (#{"null" "true" "false"} (:text leaf))))))

(defn context-at
  [tree resolved ancestors source offset schema root-entity]
  (let [base-stack (cond-> []
                     (and schema root-entity (get-in schema [:entities root-entity]))
                     (conj (get-in schema [:entities root-entity])))
        entity-stack (entity-stack-from-ancestors ancestors offset source schema base-stack)
        current-entity (peek entity-stack)

        prev-word (prev-word-at source offset)
        prev-char (prev-non-whitespace-char source offset)
        word-from (:from (current-word-range source offset))
        marker-before (marker-before-current-word source word-from)
        scope-parent (scope-parent-node tree ancestors source offset)
        siblings (sibling-bare-names scope-parent source offset)
        value-attr (attr-def-for-value-position
                     resolved ancestors current-entity schema)
        ;; Closing quote ends a string value definitively (unlike a
        ;; number, where more digits may follow) — already the joiner
        ;; slot, not a value-edit position.
        after-closed-string? (and (= :string (:node resolved))
                                  (= offset (second (:span resolved)))
                                  (let [t (:text resolved)]
                                    (and (> (count t) 1)
                                         (= "\"" (subs t (dec (count t)))))))]
    (cond
      (and value-attr (not after-closed-string?))
      {:kind :value-suggestion
       :entity-stack entity-stack
       :attr-def value-attr}

      after-closed-string?
      {:kind :after-predicate-value :entity-stack entity-stack}

      ;; Value-position keyword tails.
      (= prev-word "is")
      {:kind :after-is :entity-stack entity-stack}

      (= prev-word "not")
      (let [before-not (prev-word-before source (prev-word-start source offset))]
        (if (= before-not "is")
          {:kind :after-is-not :entity-stack entity-stack}
          {:kind :after-not :entity-stack entity-stack}))

      (or (= prev-word "like") (= prev-word "ilike"))
      {:kind :after-like :entity-stack entity-stack}

      (= prev-word "in")
      {:kind :after-in :entity-stack entity-stack}

      ;; `_on |` — the tree-op recursion slot: self-relations only.
      (= prev-word "_on")
      {:kind :on-relation :entity-stack entity-stack}

      :else
      ;; Look at AST context.
      (let [parens (enclosing-of-tag resolved ancestors :parens)
            in-args? (boolean
                       (or parens
                           (enclosing-of-tag resolved ancestors :arg-list)))
            arg-pred (enclosing-of-tag resolved ancestors :arg-predicate)
            path-node (or (and arg-pred (ast/find-child arg-pred :path))
                          (enclosing-of-tag resolved ancestors :path))
            attr-def-after-path (when (and path-node
                                            (>= offset (second (:span path-node)))
                                            current-entity)
                                  (resolve-attr-from-path-node
                                    path-node current-entity schema))
            path-target-entity (when (= prev-char ".")
                                 (resolved-path-target
                                   source offset current-entity schema))
            ;; Attribute of the current entity right before the cursor
            ;; with >=1 space between (same line). nil while still
            ;; typing the identifier or if it isn't a known attribute.
            inline-attr-after-space
            (let [i (loop [i (dec offset)]
                      (if (and (>= i 0) (ws-or-tab? (char-at source i)))
                        (recur (dec i)) i))]
              (when (and (< i (dec offset))
                         (>= i 0)
                         (ident-char? (char-at source i)))
                (let [end (inc i)
                      start (loop [j i]
                              (if (and (>= j 0) (ident-char? (char-at source j)))
                                (recur (dec j)) (inc j)))]
                  (get-in current-entity [:attributes (subs source start end)]))))
            ;; A COMPLETE predicate right before the cursor: the joiner
            ;; slot (`and`/`or` continue the arg-stmt). Walks all
            ;; :arg-predicate nodes deep, so grouping parens take joiners too.
            joiner-stmt (when (and in-args?
                                    (not= prev-char "(") (not= prev-char ","))
                          (when-let [al (or (enclosing-of-tag resolved ancestors :arg-list)
                                            (some-> parens (ast/find-child :arg-list)))]
                            (let [preds (volatile! [])]
                              (letfn [(walk [n]
                                        (when (map? n)
                                          (when (= :arg-predicate (:node n))
                                            (vswap! preds conj n))
                                          (run! walk (:children n))))]
                                (walk al))
                              (->> @preds
                                   (filter (fn [p]
                                             (let [e (second (:span p))]
                                               (and (<= e offset)
                                                    (str/blank? (subs source e (min offset (count source))))))))
                                   last))))]
        (cond
          in-args?
          (cond
            (or (= prev-word "asc") (= prev-word "desc"))
            {:kind :arg-list-start :entity-stack entity-stack}

            (or (= prev-char "(") (= prev-char ","))
            {:kind :arg-list-start :entity-stack entity-stack}

            attr-def-after-path
            {:kind :after-attr-in-pred
             :entity-stack entity-stack
             :attr-def attr-def-after-path}

            path-target-entity
            {:kind :path-continuation
             :entity-stack entity-stack
             :path-target-entity path-target-entity}

            (and joiner-stmt (complete-predicate-stmt? joiner-stmt))
            {:kind :after-predicate-value :entity-stack entity-stack}

            :else
            {:kind :arg-list-start :entity-stack entity-stack})

          ;; Unterminated relation parens: span-based resolution can't
          ;; see the cursor is still inside, so detect the joiner slot
          ;; lexically instead (unclosed depth + arg segment ends in a
          ;; literal value, not a `_meta` key).
          (and (pos? (open-paren-depth source offset))
               (let [seg (str/trim (current-arg-text source offset))]
                 (and (seq seg)
                      (not (str/starts-with? seg "_"))
                      (re-find #"(\d|\"|\bnull|\btrue|\bfalse)$" seg))))
          {:kind :after-predicate-value :entity-stack entity-stack}

          ;; Inline scalar predicate with a trailing space (`xid != "a" |`):
          ;; the parens-free joiner slot, only when the tail is complete.
          (when-let [sc (enclosing-of-tag resolved ancestors :scalar)]
            (when-let [tail (or (ast/find-child sc :scalar-filter)
                                (ast/find-child sc :pred-op))]
              (let [e (second (:span tail))]
                (and (<= e offset)
                     (str/blank? (subs source e (min offset (count source))))
                     (complete-predicate-stmt? tail)))))
          {:kind :after-predicate-value :entity-stack entity-stack}

          ;; Scalar-filter parens (`title(…)`) are a different node
          ;; family from :parens/:arg-list — needs its own branch, or
          ;; the body-statement fallthrough would wrongly offer
          ;; attributes/relations/_count here. Operator-first grammar:
          ;; after `(` or a joiner the slot is an operator; after a
          ;; binary op it's a value.
          (or (enclosing-of-tag resolved ancestors :scalar-filter)
              (when (pos? (open-paren-depth source offset))
                (when-let [op-pos (innermost-open-paren source offset)]
                  (when-let [owner (prev-word-at source op-pos)]
                    (get-in current-entity [:attributes owner])))))
          (let [scalar (or (when (= :scalar (:node resolved)) resolved)
                           (some #(when (= :scalar (:node %)) %) (reverse ancestors)))
                attr-name (or (some #(when (= :identifier (:node %)) (:text %))
                                    (:children scalar))
                              (some->> (innermost-open-paren source offset)
                                       (prev-word-at source)))
                attr-def (get-in current-entity [:attributes attr-name])
                before-word-char (prev-non-whitespace-char source word-from)]
            (if (#{"=" "<" ">" "!"} before-word-char)
              {:kind :value-suggestion :entity-stack entity-stack :attr-def attr-def}
              {:kind :after-attr-in-pred :entity-stack entity-stack :attr-def attr-def}))

          ;; AggAttr: cursor before the (silently-consumed) colon is the
          ;; attr-name slot, at/past it is the fn slot; no identifier
          ;; parsed yet also means attr-name.
          (enclosing-of-tag resolved ancestors :agg-attr)
          (let [agg-attr-node (enclosing-of-tag resolved ancestors :agg-attr)
                attr-id       (ast/find-child agg-attr-node :identifier)
                in-attr-name? (or (nil? attr-id)
                                  (< offset (second (:span attr-id))))]
            (if in-attr-name?
              {:kind :agg-attr-name :entity-stack entity-stack
               :used-attrs (if-let [ar (enclosing-of-tag resolved ancestors :agg-relation)]
                             (agg-attr-names-of ar offset) #{})}
              {:kind :agg-fn        :entity-stack entity-stack
               :used-fns (agg-fn-names-of agg-attr-node)}))

          ;; Inside a `_count` body: only relations of the parent
          ;; entity are valid (count-child's own parens fall through
          ;; to arg-list logic instead).
          (and (enclosing-of-tag resolved ancestors :count-block)
               (not parens))
          {:kind :count-block-body :entity-stack entity-stack
           :used-rels (unaliased-child-names
                        (enclosing-of-tag resolved ancestors :count-block)
                        :count-child offset)}

          ;; `_agg` header line (choosing what to aggregate over): only
          ;; relation names of the parent entity are valid. Distinguished
          ;; from the body by `:agg-attr` rather than `:agg-relation` —
          ;; the cursor is already inside an eagerly-parsed :agg-relation
          ;; while typing the header, and `:agg-attr` only appears on
          ;; body lines. A blank line under the header has no :agg-attr
          ;; yet either; it falls through to the indent-based :else.
          (and (enclosing-of-tag resolved ancestors :agg-block)
               (not (enclosing-of-tag resolved ancestors :agg-attr))
               (not parens)
               (not= :agg-relation
                     (enclosing-relation-kind-by-indent
                       tree source offset
                       (current-line-indent-text source offset)
                       schema root-entity)))
          {:kind :agg-block-body :entity-stack entity-stack
           :used-rels (unaliased-child-names
                        (enclosing-of-tag resolved ancestors :agg-block)
                        :agg-relation offset)}

          ;; Bare attribute + space (`xid |`): only an operator can
          ;; follow. Below the agg/count checks: `value |` in an _agg
          ;; body is the `value: avg` slot, not a predicate.
          inline-attr-after-space
          {:kind :after-attr-in-pred
           :entity-stack entity-stack
           :attr-def inline-attr-after-space}

          ;; Indent-based fallback for mid-typing relations whose Block
          ;; hasn't formed yet.
          :else
          (let [line-indent (current-line-indent-text source offset)]
            (cond
              (and (pos? line-indent) schema root-entity)
              (if-let [target (enclosing-relation-target-by-indent
                                tree source offset line-indent schema root-entity)]
                (let [rel-kind (enclosing-relation-kind-by-indent
                                 tree source offset line-indent schema root-entity)]
                  {:kind :block-start :entity-stack [target]
                   :marker-before marker-before
                   :siblings siblings
                   :in-agg-body? (= :agg-relation rel-kind)})
                (if (enclosing-of-tag resolved ancestors :block)
                  {:kind :block-start :entity-stack entity-stack
                   :marker-before marker-before
                   :siblings siblings}
                  {:kind :line-start-root :entity-stack entity-stack
                   :marker-before marker-before
                   :siblings siblings}))

              (enclosing-of-tag resolved ancestors :block)
              {:kind :block-start :entity-stack entity-stack
               :marker-before marker-before
               :siblings siblings}

              ;; Flush-left is the root entity line of a rooted query.
              :else
              {:kind :root-entity-line
               :marker-before marker-before
               :siblings siblings})))))))

;; ── Option emission ──────────────────────────────────────────────────────

;; Audit-injected columns (created_on/modified_on, created_by/modified_by)
;; are offered like any other field — real, queryable runtime columns.

(defn sorted-attr-keys
  "Sorted attribute keys for option emission."
  [entity-def]
  (sort (keys (:attributes entity-def))))

(defn sorted-relation-keys
  "Sorted relation keys for option emission."
  [entity-def]
  (sort (keys (:relations entity-def))))

(defn many-relation-keys
  "Sorted to-many relation keys — the only kind `_count`/`_agg` can
   meaningfully aggregate over."
  [entity-def]
  (->> (:relations entity-def)
       (filter (fn [[_ r]] (= "many" (:cardinality r))))
       (map first)
       sort))

(defn numeric-attr-options
  "Number-typed attribute options, for `_agg` relation bodies. `used`
   (optional) drops attrs already aggregated in this body."
  ([entity-def] (numeric-attr-options entity-def #{}))
  ([entity-def used]
   (when entity-def
     (->> (:attributes entity-def)
          (filter (fn [[_ a]] (= "number" (:type a))))
          (map first)
          (remove used)
          sort
          (mapv (fn [a] {:label a :type "attribute" :section "Attributes"}))))))

(defn alias-template-option
  "Completion option for a relation already projected bare at this scope:
   a snippet inserting `alias:rel` with the alias placeholder selected,
   rather than hiding the relation. `prefix` carries a `->` join marker."
  [rel prefix]
  {:label rel :type "relation" :section "Relations"
   :detail "already used — alias it"
   :snippet (str prefix "${alias}:" rel)})

(defn options-for-entity
  "Attribute + relation options for an entity, attrs first. `:siblings`
   (optional) is `{:attrs #{} :rels #{}}` already used at this scope —
   filtered from attrs, flipped to alias templates for relations."
  [entity-def {:keys [relation-prefix relation-detail siblings]}]
  (let [used-attrs (:attrs siblings #{})
        used-rels  (:rels  siblings #{})
        attrs (->> (sorted-attr-keys entity-def)
                   (remove used-attrs))]
    (concat
      (mapv (fn [a] {:label a :type "attribute" :section "Attributes"}) attrs)
      (mapv (fn [r]
              (if (used-rels r)
                (alias-template-option r (or relation-prefix ""))
                (cond-> {:label (str (or relation-prefix "") r)
                         :type "relation"
                         :section "Relations"}
                  relation-detail (assoc :detail relation-detail))))
            (sorted-relation-keys entity-def)))))

(defn bare-relation-options
  "Plain relation options, no `-`/`->` prefix — for when the user
   already typed the marker. `siblings` (optional) flips already-used
   relations to alias templates, mirroring `options-for-entity`."
  ([entity-def] (bare-relation-options entity-def nil))
  ([entity-def siblings]
   (when entity-def
     (let [used (:rels siblings #{})]
       (mapv (fn [r]
               (if (used r)
                 (alias-template-option r "")
                 {:label r :type "relation" :section "Relations"}))
             (sorted-relation-keys entity-def))))))

(defn options-for-context [ctx schema root-entity]
  (let [entity-def (or (peek (:entity-stack ctx))
                       (when (and schema root-entity)
                         (get-in schema [:entities root-entity])))
        marker (:marker-before ctx)]
    (case (:kind ctx)
      :line-start-root
      (if marker
        (bare-relation-options entity-def (:siblings ctx))
        (concat
          ;; Default to `->` (left join) — see docs for rationale.
          (when entity-def
            (options-for-entity entity-def {:relation-prefix "->"
                                            :siblings (:siblings ctx)}))
          [{:label "_count" :type "keyword" :section "Operators" :detail "aliased counters"  :apply :block-opener}
           {:label "_agg"   :type "keyword" :section "Operators" :detail "aggregates"        :apply :block-opener}]))

      :root-entity-line
      (mapv (fn [e] {:label e :type "class" :section "Entities" :detail "root entity"})
            (sort (keys (:entities schema))))

      :block-start
      (if (:in-agg-body? ctx)
        (numeric-attr-options entity-def)
        (if marker
          (bare-relation-options entity-def (:siblings ctx))
          (concat
            (when entity-def
              (options-for-entity entity-def {:relation-prefix "->"
                                              :siblings (:siblings ctx)}))
            [{:label "_count" :type "keyword" :section "Operators" :detail "aliased counters" :apply :block-opener}
             {:label "_agg"   :type "keyword" :section "Operators" :detail "aggregates"       :apply :block-opener}])))

      ;; Attrs + meta-keys only — a relation's args belong in its own
      ;; header parens; dotted paths complete via :path-continuation.
      :arg-list-start
      (concat
        (mapv (fn [a] {:label a :type "attribute" :section "Attributes"})
              (sorted-attr-keys entity-def))
        (mapv (fn [k] {:label k :type "keyword" :section "Operators"}) meta-keys))

      :after-attr-in-pred
      (let [type (get-in ctx [:attr-def :type])
            allowed (or (ops-by-type type) (ops-by-type "string"))]
        (mapv (fn [op] {:label op :type "operator"}) allowed))

      ;; Joiners continue the SAME arg-stmt; a comma starts a fresh one.
      ;; `:insert` carries a trailing space so the next pick doesn't glue.
      :after-predicate-value
      [{:label "and" :type "keyword" :section "Operators"
        :detail "both must match" :insert "and "}
       {:label "or" :type "keyword" :section "Operators"
        :detail "either matches" :insert "or "}]

      :after-is
      [{:label "null" :type "constant"} {:label "not null" :type "constant"}]

      :after-is-not
      [{:label "null" :type "constant"}]

      :after-not
      [{:label "in" :type "operator"}]

      :after-like
      [{:label "\"%…%\" (contains)"   :type "text" :snippet "\"%${1}%\""}
       {:label "\"…%\" (starts with)" :type "text" :snippet "\"${1}%\""}
       {:label "\"%…\" (ends with)"   :type "text" :snippet "\"%${1}\""}
       {:label "\"…\" (exact)"        :type "text" :snippet "\"${1}\""}]

      :after-in
      [{:label "(…)" :type "text" :snippet "(${1})" :detail "list of values"}]

      :path-continuation
      (when-let [target (:path-target-entity ctx)]
        (options-for-entity target {:relation-detail "path hop"}))

      ;; `_count`/`_agg` body: bare, to-many relation names only.
      (:count-block-body :agg-block-body)
      (when entity-def
        (let [used (:used-rels ctx #{})
              rels (many-relation-keys entity-def)
              ;; _agg additionally needs a numeric attr on the target.
              rels (if (= :agg-block-body (:kind ctx))
                     (filterv
                       (fn [r]
                         (let [target-name (get-in entity-def [:relations r :target])
                               target      (get-in schema [:entities target-name])]
                           (boolean (some (fn [[_ a]] (= "number" (:type a)))
                                          (:attributes target)))))
                       rels)
                     rels)]
          (mapv (fn [r]
                  (if (used r)
                    (alias-template-option r "")
                    {:label r :type "relation" :section "Relations"}))
                rels)))

      :agg-attr-name
      (numeric-attr-options entity-def (:used-attrs ctx #{}))

      :agg-fn
      (mapv (fn [f] {:label f :type "function"})
            (remove (:used-fns ctx #{}) agg-fns))

      :order-dir
      [{:label "asc" :type "constant"} {:label "desc" :type "constant"}]

      ;; Only self-relations of the root entity can be recursed.
      :on-relation
      (when entity-def
        (let [root-name (or root-entity
                            (some (fn [[n d]] (when (= d entity-def) n))
                                  (:entities schema)))]
          (->> (:relations entity-def)
               (filter (fn [[_ r]] (= root-name (:target r))))
               (map first)
               sort
               (mapv (fn [r] {:label r :type "relation" :section "Relations"
                              :detail "self-relation"})))))

      :value-suggestion
      (case (get-in ctx [:attr-def :type])
        "boolean" [{:label "true"  :type "constant"}
                   {:label "false" :type "constant"}]
        ;; Other types are hand-typed literals — empty suppresses the popup.
        [])

      [])))

;; ── Op-specific option filters (multimethod) ────────────────────────────
;; Methods take `[op ctx options entity-def]` and return a new option
;; vector; new ops register a `defmethod`.

(defmulti op-complete-options
  "Transform completion options for op-specific surface. Dispatches on
   the wire op string; `:default` is identity."
  (fn [op _ctx _options _entity-def] (or op :default)))

(defmethod op-complete-options :default
  [_ _ options _]
  options)

(defmethod op-complete-options "get"
  [_ {:keys [kind]} options entity-def]
  (case kind
    ;; Root level: mark unique-constrained attrs so the user sees
    ;; which can carry an identity predicate.
    :line-start-root
    (let [unique? (fn [name]
                    (boolean (get-in entity-def [:attributes name :unique])))
          has-unique-info? (some (fn [[_ a]] (:unique a))
                                 (:attributes entity-def))]
      (->> options
           (mapv (fn [opt]
                   (if (and has-unique-info?
                            (= "attribute" (:type opt))
                            (unique? (:label opt)))
                     (assoc opt :detail "unique — identity")
                     opt)))))

    ;; Other contexts fall back to the default option list.
    options))

(defn tree-op-options
  "Tree ops (@search-tree/@get-tree) surface `_on <self-relation>` in
   the root arg-list only (entity-stack still just the root there)."
  [_ {:keys [kind entity-stack]} options _]
  (if (and (= :arg-list-start kind) (<= (count entity-stack) 1))
    (conj (vec options)
          {:label "_on" :type "keyword" :section "Operators"
           :detail "self-relation to recurse over"})
    options))

(defmethod op-complete-options "search-tree" [op ctx options entity-def]
  (tree-op-options op ctx options entity-def))

(defmethod op-complete-options "get-tree" [op ctx options entity-def]
  (tree-op-options op ctx options entity-def))

;; ── Public entry ─────────────────────────────────────────────────────────

(defn complete
  "Compute completion options at `offset` in `source`. `:op` is the
   wire op string (\"search\" / \"get\" / …); when provided, op-specific
   filters apply (see `op-complete-options`)."
  [{:keys [source offset schema root-entity op]
    :or {offset 0}}]
  (let [tree (parser/parse (or source ""))
        ;; Rooted XSQL is self-describing: the query-root entity wins
        ;; over the (legacy) external arg, which is only a fallback for
        ;; a bodyless source.
        root-entity (or (some-> (:root-entity tree) :text) root-entity)
        [resolved ancestors] (innermost-with-ancestors tree offset (or source ""))
        ctx (context-at tree resolved ancestors source offset schema root-entity)
        entity-def (or (peek (:entity-stack ctx))
                       (when (and schema root-entity)
                         (get-in schema [:entities root-entity])))
        options (vec (options-for-context ctx schema root-entity))
        options (op-complete-options op ctx options entity-def)
        {:keys [from to]} (current-word-range (or source "") offset)]
    {:from from :to to :options options}))

;; ── Persistent scope panel (as opposed to token-position completion) ────
;; `scope-at` buckets `context-at`'s resolution for a panel that stays
;; stable while the cursor wanders, rather than one popup position: see
;; docs/core/synthigy/xsql/complete.md for the :args/:scope/:root split.

(def ^:private args-kinds
  #{:value-suggestion :after-is :after-is-not :after-not :after-like
    :after-in :arg-list-start :after-attr-in-pred :after-predicate-value
    :path-continuation :agg-fn :order-dir})

(defn scope-path-by-indent
  "Breadcrumb of relation names enclosing the cursor, rooted at the
   entity name, using the same indent logic that decides scope."
  [tree source offset root-entity]
  (let [rels (collect-relations-before tree source offset)]
    (loop [i (dec (count rels))
           col (current-line-indent-text source offset)
           acc ()]
      (if (< i 0)
        (into [] (remove nil?) (cons root-entity acc))
        (let [r (nth rels i)]
          (if (< (:indent r) col)
            (recur (dec i) (:indent r) (conj acc (:name r)))
            (recur (dec i) col acc)))))))

(defn scope-at
  [{:keys [source offset schema root-entity]}]
  (let [tree (parser/parse (or source ""))
        root-entity (or (some-> (:root-entity tree) :text) root-entity)
        [resolved ancestors] (innermost-with-ancestors tree offset (or source ""))
        ctx (context-at tree resolved ancestors source offset schema root-entity)
        entity-stack (:entity-stack ctx)
        current-entity (peek entity-stack)
        ;; Cursor's physical line indent, not `(count entity-stack)` —
        ;; the indent-based fallback in context-at truncates the stack
        ;; to `[target]` on a blank child line, so depth alone would
        ;; misjudge nesting there.
        indent (max 2 (current-line-indent-text source offset))
        ;; Sitting on a relation's own header line resolves to ITS
        ;; target instead of the parent's scope; only overrides a plain
        ;; :block-start/:line-start-root result, and only when
        ;; context-at hasn't already descended into it (would
        ;; double-descend otherwise). Additive to scope-at only.
        header-rel (when (#{:block-start :line-start-root} (:kind ctx))
                     (when-let [rel (enclosing-of-tag resolved ancestors :relation)]
                       (when (= (line-start-offset source offset)
                                (line-start-offset source (first (:span rel))))
                         rel)))
        header-target (when header-rel
                        (let [rel-name (relation-name-of header-rel)
                              rel-def (get-in current-entity [:relations rel-name])
                              target (when rel-def (get-in schema [:entities (:target rel-def)]))]
                          (when (and target (not= target current-entity))
                            target)))
        ;; Same idea one level up: a resolved root-entity line shows
        ;; that entity's own scope instead of the entity picker.
        root-target (when (and (= :root-entity-line (:kind ctx)) (seq root-entity))
                      (get-in schema [:entities root-entity]))
        ;; Sibling-safety: when context-at's indent heuristic truncated
        ;; the stack to `[target]` (blank child line, no parsed block
        ;; yet), ctx's own :siblings would be inherited from the
        ;; PARENT's block — report empty instead of wrong siblings. See
        ;; docs/core/synthigy/xsql/complete.md for the full analysis.
        indent-heuristic-only? (and (= 1 (count entity-stack))
                                    (not= current-entity
                                          (get-in schema [:entities root-entity])))
        ;; A REAL parsed relation block's own statements are the
        ;; authoritative sibling set (same cursor-column dedent hint
        ;; as entity-stack-from-ancestors) — wins over the guard above.
        enclosing-rel-block (when-let [rel (enclosing-of-tag resolved ancestors :relation)]
                              (when-let [block (ast/find-child rel :block)]
                                (let [[from to] (:span block)
                                      body-ind (block-body-indent block source)]
                                  (when (and (>= offset from) (<= offset to)
                                             (not (and body-ind
                                                       (< (current-line-indent-text source offset)
                                                          body-ind))))
                                    block))))
        safe-siblings (cond
                        enclosing-rel-block (sibling-bare-names enclosing-rel-block source offset)
                        indent-heuristic-only? {:attrs #{} :rels #{}}
                        :else (:siblings ctx))]
    (let [path (scope-path-by-indent tree source offset root-entity)]
      (cond
        header-target
        {:mode :scope
         :entity-def header-target
         :siblings (if-let [block (ast/find-child header-rel :block)]
                     (sibling-bare-names block source offset)
                     {:attrs #{} :rels #{}})
         ;; Path's indent walk stops above this relation; append it.
         :path (conj path (relation-name-of header-rel))
         :indent (+ 2 (current-line-indent-text source offset))}

        root-target
        {:mode :scope
         :entity-def root-target
         :siblings (sibling-bare-names tree source offset)
         :path path
         :indent indent}

        :else
        (case (:kind ctx)
          :root-entity-line
          {:mode :root :entities (vec (sort (keys (:entities schema))))}

          :count-block-body
          {:mode :scope :entity-def current-entity
           :siblings {:attrs #{} :rels (:used-rels ctx #{})}
           :relations-only? true :agg-kind :count :path path :indent indent}

          :agg-block-body
          {:mode :scope :entity-def current-entity
           :siblings {:attrs #{} :rels (:used-rels ctx #{})}
           :relations-only? true :agg-kind :agg :path path :indent indent}

          :agg-attr-name
          {:mode :scope :entity-def current-entity :numeric-only? true
           :siblings {:attrs (:used-attrs ctx #{}) :rels #{}}
           :agg-kind :agg-attr :path path :indent indent}

          (if (contains? args-kinds (:kind ctx))
            {:mode :args :path path
             :options (vec (options-for-context ctx schema root-entity))}
            {:mode :scope :entity-def current-entity :siblings safe-siblings
             :path path :indent indent}))))))
