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

(ns synthigy.xsql.complete
  "Context-aware autocompletion engine for XSQL."
  (:require [clojure.string :as str]
            [synthigy.xsql.ast :as ast]
            [synthigy.xsql.compile :as compile]
            [synthigy.xsql.parser :as parser]))

;; ── Constants ────────────────────────────────────────────────────────────

(def ^:private meta-keys ["limit" "offset" "order by" "distinct" "join"])
(def ^:private operation-fns ["count" "sum" "avg" "min" "max"])

(def ^:private operation-field-types
  {"sum" #{"number"}
   "avg" #{"number"}
   "min" #{"number" "timestamp" "string"}
   "max" #{"number" "timestamp" "string"}})

(def ^:private uncountable-types #{"json" "hashed" "encrypted"})

(def ^:private ops-by-type
  {"string"    ["=" "!=" "<" "<=" ">" ">=" "like" "ilike" "in" "not in" "is" "is not null"]
   "number"    ["=" "!=" "<" "<=" ">" ">=" "in" "not in" "is"]
   "boolean"   ["=" "!=" "is"]
   "timestamp" ["=" "!=" "<" "<=" ">" ">=" "in" "not in" "is"]
   "enum"      ["=" "!=" "like" "ilike" "in" "not in" "is"]})

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
  "`:arrow` when `->` immediately precedes the current word, `:dash` for `-`, `:bang` for `!`, else nil."
  [^String source from]
  (let [from (if (= ":" (char-at source (dec from)))
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

      (and (>= from 1)
           (= "!" (char-at source (dec from))))
      :bang

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
  "Innermost AST node whose span contains `offset`, as `[node ancestors]`."
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
  "Relation nodes starting before `offset`, as `{:name :indent :from :kind}` entries."
  [root source offset]
  (let [acc (volatile! [])
        rel-tags #{:relation}]
    (letfn [(walk [n]
              (when (ast/container? n)
                (when (and (rel-tags (:node n))
                           (< (first (:span n)) offset))
                  (vswap! acc conj
                          {:name   (relation-name-of n)
                           :indent (line-start-indent source (first (:span n)))
                           :from   (first (:span n))
                           :kind   (:node n)
                           :node   n}))
                (run! walk (:children n))))]
      (walk root))
    @acc))

(defn enclosing-relation-target-by-indent
  "Entity context for a line at `child-indent`, from the innermost preceding relation at smaller indent."
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

(defn none-scope-by-indent
  "Name of the innermost `!rel` (or `_join none`) relation enclosing the cursor by indent, or nil."
  [root source offset]
  (let [rels (collect-relations-before root source offset)]
    (loop [i   (dec (count rels))
           col (current-line-indent-text source offset)]
      (when (>= i 0)
        (let [r (nth rels i)]
          (cond
            (>= (:indent r) col) (recur (dec i) col)
            (and (= :relation (:kind r))
                 (= "none" (compile/relation-join (:node r)))) (:name r)
            :else (recur (dec i) (:indent r))))))))

(defn with-none-scope
  "Tag a field-position ctx with `:none-scope` when it sits inside a `!rel` block."
  [ctx tree source offset]
  (if-let [rel (and (#{:block-start :line-start-root} (:kind ctx))
                    (none-scope-by-indent tree source offset))]
    (assoc ctx :none-scope rel)
    ctx))

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
  "Final attr-def of a :path AST node walked from the current entity, or nil."
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

(defn entity-stack-from-ancestors
  "Push relation-target entities for ancestors whose block/parens contain offset, honoring the cursor-column hint."
  [ancestors offset source schema entity-stack]
  (let [cursor-col (current-line-indent-text source offset)
        outdented? (fn [body-indent]
                     (and body-indent (< cursor-col body-indent)))]
    (reduce
      (fn [stack node]
        (cond
          (= :relation (:node node))
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

          :else stack))
      entity-stack
      ancestors)))

(defn attr-def-for-value-position
  "Attr-def of the field being predicated when the cursor is right after a binary op."
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
  "AST node whose direct children are the sibling statements at the cursor's scope."
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
  "Bare attr/relation names already used at this scope, as `{:attrs #{} :rels #{}}`."
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

                    (= :relation (:node c))
                    (when-not (some #(= :alias (:node %)) (:children c))
                      (when-let [id (some #(when (= :identifier (:node %)) %)
                                          (:children c))]
                        (vswap! rels conj (:text id))))))))]
      (run! walk-stmt
            (filter #(= :statement (:node %))
                    (:children scope-parent))))
    {:attrs @attrs :rels @rels}))

(defn last-leaf-of
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

(defn open-paren-stack
  "Positions of the unmatched `(` before `offset`, outermost first."
  [^String source offset]
  (loop [i 0 stack []]
    (if (>= i offset)
      stack
      (let [c (char-at source i)]
        (recur (inc i) (cond (= c "(") (conj stack i)
                             (= c ")") (if (seq stack) (pop stack) stack)
                             :else stack))))))

(defn innermost-open-paren
  "Position of the innermost unmatched `(` before `offset`, or nil."
  [^String source offset]
  (peek (open-paren-stack source offset)))

(def ^:private order-kw-re #"^\s*(?:_order_by|order\s+by)(?=\s|$)")
(def ^:private order-spec-re #"^\s*[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*(?:\s+(?:asc|desc))?\s*$")

(defn order-path-prefix
  "Segments typed before the last `.` of the `order by` column at `offset` (`studio.na|` → [\"studio\"])."
  [^String source offset]
  (let [word (re-find #"[A-Za-z0-9_.]*$" (subs (or source "") 0 offset))
        segs (str/split word #"\." -1)]
    (vec (remove str/blank? (pop (vec segs))))))

(defn order-by-slot
  "`:attr` or `:dir` slot of an `order by` spec list at `offset`, else nil."
  [^String source offset]
  (let [source (or source "")
        start (if-let [p (innermost-open-paren source offset)] (inc p) 0)
        segs (vec (str/split (subs source start offset) #"," -1))
        cur (peek segs)
        head (re-find order-kw-re cur)
        in-list? (or (some? head)
                     (loop [i (- (count segs) 2)]
                       (cond
                         (neg? i) false
                         (re-find order-kw-re (nth segs i)) true
                         (re-matches order-spec-re (nth segs i)) (recur (dec i))
                         :else false)))]
    (when in-list?
      (let [body (if head (subs cur (count head)) cur)
            words (into [] (remove str/blank?) (str/split body #"\s+"))
            trailing-ws? (boolean (re-find #"\s$" body))]
        (cond
          (str/starts-with? (str (first words)) "?") nil
          (and (empty? words) head (not trailing-ws?)) nil
          (empty? words) :attr
          (= 1 (count words)) (if trailing-ws? :dir :attr)
          (and (= 2 (count words)) (not trailing-ws?)) :dir
          :else nil)))))

(defn complete-predicate-stmt?
  "True when an :arg-stmt ends in a literal value or a null test (meta-key stmts never)."
  [stmt]
  (and (nil? (ast/find-child stmt :meta-key))
       (let [leaf (last-leaf-of stmt)]
         (or (#{:number :string :param-ref} (:node leaf))
             (#{"null" "true" "false"} (:text leaf))))))

(defn value-slot
  "Attribute path when `offset` sits where an `=`, `!=` or `in (…)` value goes, and whether inside an open quote."
  [source offset]
  (let [t (subs source (line-start-offset source offset) offset)
        eq (re-find #"([A-Za-z_][A-Za-z0-9_.]*)\s*(?:!=|=)\s*(\"[^\"]*|[A-Za-z0-9_]*)$" t)
        in (re-find #"([A-Za-z_][A-Za-z0-9_.]*)\s+(?:not\s+)?in\s*\(((?:\s*(?:\"[^\"]*\"|[^\s,()\"]+)\s*,)*)\s*(\"[^\"]*|[A-Za-z0-9_]*)$" t)]
    (cond
      eq {:path (nth eq 1) :in-string? (str/starts-with? (nth eq 2) "\"")}
      in {:path (nth in 1) :in-string? (str/starts-with? (nth in 3) "\"")
          :taken (into #{} (map second) (re-seq #"\"([^\"]*)\"" (nth in 2)))})))

(defn path-prefix-target
  "Entity reached from `entity` over every complete segment of a dotted `path-text` (the last, partial one is skipped)."
  [path-text entity schema]
  (reduce (fn [e seg]
            (if-let [t (some->> (get-in e [:relations seg :target]) (conj [:entities]) (get-in schema))]
              t
              (reduced nil)))
          entity
          (butlast (str/split path-text #"\." -1))))

(defn path-attr
  "Attribute a dotted `path-text` ends on, walked from `entity`."
  [path-text entity schema]
  (get-in (path-prefix-target path-text entity schema)
          [:attributes (last (str/split path-text #"\." -1))]))

;; ── Aggregate calls (`alias: count(rel …)`) ─────────────────────────────

(def ^:private call-head-re #"[ \t]*[a-z_][a-z0-9_]*[ \t]*:[ \t]*([a-z_]+)[ \t]*")

(defn call-at
  "`{:fn :open}` of the aggregate call whose parens enclose `offset`, or nil."
  [^String source offset]
  (some (fn [p]
          (when-let [[_ f] (re-matches call-head-re (subs source (line-start-offset source p) p))]
            (when (some #{f} operation-fns) {:fn f :open p})))
        (rseq (open-paren-stack source offset))))

(defn aggregate-fn-slot?
  "True when the cursor sits where an operation name goes: `alias: |`."
  [^String source offset]
  (boolean (re-matches #"[ \t]*[a-z_][a-z0-9_]*[ \t]*:[ \t]*[a-z_]*"
                       (subs source (line-start-offset source offset) offset))))

(defn call-scope-entity
  "Entity whose relations an aggregate call at `open` reads: its line's indent scope, else the AST stack."
  [tree source open schema root-entity entity-stack]
  (let [ind (line-start-indent source open)]
    (or (when (pos? ind)
          (enclosing-relation-target-by-indent tree source open ind schema root-entity))
        (peek entity-stack)
        (get-in schema [:entities root-entity]))))

(defn call-context
  "Completion ctx inside an aggregate call, or nil to leave the cursor to the generic branches."
  [tree source offset schema root-entity entity-stack]
  (when-let [{f :fn p :open} (call-at source offset)]
    (let [scope (call-scope-entity tree source p schema root-entity entity-stack)
          stack [scope]
          inner (subs source (inc p) offset)
          parts (str/split inner #",|\(|\)|\band\b|\bor\b" -1)
          rel   (second (re-find #"([a-z_][a-z0-9_]*)" (str/replace inner #"^\s*distinct\s+" "")))
          path  "[a-z_][a-z0-9_]*(?:\\.[a-z_][a-z0-9_]*)"]
      (if (= 1 (count parts))
        (let [distinct? (boolean (re-find #"^\s*distinct\s+" inner))
              body      (str/replace inner #"^\s*(?:distinct\s+)?" "")
              shortcut? (and (= "count" f) (not distinct?))]
          (cond
            (re-matches #"[a-z0-9_]*" body)
            {:kind :call-relation :entity-stack stack :fn f :distinct? distinct?
             :offer-distinct? shortcut?}

            (re-matches #"[a-z_][a-z0-9_]*\.[a-z0-9_]*" body)
            {:kind :call-field :entity-stack stack :fn f :distinct? distinct?
             :target (path-prefix-target body scope schema)}

            (re-matches (re-pattern (str path "+\\.[a-z0-9_]*")) body)
            (if shortcut?
              {:kind :path-continuation :entity-stack stack
               :path-target-entity (path-prefix-target body scope schema)}
              {:kind :call-none :entity-stack stack})

            (re-matches (re-pattern (str path "+\\s+")) body)
            (if shortcut?
              {:kind :after-attr-in-pred :entity-stack stack :attr-def (path-attr (str/trim body) scope schema)}
              {:kind :call-none :entity-stack stack})))
        (let [tail (peek parts)]
          (cond
            (re-find #"\)\s*$" inner)
            {:kind :after-predicate-value :entity-stack stack}

            (re-matches #"\s*[a-z0-9_]*" tail)
            {:kind :call-filter-head :entity-stack stack :relation rel}

            (re-matches (re-pattern (str "\\s*" path "*\\.[a-z0-9_]*")) tail)
            {:kind :path-continuation :entity-stack stack
             :path-target-entity (path-prefix-target (str/trim tail) scope schema)}

            (re-matches (re-pattern (str "\\s*" path "+\\s+")) tail)
            {:kind :after-attr-in-pred :entity-stack stack :attr-def (path-attr (str/trim tail) scope schema)}))))))

(defn order-scope-aliases
  "Aggregate aliases of the level whose parens hold the cursor."
  [tree resolved ancestors]
  (compile/scope-aggregate-aliases
   (if-let [rel (enclosing-of-tag resolved ancestors :relation)]
     (ast/find-child rel :block)
     tree)))

(defn aliases-at
  "Aggregate aliases of the level whose parens hold `offset` in `source`."
  [source offset]
  (let [tree (parser/parse (or source ""))
        [resolved ancestors] (innermost-with-ancestors tree offset (or source ""))]
    (order-scope-aliases tree resolved ancestors)))

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
        after-closed-string? (and (= :string (:node resolved))
                                  (= offset (second (:span resolved)))
                                  (let [t (:text resolved)]
                                    (and (> (count t) 1)
                                         (= "\"" (subs t (dec (count t)))))))
        order-slot (order-by-slot source offset)
        slot (value-slot source offset)
        slot-attr (when slot (path-attr (:path slot) current-entity schema))]
    (cond
      ;; `order by` branches must stay first — the predicate branches would offer operators after the column
      (= :attr order-slot)
      {:kind :order-attr :entity-stack entity-stack
       :path-prefix (order-path-prefix source offset)
       :aliases (order-scope-aliases tree resolved ancestors)}

      (= :dir order-slot)
      {:kind :order-dir :entity-stack entity-stack}

      slot-attr
      {:kind :value-suggestion
       :entity-stack entity-stack
       :attr-def slot-attr
       :in-string? (:in-string? slot)
       :taken (:taken slot)}

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

      ;; `on |` — the tree-op recursion slot: self-relations only.
      (#{"on" "_on"} prev-word)
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
            call-ctx (call-context tree source offset schema root-entity entity-stack)
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

          (and (pos? (open-paren-depth source offset))
               (let [seg (str/trim (current-arg-text source offset))]
                 (and (seq seg)
                      (not (str/starts-with? seg "_"))
                      (re-find #"(\d|\"|\bnull|\btrue|\bfalse)$" seg))))
          {:kind :after-predicate-value :entity-stack entity-stack}

          (when-let [sc (enclosing-of-tag resolved ancestors :scalar)]
            (when-let [tail (or (ast/find-child sc :scalar-filter)
                                (ast/find-child sc :pred-op))]
              (let [e (second (:span tail))]
                (and (<= e offset)
                     (str/blank? (subs source e (min offset (count source))))
                     (complete-predicate-stmt? tail)))))
          {:kind :after-predicate-value :entity-stack entity-stack}

          call-ctx
          call-ctx

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

          (aggregate-fn-slot? source offset)
          {:kind :aggregate-fn :entity-stack entity-stack}

          inline-attr-after-space
          {:kind :after-attr-in-pred
           :entity-stack entity-stack
           :attr-def inline-attr-after-space}

          :else
          (let [line-indent (current-line-indent-text source offset)]
            (cond
              (and (pos? line-indent) schema root-entity)
              (if-let [target (enclosing-relation-target-by-indent
                                tree source offset line-indent schema root-entity)]
                {:kind :block-start :entity-stack [target]
                 :marker-before marker-before
                 :siblings siblings}
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

(defn sorted-attr-keys
  [entity-def]
  (sort (keys (:attributes entity-def))))

(defn sorted-relation-keys
  [entity-def]
  (sort (keys (:relations entity-def))))

(defn many-relation-keys
  "Sorted to-many relation keys, the only kind an operation can read."
  [entity-def]
  (->> (:relations entity-def)
       (filter (fn [[_ r]] (= "many" (:cardinality r))))
       (map first)
       sort))

(defn operation-attr-options
  "Attributes of `target` that operation `f` can read."
  [target f distinct?]
  (let [allowed (operation-field-types f)]
    (->> (:attributes target)
         (filter (fn [[_ a]] (if distinct?
                               (not (uncountable-types (:type a)))
                               (and allowed (allowed (:type a))))))
         (map first)
         sort
         (mapv (fn [a] {:label a :type "attribute" :section "Attributes"})))))

(defn alias-template-option
  "Snippet option inserting `alias:rel` for a relation already projected bare at this scope."
  [rel prefix]
  {:label rel :type "relation" :section "Relations"
   :detail "already used — alias it"
   :snippet (str prefix "${alias}:" rel)})

(defn options-for-entity
  "Attribute + relation options for an entity, attrs first, used siblings filtered or aliased."
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
  "Relation options without a `-`/`->` prefix, for when the user already typed the marker."
  ([entity-def] (bare-relation-options entity-def nil))
  ([entity-def siblings]
   (when entity-def
     (let [used (:rels siblings #{})]
       (mapv (fn [r]
               (if (used r)
                 (alias-template-option r "")
                 {:label r :type "relation" :section "Relations"}))
             (sorted-relation-keys entity-def))))))

(defn none-scope-options
  "Inside a `!rel` block: attributes as filters, relations as constraints, nothing to select."
  [entity-def siblings]
  (when entity-def
    (concat
      (mapv (fn [a] {:label a :type "attribute" :section "Attributes" :detail "filter"
                     :snippet (str a " = ${value}")})
            (sorted-attr-keys entity-def))
      (mapv (fn [r] {:label (str "-" r) :type "relation" :section "Relations" :detail "constraint"})
            (remove (:rels siblings #{}) (sorted-relation-keys entity-def))))))

(defn options-for-context [ctx schema root-entity]
  (let [entity-def (or (peek (:entity-stack ctx))
                       (when (and schema root-entity)
                         (get-in schema [:entities root-entity])))
        marker (:marker-before ctx)]
    (case (:kind ctx)
      :line-start-root
      (cond
        marker
        (bare-relation-options entity-def (:siblings ctx))

        (:none-scope ctx)
        (none-scope-options entity-def (:siblings ctx))

        :else
        (when entity-def
          (options-for-entity entity-def {:relation-prefix "->"
                                          :siblings (:siblings ctx)})))

      :root-entity-line
      (mapv (fn [e] {:label e :type "class" :section "Entities" :detail "root entity"})
            (sort (keys (:entities schema))))

      :block-start
      (cond
        marker
        (bare-relation-options entity-def (:siblings ctx))

        (:none-scope ctx)
        (none-scope-options entity-def (:siblings ctx))

        :else
        (when entity-def
          (options-for-entity entity-def {:relation-prefix "->"
                                          :siblings (:siblings ctx)})))

      :arg-list-start
      (concat
        (mapv (fn [a] {:label a :type "attribute" :section "Attributes"})
              (sorted-attr-keys entity-def))
        (mapv (fn [k] {:label k :type "keyword" :section "Operators"}) meta-keys))

      :after-attr-in-pred
      (let [type (get-in ctx [:attr-def :type])
            allowed (or (ops-by-type type) (ops-by-type "string"))]
        (mapv (fn [op] {:label op :type "operator"}) allowed))

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

      :order-attr
      (let [prefix (:path-prefix ctx)
            target (reduce (fn [ent seg]
                             (when-let [rel (get-in ent [:relations seg])]
                               (get-in schema [:entities (:target rel)])))
                           entity-def prefix)]
        (when target
          (concat
            (mapv (fn [a] {:label a :type "attribute" :section "Attributes"})
                  (sorted-attr-keys target))
            (mapv (fn [r] {:label r :type "relation" :section "Relations" :detail "sort by its attribute: ."})
                  (sort (keys (:relations target))))
            (when (empty? prefix)
              (mapv (fn [a] {:label a :type "property" :section "Operations" :detail "operation"})
                    (sort (:aliases ctx)))))))

      :aggregate-fn
      [{:label "count" :type "function" :section "Operations" :detail "related rows"}
       {:label "sum"   :type "function" :section "Operations" :detail "sum of a number field"}
       {:label "avg"   :type "function" :section "Operations" :detail "average of a number field"}
       {:label "min"   :type "function" :section "Operations" :detail "smallest value"}
       {:label "max"   :type "function" :section "Operations" :detail "largest value"}]

      :call-relation
      (when entity-def
        (concat
          (mapv (fn [r] {:label r :type "relation" :section "Relations"})
                (filterv (fn [r]
                           (let [target (get-in schema [:entities (get-in entity-def [:relations r :target])])]
                             (or (and (= "count" (:fn ctx)) (not (:distinct? ctx)))
                                 (seq (operation-attr-options target (:fn ctx) (:distinct? ctx))))))
                         (many-relation-keys entity-def)))
          (when (:offer-distinct? ctx)
            [{:label "distinct" :type "keyword" :section "Operators" :detail "count distinct values"}])))

      :call-field
      (when-let [target (:target ctx)]
        (if (and (= "count" (:fn ctx)) (not (:distinct? ctx)))
          (options-for-entity target {:relation-detail "path hop"})
          (operation-attr-options target (:fn ctx) (:distinct? ctx))))

      :call-filter-head
      (when-let [r (:relation ctx)]
        [{:label r :type "relation" :section "Relations" :detail "the operation's relation"}])

      :call-none
      []

      :order-dir
      [{:label "asc"  :type "constant" :detail "ascending"}
       {:label "desc" :type "constant" :detail "descending"}]

      ;; Only single-parent self-relations of the root entity can be recursed.
      :on-relation
      (when entity-def
        (let [root-name (or root-entity
                            (some (fn [[n d]] (when (= d entity-def) n))
                                  (:entities schema)))]
          (->> (:relations entity-def)
               (filter (fn [[_ r]] (and (= root-name (:target r))
                                        (not= "many" (:cardinality r)))))
               (map first)
               sort
               (mapv (fn [r] {:label r :type "relation" :section "Relations"
                              :detail "self-relation"})))))

      :value-suggestion
      (case (get-in ctx [:attr-def :type])
        "boolean" (if (:in-string? ctx)
                    []
                    [{:label "true"  :type "constant"}
                     {:label "false" :type "constant"}])
        "enum" (mapv (fn [v] {:label (if (:in-string? ctx) v (str "\"" v "\""))
                              :type "constant" :detail "enum"})
                     (remove (or (:taken ctx) #{}) (get-in ctx [:attr-def :values])))
        ;; Other types are hand-typed literals — empty suppresses the popup.
        [])

      [])))

;; ── Op-specific option filters (multimethod) ────────────────────────────

(defmulti op-complete-options
  "Op-specific transform of completion options, dispatched on the wire op string."
  (fn [op _ctx _options _entity-def] (or op :default)))

(defmethod op-complete-options :default
  [_ _ options _]
  options)

(defmethod op-complete-options "get"
  [_ {:keys [kind]} options entity-def]
  (case kind
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

    options))

(defn tree-op-options
  "Tree ops surface `on <self-relation>` in the root arg-list only."
  [_ {:keys [kind entity-stack]} options _]
  (if (and (= :arg-list-start kind) (<= (count entity-stack) 1))
    (conj (vec options)
          {:label "on" :type "keyword" :section "Operators"
           :detail "self-relation to recurse over"})
    options))

(defmethod op-complete-options "search-tree" [op ctx options entity-def]
  (tree-op-options op ctx options entity-def))

(defmethod op-complete-options "get-tree" [op {:keys [kind entity-stack] :as ctx} options entity-def]
  (let [root-args? (and (= :arg-list-start kind) (<= (count entity-stack) 1))
        options (if root-args?
                  (filterv #(and (= "attribute" (:type %)) (#{"xid" "euuid"} (:label %))) options)
                  options)]
    (tree-op-options op ctx options entity-def)))

;; ── Public entry ─────────────────────────────────────────────────────────

(defn complete
  "Completion options at `offset` in `source`; `:op` applies op-specific filters."
  [{:keys [source offset schema root-entity op]
    :or {offset 0}}]
  (let [tree (parser/parse (or source ""))
        root-entity (or (some-> (:root-entity tree) :text) root-entity)
        [resolved ancestors] (innermost-with-ancestors tree offset (or source ""))
        ctx (-> (context-at tree resolved ancestors source offset schema root-entity)
                (with-none-scope tree (or source "") offset))
        entity-def (or (peek (:entity-stack ctx))
                       (when (and schema root-entity)
                         (get-in schema [:entities root-entity])))
        options (vec (options-for-context ctx schema root-entity))
        options (op-complete-options op ctx options entity-def)
        {:keys [from to]} (current-word-range (or source "") offset)]
    {:from from :to to :options options}))

;; ── Persistent scope panel (as opposed to token-position completion) ────

(def ^:private args-kinds
  #{:value-suggestion :after-is :after-is-not :after-not :after-like
    :after-in :arg-list-start :after-attr-in-pred :after-predicate-value
    :path-continuation :order-attr :order-dir
    :aggregate-fn :call-relation :call-field :call-filter-head :call-none})

(defn scope-path-by-indent
  "Breadcrumb of relation names enclosing the cursor, via the same indent walk that decides scope."
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

(defn innermost-relation-by-indent
  [tree source offset]
  (let [col (current-line-indent-text source offset)]
    (some #(when (< (:indent %) col) %) (rseq (collect-relations-before tree source offset)))))

(defn scope-at
  [{:keys [source offset schema root-entity]}]
  (let [tree (parser/parse (or source ""))
        root-entity (or (some-> (:root-entity tree) :text) root-entity)
        [resolved ancestors] (innermost-with-ancestors tree offset (or source ""))
        ctx (-> (context-at tree resolved ancestors source offset schema root-entity)
                (with-none-scope tree (or source "") offset))
        entity-stack (:entity-stack ctx)
        current-entity (peek entity-stack)
        indent (max 2 (current-line-indent-text source offset))
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
        root-target (when (and (= :root-entity-line (:kind ctx)) (seq root-entity))
                      (get-in schema [:entities root-entity]))
        indent-heuristic-only? (and (= 1 (count entity-stack))
                                    (not= current-entity
                                          (get-in schema [:entities root-entity])))
        enclosing-rel (enclosing-of-tag resolved ancestors :relation)
        enclosing-rel-block (when-let [rel enclosing-rel]
                              (when-let [block (ast/find-child rel :block)]
                                (let [[from to] (:span block)
                                      body-ind (block-body-indent block source)]
                                  (when (and (>= offset from) (<= offset to)
                                             (not (and body-ind
                                                       (< (current-line-indent-text source offset)
                                                          body-ind))))
                                    block))))
        deeper-rel (when-let [r (innermost-relation-by-indent tree source offset)]
                     (when (and (= :relation (:kind r))
                                (or (nil? enclosing-rel-block)
                                    (> (:from r) (first (:span enclosing-rel)))))
                       r))
        safe-siblings (cond
                        deeper-rel (if-let [block (ast/find-child (:node deeper-rel) :block)]
                                     (sibling-bare-names block source offset)
                                     {:attrs #{} :rels #{}})
                        enclosing-rel-block (sibling-bare-names enclosing-rel-block source offset)
                        indent-heuristic-only? {:attrs #{} :rels #{}}
                        :else (:siblings ctx))]
    (let [path (scope-path-by-indent tree source offset root-entity)]
      (cond
        header-target
        {:mode :scope
         :none-scope (or (:none-scope ctx)
                         (when (= "none" (compile/relation-join header-rel))
                           (relation-name-of header-rel)))
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

          (if (contains? args-kinds (:kind ctx))
            {:mode :args :path path
             :options (vec (options-for-context ctx schema root-entity))}
            {:mode :scope :entity-def current-entity :siblings safe-siblings
             :none-scope (:none-scope ctx)
             :path path :indent indent}))))))
