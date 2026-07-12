(ns synthigy.xsql.complete
  "Context-aware autocompletion engine.

   Public entry: `(complete {:source s :offset n :schema sch :root-entity e})`
   returns `{:from int :to int :options [{:label :type :detail? :info? :snippet?}]}`.

   The package is CM6-agnostic. The CLJS adapter in
   `query_dsl_cm.cljs` wraps it as a `CompletionSource`.

   Tier 1 contexts:
   - line-start-root / block-start: attrs + -relations + meta-keywords
   - arg-list-start: attrs + relations (path hop) + meta-keys
   - after-attr-in-pred: operators (type-aware)
   - after-is / after-is-not / after-not: keyword tails
   - after-like / after-in: snippet templates
   - path-continuation: target entity's attrs + relations
   - agg-fn: avg/sum/min/max"
  (:require [clojure.string :as str]
            [synthigy.xsql.ast :as ast]
            [synthigy.xsql.parser :as parser]))

;; ── Constants ────────────────────────────────────────────────────────────

(def ^:private meta-keys ["_limit" "_offset" "_order_by" "_distinct" "_join"])
(def ^:private agg-fns ["avg" "sum" "min" "max"])

(def ^:private ops-by-type
  ;; Operators offered after typing a field. `is` chains into a follow-up
  ;; suggestion of `null` / `not null` via the :after-is context, so it
  ;; covers null-checks for every type. Booleans use SQL-standard
  ;; `=` / `!=` against `true` / `false`, plus `is null` / `is not null`.
  {"string"    ["=" "!=" "<" "<=" ">" ">=" "like" "ilike" "in" "not in" "is" "is not null"]
   "number"    ["=" "!=" "<" "<=" ">" ">=" "in" "not in" "is"]
   "boolean"   ["=" "!=" "is"]
   "timestamp" ["=" "!=" "<" "<=" ">" ">=" "in" "not in" "is"]
   "enum"      ["=" "!=" "in" "not in" "is"]})

;; ── Lexical helpers ──────────────────────────────────────────────────────

(defn- char-at [^String s i]
  (when (and (>= i 0) (< i (count s)))
    (subs s i (inc i))))

(defn- ws-or-tab? [c] (or (= " " c) (= "\t" c)))
(defn- ident-char? [c]
  (and c (re-matches #"[A-Za-z0-9_]" c)))
(defn- ident-letter? [c]
  (and c (re-matches #"[A-Za-z_]" c)))

(defn- prev-non-whitespace-char [^String source offset]
  (loop [i (dec offset)]
    (cond
      (< i 0) nil
      (let [c (char-at source i)]
        (or (= c " ") (= c "\t") (= c "\n")))
      (recur (dec i))
      :else (char-at source i))))

(defn- marker-before-current-word
  "Return `:arrow` if the chars immediately preceding `from` are `->`,
   `:dash` if just `-`, else nil. `from` should be the start of the
   current word (the relation identifier the user is typing). Used to
   detect that the cursor is at the relation-name slot of a relation
   header — completion can then narrow to relations only and emit bare
   labels (the user already typed the marker, so re-emitting `-rel`
   would land in the buffer as `->-rel`)."
  [^String source from]
  (cond
    (and (>= from 2)
         (= ">" (char-at source (dec from)))
         (= "-" (char-at source (- from 2))))
    :arrow

    (and (>= from 1)
         (= "-" (char-at source (dec from))))
    :dash

    :else nil))

(defn- prev-word-at [^String source offset]
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

(defn- prev-word-start [^String source offset]
  (loop [i (dec offset)]
    (cond
      (< i 0) 0
      (ws-or-tab? (char-at source i)) (recur (dec i))
      :else
      (loop [j i]
        (if (and (>= j 0) (ident-letter? (char-at source j)))
          (recur (dec j))
          (inc j))))))

(defn- prev-word-before [^String source offset-before]
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

(defn- current-line-indent-text [^String source offset]
  (let [start (loop [s offset]
                (if (and (pos? s) (not= "\n" (char-at source (dec s))))
                  (recur (dec s)) s))
        n (count source)]
    (loop [i start]
      (if (and (< i n) (ws-or-tab? (char-at source i)))
        (recur (inc i))
        (- i start)))))

(defn- line-start-indent [^String source pos]
  (current-line-indent-text source pos))

(defn- line-start-offset
  "Offset of the first character of the line containing `offset`."
  [^String source offset]
  (loop [s offset]
    (if (and (pos? s) (not= "\n" (char-at source (dec s))))
      (recur (dec s)) s)))

(defn- current-word-range [^String source offset]
  (let [n (count source)
        start (loop [s offset]
                (if (and (pos? s) (ident-char? (char-at source (dec s))))
                  (recur (dec s)) s))
        end (loop [e offset]
              (if (and (< e n) (ident-char? (char-at source e)))
                (recur (inc e)) e))]
    {:from start :to end}))

;; ── AST ancestor resolution ──────────────────────────────────────────────

(defn- offset-in-span? [node offset]
  (let [[from to] (:span node)]
    (and (>= offset from) (<= offset to))))

(defn- innermost-with-ancestors
  "Find innermost AST node whose span contains `offset`. Return
   `[node ancestors]` where ancestors is the chain from root down to
   the immediate parent.

   When multiple siblings both contain `offset` (e.g. `:indent [21,25]`
   and `:agg-attr [25,…]` both contain 25), we prefer the child with the
   largest `:from`, skipping zero-width nodes when a real span is already
   found. This ensures a node that *starts* at the cursor beats an older
   sibling that merely *ends* there, while zero-width error/EOF markers
   don't beat real containers."
  [root offset]
  (loop [n root anc []]
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

(defn- enclosing-of-tag
  "Innermost ancestor (or self) with the given tag, or nil."
  [node ancestors tag]
  (or (when (= tag (:node node)) node)
      (some #(when (= tag (:node %)) %) (reverse ancestors))))

;; ── Indent-based relation context ────────────────────────────────────────

(defn- relation-name-of
  "Find the relation name (skipping any leading :alias)."
  [rel-node]
  (let [seen-alias (atom false)]
    (some (fn [c]
            (cond
              (= :alias (:node c)) (do (reset! seen-alias true) nil)
              (= :identifier (:node c)) (:text c)
              :else nil))
          (:children rel-node))))

(defn- collect-relations-before
  "Walk the AST collecting all relation nodes (Relation, CountChild,
   AggRelation) that start before `offset`. Each entry is
   `{:name :indent :from :kind}`."
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

(defn- inside-args-block-by-indent?
  "True if the cursor's line sits inside an `_args` block body —
   detected by walking back through prior lines to find one at strictly
   smaller indent that begins with `_args`. Used as a fallback when the
   parser hasn't yet built an :arg-list (mid-typing)."
  [^String source offset]
  (let [my-indent (current-line-indent-text source offset)]
    (when (pos? my-indent)
      (let [line-start (loop [i offset]
                         (if (and (pos? i) (not= "\n" (char-at source (dec i))))
                           (recur (dec i)) i))
            preceding (subs source 0 (max 0 (dec line-start)))
            lines (reverse (str/split preceding #"\n"))]
        (loop [lines lines]
          (when-let [line (first lines)]
            (let [trimmed (str/triml line)]
              (if (empty? trimmed)
                (recur (rest lines))
                (let [indent (- (count line) (count trimmed))]
                  (if (< indent my-indent)
                    (str/starts-with? trimmed "_args")
                    (recur (rest lines))))))))))))

(defn- enclosing-relation-target-by-indent
  "Resolve the entity context for a line at `child-indent` by finding
   the innermost preceding relation at strictly smaller indent and
   walking the indent-stack to determine its target entity."
  [root source offset child-indent schema root-entity]
  (let [root-def (get-in schema [:entities root-entity])]
    (when root-def
      (let [rels (collect-relations-before root source offset)
            ;; Pass 2: resolve each relation's target by parent-by-indent.
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
        ;; Pick latest with indent < child-indent and resolved target.
        (loop [i (dec (count rels))]
          (cond
            (< i 0) nil
            (and (< (:indent (nth rels i)) child-indent)
                 (nth targets i))
            (nth targets i)
            :else (recur (dec i))))))))

(defn- enclosing-relation-kind-by-indent
  "Return the `:kind` (`:relation`, `:count-child`, or `:agg-relation`) of
   the innermost preceding relation at strictly smaller indent, or nil."
  [root source offset child-indent schema root-entity]
  (when (get-in schema [:entities root-entity])
    (let [rels (collect-relations-before root source offset)]
      (loop [i (dec (count rels))]
        (cond
          (< i 0) nil
          (< (:indent (nth rels i)) child-indent) (:kind (nth rels i))
          :else (recur (dec i)))))))

;; ── Schema walks ─────────────────────────────────────────────────────────

(defn- resolve-attr-from-path-node
  "Walk a :path AST node against the current entity, returning the
   final attr-def or nil."
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

(defn- resolved-path-target
  "Walk back from offset over `[a-zA-Z0-9_.]` chars, parse as path
   segments, and return the target entity (for path-continuation)."
  [^String source offset current-entity schema]
  (when (and current-entity schema)
    (let [;; Skip a leading dot at offset-1 if any
          start (loop [i offset]
                  (if (and (pos? i) (= "." (char-at source (dec i))))
                    (dec i) i))
          ;; Walk backward over identifier-chars + dots
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

(defn- block-body-indent
  "Indent (column) of the first body line of a :block node. The :block
   begins with an :indent token whose span starts at the body's first
   non-whitespace position; that position's line indent IS the body
   indent. Returns nil if no :block / no :indent token."
  [block-node source]
  (when block-node
    (when-let [indent-tok (ast/find-child block-node :indent)]
      (current-line-indent-text source (first (:span indent-tok))))))

(defn- agg-body-indent
  "Indent (column) of the body lines of an :agg-relation node. The body
   sits inline after an :indent token (no :block wrapper)."
  [agg-node source]
  (when-let [indent-tok (ast/find-child agg-node :indent)]
    (current-line-indent-text source (first (:span indent-tok)))))

(defn- entity-stack-from-ancestors
  "Walk the ancestor chain and push relation-target entities onto the
   stack when the offset is inside that relation's :block or :parens.
   For :agg-relation nodes, the body is not wrapped in a :block — detect
   it via the :indent child instead.

   **Cursor-column scope hint (2026-06-03):** XSQL.md treats blank
   lines inside a block as 'skipped' — so the AST keeps the cursor
   inside the last block until non-blank content at lower indent
   appears. That's correct semantically but user-hostile during
   autocomplete: a user who's clearly typing at column 0 after a child
   block expects root-scope suggestions, not the inner block's. To
   honor that visual intent without changing the spec, we compare the
   cursor's current-line indent against each relation's body indent
   *during* the push: when body-indent > cursor-col, we treat the
   relation as logically closed at this cursor position and skip the
   push. The AST is unchanged; only this completion-time projection
   shifts."
  [ancestors offset source schema entity-stack]
  (let [cursor-col (current-line-indent-text source offset)
        outdented? (fn [body-indent]
                     ;; Only apply the hint when the cursor's column is
                     ;; STRICTLY less than the body indent — staying at
                     ;; the body's own indent should still be inside.
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
                     ;; Honor cursor-column hint for block-body case.
                     ;; Parens (inline args) aren't column-sensitive.
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

(defn- attr-def-for-value-position
  "If the cursor is inside the Value side of a PredOp(BinaryOp …) — i.e.
   right after a binary operator (`=`, `!=`, `<`, etc.) — return the
   attr-def of the field being predicated. Used to drive value-position
   completion (e.g. suggest true/false for booleans)."
  [resolved ancestors current-entity schema]
  (let [pred-op (enclosing-of-tag resolved ancestors :pred-op)]
    (when (and pred-op
               (some #(= :binary-op (:node %)) (:children pred-op)))
      (or
        ;; Inside _args (path op …) — resolve attr from the Path.
        (when-let [arg-pred (enclosing-of-tag resolved ancestors :arg-predicate)]
          (when-let [path (ast/find-child arg-pred :path)]
            (resolve-attr-from-path-node path current-entity schema)))

        ;; Top-level inline scalar predicate — attr is the scalar's
        ;; first :identifier child (alias is wrapped in :alias node, so
        ;; bare :identifier means field name).
        (when-let [scalar (enclosing-of-tag resolved ancestors :scalar)]
          (let [field-id (some #(when (= :identifier (:node %)) %)
                               (:children scalar))]
            (when field-id
              (get-in current-entity [:attributes (:text field-id)]))))))))

(defn- scope-parent-node
  "AST node whose direct children are the sibling statements at the
   cursor's scope (after the cursor-column hint).
     - Root `:query` when cursor is at column 0.
     - Innermost ancestor `:block` whose body indent ≤ cursor column,
       otherwise."
  [tree ancestors source offset]
  (let [cursor-col (current-line-indent-text source offset)
        block-anc  (->> ancestors
                        (filter #(= :block (:node %)))
                        (filter (fn [b]
                                  (when-let [bi (block-body-indent b source)]
                                    (<= bi cursor-col))))
                        last)]
    (or block-anc tree)))

(defn- sibling-bare-names
  "Walk the direct statement children of `scope-parent` and collect the
   bare identifiers already used at this scope. Returns
   `{:attrs #{names} :rels #{names}}`.

   Why:
     - XSQL doesn't allow scalar aliases (XSQL.md, line 169), so a bare
       attribute name at a given scope can only appear once meaningfully.
     - Bare (un-aliased) relations have the same property: two `-rel`
       lines at the same scope produce the same response key. Aliased
       relations (`-good:rel`) are excluded — they legitimately repeat.

   The current word the user is actively typing is excluded by skipping
   any statement whose span contains the cursor — we don't want to
   filter out their half-typed identifier."
  [scope-parent offset]
  (let [attrs (volatile! #{})
        rels  (volatile! #{})]
    (letfn [(stmt-contains-cursor? [s]
              ;; Cursor at the statement's END offset is on the new
              ;; line that follows it — not inside the statement. Use
              ;; strict < on the upper bound so we don't accidentally
              ;; treat a completed sibling as the in-progress one and
              ;; skip it from collection.
              (let [[from to] (:span s)]
                (and (>= offset from) (< offset to))))
            (walk-stmt [s]
              (when-not (stmt-contains-cursor? s)
                (doseq [c (:children s)]
                  (cond
                    (= :scalar (:node c))
                    (when-let [id (some #(when (= :identifier (:node %)) %)
                                        (:children c))]
                      (vswap! attrs conj (:text id)))

                    (#{:relation :count-child} (:node c))
                    ;; Aliased relations can repeat — skip them.
                    (when-not (some #(= :alias (:node %)) (:children c))
                      (when-let [id (some #(when (= :identifier (:node %)) %)
                                          (:children c))]
                        (vswap! rels conj (:text id))))))))]
      (run! walk-stmt
            (filter #(= :statement (:node %))
                    (:children scope-parent))))
    {:attrs @attrs :rels @rels}))

(defn- context-at
  [tree resolved ancestors source offset schema root-entity]
  (let [base-stack (cond-> []
                     (and schema root-entity (get-in schema [:entities root-entity]))
                     (conj (get-in schema [:entities root-entity])))
        entity-stack (entity-stack-from-ancestors ancestors offset source schema base-stack)
        current-entity (peek entity-stack)

        prev-word (prev-word-at source offset)
        prev-char (prev-non-whitespace-char source offset)
        ;; If the user already typed `-` / `->` immediately before the
        ;; current word, completion at line-start-like positions should
        ;; emit BARE relation names (no prefix) and skip non-relation
        ;; entries entirely. Computed once and threaded through the
        ;; context map for `options-for-context` to act on.
        word-from (:from (current-word-range source offset))
        marker-before (marker-before-current-word source word-from)
        ;; Already-used siblings in the current scope. Filtered out of
        ;; the suggestion list because XSQL doesn't allow scalar aliases
        ;; and bare relations duplicate to the same response key.
        scope-parent (scope-parent-node tree ancestors source offset)
        siblings (sibling-bare-names scope-parent offset)
        ;; Value-position attr (cursor is past a binary op, expecting a
        ;; literal). Computed once; checked before AST/parens fall-through
        ;; so the engine doesn't propose field names instead of values.
        value-attr (attr-def-for-value-position
                     resolved ancestors current-entity schema)]
    (cond
      ;; Value position after a binary op (= / != / < / <= / > / >=).
      ;; Detected from AST so it works at root level and inside parens.
      value-attr
      {:kind :value-suggestion
       :entity-stack entity-stack
       :attr-def value-attr}

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

      :else
      ;; Look at AST context.
      (let [parens (enclosing-of-tag resolved ancestors :parens)
            ;; Block-form `_args` produces an :arg-list directly under
            ;; :root-args (no Parens). Treat both shapes as "in args".
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
                                   source offset current-entity schema))]
        (cond
          in-args?
          (cond
            (or (= prev-word "asc") (= prev-word "desc"))
            {:kind :arg-list-start :entity-stack entity-stack}

            ;; "(" / "," boundaries only matter inside the parens form.
            ;; In the block form, a fresh line is the equivalent boundary.
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

            :else
            {:kind :arg-list-start :entity-stack entity-stack})

          ;; AggAttr: split by cursor position.
          ;; The colon is consumed silently. attr-id :to is exclusive, so
          ;; offset < attr-id.to means cursor is inside the identifier (attr
          ;; name slot); offset >= attr-id.to means at/past the colon → fn.
          ;; When no identifier has been parsed yet (attr-id nil), the cursor
          ;; is still at the start of the line → attr-name position.
          (enclosing-of-tag resolved ancestors :agg-attr)
          (let [agg-attr-node (enclosing-of-tag resolved ancestors :agg-attr)
                attr-id       (ast/find-child agg-attr-node :identifier)
                in-attr-name? (or (nil? attr-id)
                                  (< offset (second (:span attr-id))))]
            (if in-attr-name?
              {:kind :agg-attr-name :entity-stack entity-stack}
              {:kind :agg-fn        :entity-stack entity-stack}))

          ;; Block-form `_args` body where the parser hasn't built the
          ;; :arg-list yet (mid-typing — only `_args` and an indent so
          ;; far). Detect via indent walk-back through source.
          (inside-args-block-by-indent? source offset)
          {:kind :arg-list-start :entity-stack entity-stack}

          ;; Inside a `_count` body: only relations of the parent
          ;; entity are valid. Suppresses attributes / `_args` / nested
          ;; `_count` / `_agg` / join markers — none of which belong
          ;; here. Excluded when the cursor is inside a count-child's
          ;; parens (those fall through to arg-list logic).
          (and (enclosing-of-tag resolved ancestors :count-block)
               (not parens))
          {:kind :count-block-body :entity-stack entity-stack}

          ;; Inside an `_agg` body, on a header line (the user is
          ;; choosing what to aggregate over) — only relation names of
          ;; the parent entity are valid. We distinguish "header" from
          ;; "agg body" by `:agg-attr`, not `:agg-relation`: while
          ;; typing the header identifier the cursor is already inside
          ;; an :agg-relation node (the parser consumed it eagerly), so
          ;; checking :agg-relation would incorrectly fall through to
          ;; `:block-start` and emit `-`-prefixed labels. `:agg-attr`
          ;; only appears on body lines.
          (and (enclosing-of-tag resolved ancestors :agg-block)
               (not (enclosing-of-tag resolved ancestors :agg-attr))
               (not parens))
          {:kind :agg-block-body :entity-stack entity-stack}

          ;; Indent-based detection — handles mid-typing relations
          ;; whose Block hasn't formed yet. `:marker-before` is threaded
          ;; into both block-start and line-start-root so option
          ;; emission can drop the `-` prefix and filter to relations
          ;; when the user's already typed `-` / `->`. `:siblings` is
          ;; the set of already-used bare attrs/relations at this scope,
          ;; threaded so the option list can drop them (XSQL has no
          ;; scalar aliases, so duplicates are meaningless).
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

              ;; Flush-left (line-indent 0) is the ROOT ENTITY line of a
              ;; rooted query — offer entity names, not fields. (Nested body
              ;; fields at indent ≥1 hit the `:line-start-root` fallback above.)
              :else
              {:kind :root-entity-line
               :marker-before marker-before
               :siblings siblings})))))))

;; ── Option emission ──────────────────────────────────────────────────────

(def ^:private audit-attr-names
  "Columns the Synthigy audit subsystem injects when `:audit/persist`
   is set on an entity. They live on the table and are valid in args
   / order-by / projection — but they aren't part of the user's ERD
   definition, so we omit them from autocomplete to keep the popup
   to what the user actually modeled. Linter still treats them as
   known (they remain in the schema), so a hand-typed `_order_by
   created_on desc` keeps working."
  #{"created_on" "modified_on" "created_by" "modified_by"})

(def ^:dynamic *include-audit?*
  "Bound true by the persistent scope PANEL (`scope-at`), which browses
   the full runtime surface — audit columns included. The transient
   autocomplete POPUP keeps its minimal what-you-modeled filter."
  false)

(def ^:private audit-relation-names
  "Same idea as audit-attr-names but for the FK relations the audit
   subsystem injects (`created_by` / `modified_by` → User). Hide them
   from autocomplete; keep them in the schema so path completion and
   the linter still recognize them when typed."
  #{"created_by" "modified_by"})

(defn- non-audit-attr-keys
  "Sorted attribute keys for autocomplete, minus the audit-injected ones
   (unless *include-audit?* — the scope panel shows everything)."
  [entity-def]
  (->> (:attributes entity-def)
       keys
       (remove (if *include-audit?* #{} audit-attr-names))
       sort))

(defn- non-audit-relation-keys
  "Sorted relation keys for autocomplete, minus the audit-injected ones
   (unless *include-audit?* — the scope panel shows everything)."
  [entity-def]
  (->> (:relations entity-def)
       keys
       (remove (if *include-audit?* #{} audit-relation-names))
       sort))

(defn- numeric-attr-options
  "Attribute options restricted to number-typed fields. Used inside
   `_agg` relation bodies where only numeric attrs are valid."
  [entity-def]
  (when entity-def
    (->> (:attributes entity-def)
         (filter (fn [[a-name a]]
                   (and (= "number" (:type a))
                        (not (audit-attr-names a-name)))))
         (map first)
         sort
         (mapv (fn [a] {:label a :type "attribute" :section "Attributes"})))))

(defn- options-for-entity
  "Attribute + relation options for the given entity. Attributes
   listed before relations to match 80% selection use case.

   `:siblings` (optional) carries `{:attrs #{names} :rels #{names}}`
   already used at the current scope — both sets are filtered out of
   the suggestion list. XSQL doesn't allow scalar aliases (XSQL.md
   line 169) so a duplicated bare attribute is meaningless; bare
   relations duplicate to the same response key for the same reason.

   Each option is tagged with `:section` so the CodeMirror autocomplete
   popup groups them — attrs first, then relations, and structural
   operators (added by the caller) last."
  [entity-def {:keys [relation-prefix relation-detail siblings]}]
  (let [used-attrs (:attrs siblings #{})
        used-rels  (:rels  siblings #{})
        attrs (->> (non-audit-attr-keys entity-def)
                   (remove used-attrs))
        rels  (->> (non-audit-relation-keys entity-def)
                   (remove used-rels))]
    (concat
      (mapv (fn [a] {:label a :type "attribute" :section "Attributes"}) attrs)
      (mapv (fn [r]
              (cond-> {:label (str (or relation-prefix "") r)
                       :type "relation"
                       :section "Relations"}
                relation-detail (assoc :detail relation-detail)))
            rels))))

(defn- bare-relation-options
  "Plain relation options for the parent entity — no `-` / `->`
   prefix, no attributes, no operators. Used when the user has
   already typed a marker, so the popup mustn't re-emit it (would
   land as `->-rel` in the buffer).

   `siblings` (optional) drops relations already used without alias at
   the current scope, mirroring `options-for-entity`."
  ([entity-def] (bare-relation-options entity-def nil))
  ([entity-def siblings]
   (when entity-def
     (let [used (:rels siblings #{})]
       (mapv (fn [r] {:label r :type "relation" :section "Relations"})
             (remove used (non-audit-relation-keys entity-def)))))))

(defn- options-for-context [ctx schema root-entity]
  (let [entity-def (or (peek (:entity-stack ctx))
                       (when (and schema root-entity)
                         (get-in schema [:entities root-entity])))
        ;; When the cursor sits right after a `-` / `->` marker on the
        ;; current line, narrow line-start-like contexts to bare
        ;; relations only — see `marker-before-current-word`.
        marker (:marker-before ctx)]
    (case (:kind ctx)
      :line-start-root
      (if marker
        (bare-relation-options entity-def (:siblings ctx))
        (concat
          ;; Default to `->` (left join). Inner join is recoverable by
          ;; deleting the `>`; the inverse (typing the extra `>` after
          ;; picking `-rel`) is the more annoying motion. Left join is
          ;; also the more-often-correct semantic when the relation is
          ;; nullable, which is the majority case for o2m/m2m.
          (when entity-def
            (options-for-entity entity-def {:relation-prefix "->"
                                            :siblings (:siblings ctx)}))
          [{:label "_args"  :type "keyword" :section "Operators" :detail "root filters"      :apply :block-opener}
           {:label "_count" :type "keyword" :section "Operators" :detail "aliased counters"  :apply :block-opener}
           {:label "_agg"   :type "keyword" :section "Operators" :detail "aggregates"        :apply :block-opener}]))

      ;; The flush-left root line of a rooted query: suggest entity names.
      :root-entity-line
      (mapv (fn [e] {:label e :type "class" :section "Entities" :detail "root entity"})
            (sort (keys (:entities schema))))

      :block-start
      (if (:in-agg-body? ctx)
        ;; Inside an _agg relation body: only numeric attrs are valid here.
        (numeric-attr-options entity-def)
        (if marker
          (bare-relation-options entity-def (:siblings ctx))
          (concat
            (when entity-def
              (options-for-entity entity-def {:relation-prefix "->"
                                              :siblings (:siblings ctx)}))
            [{:label "_count" :type "keyword" :section "Operators" :detail "aliased counters" :apply :block-opener}
             {:label "_agg"   :type "keyword" :section "Operators" :detail "aggregates"       :apply :block-opener}])))

      :arg-list-start
      (concat
        (when entity-def
          (options-for-entity entity-def {:relation-detail "path hop"}))
        (mapv (fn [k] {:label k :type "keyword" :section "Operators"}) meta-keys))

      :after-attr-in-pred
      (let [type (get-in ctx [:attr-def :type])
            allowed (or (ops-by-type type) (ops-by-type "string"))]
        (mapv (fn [op] {:label op :type "operator"}) allowed))

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

      ;; Inside `_count` body: bare relation names of the parent entity.
      ;; No `-` / `->` prefix, no attribute spam, no nested operators.
      :count-block-body
      (when entity-def
        (mapv (fn [r]
                {:label r :type "relation" :section "Relations"})
              (sort (keys (:relations entity-def)))))

      ;; Inside `_agg` body (header level): bare relation names of the
      ;; parent entity. Same shape as :count-block-body — no markers,
      ;; no attributes (those belong on the indented body lines under
      ;; each header).
      :agg-block-body
      (when entity-def
        (mapv (fn [r]
                {:label r :type "relation" :section "Relations"})
              (sort (keys (:relations entity-def)))))

      ;; Inside an agg-attr, cursor is still on the attribute name (before
      ;; the colon). entity-stack-from-ancestors has already resolved the
      ;; agg-relation's target entity, so entity-def is that target.
      :agg-attr-name
      (numeric-attr-options entity-def)

      :agg-fn
      (mapv (fn [f] {:label f :type "function"}) agg-fns)

      :order-dir
      [{:label "asc" :type "constant"} {:label "desc" :type "constant"}]

      :value-suggestion
      (case (get-in ctx [:attr-def :type])
        "boolean" [{:label "true"  :type "constant"}
                   {:label "false" :type "constant"}]
        ;; Other types (string / number / timestamp / enum) need literals
        ;; the user types themselves — empty completion suppresses the
        ;; popup so the field-name list stops appearing.
        [])

      [])))

;; ── Op-specific option filters (multimethod) ────────────────────────────
;;
;; Per-op completion adjustments layer on top of the generic option
;; list. Methods take `[op ctx options entity-def]` and return a new
;; option vector. New ops register a `defmethod`; nothing else changes.

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
    ;; Root level: hide `_args` (no root args allowed for get); mark
    ;; unique-constrained attributes so the user sees which ones can
    ;; carry an identity predicate. Relations, `_count`, `_agg`, and
    ;; non-unique attributes (which can still be bare-projected) stay.
    :line-start-root
    (let [unique? (fn [name]
                    (boolean (get-in entity-def [:attributes name :unique])))
          has-unique-info? (some (fn [[_ a]] (:unique a))
                                 (:attributes entity-def))]
      (->> options
           (remove #(= "_args" (:label %)))
           (mapv (fn [opt]
                   (if (and has-unique-info?
                            (= "attribute" (:type opt))
                            (unique? (:label opt)))
                     (assoc opt :detail "unique — identity")
                     opt)))))

    ;; Other contexts (block-start inside relation, arg-list-start
    ;; inside relation parens, value-suggestion, path continuation,
    ;; etc.) are part of the unrestricted selection tree — fall back
    ;; to the default option list.
    options))

;; ── Public entry ─────────────────────────────────────────────────────────

(defn complete
  "Compute completion options at `offset` in `source`. `:op` is the
   wire op string (\"search\" / \"get\" / …); when provided, op-specific
   filters apply (see `op-complete-options`)."
  [{:keys [source offset schema root-entity op]
    :or {offset 0}}]
  (let [tree (parser/parse (or source ""))
        ;; Rooted XSQL is self-describing: prefer the entity at the query
        ;; root over the (legacy) external arg. Falls back to the arg for a
        ;; bodyless source (the empty-editor / first-keystroke case).
        root-entity (or (some-> (:root-entity tree) :text) root-entity)
        [resolved ancestors] (innermost-with-ancestors tree offset)
        ctx (context-at tree resolved ancestors source offset schema root-entity)
        entity-def (or (peek (:entity-stack ctx))
                       (when (and schema root-entity)
                         (get-in schema [:entities root-entity])))
        options (vec (options-for-context ctx schema root-entity))
        options (op-complete-options op ctx options entity-def)
        {:keys [from to]} (current-word-range (or source "") offset)]
    {:from from :to to :options options}))

;; ── Persistent scope panel (as opposed to token-position completion) ────
;;
;; `complete` resolves ONE token position — right for a popup, wrong for a
;; panel that should stay stable while the cursor wanders around inside the
;; same block. `scope-at` buckets the same `context-at` resolution into two
;; kinds a persistent panel actually wants:
;;   :args  — cursor is inside `(...)`, after a comparison operator, or a
;;            keyword tail (is/not/like/in) — genuinely position-sensitive,
;;            same options `complete` would offer here.
;;   :scope — anywhere else in an entity/relation body — the FULL
;;            attribute/relation set of whatever's in scope (the caller
;;            renders a stable toggle-list from `:entity-def`, checked
;;            against `:siblings`), not a position-narrowed list.
;;   :root  — flush-left root-entity line — offer entity names.

(def ^:private args-kinds
  #{:value-suggestion :after-is :after-is-not :after-not :after-like
    :after-in :arg-list-start :after-attr-in-pred :path-continuation
    :agg-fn :order-dir})

(defn scope-at
  [{:keys [source offset schema root-entity]}]
  (let [tree (parser/parse (or source ""))
        root-entity (or (some-> (:root-entity tree) :text) root-entity)
        [resolved ancestors] (innermost-with-ancestors tree offset)
        ctx (context-at tree resolved ancestors source offset schema root-entity)
        entity-stack (:entity-stack ctx)
        current-entity (peek entity-stack)
        ;; Indent for a NEW sibling at this scope. NOT derived from
        ;; `(count entity-stack)` — context-at's indent-based fallback
        ;; (the path that fires for a blank child line whose :block
        ;; hasn't parsed into real AST content yet — the common case
        ;; right after auto-descending into a relation) returns
        ;; `:entity-stack [target]`, just the target ALONE, discarding
        ;; the parent chain — so stack depth only reflects true nesting
        ;; on the OTHER resolution path. The cursor's own physical line
        ;; indent is reliable either way: read straight from the source,
        ;; matching whatever's actually already indented there (by hand
        ;; or by our own auto-descend).
        indent (max 2 (current-line-indent-text source offset))
        ;; Sitting on a relation's OWN header — before its :block has
        ;; opened, or just parked on the declaration line — still means
        ;; "I'm looking at this relation": resolve to ITS target instead
        ;; of the parent's scope, so the panel offers its own
        ;; attrs/relations without requiring the cursor to be a line
        ;; deeper. Only overrides a plain "declare a sibling here"
        ;; result (:block-start / :line-start-root); anything more
        ;; specific (args, agg body, …) is left alone. This is additive
        ;; to `scope-at` only — `context-at`/`entity-stack-from-ancestors`
        ;; (shared with real autocomplete) are untouched.
        ;; Only the relation's own HEADER LINE counts — a blank line
        ;; inside its block also has the relation as an AST ancestor
        ;; (blank lines stay in the block until non-blank content at
        ;; lower indent appears), but scope there is decided by indent:
        ;; dedenting a trailing blank line must move the panel OUT of
        ;; the relation, not re-trigger this override.
        header-rel (when (#{:block-start :line-start-root} (:kind ctx))
                     (when-let [rel (enclosing-of-tag resolved ancestors :relation)]
                       (when (= (line-start-offset source offset)
                                (line-start-offset source (first (:span rel))))
                         rel)))
        header-target (when header-rel
                        (let [rel-name (relation-name-of header-rel)
                              rel-def (get-in current-entity [:relations rel-name])
                              target (when rel-def (get-in schema [:entities (:target rel-def)]))]
                          ;; Only a genuine override if context-at's OWN
                          ;; resolution hasn't already descended into this
                          ;; relation — its indent-heuristic can beat us to
                          ;; it for a blank child line one level deeper.
                          ;; Applying the override on top of that would
                          ;; double-descend and compute one indent level
                          ;; too many.
                          (when (and target (not= target current-entity))
                            target)))
        ;; Same idea, one level up: sitting on the ROOT entity line once
        ;; it's a complete, real schema entity means "I'm looking at
        ;; this entity" — show its attrs/relations, not the entity
        ;; picker. Only fires when `root-entity` resolves to a genuine
        ;; schema match (mid-typing an unrecognized name still falls
        ;; through to the entity list below, so it stays usable for
        ;; picking/renaming the root).
        root-target (when (and (= :root-entity-line (:kind ctx)) (seq root-entity))
                      (get-in schema [:entities root-entity]))
        ;; `ctx`'s own `:siblings` comes from `scope-parent-node`, which
        ;; walks up to the nearest REAL `:block` ancestor. When the
        ;; indent-heuristic fallback resolved us into a target whose OWN
        ;; block hasn't parsed into real content yet (stack truncated to
        ;; `[target]`, length 1, and that target isn't actually the root
        ;; entity), the nearest real block is the PARENT's — so `:siblings`
        ;; would silently attribute the parent's children to this scope.
        ;; A same-named attribute one level up (e.g. both entities happen
        ;; to have "name") would then show as already-used here when it
        ;; isn't. Safer to report no siblings than wrong ones — an empty
        ;; child scope is also exactly what our own auto-descend leaves
        ;; behind, so this is the common case, not a rare corner.
        indent-heuristic-only? (and (= 1 (count entity-stack))
                                    (not= current-entity
                                          (get-in schema [:entities root-entity])))
        ;; When the cursor sits inside an enclosing relation's REAL parsed
        ;; :block, that block's own statements are the authoritative
        ;; sibling set — same source the header-target branch uses. The
        ;; empty-siblings guard below exists for the indent-heuristic
        ;; fallback (blank child line, no parsed block yet), but the
        ;; indent path also wins for fully-parsed relation bodies —
        ;; where wiping siblings made every present attribute show
        ;; unchecked (and let duplicates in).
        enclosing-rel-block (when-let [rel (enclosing-of-tag resolved ancestors :relation)]
                              (when-let [block (ast/find-child rel :block)]
                                (let [[from to] (:span block)
                                      body-ind (block-body-indent block source)]
                                  ;; Same cursor-column hint as
                                  ;; entity-stack-from-ancestors: a blank
                                  ;; line dedented BELOW the body indent
                                  ;; is logically outside this block even
                                  ;; though its span still contains it.
                                  (when (and (>= offset from) (<= offset to)
                                             (not (and body-ind
                                                       (< (current-line-indent-text source offset)
                                                          body-ind))))
                                    block))))
        safe-siblings (cond
                        enclosing-rel-block (sibling-bare-names enclosing-rel-block offset)
                        indent-heuristic-only? {:attrs #{} :rels #{}}
                        :else (:siblings ctx))]
    (cond
      header-target
      {:mode :scope
       :entity-def header-target
       :siblings (if-let [block (ast/find-child header-rel :block)]
                   (sibling-bare-names block offset)
                   {:attrs #{} :rels #{}})
       :indent (+ 2 (current-line-indent-text source offset))}

      root-target
      {:mode :scope
       :entity-def root-target
       ;; The query node's own :children ARE the body statements (no
       ;; wrapping :block — see parser.cljc's Query production), so it
       ;; doubles as its own scope-parent for sibling lookup.
       :siblings (sibling-bare-names tree offset)
       :indent indent}

      :else
      (case (:kind ctx)
        :root-entity-line
        {:mode :root :entities (vec (sort (keys (:entities schema))))}

        :count-block-body
        {:mode :scope :entity-def current-entity :siblings safe-siblings
         :relations-only? true :indent indent}

        :agg-block-body
        {:mode :scope :entity-def current-entity :siblings safe-siblings
         :relations-only? true :indent indent}

        :agg-attr-name
        {:mode :scope :entity-def current-entity :numeric-only? true :indent indent}

        (if (contains? args-kinds (:kind ctx))
          {:mode :args :options (binding [*include-audit?* true]
                                  (vec (options-for-context ctx schema root-entity)))}
          {:mode :scope :entity-def current-entity :siblings safe-siblings :indent indent})))))
