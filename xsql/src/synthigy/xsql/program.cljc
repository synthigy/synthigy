(ns synthigy.xsql.program
  "Operation-document PROGRAM layer for XSQL — first-class `@`-declarations.

   A buffer is a PROGRAM of one or more operations. Each operation is:

       @search top_movies            ; @<op> <name>   (name required)
       @description Top movies         ; optional; MULTILINE via indented
         since a given year.           ; continuation (ends at next flush-left line)
       movie                           ; root entity + rooted body — snake_case,
         title                         ; like every other XSQL identifier
         _args (release_year >= 2000)

   `@sql` is the exception: `@sql name` + optional @description, then a RAW SQL
   body (no root entity — passed through opaque).

   XSQL is the query language for READ + DESTROY ops only:
   search / get / sql / slice / purge. Mutations (sync/stack/delete) are NOT
   XSQL — they carry no operation document.

   This namespace OWNS declarations so consumers (lint/complete/compile) never
   strip headers themselves. The rooted-body grammar is unchanged: each op's
   body is handed to the existing parser/compiler/linter with offset tracking
   so positions map back to the full buffer."
  (:refer-clojure :exclude [compile])
  (:require [clojure.string :as str]
            [synthigy.xsql.compile :as compile-impl]
            [synthigy.xsql.lint :as lint-impl]
            [synthigy.xsql.complete :as complete-impl]))

(def doc-verbs
  "Op verbs that form XSQL operation documents (read + destroy)."
  #{"search" "get" "sql-template" "slice" "purge"})

(def mutate-verbs
  "Mutation verbs — body is `?var:Entity[]` (a data variable carrying the
   entity), not a rooted XSQL query. Data is filled at run time."
  #{"sync" "stack" "delete"})

(def ^:private raw-verbs
  "Ops whose body is raw (not rooted XSQL)."
  #{"sql-template"})

;; ── Line offsets ──────────────────────────────────────────────────────────

(defn- lines-with-offsets
  "Vector of `[line-text start-offset]` for each line of `source` (text has no
   trailing newline; offset is the char index of the line's first char)."
  [source]
  (loop [s source, off 0, acc []]
    (if-let [nl (str/index-of s "\n")]
      (recur (subs s (inc nl)) (+ off nl 1) (conj acc [(subs s 0 nl) off]))
      (conj acc [s off]))))

(defn- flush-op-line?
  "True if `text` is a flush-left `@<doc-verb>` or `@batch` line — a declaration
   boundary. `@description` and indented `@` lines are NOT boundaries."
  [text]
  ;; verbs may contain hyphens (e.g. `@sql-template`).
  (when-let [m (re-find #"^@([\w-]+)" text)]
    (boolean (or (doc-verbs (nth m 1)) (mutate-verbs (nth m 1)) (= "batch" (nth m 1))))))

(defn- indented? [text] (boolean (re-find #"^\s" text)))

;; ── Chunking ────────────────────────────────────────────────────────────────

(defn- chunk-ops
  "Split lines into per-operation chunks at each flush-left `@<doc-verb>`.
   Leading content before the first declaration becomes an anonymous chunk."
  [lines]
  (loop [ls lines, chunks [], cur []]
    (if (empty? ls)
      (if (seq cur) (conj chunks cur) chunks)
      (let [[text _ :as line] (first ls)]
        (if (flush-op-line? text)
          (recur (rest ls) (if (seq cur) (conj chunks cur) chunks) [line])
          (recur (rest ls) chunks (conj cur line)))))))

;; ── Per-operation parse ──────────────────────────────────────────────────────

(def ^:private header-directives
  "Op-doc HEADER declarations — appear after the @verb line, before the body.
   NOT operation boundaries. They carry codegen/runtime identity + metadata:
     @namespace → grouping (FP namespace / OOP static facade)
     @watch     → emit a live watch variant (bare flag, or sql-template entities)
     @returns   → sql-template result columns (the only op kind with no derivable shape)
     @description → human doc (multiline)."
  #{"description" "namespace" "watch" "returns"})

(defn- parse-header
  "Consume the header-directive lines after the @verb declaration, stopping at
   the first non-directive line (the body root). `@description` absorbs its
   indented/blank continuation. Returns
   `[{:description :namespace :watch :returns} remaining-lines]`."
  [lines]
  (loop [ls lines, h {}]
    (let [[text _] (first ls)
          dir      (when text (second (re-find #"^@([\w-]+)" text)))]
      (cond
        (not (header-directives dir)) [h ls]

        (= dir "description")
        (let [head  (str/trim (subs text (count "@description")))
              cont  (take-while (fn [[t _]] (or (str/blank? t) (indented? t))) (rest ls))
              text* (->> (cons head (map (comp str/trim first) cont))
                         (remove str/blank?)
                         (str/join " "))]
          (recur (drop (count cont) (rest ls))
                 (cond-> h (seq text*) (assoc :description text*))))

        :else
        (let [val (str/trim (or (nth (re-find #"^@[\w-]+\s*(.*)$" text) 1 nil) ""))]
          (recur (rest ls)
                 (case dir
                   "namespace" (cond-> h (seq val) (assoc :namespace val))
                   "watch"     (assoc h :watch (if (seq val)
                                                 (vec (remove str/blank? (str/split val #"[\s,]+")))
                                                 true))
                   "returns"   (cond-> h (seq val) (assoc :returns val))
                   h)))))))

(defn- parse-operation [op-lines]
  (let [[[decl-text decl-off] & after-decl] op-lines
        m     (re-find #"^@([\w-]+)(?:\s+(\S+))?" decl-text)
        verb  (nth m 1)
        name  (nth m 2 nil)
        sql?  (boolean (raw-verbs verb))
        mut?  (boolean (mutate-verbs verb))
        decl-to    (+ decl-off (count decl-text))
        [header rest-lines] (parse-header after-decl)
        {:keys [description namespace watch returns]} header
        body-lines (drop-while (fn [[t _]] (str/blank? t)) rest-lines)
        body-off   (if (seq body-lines) (second (first body-lines)) decl-to)
        body       (str/join "\n" (map first body-lines))
        errors     (cond-> []
                     (nil? name)
                     (conj {:severity :error
                            :message  (str "operation @" verb " requires a name")
                            :from decl-off :to decl-to}))]
    (if mut?
      ;; Mutation: the body is a data variable `?records:Entity[]` — the entity
      ;; rides in the type, the records are filled at run time.
      (let [vm       (re-find #"\?(\w+)(?::(\w+))?(\[\])?" body)
            data-var (nth vm 1 nil)
            entity   (nth vm 2 nil)
            array?   (boolean (nth vm 3 nil))]
        {:mutate true :op verb :name name :description description :namespace namespace
         :entity entity :data-var data-var :array? array?
         :body body :body-offset body-off :decl-offset decl-off
         :errors (cond-> errors
                   (nil? data-var)
                   (conj {:severity :error
                          :message (str "@" verb " needs a data variable, e.g. ?records:Entity[]")
                          :from decl-off :to decl-to})
                   (and data-var (nil? entity))
                   (conj {:severity :error
                          :message (str "@" verb " data variable ?" data-var
                                        " must carry the entity, e.g. ?" data-var ":Entity[]")
                          :from decl-off :to decl-to}))})
      {:op verb :name name :description description :sql? sql?
       :namespace namespace :watch watch :returns returns
       ;; Root entity (first body token) — the namespace an op falls under when it
       ;; declares no @namespace. nil for raw @sql bodies (they need @namespace).
       :root (when (and (not sql?) (seq (str/trim (or body ""))))
               (some-> (re-find #"^\s*([\w.-]+)" body) (nth 1)))
       :body body :body-offset body-off :decl-offset decl-off :errors errors})))

(defn- parse-anonymous [chunk-lines]
  (let [[_ off] (first chunk-lines)
        ;; A flush-left @<header-directive> before the first op (e.g. a
        ;; buffer-level @namespace default) is NOT body — drop it so it isn't
        ;; lint-parsed as XSQL. The default itself is picked up by `buffer-namespace`.
        kept (remove (fn [[t _]] (header-directives (second (re-find #"^@([\w-]+)" (or t ""))))) chunk-lines)
        body-lines (drop-while (fn [[t _]] (str/blank? t)) kept)]
    {:op nil :name nil :description nil :sql? false
     :body (str/join "\n" (map first body-lines))
     :body-offset (if (seq body-lines) (second (first body-lines)) off)
     :decl-offset off :errors []}))

(defn- buffer-namespace
  "A flush-left `@namespace <Ident>` appearing before the first operation is the
   buffer-level default namespace for every op that omits its own. nil if none."
  [lines]
  (loop [ls lines]
    (when-let [[text _] (first ls)]
      (cond
        (flush-op-line? text)                    nil
        (re-find #"^@namespace\s+(\S+)" text)     (second (re-find #"^@namespace\s+(\S+)" text))
        :else                                     (recur (rest ls))))))

(defn- parse-batch
  "Parse a `@batch <name>: <op> <op> …` declaration — a named composition that
   references other ops by name. No body. → `{:batch true :name :members …}`."
  [[[text off] & _]]
  (let [m       (re-find #"^@batch\s+(\S+)\s*:\s*(.*)$" text)
        name    (nth m 1 nil)
        members (if m (vec (remove str/blank? (str/split (str/trim (nth m 2 "")) #"\s+"))) [])
        to      (+ off (count text))]
    {:batch true :name name :members members :decl-offset off :end-decl to
     :errors (cond-> []
               (nil? name)
               (conj {:severity :error
                      :message "@batch requires a name (@batch <name>: op op …)"
                      :from off :to to})
               (and name (empty? members))
               (conj {:severity :error
                      :message (str "@batch '" name "' lists no operations")
                      :from off :to to}))}))

(defn parse
  "Parse a buffer into a vector of declaration maps. Operations:
   `{:op :name :description :sql? :body :body-offset :decl-offset :errors}`.
   Batches: `{:batch true :name :members :decl-offset :errors}`.
   `:errors` are declaration-level diagnostics in full-buffer coordinates."
  [source]
  (let [lines      (lines-with-offsets source)
        default-ns (buffer-namespace lines)
        apply-dflt (fn [d] (if (and (:op d) (not (:namespace d)) default-ns)
                             (assoc d :namespace default-ns)
                             d))]
    (mapv (fn [[[text _] :as chunk]]
            (let [dir (when (seq text) (second (re-find #"^@([\w-]+)" text)))]
              (apply-dflt
               (cond
                 (= "batch" dir)                            (parse-batch chunk)
                 (or (doc-verbs dir) (mutate-verbs dir))    (parse-operation chunk)
                 ;; A leading @namespace/@watch/etc. line is buffer-level metadata,
                 ;; not an operation — fall through to anonymous (which strips it).
                 :else                                      (parse-anonymous chunk)))))
          (chunk-ops lines))))

;; ── Program-level lint / compile / complete ─────────────────────────────────

(defn- duplicate-alias-errors
  "Identity is `(namespace, name)`. The namespace an op falls under is its
   declared `@namespace`, else its ROOT ENTITY (the same default the codegen
   applies) — so two bare `@search list` ops over DIFFERENT entities do not
   collide, while two under the same entity do. Case-insensitive, mirroring the
   emitter's case-normalised identifier."
  [ops]
  (let [nsk     (fn [{:keys [namespace root entity]}]
                  (some-> (or namespace root entity) str/lower-case))
        key-of  (fn [{:keys [name] :as op}] [(nsk op) (str/lower-case name)])
        counts  (frequencies (keep #(when (:name %) (key-of %)) ops))]
    (for [{:keys [name namespace root entity decl-offset] :as op} ops
          :when (and name (> (get counts (key-of op) 0) 1))]
      {:severity :error
       :message  (str "duplicate operation '"
                      (when-let [n (or namespace root entity)] (str n "/")) name
                      "' — (namespace, name) must be unique")
       :from decl-offset
       :to   (+ decl-offset 1 (count name))})))

(defn- batch-member-errors
  "A `@batch` must reference operations that exist in the buffer."
  [decls]
  (let [op-names (set (keep #(when-not (:batch %) (:name %)) decls))]
    (for [{:keys [batch name members decl-offset end-decl]} decls
          :when batch
          m members
          :when (not (op-names m))]
      {:severity :error
       :message  (str "@batch '" name "' references unknown operation '" m "'")
       :from decl-offset :to (or end-decl (inc decl-offset))})))

(defn lint
  "Lint every declaration in `source` against `schema`. Declaration errors
   (missing name, duplicate alias, @batch unknown members) plus each rooted
   body's diagnostics, all in full-buffer offsets. `@sql` bodies and `@batch`
   declarations are not XSQL-body-linted."
  [source schema]
  (let [decls (parse source)]
    (vec
     (concat
      (duplicate-alias-errors decls)
      (batch-member-errors decls)
      (mapcat
       (fn [{:keys [op body body-offset sql? mutate errors]}]
         (concat
          errors
          (when (and (not sql?) (not mutate) (seq (str/trim (or body ""))))
            (for [d (lint-impl/lint body schema nil op)]
              (-> d
                  (update :from + body-offset)
                  (update :to + body-offset))))))
       decls)))))

(defn compile
  "Compile every declaration to its wire shape, preserving order. Rooted ops →
   `{:op :name :description :entity :selections :args}`; `@sql` →
   `{:op :name :description :sql <raw>}`; `@batch` passes through unchanged
   (`{:batch true :name :members}`)."
  ([source] (compile source nil))
  ([source params]
   (mapv (fn [{:keys [op name description namespace watch returns body sql? batch mutate] :as decl}]
           (cond
             batch  decl
             mutate decl
             ;; carry identity/metadata only when present — keeps directive-less
             ;; ops byte-identical to the pre-namespace wire shape.
             sql?   (cond-> {:op op :name name :description description :sql body}
                      namespace (assoc :namespace namespace)
                      watch     (assoc :watch watch)
                      returns   (assoc :returns returns))
             :else  (cond-> (merge {:op op :name name :description description}
                                   (compile-impl/compile body op params))
                      namespace (assoc :namespace namespace)
                      watch     (assoc :watch watch))))
         (parse source))))

(defn- op-spans
  "Attach `:end-offset` to each op (where the next op's declaration starts, or
   end of source)."
  [ops source]
  (let [n (count ops)]
    (mapv (fn [i o]
            (assoc o :end-offset
                   (if (< (inc i) n) (:decl-offset (nth ops (inc i))) (count source))))
          (range n) ops)))

(defn op-at-offset
  "The operation whose span contains `offset` (the op the cursor is in), or the
   last op when past the end. nil for an empty program. Used for run-at-cursor."
  [source offset]
  (let [spans (op-spans (parse (or source "")) (or source ""))]
    (or (some (fn [o] (when (and (>= offset (:decl-offset o))
                                 (< offset (:end-offset o))) o))
              spans)
        (last spans))))

(defn op-index-at-offset
  "The 0-based index of the operation the cursor (`offset`) is in — same order
   as `parse`/`compile`. Last op when past the end; 0 for an empty program."
  [source offset]
  (let [spans (op-spans (parse (or source "")) (or source ""))]
    (or (some (fn [[i o]] (when (and (>= offset (:decl-offset o))
                                     (< offset (:end-offset o))) i))
              (map-indexed vector spans))
        (max 0 (dec (count spans))))))

(defn complete
  "Context-aware completion across a program. Finds the operation containing
   `:offset` and completes its rooted body (root derived from the body),
   mapping positions back to the full buffer. Silent inside a declaration /
   description / raw SQL body."
  [{:keys [source offset schema] :or {offset 0}}]
  (let [spans (op-spans (parse (or source "")) (or source ""))
        cur   (or (some (fn [o] (when (and (>= offset (:decl-offset o))
                                           (< offset (:end-offset o))) o))
                        spans)
                  (last spans))]
    (if (and cur (not (:batch cur)) (not (:mutate cur)) (not (:sql? cur))
             (:body-offset cur) (>= offset (:body-offset cur)))
      (let [r (complete-impl/complete
               {:source (:body cur)
                :offset (- offset (:body-offset cur))
                :schema schema
                :root-entity nil
                :op (:op cur)})]
        (-> r
            (update :from + (:body-offset cur))
            (update :to + (:body-offset cur))))
      {:from offset :to offset :options []})))

(defn scope-at
  "Persistent-panel counterpart to `complete` — see `complete.cljc/scope-at`
   for the :args vs :scope distinction. Finds the operation containing
   `:offset`, resolves scope in its isolated body, and threads back
   `:op` (the declaration's own op string) plus `:body-offset`/`:body-end`
   (the buffer span the body occupies — the caller needs this to bound
   its own line edits to THIS declaration only; a same-named attribute
   at the same indent could otherwise match in a different declaration
   sharing the buffer)."
  [{:keys [source offset schema] :or {offset 0}}]
  (let [spans (op-spans (parse (or source "")) (or source ""))
        cur   (or (some (fn [o] (when (and (>= offset (:decl-offset o))
                                           (< offset (:end-offset o))) o))
                        spans)
                  (last spans))]
    (if (and cur (not (:batch cur)) (not (:mutate cur)) (not (:sql? cur))
             (:body-offset cur) (>= offset (:body-offset cur)))
      (assoc (complete-impl/scope-at
              {:source (:body cur)
               :offset (- offset (:body-offset cur))
               :schema schema
               :root-entity nil})
             :op (:op cur)
             :body-offset (:body-offset cur)
             :body-end (:end-offset cur))
      {:mode :none})))

(defn batch-member-ops
  "The compiled ops referenced by the `@batch` named `batch-name`, in member
   order. nil if no such batch. Used to run a batch (its members as a batch)."
  ([source batch-name] (batch-member-ops source batch-name nil))
  ([source batch-name params]
   (let [compiled (compile source params)
         by-name  (into {} (map (juxt :name identity) (remove :batch compiled)))]
     (when-let [b (some #(when (and (:batch %) (= batch-name (:name %))) %) compiled)]
       (vec (keep by-name (:members b)))))))

;; ── Emit (export) ───────────────────────────────────────────────────────────

(defn emit-op
  "Serialise one operation map back to text."
  [{:keys [op name description namespace watch returns body]}]
  (str "@" op (when name (str " " name)) "\n"
       (when (seq description) (str "@description " description "\n"))
       (when (seq namespace) (str "@namespace " namespace "\n"))
       (when watch (str "@watch" (when (sequential? watch) (str " " (str/join ", " watch))) "\n"))
       (when (seq returns) (str "@returns " returns "\n"))
       body (when-not (str/ends-with? (or body "") "\n") "\n")))

(defn emit
  "Serialise a vector of operation maps to a multi-op program."
  [ops]
  (str/join "\n" (map emit-op ops)))
