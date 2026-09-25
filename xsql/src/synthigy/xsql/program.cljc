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

(ns synthigy.xsql.program
  "Operation-document PROGRAM layer for XSQL — first-class `@`-declarations.
   A buffer is a PROGRAM of one or more operations; this ns owns declarations
   so lint/complete/compile never strip headers themselves."
  (:refer-clojure :exclude [compile])
  (:require [clojure.string :as str]
            [synthigy.xsql.compile :as compile-impl]
            [synthigy.xsql.lint :as lint-impl]
            [synthigy.xsql.complete :as complete-impl]
            [synthigy.xsql.parser :as xparser]
            [synthigy.xsql.sql-lint :as sql-lint]
            [synthigy.xsql.sql-params :as sql-params]))

(def doc-verbs
  "Op verbs that form XSQL operation documents (read + destroy)."
  #{"search" "get" "search-tree" "get-tree" "sql-template" "slice" "purge"})

(def tree-verbs
  "Recursive read verbs — walk a self-relation named by `_on` in the root
   parens; return a flat row list (see docs for why recursion is a verb)."
  #{"search-tree" "get-tree"})

(def mutate-verbs
  "Mutation verbs — body is `?var:Entity[]` (a data variable carrying the
   entity), not a rooted XSQL query. Data is filled at run time."
  #{"sync" "stack" "delete"})

(def ^:private raw-verbs
  "Ops whose body is raw (not rooted XSQL)."
  #{"sql-template"})

;; ── Line offsets ──────────────────────────────────────────────────────────

(defn lines-with-offsets
  "Vector of `[line-text start-offset]` for each line of `source`."
  [source]
  (loop [s source, off 0, acc []]
    (if-let [nl (str/index-of s "\n")]
      (recur (subs s (inc nl)) (+ off nl 1) (conj acc [(subs s 0 nl) off]))
      (conj acc [s off]))))

(defn flush-op-line?
  "True if `text` is a flush-left declaration boundary — an op verb or `@batch`."
  [text]
  (when-let [m (re-find #"^@([\w-]+)" text)]
    (boolean (or (doc-verbs (nth m 1)) (mutate-verbs (nth m 1)) (= "batch" (nth m 1))))))

(defn indented? [text] (boolean (re-find #"^\s" text)))

;; ── Chunking ────────────────────────────────────────────────────────────────

(defn chunk-ops
  "Split lines into per-operation chunks at each flush-left `@<doc-verb>`."
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
  "Op-doc header declarations — @namespace/@watch/@returns/@description,
   between the @verb line and the body. Not operation boundaries."
  #{"description" "namespace" "watch" "returns"})

(def ^:private buffer-directives
  "Buffer-level declarations, flush-left before the first operation and
   ignored by codegen: @workspace."
  #{"workspace"})

(defn parse-header
  "Consume header-directive lines after the @verb declaration, stopping at
   the first non-directive line. Returns
   `[{:description :namespace :watch :returns :offsets} remaining-lines]`."
  [lines]
  (loop [ls lines, h {}]
    (let [[text off] (first ls)
          dir        (when text (second (re-find #"^@([\w-]+)" text)))
          mark       (fn [h] (assoc-in h [:offsets dir] [off (+ off (count text))]))]
      (cond
        (not (header-directives dir)) [h ls]

        (= dir "description")
        (let [head (str/trim (subs text (count "@description")))
              to1  (+ off (count text))]
          (if (str/starts-with? head "\"")
            ;; Quoted form: closing quote may sit lines below, newlines
            ;; preserved verbatim, no escape syntax — first `"` closes.
            (let [opened (subs head 1)
                  qi     (str/index-of opened "\"")]
              (if qi
                (recur (rest ls)
                       (cond-> (assoc-in h [:offsets dir] [off to1])
                         (pos? qi) (assoc :description (subs opened 0 qi))))
                (let [[desc-lines closer more]
                      (loop [cont (rest ls), acc []]
                        (if-let [[t _ :as line] (first cont)]
                          (if-let [q (str/index-of t "\"")]
                            [(conj acc (subs t 0 q)) line (rest cont)]
                            (recur (rest cont) (conj acc t)))
                          [acc nil nil]))]
                  (if closer
                    (recur more
                           (-> h
                               (assoc :description
                                      (str/join "\n" (into [opened] desc-lines)))
                               (assoc-in [:offsets dir]
                                         [off (+ (second closer)
                                                 (count (first closer)))])))
                    ;; unterminated — consume the rest of the chunk and flag it
                    [(-> h
                         (assoc :description (str/join "\n" (into [opened] desc-lines)))
                         (assoc-in [:offsets dir] [off to1])
                         (assoc :desc-error
                                {:message "unterminated @description string — missing closing quote"
                                 :from off :to to1}))
                     nil]))))
            ;; Bare (unquoted) form still parses but is flagged.
            (let [cont  (take-while (fn [[t _]] (or (str/blank? t) (indented? t))) (rest ls))
                  text* (->> (cons head (map (comp str/trim first) cont))
                             (remove str/blank?)
                             (str/join " "))]
              (recur (drop (count cont) (rest ls))
                     (mark (cond-> h
                             (seq text*) (assoc :description text*)
                             (seq text*) (assoc :desc-error
                                                {:severity :warning
                                                 :message  "@description should be a quoted string, e.g. @description \"…\""
                                                 :from off :to to1})))))))

        :else
        (let [val (str/trim (or (nth (re-find #"^@[\w-]+\s*(.*)$" text) 1 nil) ""))]
          (recur (rest ls)
                 (mark
                  (case dir
                    "namespace" (cond-> h (seq val) (assoc :namespace val))
                    "watch"     (assoc h :watch (if (seq val)
                                                  (vec (remove str/blank? (str/split val #"[\s,]+")))
                                                  true))
                    "returns"   (cond-> h (seq val) (assoc :returns val))
                    h))))))))

(defn parse-operation [op-lines]
  (let [[[decl-text decl-off] & after-decl] op-lines
        m     (re-find #"^@([\w-]+)(?:\s+(\S+))?" decl-text)
        verb  (nth m 1)
        name  (nth m 2 nil)
        sql?  (boolean (raw-verbs verb))
        mut?  (boolean (mutate-verbs verb))
        decl-to    (+ decl-off (count decl-text))
        [header rest-lines] (parse-header after-decl)
        {:keys [description namespace watch returns offsets desc-error]} header
        anchor     (fn [dir] (get offsets dir [decl-off decl-to]))
        body-lines (drop-while (fn [[t _]] (str/blank? t)) rest-lines)
        ;; Empty body starts past the decl line's newline, never ON it —
        ;; see docs for the completion-guard bug this avoids.
        body-off   (if (seq body-lines) (second (first body-lines)) (inc decl-to))
        body       (str/join "\n" (map first body-lines))
        errors     (cond-> []
                     (nil? name)
                     (conj {:severity :error
                            :message  (str "operation @" verb " requires a name")
                            :from decl-off :to decl-to})
                     desc-error
                     (conj (update desc-error :severity #(or % :error))))]
    (if mut?
      ;; Mutation body is a data variable `?records:Entity[]`.
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
                          :from decl-off :to decl-to})
                   ;; Consumed by parse-header but never used on mutations.
                   watch
                   (conj (let [[from to] (anchor "watch")]
                           {:severity :warning
                            :message (str "@watch is ignored on @" verb
                                          " — mutations have no watch variant")
                            :from from :to to}))
                   returns
                   (conj (let [[from to] (anchor "returns")]
                           {:severity :warning
                            :message (str "@returns is ignored on @" verb
                                          " — mutation results are schema-derived")
                            :from from :to to})))})
      {:op verb :name name :description description :sql? sql?
       :namespace namespace :watch watch :returns returns
       ;; directive name → [from to] of its own line, for anchored edits/lints.
       :header-offsets offsets
       ;; Root entity (first body token) — the namespace an op falls under when
       ;; it
       ;; declares no @namespace. nil for raw @sql bodies (they need
       ;; @namespace — `missing-namespace-errors`).
       :root (when (and (not sql?) (seq (str/trim (or body ""))))
               (some-> (re-find #"^\s*([\w.-]+)" body) (nth 1)))
       :body body :body-offset body-off :decl-offset decl-off
       :errors (cond-> errors
                 ;; Directives that are carried but meaningless on this verb —
                 ;; warn instead of silently ignoring downstream.
                 (and returns (not sql?))
                 (conj (let [[from to] (anchor "returns")]
                         {:severity :warning
                          :message (str "@returns is only used by @sql-template — @"
                                        verb " derives its result shape")
                          :from from :to to}))
                 (and sql? (nil? returns))
                 (conj {:severity :warning
                        :message "@sql-template without @returns — result columns are untyped in generated SDKs"
                        :from decl-off :to decl-to})
                 (and (sequential? watch) (not sql?))
                 (conj (let [[from to] (anchor "watch")]
                         {:severity :warning
                          :message (str "@watch entity list is only used by @sql-template — @"
                                        verb " derives its dependencies; use bare @watch")
                          :from from :to to}))
                 (and watch (tree-verbs verb))
                 (conj (let [[from to] (anchor "watch")]
                         {:severity :warning
                          :message (str "@watch is not supported on @" verb
                                        " — tree walks have no live variant yet")
                          :from from :to to})))})))

(defn parse-anonymous [chunk-lines]
  (let [[_ off] (first chunk-lines)
        ;; Buffer-level directives (picked up by
        ;; buffer-namespace/workspace-name)
        ;; are not body — drop them so they aren't lint-parsed as XSQL.
        kept (remove (fn [[t _]]
                       (let [d (second (re-find #"^@([\w-]+)" (or t "")))]
                         (or (header-directives d) (buffer-directives d))))
                     chunk-lines)
        body-lines (drop-while (fn [[t _]] (str/blank? t)) kept)]
    {:op nil :name nil :description nil :sql? false
     :body (str/join "\n" (map first body-lines))
     :body-offset (if (seq body-lines) (second (first body-lines)) off)
     :decl-offset off :errors []}))

(defn buffer-namespace
  "Flush-left `@namespace <Ident>` before the first op — the buffer-level
   default namespace for ops that omit their own. nil if none."
  [lines]
  (loop [ls lines]
    (when-let [[text _] (first ls)]
      (cond
        (flush-op-line? text)                    nil
        (re-find #"^@namespace\s+(\S+)" text)     (second (re-find #"^@namespace\s+(\S+)" text))
        :else                                     (recur (rest ls))))))

(defn workspace-name
  "Flush-left `@workspace \"<name>\"` before the first op — the buffer's
   console identity. Quoted since the name may contain spaces; an old
   unquoted single-token form still reads. nil if none."
  [source]
  (loop [ls (lines-with-offsets (or source ""))]
    (when-let [[text _] (first ls)]
      (cond
        (flush-op-line? text) nil
        (re-find #"^@workspace\s+\"([^\"]*)\"" text)
        (second (re-find #"^@workspace\s+\"([^\"]*)\"" text))
        (re-find #"^@workspace\s+(\S+)" text)
        (second (re-find #"^@workspace\s+(\S+)" text))
        :else (recur (rest ls))))))

(defn parse-batch
  "Parse a `@batch <name>: <op> <op> …` declaration into `{:batch true
   :name :members …}`."
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
                 :else                                      (parse-anonymous chunk)))))
          (chunk-ops lines))))

(defn watch-toggle-edit
  "Editor transaction `{:from :to :insert :watch}` toggling the `@watch`
   header of the op declared at `decl-offset` (never a content replace —
   the console must keep cursor/undo). nil for `@batch`/mutation ops."
  [source decl-offset]
  (let [source (or source "")
        {:keys [op batch mutate header-offsets] :as decl}
        (some #(when (= decl-offset (:decl-offset %)) %) (parse source))]
    (when (and decl op (not batch) (not mutate))
      (if-let [[from to] (get header-offsets "watch")]
        (let [nl? (and (< to (count source)) (= \newline (nth source to)))]
          {:from from :to (if nl? (inc to) to) :insert "" :watch false})
        (let [decl-end (or (str/index-of source "\n" decl-offset) (count source))]
          (if (= decl-end (count source))
            {:from decl-end :to decl-end :insert "\n@watch" :watch true}
            {:from (inc decl-end) :to (inc decl-end) :insert "@watch\n" :watch true}))))))

;; ── Program-level lint / compile / complete ─────────────────────────────────

(defn op-namespace
  "The namespace an op falls under — its declared `@namespace`, else its root
   entity. The SAME default every emitter applies (bare op = method on the
   root entity's generated class; @namespace = detached module)."
  [{:keys [namespace root entity]}]
  (some-> (or namespace root entity) str/lower-case))

(defn op-identity
  "Qualified `namespace/name` identity string (lower-case), e.g. `movie/list`."
  [{:keys [name] :as op}]
  (str (op-namespace op) "/" (some-> name str/lower-case)))

(defn resolve-batch-member
  "Resolve a `@batch` member reference — bare `name` or qualified `ns/name` —
   against `decls` (parse OR compile output; both carry the identity keys).
   → `{:op <decl>}` | `{:ambiguous [<decl> …]}` | `{:missing true}`."
  [decls ref]
  (let [ops     (filter :name (remove :batch decls))
        [ns nm] (if (str/includes? ref "/")
                  (let [i (str/index-of ref "/")]
                    [(str/lower-case (subs ref 0 i)) (subs ref (inc i))])
                  [nil ref])
        nm      (str/lower-case nm)
        matches (filterv (fn [d]
                           (and (= nm (str/lower-case (:name d)))
                                (or (nil? ns) (= ns (op-namespace d)))))
                         ops)]
    (cond
      (empty? matches)                        {:missing true}
      (and (nil? ns) (> (count matches) 1))   {:ambiguous matches}
      :else                                   {:op (first matches)})))

(defn duplicate-alias-errors
  "Flag ops sharing the same `(namespace, name)` identity, case-insensitive."
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

(defn missing-namespace-ops
  "Ops codegen can't place: a raw `@sql-template` has no root entity, so it needs a `@namespace`."
  [decls]
  (filter #(and (= "sql-template" (:op %)) (not (seq (:namespace %)))) decls))

(defn missing-namespace-errors
  [decls]
  (for [{:keys [name decl-offset]} (missing-namespace-ops decls)]
    {:severity :warning
     :message  (str "@sql-template '" name "' has no root entity — add @namespace "
                    "(e.g. @namespace dashboard) so generated SDKs know where it goes")
     :from decl-offset
     :to   (+ decl-offset (count "@sql-template ") (count name))}))

(defn batch-member-errors
  "A `@batch` member must resolve to exactly one op in the buffer."
  [decls]
  (for [{:keys [batch name members decl-offset end-decl]} decls
        :when batch
        m members
        :let [{:keys [missing ambiguous]} (resolve-batch-member decls m)]
        :when (or missing ambiguous)]
    {:severity :error
     :message  (if missing
                 (str "@batch '" name "' references unknown operation '" m "'")
                 (str "@batch '" name "' — '" m "' is ambiguous: "
                      (str/join ", " (map op-identity ambiguous))
                      " — qualify the reference"))
     :from decl-offset :to (or end-decl (inc decl-offset))}))

(defn directive-errors
  "Flush-left `@` lines that aren't valid declarations at their position
   (unknown, misplaced buffer directive, or detached header directive)."
  [source decls]
  (let [first-op-off (when-let [xs (seq (keep #(when (or (:op %) (:batch %)) (:decl-offset %))
                                              decls))]
                       (apply min xs))
        ;; [from to) span of each raw-SQL body — decl order gives the ends.
        ends         (conj (mapv :decl-offset (rest decls)) (count source))
        sql-spans    (keep identity
                           (map (fn [d end] (when (:sql? d) [(:body-offset d) end]))
                                decls ends))
        in-sql?      (fn [off] (boolean (some (fn [[a b]] (and (>= off a) (< off b))) sql-spans)))
        ;; [decl-offset body-offset) is where an op's header directives live.
        header-spans (keep (fn [{:keys [op decl-offset body-offset]}]
                             (when op [decl-offset body-offset]))
                           decls)
        in-header?   (fn [off] (boolean (some (fn [[a b]] (and (>= off a) (< off b))) header-spans)))
        ;; lines INSIDE a quoted multiline @description are prose, not
        ;; declarations — an `@…` there must not lint as a misplaced directive.
        desc-spans   (keep #(get-in % [:header-offsets "description"]) decls)
        in-desc?     (fn [off] (boolean (some (fn [[a b]] (and (> off a) (<= off b))) desc-spans)))]
    (for [[text off] (lines-with-offsets (or source ""))
          :let [d  (second (re-find #"^@([\w-]+)" text))
                to (+ off (count text))]
          :when (and d
                     (not (doc-verbs d)) (not (mutate-verbs d)) (not= "batch" d)
                     (not (in-sql? off))
                     (not (in-desc? off)))
          :let [;; namespaces live in the entity naming universe — snake_case
                ;; (emitters re-case per language: movie_genre → MovieGenre).
                ns-casing
                (fn []
                  (when (and (= d "namespace")
                             (when-let [v (second (re-find #"^@namespace\s+(\S+)" text))]
                               (not (re-matches #"[a-z][a-z0-9_]*" v))))
                    {:severity :warning
                     :message "@namespace should be snake_case, like entity names (e.g. movie_genre)"
                     :from off :to to}))
                diag
                (cond
                  (not (or (header-directives d) (buffer-directives d)))
                  {:severity :error
                   :message (str "unknown declaration '@" d "'")
                   :from off :to to}

                  (and (buffer-directives d) first-op-off (> off first-op-off))
                  {:severity :error
                   :message (str "@" d " must appear before the first operation")
                   :from off :to to}

                  (and (header-directives d) (not (in-header? off)))
                  (if (or (nil? first-op-off) (< off first-op-off))
                    ;; Before the first op only @namespace means something
                    ;; (the buffer-level default) — the rest attach to nothing.
                    (if (= d "namespace")
                      (ns-casing)
                      {:severity :warning
                       :message (str "@" d " before the first operation attaches to nothing and is ignored")
                       :from off :to to})
                    {:severity :error
                     :message (str "@" d " must sit in an operation header, directly after its @verb line")
                     :from off :to to})

                  :else (ns-casing))]
          :when diag]
      diag)))

;; ── sql-template placeholder validation ──────────────────────────────────────

(def ^:private sql-join-split
  "Join operators inside a `{…}` placeholder — mirrors the server-side
   template resolver's join-pattern. Whitespace is optional: identifiers
   are strict snake_case, so `-` can never be part of a name and
   `{a->b.f}` parses identically to `{a -> b.f}`."
  #"\s*(?:=>|->|<-|-)\s*")

(def ^:private sql-ident-pattern #"[a-z][a-z0-9_]*")

(defn sql-placeholder-spans
  "All `{…}` spans in a raw-SQL body as [open-idx close-idx inner]."
  [^String body]
  (loop [i 0 acc []]
    (if-let [open (str/index-of body "{" i)]
      (if-let [close (str/index-of body "}" (inc open))]
        (recur (inc close) (conj acc [open close (subs body (inc open) close)]))
        acc)
      acc)))

(defn sql-placeholder-errors
  "Validate every `{…}` placeholder of a raw-SQL body against the schema
   (snake_case identifiers, entity/relation/attribute chain must resolve).
   Offsets are body-local; `lint` shifts them. nil-schema → no checks."
  [^String body schema]
  (when (:entities schema)
    (for [[open close inner] (sql-placeholder-spans body)
          :let [chain-src (str/trim inner)
                dot       (str/last-index-of chain-src ".")
                field     (when dot (subs chain-src (inc dot)))
                segs      (let [v (str/split (if dot (subs chain-src 0 dot) chain-src)
                                             sql-join-split -1)]
                            (if (seq v) v [""]))
                snake     (fn [s]
                            (when-not (re-matches sql-ident-pattern (or s ""))
                              (str "'" s "' — identifiers are snake_case"
                                   (let [lc (str/lower-case (or s ""))]
                                     (when (get-in schema [:entities lc])
                                       (str "; use \"" lc "\""))))))
                diag      (or
                           (some snake segs)
                           (when (and field (not (str/starts-with? field "_")))
                             (when-not (re-matches sql-ident-pattern field)
                               (str "'" field "' — identifiers are snake_case")))
                           (loop [ename (first segs) rels (rest segs)]
                             (cond
                               (not (get-in schema [:entities ename]))
                               (str "unknown entity '" ename "'")

                               (seq rels)
                               (if-let [target (get-in schema [:entities ename :relations
                                                               (first rels) :target])]
                                 (recur target (rest rels))
                                 (str "entity '" ename "' has no relation '" (first rels) "'"))

                               (and field
                                    (not (str/starts-with? field "_"))
                                    (not (get-in schema [:entities ename :attributes field])))
                               (str "entity '" ename "' has no attribute '" field "'"))))]
          :when diag]
      {:severity :error :message diag :from open :to (inc close)})))

(declare sql-output-columns)

(defn sql-restriction-set-warnings
  "Warn (not error) when a `?name:order(cols…)` restriction-set entry
   doesn't match a detected output column — `sql-output-columns` is a
   regex heuristic that can miss legitimate columns, so a false positive
   here must never block Execute."
  [^String body]
  (let [known (set (sql-output-columns body))]
    (when (seq known)
      (for [{:keys [kind type type-args from to]} (sql-params/scan-placeholders body)
            :when (and (= :named kind) (= :order type) (seq type-args))
            col type-args
            :when (not (contains? known col))]
        {:severity :warning
         :message (str "'" col "' is not one of this query's output columns ("
                       (str/join ", " (sort known)) ")")
         :from from :to to}))))

(defn sql-param-errors
  "Wire `sql-params/analyze`'s param-level checks into the console's live
   lint for `@sql-template` bodies (previously server-only)."
  [^String body]
  (let [{:keys [errors warnings]} (sql-params/analyze body)]
    (concat
     (map #(assoc % :severity :error) errors)
     (map #(assoc % :severity :warning) warnings)
     (sql-restriction-set-warnings body))))

(defn lint
  "Lint every declaration in `source` against `schema`. Declaration errors
   (missing name, duplicate alias, @batch unknown members, misplaced/unknown
   directives) plus each rooted body's diagnostics, all in full-buffer
   offsets. `@sql` bodies get placeholder validation (every `{…}` chain
   checked against the schema) instead of XSQL body lint; `@batch`
   declarations get neither."
  [source schema]
  (let [decls (parse source)]
    (vec
     (concat
      (duplicate-alias-errors decls)
      (missing-namespace-errors decls)
      (batch-member-errors decls)
      (directive-errors source decls)
      (mapcat
       (fn [{:keys [op body body-offset sql? mutate errors]}]
         (concat
          errors
          (when (and sql? (seq (str/trim (or body ""))))
            (for [d (concat (sql-placeholder-errors body schema)
                            (sql-param-errors body)
                            (sql-lint/lint body))]
              (-> d
                  (update :from + body-offset)
                  (update :to + body-offset))))
          (when (and (not sql?) (not mutate) (seq (str/trim (or body ""))))
            (for [d (lint-impl/lint body schema nil op)]
              (-> d
                  (update :from + body-offset)
                  (update :to + body-offset))))))
       decls)))))

(defn lift-tree-shape
  "Tree ops carry `on` (and `root` for @get-tree) as top-level wire keys,
   not args; lift `:_on` out of :args, and for @get-tree the root-parens
   identity becomes wire :root with args dropped entirely."
  [op {:keys [args] :as compiled}]
  (let [on    (:_on args)
        args' (not-empty (dissoc args :_on))]
    (if (= "get-tree" op)
      (let [root (some (fn [k] (get-in args' [k :_eq])) [:xid :euuid])]
        (cond-> (dissoc compiled :args)
          on   (assoc :on on)
          root (assoc :root root)))
      (cond-> (assoc compiled :args (or args' {}))
        on (assoc :on on)))))

(defn parse-compiled
  "Parse a document into declaration maps with each rooted body PRE-PARSED
   to an AST under `:body-ast` — the cacheable half of `compile`. The
   ASTs are immutable; `compile-parsed` binds params per call, so one
   parse serves every request for the same document."
  [source]
  (mapv (fn [decl]
          (if (and (:op decl) (not (:sql? decl)) (not (:batch decl))
                   (not (:mutate decl)))
            (assoc decl :body-ast (xparser/parse (:body decl)))
            decl))
        (parse source)))

(defn compile-parsed
  "Bind `params` into a `parse-compiled` result. Identical output to
   `compile` on the same source."
  [decls params]
  (mapv (fn [{:keys [op name description namespace watch returns body body-ast sql? batch mutate] :as decl}]
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
                                  (let [compiled (compile-impl/compile-ast body-ast op params)]
                                    (if (tree-verbs op)
                                      (lift-tree-shape op compiled)
                                      compiled)))
                     namespace (assoc :namespace namespace)
                     watch     (assoc :watch watch))))
        decls))

(defn compile
  "Compile every declaration to its wire shape, preserving order. Rooted ops →
   `{:op :name :description :entity :selections :args}`; tree ops add
   top-level `:on` (and `:root` for get-tree); `@sql` →
   `{:op :name :description :sql <raw>}`; `@batch` passes through unchanged
   (`{:batch true :name :members}`)."
  ([source] (compile source nil))
  ([source params]
   (compile-parsed (parse-compiled source) params)))

(defn op-spans
  "Attach `:end-offset` to each op (next op's decl start, or end of source)."
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

(def ^:private verb-options
  "Completion options for the `@`-directive slot of a declaration line.
   The op NAME after the verb is free-form (a codegen identifier) and
   never completed."
  [{:label "@search"       :type "keyword" :section "Operations" :detail "query — matching rows"}
   {:label "@get"          :type "keyword" :section "Operations" :detail "query — one row by identity"}
   {:label "@search-tree"  :type "keyword" :section "Operations" :detail "query — matches + ancestors via _on"}
   {:label "@get-tree"     :type "keyword" :section "Operations" :detail "query — root + descendants via _on"}
   {:label "@sql-template" :type "keyword" :section "Operations" :detail "raw SQL, {Entity.field} placeholders"}
   {:label "@slice"        :type "keyword" :section "Operations" :detail "destroy — slice matched rows"}
   {:label "@purge"        :type "keyword" :section "Operations" :detail "destroy — delete matched rows"}
   {:label "@sync"         :type "keyword" :section "Operations" :detail "mutate — declarative records"}
   {:label "@stack"        :type "keyword" :section "Operations" :detail "mutate — additive records"}
   {:label "@delete"       :type "keyword" :section "Operations" :detail "mutate — by ids"}
   {:label "@description"  :type "keyword" :section "Directives" :detail "document the op above"}
   {:label "@namespace"    :type "keyword" :section "Directives" :detail "codegen namespace"}
   {:label "@returns"      :type "keyword" :section "Directives" :detail "sql-template result columns — name:type, …"}
   {:label "@watch"        :type "keyword" :section "Directives" :detail "sql-template live deps — entity list"}
   {:label "@workspace"    :type "keyword" :section "Directives" :detail "console workspace — e.g. @workspace \"My Queries\""}
   {:label "@batch"        :type "keyword" :section "Directives" :detail "named composition of ops"}])

(def ^:private scalar-type-names
  "Bind-param scalar type vocabulary — mirrors sql-params/canonical-types
   minus `order`, which is never a plain scalar (see `xsql-param-type-completion`)."
  ["int" "float" "string" "boolean" "timestamp" "uuid"])

(def ^:private returns-type-options
  "Type vocabulary for `@returns name:type` annotations."
  (mapv (fn [t] {:label t :type "type" :detail "@returns column type"})
        scalar-type-names))

(defn returns-type-completion
  "Type vocabulary when the cursor sits right after `:` on a flush-left
   `@returns` line, else nil."
  [^String source offset]
  (let [ls (loop [i offset]
             (if (and (pos? i) (not= \newline (.charAt source (dec i))))
               (recur (dec i)) i))
        line-to-cursor (subs source ls offset)]
    (when (re-find #"^@returns\s" line-to-cursor)
      (when-let [[_ typed] (re-find #":\s*([A-Za-z_][A-Za-z0-9_]*)?$" line-to-cursor)]
        (let [typed (or typed "")]
          {:from (- offset (count typed)) :to offset
           :options returns-type-options})))))

(defn mutate-type-completion
  "Entity names after `?var:` in a mutation body, inserted as `entity[]`."
  [^String source offset cur schema]
  (when (and (:mutate cur) schema (:body-offset cur) (>= offset (:body-offset cur)))
    (let [ls (loop [i offset]
               (if (and (pos? i) (not= \newline (.charAt source (dec i))))
                 (recur (dec i)) i))]
      (when-let [[_ typed] (re-find #"^\s*\?\w+:([A-Za-z0-9_]*)$" (subs source ls offset))]
        {:from (- offset (count typed)) :to offset
         :options (mapv (fn [e] {:label e :type "type" :detail "records" :insert (str e "[]")})
                        (sort (keys (:entities schema))))}))))

(defn decl-verb-completion
  "Verb/directive vocabulary when the cursor sits inside the leading
   `@word` token of a flush-left line, else nil."
  [^String source offset]
  (let [ls (loop [i offset]
             (if (and (pos? i) (not= \newline (.charAt source (dec i))))
               (recur (dec i)) i))]
    (when (and (< ls (count source)) (= \@ (.charAt source ls)))
      (let [n (count source)
            tok-end (loop [i (inc ls)]
                      (if (and (< i n)
                               (re-matches #"[\w-]" (str (.charAt source i))))
                        (recur (inc i)) i))]
        (when (<= offset tok-end)
          {:from ls :to tok-end :options verb-options})))))

;; ── sql-template body completion ─────────────────────────────────────────────

(defn sql-chain-target
  "Resolve a placeholder chain [root rel …] to the entity it lands on,
   or nil when any hop is unknown."
  [schema [root & rels]]
  (reduce (fn [ename rel]
            (or (get-in schema [:entities ename :relations rel :target])
                (reduced nil)))
          (when (get-in schema [:entities root]) root)
          rels))

(defn sql-entity-options [schema]
  (mapv (fn [ename] {:label ename :type "class" :detail "entity"})
        (sort (keys (:entities schema)))))

(defn sql-relation-options [schema ename]
  (mapv (fn [[rname {:keys [target cardinality]}]]
          {:label rname :type "property" :section "Relations"
           :detail (str target (when cardinality (str " · " cardinality)))})
        (sort-by key (get-in schema [:entities ename :relations]))))

(defn sql-attribute-options [schema ename]
  (mapv (fn [[aname adef]]
          {:label aname :type "property" :section "Attributes"
           :detail (or (:type adef) "string")})
        (sort-by key (get-in schema [:entities ename :attributes]))))

(defn sql-placeholder-completion
  "Completion inside an unclosed `{…}` placeholder: entity names at `{`,
   relations after a join operator, attributes after `.`; an unknown
   hop goes silent rather than guessing."
  [^String body offset schema]
  (let [prefix (subs body 0 offset)
        open   (str/last-index-of prefix "{")
        close  (str/last-index-of prefix "}")]
    (when (and open
               (or (nil? close) (< close open))
               (not (str/includes? (subs prefix open) "\n")))
      (let [inner    (str/triml (subs prefix (inc open)))
            pending  (re-find #"\s*(?:=>|->|<-|-)\s*$" inner)
            base     (if pending
                       (subs inner 0 (- (count inner) (count pending)))
                       inner)
            ;; JVM and JS differ on splitting ""/trailing empties — pin [""]
            segs     (let [v (str/split base sql-join-split -1)]
                       (if (seq v) v [""]))
            last-seg (peek segs)
            dot      (when (and (not pending) last-seg)
                       (str/index-of last-seg "."))]
        (cond
          pending
          (when-let [target (sql-chain-target schema segs)]
            {:from offset :to offset
             :options (sql-relation-options schema target)})

          dot
          (let [typed (subs last-seg (inc dot))
                chain (conj (pop segs) (subs last-seg 0 dot))]
            (when-let [target (sql-chain-target schema chain)]
              {:from (- offset (count typed)) :to offset
               :options (sql-attribute-options schema target)}))

          (= 1 (count segs))
          {:from (- offset (count last-seg)) :to offset
           :options (sql-entity-options schema)}

          :else
          (when-let [target (sql-chain-target schema (pop segs))]
            {:from (- offset (count last-seg)) :to offset
             :options (sql-relation-options schema target)}))))))

(defn sql-output-columns
  "Output-column names of a raw-SQL body: `AS` aliases plus bare
   `{entity.field}` items directly followed by a comma, newline, or EOF."
  [body]
  (distinct
   (concat
    (map second (re-seq #"(?i)\bas\s+([A-Za-z_][A-Za-z0-9_]*)" body))
    (map second (re-seq #"\{[^{}]*\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\s*(?=,|\n|$)" body)))))

(defn sql-clause-completion
  "After an ORDER BY / GROUP BY in a raw-SQL body, offer output columns.
   Keyword detection is last-match, not paren-depth aware — harmless,
   these are suggestions, not validation."
  [^String body offset]
  (let [prefix (subs body 0 offset)
        typed  (or (re-find #"[A-Za-z_][A-Za-z0-9_]*$" prefix) "")
        before (subs prefix 0 (- (count prefix) (count typed)))
        kw     (some-> (last (re-seq #"(?i)\b(?:order\s+by|group\s+by|select|where|having|limit|offset|union|when|then|on)\b"
                                     before))
                       str/lower-case
                       (str/replace #"\s+" " "))]
    (when (contains? #{"order by" "group by"} kw)
      (let [cols (sql-output-columns body)]
        (when (seq cols)
          {:from (- offset (count typed)) :to offset
           :options (mapv (fn [c] {:label c :type "property" :detail "output column"})
                          cols)})))))

(defn sql-type-completion
  "Completion for `?name:type` inside a `@sql-template` body: `order` is
   valid only inside ORDER BY / GROUP BY, reusing sql-params's exact
   `re-identifier-position` regex so completion and lint can't drift."
  [^String body offset]
  (let [prefix (subs body 0 offset)]
    (when-let [[whole typed] (re-find #"\?[a-z_][a-z0-9_]*\??:([a-zA-Z]*)$" prefix)]
      (let [typed       (or typed "")
            ;; Must see the prefix up to the `?`, not the `?name:` text
            ;; itself, or "order by ?sort:" never matches.
            param-start (- (count prefix) (count whole))
            types (if (re-find sql-params/re-identifier-position (subs prefix 0 param-start))
                    ["order"]
                    scalar-type-names)]
        {:from (- offset (count typed)) :to offset
         :options (mapv (fn [t] {:label t :type "type"}) types)}))))

(defn sql-order-restriction-completion
  "Completion inside `?name:order(...)`'s restriction set in a
   `@sql-template` body — sourced from `sql-output-columns` (the query's
   own output columns, not a schema entity)."
  [^String body offset]
  (let [open (loop [i (dec offset) depth 0]
               (cond
                 (neg? i)                nil
                 (= \) (.charAt body i)) (recur (dec i) (inc depth))
                 (= \( (.charAt body i)) (if (zero? depth) i (recur (dec i) (dec depth)))
                 :else                   (recur (dec i) depth)))]
    (when (and open
               (re-find #"\?[a-z_][a-z0-9_]*\??(?::[a-zA-Z]+)?$" (subs body 0 open)))
      (let [cols  (sql-output-columns body)
            typed (or (re-find #"[A-Za-z_][A-Za-z0-9_]*$" (subs body 0 offset)) "")]
        (when (seq cols)
          {:from (- offset (count typed)) :to offset
           :options (mapv (fn [c] {:label c :type "property" :detail "output column"}) cols)})))))

(defn returns-name-completion
  "On a flush-left `@returns` line, complete column names from the
   body's output columns, skipping already-declared ones."
  [^String source offset cur]
  (when (and cur (:sql? cur) (seq (:body cur)))
    (let [ls (loop [i offset]
               (if (and (pos? i) (not= \newline (.charAt source (dec i))))
                 (recur (dec i)) i))
          line-to-cursor (subs source ls offset)]
      (when (re-find #"^@returns(\s|$)" line-to-cursor)
        (when-let [[_ typed] (re-find #"(?:@returns\s+|,\s*)([A-Za-z_][A-Za-z0-9_]*)?$"
                                      line-to-cursor)]
          (let [typed    (or typed "")
                declared (set (map second (re-seq #"([A-Za-z_][A-Za-z0-9_]*)\s*:"
                                                  line-to-cursor)))
                cols     (remove declared (sql-output-columns (:body cur)))]
            (when (seq cols)
              {:from (- offset (count typed)) :to offset
               :options (mapv (fn [c] {:label c :type "property"
                                       :detail "column — add :type"})
                              cols)})))))))

(defn xsql-in-order-by?
  "True when body-local `offset` sits inside an `order by` clause of the
   current arg-list, bounded to the innermost unclosed paren. Textual
   approximation of lint.cljc's AST-walked check — a mis-scoped
   suggestion is a UX nit here, not a wire-safety issue."
  [^String body offset]
  (let [open (loop [i (dec offset) depth 0]
               (cond
                 (neg? i)                 nil
                 (= \) (.charAt body i))  (recur (dec i) (inc depth))
                 (= \( (.charAt body i))  (if (zero? depth) i (recur (dec i) (dec depth)))
                 :else                    (recur (dec i) depth)))]
    (when open
      (let [scope (subs body (inc open) offset)
            m     (re-find #"(?is)\border\s+by\b" scope)]
        (when m
          (let [after (subs scope (+ (str/index-of scope m) (count m)))]
            (not (re-find #"(?is)\b(?:limit|offset|distinct|join|_limit|_offset|_distinct|_join)\b"
                          after))))))))

(defn xsql-param-type-completion
  "Completion for `?name:type` inside a regular XSQL body, position-
   restricted to match lint.cljc's collect-param-ref-errors exactly:
   `order` is valid only inside `order by`, every other type elsewhere."
  [^String body offset]
  (let [prefix (subs body 0 offset)]
    (when-let [[_ typed] (re-find #"\?[a-z_][a-z0-9_]*\??:([a-zA-Z]*)$" prefix)]
      (let [typed (or typed "")
            types (if (xsql-in-order-by? body offset)
                    ["order"]
                    scalar-type-names)]
        {:from (- offset (count typed)) :to offset
         :options (mapv (fn [t] {:label t :type "type"}) types)}))))

(defn xsql-order-restriction-completion
  "Completion inside `?name[:type](...)`'s restriction set — only
   attribute names are valid, so filter `complete-impl/complete`'s
   generic 'inside args' result down to attributes."
  [^String body offset schema op]
  (let [open (loop [i (dec offset) depth 0]
               (cond
                 (neg? i)                nil
                 (= \) (.charAt body i)) (recur (dec i) (inc depth))
                 (= \( (.charAt body i)) (if (zero? depth) i (recur (dec i) (dec depth)))
                 :else                   (recur (dec i) depth)))]
    (when (and open schema
               (re-find #"\?[a-z_][a-z0-9_]*\??(?::[a-zA-Z]+)?$" (subs body 0 open)))
      (let [r     (complete-impl/complete {:source body :offset offset :schema schema
                                           :root-entity nil :op op})
            attrs (filterv #(= "attribute" (:type %)) (:options r))]
        (when (seq attrs)
          (assoc r :options attrs))))))

(defn complete
  "Context-aware completion across a program. Finds the operation containing
   `:offset` and completes its rooted body (root derived from the body),
   mapping positions back to the full buffer. On a declaration line only
   the `@`-directive token itself completes (the verb vocabulary); the
   free-form op name is silent. Raw SQL bodies complete `{…}` placeholders
   (entities / relations / attributes) and ORDER BY / GROUP BY output
   columns; descriptions stay silent."
  [{:keys [source offset schema] :or {offset 0}}]
  (let [source (or source "")
        spans (op-spans (parse source) source)
        cur   (or (some (fn [o] (when (and (>= offset (:decl-offset o))
                                           (< offset (:end-offset o))) o))
                        spans)
                  (last spans))]
    (or (decl-verb-completion source offset)
        (returns-type-completion source offset)
        (returns-name-completion source offset cur)
        (mutate-type-completion source offset cur schema)
        (when (and cur (:sql? cur) schema
                   (:body-offset cur) (>= offset (:body-offset cur)))
          (let [body    (:body cur)
                rel-off (min (- offset (:body-offset cur)) (count body))]
            (when-let [r (or (sql-placeholder-completion body rel-off schema)
                             (sql-order-restriction-completion body rel-off)
                             (sql-type-completion body rel-off)
                             (sql-clause-completion body rel-off))]
              (-> r
                  (update :from + (:body-offset cur))
                  (update :to + (:body-offset cur))))))
        (if (and cur (not (:batch cur)) (not (:mutate cur)) (not (:sql? cur))
                 (:body-offset cur) (>= offset (:body-offset cur)))
          (let [body    (:body cur)
                rel-off (- offset (:body-offset cur))
                r (or (xsql-order-restriction-completion body rel-off schema (:op cur))
                      (xsql-param-type-completion body rel-off)
                      (complete-impl/complete
                       {:source body
                        :offset rel-off
                        :schema schema
                        :root-entity nil
                        :op (:op cur)}))]
            (-> r
                (update :from + (:body-offset cur))
                (update :to + (:body-offset cur))))
          {:from offset :to offset :options []}))))

(defn scope-at
  "Persistent-panel counterpart to `complete` (see complete.cljc/scope-at):
   resolves scope in the containing op's isolated body and threads back
   `:op`/`:body-offset`/`:body-end` so the caller can bound edits to
   this declaration only."
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
   order — bare or `ns/name`-qualified references (ambiguous bare refs resolve
   to nothing; lint flags them). nil if no such batch."
  ([source batch-name] (batch-member-ops source batch-name nil))
  ([source batch-name params]
   (let [compiled (compile source params)]
     (when-let [b (some #(when (and (:batch %) (= batch-name (:name %))) %) compiled)]
       (vec (keep #(:op (resolve-batch-member compiled %)) (:members b)))))))

;; ── Emit (export) ───────────────────────────────────────────────────────────

(defn emit-op
  "Serialise one operation map back to text."
  [{:keys [op name description namespace watch returns body]}]
  (str "@" op (when name (str " " name)) "\n"
       (when (seq description)
         ;; quoted is the canonical form (multiline-safe); a description
         ;; that itself contains `"` falls back to the legacy bare line.
         (if (str/includes? description "\"")
           (str "@description " (str/replace description "\n" " ") "\n")
           (str "@description \"" description "\"\n")))
       (when (seq namespace) (str "@namespace " namespace "\n"))
       (when watch (str "@watch" (when (sequential? watch) (str " " (str/join ", " watch))) "\n"))
       (when (seq returns) (str "@returns " returns "\n"))
       body (when-not (str/ends-with? (or body "") "\n") "\n")))

(defn emit
  "Serialise a vector of operation maps to a multi-op program."
  [ops]
  (str/join "\n" (map emit-op ops)))
