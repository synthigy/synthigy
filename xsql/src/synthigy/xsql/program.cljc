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

(ns synthigy.xsql.program
  "Operation-document PROGRAM layer for XSQL: first-class `@`-declarations."
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
  "Recursive read verbs walking the self-relation named by `_on`; they return a flat row list."
  #{"search-tree" "get-tree"})

(def mutate-verbs
  "Mutation verbs: the body is a `?var:Entity[]` data variable, not a rooted XSQL query."
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
  "Split lines into per-declaration chunks at each flush-left op verb or `@batch` line."
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
  "Op header directives, between the @verb line and the body; not boundaries."
  #{"description" "namespace" "watch" "returns"})

(def ^:private buffer-directives
  "Buffer-level directives, flush-left before the first op; ignored by codegen."
  #{"workspace"})

(defn parse-header
  "Consume header-directive lines after the @verb line; returns `[header remaining-lines]`."
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
        ;; empty body starts past the decl line's newline, never ON it — else an empty body completes on the decl line
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
       :root (when (and (not sql?) (seq (str/trim (or body ""))))
               (some-> (re-find #"^\s*([\w.-]+)" body) (nth 1)))
       :body body :body-offset body-off :decl-offset decl-off
       :errors (cond-> errors
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
  "Flush-left `@namespace` before the first op: the buffer-level default namespace, or nil."
  [lines]
  (loop [ls lines]
    (when-let [[text _] (first ls)]
      (cond
        (flush-op-line? text)                    nil
        (re-find #"^@namespace\s+(\S+)" text)     (second (re-find #"^@namespace\s+(\S+)" text))
        :else                                     (recur (rest ls))))))

(defn workspace-name
  "Flush-left `@workspace \"<name>\"` before the first op: the buffer's console identity, or nil."
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
  "Parse a `@batch <name>: <op> <op> …` declaration."
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
  "Parse a buffer into a vector of operation/batch declaration maps, errors in full-buffer offsets."
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
  "Editor transaction `{:from :to :insert :watch}` toggling `@watch` on the op at `decl-offset`."
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
  "The namespace an op falls under: its `@namespace`, else its root entity (the emitters' default)."
  [{:keys [namespace root entity]}]
  (some-> (or namespace root entity) str/lower-case))

(defn op-identity
  "Qualified `namespace/name` identity string (lower-case), e.g. `movie/list`."
  [{:keys [name] :as op}]
  (str (op-namespace op) "/" (some-> name str/lower-case)))

(defn resolve-batch-member
  "Resolve a bare or `ns/name` batch member to `{:op}`, `{:ambiguous}` or `{:missing true}`."
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
  "Flush-left `@` lines that aren't valid declarations at their position."
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
        ;; lines inside a quoted multiline @description are prose, not declarations
        desc-spans   (keep #(get-in % [:header-offsets "description"]) decls)
        in-desc?     (fn [off] (boolean (some (fn [[a b]] (and (> off a) (<= off b))) desc-spans)))]
    (for [[text off] (lines-with-offsets (or source ""))
          :let [d  (second (re-find #"^@([\w-]+)" text))
                to (+ off (count text))]
          :when (and d
                     (not (doc-verbs d)) (not (mutate-verbs d)) (not= "batch" d)
                     (not (in-sql? off))
                     (not (in-desc? off)))
          :let [ns-casing
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
                    ;; before the first op only @namespace (the buffer default) means something
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
  "Join operators inside a `{…}` placeholder; mirrors the server resolver's join pattern."
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
  "Validate every `{…}` placeholder of a raw-SQL body against the schema, in body-local offsets."
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
  "Warn when a `?name:order(cols…)` entry isn't a detected output column (heuristic, so never an error)."
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
  "`sql-params/analyze` param checks for `@sql-template` bodies."
  [^String body]
  (let [{:keys [errors warnings]} (sql-params/analyze body)]
    (concat
     (map #(assoc % :severity :error) errors)
     (map #(assoc % :severity :warning) warnings)
     (sql-restriction-set-warnings body))))

(defn lint
  "Lint every declaration in `source` against `schema`, in full-buffer offsets."
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
  "Lift tree-op `:_on` (and get-tree's root identity) out of :args into top-level wire keys."
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
  "Parse a document with each rooted body pre-parsed to `:body-ast`, the cacheable half of `compile`."
  [source]
  (mapv (fn [decl]
          (if (and (:op decl) (not (:sql? decl)) (not (:batch decl))
                   (not (:mutate decl)))
            (assoc decl :body-ast (xparser/parse (:body decl)))
            decl))
        (parse source)))

(defn compile-parsed
  "Bind `params` into a `parse-compiled` result; same output as `compile`."
  [decls params]
  (mapv (fn [{:keys [op name description namespace watch returns body body-ast sql? batch mutate] :as decl}]
          (cond
            batch  decl
            mutate decl
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
  "Compile every declaration to its wire shape, preserving order."
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
  "The operation containing `offset` (last op when past the end), or nil for an empty program."
  [source offset]
  (let [spans (op-spans (parse (or source "")) (or source ""))]
    (or (some (fn [o] (when (and (>= offset (:decl-offset o))
                                 (< offset (:end-offset o))) o))
              spans)
        (last spans))))

(defn op-index-at-offset
  "0-based index of the operation containing `offset` (last when past the end, 0 when empty)."
  [source offset]
  (let [spans (op-spans (parse (or source "")) (or source ""))]
    (or (some (fn [[i o]] (when (and (>= offset (:decl-offset o))
                                     (< offset (:end-offset o))) i))
              (map-indexed vector spans))
        (max 0 (dec (count spans))))))

(def ^:private verb-options
  "Completion options for the `@`-token of a declaration line."
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
  "Bind-param scalar types: sql-params/canonical-types minus `order`."
  ["int" "float" "string" "boolean" "timestamp" "uuid"])

(def ^:private returns-type-options
  "Type vocabulary for `@returns name:type` annotations."
  (mapv (fn [t] {:label t :type "type" :detail "@returns column type"})
        scalar-type-names))

(defn returns-type-completion
  "Type vocabulary right after `:` on a flush-left `@returns` line, else nil."
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
  "Verb/directive vocabulary inside the leading `@word` of a flush-left line, else nil."
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
  "Entity a placeholder chain [root rel …] lands on, or nil when a hop is unknown."
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
  "Completion inside an unclosed `{…}` placeholder: entities, relations after a join, attributes after `.`."
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
  "Output-column names of a raw-SQL body: `AS` aliases plus bare `{entity.field}` items."
  [body]
  (distinct
   (concat
    (map second (re-seq #"(?i)\bas\s+([A-Za-z_][A-Za-z0-9_]*)" body))
    (map second (re-seq #"\{[^{}]*\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\s*(?=,|\n|$)" body)))))

(defn sql-clause-completion
  "Output-column options after ORDER BY / GROUP BY in a raw-SQL body."
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
  "`?name:type` completion in a `@sql-template` body; `order` only in identifier position."
  [^String body offset]
  (let [prefix (subs body 0 offset)]
    (when-let [[whole typed] (re-find #"\?[a-z_][a-z0-9_]*\??:([a-zA-Z]*)$" prefix)]
      (let [typed       (or typed "")
            ;; regex must see the prefix up to the `?`, not `?name:` — else "order by ?sort:" never matches
            param-start (- (count prefix) (count whole))
            types (if (re-find sql-params/re-identifier-position (subs prefix 0 param-start))
                    ["order"]
                    scalar-type-names)]
        {:from (- offset (count typed)) :to offset
         :options (mapv (fn [t] {:label t :type "type"}) types)}))))

(defn sql-order-restriction-completion
  "Output-column options inside a `@sql-template` `?name:order(...)` restriction set."
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
  "Output-column names on a flush-left `@returns` line, minus already-declared ones."
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
  "True when body-local `offset` sits in an `order by` clause of the innermost unclosed paren."
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
  "`?name:type` completion in an XSQL body; `order` only inside `order by`, like lint."
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
  "Attribute-only options inside an XSQL `?name[:type](...)` restriction set."
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
  "Context-aware completion across a program, mapped back to full-buffer offsets."
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
  "Panel scope in the containing op's body, threading back `:op`/`:body-offset`/`:body-end`."
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
  "Compiled ops referenced by the `@batch` named `batch-name`, in member order, or nil."
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
