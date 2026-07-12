(ns synthigy.xsql.operations
  "Operation documents — a thin `@`-directive header over a rooted XSQL
   (or raw SQL) body.

   An operation document is the portable, codegen-facing artifact: the
   console serialised. Format is sqlc-style — many ops per file, each
   introduced by a verb-first header, the body handed verbatim to the
   existing XSQL parser/compiler:

       @search       top_movies
       @description  Top-rated movies released since a given year.
       movie
         title
         _args (year >= ?min_year:int)

   The op boundary is a flush-left `@` (illegal inside an XSQL body), so
   no grammar change is needed — this layer sits ABOVE `parse`/`compile`.

   Only QUERY ops form documents (`search`/`get`/`sql`/`slice`/`purge`);
   `sync`/`stack`/`delete` are schema-derived per-entity methods (see
   `OPERATIONS.md` / the two-tier split), never documents."
  (:require [clojure.string :as str]
            [synthigy.xsql.api :as xsql]))

(def doc-verbs
  "Op verbs that form operation documents. Excludes the write/destroy-by-id
   ops (`sync`/`stack`/`delete`) which are schema-derived methods."
  #{"search" "get" "sql" "slice" "purge"})

;; ── Parse ────────────────────────────────────────────────────────────────

(defn- header-line? [^String line] (str/starts-with? line "@"))
(defn- blank-line?  [^String line] (str/blank? line))

(defn- parse-header-line
  "`@search top_movies` → [\"search\" \"top_movies\"]. Splits the directive
   key from its trimmed value (value may be nil for a bare `@key`)."
  [line]
  (let [[k v] (str/split (subs line 1) #"\s+" 2)]
    [k (some-> v str/trim)]))

(defn- strip-blank-edges
  "Drop leading and trailing blank lines from a seq of lines."
  [lines]
  (->> lines (drop-while blank-line?) reverse (drop-while blank-line?) reverse))

(defn- finalize-block [header-lines body-lines]
  (let [directives (map parse-header-line header-lines)
        verb-dir   (first (filter (comp doc-verbs first) directives))
        descr      (some (fn [[k v]] (when (= k "description") v)) directives)
        body-str   (let [b (str/join "\n" (strip-blank-edges body-lines))]
                     (when (seq b) (str b "\n")))]
    (cond-> {:op (first verb-dir) :name (second verb-dir)}
      descr    (assoc :description descr)
      body-str (assoc :body body-str)
      (and body-str (not= "sql" (first verb-dir)))
      (assoc :entity (xsql/root-entity body-str)))))

(defn parse-document
  "Parse a multi-op operation document into a vector of op-doc maps
   `{:op :name :description? :entity? :body?}`. Op boundary = a flush-left
   `@` that follows body content; consecutive `@` lines are one header."
  [source]
  (loop [lines (str/split-lines source)
         header [] body [] in-body? false out []]
    (if-let [line (first lines)]
      (if (header-line? line)
        (if (and (seq header) in-body?)
          (recur (rest lines) [line] [] false
                 (conj out (finalize-block header body)))
          (recur (rest lines) (conj header line) body in-body? out))
        (recur (rest lines) header (conj body line)
               (or in-body? (not (blank-line? line))) out))
      (cond-> out
        (seq header) (conj (finalize-block header body))))))

;; ── Emit ─────────────────────────────────────────────────────────────────

(defn emit-op
  "Serialise one op-doc map back to header + body text."
  [{:keys [op name description body]}]
  (str "@" op (when name (str " " name)) "\n"
       (when description (str "@description " description "\n"))
       body))

(defn emit-document
  "Serialise a vector of op-doc maps to a multi-op document (blank line
   between ops)."
  [ops]
  (str/join "\n" (map emit-op ops)))

;; ── Compile (codegen helper) ───────────────────────────────────────────────

(defn compile-op
  "Augment an op-doc with the compiled wire shape of its body
   (`:selections`/`:args`/`:entity`). No-op for `sql` ops (raw SQL body)."
  [{:keys [op body] :as op-doc}]
  (if (and body (not= "sql" op))
    (merge op-doc (xsql/compile body op))
    op-doc))
