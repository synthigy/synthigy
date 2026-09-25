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

(ns synthigy.xsql.sql-lint
  "Lexical lints for `@sql-template` bodies: misspelled keywords and array placeholders in contexts that expand to broken SQL."
  (:require [clojure.string :as str]
            [synthigy.xsql.sql-params :as sql-params]))

;; ── Keyword typos ────────────────────────────────────────────────────────────

(def sql-keywords
  ["SELECT" "FROM" "WHERE" "GROUP BY" "HAVING" "ORDER BY" "LIMIT" "OFFSET"
   "JOIN" "LEFT JOIN" "RIGHT JOIN" "INNER JOIN" "ON" "AS" "AND" "OR" "NOT"
   "IN" "BETWEEN" "LIKE" "ILIKE" "IS NULL" "IS NOT NULL" "DISTINCT"
   "WITH" "UNION" "UNION ALL" "CASE" "WHEN" "THEN" "ELSE" "END"
   "ASC" "DESC" "NULLS FIRST" "NULLS LAST"
   "COUNT" "SUM" "AVG" "MIN" "MAX" "ROUND" "COALESCE" "NULLIF" "CAST"])

(def known-words
  (into (into #{} (mapcat #(str/split % #"\s+")) sql-keywords)
        ["TRUE" "FALSE" "NULL" "LOWER" "UPPER" "LENGTH" "TRIM" "CONCAT" "SUBSTRING"
         "SUBSTR" "EXTRACT" "ABS" "CEIL" "FLOOR" "EXISTS" "ANY" "ALL" "DATE"
         "INTERVAL" "OVER" "PARTITION" "USING" "FULL" "OUTER" "CROSS" "FILTER"]))

(def suggest-vocab
  "Keywords long enough to suggest as a fix — shorter ones collide with short identifiers."
  (filterv #(>= (count %) 4) known-words))

(defn ch [s i]
  (when (< -1 i (count s)) (subs s i (inc i))))

(defn one-edit-away?
  "True when `a` and `b` differ by exactly one insert, delete, substitution or adjacent swap."
  [a b]
  (let [la (count a) lb (count b) d (- la lb)]
    (cond
      (= a b) false
      (> (if (neg? d) (- d) d) 1) false
      (= la lb)
      (let [idxs (keep-indexed (fn [i _] (when (not= (ch a i) (ch b i)) i)) a)]
        (case (count idxs)
          1 true
          2 (let [[i j] idxs]
              (and (= j (inc i)) (= (ch a i) (ch b j)) (= (ch a j) (ch b i))))
          false))
      :else
      (let [[s l] (if (< la lb) [a b] [b a])]
        (loop [i 0 j 0 skipped? false]
          (cond
            (>= i (count s))       true
            (= (ch s i) (ch l j))  (recur (inc i) (inc j) skipped?)
            skipped?               false
            :else                  (recur i (inc j) true)))))))

(defn word-start? [c] (boolean (and c (re-find #"[A-Za-z_]" c))))
(defn word-char?  [c] (boolean (and c (re-find #"[A-Za-z0-9_]" c))))

(defn bare-words
  "`{:word :from :to}` for identifier runs outside strings, comments, `{…}` placeholders and `?name` tokens."
  [s]
  (let [n (count s)
        run-end (fn [i] (loop [j i] (if (word-char? (ch s j)) (recur (inc j)) j)))]
    (loop [i 0 mode :normal out []]
      (if (>= i n)
        out
        (let [c (ch s i) c2 (ch s (inc i))]
          (case mode
            :string        (cond (and (= c "'") (= c2 "'")) (recur (+ i 2) :string out)
                                 (= c "'")                  (recur (inc i) :normal out)
                                 :else                      (recur (inc i) :string out))
            :dstring       (recur (inc i) (if (= c "\"") :normal :dstring) out)
            :line-comment  (recur (inc i) (if (= c "\n") :normal :line-comment) out)
            :block-comment (if (and (= c "*") (= c2 "/"))
                             (recur (+ i 2) :normal out)
                             (recur (inc i) :block-comment out))
            :brace         (recur (inc i) (if (= c "}") :normal :brace) out)
            (cond
              (= c "'")                  (recur (inc i) :string out)
              (= c "\"")                 (recur (inc i) :dstring out)
              (and (= c "-") (= c2 "-")) (recur (+ i 2) :line-comment out)
              (and (= c "/") (= c2 "*")) (recur (+ i 2) :block-comment out)
              (= c "{")                  (recur (inc i) :brace out)
              (= c "?")                  (recur (max (inc i) (run-end (inc i))) :normal out)
              (word-start? c)            (let [end (run-end (inc i))]
                                           (recur end :normal (conj out {:word (subs s i end) :from i :to end})))
              :else                      (recur (inc i) :normal out))))))))

(defn keyword-typo-warnings
  "A warning per bare word (4+ chars) one edit away from a SQL keyword it isn't."
  [body]
  (for [{:keys [word from to]} (bare-words body)
        :let [uc (str/upper-case word)]
        :when (and (>= (count word) 4) (not (known-words uc)))
        :let [hit (some #(when (one-edit-away? uc %) %) suggest-vocab)]
        :when hit]
    {:severity :warning
     :message  (str "Unknown word '" word "' — did you mean '" hit "'?")
     :from from :to to}))

;; ── Array placeholder context ────────────────────────────────────────────────

(defn render-placeholder [{:keys [name raw-type]}]
  (str "?" name (when raw-type (str ":" raw-type)) "[]"))

(def bad-array-messages
  {:in-parens  #(str "Drop the outer parens — " % " already expands to (?, ?, ?). Write `IN " % "`.")
   :any-parens #(str "ANY() needs a real array, but " % " expands to a row (?, ?, ?). Write `IN " % "`.")
   :eq-in      #(str "`=` and `IN` don't combine. Write `IN " % "`.")
   :eq-parens  #(str % " expands to (?, ?, ?) — `=` against a row is a type mismatch. Write `IN " % "`.")})

(defn bad-array-context
  "`{:kind :start}` when the text before `from` is one of the four bad array contexts, else nil."
  [body from]
  (let [look-back (max 0 (- from 48))
        ctx       (subs body look-back from)
        lc        (str/lower-case ctx)
        check     (fn [re kind]
                    (when-let [m (re-find re lc)]
                      (let [m (if (string? m) m (first m))]
                        {:kind kind :start (+ look-back (- (count ctx) (count m)))})))]
    (or (check #"\bin\s*\(\s*$"  :in-parens)
        (check #"\bany\s*\(\s*$" :any-parens)
        (check #"=\s*in\s+$"     :eq-in)
        (check #"=\s*\(\s*$"     :eq-parens))))

(defn array-context-errors
  "An error per `?name:type[]` written as `IN (…)`, `ANY(…)`, `= IN …` or `= (…)`."
  [body]
  (for [{:keys [kind array? from to] :as p} (sql-params/scan-placeholders body)
        :when (and (= :named kind) array?)
        :let [bad (bad-array-context body from)]
        :when bad]
    {:severity :error
     :message  ((bad-array-messages (:kind bad)) (render-placeholder p))
     :from (:start bad) :to to}))

(defn lint
  "Keyword typos and array-context misuse in a `@sql-template` body, body-relative offsets."
  [body]
  (concat (array-context-errors body) (keyword-typo-warnings body)))
