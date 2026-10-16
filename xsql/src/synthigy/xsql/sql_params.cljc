;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.xsql.sql-params
  "Shared grammar for named SQL-template parameters: `rewrite` for the server, `analyze` for the console."
  (:require [clojure.string :as str]
            [synthigy.timestamp :as ts]
            [clojure.edn :as edn]))

;;; ============================================================================
;;; Type vocabulary
;;; ============================================================================

(def type-aliases
  "Surface type token → canonical type keyword."
  {"int"       :int
   "integer"   :int
   "float"     :float
   "number"    :float
   "numeric"   :float
   "string"    :string
   "text"      :string
   "boolean"   :boolean
   "bool"      :boolean
   "timestamp" :timestamp
   "datetime"  :timestamp
   "uuid"      :uuid
   ;; SECURITY: the ONE interpolating type — safe only via normalize-order-specs' allowlist; no other type may take this path
   "order"     :order})

(def canonical-types
  "The set of canonical type keywords."
  #{:int :float :string :boolean :timestamp :uuid :order})

(defn canonical-type
  "Resolve a surface type token to a canonical keyword, or nil if unknown."
  [token]
  (when token (get type-aliases (str/lower-case token))))

;;; ============================================================================
;;; Char helpers (portable — 1-char strings, no java.lang.Character)
;;; ============================================================================

(def ^:private re-ident-start    #"[A-Za-z_]")
(def ^:private re-ident-continue #"[A-Za-z0-9_]")

(defn char-at
  [s i]
  (when (and (>= i 0) (< i (count s)))
    (subs s i (inc i))))

(defn ident-start?    [c] (boolean (and c (re-matches re-ident-start c))))
(defn ident-continue? [c] (boolean (and c (re-matches re-ident-continue c))))

(defn scan-while
  "Index just past the last char from `pos` for which `pred?` holds."
  [s pos pred?]
  (let [n (count s)]
    (loop [i pos]
      (if (and (< i n) (pred? (char-at s i)))
        (recur (inc i))
        i))))

;;; ============================================================================
;;; prettify
;;; ============================================================================

(defn prettify
  "Turn a snake/space identifier into a human label: `from_year` → \"From year\"."
  [nm]
  (let [words (->> (str/split (str nm) #"[_\s]+")
                   (remove str/blank?))]
    (if (empty? words)
      (str nm)
      (str/join " " (cons (str/capitalize (first words))
                          (rest words))))))

;;; ============================================================================
;;; Comment stripping (portable)
;;; ============================================================================

(defn strip-comments
  "Remove `--` and `/* */` comments (collapsed to whitespace), leaving single-quoted literals intact."
  [sql]
  (let [n (count sql)]
    (loop [i 0 out []]
      (if (>= i n)
        (str/join out)
        (let [c  (char-at sql i)
              c2 (char-at sql (inc i))]
          (cond
            ;; single-quoted literal — copy verbatim through its close
            (= "'" c)
            (let [end (loop [j (inc i)]
                        (cond
                          (>= j n) j
                          (and (= "'" (char-at sql j))
                               (= "'" (char-at sql (inc j))))
                          (recur (+ j 2))
                          (= "'" (char-at sql j)) (inc j)
                          :else (recur (inc j))))]
              (recur (min end n) (conj out (subs sql i (min end n)))))

            (and (= "-" c) (= "-" c2))
            (let [nl (str/index-of sql "\n" i)]
              (if nl
                (recur (inc nl) (conj out "\n"))
                (str/join out)))

            (and (= "/" c) (= "*" c2))
            (let [end (str/index-of sql "*/" (+ i 2))]
              (if end
                (recur (+ end 2) (conj out " "))
                (str/join out)))

            :else
            (recur (inc i) (conj out c))))))))

;;; ============================================================================
;;; Placeholder scanning — comment-free SQL
;;; ============================================================================

(defn scan-placeholders
  "Scan `sql` for `?` placeholders in document order, skipping string literals and comments."
  [sql]
  (let [n (count sql)]
    (loop [i 0 mode :normal out []]
      (if (>= i n)
        out
        (let [c  (char-at sql i)
              c2 (char-at sql (inc i))]
          (case mode
            :string
            (cond
              (and (= "'" c) (= "'" c2)) (recur (+ i 2) :string out) ; escaped ''
              (= "'" c)                  (recur (inc i) :normal out)
              :else                      (recur (inc i) :string out))

            :line-comment
            (if (= "\n" c)
              (recur (inc i) :normal out)
              (recur (inc i) :line-comment out))

            :block-comment
            (if (and (= "*" c) (= "/" c2))
              (recur (+ i 2) :normal out)
              (recur (inc i) :block-comment out))

            ;; :normal
            (cond
              (= "'" c)                  (recur (inc i) :string out)
              (and (= "-" c) (= "-" c2)) (recur (+ i 2) :line-comment out)
              (and (= "/" c) (= "*" c2)) (recur (+ i 2) :block-comment out)

              (= "?" c)
              (if (ident-start? c2)
                (let [name-end (scan-while sql (+ i 2) ident-continue?)
                      nm       (subs sql (inc i) name-end)
                      opt?     (= "?" (char-at sql name-end))
                      mark-end (if opt? (inc name-end) name-end)
                      typed?   (and (= ":" (char-at sql mark-end))
                                    (ident-start? (char-at sql (inc mark-end))))
                      type-end (if typed?
                                 (scan-while sql (+ mark-end 2) ident-continue?)
                                 mark-end)
                      type-str (when typed?
                                 (subs sql (inc mark-end) type-end))
                      paren-at (cond
                                 (and typed? (= "(" (char-at sql type-end))) type-end
                                 (and (not typed?) (= "(" (char-at sql mark-end))) mark-end)
                      set-close (when paren-at
                                  (loop [j (inc paren-at)]
                                    (cond (>= j n)                 nil
                                          (= ")" (char-at sql j))  j
                                          (= "\n" (char-at sql j)) nil
                                          :else                    (recur (inc j)))))
                      type-args (when set-close
                                  (->> (str/split (subs sql (inc paren-at) set-close) #",")
                                       (map str/trim)
                                       (remove empty?)
                                       vec))
                      set-end  (if set-close (inc set-close) type-end)
                      ;; optional trailing `[]`
                      array?   (and (= "[" (char-at sql set-end))
                                    (= "]" (char-at sql (inc set-end))))
                      end0     (if array? (+ set-end 2) set-end)
                      ;; optional `=default` immediately after (tight binding)
                      has-def? (= "=" (char-at sql end0))
                      def-s    (inc end0)
                      def-e    (when has-def?
                                 (cond
                                   (= "[" (char-at sql def-s))
                                   (loop [j (inc def-s)]
                                     (cond (>= j n)                  j
                                           (= "]" (char-at sql j))   (inc j)
                                           :else                     (recur (inc j))))
                                   (= "\"" (char-at sql def-s))
                                   (loop [j (inc def-s)]
                                     (cond (>= j n)                  j
                                           (= "\"" (char-at sql j))  (inc j)
                                           :else                     (recur (inc j))))
                                   :else
                                   (scan-while sql def-s
                                               #(and % (not (#{" " "\t" "\n" "\r"
                                                               "," ")" "]"} %))))))
                      to       (if has-def? def-e end0)]
                  (recur to :normal
                         (conj out (cond-> {:kind     :named
                                            :name     nm
                                            :raw-type type-str
                                            :type     (canonical-type type-str)
                                            :array?   array?
                                            :from     i
                                            :to       to}
                                     opt?      (assoc :optional? true)
                                     type-args (assoc :type-args type-args)
                                     has-def? (assoc :default (subs sql def-s def-e))))))
                (recur (inc i) :normal
                       (conj out {:kind :positional :from i :to (inc i)})))

              :else (recur (inc i) :normal out))))))))

;;; ============================================================================
;;; Analysis — for the console form
;;; ============================================================================

(defn array-clash-errors
  "Per-name errors: a name used both as scalar and as array."
  [named distinct-names]
  (for [nm distinct-names
        :let [occs (filter #(= nm (:name %)) named)]
        :when (> (count (distinct (map :array? occs))) 1)]
    (str "?" nm " is used as both a scalar and an array — pick one")))

(defn normalize-order-specs
  "SECURITY: normalize an `order` param value to `[[col dir] …]`, re-validating every token against the allowlist so it is safe to interpolate."
  [v {:keys [name span type-args]}]
  (let [fail (fn [msg]
               (throw (ex-info (str "Parameter ?" name " " msg)
                               {:code "PARAM_TYPE_MISMATCH"
                                :param name
                                :type :order
                                :value v
                                :span span})))
        spec->pair
        (fn [s]
          (cond
            (string? s)
            (let [parts (str/split (str/trim s) #"\s+")]
              (when (or (empty? parts) (> (count parts) 2))
                (fail (str "has a malformed order spec \"" s "\"")))
              [(first parts) (or (second parts) "asc")])
            (sequential? s)
            [(str (first s)) (str (or (second s) "asc"))]
            :else (fail "entries must be \"column [asc|desc]\" strings or [column direction] pairs")))
        specs (cond
                (string? v)     (remove str/blank? (map str/trim (str/split v #",")))
                (sequential? v) v
                :else (fail "must be an order spec string or array"))
        pairs (mapv spec->pair specs)
        allowed (not-empty (set type-args))]
    (when (empty? pairs)
      (fail "resolved to an empty order"))
    (doseq [[col dir] pairs]
      (when-not (re-matches #"[a-z_][a-z0-9_]*" (str col))
        (fail (str "has an invalid order column \"" col "\"")))
      (when-not (#{"asc" "desc"} (str/lower-case (str dir)))
        (fail (str "has an invalid direction \"" dir "\" (asc|desc)")))
      (when (and allowed (not (allowed col)))
        (fail (str "may only order by: " (str/join ", " (sort allowed))))))
    pairs))

(def re-identifier-position
  "Matches text ending at a placeholder in an identifier position (`ORDER BY ?x`); shared with program completion."
  #"(?is)\b(ORDER|GROUP)\s+BY\s+(?:[^,()]*,\s*)*$")

(defn identifier-position-errors
  "SECURITY: a bind param in ORDER BY/GROUP BY silently sorts by a constant — error unless a restricted `:order` param."
  [sql named]
  (vec (for [{:keys [from to name type type-args]} named
             :when (re-find re-identifier-position (subs sql 0 from))
             :let [order? (= :order type)]
             :when (or (not order?) (empty? type-args))]
         {:message
          (if order?
            (str "?" name " is an order param without a restriction set — "
                 "write ?" name ":order(col_a, col_b) to declare which columns "
                 "callers may sort by")
            (str "?" name " is in ORDER BY / GROUP BY position, where a "
                 "bind parameter silently sorts by a constant instead of "
                 "a column — use a literal column name, or ?" name
                 ":order(col_a, col_b) for a caller-chosen one"))
          :from from :to to})))

(defn analyze
  "Analyze a raw SQL template's parameter use into `{:mode :params :positional-count :errors :warnings}`."
  [raw]
  (let [occ        (scan-placeholders (or raw ""))
        named      (filterv #(= :named (:kind %)) occ)
        positional (filterv #(= :positional (:kind %)) occ)]
    (cond
      (and (empty? named) (empty? positional))
      {:mode :none :params [] :positional-count 0 :errors [] :warnings []}

      (empty? named)
      {:mode :positional :params [] :positional-count (count positional)
       :errors [] :warnings []}

      :else
      (let [distinct-names (->> named (map :name) distinct vec)
            occs-of        (fn [nm] (filter #(= nm (:name %)) named))
            params
            (mapv (fn [nm]
                    (let [occs       (occs-of nm)
                          array?     (:array? (first occs))
                          raw-types  (vec (distinct (keep :raw-type occs)))
                          types      (vec (distinct (keep :type occs)))
                          unknown    (vec (remove canonical-type raw-types))]
                      (cond-> {:name           nm
                               :array?         array?
                               :type           (or (first types) :string)
                               :raw-type       (first raw-types)
                               :label          (prettify nm)
                               :typed?         (boolean (seq raw-types))
                               :type-conflict? (> (count types) 1)
                               :unknown-types  unknown}
                        ;; any `?name?` occurrence makes the param optional
                        (some :optional? occs)
                        (assoc :optional true)
                        (some :default occs)
                        (assoc :default (some :default occs)))))
                  distinct-names)
            errors
            (-> []
                (into (for [nm distinct-names
                            :let [occs (occs-of nm)]
                            :when (> (count (distinct (map :array? occs))) 1)
                            o occs]
                        {:message (str "?" nm " is used as both a scalar "
                                       "and an array — pick one")
                         :from (:from o) :to (:to o)}))
                (into (for [p params
                            :when (:type-conflict? p)
                            o (occs-of (:name p))]
                        {:message (str "?" (:name p)
                                       " is annotated with conflicting types")
                         :from (:from o) :to (:to o)}))
                (into (for [o positional]
                        {:message (str "Template mixes named (?name) and "
                                       "positional (?) placeholders — use one style")
                         :from (:from o) :to (:to o)}))
                (into (identifier-position-errors raw named)))
            warnings
            (vec (for [p params
                       t (:unknown-types p)
                       o (occs-of (:name p))
                       :when (= t (:raw-type o))]
                   {:message (str "?" (:name p) ": unknown type '" t
                                  "' — treated as string")
                    :from (:from o) :to (:to o)}))]
        {:mode :named :params params :positional-count 0
         :errors errors :warnings warnings}))))

;;; ============================================================================
;;; Value validation — for the console form (client-side type check)
;;; ============================================================================

(defn validate-value
  "Error string for a parsed `value` invalid against `type`, else nil; nil values always pass."
  [type array? value]
  (letfn [(scalar-err [t v]
            (when (some? v)
              (case t
                :int       (when-not (and (number? v) (integer? v))
                             "expects an integer")
                :float     (when-not (number? v) "expects a number")
                :boolean   (when-not (boolean? v) "expects true or false")
                :string    (when-not (string? v) "expects a string")
                :timestamp (cond (not (string? v)) "expects a timestamp string"
                                   (not (ts/valid? v)) (ex-message (ts/invalid v)))
                :uuid      (when-not (string? v) "expects a UUID string")
                nil)))]
    (if array?
      (cond
        (nil? value)              nil
        (not (sequential? value)) "expects an array"
        :else                     (some #(scalar-err type %) value))
      (scalar-err type value))))

(defn validate-params
  "`[name error]` for every invalid or missing required entry of `values`."
  [param-specs values]
  (vec
   (for [{:keys [name array? type optional]} param-specs
         :let [err (if-not (contains? values name)
                     (when-not optional "required")
                     (validate-value type array? (get values name)))]
         :when err]
     [name err])))

;;; ============================================================================
;;; Optional conjunct drop — `?name?` with no supplied value
;;; ============================================================================

(defn clause-tokens
  "Word and close-paren tokens of comment-free `sql`, enough for clause/conjunct boundaries."
  [sql]
  (let [n (count sql)]
    (loop [i 0 depth 0 quote nil out []]
      (if (>= i n)
        out
        (let [c (char-at sql i)]
          (cond
            quote
            (if (= quote c)
              (if (and (= "'" c) (= "'" (char-at sql (inc i))))
                (recur (+ i 2) depth quote out)          ; escaped ''
                (recur (inc i) depth nil out))
              (recur (inc i) depth quote out))

            (or (= "'" c) (= "\"" c)) (recur (inc i) depth c out)
            (= "(" c)  (recur (inc i) (inc depth) nil out)
            (= ")" c)  (recur (inc i) (dec depth) nil
                              (conj out {:kind :close :from i :depth (dec depth)}))
            ;; skip `?name…` heads so a param named `and` never reads as a connective
            (= "?" c)  (recur (scan-while sql (inc i)
                                          #(or (ident-continue? %) (= "?" %)))
                              depth nil out)
            (ident-start? c)
            (let [e (scan-while sql (inc i) ident-continue?)]
              (recur e depth nil
                     (conj out {:kind :word :word (str/lower-case (subs sql i e))
                                :from i :to e :depth depth})))
            :else (recur (inc i) depth nil out)))))))

(def ^:private clause-end-words
  #{"group" "order" "limit" "offset" "union" "intersect" "except"
    "window" "having" "returning" "fetch" "for"})

(defn drop-optional-conjuncts
  "Drop the top-level WHERE/HAVING conjunct holding each unsupplied optional param; `{:sql}` or `{:errors}`."
  [sql dropped]
  (let [toks    (clause-tokens sql)
        clauses (->> toks
                     (filter #(and (= :word (:kind %))
                                   (#{"where" "having"} (:word %))))
                     (mapv (fn [{:keys [from to depth]}]
                             {:kw-from from :body-start to :depth depth
                              :end (or (some (fn [t]
                                               (when (> (:from t) from)
                                                 (cond
                                                   (and (= :close (:kind t))
                                                        (< (:depth t) depth))
                                                   (:from t)
                                                   (and (= :word (:kind t))
                                                        (= depth (:depth t))
                                                        (clause-end-words (:word t)))
                                                   (:from t))))
                                             toks)
                                       (count sql))})))
        home    (fn [{:keys [from name]}]
                  (if-let [cs (seq (filter #(and (<= (:body-start %) from)
                                                 (< from (:end %)))
                                           clauses))]
                    (apply max-key :kw-from cs)   ; innermost clause wins
                    {:error (str "?" name "? — optional params are only "
                                 "supported in WHERE/HAVING predicates")}))
        homes   (map home dropped)
        errs    (into [] (keep :error) homes)]
    (if (seq errs)
      {:errors (vec (distinct errs))}
      (let [by-clause (group-by first (map vector homes dropped))
            edits
            (for [[{:keys [kw-from body-start end depth]} pairs] by-clause
                  :let [conns (filterv #(and (= :word (:kind %))
                                             (= depth (:depth %))
                                             (<= body-start (:from %))
                                             (< (:from %) end)
                                             (#{"and" "or"} (:word %)))
                                       toks)]]
              (if (some #(= "or" (:word %)) conns)
                {:error (str "optional ?"
                             (:name (second (first pairs)))
                             "? under a top-level OR — dropping a conjunct is "
                             "ambiguous; parenthesize the OR group")}
                (let [bounds (concat [{:to body-start}] conns [{:from end}])
                      segs   (mapv (fn [[a b]] {:from (:to a) :to (:from b)})
                                   (partition 2 1 bounds))
                      hit?   (fn [{:keys [from to]}]
                               (some (fn [[_ occ]] (<= from (:from occ) (dec to)))
                                     pairs))
                      kept   (->> segs
                                  (remove hit?)
                                  (mapv #(str/trim (subs sql (:from %) (:to %)))))]
                  {:from kw-from :to end
                   :text (if (empty? kept)
                           ""
                           (str (subs sql kw-from body-start) " "
                                (str/join "\n  and " kept) "\n"))})))
            edit-errs (into [] (keep :error) edits)]
        (if (seq edit-errs)
          {:errors (vec (distinct edit-errs))}
          {:sql (reduce (fn [s {:keys [from to text]}]
                          (str (subs s 0 from) text (subs s to)))
                        sql
                        (sort-by :from > (remove :error edits)))})))))

;;; ============================================================================
;;; Rewrite — named → positional (for the server)
;;; ============================================================================

(defn coerce-default
  "Coerce a raw `=default` literal to its typed value."
  [raw type]
  (cond
    (nil? raw) nil
    (or (str/starts-with? raw "[")
        (#{:int :integer :float :number :currency :boolean} type))
    (try (edn/read-string raw) (catch #?(:clj Exception :cljs :default) _ raw))
    :else
    (if (and (>= (count raw) 2) (str/starts-with? raw "\"") (str/ends-with? raw "\""))
      (subs raw 1 (dec (count raw)))
      raw)))

(defn apply-defaults
  "Fill `pmap` with inline defaults for unsupplied params; caller value always wins."
  [named pmap]
  (reduce (fn [m {:keys [name type default]}]
            (if (and (some? default) (not (contains? m name)))
              (assoc m name (coerce-default default type))
              m))
          pmap
          named))

(defn check-consistency
  "Errors that block a rewrite: scalar/array clash and missing values."
  [named pmap]
  (let [distinct-names (->> named (map :name) distinct vec)
        optional?      (fn [nm] (some #(and (= nm (:name %)) (:optional? %)) named))]
    (-> (vec (array-clash-errors named distinct-names))
        (into (for [nm distinct-names
                    :when (and (not (contains? pmap nm))
                               (not (optional? nm)))]
                (str "Missing value for parameter ?" nm)))
        (into (for [nm distinct-names
                    :let [occs   (filter #(= nm (:name %)) named)
                          array? (:array? (first occs))
                          v      (get pmap nm)]
                    :when (and (contains? pmap nm) array?
                               (some? v) (not (sequential? v)))]
                (str "Parameter ?" nm "[] expects an array value"))))))

(defn split-select-items
  "Top-level SELECT-list items of `sql`, or nil when there is no depth-0 SELECT."
  [sql]
  (let [toks   (clause-tokens sql)
        sel    (first (filter #(and (= :word (:kind %)) (zero? (:depth %))
                                    (= "select" (:word %)))
                              toks))
        end    (when sel
                 (or (some #(when (and (= :word (:kind %)) (zero? (:depth %))
                                       (> (:from %) (:to sel))
                                       (#{"from" "where" "group" "having"
                                          "order" "limit" "offset" "union"} (:word %)))
                             (:from %))
                           toks)
                     (count sql)))]
    (when sel
      (let [span (subs sql (:to sel) end)
            n    (count span)
            cuts (loop [i 0 depth 0 quote nil out []]
                   (if (>= i n)
                     out
                     (let [c (char-at span i)]
                       (cond
                         quote (if (= quote c)
                                 (recur (inc i) depth nil out)
                                 (recur (inc i) depth quote out))
                         (or (= "'" c) (= "\"" c)) (recur (inc i) depth c out)
                         (= "{" c) (let [e #?(:clj (.indexOf ^String span "}" (int i))
                                              :cljs (.indexOf span "}" i))]
                                     (if (neg? e) out (recur (inc e) depth quote out)))
                         (= "(" c) (recur (inc i) (inc depth) nil out)
                         (= ")" c) (recur (inc i) (dec depth) nil out)
                         (and (= "," c) (zero? depth)) (recur (inc i) depth nil (conj out i))
                         :else (recur (inc i) depth nil out)))))
            bounds (partition 2 1 (concat [-1] cuts [n]))]
        (->> bounds
             (map (fn [[a b]] (str/trim (subs span (inc a) b))))
             (map-indexed (fn [i item]
                            (if (zero? i)
                              (str/replace item #"(?i)^distinct\s+" "")
                              item)))
             (remove str/blank?)
             vec)))))

(def re-simple-column
  "A SELECT item naming one column: a `{…}` placeholder or a dotted identifier chain."
  #"(?:\{[^{}]+\})|(?:[A-Za-z_][A-Za-z0-9_]*(?:\.\"?[A-Za-z_][A-Za-z0-9_]*\"?)+)")

(defn select-order-exprs
  "Map ORDER-BY key → the SELECT list's own simple-column expression for it."
  [sql]
  (let [entries
        (keep (fn [item]
                (if-let [[_ expr alias]
                         (re-matches #"(?is)(.*?)\s+[Aa][Ss]\s+([A-Za-z_][A-Za-z0-9_]*)\s*" item)]
                  (when (re-matches re-simple-column (str/trim expr))
                    [alias (str/trim expr)])
                  (when (re-matches re-simple-column item)
                    (let [tail (last (str/split item #"\."))
                          nm   (-> tail (str/replace #"[\}\"]" "") str/trim)]
                      (when (re-matches #"[A-Za-z_][A-Za-z0-9_]*" nm)
                        [nm item])))))
              (split-select-items sql))
        dups (->> entries (map first) frequencies
                  (keep (fn [[k n]] (when (> n 1) k)))
                  set)]
    (into {} (remove (comp dups first)) entries)))

(defn build-positional
  "Splice named placeholders out of `sql`, returning `[new-sql params types]`."
  [sql named pmap]
  (let [order-exprs (delay (select-order-exprs sql))
        declared    (reduce (fn [m {:keys [name type]}]
                              (cond-> m (and type (not (contains? m name))) (assoc name type)))
                            {} named)]
    (loop [occs   (sort-by :from named)
           cursor 0
           pieces []
           vals   []
           types  []]
      (if (empty? occs)
        [(str (str/join pieces) (subs sql cursor)) vals types]
        (let [{:keys [from to name array? type type-args]} (first occs)
              v (get pmap name)
              [placeholder add-vals]
              (cond
                ;; SECURITY: only interpolation — normalize-order-specs' allowlist keeps caller text out; never bind here (sorts by a constant)
                (= :order type)
                [(->> (normalize-order-specs v {:name name :type-args type-args})
                      (map (fn [[col dir]]
                             (str (get @order-exprs col col) " "
                                  (str/upper-case dir) " NULLS LAST")))
                      (str/join ", "))
                 []]

              array?
              (let [coll (if (sequential? v) (vec v) [])]
                (if (empty? coll)
                  ["(NULL)" []]
                  [(str "(" (str/join "," (repeat (count coll) "?")) ")") coll]))

              :else ["?" [v]])]
          (recur (rest occs)
                 to
                 (conj pieces (subs sql cursor from) placeholder)
                 (into vals add-vals)
                 (into types (repeat (count add-vals) (get declared name)))))))))

(defn rewrite
  "Compile named placeholders in comment-free `sql` to positional `?`: `{:sql :params :types}` or `{:errors}`."
  [sql params]
  (let [occ        (scan-placeholders sql)
        named      (filterv #(= :named (:kind %)) occ)
        positional (filterv #(= :positional (:kind %)) occ)]
    (cond
      (empty? named)
      (cond
        (or (nil? params) (sequential? params))
        {:sql sql :params (vec params)}

        (map? params)
        (if (empty? positional)
          {:sql sql :params []}
          {:errors ["Template uses positional ? but params is an object — pass an array"]})

        :else
        {:sql sql :params [params]})

      (seq positional)
      {:errors ["Template mixes named (?name) and positional (?) placeholders — use one style"]}

      (not (or (nil? params) (map? params)))
      {:errors ["Named-parameter template requires params as an object {name: value}"]}

      :else
      (let [pmap    (->> params
                         (into {} (map (fn [[k v]] [(name k) v])))
                         (apply-defaults named))
            dropped (filterv #(and (:optional? %)
                                   (not (contains? pmap (:name %))))
                             named)]
        (if (seq dropped)
          (let [r (drop-optional-conjuncts sql dropped)]
            (if (:errors r)
              r
              (rewrite (:sql r) params)))
          (let [errs (check-consistency named pmap)]
            (if (seq errs)
              {:errors errs}
              (let [[new-sql vals types] (build-positional sql named pmap)]
                {:sql new-sql :params vals :types types}))))))))
