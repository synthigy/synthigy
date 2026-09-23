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

(ns synthigy.xsql.sql-params
  "Shared grammar for named SQL-template parameters: `?name[:type][[]]`,
   with `?name?` optional, `(a,b)` restriction sets, and `=default`.
   Server's `rewrite` compiles to positional `?` for JDBC; console's
   `analyze` builds a typed form from the template text. Pure, no deps —
   safe on JVM and CLJS. Mixing named and bare `?` in one template is an
   error."
  (:require [clojure.string :as str]
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
   ;; SECURITY: `order` is the ONE token that INTERPOLATES into SQL text
   ;; instead of binding — safe only because normalize-order-specs
   ;; enforces a closed, author-declared allowlist. No other type may
   ;; ever take this path.
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
  "1-char string at index `i`, or nil if out of bounds."
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
  "Turn a snake/space identifier into a human label: `from_year` →
   \"From year\", `ratings` → \"Ratings\"."
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
  "Remove `-- …` line comments and `/* … */` block comments, leaving
   single-quoted string literals intact (comments collapse to whitespace
   so tokens never merge). Portable variant of the server's StringBuilder version."
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
  "Scan `sql` for `?` placeholders, skipping string literals and
   comments, returning occurrences in document order:
     {:kind :named  :name str  :raw-type str-or-nil  :type kw-or-nil
      :array? bool  :from int  :to int}
     {:kind :positional        :from int  :to int}"
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
                      ;; `?name?` — optional marker, tight after the name,
                      ;; before `:type`. Absent param ⇒ the containing
                      ;; conjunct is dropped by `rewrite` instead of erroring.
                      opt?     (= "?" (char-at sql name-end))
                      mark-end (if opt? (inc name-end) name-end)
                      ;; optional `:type` — a single `:` (not `::`)
                      ;; directly followed by an identifier.
                      typed?   (and (= ":" (char-at sql mark-end))
                                    (ident-start? (char-at sql (inc mark-end))))
                      type-end (if typed?
                                 (scan-while sql (+ mark-end 2) ident-continue?)
                                 mark-end)
                      type-str (when typed?
                                 (subs sql (inc mark-end) type-end))
                      ;; optional `(a, b, c)` restriction set — tight after
                      ;; the type OR the bare name (XSQL order params; see
                      ;; tokens.cljc).
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
  "Normalize an `order`-typed param VALUE to `[[col dir] …]` — a string,
   a sequence of strings, or `[col dir]` pairs; direction defaults asc.
   `type-args` is the author-declared restriction set. SECURITY: every
   column/direction is re-validated here (`[a-z_][a-z0-9_]*`, membership
   in the restriction set) — this is what makes it safe for the caller
   below to interpolate the result straight into SQL text."
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
  "Matches text ENDING at a placeholder that sits in an identifier position —
   `ORDER BY ?x`, `GROUP BY a, ?x`. Public: also used by
   `synthigy.xsql.program`'s completion, so the two can never drift."
  #"(?is)\b(ORDER|GROUP)\s+BY\s+(?:[^,()]*,\s*)*$")

(defn identifier-position-errors
  "SECURITY: a bind param in ORDER BY/GROUP BY is a SILENT no-op — the
   plan is fixed before values bind, so `ORDER BY ?'name'` sorts by a
   constant, every row ties, and no error is ever raised (this is legal
   SQL). Hence error, not warning. `:order`-typed params are the one
   sanctioned exception, and ONLY when they carry a restriction set — an
   unrestricted `:order` param would let caller text reach the
   identifier regex unchecked, which is not an allowlist."
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
  "Analyze a **raw** SQL template for parameter use. Returns:

     {:mode  :named | :positional | :none
      :params [{:name :array? :type :label :typed?
                :type-conflict? :unknown-types} …]
      :positional-count int
      :errors   [{:message str :from int :to int} …]
      :warnings [{:message str :from int :to int} …]}

   `:params` (named mode only) is in document order, one entry per
   distinct name. Each error/warning carries the `:from`/`:to` span of
   the offending placeholder token so the editor can underline it."
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
            ;; Each diagnostic is pinned to the span of the offending
            ;; token(s) — `analyze` runs on the raw template, so `:from`/
            ;; `:to` are real document offsets.
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
  "Validate an already-parsed `value` against a declared `type`. Returns
   an error string, or nil when valid. A nil value is always allowed —
   it binds SQL NULL. For array params every element is checked."
  [type array? value]
  (letfn [(scalar-err [t v]
            (when (some? v)
              (case t
                :int       (when-not (and (number? v) (integer? v))
                             "expects an integer")
                :float     (when-not (number? v) "expects a number")
                :boolean   (when-not (boolean? v) "expects true or false")
                :string    (when-not (string? v) "expects a string")
                :timestamp (when-not (string? v) "expects a timestamp string")
                :uuid      (when-not (string? v) "expects a UUID string")
                nil)))]
    (if array?
      (cond
        (nil? value)              nil
        (not (sequential? value)) "expects an array"
        :else                     (some #(scalar-err type %) value))
      (scalar-err type value))))

(defn validate-params
  "Given the `analyze` `:params` spec and a `{name value}` map, return a
   vector of `[name error-string]` for every invalid entry. A name that
   is absent from `values` is reported as \"required\"."
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
  "Light scan of comment-free `sql`: word tokens `{:kind :word :word lc
   :from :to :depth}` and closing parens `{:kind :close :from :depth}`
   — enough to find clause/conjunct boundaries without parsing SQL."
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
            ;; skip a `?name…` placeholder head so a param named `and`
            ;; can never read as a connective
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
  "Remove the containing top-level WHERE/HAVING conjunct for each
   unsupplied optional `?name?` in `dropped`. Returns `{:sql new-sql}`
   or `{:errors [str …]}` (occurrence outside WHERE/HAVING, or under a
   top-level OR where dropping an AND-conjunct is ambiguous)."
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
  "Coerce a `?name:type=default` literal (a raw string) to its typed value:
   array literals `[…]` and numbers/booleans/quoted-strings via EDN (commas
   read as whitespace, so `[\"a\",\"b\"]` works); otherwise the bare string.
   Shared by the XSQL compiler and the sql-template rewrite."
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
  "Fill `pmap` with each named placeholder's inline `=default` for params
   the caller did not supply. Caller value always wins."
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
  "Top-level SELECT-list items of `sql` as trimmed strings, or nil when no
   depth-0 SELECT is found. Splits on depth-0 commas, skipping quotes and
   `{…}` placeholders; strips a leading DISTINCT."
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
  "A SELECT item that names one column: a `{…}` placeholder or a dotted
   (optionally quoted) identifier chain."
  #"(?:\{[^{}]+\})|(?:[A-Za-z_][A-Za-z0-9_]*(?:\.\"?[A-Za-z_][A-Za-z0-9_]*\"?)+)")

(defn select-order-exprs
  "Map ORDER-BY key → the SELECT list's own expression for it. A bare
   `{entity.field}` (or `tbl.col`) item maps its output name to the item
   itself, so an `:order` interpolation emits the qualified expression —
   a bare column name is ambiguous on SQLite once auto-joins share the
   name. Explicitly aliased items keep the bare alias (both engines
   resolve output aliases in ORDER BY). Duplicate output names drop out."
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
  "Splice named placeholders out of `sql`, returning `[new-sql params]`.
   A scalar param emits `?`; an array emits `(?,?,…)`, or `(NULL)` when
   empty (the standard empty-IN workaround); portable across PG+SQLite —
   PG callers wanting `ANY` write `ANY(ARRAY[?names[]])` instead."
  [sql named pmap]
  (let [order-exprs (delay (select-order-exprs sql))]
    (loop [occs   (sort-by :from named)
           cursor 0
           pieces []
           vals   []]
      (if (empty? occs)
        [(str (str/join pieces) (subs sql cursor)) vals]
        (let [{:keys [from to name array? type type-args]} (first occs)
              v (get pmap name)
              [placeholder add-vals]
              (cond
                ;; SECURITY: the ONE interpolating type. normalize-order-specs
                ;; enforces a closed, author-declared column allowlist, so this
                ;; never carries caller text. No bind values — a bound `?`
                ;; here would silently sort by a constant instead.
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
                 (into vals add-vals)))))))

(defn rewrite
  "Compile named placeholders in **comment-free** `sql` down to positional
   `?`. `params` is a `{name value}` map (named mode) or a vector/seq
   (positional mode). Returns `{:sql str :params vector}` on success, or
   `{:errors [str …]}` on a validation failure.

   A template with no `?name` placeholder is passed through unchanged
   (legacy positional path). Inline `:type` annotations and `[]` markers
   are stripped — the rewritten SQL carries only positional `?`."
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
            ;; `?name?` occurrences with no value (and no default): their
            ;; containing conjunct is dropped, then the reduced text is
            ;; rewritten from scratch. Terminates — dropped occurrences
            ;; leave the text with the conjunct.
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
              (let [[new-sql vals] (build-positional sql named pmap)]
                {:sql new-sql :params vals}))))))))
