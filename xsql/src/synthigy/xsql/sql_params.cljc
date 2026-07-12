(ns synthigy.xsql.sql-params
  "Shared grammar for **named SQL-template parameters**.

   One parser, two consumers:

   - server — `synthigy.dataset.sql.template` calls `rewrite` to compile
     `?name` placeholders down to positional `?` (the only thing JDBC
     binds) before CTE-splitting and `{Entity.field}` resolution.
   - console — the params pane calls `analyze` to build a typed,
     labelled form purely from parsing the template text.

   ## Grammar

   A placeholder is `?` followed by a name, an optional `:type`, and an
   optional trailing `[]`:

     ?name              — scalar, untyped (defaults to `string`)
     ?name:type         — scalar, typed
     ?name[]            — array, untyped
     ?name:type[]       — array, typed

   Examples: `?from_year:int`, `?price:float`, `?roles:integer[]`,
   `?role_names:string[]`.

   The type sits inline after a single `:` (a `::` cast is left alone).
   The `[]` array marker is always last. The form label is the
   prettified name (`from_year` → \"From year\") — there is no separate
   label syntax.

   Plain bare `?` is still positional and back-compatible. **Mixing named
   and bare `?` in one template is an error.**

   Type vocabulary: `int`/`integer`, `float`/`number`, `string`/`text`,
   `boolean`, `timestamp`, `uuid`.

   This namespace is pure and has no external dependencies — safe to load
   on the server (JVM) and in the console (ClojureScript)."
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
   "uuid"      :uuid})

(def canonical-types
  "The set of canonical type keywords."
  #{:int :float :string :boolean :timestamp :uuid})

(defn- canonical-type
  "Resolve a surface type token to a canonical keyword, or nil if unknown."
  [token]
  (when token (get type-aliases (str/lower-case token))))

;;; ============================================================================
;;; Char helpers (portable — 1-char strings, no java.lang.Character)
;;; ============================================================================

(def ^:private re-ident-start    #"[A-Za-z_]")
(def ^:private re-ident-continue #"[A-Za-z0-9_]")

(defn- char-at
  "1-char string at index `i`, or nil if out of bounds."
  [s i]
  (when (and (>= i 0) (< i (count s)))
    (subs s i (inc i))))

(defn- ident-start?    [c] (boolean (and c (re-matches re-ident-start c))))
(defn- ident-continue? [c] (boolean (and c (re-matches re-ident-continue c))))

(defn- scan-while
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
   single-quoted string literals intact. Line comments collapse to a
   newline, block comments to a space, so tokens never merge.

   The server has its own StringBuilder version; this portable variant
   exists so the console can scan a comment-free template the same way."
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
  "Scan `sql` for `?` placeholders, skipping string literals and `--` /
   `/* … */` comments — so `:from`/`:to` are real positions in the
   string passed in (the editor passes the raw template; the server
   passes comment-free SQL). Returns occurrences in document order:

     {:kind :named  :name str  :raw-type str-or-nil  :type kw-or-nil
      :array? bool  :from int  :to int}
     {:kind :positional        :from int  :to int}

   A `?` followed by an identifier is named (`?name`, `?name:type`,
   `?name:type[]`, `?name[]`); any other `?` is positional. A `::` cast
   after a name is left intact — only a single `:` introduces a type."
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
                      ;; optional `:type` — a single `:` (not `::`)
                      ;; directly followed by an identifier.
                      typed?   (and (= ":" (char-at sql name-end))
                                    (ident-start? (char-at sql (inc name-end))))
                      type-end (if typed?
                                 (scan-while sql (+ name-end 2) ident-continue?)
                                 name-end)
                      type-str (when typed?
                                 (subs sql (inc name-end) type-end))
                      ;; optional trailing `[]`
                      array?   (and (= "[" (char-at sql type-end))
                                    (= "]" (char-at sql (inc type-end))))
                      end0     (if array? (+ type-end 2) type-end)
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
                                     has-def? (assoc :default (subs sql def-s def-e))))))
                (recur (inc i) :normal
                       (conj out {:kind :positional :from i :to (inc i)})))

              :else (recur (inc i) :normal out))))))))

;;; ============================================================================
;;; Analysis — for the console form
;;; ============================================================================

(defn- array-clash-errors
  "Per-name errors: a name used both as scalar and as array."
  [named distinct-names]
  (for [nm distinct-names
        :let [occs (filter #(= nm (:name %)) named)]
        :when (> (count (distinct (map :array? occs))) 1)]
    (str "?" nm " is used as both a scalar and an array — pick one")))

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
                         :from (:from o) :to (:to o)})))
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
   (for [{:keys [name array? type]} param-specs
         :let [err (if-not (contains? values name)
                     "required"
                     (validate-value type array? (get values name)))]
         :when err]
     [name err])))

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

(defn- apply-defaults
  "Fill `pmap` with each named placeholder's inline `=default` for params the
   caller did not supply. Caller value always wins; a param that is present
   (even nil) is left alone."
  [named pmap]
  (reduce (fn [m {:keys [name type default]}]
            (if (and (some? default) (not (contains? m name)))
              (assoc m name (coerce-default default type))
              m))
          pmap
          named))

(defn- check-consistency
  "Errors that block a rewrite: scalar/array clash and missing values."
  [named pmap]
  (let [distinct-names (->> named (map :name) distinct vec)]
    (-> (vec (array-clash-errors named distinct-names))
        (into (for [nm distinct-names
                    :when (not (contains? pmap nm))]
                (str "Missing value for parameter ?" nm)))
        (into (for [nm distinct-names
                    :let [occs   (filter #(= nm (:name %)) named)
                          array? (:array? (first occs))
                          v      (get pmap nm)]
                    :when (and (contains? pmap nm) array?
                               (some? v) (not (sequential? v)))]
                (str "Parameter ?" nm "[] expects an array value"))))))

(defn- build-positional
  "Splice named placeholders out of `sql`, returning `[new-sql params]`.
   A scalar param emits `?`; an array emits `(?,?,…)` (parenthesized list,
   one slot per element), or `(NULL)` when empty.

   The parenthesized-list form is portable across PostgreSQL and SQLite:
   `WHERE x IN ?names[]` becomes `WHERE x IN (?,?,?)` on both backends.
   Empty list → `(NULL)` so `x IN (NULL)` evaluates to UNKNOWN (no rows
   match), the standard SQL workaround for an empty IN clause.

   Note for PG users: the previous expansion was `ARRAY[?,?,?]` (suited
   to `= ANY(…)`). The portable shift to `(?,?,?)` means callers wanting
   PG `ANY` should write `ANY(ARRAY[?names[]])` and accept the redundant
   outer parens, or switch to `IN`."
  [sql named pmap]
  (loop [occs   (sort-by :from named)
         cursor 0
         pieces []
         vals   []]
    (if (empty? occs)
      [(str (str/join pieces) (subs sql cursor)) vals]
      (let [{:keys [from to name array?]} (first occs)
            v (get pmap name)
            [placeholder add-vals]
            (if array?
              (let [coll (if (sequential? v) (vec v) [])]
                (if (empty? coll)
                  ["(NULL)" []]
                  [(str "(" (str/join "," (repeat (count coll) "?")) ")") coll]))
              ["?" [v]])]
        (recur (rest occs)
               to
               (conj pieces (subs sql cursor from) placeholder)
               (into vals add-vals))))))

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
      (let [pmap (->> params
                      (into {} (map (fn [[k v]] [(name k) v])))
                      (apply-defaults named))
            errs (check-consistency named pmap)]
        (if (seq errs)
          {:errors errs}
          (let [[new-sql vals] (build-positional sql named pmap)]
            {:sql new-sql :params vals}))))))
