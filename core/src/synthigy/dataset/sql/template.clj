(ns synthigy.dataset.sql.template
  "SQL template resolver for analytics queries.

   Write standard SQL with ERD-aware placeholders. FROM and JOINs are
   generated automatically from the deployed schema — you never write them.

   ## Placeholders

   ### Entity field: `{Entity.field}`
   References a scalar field on an entity. The entity becomes a FROM source.

     SELECT {User.name}, {User.active}
     WHERE {User.active} = ?

   ### Relation field: `{Entity OPERATOR relation.field}`
   Traverses a relation and references a field on the target entity.
   The join (including junction table) is auto-generated.

     SELECT {User.name}, {User -> roles.name} as role_name
     WHERE {User -> roles.active} = ?

   ### Chain joins: `{Entity -> rel1 -> rel2.field}`
   Multi-hop traversal through relations. Each hop generates the
   appropriate join pair (junction + target).

     SELECT {User.name},
            {User -> roles.name} as role,
            {User -> roles -> scopes.name} as scope

   ### Bare entity: `{Entity}`
   Resolves to the physical table name with alias. Rarely needed since
   FROM is auto-generated, but available for edge cases.

   ## Join Operators

   | Operator | Join Type  | Direction                          |
   |----------|------------|------------------------------------|
   | `-`      | INNER      | Only matching rows                 |
   | `->`     | LEFT       | All from source, matching target   |
   | `<-`     | RIGHT      | Matching source, all from target   |
   | `=>`     | FULL OUTER | All rows from both sides           |

   The direction reads naturally: `{User -> roles}` means 'from User,
   left join to roles'. `{User Role <- users}` means 'from Role,
   right join to users'.

   ## Auto-generated SQL

   - **FROM**: Generated from entity references. Each distinct root entity
     becomes a FROM source.
   - **JOINs**: Generated from relation traversals. Each relation produces
     two joins (junction table + target table). Duplicate relations are
     deduplicated — using the same relation in multiple placeholders
     produces only one join pair.
   - **Placement**: FROM and JOINs are injected before the first
     WHERE/GROUP BY/HAVING/ORDER BY/LIMIT clause.

   ## Parameters

   Use `?` positional parameters (compatible with all database backends):

     (execute-template
       \"SELECT {User.name} WHERE {User.active} = ? AND {User.name} LIKE ?\"
       [true \"A%\"])

   Or named `?name:type` / `?name:type[]` placeholders bound from a map —
   the inline type is advisory (the console type-checks; the server
   strips it). See `synthigy.xsql.sql-params`:

     (execute-template
       \"SELECT {User.name} WHERE {User.active} = ?active:boolean
         AND {User.name} IN ?names:string[]\"
       {\"active\" true \"names\" [\"Alice\" \"Bob\"]})

   ## Examples

   Simple field selection:

     (execute-template
       \"SELECT {User.name}, {User.active} WHERE {User.active} = ?\"
       [true])

   Join with aggregate:

     (execute-template
       \"SELECT {User Role.name}, count({User Role <- users.name}) as cnt
        GROUP BY {User Role.name}
        ORDER BY cnt DESC\"
       nil)

   Chain join:

     (execute-template
       \"SELECT {User.name},
               {User -> roles.name} as role,
               {User -> roles -> scopes.name} as scope
        WHERE {User.name} = ?\"
       [\"Alice\"])

   Mixed joins in one query:

     (execute-template
       \"SELECT {User.name},
               count(DISTINCT {User -> roles.name}) as role_count,
               count(DISTINCT {User -> groups.name}) as group_count
        WHERE {User.active} = ?
        GROUP BY {User.name}
        ORDER BY role_count DESC
        LIMIT ?\"
       [true 10])

   Via the Clojure client:

     (client/query c
       \"SELECT {User.name}, count({User -> roles._eid}) as cnt
        WHERE {User.active} = ?
        GROUP BY {User.name}
        HAVING cnt > ?\"
       [true 2])

   ## Constraints

   - SELECT only — INSERT/UPDATE/DELETE are rejected
   - Templates are cached (TTL 30 min), cleared on model deploy
   - Cache can be bypassed with `{:cached false}`"
  (:require
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.dataset.rls :as rls]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.xsql.sql-params :as sql-params]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]))

;;; ============================================================================
;;; Placeholder Parsing
;;; ============================================================================

;; Join operators are WHITESPACE-DELIMITED: `{A -> B.f}`, `{A - B.f}` (inner).
;; The required spaces fence a join `-` from a `-` inside a kebab identifier —
;; `{identity-document.country}` (tight) is a name, `{a - b}` (spaced) is a join.
;; Field access (`.`) stays tight; only the inter-entity join op needs spaces.
(def ^:private join-pattern
  #"\s+(=>|->|<-|-)\s+")

(def ^:private join-type-map
  {"=>" "FULL OUTER"
   "->" "LEFT"
   "<-" "RIGHT"
   "-"  "INNER"})

(defn- split-on-joins
  "Split a placeholder string on join operators.
   Returns {:root \"entity\" :chain [{:label \"rel\" :join \"INNER\"} ...]}"
  [s]
  (let [matcher (re-matcher join-pattern s)]
    (loop [last-end 0
           segments []
           join-types []]
      (if (.find matcher)
        (recur (.end matcher)
               (conj segments (str/trim (subs s last-end (.start matcher))))
               (conj join-types (get join-type-map (.group matcher 1))))
        (let [final (str/trim (subs s last-end))
              all-segments (conj segments final)]
          {:root (first all-segments)
           :chain (mapv (fn [segment join-type]
                          {:label segment :join join-type})
                        (rest all-segments)
                        join-types)})))))

(defn- parse-placeholder
  "Parse a single placeholder string (without braces) into a structured form."
  [s]
  (let [s (str/trim s)
        {:keys [root chain]} (split-on-joins s)]
    (if (empty? chain)
      (let [[entity field] (str/split root #"\." 2)]
        (if field
          {:type :field :entity entity :field field}
          {:type :entity :entity entity}))
      (let [entity root
            last-item (last chain)
            [last-rel field] (str/split (:label last-item) #"\." 2)
            joins (conj (vec (butlast chain))
                        (assoc last-item :label last-rel))]
        (if field
          {:type :relation-field :entity entity :path (mapv :label joins)
           :field field :joins joins}
          {:type :relation :entity entity :path (mapv :label joins)
           :joins joins})))))

(defn- extract-placeholders
  "Extract all {placeholder} occurrences from template string."
  [template]
  (let [matcher (re-matcher #"\{([^}]+)\}" template)]
    (loop [results []]
      (if (.find matcher)
        (recur (conj results
                     {:raw (.group matcher 0)
                      :inner (.group matcher 1)
                      :parsed (parse-placeholder (.group matcher 1))
                      :start (.start matcher)
                      :end (.end matcher)}))
        results))))

;;; ============================================================================
;;; Entity & Schema Resolution
;;; ============================================================================

(defn- resolve-relation
  "Resolve a relation label to a traversal spec.

   Three flavours of relation exist, all supported here:

   1. :junction — m2m/o2m via a junction table. Two JOINs needed
      (source ↔ junction ↔ target). Spec comes from schema :relations.

   2. :fk-self — self-referential tree cardinality (e.g. :father, :mother
      on Human). Single JOIN: target._eid = source.<fk-col>.
      Source in schema :recursions set; fk column name matches the label.

   3. :fk-ref — field-level entity reference (e.g. :assignee on Project
      Task, typed \"user\"). Single JOIN: target._eid = source.<fk-col>.
      Target entity id read from field's :reference/entity."
  [entity-id relation-label]
  (let [schema (sql-query/deployed-schema-entity entity-id)
        label-kw (keyword (-> relation-label
                              str/trim
                              (str/replace #"([a-z])([A-Z])" "$1_$2")
                              str/lower-case
                              (str/replace #"[\s]+" "_")))
        junction-rel (get-in schema [:relations label-kw])
        recursion? (contains? (:recursions schema) label-kw)
        attr-id (get-in schema [:field->attribute label-kw])
        ref-entity (when attr-id
                     (:reference/entity (get-in schema [:fields attr-id])))]
    (cond
      junction-rel
      (assoc junction-rel :kind :junction)

      recursion?
      {:kind :fk-self
       :to entity-id
       :fk-column (name label-kw)
       :to/table (:table schema)}

      ref-entity
      (let [target-schema (sql-query/deployed-schema-entity ref-entity)]
        {:kind :fk-ref
         :to ref-entity
         :fk-column (name label-kw)
         :to/table (:table target-schema)})

      :else
      (throw (ex-info (str "Unknown relation: " relation-label)
                      {:code "UNKNOWN_TEMPLATE_RELATION"
                       :entity entity-id
                       :relation relation-label})))))

;;; ============================================================================
;;; Alias Resolution (pure — returns updated state)
;;; ============================================================================

(defn- get-alias
  "Get or create an alias for a key. Returns [alias updated-state]."
  [state key]
  (if-let [existing (get-in state [:aliases key])]
    [existing state]
    (let [n (inc (:counter state 0))
          alias (str "e" n)
          state (-> state
                    (assoc-in [:aliases key] alias)
                    (assoc :counter n))]
      [alias state])))

;;; ============================================================================
;;; Relation Chain Walker (pure — returns updated state)
;;; ============================================================================

(defn- walk-relation-chain
  "Walk a relation chain, collecting JOINs. Returns [final-alias updated-state].
  Also registers the target alias under [:entity target-id] so that
  subsequent field references to the same entity reuse the join alias.

  Junction relations emit two JOINs (source ↔ junction ↔ target).
  FK-style relations (self recursions, field refs) emit one JOIN."
  [entity-id entity-alias joins-spec state]
  (reduce
    (fn [[current-id current-alias state] {:keys [label join]}]
      (let [join-type (or join "INNER")
            rel (resolve-relation current-id label)
            target-id (:to rel)
            {:keys [table]} (sql-query/deployed-schema-entity target-id)
            [target-alias state] (get-alias state [:relation current-id label])
            join-key [current-id label]
            ;; Register target alias under [:entity target-id] so bare
            ;; references like {User Role.name} can reuse the join.
            ;; BUT don't clobber an existing mapping — the root entity
            ;; already owns its alias, and self-referencing joins
            ;; (tree father/mother on same entity) or multiple joins to
            ;; the same target must not steal the :entity slot from them.
            state (cond-> (update state :entities conj target-id)
                    (not (get-in state [:aliases [:entity target-id]]))
                    (assoc-in [:aliases [:entity target-id]] target-alias))]
        (if (contains? (:join-keys-seen state) join-key)
          [target-id target-alias state]
          (case (:kind rel)
            :junction
            (let [[link-alias state] (get-alias state [:link current-id label])
                  state (-> state
                            (update :join-keys-seen conj join-key)
                            (update :joins conj
                                    (format "%s JOIN \"%s\" %s ON %s._eid=%s.%s"
                                            join-type (:table rel) link-alias
                                            current-alias link-alias (:from/field rel)))
                            (update :joins conj
                                    (format "%s JOIN \"%s\" %s ON %s.%s=%s._eid"
                                            join-type table target-alias
                                            link-alias (:to/field rel) target-alias)))]
              [target-id target-alias state])

            ;; FK-style (self recursion or field reference): single JOIN
            (:fk-self :fk-ref)
            (let [state (-> state
                            (update :join-keys-seen conj join-key)
                            (update :joins conj
                                    (format "%s JOIN \"%s\" %s ON %s._eid=%s.%s"
                                            join-type table target-alias
                                            target-alias current-alias (:fk-column rel))))]
              [target-id target-alias state])))))
    [entity-id entity-alias state]
    joins-spec))

;;; ============================================================================
;;; Template Resolution (pure reduce)
;;; ============================================================================

(defn- enum-aware-column
  "Render `alias.field`. An enum-typed column gets a `CAST(… AS TEXT)`:
   a PG enum has no `=` operator against a bound varchar parameter, so a
   template comparing `{Entity.enumField} = ?` would otherwise fail.
   Casting to text also keeps projected values as plain strings.

   Uses the standard SQL `CAST(x AS TEXT)` form rather than the PG-specific
   `::text` shorthand so SQLite (which has no `::` cast operator) accepts
   the same compiled SQL. SQLite stores enums as TEXT already, so the
   cast is a no-op there; the cost is negligible."
  [entity-id alias field]
  (let [{:keys [fields]} (sql-query/deployed-schema-entity entity-id)
        fkey  (keyword field)
        enum? (some (fn [[_ f]] (and (= (:key f) fkey) (:enum/name f)))
                    fields)]
    (if enum?
      (format "CAST(%s.%s AS TEXT)" alias field)
      (format "%s.%s" alias field))))

(defn resolve-template
  "Resolve a SQL template: parse placeholders, resolve to physical names,
   auto-generate FROM + JOINs. Pure function — no mutation."
  [template]
  (let [;; Process relation/relation-field first so join aliases exist
        ;; before field references try to resolve them
        type-order {:relation 0 :relation-field 1 :entity 2 :field 3}
        placeholders (sort-by #(get type-order (get-in % [:parsed :type]) 9)
                              (extract-placeholders template))
        init-state {:aliases {}
                    :counter 0
                    :entities #{}
                    :joins []
                    :join-keys-seen #{}
                    :root-entities {}
                    :replacements []
                    :errors []}

        final-state
        (reduce
          (fn [state {:keys [raw parsed start]}]
            (try
              (let [resolve-root
                    (fn [state entity-name]
                      (let [id (sql-query/resolve-entity entity-name)
                            [alias state] (get-alias state [:entity id])]
                        [id alias (-> state
                                      (update :entities conj id)
                                      (update :root-entities assoc id alias))]))]
                (case (:type parsed)
                  ;; {User} — bare entity
                  :entity
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        {:keys [table]} (sql-query/deployed-schema-entity id)]
                    (update state :replacements conj
                            {:raw raw :replacement (format "\"%s\" %s" table alias)}))

                  ;; {User.name} → e1.name  (enum columns → CAST(e1.col AS TEXT)
                  ;; so they compare against / project as plain strings rather
                  ;; than requiring a PG enum-typed parameter; portable across
                  ;; PG + SQLite — see enum-aware-column)
                  :field
                  (let [[id alias state] (resolve-root state (:entity parsed))]
                    (update state :replacements conj
                            {:raw raw
                             :replacement (enum-aware-column
                                           id alias (:field parsed))}))

                  ;; {User - roles} → "table" alias (for FROM) or alias (in other positions)
                  :relation
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        joins-spec (or (:joins parsed)
                                       (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                        [target-id target-alias state] (walk-relation-chain id alias joins-spec state)
                        {:keys [table]} (sql-query/deployed-schema-entity target-id)]
                    (update state :replacements conj
                            {:raw raw :replacement (format "\"%s\" %s" table target-alias)}))

                  ;; {User - roles.name} → e2.name
                  :relation-field
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        joins-spec (or (:joins parsed)
                                       (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                        [target-id target-alias state] (walk-relation-chain id alias joins-spec state)]
                    (update state :replacements conj
                            {:raw raw
                             :replacement (enum-aware-column
                                           target-id target-alias (:field parsed))}))))
              (catch Exception e
                (update state :errors conj
                        {:raw raw :error (ex-message e) :position start}))))
          init-state
          placeholders)]

    (if (not-empty (:errors final-state))
      {:errors (:errors final-state)}
      (let [{:keys [replacements root-entities joins]} final-state
            ;; Apply replacements to template
            resolved-sql (reduce
                           (fn [sql {:keys [raw replacement]}]
                             (str/replace-first sql raw replacement))
                           template
                           replacements)

            ;; Build FROM clause from root entities
            from-parts (mapv
                         (fn [[id alias]]
                           (let [{:keys [table]} (sql-query/deployed-schema-entity id)]
                             (format "\"%s\" %s" table alias)))
                         root-entities)
            from-clause (str "FROM " (str/join ", " from-parts))
            has-from? (re-find #"(?i)\bFROM\b" resolved-sql)
            join-str (when (not-empty joins) (str/join "\n" joins))

            ;; What needs injecting
            inject-str (if has-from? join-str
                           (str from-clause
                                (when join-str (str "\n" join-str))))

            final-sql
            (if-not inject-str
              resolved-sql
              (let [insert-point (re-find #"(?i)\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION)"
                                          resolved-sql)]
                (if insert-point
                  (let [kw (first insert-point)
                        idx (str/index-of resolved-sql kw)]
                    (if (and idx (pos? idx))
                      (str (str/trimr (subs resolved-sql 0 idx))
                           "\n" inject-str "\n"
                           (subs resolved-sql idx))
                      (str resolved-sql "\n" inject-str)))
                  (str resolved-sql "\n" inject-str))))]

        {:sql final-sql
         :entities (:entities final-state)
         :aliases (:aliases final-state)}))))

;;; ============================================================================
;;; Validation
;;; ============================================================================

(defn strip-leading-sql-comments
  "Drop leading whitespace and SQL comments — `-- …` line comments and
   `/* … */` block comments — from `sql`, returning the remainder
   starting at the first substantive token. A template may open with a
   documentation header (e.g. explaining each `?` parameter); without
   this the SELECT-only guard would see `--` and reject the query."
  [^String sql]
  (loop [s (str/triml (or sql ""))]
    (cond
      (str/starts-with? s "--")
      (let [nl (str/index-of s "\n")]
        (recur (str/triml (if nl (subs s (inc nl)) ""))))

      (str/starts-with? s "/*")
      (let [end (str/index-of s "*/")]
        (recur (str/triml (if end (subs s (+ end 2)) ""))))

      :else s)))

(defn strip-sql-comments
  "Remove ALL SQL comments — `-- …` line comments and `/* … */` block
   comments — from `sql`, leaving string literals intact (a `--` or
   `/*` inside a single-quoted string is content, not a comment).

   Comments are stripped before resolution because the resolver scans
   for keywords (`FROM`, `WHERE`, `GROUP BY`, …) with plain regexes; a
   comment like `-- ?1 from year` would otherwise make `has-from?`
   match the word `from` and the engine would skip FROM injection,
   producing `SELECT … LEFT JOIN` with no FROM. Line comments collapse
   to a newline and block comments to a space so tokens never merge."
  [^String sql]
  (let [n  (count sql)
        sb (StringBuilder.)]
    (loop [i 0]
      (if (>= i n)
        (.toString sb)
        (let [c  (.charAt sql i)
              c2 (when (< (inc i) n) (.charAt sql (inc i)))]
          (cond
            ;; single-quoted string literal — copy verbatim to its close
            (= c \')
            (let [end (loop [j (inc i)]
                        (cond
                          (>= j n) j
                          (and (= (.charAt sql j) \')
                               (< (inc j) n)
                               (= (.charAt sql (inc j)) \'))
                          (recur (+ j 2))           ; escaped ''
                          (= (.charAt sql j) \') (inc j)
                          :else (recur (inc j))))]
              (.append sb (subs sql i (min end n)))
              (recur (min end n)))

            ;; -- line comment → newline
            (and (= c \-) (= c2 \-))
            (let [nl (str/index-of sql "\n" i)]
              (if nl
                (do (.append sb \newline) (recur (inc nl)))
                (.toString sb)))

            ;; /* block comment */ → single space
            (and (= c \/) (= c2 \*))
            (let [end (str/index-of sql "*/" (+ i 2))]
              (if end
                (do (.append sb \space) (recur (+ end 2)))
                (.toString sb)))

            :else
            (do (.append sb c) (recur (inc i)))))))))

(defn validate-select-only!
  "Ensure the template is a SELECT statement (optionally fronted by a WITH
   clause). Leading SQL comments are skipped before the check. Throws on
   violation."
  [template]
  (let [trimmed (str/upper-case (strip-leading-sql-comments template))]
    (when-not (or (str/starts-with? trimmed "SELECT")
                  (str/starts-with? trimmed "WITH "))
      (throw (ex-info "Only SELECT queries are allowed"
                      {:code "QUERY_NOT_SELECT"})))))

;;; ============================================================================
;;; CTE Scope Parsing
;;; ============================================================================

(defn- find-matching-close
  "Given a string and the index of an open '(', return the index of the
   matching ')'. Throws on unbalanced parens."
  [^String s open-idx]
  (loop [i (inc open-idx) depth 1]
    (cond
      (>= i (.length s))
      (throw (ex-info "Unbalanced parens in template"
                      {:code "TEMPLATE_UNBALANCED_PARENS"
                       :position open-idx}))
      :else
      (let [c (.charAt s i)]
        (cond
          (= c \() (recur (inc i) (inc depth))
          (= c \)) (if (= 1 depth) i (recur (inc i) (dec depth)))
          :else    (recur (inc i) depth))))))

(defn- skip-ws [^String s i]
  (loop [i i]
    (if (and (< i (.length s)) (Character/isWhitespace (.charAt s i)))
      (recur (inc i))
      i)))

(defn- read-ident
  "Read a SQL identifier (optionally double-quoted) starting at i. Returns
   [identifier-string next-index]."
  [^String s i]
  (if (and (< i (.length s)) (= \" (.charAt s i)))
    (let [end (str/index-of s "\"" (inc i))]
      (when-not end
        (throw (ex-info "Unterminated quoted identifier in CTE name"
                        {:code "TEMPLATE_BAD_CTE"})))
      [(subs s (inc i) end) (inc end)])
    (let [end (loop [j i]
                (if (and (< j (.length s))
                         (let [c (.charAt s j)]
                           (or (Character/isLetterOrDigit c) (= c \_))))
                  (recur (inc j))
                  j))]
      (when (= i end)
        (throw (ex-info "Expected identifier"
                        {:code "TEMPLATE_BAD_CTE" :position i})))
      [(subs s i end) end])))

(defn parse-cte-scopes
  "Peel off a leading `WITH name AS (body) [, name AS (body)]*` clause.
   Returns `[ctes outer-sql]`, where `ctes` is a vector of
   `{:name string :body string}` in source order, and `outer-sql` is the
   remaining SELECT statement. If the template does not start with WITH,
   returns `[[] template]` unchanged."
  [^String template]
  ;; Strip a leading comment header first — otherwise a template that
  ;; opens with `-- …` documentation never matches the `WITH` check,
  ;; CTE detection silently fails, and the whole query collapses into
  ;; a single mis-resolved scope (the `syntax error at LEFT` bug).
  (let [t       (str/triml (strip-leading-sql-comments template))
        upper-t (str/upper-case t)]
    (if-not (or (str/starts-with? upper-t "WITH ")
                (str/starts-with? upper-t "WITH\t")
                (str/starts-with? upper-t "WITH\n"))
      [[] t]
      (let [;; Skip "WITH" and any "RECURSIVE" qualifier
            after-with (skip-ws t 4)
            [after-with]
            (let [tail (subs upper-t (skip-ws upper-t 4))]
              (if (str/starts-with? tail "RECURSIVE")
                [(skip-ws t (+ (skip-ws upper-t 4) (count "RECURSIVE")))]
                [after-with]))]
        (loop [i     after-with
               ctes  []]
          (let [i (skip-ws t i)
                [cte-name name-end] (read-ident t i)
                after-name (skip-ws t name-end)
                ;; expect AS
                after-as
                (do
                  (when-not (and (<= (+ after-name 2) (.length t))
                                 (= "AS" (str/upper-case (subs t after-name (+ after-name 2)))))
                    (throw (ex-info (str "Expected AS after CTE name '" cte-name "'")
                                    {:code "TEMPLATE_BAD_CTE" :position after-name})))
                  (skip-ws t (+ after-name 2)))
                _ (when-not (and (< after-as (.length t))
                                 (= \( (.charAt t after-as)))
                    (throw (ex-info (str "Expected '(' after AS for CTE '" cte-name "'")
                                    {:code "TEMPLATE_BAD_CTE" :position after-as})))
                close-idx (find-matching-close t after-as)
                body (subs t (inc after-as) close-idx)
                after-body (skip-ws t (inc close-idx))
                ctes' (conj ctes {:name cte-name :body body})]
            (cond
              (and (< after-body (.length t)) (= \, (.charAt t after-body)))
              (recur (inc after-body) ctes')

              :else
              [ctes' (subs t after-body)])))))))

(defn- count-question-marks
  "Count `?` parameter placeholders in s, ignoring `?` characters that
   appear inside single-quoted SQL string literals. Adequate for the
   shapes templates actually carry; not a full lexer."
  [^String s]
  (loop [i 0 in-str? false n 0]
    (if (>= i (.length s))
      n
      (let [c (.charAt s i)]
        (cond
          (and in-str? (= c \'))
          (if (and (< (inc i) (.length s)) (= \' (.charAt s (inc i))))
            (recur (+ i 2) true n)         ; escaped ''
            (recur (inc i) false n))       ; closing quote

          in-str?
          (recur (inc i) true n)

          (= c \')
          (recur (inc i) true n)           ; opening quote

          (= c \?)
          (recur (inc i) false (inc n))

          :else
          (recur (inc i) false n))))))

;;; ============================================================================
;;; RLS Injection
;;; ============================================================================

(defn- split-at-tail-clauses
  "Split `sql` at the first top-level ORDER BY / GROUP BY / HAVING / LIMIT
   / OFFSET / FETCH clause, returning `[head tail]`. The injector uses
   this so an RLS predicate lands INSIDE the WHERE clause rather than
   after a trailing LIMIT (which would produce `LIMIT 100 AND <rls>` —
   a syntax error). Returns `[sql \"\"]` when no tail clause is present."
  [sql]
  (let [m (re-matcher
           #"(?i)\b(?:ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|OFFSET|FETCH\s+FIRST)\b"
           sql)]
    (if (.find m)
      [(subs sql 0 (.start m)) (subs sql (.start m))]
      [sql ""])))

(defn- inject-rls
  "Inject RLS WHERE conditions for all entities referenced in the template.
  Uses the same compiled guards as the regular query pipeline. The
  predicate is appended to (or used to introduce) the WHERE clause —
  BEFORE any trailing ORDER BY / LIMIT / OFFSET so the resulting SQL
  stays valid."
  [sql params entities aliases]
  (if-not (rls/should-apply-guards?)
    [sql params]
    (reduce
     (fn [[sql params] entity-id]
       (let [alias (get aliases [:entity entity-id])
             {:keys [rls]} (sql-query/deployed-schema-entity entity-id)
             {:keys [enabled guards]} rls]
         (if (and enabled alias)
           (let [{rls-sql :sql rls-params :params}
                 (rls/compile-guards-to-sql alias guards :read)
                 effective-sql    (or rls-sql "1=0")
                 effective-params (if rls-sql rls-params [])
                 [head tail] (split-at-tail-clauses sql)
                 head' (if (re-find #"(?i)\bWHERE\b" head)
                         (str (str/trimr head) " AND " effective-sql)
                         (str (str/trimr head) " WHERE " effective-sql))]
             [(if (empty? tail) head' (str head' " " tail))
              (into params effective-params)])
           [sql params])))
     [sql (vec params)]
     entities)))

;;; ============================================================================
;;; Execution
;;; ============================================================================

(defn- resolve-scope-cached
  "Resolve a single scope's body to SQL+entities+aliases, using the
   template cache when `cached?` is truthy."
  [body cached?]
  (if cached?
    (or (sql-query/cached-template body)
        (let [resolved (resolve-template body)]
          (when-not (:errors resolved)
            (sql-query/cache-template body resolved))
          resolved))
    (resolve-template body)))

(defn- throw-on-errors! [{:keys [errors]}]
  (when errors
    (throw (ex-info (str "Template resolution failed: "
                         (str/join "; "
                                   (map (fn [{:keys [error position]}]
                                          (if position
                                            (format "%s (at position %d)" error position)
                                            error))
                                        errors)))
                    {:code "TEMPLATE_ERROR" :errors errors}))))

(defn execute-template
  "Parse, resolve, validate, and execute a SQL template.

   The template should contain SELECT, WHERE, GROUP BY, etc. — optionally
   prefixed by a `WITH … AS (…)[, …]*` CTE block. FROM and JOINs are
   generated automatically from entity references **within each scope**
   (each CTE body and the outer SELECT are independent scopes).

   Args:
     template - SQL string with {Entity.field} placeholders (no FROM needed)
     params   - Either a vector of positional values (for bare `?`
                placeholders) or a `{name value}` map (for named
                `?name` / `?name[]` placeholders)

   Returns:
     Vector of result maps"
  ([template params] (execute-template template params nil))
  ([template params opts]
   ;; Strip comments up front — every downstream step (validate,
   ;; CTE-split, resolve) scans for SQL keywords with plain regexes and
   ;; a keyword inside a comment would mislead them. The console keeps
   ;; the commented template; only the executed SQL is comment-free.
   (let [template          (strip-sql-comments template)
         _                 (validate-select-only! template)
         ;; Compile named `?name` placeholders down to positional `?`
         ;; *before* CTE-splitting — every downstream step (per-scope
         ;; `?`-counting, JDBC binding) only understands positional `?`.
         ;; A template with no `?name` passes through unchanged.
         {rw-sql :sql rw-params :params rw-errors :errors}
         (sql-params/rewrite template params)
         _ (when (seq rw-errors)
             (throw (ex-info (str "Parameter error: " (str/join "; " rw-errors))
                             {:code "TEMPLATE_PARAM_ERROR" :errors rw-errors})))
         ;; SQLite has no `::type` cast operator. The named-param rewriter
         ;; preserves user-written `?name::type` casts as `?::type` so PG
         ;; consumers get the cast applied at bind time; on SQLite the
         ;; surviving `::type` would be a parse error. Strip the cast — SQLite
         ;; is dynamically typed, the cast is informational only.
         rw-sql            (if (= "synthigy.db.SQLite" (.getName (class *db*)))
                             (str/replace rw-sql #"::\w+" "")
                             rw-sql)
         template          rw-sql
         cached?           (get opts :cached true)
         [ctes outer-body] (parse-cte-scopes template)
         user-params       (vec rw-params)
         scopes            (-> (mapv #(assoc % :outer? false) ctes)
                               (conj {:outer? true :body outer-body}))
         ;; Slice user-params across scopes by `?` count, preserving order.
         [scopes _]
         (reduce (fn [[acc remaining] scope]
                   (let [n (count-question-marks (:body scope))
                         [taken left] (split-at n remaining)]
                     [(conj acc (assoc scope :user-params (vec taken)))
                      (vec left)]))
                 [[] user-params]
                 scopes)
         ;; Resolve + inject RLS per scope.
         resolved-scopes
         (mapv (fn [{:keys [body user-params] :as scope}]
                 (let [{:keys [sql entities aliases] :as r}
                       (resolve-scope-cached body cached?)]
                   (throw-on-errors! r)
                   (let [[final-sql final-params]
                         (inject-rls sql user-params entities aliases)]
                     (assoc scope
                            :sql final-sql
                            :params final-params
                            :entities entities))))
               scopes)
         ;; Reassemble: WITH name1 AS ( sql1 ), name2 AS ( sql2 ) outer-sql
         outer-scope    (last resolved-scopes)
         cte-scopes     (butlast resolved-scopes)
         final-sql      (if (empty? cte-scopes)
                          (:sql outer-scope)
                          (str "WITH "
                               (str/join ",\n"
                                         (map (fn [{:keys [name sql]}]
                                                (str name " AS (\n" sql "\n)"))
                                              cte-scopes))
                               "\n"
                               (:sql outer-scope)))
         final-params   (vec (mapcat :params resolved-scopes))
         all-entities   (reduce into #{} (map :entities resolved-scopes))]
     (log/debug {:id ::resolved-sql-template
                 :data {:sql final-sql
                        :param-count (count final-params)
                        :entities all-entities
                        :scopes (count resolved-scopes)}}
                "Resolved SQL template")
     (log/trace {:id ::resolved-sql-template-params
                 :data {:params final-params}}
                "Resolved SQL template params")
     (sql/execute!
       (:datasource *db*)
       (into [final-sql] final-params)
       :edn))))
