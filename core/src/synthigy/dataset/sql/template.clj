(ns synthigy.dataset.sql.template
  "SQL template resolver for analytics queries.

   Parses SQL templates with entity/relation/attribute placeholders
   and resolves them to physical SQL using the deployed schema.
   FROM clause and JOINs are generated automatically.

   ## Placeholder Syntax

   | Form | Example | Resolves to |
   |------|---------|-------------|
   | `{Entity.attr}` | `{User.name}` | `e1.name` |
   | `{Entity._eid}` | `{User._eid}` | `e1._eid` |
   | `{Entity -> rel.attr}` | `{User -> roles.name}` | `e2.name` |
   | `{Entity -> rel._eid}` | `{User -> roles._eid}` | `e2._eid` |
   | `{E -> r1 -> r2.attr}` | `{User -> groups -> projects.name}` | chained |

   FROM and JOINs are inferred — never written by the user.

   ## Usage

   ```clojure
   (execute-template
     \"SELECT {User.name}, COUNT({User -> roles._eid}) as cnt
      WHERE {User.active} = ?
      GROUP BY {User.name}\"
     [true])
   ```"
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]
))

;;; ============================================================================
;;; Placeholder Parsing
;;; ============================================================================

(def ^:private join-pattern
  "Regex matching join operators. Order matters — check multi-char before single-char.
   =>  full outer join
   ->  left join
   <-  right join
   -   inner join"
  #"\s*(=>|->|<-|-)\s*")

(def ^:private join-type-map
  {"=>" "FULL OUTER"
   "->" "LEFT"
   "<-" "RIGHT"
   "-"  "INNER"})

(defn- split-on-joins
  "Split a placeholder string on join operators.
   Returns {:root \"entity\" :chain [{:label \"rel\" :join \"INNER\"} ...]}
   The join type belongs to the segment AFTER the operator."
  [s]
  (let [matcher (re-matcher join-pattern s)]
    (loop [last-end 0
           segments []
           join-types []]
      (if (.find matcher)
        (recur (.end matcher)
               (conj segments (str/trim (subs s last-end (.start matcher))))
               (conj join-types (get join-type-map (.group matcher 1))))
        ;; Done — collect final segment
        (let [final (str/trim (subs s last-end))
              all-segments (conj segments final)]
          {:root (first all-segments)
           :chain (mapv (fn [segment join-type]
                          {:label segment :join join-type})
                        (rest all-segments)
                        join-types)})))))

(defn- parse-placeholder
  "Parse a single placeholder string (without braces) into a structured form.

   Join operators:
     -   inner join
     ->  left join
     <-  right join
     =>  full outer join

   'User.name'                     → {:type :field :entity \"User\" :field \"name\"}
   'User - roles.name'             → {:type :relation-field ... :joins [{:label \"roles\" :join \"INNER\"}]}
   'User -> roles.name'            → {:type :relation-field ... :joins [{:label \"roles\" :join \"LEFT\"}]}
   'User - roles - permissions.x'  → chained joins"
  [s]
  (let [s (str/trim s)
        {:keys [root chain]} (split-on-joins s)]
    (if (empty? chain)
      ;; No join operators — entity.field or bare entity
      (let [[entity field] (str/split root #"\." 2)]
        (if field
          {:type :field :entity entity :field field}
          {:type :entity :entity entity}))
      ;; Has join operators — relation chain
      (let [entity root
            ;; Last segment may have .field suffix
            last-item (last chain)
            last-label (:label last-item)
            [last-rel field] (str/split last-label #"\." 2)
            ;; Rebuild chain with corrected last label
            joins (conj (vec (butlast chain))
                        (assoc last-item :label last-rel))
            path (mapv :label joins)]
        (if field
          {:type :relation-field :entity entity :path path :field field
           :joins joins}
          {:type :relation :entity entity :path path
           :joins joins})))))

(defn- extract-placeholders
  "Extract all {placeholder} occurrences from template string.
   Returns vector of {:raw, :inner, :parsed, :start, :end}"
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

(defn- resolve-entity-id
  "Resolve entity name to entity ID using sql-query/resolve-entity."
  [entity-name]
  (try
    (sql-query/resolve-entity entity-name)
    (catch clojure.lang.ExceptionInfo e
      (throw (ex-info (str "Unknown entity in template: " entity-name)
                      {:code "UNKNOWN_TEMPLATE_ENTITY" :entity entity-name})))))

(defn- resolve-relation
  [entity-id relation-label]
  (let [schema (sql-query/deployed-schema-entity entity-id)
        label-kw (keyword (-> relation-label
                              str/trim
                              (str/replace #"([a-z])([A-Z])" "$1_$2")
                              str/lower-case
                              (str/replace #"[\s]+" "_")))
        rel (get-in schema [:relations label-kw])]
    (when-not rel
      (throw (ex-info (str "Unknown relation: " relation-label)
                      {:code "UNKNOWN_TEMPLATE_RELATION"
                       :entity entity-id
                       :relation relation-label})))
    rel))

;;; ============================================================================
;;; Alias Management
;;; ============================================================================

(defn- make-alias-manager []
  (let [counter (volatile! 0)
        aliases (volatile! {})]
    {:get-alias
     (fn [key]
       (or (get @aliases key)
           (let [alias (str "e" (vswap! counter inc))]
             (vswap! aliases assoc key alias)
             alias)))
     :all-aliases (fn [] @aliases)}))

;;; ============================================================================
;;; Relation Chain Walker
;;; ============================================================================

(defn- walk-relation-chain
  "Walk a relation chain from an entity, collecting JOINs.
   Each step can have a different join type (INNER, LEFT, RIGHT, FULL OUTER).
   Returns {:current-uuid :current-alias} of the final entity."
  [entity-id entity-alias joins-spec
   {:keys [get-alias entities joins join-keys-seen]}]
  (reduce
   (fn [{:keys [current-uuid current-alias]} {:keys [label join]}]
     (let [join-type (or join "INNER")
           rel (resolve-relation current-uuid label)
           target-uuid (:to rel)
           {:keys [table]} (sql-query/deployed-schema-entity target-uuid)
           target-alias (get-alias [:relation current-uuid label])
           link-alias (get-alias [:link current-uuid label])
           join-key [current-uuid label]]
       (vswap! entities conj target-uuid)
       (when-not (contains? @join-keys-seen join-key)
         (vswap! join-keys-seen conj join-key)
         (vswap! joins conj
                (format "%s JOIN \"%s\" %s ON %s._eid=%s.%s"
                        join-type
                        (:table rel) link-alias
                        current-alias link-alias
                        (:from/field rel)))
         (vswap! joins conj
                (format "%s JOIN \"%s\" %s ON %s.%s=%s._eid"
                        join-type
                        table target-alias
                        link-alias (:to/field rel)
                        target-alias)))
       {:current-uuid target-uuid
        :current-alias target-alias}))
   {:current-uuid entity-id
    :current-alias entity-alias}
   joins-spec))

;;; ============================================================================
;;; Template Resolution
;;; ============================================================================

(defn resolve-template
  "Resolve a SQL template: parse placeholders, resolve to physical names,
   auto-generate FROM + JOINs.

   The template should NOT contain FROM — it is generated automatically
   from the entities and relations referenced in the placeholders.
   If FROM is present, JOINs are injected after it (backward compatible).

   Args:
     template     - SQL string with {Entity.field} placeholders
     entity-index - Optional {normalized-name -> entity-id} map.
                    If nil, builds from deployed schema.

   Returns:
   {:sql       - Resolved SQL with physical names + auto FROM/JOINs
    :entities  - Set of entity UUIDs referenced
    :errors    - Resolution errors with position info}"
  [template]
  (let [placeholders (extract-placeholders template)
        alias-mgr (make-alias-manager)
        get-alias (:get-alias alias-mgr)
        all-aliases (:all-aliases alias-mgr)
        entities (volatile! #{})
        joins (volatile! [])
        join-keys-seen (volatile! #{})
        ;; Track root entities (referenced directly, not via relation)
        root-entities (volatile! {}) ;; {uuid -> alias}

        ctx {:get-alias get-alias
             :entities entities
             :joins joins
             :join-keys-seen join-keys-seen}

        replacements
        (mapv
         (fn [{:keys [raw parsed start]}]
           (try
             (case (:type parsed)
                ;; {User} — bare entity (backward compat, or in FROM)
               :entity
               (let [uuid (resolve-entity-id (:entity parsed))
                     {:keys [table]} (sql-query/deployed-schema-entity uuid)
                     alias (get-alias [:entity uuid])]
                 (vswap! entities conj uuid)
                 (vswap! root-entities assoc uuid alias)
                 {:raw raw :replacement (format "\"%s\" %s" table alias)})

                ;; {User.name} → e1.name
               :field
               (let [uuid (resolve-entity-id (:entity parsed))
                     {:keys [table]} (sql-query/deployed-schema-entity uuid)
                     alias (get-alias [:entity uuid])]
                 (vswap! entities conj uuid)
                 (vswap! root-entities assoc uuid alias)
                 {:raw raw :replacement (format "%s.%s" alias (:field parsed))})

                ;; {User - roles} or {User -> roles} → target alias
               :relation
               (let [uuid (resolve-entity-id (:entity parsed))
                     alias (get-alias [:entity uuid])]
                 (vswap! entities conj uuid)
                 (vswap! root-entities assoc uuid alias)
                 (let [joins-spec (or (:joins parsed)
                                      (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                       result (walk-relation-chain uuid alias joins-spec ctx)]
                   {:raw raw :replacement (:current-alias result)}))

                ;; {User - roles.name} or {User -> roles.name} → e2.name
               :relation-field
               (let [uuid (resolve-entity-id (:entity parsed))
                     alias (get-alias [:entity uuid])]
                 (vswap! entities conj uuid)
                 (vswap! root-entities assoc uuid alias)
                 (let [joins-spec (or (:joins parsed)
                                      (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                       result (walk-relation-chain uuid alias joins-spec ctx)]
                   {:raw raw :replacement (format "%s.%s"
                                                  (:current-alias result)
                                                  (:field parsed))})))
             (catch Exception e
               {:raw raw :error (ex-message e) :position start})))
         placeholders)
        errors (filterv :error replacements)]
    (if (not-empty errors)
      {:errors errors}
      (let [;; Apply replacements
            resolved-sql (reduce
                          (fn [sql {:keys [raw replacement]}]
                            (str/replace-first sql raw replacement))
                          template
                          replacements)

              ;; Build FROM clause from root entities
            from-parts (mapv
                        (fn [[uuid alias]]
                          (let [{:keys [table]} (sql-query/deployed-schema-entity uuid)]
                            (format "\"%s\" %s" table alias)))
                        @root-entities)
            from-clause (str "FROM " (str/join ", " from-parts))

              ;; Check if template already has FROM (backward compat)
            has-from? (re-find #"(?i)\bFROM\b" resolved-sql)

              ;; Build JOIN string
            join-str (when (not-empty @joins) (str/join "\n" @joins))

              ;; Build final SQL
            ;; What needs injecting
            inject-str (if has-from?
                         ;; FROM present — only inject JOINs
                         join-str
                         ;; No FROM — inject FROM + JOINs
                         (str from-clause
                              (when join-str (str "\n" join-str))))

            final-sql
            (if-not inject-str
              resolved-sql
              ;; Find WHERE/GROUP BY/HAVING/ORDER BY/LIMIT to insert before
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
         :entities @entities}))))

;;; ============================================================================
;;; Validation
;;; ============================================================================

(defn validate-select-only!
  "Ensure the template is a SELECT statement. Throws on violation."
  [template]
  (let [trimmed (str/trim template)]
    (when-not (str/starts-with? (str/upper-case trimmed) "SELECT")
      (throw (ex-info "Only SELECT queries are allowed"
                      {:code "QUERY_NOT_SELECT"})))))

;;; ============================================================================
;;; Execution
;;; ============================================================================

(defn execute-template
  "Parse, resolve, validate, and execute a SQL template.

   The template should contain SELECT, WHERE, GROUP BY, etc.
   FROM and JOINs are generated automatically from entity references.

   Args:
     template     - SQL string with {Entity.field} placeholders (no FROM needed)
     params       - Vector of query parameters (for ? placeholders)
     entity-index - Optional {normalized-name -> entity-id} map from data handler

   Returns:
     Vector of result maps"
  [template params]
  (validate-select-only! template)
  (let [{:keys [sql entities errors]} (resolve-template template)]
    (when errors
      (throw (ex-info (str "Template resolution failed: "
                           (str/join "; "
                                     (map (fn [{:keys [error position]}]
                                            (if position
                                              (format "%s (at position %d)" error position)
                                              error))
                                          errors)))
                      {:code "TEMPLATE_ERROR" :errors errors})))
    (log/debugf "[TEMPLATE] Resolved SQL:\n%s\nParams: %s\nEntities: %s"
                sql (pr-str params) (pr-str entities))
    (sql/execute!
     (:datasource *db*)
     (into [sql] (or params []))
     :edn)))
