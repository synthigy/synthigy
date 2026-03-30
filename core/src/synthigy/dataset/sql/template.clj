(ns synthigy.dataset.sql.template
  "SQL template resolver for analytics queries.

   Parses SQL templates with entity/relation/attribute placeholders
   and resolves them to physical SQL using the deployed schema.
   FROM clause and JOINs are generated automatically.

   ## Join Syntax

   | Operator | Join Type | Example |
   |----------|-----------|---------|
   | `-` | INNER | `{User - roles.name}` |
   | `->` | LEFT | `{User -> roles.name}` |
   | `<-` | RIGHT | `{User <- roles.name}` |
   | `=>` | FULL OUTER | `{User => roles.name}` |

   FROM and JOINs are inferred — never written by the user.
   Raw SQL and {placeholders} can coexist freely.

   ## Usage

   ```clojure
   (execute-template
     \"SELECT {User.name}, COUNT({User - roles._eid}) as cnt
      WHERE {User.active} = ?
      GROUP BY {User.name}\"
     [true])
   ```"
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]))

;;; ============================================================================
;;; Placeholder Parsing
;;; ============================================================================

(def ^:private join-pattern
  #"\s*(=>|->|<-|-)\s*")

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
  "Walk a relation chain, collecting JOINs. Returns [final-alias updated-state]."
  [entity-id entity-alias joins-spec state]
  (reduce
    (fn [[current-id current-alias state] {:keys [label join]}]
      (let [join-type (or join "INNER")
            rel (resolve-relation current-id label)
            target-id (:to rel)
            {:keys [table]} (sql-query/deployed-schema-entity target-id)
            [target-alias state] (get-alias state [:relation current-id label])
            [link-alias state] (get-alias state [:link current-id label])
            join-key [current-id label]
            state (update state :entities conj target-id)]
        (if (contains? (:join-keys-seen state) join-key)
          [target-id target-alias state]
          (let [state (-> state
                          (update :join-keys-seen conj join-key)
                          (update :joins conj
                                  (format "%s JOIN \"%s\" %s ON %s._eid=%s.%s"
                                          join-type (:table rel) link-alias
                                          current-alias link-alias (:from/field rel)))
                          (update :joins conj
                                  (format "%s JOIN \"%s\" %s ON %s.%s=%s._eid"
                                          join-type table target-alias
                                          link-alias (:to/field rel) target-alias)))]
            [target-id target-alias state]))))
    [entity-id entity-alias state]
    joins-spec))

;;; ============================================================================
;;; Template Resolution (pure reduce)
;;; ============================================================================

(defn resolve-template
  "Resolve a SQL template: parse placeholders, resolve to physical names,
   auto-generate FROM + JOINs. Pure function — no mutation."
  [template]
  (let [placeholders (extract-placeholders template)
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

                  ;; {User.name} → e1.name
                  :field
                  (let [[id alias state] (resolve-root state (:entity parsed))]
                    (update state :replacements conj
                            {:raw raw :replacement (format "%s.%s" alias (:field parsed))}))

                  ;; {User - roles} → target alias
                  :relation
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        joins-spec (or (:joins parsed)
                                       (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                        [_ target-alias state] (walk-relation-chain id alias joins-spec state)]
                    (update state :replacements conj
                            {:raw raw :replacement target-alias}))

                  ;; {User - roles.name} → e2.name
                  :relation-field
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        joins-spec (or (:joins parsed)
                                       (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                        [_ target-alias state] (walk-relation-chain id alias joins-spec state)]
                    (update state :replacements conj
                            {:raw raw :replacement (format "%s.%s" target-alias (:field parsed))}))))
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
         :entities (:entities final-state)}))))

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
     template - SQL string with {Entity.field} placeholders (no FROM needed)
     params   - Vector of query parameters (for ? placeholders)

   Returns:
     Vector of result maps"
  ([template params] (execute-template template params nil))
  ([template params opts]
  (validate-select-only! template)
  (let [cached? (get opts :cached true)
        {:keys [sql entities errors]}
        (if cached?
          (or (sql-query/cached-template template)
              (let [resolved (resolve-template template)]
                (when-not (:errors resolved)
                  (sql-query/cache-template template resolved))
                resolved))
          (resolve-template template))]
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
      :edn))))
