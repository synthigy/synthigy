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

(ns synthigy.dataset.sql.template
  "SQL template resolver for analytics queries — standard SQL with ERD-aware {…}
   placeholders; FROM and JOINs are generated from the deployed schema."
  (:require
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.dataset.rls :as rls]
   [synthigy.dataset.sql.protocol :as proto]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.xsql.sql-params :as sql-params]
   [next.jdbc :as jdbc]
   [synthigy.db :as db :refer [*db*]]
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

(defn split-on-joins
  "Split a placeholder string into {:root entity :chain [{:label rel :join type}
   ...]}."
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

(defn outside-parens?
  "True when position idx in sql sits at paren depth 0, outside quoted regions and `{…}` placeholders."
  [^String sql idx]
  (loop [i 0 depth 0 quote nil]
    (if (>= i idx)
      (and (zero? depth) (nil? quote))
      (let [c (.charAt sql i)]
        (cond
          quote                  (recur (inc i) depth (when (not= c quote) quote))
          (= c \()               (recur (inc i) (inc depth) nil)
          (= c \))               (recur (inc i) (dec depth) nil)
          (or (= c \') (= c \")) (recur (inc i) depth c)
          (= c \{)               (recur (inc i) depth \})
          :else                  (recur (inc i) depth nil))))))

(defn top-level-match
  "Start index of the first match of re at paren depth 0 outside quoted strings,
   or nil — clause keywords inside parens/quotes are not clause boundaries."
  [re ^String sql]
  (let [m (re-matcher re sql)]
    (loop []
      (when (.find m)
        (if (outside-parens? sql (.start m))
          (.start m)
          (recur))))))

(def ^:private ident-pattern
  "Strict snake_case, matching the XSQL surface."
  #"[a-z_][a-z0-9_]*")

(defn assert-ident!
  "Throw INVALID_TEMPLATE_IDENTIFIER unless s is strict snake_case."
  [s placeholder]
  (when-not (re-matches ident-pattern s)
    (let [hint (-> s
                   (str/replace #"([a-z0-9])([A-Z])" "$1_$2")
                   str/lower-case
                   (str/replace #"[\s\-.]+" "_"))]
      (throw (ex-info (str "Invalid identifier \"" s "\" in {" placeholder "}"
                           " — identifiers are snake_case; use \"" hint "\"")
                      {:code "INVALID_TEMPLATE_IDENTIFIER"
                       :identifier s
                       :placeholder placeholder
                       :hint hint}))))
  s)

(defn parse-placeholder
  "Parse a single placeholder string (without braces) into a structured form."
  [s]
  (let [s (str/trim s)
        {:keys [root chain]} (split-on-joins s)]
    (if (empty? chain)
      (let [[entity field] (str/split root #"\." 2)]
        (assert-ident! entity s)
        (if field
          {:type :field :entity entity :field (assert-ident! field s)}
          {:type :entity :entity entity}))
      (let [entity (assert-ident! root s)
            last-item (last chain)
            [last-rel field] (str/split (:label last-item) #"\." 2)
            joins (conj (vec (butlast chain))
                        (assoc last-item :label last-rel))]
        (run! #(assert-ident! (:label %) s) joins)
        (when field (assert-ident! field s))
        (if field
          {:type :relation-field :entity entity :path (mapv :label joins)
           :field field :joins joins}
          {:type :relation :entity entity :path (mapv :label joins)
           :joins joins})))))

(defn extract-placeholders
  "Extract all {placeholder} occurrences with parsed forms and positions."
  [template]
  (let [matcher (re-matcher #"\{([^}]+)\}" template)]
    (loop [results []]
      (if (.find matcher)
        (recur (conj results
                     {:raw (.group matcher 0)
                      :inner (.group matcher 1)
                      :parsed (try
                                (parse-placeholder (.group matcher 1))
                                (catch clojure.lang.ExceptionInfo e
                                  {:type :invalid :error (ex-message e)}))
                      :start (.start matcher)
                      :end (.end matcher)}))
        results))))

;;; ============================================================================
;;; Entity & Schema Resolution
;;; ============================================================================

(defn resolve-relation
  "Resolve a relation label to a traversal spec (:junction, :fk-self, or
   :fk-ref)."
  [entity-id relation-label]
  (let [schema (sql-query/deployed-schema-entity entity-id)
        label-kw (keyword relation-label)
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

(defn get-alias
  "Get or create an alias for a key, returning [alias updated-state]."
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

(defn entity-ref
  "An entity's table reference, tagged so inject-rls can swap in its guarded view; the tag is an SQL comment, inert when no guard applies."
  [table alias]
  (format "/*rls:%s*/\"%s\" %s" alias table alias))

(defn walk-relation-chain
  "Walk a relation chain collecting JOINs, returning [final-alias
   updated-state]."
  [entity-id entity-alias joins-spec state]
  (reduce
    (fn [[current-id current-alias state] {:keys [label join]}]
      (let [join-type (or join "INNER")
            rel (resolve-relation current-id label)
            target-id (:to rel)
            {:keys [table]} (sql-query/deployed-schema-entity target-id)
            [target-alias state] (get-alias state [:relation current-id label])
            join-key [current-id label]
            state (cond-> (update state :entities conj target-id)
                    (:relation rel)
                    (update :relations conj (select-keys rel [:relation :from :to])))]
        (if (contains? (:join-keys-seen state) join-key)
          [target-id target-alias state]
          (case (:kind rel)
            :junction
            (let [[link-alias state] (get-alias state [:link current-id label])
                  state (-> state
                            (update :join-keys-seen conj join-key)
                            (update :refs conj
                                    {:alias target-alias :entity target-id}
                                    {:alias link-alias :entity target-id
                                     :join (if (= "LEFT" join-type) "INNER" join-type)
                                     :link (:table rel) :to-field (:to/field rel)
                                     :from-entity current-id :from-field (:from/field rel)})
                            (update :joins conj
                                    (if (= "LEFT" join-type)
                                      (format "LEFT JOIN (%s INNER JOIN %s ON %s.%s=%s._eid) ON %s._eid=%s.%s"
                                              (entity-ref (:table rel) link-alias)
                                              (entity-ref table target-alias)
                                              link-alias (:to/field rel) target-alias
                                              current-alias link-alias (:from/field rel))
                                      (str (format "%s JOIN %s ON %s._eid=%s.%s"
                                                   join-type (entity-ref (:table rel) link-alias)
                                                   current-alias link-alias (:from/field rel))
                                           "\n"
                                           (format "%s JOIN %s ON %s.%s=%s._eid"
                                                   join-type (entity-ref table target-alias)
                                                   link-alias (:to/field rel) target-alias)))))]
              [target-id target-alias state])

            (:fk-self :fk-ref)
            (let [state (-> state
                            (update :join-keys-seen conj join-key)
                            (update :refs conj {:alias target-alias :entity target-id})
                            (update :joins conj
                                    (format "%s JOIN %s ON %s._eid=%s.%s"
                                            join-type (entity-ref table target-alias)
                                            target-alias current-alias (:fk-column rel))))]
              [target-id target-alias state])))))
    [entity-id entity-alias state]
    joins-spec))

;;; ============================================================================
;;; Template Resolution (pure reduce)
;;; ============================================================================

(defn field-type
  [entity-id field]
  (let [fkey (keyword field)]
    (some (fn [[_ f]] (when (= (:key f) fkey) (:type f)))
          (:fields (sql-query/deployed-schema-entity entity-id)))))

(def ^:private select-end-pattern
  #"(?i)\b(?:FROM|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION)\b")

(defn select-item-label
  "The result column a placeholder at [start end) names when it is a whole select item — `{e.f}` or `{e.f} AS x` — else nil."
  [^String template select-end start end field]
  (when (and (< start select-end) (outside-parens? template start))
    (let [before (str/trimr (subs template 0 start))
          after  (subs template end select-end)]
      (when (or (str/ends-with? before ",")
                (re-find #"(?i)\b(?:select|distinct)$" before))
        (when-let [[_ alias] (re-find #"(?is)\A\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*)\s*)?(?:,|\z)" after)]
          (or alias field))))))

(defn record-column
  [state label type]
  (if-not (and label type)
    state
    (update state :columns
            (fn [cols]
              (let [prev (get cols label ::none)]
                (assoc cols label (if (#{::none type} prev) type ::ambiguous)))))))

(def ^:private placeholder-or-literal
  "`{…}` placeholders and single-quoted literals are never table references."
  #"\{[^}]*\}|'(?:[^']|'')*'")

(defn unguarded-entity-tables
  "Deployed entity tables named directly in FROM/JOIN instead of through a
   {entity} placeholder — a bare table name would silently opt the scope out of
   RLS."
  [template]
  ;; ponytail: a CTE aliased to an entity table name false-positives — track
  ;; WITH-bound names if that ever bites
  (let [tables (into #{}
                     (comp (keep :table) (map str/lower-case))
                     (vals (sql-query/deployed-schema)))
        ;; substituted, not deleted — blanking {movie} would leave "FROM JOIN"
        ;; and mis-capture the table name
        stripped (str/replace template placeholder-or-literal " __ph__ ")]
    (into #{}
          (comp (map second)
                (filter #(contains? tables (str/lower-case %))))
          (re-seq #"(?i)\b(?:FROM|JOIN)\s+\"?(\w+)\"?" stripped))))

(defn enforce-rbac!
  "Apply search's entity/relation RBAC checks; must run per execution, never inside the memoized resolve."
  [{:keys [entities relations]}]
  (run! #(sql-query/entity-accessible? % #{:read :owns}) entities)
  (run! (fn [{:keys [relation from to]}]
          (sql-query/relation-accessible? relation [from to] #{:read}))
        relations))

(defn resolve-template
  "Resolve a SQL template to {:sql :entities :aliases :relations},
   auto-generating FROM and JOINs — pure, no access checks."
  [template]
  (let [type-order {:relation 0 :relation-field 1 :entity 2 :field 3}
        placeholders (sort-by #(get type-order (get-in % [:parsed :type]) 9)
                              (extract-placeholders template))
        init-state {:aliases {}
                    :counter 0
                    :entities #{}
                    :relations #{}
                    :joins []
                    :columns {}
                    :join-keys-seen #{}
                    :refs []
                    :root-entities {}
                    :replacements []
                    :errors (mapv (fn [t]
                                    {:raw t
                                     :position 0
                                     :error (str "Table \"" t "\" is a deployed entity but is "
                                                 "referenced directly. Write it as a {entity} "
                                                 "placeholder — a bare table name bypasses "
                                                 "row-level security.")})
                                  (sort (unguarded-entity-tables template)))}

        select-end (or (top-level-match select-end-pattern template) (count template))
        final-state
        (reduce
          (fn [state {:keys [raw parsed start end]}]
            (try
              (let [resolve-root
                    (fn [state entity-name]
                      (let [id (sql-query/resolve-entity entity-name)
                            [alias state] (get-alias state [:entity id])]
                        [id alias (cond-> (-> state
                                              (update :entities conj id)
                                              (update :root-entities assoc id alias))
                                    (not (get-in state [:root-entities id]))
                                    (update :refs conj {:alias alias :entity id}))]))]
                (case (:type parsed)
                  :invalid
                  (update state :errors conj
                          {:raw raw :error (:error parsed) :position start})

                  :entity
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        {:keys [table]} (sql-query/deployed-schema-entity id)]
                    (update state :replacements conj
                            {:raw raw :replacement (entity-ref table alias)}))

                  :field
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        field (:field parsed)]
                    (-> state
                        (update :replacements conj
                                {:raw raw :replacement (format "%s.\"%s\"" alias field)})
                        (record-column (select-item-label template select-end start end field)
                                       (field-type id field))))

                  :relation
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        joins-spec (or (:joins parsed)
                                       (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                        [target-id target-alias state] (walk-relation-chain id alias joins-spec state)
                        {:keys [table]} (sql-query/deployed-schema-entity target-id)]
                    (update state :replacements conj
                            {:raw raw :replacement (entity-ref table target-alias)}))

                  :relation-field
                  (let [[id alias state] (resolve-root state (:entity parsed))
                        joins-spec (or (:joins parsed)
                                       (mapv #(hash-map :label % :join "INNER") (:path parsed)))
                        [target-id target-alias state] (walk-relation-chain id alias joins-spec state)
                        field (:field parsed)]
                    (-> state
                        (update :replacements conj
                                {:raw raw :replacement (format "%s.\"%s\"" target-alias field)})
                        (record-column (select-item-label template select-end start end field)
                                       (field-type target-id field))))))
              (catch Exception e
                (update state :errors conj
                        {:raw raw :error (ex-message e) :position start}))))
          init-state
          placeholders)]

    (if (not-empty (:errors final-state))
      {:errors (:errors final-state)}
      (let [{:keys [replacements root-entities joins]} final-state
            resolved-sql (reduce
                           (fn [sql {:keys [raw replacement]}]
                             (str/replace-first sql raw replacement))
                           template
                           replacements)
            from-parts (mapv
                         (fn [[id alias]]
                           (let [{:keys [table]} (sql-query/deployed-schema-entity id)]
                             (entity-ref table alias)))
                         root-entities)
            from-clause (str "FROM " (str/join ", " from-parts))
            has-from? (top-level-match #"(?i)\bFROM\b" resolved-sql)
            join-str (when (not-empty joins) (str/join "\n" joins))
            inject-str (if has-from? join-str
                           (str from-clause
                                (when join-str (str "\n" join-str))))

            final-sql
            (if-not inject-str
              resolved-sql
              (let [idx (top-level-match
                         #"(?i)\b(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION)\b"
                         resolved-sql)]
                (if (and idx (pos? idx))
                  (str (str/trimr (subs resolved-sql 0 idx))
                       "\n" inject-str "\n"
                       (subs resolved-sql idx))
                  (str resolved-sql "\n" inject-str))))]

        {:sql final-sql
         :entities (:entities final-state)
         :relations (:relations final-state)
         :aliases (:aliases final-state)
         :refs (:refs final-state)
         :rls-shape 2
         :columns (into {} (remove (comp #{::ambiguous} val)) (:columns final-state))}))))

;;; ============================================================================
;;; Validation
;;; ============================================================================

(defn strip-leading-sql-comments
  "Drop leading whitespace and SQL comments, returning the remainder from the
   first substantive token."
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
  "Remove all SQL comments, leaving string literals intact."
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
  "Throw unless the template is a SELECT statement (optionally fronted by WITH)."
  [template]
  (let [trimmed (str/upper-case (strip-leading-sql-comments template))]
    (when-not (or (str/starts-with? trimmed "SELECT")
                  (str/starts-with? trimmed "WITH "))
      (throw (ex-info "Only SELECT queries are allowed"
                      {:code "QUERY_NOT_SELECT"})))))

(defn select-body? [body]
  (let [b (str/upper-case (str/triml body))]
    (some #(str/starts-with? b %) ["SELECT" "VALUES" "("])))

(defn assert-select-scopes!
  "Throw unless every CTE body and the outer statement is a SELECT."
  [ctes outer-body]
  (when-not (every? select-body? (conj (mapv :body ctes) outer-body))
    (throw (ex-info "Only SELECT queries are allowed"
                    {:code "QUERY_NOT_SELECT"}))))

(defn execute-read-only!
  "Runs `sqlvec` in a read-only transaction, so no statement it carries can write."
  [sqlvec]
  (let [{:keys [on off]} (db/read-only-statements *db*)]
    (jdbc/with-transaction [tx (:datasource *db*)]
      (sql/execute! tx [on])
      (try
        (sql/execute! tx sqlvec :edn)
        (finally
          (when off (sql/execute! tx [off])))))))

;;; ============================================================================
;;; CTE Scope Parsing
;;; ============================================================================

(defn find-matching-close
  "Return the index of the ')' matching the '(' at open-idx; throws on
   unbalanced parens."
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

(defn skip-ws [^String s i]
  (loop [i i]
    (if (and (< i (.length s)) (Character/isWhitespace (.charAt s i)))
      (recur (inc i))
      i)))

(defn read-ident
  "Read a SQL identifier (optionally double-quoted) starting at i, returning
   [identifier next-index]."
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
  "Peel off a leading WITH clause, returning [ctes outer-sql]; [[] template]
   when absent."
  [^String template]
  (let [t       (str/triml (strip-leading-sql-comments template))
        upper-t (str/upper-case t)]
    (if-not (or (str/starts-with? upper-t "WITH ")
                (str/starts-with? upper-t "WITH\t")
                (str/starts-with? upper-t "WITH\n"))
      [[] t]
      (let [after-with (skip-ws t 4)
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

(defn scan-sql
  "Indexes of every character of s that is live SQL — outside '…' literals and \"…\" identifiers (doubled quotes escape)."
  [^String s]
  (loop [i 0 quote nil acc (transient [])]
    (if (>= i (.length s))
      (persistent! acc)
      (let [c (.charAt s i)]
        (cond
          quote (if (= c quote)
                  (if (and (< (inc i) (.length s)) (= quote (.charAt s (inc i))))
                    (recur (+ i 2) quote acc)
                    (recur (inc i) nil acc))
                  (recur (inc i) quote acc))
          (or (= c \') (= c \")) (recur (inc i) c acc)
          :else (recur (inc i) nil (conj! acc i)))))))

(defn assert-plain-literals!
  "Reject PG dollar-quoted and E'' strings — scan-sql cannot see their ends, so ?-counting and guard placement would disagree with the driver."
  [^String s]
  (doseq [i (scan-sql s)
          :let [c (.charAt s i)
                prev (when (pos? i) (.charAt s (dec i)))]]
    (when (or (and (= c \$) (re-find #"^\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$" (subs s i)))
              (and (#{\E \e} c)
                   (< (inc i) (.length s)) (= \' (.charAt s (inc i)))
                   (not (and prev (or (Character/isLetterOrDigit prev) (= prev \_))))))
      (throw (ex-info "Dollar-quoted and E'' strings are not supported in templates; use a plain '...' literal."
                      {:code "TEMPLATE_ERROR" :position i})))))

(defn count-question-marks
  "Count ? parameter placeholders in s, ignoring ? inside literals and quoted identifiers."
  [^String s]
  (count (filter #(= \? (.charAt s %)) (scan-sql s))))

(defn live-indexes-of
  "Indexes where needle starts as live SQL (not inside a literal or quoted identifier)."
  [^String s ^String needle]
  (let [live (set (scan-sql s))]
    (loop [from 0 acc []]
      (let [i (.indexOf s needle (int from))]
        (if (neg? i)
          acc
          (recur (inc i) (if (live i) (conj acc i) acc)))))))

;;; ============================================================================
;;; RLS Injection
;;; ============================================================================

(defn read-guard
  "[sql params] of entity's compiled :read guard against alias, or nil when no guard applies."
  [entity-id alias]
  (let [{{:keys [enabled guards]} :rls} (sql-query/deployed-schema-entity entity-id)]
    (when (and enabled (rls/should-apply-guards? entity-id :read))
      (let [{rls-sql :sql rls-params :params} (rls/compile-guards-to-sql alias guards :read)]
        (if rls-sql [rls-sql rls-params] ["1=0" []])))))

(defn endpoint-visible
  "[sql params] EXISTS-ing that the row a junction column points at is readable, or nil when that entity has no guard."
  [entity-id link-alias field t]
  (when-let [[g gp] (read-guard entity-id t)]
    (let [{:keys [table]} (sql-query/deployed-schema-entity entity-id)]
      [(format "EXISTS (SELECT 1 FROM \"%s\" %s WHERE %s._eid = %s.%s AND %s)"
               table t t link-alias field g)
       gp])))

(def link-ends
  "Which junction ends need their own check, by join type: INNER (also the inside of a LEFT junction unit) already pairs links with readable rows on both sides, RIGHT leaves the source side unpaired, FULL both."
  {"INNER" #{}
   "RIGHT" #{:source}
   "FULL OUTER" #{:source :target}})

(defn guarded-view
  "[sql params] replacing one tagged reference: the entity's readable rows, or for a junction only the links whose unjoined ends are readable."
  [{:keys [alias entity link to-field from-entity from-field join]}]
  (if link
    (let [checked (get link-ends join #{:source :target})
          ends (keep identity [(when (checked :source)
                                 (endpoint-visible from-entity alias from-field (str alias "s")))
                               (when (checked :target)
                                 (endpoint-visible entity alias to-field (str alias "t")))])]
      (when (seq ends)
        [(format "(SELECT %s.* FROM \"%s\" %s WHERE %s) %s"
                 alias link alias (str/join " AND " (map first ends)) alias)
         (vec (mapcat second ends))]))
    (when-let [[g gp] (read-guard entity alias)]
      (let [{:keys [table]} (sql-query/deployed-schema-entity entity)]
        [(format "(SELECT %s.* FROM \"%s\" %s WHERE %s) %s" alias table alias g alias) gp]))))

(defn guard-ref
  "Swap every live occurrence of ref's tag for its guarded view; a guard with nowhere to go fails closed."
  [[sql params] {:keys [alias entity link] :as ref}]
  (if-let [[view view-params] (guarded-view ref)]
    (let [tag (entity-ref (or link (:table (sql-query/deployed-schema-entity entity))) alias)
          ats (live-indexes-of sql tag)]
      (when (empty? ats)
        (throw (ex-info "RLS guard has no table reference to attach to"
                        {:code "TEMPLATE_RLS_UNPLACED" :alias alias})))
      (reduce (fn [[sql params] at]
                (let [n (count-question-marks (subs sql 0 at))]
                  [(str (subs sql 0 at) view (subs sql (+ at (count tag))))
                   (-> [] (into (take n params)) (into view-params) (into (drop n params)))]))
              [sql params]
              (reverse ats)))
    [sql params]))

(defn inject-rls
  "Replace every entity table reference in one scope with the rows the principal may read — call once per CTE body/outer SELECT."
  [sql params {:keys [refs rls-shape]}]
  (cond
    (not (rls/should-apply-guards?)) [sql (vec params)]
    (not= 2 rls-shape) (throw (ex-info "Stale template resolution" {:code "TEMPLATE_RLS_STALE"}))
    :else (reduce guard-ref [sql (vec params)] refs)))

;;; ============================================================================
;;; Execution
;;; ============================================================================

(defn resolve-scope-cached
  "Resolve one scope's body (via the template cache when cached?) and run RBAC
   per call — never behind the memo, which is keyed by text alone."
  [body cached?]
  (let [resolved (if cached?
                   (or (let [c (sql-query/cached-template body)] (when (= 2 (:rls-shape c)) c))
                       (let [resolved (resolve-template body)]
                         (when-not (:errors resolved)
                           (sql-query/cache-template body resolved))
                         resolved))
                   (resolve-template body))]
    (when-not (:errors resolved)
      (enforce-rbac! resolved))
    resolved))

(defn throw-on-errors! [{:keys [errors]}]
  (when errors
    (throw (ex-info (str "Template resolution failed: "
                         (str/join "; "
                                   (map (fn [{:keys [error position]}]
                                          (if position
                                            (format "%s (at position %d)" error position)
                                            error))
                                        errors)))
                    {:code "TEMPLATE_ERROR" :errors errors}))))

(def decoded-types
  #{"boolean" "timestamp" "json"})

(defn decode-rows
  "Decode result columns that are whole field select items through the dialect's TypeCodec, as every model read is."
  [rows columns]
  (let [label  (sql/get-label-fn :edn)
        codecs (into {}
                     (keep (fn [[c t]] (when (decoded-types t) [(keyword (label c)) t])))
                     columns)]
    (if (empty? codecs)
      rows
      (mapv (fn [row]
              (reduce-kv (fn [r k t]
                           (if (contains? r k) (update r k #(proto/decode *db* t %)) r))
                         row codecs))
            rows))))

(defn execute-template
  "Parse, resolve, validate, and execute a SQL template with positional or named
   params."
  ([template params] (execute-template template params nil))
  ([template params opts]
   (let [template          (strip-sql-comments template)
         _                 (validate-select-only! template)
         _                 (assert-plain-literals! template)
         {rw-sql :sql rw-params :params rw-types :types rw-errors :errors}
         (sql-params/rewrite template params)
         _ (when (seq rw-errors)
             (throw (ex-info (str "Parameter error: " (str/join "; " rw-errors))
                             {:code "TEMPLATE_PARAM_ERROR" :errors rw-errors})))
         rw-sql            (db/template-sql *db* rw-sql)
         template          rw-sql
         cached?           (get opts :cached true)
         [ctes outer-body] (parse-cte-scopes template)
         _                 (assert-select-scopes! ctes outer-body)
         user-params       (if rw-types
                             (mapv (fn [v t]
                                     (if (and (= :timestamp t) (string? v))
                                       (proto/encode *db* "timestamp" v)
                                       v))
                                   rw-params rw-types)
                             (vec rw-params))
         scopes            (-> (mapv #(assoc % :outer? false) ctes)
                               (conj {:outer? true :body outer-body}))
         [scopes _]
         (reduce (fn [[acc remaining] scope]
                   (let [n (count-question-marks (:body scope))
                         [taken left] (split-at n remaining)]
                     [(conj acc (assoc scope :user-params (vec taken)))
                      (vec left)]))
                 [[] user-params]
                 scopes)
         resolved-scopes
         (mapv (fn [{:keys [body user-params] :as scope}]
                 (let [{:keys [sql entities columns] :as r}
                       (resolve-scope-cached body cached?)]
                   (throw-on-errors! r)
                   (let [[final-sql final-params]
                         (inject-rls sql user-params r)]
                     (assoc scope
                            :sql final-sql
                            :params final-params
                            :entities entities
                            :columns columns))))
               scopes)
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
     (try
       (decode-rows
        (execute-read-only! (into [final-sql] final-params))
        (:columns outer-scope))
       (catch clojure.lang.ExceptionInfo e (throw e))
       (catch Exception e
         ;; never leak a bare driver exception — the resolved SQL is the caller's only context
         (throw (ex-info (str "Template execution failed: " (ex-message e))
                         {:code "TEMPLATE_EXECUTION_ERROR"
                          :resolved-sql final-sql
                          :entities all-entities
                          :param-count (count final-params)}
                         e)))))))
