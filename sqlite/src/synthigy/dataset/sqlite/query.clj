(ns synthigy.dataset.sqlite.query
  "SQLite query implementation - ported from Postgres with SQL syntax adaptations.

  This namespace contains the full query implementation including:
  - Relation pulling (one-to-many, many-to-one, junction tables)
  - Nested relations (recursive pulling)
  - Tree operations (recursive CTEs)
  - Aggregations
  - All complex query logic

  SQL syntax differences are handled via the SQLDialect protocol."
  (:require
   clojure.set
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.prepare :as p]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.substrate.sqlite :as substrate]
   synthigy.dataset.sql.naming
   [synthigy.dataset.sql.query :as sql-query
    :refer [*operation-rules*
            entity-accessible?
            construct-response
            focus-order
            selection->schema
            schema->aggregate-cursors
            shave-schema-arguments
            shave-schema-relations
            shave-schema-aggregates
            pprint
            j-and
            pull-cursors
            pull-roots
            modifiers-selection->sql
            search-stack-args
            search-stack-from
            inner-exists]]
   [synthigy.db :refer [*db*] :as db]
   [synthigy.db.sql :as sql]
   [synthigy.log :as log])
  (:import
   [java.sql PreparedStatement]
   [synthigy.db SQLite]))

(extend-protocol p/SettableParameter
  ;; Java Time type conversion:
  clojure.lang.Keyword
  (set-parameter [^clojure.lang.Keyword v ^PreparedStatement s ^long i]
    (.setString s i (name v))))

; (def known-errors
;   {"23502" :null_constraint
;    "23505" :unique_violation})

(defn- write-entity
  "SQLite write path for the sync/stack protocol methods. Opens a fresh
   transaction, binds the tx-local audit GUCs, runs the shared
   `sql-query/set-entity`, then wakes the audit drainer — SQLite has no
   LISTEN/NOTIFY, so the drainer must be pinged explicitly after commit.
   `*fetch-mode*` is forced :serial: the SQLite pool is size 1, so a
   parallel pull-cursors fan-out would self-deadlock. `stack?` picks
   sync (false) vs stack (true)."
  [entity-id data stack?]
  (binding [sql-query/*fetch-mode* :serial]
    (let [r (with-open [connection (jdbc/get-connection (:datasource *db*))]
              (jdbc/with-transaction [tx connection]
                (substrate/set-context! *db* tx)
                (sql-query/set-entity tx (id/entity entity-id) data stack?)))]
      (substrate/wake-drainer!)
      r)))

(defn- distinct-attributes->group-by
  "Converts _distinct {:attributes [...]} to GROUP BY clause for SQLite.
   PostgreSQL uses DISTINCT ON, but SQLite doesn't support it.
   We use GROUP BY on the specified attributes instead."
  [schema]
  (when-let [attrs (get-in schema [:args :_distinct :attributes])]
    (let [{:keys [entity/as]} schema]
      (str "GROUP BY "
           (str/join ", " (map #(str (name as) "." (name %)) attrs))))))

(defn search-entity-roots
  "Find root-entity `_eid`s matching the schema's filters.

  Roots are filtered by the root entity's own predicates plus a correlated
  `EXISTS (...)` per INNER relation (`inner-exists`) — never by JOINing
  through relation tables. A JOIN multiplies root rows (one per matching
  child), so `_limit` would slice the *joined* set and under-count roots;
  `EXISTS` keeps one row per root, so `_limit`/`_order_by` are exact."
  ([schema]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (search-entity-roots connection schema)))
  ([connection schema]
   (let [focused-schema (focus-order schema)
         root-as  (:entity/as focused-schema)
         root-tbl (:entity/table focused-schema)
         ;; Root's OWN field predicates only — dissoc :relations so
         ;; search-stack-args never emits predicates against relation
         ;; aliases that the EXISTS rewrite does not join into FROM.
         [where where-data] (search-stack-args (dissoc focused-schema :relations))
         ;; INNER relations scope the root via correlated EXISTS.
         [exists-sql exists-data] (inner-exists focused-schema root-as)
         where-all (clojure.string/join
                    " and " (remove clojure.string/blank? [where exists-sql]))
         ;; SQLite has no DISTINCT ON — plain DISTINCT for attribute-less
         ;; _distinct, GROUP BY for attribute-scoped _distinct. The old
         ;; JOIN-dedup DISTINCT-on-_limit hotfix is gone: EXISTS does not
         ;; multiply root rows.
         distinct-attrs  (get-in schema [:args :_distinct :attributes])
         distinct-on     (when (and (not distinct-attrs)
                                    (get-in schema [:args :_distinct]))
                           "DISTINCT")
         group-by-clause (distinct-attributes->group-by schema)
         modifiers (modifiers-selection->sql schema)
         query (as-> (format "select %s%s._eid as %s from \"%s\" %s"
                             (if distinct-on (str distinct-on " ") "")
                             root-as root-as root-tbl root-as)
                     query
                 (if (not-empty where-all) (str query \newline "where " where-all) query)
                 (if group-by-clause (str query \newline group-by-clause) query)
                 (if modifiers (str query " " modifiers) query))
         data (vec (concat where-data exists-data))
         [r :as ids] (if (and (empty? where-all)
                              (nil? distinct-on)
                              (nil? group-by-clause)
                              (not (some #{:_limit :_offset :_order_by}
                                         (keys (:args schema)))))
                       ;; Whole-table query — no filter / paging / ordering.
                       ;; nil signals the caller to pull the whole table.
                       nil
                       (do
                         (log/trace {:id :synthigy.dataset.sql/roots-query
                                     :data {:query query :data (pprint data)}}
                                    "Query for roots")
                         (sql/execute!
                          connection (into [query] data)
                          core/*return-type*)))]
     (if (not-empty r)
       (reduce
        (fn [ids' k]
          (assoc ids' k (distinct (map #(get % k) ids))))
        nil
        (keys r))
       (if (nil? ids) {} nil)))))

(defn search-entity
  ([entity-id args selection]
   (search-entity entity-id args selection #{:search :read}))
  ([entity-id args selection operations]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (search-entity connection entity-id args selection operations)))
  ([connection entity-id args selection operations]
   ; (def entity-id entity-id)
   ; (def args args)
   ; (def selection selection)
   ; (def operations operations)
   (entity-accessible? entity-id operations)
   #_(comment
       (def con connection)
       (def found-records {})
       (def connection (jdbc/get-connection (:datasource *db*))))
   (binding [*operation-rules* operations]
     (let [schema (selection->schema entity-id selection args)
           _ (log/trace {:id :synthigy.dataset.sql/searching-entity :data {:schema schema}}
                        "Searching for entity")
           roots (search-entity-roots connection schema)]
       (when (some? roots)
         (log/trace {:id :synthigy.dataset.sql/roots-found
                     :data {:entity-id entity-id :roots (str/join ", " roots)}}
                    "Found roots")
         (pull-roots connection schema roots))))))

(defn purge-entity
  ;; See PG purge-entity for the rationale on dropping the previous
  ;; `enforce-purge` gate (2026-06-02). RLS via search-entity-roots is
  ;; the per-row protection; we don't add a second schema-equivalence
  ;; check on top.
  ([entity-id args selection]
   (let [r (with-open [connection (jdbc/get-connection (:datasource *db*))]
             (jdbc/with-transaction [tx connection]
               (substrate/set-context! *db* tx)
               (binding [*operation-rules* #{:purge :delete}]
                 (let [schema (selection->schema entity-id selection args)
                       roots (search-entity-roots connection schema)]
                   (if (some? roots)
                     (letfn [(construct-statement
                               [table _eids]
                               (log/debug {:id :synthigy.dataset.sql/constructing-purge
                                           :data {:table table :count (count _eids)
                                                  :eids (str/join ", " _eids)}}
                                          "Constructing purge for eids")
                               ;; SQLite: IN clause (no =any(?))
                               (let [placeholders (str/join "," (repeat (count _eids) "?"))]
                                 (into [(str "delete from \"" table "\" where _eid IN (" placeholders ")")] _eids)))
                             (process-statement [r k v]
                               (conj r (construct-statement k (keys v))))]
                       (let [db (pull-cursors connection schema roots)
                             response (construct-response schema db roots)
                             delete-statements (reduce-kv process-statement [] db)]
                         (doseq [query delete-statements]
                           (log/debug {:id :synthigy.dataset.sql/purging-rows
                                       :data {:entity-id entity-id :query query}}
                                      "Purging entity rows")
                           (sql/execute! connection query core/*return-type*))
                         response))
                     [])))))]
     (substrate/wake-drainer!)
     r)))

(defn get-entity
  "See PG get-entity for the IDENTITY_REQUIRED rationale (2026-06-03)."
  ([entity-id args selection]
   (get-entity entity-id args selection #{:read :get}))
  ([entity-id args selection operations]
   (when (empty? args)
     (throw (ex-info
             "get-entity requires identity. Provide at least one args predicate (e.g. {:xid \"…\"}); empty args is not supported."
             {:type ::identity-required
              :code "IDENTITY_REQUIRED"
              :entity entity-id})))
   (entity-accessible? entity-id operations)
   (log/debug {:id :synthigy.dataset.sql/getting-entity
               :data {:entity-id entity-id
                      :args (pprint args)
                      :selection (pprint selection)}}
              "Getting entity")
   (let [args (reduce-kv
               (fn [args k v]
                 (assoc args k {:_eq v}))
               nil
               args)]
     (with-open [connection (jdbc/get-connection (:datasource *db*))]
       (binding [*operation-rules* operations]
         (let [schema (selection->schema entity-id selection args)
               roots (search-entity-roots connection schema)]
           (when (not-empty roots)
             (let [roots' (pull-roots connection schema roots)
                   response (first roots')]
               (log/trace {:id :synthigy.dataset.sql/response-returned
                           :data {:entity-id entity-id
                                  :response (pprint response)}}
                          "Returning response")
               response))))))))

(defn get-entity-tree
  [entity-id root on selection]
  (entity-accessible? entity-id #{:read})
  (let [{:keys [entity/table entity/as]
         :as schema} (binding [*operation-rules* #{:read}]
                       (selection->schema entity-id selection))
        on' (name on)
        id-field (id/field)
        sql (if (some? root)
              (format
               "with recursive tree as (
                select
                _eid, %s, %s
                from %s where %s='%s'
                union
                select o._eid, o.%s, o.%s
                from %s o
                inner join tree t on t._eid=o.%s
                ) select * from tree"
               id-field on' table id-field root
               id-field on' table on')
              (format "select _eid, %s from %s" id-field table))]
    (with-open [connection (jdbc/get-connection (:datasource *db*))]
      (log/trace {:id :synthigy.dataset.sql/tree-roots-query
                  :data {:entity-id entity-id :sql sql}}
                 "Get entity tree roots")
      (let [roots (map
                   :_eid
                   (sql/execute!
                    connection
                    [sql]
                    core/*return-type*))]
        (when (not-empty roots)
          (pull-roots connection schema {(keyword as) roots}))))))

(defn search-entity-tree
  "Function searches entity tree and returns results by requested selection."
  [entity-id on {order-by :_order_by
                 :as args} selection]
  (entity-accessible? entity-id #{:read})
  (let [{:keys [entity/table entity/as]
         :as schema}
        (binding [*operation-rules* #{:read}]
          (selection->schema
           entity-id
           selection
           args))
        ;;
        on' (name on)]
    (with-open [connection (jdbc/get-connection (:datasource *db*))]
      (letfn [(targeting-args? [args]
                (when (or
                       (and args (not (vector? args)))
                       (and args (vector? args) (not-empty args)))
                  (if (vector? args)
                    (some targeting-args? args)
                    (let [args' (dissoc args :_offset :_limit)
                          some-constraint? (not-empty (dissoc args' :_and :_or :_where))]
                      (if some-constraint?
                        true
                        (some
                         targeting-args?
                         ((juxt :_and :_or :_where :_maybe) args')))))))
              (targeting-schema? [{:keys [args fields relations]}]
                (or
                 (targeting-args? args)
                 (some targeting-args? (vals fields))
                 (some targeting-schema? (vals relations))))]
        (let [targeting? (targeting-schema? schema)
              targets (when targeting?
                        (when-let [found-roots
                                   (search-entity-roots
                                    connection
                                    (update schema :args dissoc :_distinct :_limit :_offset))]
                          (not-empty (get found-roots (keyword as)))))]
          (cond
            ;; If there some targets are found with search-entity-roots
            (not-empty targets)
            (let [sql (format
                       "with recursive tree(_eid,link,path,cycle) as (
                        select
                        g._eid, g.%s, ',' || g._eid || ',', 0
                        from %s g where g._eid in (%s)
                        union all
                        select g._eid, g.%s, o.path || g._eid || ',',
                               CASE WHEN INSTR(o.path, ',' || g._eid || ',') > 0 THEN 1 ELSE 0 END
                        from %s g, tree o
                        where g._eid=o.link and not cycle
                        ) select * from tree group by _eid"
                       on' table (clojure.string/join ", " targets)
                       on' table)
                  tree (sql/execute! connection [sql] core/*return-type*)
                  maybe-roots (set (map :_eid tree))
                  roots (map :_eid (remove (comp maybe-roots :link) tree))
                  ranked-selection (fn [roots]
                                     (let [roots' (clojure.string/join ", " roots)]
                                       (if (not-empty order-by)
                                         (format
                                          "(select _eid, %s, row_number() over (%s) as _rank from %s where _eid in (%s))"
                                          on' (modifiers-selection->sql {:args {:_order_by order-by}}) table roots')
                                         (format "(select _eid, %s, _eid as _rank from %s where _eid in (%s))" on' table roots'))))
                  sql-final [(format
                              "with recursive tree(_eid,link,path,prank,cycle) as (
                               select
                               g._eid, g.%s, ',' || g._eid || ',', ',' || g._rank || ',', 0
                               from %s g
                               union all
                               select g._eid, g.%s, o.path || g._eid || ',', o.prank || g._rank || ',',
                                      CASE WHEN INSTR(o.path, ',' || g._eid || ',') > 0 THEN 1 ELSE 0 END
                               from %s g, tree o
                               where g.%s = o._eid and not cycle
                               ) select * from tree order by prank %s"
                              on' (ranked-selection roots)
                              on' (ranked-selection maybe-roots) on'
                              (modifiers-selection->sql {:args (dissoc args :_order_by)}))]]
              (log/trace {:id :synthigy.dataset.sql/tree-roots-query
                          :data {:entity-id entity-id :sql (first sql-final)}}
                         "Get entity tree roots")
              (when (some? roots)
                (pull-roots
                 connection (shave-schema-arguments schema)
                 {(keyword as)
                  (distinct
                   (map
                    :_eid
                    (sql/execute! connection sql-final core/*return-type*)))})))
            ;; If schema is targeted but no results are found
            (and targeting? (empty? targets))
            nil
            :else
            ;; If targets aren't found
            (let [ranked-init-selection (if (not-empty order-by)
                                          (format
                                           "(select _eid, %s, row_number() over (%s) as _rank from %s where %s is null)"
                                           on' (modifiers-selection->sql {:args {:_order_by order-by}}) table on')
                                          (format "(select _eid, %s, _eid as _rank from %s where %s is null)" on' table on'))
                  ranked-selection (if (not-empty order-by)
                                     (format
                                      "(select _eid, %s, row_number() over (%s) as _rank from %s)"
                                      on' (modifiers-selection->sql {:args {:_order_by order-by}}) table)
                                     (format "(select _eid, %s, _eid as _rank from %s)" on' table))
                  sql-final [(format
                              "with recursive tree(_eid,link,path,prank,cycle) as (
                               select
                               g._eid, g.%s, ',' || g._eid || ',', ',' || g._rank || ',', 0
                               from %s g
                               union all
                               select g._eid, g.%s, o.path || g._eid || ',', o.prank || g._rank || ',',
                                      CASE WHEN INSTR(o.path, ',' || g._eid || ',') > 0 THEN 1 ELSE 0 END
                               from %s g, tree o
                               where g.%s = o._eid and not cycle
                               ) select * from tree order by prank %s"
                              on' ranked-init-selection
                              on' ranked-selection on'
                              (modifiers-selection->sql {:args (dissoc args :_order_by)}))]]
              (log/trace {:id :synthigy.dataset.sql/tree-roots-query
                          :data {:entity-id entity-id :sql (first sql-final)}}
                         "Get entity tree roots")
              (pull-roots
               connection (shave-schema-aggregates schema)
               {(keyword as)
                (distinct
                 (map
                  :_eid
                  (sql/execute! connection sql-final core/*return-type*)))}))))))))

(defn delete-entity
  "See PG delete-entity for the RLS-injection rationale (2026-06-02) and
   the RLS-denied vs nonexistent disambiguation (2026-06-03)."
  ([entity-id args]
   (let [r (with-open [connection (jdbc/get-connection (:datasource *db*))]
             (jdbc/with-transaction [tx connection]
               (substrate/set-context! *db* tx)
               (delete-entity tx entity-id args)))]
     (substrate/wake-drainer!)
     r))
  ([connection entity-id args]
   (binding [*operation-rules* #{:delete}]
     (let [entity-schema (sql-query/deployed-schema-entity entity-id)
           table (:table entity-schema)
           uniques (set (flatten ((comp :unique :constraints) entity-schema)))
           unique-attribute-keys (as-> (:fields entity-schema) result
                                   (select-keys result uniques)
                                   (vals result)
                                   (conj (map :key result) (id/key)))
           filtered-args (select-keys args unique-attribute-keys)
           predicate-args (reduce-kv
                           (fn [acc k v] (assoc acc k {:_eq v}))
                           nil
                           filtered-args)]
       (enhance/apply-delete entity-id args nil)
       (boolean
        (when (and (not-empty predicate-args) table)
          (let [schema (selection->schema entity-id nil predicate-args)
                roots (search-entity-roots connection schema)
                eids (when (seq roots) (first (vals roots)))]
            (cond
              (seq eids)
              (let [placeholders (str/join "," (repeat (count eids) "?"))
                    sql (into [(format "delete from \"%s\" where _eid IN (%s)"
                                       table placeholders)]
                              eids)]
                (log/trace {:id :synthigy.dataset.sql/deleting-entity
                            :data {:entity-id entity-id :sql sql}}
                           "Deleting entity")
                (sql/execute! connection sql core/*return-type*)
                true)

              :else
              (let [where-cols (mapv (fn [k] (format "\"%s\" = ?" (name k)))
                                     (keys filtered-args))
                    probe-sql (format "select _eid from \"%s\" where %s limit 1"
                                      table (str/join " AND " where-cols))
                    probe-result (sql/execute! connection
                                               (into [probe-sql]
                                                     (vals filtered-args))
                                               core/*return-type*)]
                (if (seq probe-result)
                  (let [entity-name (:name entity-schema)]
                    (throw
                     (ex-info
                      (format "You don't have permission to delete this %s"
                              entity-name)
                      {:type ::delete-rls-denied
                       :code "DELETE_FORBIDDEN"
                       :entity entity-id
                       :entity-name entity-name
                       :args filtered-args})))
                  true))))))))))

;; FIXME
(defn slice-entity
  ([entity-id args selection]
   (let [r (with-open [connection (jdbc/get-connection (:datasource *db*))]
             (jdbc/with-transaction [tx connection]
               (substrate/set-context! *db* tx)
               (slice-entity tx entity-id args selection)))]
     (substrate/wake-drainer!)
     r))
  ([tx entity-id args selection]
   (letfn [(targeting-args? [args]
             (when args
               (if (vector? args)
                 (some targeting-args? args)
                 (let [args' (dissoc args :_offset :_limit)
                       some-constraint? (not-empty (dissoc args' :_and :_or :_where :_maybe))]
                   (if some-constraint?
                     true
                     (some
                      targeting-args?
                      ((juxt :_and :_or :_where :_maybe) args')))))))]
     (let [{:keys [relations entity/table entity/as]
            :as schema}
           (selection->schema entity-id selection args)
           enforced-schema (binding [*operation-rules* #{:delete}]
                             (selection->schema entity-id selection args))]
       (if (and
            (not= enforced-schema schema)
            (not (access/superuser?)))
         (throw
          (ex-info
           "User doesn't have :delete rule for some of sliced relations or entities"
           {:type ::enforce-slice
            :roles (access/role-ids)}))
         (let [queries (reduce-kv
                        (fn [r k {tt :to/table
                                  tf :to/field
                                  ff :from/field
                                  rt :relation/table
                                  tas :entity/as
                                  args' :args
                                  :as schema'}]
                          (let [query (str "delete from " \" rt \")
                                 ;;
                                [where-from from-data] (search-stack-args (dissoc schema :relations))
                                 ;;
                                select-from
                                (when (targeting-args? args)
                                  (format
                                   "(select _eid from \"%s\" as %s where %s)"
                                   table as where-from))
                                 ;;
                                [where-to to-data] (search-stack-args schema')
                                 ;;
                                select-to
                                (when (targeting-args? args')
                                  (format
                                   "(select _eid from \"%s\" as %s where %s)"
                                   tt tas where-to))
                                 ;;
                                where (when (or select-to select-from)
                                        (j-and
                                         (cond-> []
                                           select-from (conj (str ff " in " select-from))
                                           select-to (conj (str tf " in " select-to)))))]
                            (assoc r k (into
                                        [(str query (when (not-empty where) "\nwhere ") where)]
                                        (into from-data to-data)))))
                        {}
                        relations)
               result (reduce-kv
                       (fn [r k query]
                         (assoc r k
                                (try
                                  (log/debug {:id :synthigy.dataset.sql/slicing-entity
                                              :data {:entity-id entity-id :query query}}
                                             "Slicing query")
                                  (sql/execute! tx query core/*return-type*)
                                   ;; TODO - Enable this
                                   ; (async/put!
                                   ;   core/client
                                   ;   {:type :entity/slice
                                   ;    :entity entity-id
                                   ;    :args args
                                   ;    :selection selection})
                                  true
                                  (catch Throwable e
                                    (log/error! {:id :synthigy.dataset.sql/slice-failed
                                                 :data {:entity-id entity-id}} e)
                                    false))))
                       {}
                       queries)]
           result))))))

(extend-type synthigy.db.SQLite
  db/ModelQueryProtocol
  (db/sync-entity
    [_ entity-id data]
    (write-entity entity-id data false))
  (db/stack-entity
    [_ entity-id data]
    (write-entity entity-id data true))
  (db/slice-entity
    [_ entity-id args selection]
    (binding [sql-query/*fetch-mode* :serial]
      (slice-entity (id/entity entity-id) args selection)))
  (db/get-entity
    [_ entity-id args selection]
    (binding [sql-query/*fetch-mode* :serial]
      (get-entity (id/entity entity-id) args selection)))
  (db/get-entity-tree
    [_ entity-id root on selection]
    (binding [sql-query/*fetch-mode* :serial]
      (get-entity-tree (id/entity entity-id) root on selection)))
  (db/search-entity
    [_ entity-id args selection]
    (binding [sql-query/*fetch-mode* :serial]
      (search-entity (id/entity entity-id) args selection)))
  (db/search-entity-tree
    [_ entity-id on args selection]
    (binding [sql-query/*fetch-mode* :serial]
      (search-entity-tree (id/entity entity-id) on args selection)))
  (db/purge-entity
    [_ entity-id args selection]
    (binding [sql-query/*fetch-mode* :serial]
      (purge-entity (id/entity entity-id) args selection)))
  (db/delete-entity
    [_ entity-id data]
    (binding [sql-query/*fetch-mode* :serial]
      (delete-entity (id/entity entity-id) data))))
