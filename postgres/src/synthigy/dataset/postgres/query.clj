(ns synthigy.dataset.postgres.query
  (:require
   clojure.set
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.prepare :as p]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.postgres.fused :as fused]
   [synthigy.substrate.postgres :as substrate]
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
            distinct->sql
            modifiers-selection->sql
            search-stack-args
            search-stack-from
            inner-exists]]
   [synthigy.db :refer [*db*] :as db]
   [synthigy.db.postgres]  ; Load Postgres JDBCBackend implementation
   [synthigy.db.sql :as sql]
   [synthigy.log :as log])
  (:import
   [java.sql PreparedStatement]
   [org.postgresql.util PSQLException]))

(extend-protocol p/SettableParameter
  ;; Java Time type conversion:
  clojure.lang.Keyword
  (set-parameter [^clojure.lang.Keyword v ^PreparedStatement s ^long i]
    (.setString s i (name v))))

(def ^:dynamic *write-retries*
  "Max retries on transient Postgres errors (40P01 deadlock, 40001
   serialization failure) for the sync/stack write path. Each retry
   re-opens the transaction from scratch. Set to 0 to surface the error
   directly — used by tests that want to observe a deadlock."
  3)

(defn- transient-write-error?
  "True if `t` or its cause is a Postgres deadlock / serialization
   failure — safe to retry by re-running the whole transaction."
  [^Throwable t]
  (let [e (cond
            (instance? PSQLException t) t
            (instance? PSQLException (.getCause t)) (.getCause t))]
    (boolean
     (when e
       (#{"40P01" "40001"} (.getSQLState ^PSQLException e))))))

(defn- with-write-retry
  "Run `thunk`, retrying on transient Postgres errors up to
   *write-retries* times with exponential backoff + jitter. `thunk` must
   open and own its transaction — on retry it is re-invoked from scratch.
   Safe for sync/stack: a deadlocked transaction rolls back completely,
   so the retry runs against the same pre-attempt state."
  [thunk]
  (loop [attempt 0]
    (let [outcome (try
                    [::ok (thunk)]
                    (catch Throwable e
                      (if (and (transient-write-error? e)
                               (< attempt *write-retries*))
                        [::retry e]
                        (throw e))))]
      (if (= ::ok (first outcome))
        (second outcome)
        (let [backoff (long (+ (* 25 (Math/pow 2 attempt)) (rand-int 50)))]
          (log/debug {:id :synthigy.dataset.sql/write-retry
                      :data {:action :retrying :subject :write
                             :attempt (inc attempt) :backoff-ms backoff}}
                     "Retrying write after transient Postgres error")
          (Thread/sleep backoff)
          (recur (inc attempt)))))))

(defn- write-entity
  "Postgres write path for the sync/stack protocol methods. Opens a fresh
   transaction, binds the tx-local audit GUCs so the relation-audit
   triggers can attribute the mutation, then runs the shared
   `sql-query/set-entity`. `stack?` picks sync (false) vs stack (true).
   Callers wrap this in `with-write-retry` so a transient deadlock /
   serialization failure re-runs the whole transaction."
  [entity-id data stack?]
  (with-open [connection (jdbc/get-connection (:datasource *db*))]
    (jdbc/with-transaction [tx connection]
      (substrate/set-context! *db* tx)
      (sql-query/set-entity tx (id/entity entity-id) data stack?))))

(defn search-entity-roots
  "Find root-entity `_eid`s matching the schema's filters.

  Roots are filtered by the root entity's own predicates plus a correlated
  `EXISTS (...)` per INNER relation (`inner-exists`) — never by JOINing
  through relation tables. A JOIN multiplies root rows (one per matching
  child), so `_limit` would slice the *joined* set and under-count roots;
  `EXISTS` keeps one row per root, so `_limit`/`_order_by` are exact and no
  DISTINCT dedup is needed."
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
         distinct-on (distinct->sql schema)
         modifiers (modifiers-selection->sql schema)
         query (as-> (format "select %s%s._eid as %s from \"%s\" %s"
                             (if distinct-on (str distinct-on " ") "")
                             root-as root-as root-tbl root-as)
                     query
                 (if (not-empty where-all) (str query \newline "where " where-all) query)
                 (if modifiers (str query " " modifiers) query))
         data (vec (concat where-data exists-data))
         [r :as ids] (if (and (empty? where-all)
                              (nil? distinct-on)
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
   (binding [*operation-rules* operations]
     (let [schema (selection->schema entity-id selection args)
           ; _ (do
           ;     (def schema schema)
           ;     (def selection selection)
           ;     (def args args))
           _ (log/trace {:id :synthigy.dataset.sql/searching-entity :data {:schema schema}}
                        "Searching for entity")
           roots (search-entity-roots connection schema)]
       (when (some? roots)
         (log/trace {:id :synthigy.dataset.sql/roots-found
                     :data {:entity-id entity-id :roots (str/join ", " roots)}}
                    "Found roots")
         (pull-roots connection schema roots))))))

(defn purge-entity
  ;; PG-side: drainer wakeup is handled exclusively by the statement-level
  ;; pg_notify trigger; no app-path wake-drainer! call is needed.
  ;;
  ;; The previous `enforce-purge` gate (comparing default-rules schema vs
  ;; `:delete`-projected schema via an as-> nil _ chain) was dropped
  ;; 2026-06-02: the as-> chain discarded the `:owns` intermediate and
  ;; effectively threw for any non-superuser when the entity had a
  ;; `:delete` RLS guard (the two projections always differ in that case).
  ;; RLS scoping via `search-entity-roots` is the load-bearing protection:
  ;; users can only purge rows their RLS WHERE injection lets them see.
  ([entity-id args selection]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
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
                       [(str "delete from \"" table "\" where _eid=any(?)") (long-array _eids)])
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
             [])))))))

(defn get-entity
  "Get a single entity by identity. Args must carry at least one
   predicate — `get` is row-identity, not 'search and pick the first'.
   Calling with `nil` or `{}` throws `IDENTITY_REQUIRED`. Pre-fix
   (2026-06-03) the empty-args path silently ran an unfiltered scan
   and returned the first row by `_eid`, which is dangerous: an
   invalid query looks like a successful identity lookup."
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
               (id/field) on' table id-field root
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
        (comment
          (with-open [connection (jdbc/get-connection (:datasource *db*))]
            (pull-roots connection schema {(keyword as) roots})))
        (when (not-empty roots)
          (pull-roots connection schema {(keyword as) roots}))))))

;; Note: shave-schema-relations moved to synthigy.dataset.sql.query
;; Note: shave-schema-aggregates moved to synthigy.dataset.sql.query

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
                        g._eid, g.%s, array[g._eid], false
                        from %s g where g._eid in (%s)
                        union all
                        select g._eid, g.%s, g._eid || path, g._eid=any(path)
                        from %s g, tree o
                        where g._eid=o.link and not cycle
                        ) select distinct on (_eid) * from tree"
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
                               g._eid, g.%s, array[g._eid],array[g._rank], false
                               from %s g
                               union all
                               select g._eid, g.%s, path || g._eid, prank || g._rank, g._eid=any(path)
                               from %s g, tree o
                               where g.%s =o._eid and not cycle
                               ) select * from tree order by prank asc %s"
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
                               g._eid, g.%s, array[g._eid],array[g._rank], false
                               from %s g
                               union all
                               select g._eid, g.%s, path || g._eid, prank || g._rank, g._eid=any(path)
                               from %s g, tree o
                               where g.%s =o._eid and not cycle
                               ) select * from tree order by prank asc %s"
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
  "PK-by-PK delete. Routes through `search-entity-roots` so RLS WHERE
   conditions (for `:delete` operation) are injected — previously the
   raw `DELETE WHERE pk=?` bypassed RLS entirely, letting any
   authenticated user delete any row whose PK they could guess.

   On RLS denial:
     - If the row truly doesn't exist for anyone → returns true
       (preserves pre-2026-06-02 'silent no-op on miss' contract,
       matching test-delete-entity-nonexistent).
     - If the row exists but RLS hides it from the caller →
       THROWS ex-info with :code \"DELETE_FORBIDDEN\". Single-target
       delete is unambiguous — caller named one row, returning true
       when nothing happened would silently mislead them.

   Contract preserved on args: filtered to unique-constraint keys (+
   id-key) before the delete runs — non-unique args are silently
   dropped, matching pre-2026-06-02 behavior."
  ([entity-id args]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (jdbc/with-transaction [tx connection]
       (substrate/set-context! *db* tx)
       (delete-entity tx entity-id args))))
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
              (let [sql [(format "delete from \"%s\" where _eid=any(?)" table)
                         (long-array eids)]]
                (log/trace {:id :synthigy.dataset.sql/deleting-entity
                            :data {:entity-id entity-id :sql sql}}
                           "Deleting entity")
                (sql/execute! connection sql core/*return-type*)
                true)

              ;; Empty RLS scope. Run an RLS-bypassed existence probe
              ;; against the predicate to distinguish "doesn't exist"
              ;; (return true, preserves nonexistent contract) from
              ;; "exists but RLS hides it" (throw DELETE_FORBIDDEN).
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
                  ;; Row truly doesn't exist — preserve nonexistent contract.
                  true))))))))))

;; FIXME
(defn slice-entity
  ;; PG-side: drainer wakeup is handled exclusively by the statement-level
  ;; pg_notify trigger; no app-path wake-drainer! call is needed.
  ([entity-id args selection]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (jdbc/with-transaction [tx connection]
       (substrate/set-context! *db* tx)
       (slice-entity tx entity-id args selection))))
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

(extend-type synthigy.db.Postgres
  db/ModelQueryProtocol
  (db/sync-entity
    [_ entity-id data]
    (with-write-retry #(write-entity entity-id data false)))
  ;;
  (db/stack-entity
    [_ entity-id data]
    (with-write-retry #(write-entity entity-id data true)))
  ;;
  (db/slice-entity
    [_ entity-id args selection]
    (slice-entity (id/entity entity-id) args selection))
  ;;
  (db/get-entity
    [_ entity-id args selection]
    (fused/get-one (id/entity entity-id) args selection))
  ;;
  (db/get-entity-tree
    [_ entity-id root on selection]
    (get-entity-tree (id/entity entity-id) root on selection))
  ;;
  (db/search-entity
    [_ entity-id args selection]
    (fused/search (id/entity entity-id) args selection))
  ;;
  (db/search-entity-tree
    [_ entity-id on args selection]
    (binding [sql-query/*fetch-mode* :serial]
      (search-entity-tree (id/entity entity-id) on args selection)))
  ;;
  (db/purge-entity
    [_ entity-id args selection]
    (purge-entity (id/entity entity-id) args selection))
  ;;
  (db/delete-entity
    [_ entity-id data]
    (delete-entity (id/entity entity-id) data)))
