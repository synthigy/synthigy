(ns synthigy.dataset.postgres.query
  (:require
    [clojure.core.async :as async]
    clojure.set
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [next.jdbc :as jdbc]
    [next.jdbc.prepare :as p]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.enhance :as enhance]
    [synthigy.dataset.id :as id]
    synthigy.dataset.sql.naming
    [synthigy.dataset.sql.query :as sql-query
     :refer [*operation-rules*
             store-entity-records
             entity-accessible?
             construct-response
             focus-order
             analyze-data
             selection->schema
             schema->aggregate-cursors
             shave-schema-arguments
             shave-schema-relations
             shave-schema-aggregates
             pprint
             j-and
             enhance-write
             pull-cursors
             pull-roots
             distinct->sql
             modifiers-selection->sql
             search-stack-args
             search-stack-from
             prepare-references
             link-relations
             publish-delta]]
    [synthigy.db :refer [*db*] :as db]
    [synthigy.db.postgres]  ; Load Postgres JDBCBackend implementation
    [synthigy.db.sql :as sql])
  (:import
    [java.sql PreparedStatement]))

(extend-protocol p/SettableParameter
  ;; Java Time type conversion:
  clojure.lang.Keyword
  (set-parameter [^clojure.lang.Keyword v ^PreparedStatement s ^long i]
    (.setString s i (name v))))

; (def known-errors
;   {"23502" :null_constraint
;    "23505" :unique_violation})

;; Note: freeze moved to synthigy.dataset.sql.query
;; Note: j-and moved to synthigy.dataset.sql.query
;; Note: enhance-write moved to synthigy.dataset.sql.query

;; Use sql-query/group-entity-rows directly - no re-export

;; Note: pull-references moved to synthigy.dataset.sql.query
;; Note: prepare-references moved to synthigy.dataset.sql.query

;; Use sql-query/project-saved-entities directly - no re-export

;; Note: link-relations moved to synthigy.dataset.sql.query
;; Note: publish-delta moved to synthigy.dataset.sql.query

(defn set-entity
  ([entity-id data]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (jdbc/with-transaction [tx connection]
       (set-entity tx entity-id data true))))
  ([entity-id data stack?]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (jdbc/with-transaction [tx connection]
       (set-entity tx entity-id data stack?))))
  ([tx entity-id data stack?]
   #_(do
       (def entity-id entity-id)
       (def data data)
       (def stack? stack?)
       (def roles *roles*)
       (def user *user*))
   (letfn [(pull-roots [{:keys [root entity root/table]}]
             (log/tracef
               "[%s]Pulling root(%s) entity after mutation"
               entity-id root)
             (if (sequential? root)
               (mapv #(get-in entity [table %]) root)
               (get-in entity [table root])))]
     (let [analysis (analyze-data tx entity-id data stack?)]
       (log/tracef "Storing based on analysis\n%s" (pprint analysis))
       (as-> analysis result
         (prepare-references tx result)
         (enhance-write tx result)
         (store-entity-records tx result)
         (sql-query/project-saved-entities result)
         (link-relations tx result stack?)
         (publish-delta result)
         (pull-roots result))))))

; (defn shave-schema-relations
;   ([schema]
;    (let [arg-keys (set (keys (:args schema)))
;          relation-keys (set (keys (:relations schema)))
;          valid-keys (clojure.set/intersection arg-keys relation-keys)]
;      (update schema :relations select-keys valid-keys)))
;   ([schema cursor]
;    (letfn [(shave [schema cursor]
;              (if (not-empty cursor)
;                (let [c (butlast cursor)
;                      k [(last cursor)]]
;                  (recur
;                    (->
;                      schema 
;                      (update-in (conj (sql-query/relations-cursor c) :relations) select-keys k))
;                    c))
;                schema))]
;      (if (not-empty cursor)
;        (shave 
;          (update-in schema (sql-query/relations-cursor cursor) dissoc :relations)
;          cursor)
;        (dissoc schema :relations)))))

;; Note: shave-schema-arguments moved to synthigy.dataset.sql.query

(defn search-entity-roots
  ([schema]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (search-entity-roots connection schema)))
  ([connection schema]
   ; (log/tracef "Searching entity roots for schema:\n%s" (pprint schema))
   ;; Prepare tables target table by inner joining all required tables
   ; (comment
   ;   (def focused-schema (focus-order schema))
   ;   (search-stack-from focused-schema))
   ; (def schema schema)
   (let [focused-schema (focus-order schema)
         [[root-table :as tables] from] (search-stack-from focused-schema)
         ;; then prepare where statements and target data
         [where data] (search-stack-args focused-schema)
         ;; select only _eid for each table
         ;; TODO - this is potentially unnecessary... I've thought
         ;; to focus as much as possible roots and pin all records
         ;; at all tables for defined conditions. But this will
         ;; not work on LEFT JOIN, because it will break _limit
         selected-ids (clojure.string/join
                        ", "
                        (map
                          #(str (name %) "._eid as " (name %))
                          tables))
         ; selected-ids (str (name (first tables)) "._eid as " (name (first tables)))
         distinct-on (or (distinct->sql schema)
                         (when (and
                                 (contains? (:args focused-schema) :_limit)
                                 ;; BUG - bellow is hotfix for
                                 ;; ERROR: SELECT DISTINCT ON expressions must match initial ORDER BY expressions
                                 ;; Distinct on was inserted so that when left join is used
                                 ;; in combination with _limit, that proper results will
                                 ;; be returned. Left joining with _limit is problematic
                                 (not (contains? (:args focused-schema) :_order_by)))
                           (format "distinct on (%s._eid)" (name root-table))))
         ;; Create query
         modifiers (modifiers-selection->sql schema)
         query (as-> (format "select %s %s from %s" (str distinct-on) selected-ids from) query
                 (if (not-empty where) (str query \newline "where " where) query)
                 (if modifiers (str query " " modifiers) query))
         ;; Execute query
         [r :as ids] (if (and
                           (empty? distinct-on)
                           (empty? where)
                           (empty? data)
                           (empty? (get-in schema [:args :_order_by]))
                           (= 1 (count tables)))
                       ;; If schema want's to return whole table
                       ;; return nil to mark that there are no root
                       nil
                       ;; Otherwise try to find roots and if
                       ;; none are found return empty vector
                       (do
                         (log/tracef
                           "Query for roots:\n%s\nData:\n%s"
                           query (pprint data))
                         (sql/execute!
                           connection (into [query] data)
                           core/*return-type*)))]
     ;; when there are some results
     (if (not-empty r)
       ;; get table keys
       (let [ks (keys r)]
         ;; for given tables take only distinct found keys and
         ;; associate found ids with table key
         (reduce
           (fn [ids' k]
             (assoc ids' k (distinct (map #(get % k) ids))))
           nil
           ks))
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
           _ (log/tracef "Searching for entity\n%s" schema)
           roots (search-entity-roots connection schema)]
       (when (some? roots)
         (log/tracef "[%s] Found roots: %s" entity-id (str/join ", " roots))
         (pull-roots connection schema roots))))))

(defn purge-entity
  ([entity-id args selection]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (let [schema (selection->schema entity-id selection args)
           enforced-schema (as-> nil _
                             (binding [*operation-rules* #{:owns}]
                               (selection->schema entity-id selection args))
                             (binding [*operation-rules* #{:delete}]
                               (selection->schema entity-id selection args)))]
       ; (log/info
       ;   :entity entity-id
       ;   :args args
       ;   :selection selection
       ;   :schema schema)
       (if (and
             (not= enforced-schema schema)
             (not (access/superuser?)))
         (throw
           (ex-info
             "Purge not allowed. User doesn't own all entites included in purge"
             {:type ::enforce-purge
              :roles (access/current-roles)}))
         (binding [*operation-rules* #{:purge :delete}]
           (let [roots (search-entity-roots connection schema)]
             (if (some? roots)
               (letfn [(construct-statement
                         [table _eids]
                         (log/debugf "[%s]Constructing purge for eids #%d: %s" table (count _eids) (str/join ", " _eids))
                         [(str "delete from \"" table "\" where _eid=any(?)") (long-array _eids)])
                       ; [(str "delete from \"" table "\" where _eid in (select _eid from \"" table "\" where _eid=any(?))") (long-array _eids)])
                       (process-statement [r k v]
                         (conj r (construct-statement k (keys v))))]
                 (let [db (pull-cursors connection schema roots)
                       response (construct-response schema db roots)
                       delete-statements (reduce-kv process-statement [] db)]
                   (doseq [query delete-statements]
                     (log/debugf "[%s]Purgin entity rows with %s" entity-id query)
                     (sql/execute! connection query core/*return-type*))
                   (async/put! core/*delta-client* {:element entity-id
                                                    :delta {:type :purge
                                                            :data response}})
                   response))
               []))))))))

(defn get-entity
  ([entity-id args selection]
   (get-entity entity-id args selection #{:read :get}))
  ([entity-id args selection operations]
   (assert (some? args) "No arguments to get entity for...")
   (entity-accessible? entity-id operations)
   (log/debugf
     "[%s] Getting entity\nArgs:\n%s\nSelection:\n%s"
     entity-id (pprint args) (pprint selection))
   ; (def entity-id entity-id)
   ; (def args args)
   ; (def selection selection)
   ; (def operations operations)
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
               (log/tracef
                 "[%s] Returning response\n%s"
                 entity-id (pprint response))
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
      (log/tracef
        "[%s] Get entity tree roots\n%s"
        entity-id sql)
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
              (log/tracef
                "[%s] Get entity tree roots\n%s"
                entity-id (first sql-final))
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
              (log/tracef
                "[%s] Get entity tree roots\n%s"
                entity-id (first sql-final))
              (pull-roots
                connection (shave-schema-aggregates schema)
                {(keyword as)
                 (distinct
                   (map
                     :_eid
                     (sql/execute! connection sql-final core/*return-type*)))}))))))))

(defn aggregate-entity
  ([entity-id args selection]
   (let [{:keys [fields]
          :as schema} (selection->schema entity-id selection args)
         cursors (schema->aggregate-cursors schema)
         args (reduce-kv
                (fn [args k v]
                  (if (some? v)
                    (assoc args k v)
                    args))
                args
                fields)
         schema (assoc schema :args args)]
     (log/tracef "[%s] Aggregate cursors:\n%s" entity-id (pr-str cursors))
     (letfn [(cursor-field [cursor & fields]
               (clojure.string/join "$$" (map name (concat cursor fields))))]
       (with-open [connection (jdbc/get-connection (:datasource *db*))]
         (reduce
           (fn [result cursor]
             (let [shaved-schema (if (empty? cursor)
                                   (shave-schema-relations schema)
                                   (->
                                     schema
                                     (shave-schema-relations cursor)
                                     (shave-schema-aggregates (butlast cursor))))
                  ;;
                   {:keys [counted? aggregate entity/as]}
                   (if (empty? cursor)
                     shaved-schema
                     (get-in shaved-schema (sql-query/relations-cursor cursor)))
                  ;;
                   selection (clojure.string/join
                               ", "
                               (cond->
                                 []
                                 counted?
                                 (conj
                                   (format
                                     "count(%s._eid) as %s"
                                     as
                                     (cursor-field cursor "count")))
                                ;;
                                 (not-empty aggregate)
                                 (as-> stack'
                                       (reduce-kv
                                         (fn [stack'' field aggregate]
                                           (concat stack''
                                                   (map
                                                     #(format "%s(%s.%s) as %s" % as (name field) (cursor-field cursor (name field) %))
                                                     (:operations aggregate))))
                                         stack'
                                         aggregate))))
                  ;;
                   [_ from maybe-data] (search-stack-from shaved-schema)
                  ;;
                   [where data] (search-stack-args shaved-schema)
                  ;;
                   query (as-> (format "select %s from %s" selection from) query
                           (if (not-empty where) (str query \newline "where " where) query))]
               (log/tracef
                 "[%s] Aggregate query for cursor %s\nQuery:\n%sData:\n%s"
                 entity-id cursor query (pr-str data))
               (reduce-kv
                 (fn [result k v]
                   (let [[_ selection :as path] (clojure.string/split (name k) #"\$\$")]
                     (if (some? selection)
                       (assoc-in result (map keyword path) v)
                       (assoc result k v))))
                 result
                 (sql/execute-one! connection (into [query] ((fnil into []) maybe-data data))))))
           nil
           (concat [[]] cursors)))))))

(defn aggregate-entity-tree
  [entity-id on args selection]
  (let [{:keys [entity/table entity/as]
         :as schema} (selection->schema
                       entity-id
                       selection
                       args)
        on' (name on)]
    (with-open [connection (jdbc/get-connection (:datasource *db*))]
      (when-let [found-roots (search-entity-roots
                               connection
                               (update schema :args dissoc :_distinct :_limit :_offset))]
        (when-let [roots (get found-roots (keyword as))]
          (let [sql (format
                      "with recursive tree(_eid,link,depth,path,cycle) as (
                      select
                      g._eid, g.%s, 1, array[g._eid], false
                      from %s g where g._eid = any(?)
                      union all
                      select g._eid, g.%s, o.depth + 1, path || g._eid, g._eid=any(path)
                      from %s g, tree o
                      where g._eid=o.link and not cycle
                      ) select distinct on (_eid) * from tree"
                      on' table on' table)]
            (log/tracef
              "[%s] Get entity tree roots\n%s"
              entity-id sql)
            (if-let [rows (when (not-empty roots)
                            (not-empty
                              (map
                                :_eid
                                (sql/execute!
                                  connection
                                  [sql (int-array roots)]
                                  core/*return-type*))))]
              (aggregate-entity entity-id {:_eid {:_in rows}} selection)
              (aggregate-entity entity-id nil selection))))))))

(defn delete-entity
  ([entity-id args]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (delete-entity connection entity-id args)))
  ([connection entity-id args]
   (binding [*operation-rules* #{:delete}]
     (let [{:keys [entity/table]} (selection->schema entity-id nil nil)
           entity-schema (sql-query/deployed-schema-entity entity-id)
           uniques (set
                     (flatten ((comp :unique :constraints)
                               entity-schema)))
           unique-attribute-keys (as-> (:fields entity-schema) result
                                   (select-keys result uniques)
                                   (vals result)
                                   (conj (map :key result) (id/key)))]
       (enhance/apply-delete entity-id args nil)
       (boolean
         (when (and (not-empty args) table)
           (let [[statements data] (reduce-kv
                                     (fn [[statements data] k v]
                                       [(conj statements (str (name k) "=?"))
                                        (conj data v)])
                                     [[] []]
                                     (select-keys args unique-attribute-keys))
                 sql (cond->
                       [(format
                          "delete from \"%s\" where %s"
                          table
                          (j-and statements))]
                       (not-empty data) (into data))
                 _ (log/tracef
                     "[%s] Deleting entity\n%s"
                     entity-id sql)]
             (sql/execute! connection sql core/*return-type*)
             (async/put! core/*delta-client* {:element entity-id
                                              :delta {:type :delete
                                                      :data args}})
             ; (async/put!
             ;   core/client
             ;   {:type :entity/delete
             ;    :entity entity-id
             ;    :args args})
             true)))))))

;; FIXME
(defn slice-entity
  ([entity-id args selection]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (slice-entity connection entity-id args selection)))
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
              :roles (access/current-roles)}))
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
                                   (log/debugf
                                     "[%s] slicing query:\n%s"
                                     entity-id query)
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
                                     (log/error e "Couldn't slice entity")
                                     false))))
                        {}
                        queries)]
           ; (def relations relations)
           (doseq [[label {:keys [relation]
                           :as slice}] relations]
             (async/put! core/*delta-client*
                         {:element relation
                          :delta {:type :slice
                                  :data {label slice}}}))
           result))))))

(extend-type synthigy.db.Postgres
  db/ModelQueryProtocol
  (db/sync-entity
    [_ entity-id data]
    (set-entity (id/entity entity-id) data false))
  (db/stack-entity
    [_ entity-id data]
    (set-entity (id/entity entity-id) data true))
  (db/slice-entity
    [_ entity-id args selection]
    (slice-entity (id/entity entity-id) args selection))
  (db/get-entity
    [_ entity-id args selection]
    (get-entity (id/entity entity-id) args selection))
  (db/get-entity-tree
    [_ entity-id root on selection]
    (get-entity-tree (id/entity entity-id) root on selection))
  (db/search-entity
    [_ entity-id args selection]
    (search-entity (id/entity entity-id) args selection))
  (db/search-entity-tree
    [_ entity-id on args selection]
    (search-entity-tree (id/entity entity-id) on args selection))
  (db/purge-entity
    [_ entity-id args selection]
    (purge-entity (id/entity entity-id) args selection))
  (db/aggregate-entity
    [_ entity-id args selection]
    (aggregate-entity (id/entity entity-id) args selection))
  (db/aggregate-entity-tree
    [_ entity-id on args selection]
    (aggregate-entity-tree (id/entity entity-id) on args selection))
  (db/delete-entity
    [_ entity-id data]
    (delete-entity (id/entity entity-id) data)))
