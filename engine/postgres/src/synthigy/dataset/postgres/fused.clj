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

(ns synthigy.dataset.postgres.fused
  "Per-level read compiler for Postgres: one SQL statement per tree level, relations as LATERALs."
  (:require [synthigy.db :as db]
            [synthigy.log :as log]
            [synthigy.dataset.id :as id]
            [synthigy.dataset.access :as access]
            [synthigy.dataset.sql.query :as q]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]
            [jsonista.core :as json]))

(defn qi  "quote SQL identifier" [s] (str \" (str/replace (name s) "\"" "") \"))
(defn lit "single-quoted SQL string literal" [s]
  (str \' (str/replace (name s) "'" "''") \'))

(defn gen
  [p]
  (name (gensym (str "__" p))))

(def ^:private meta-keys #{:_join :_limit :_offset :_order_by :_distinct})

(defn order-by-pairs
  "Normalizes any `_order_by` spec shape into an ordered seq of [field direction]."
  [order-by]
  (cond
    (map? order-by) (seq order-by)
    (sequential? order-by)
    (mapcat (fn [p]
              (cond
                (map? p)        (seq p)
                (sequential? p) [p]
                :else           [[p :asc]]))
            order-by)
    :else nil))

(defn order-clause [order-by alias]
  (for [[f d] (order-by-pairs order-by)]
    [(str alias "." (qi f)) (if (= :desc d) "desc nulls last" "asc nulls first")]))

(defn has-filter?
  [args] (seq (apply dissoc args meta-keys)))

(def ^:private relation-on-predicate @#'q/relation-on-predicate)

(defn field-entries
  "json_build_object pieces for a node's scalar fields plus _eid; json columns go as ::text."
  [schema alias]
  (let [ftypes (:field-types schema)]
    (cons (str "'_eid', " alias "._eid")
          (for [f (keys (:fields schema)) :when (not= :_eid f)]
            (str (lit f) ", " alias "." (qi f)
                 (when (= "json" (some-> (get ftypes f) name)) "::text"))))))

(defn emit-relation
  "Rows of relation `k` as a single LATERAL."
  [k schema row-alias]
  (let [rel (get-in schema [:relations k])
        _ (when (and (:relation rel)
                     (not (access/relation-allows? (:relation rel) #{:read})))
            (throw (ex-info (str "Relation access forbidden: " k)
                            {:code "RELATION_FORBIDDEN" :relation (:relation rel)})))
        junction   (:relation/table rel)
        from-field (:from/field rel)
        to-field   (:to/field rel)
        child-tbl  (:entity/table rel)
        rel-args   (:args rel)
        ;; must be rel's :entity/as — the predicate text bakes it in ("missing FROM-clause entry")
        ex (or (:entity/as rel) (gen "e"))
        jx (gen "j") cx (gen "c")
        [rls-sql rls-data] (q/node-read-guard rel ex)
        [filter-sql filter-data]
        (when (has-filter? rel-args)
          (or (relation-on-predicate rel) ["" []]))
        [ie-sql ie-data] (q/inner-exists rel ex)
        child-source (str (qi junction) " " jx
                          " join " (qi child-tbl) " " ex
                          " on " ex "._eid = " jx "." (qi to-field))
        correlate (str jx "." (qi from-field) " = " row-alias "._eid"
                       (when (seq filter-sql) (str " and (" filter-sql ")"))
                       (when (seq ie-sql)  (str " and " ie-sql))
                       (when (seq rls-sql) (str " and (" rls-sql ")")))
        child-obj  (str "json_build_object(" (str/join ", " (field-entries rel ex)) ")")
        lat (gen "lat")
        [ords ord-data] (reduce (fn [[os ps] [f d]]
                                  (let [dir (if (= :desc d) "desc nulls last" "asc nulls first")]
                                    (if-let [spec (get-in rel [:ops (keyword (name f))])]
                                      (let [[sql sps] (q/op-order-sql spec ex)] [(conj os [sql dir]) (into ps sps)])
                                      [(conj os [(str ex "." (qi f)) dir]) ps])))
                                [[] []]
                                (order-by-pairs (:_order_by rel-args)))
        limit (:_limit rel-args)
        offset (long (or (:_offset rel-args) 0))
        windowed? (or limit (pos? offset))
        ords (cond-> ords windowed? (conj [(str ex "._eid") "asc"]))
        ord-cols (map-indexed (fn [i [e d]] [(str "__od" i) e d]) ords)
        rn-over (if (seq ords)
                  (str "order by " (str/join "," (map (fn [[e d]] (str e " " d)) ords)))
                  "")
        inner (str "select " child-obj " as __v"
                   (apply str (for [[a e _] ord-cols] (str ", " e " as " a)))
                   (when windowed? (str ", row_number() over (" rn-over ") as __rn"))
                   " from " child-source " where " correlate)
        agg-by (if (seq ord-cols)
                 (str " order by " (str/join "," (map (fn [[a _ d]] (str cx "." a " " d)) ord-cols)))
                 "")
        filt (when windowed?
               (str " filter (where " cx ".__rn > " offset
                    (when limit (str " and " cx ".__rn <= " (+ offset (long limit))))
                    ")"))
        outer (str "select coalesce(json_agg(" cx ".__v" agg-by ")" filt
                   ", '[]'::json) as __rows from (" inner ") " cx)]
    {:lateral (str "left join lateral (" outer ") " lat " on true")
     :json-pair (str (lit k) ", " lat ".__rows")
     :params (vec (concat ord-data (when windowed? ord-data) filter-data ie-data rls-data))}))

(defn emit-one-relation
  "A :one relation as a LIMIT 1 LATERAL."
  [k schema row-alias]
  (let [rel (get-in schema [:relations k])]
    (when (and (:relation rel)
               (not (access/relation-allows? (:relation rel) #{:read})))
      (throw (ex-info (str "Relation access forbidden: " k)
                      {:code "RELATION_FORBIDDEN" :relation (:relation rel)})))
    (let [ex (or (:entity/as rel) (gen "e"))
          jx (gen "j") lat (gen "lat")
          [filter-sql filter-data]
          (when (has-filter? (:args rel))
            (or (relation-on-predicate rel) ["" []]))
          [ie-sql ie-data] (q/inner-exists rel ex)
          [rls-sql rls-data] (q/node-read-guard rel ex)
          fk-on-parent? (or (:recursion? rel) (:ref? rel))
          child-source (if fk-on-parent?
                         (str (qi (:entity/table rel)) " " ex)
                         (str (qi (:relation/table rel)) " " jx
                              " join " (qi (:entity/table rel)) " " ex
                              " on " ex "._eid = " jx "." (qi (:to/field rel))))
          correlate (str (if fk-on-parent?
                           (str ex "._eid = " row-alias "." (qi (:from/field rel)))
                           (str jx "." (qi (:from/field rel)) " = " row-alias "._eid"))
                         (when (seq filter-sql) (str " and (" filter-sql ")"))
                         (when (seq ie-sql)  (str " and " ie-sql))
                         (when (seq rls-sql) (str " and (" rls-sql ")")))]
      {:lateral (str "left join lateral (select json_build_object("
                     (str/join ", " (field-entries rel ex))
                     ") as __v from " child-source " where " correlate " limit 1) "
                     lat " on true")
       :json-pair (str (lit k) ", " lat ".__v")
       :params (vec (concat filter-data ie-data rls-data))})))

(defn emit-node
  "One node's level as [laterals-sql json-entries params]."
  [schema row-alias include-fields?]
  (let [rels (map (fn [k]
                    (if (= :one (get-in schema [:relations k :type]))
                      (emit-one-relation k schema row-alias)
                      (emit-relation k schema row-alias)))
                  (keys (:relations schema)))
        entries (concat
                 (when include-fields? (field-entries schema row-alias))
                 (keep :json-pair rels))]
    [(str/join "\n" (map :lateral rels))
     entries
     (vec (mapcat :params rels))]))

(defn order-terms
  "Root sort terms `[sql dir params join join-params]`: an operation alias through one grouped join, the rest as `q/order-terms`."
  [schema ob]
  (mapcat (fn [[k d]]
            (if-let [spec (get-in schema [:ops k])]
              (let [[join value jps] (q/op-order-join spec (:entity/as schema))]
                [[value d [] join jps]])
              (map (fn [[sql d ps]] [sql d ps nil []]) (q/order-terms schema {k d}))))
          ob))

(defn compile-root-level
  "Root level as one statement yielding a json array of root objects."
  [schema]
  (let [eas (:entity/as schema)               ; engine-fragment alias
        rt  (gen "rt")                        ; derived-table alias
        [laterals entries lat-params] (emit-node schema rt true)
        [where where-data] (q/search-stack-args (dissoc schema :relations))
        [ie-sql ie-data] (q/inner-exists schema eas)
        [rls-sql rls-data] (q/node-read-guard schema eas)
        where-all (str/join " and " (remove str/blank? [where ie-sql rls-sql]))
        args (:args schema)
        ob   (:_order_by args)
        related (when (q/term-order? schema)
                  (map-indexed (fn [i [sql d ps join jps]]
                                 [(str "__rk" i) sql (if (= :desc d) "desc nulls last" "asc nulls first") ps join jps])
                               (order-terms schema ob)))
        ords-inner (if related (map (fn [[a _ d]] [(qi a) d]) related) (order-clause ob eas))
        ords-rt    (if related (map (fn [[a _ d]] [(str rt "." (qi a)) d]) related) (order-clause ob rt))
        paged?     (or (:_limit args) (:_offset args))
        ords-inner (cond-> (vec ords-inner)
                     paged? (into (map (fn [c] [c "asc"]) (q/tiebreak-columns eas (:_distinct args)))))
        ords-rt    (cond-> (vec ords-rt) paged? (conj [(str rt "._eid") "asc"]))
        fk-cols (keep (fn [[_ r]]
                        (when (or (:recursion? r) (:ref? r))
                          (:from/field r)))
                      (:relations schema))
        cols (distinct (remove #{:_eid}
                               (map #(if (keyword? %) % (keyword %))
                                    (concat (keys (:fields schema))
                                            (when-not related (map first (order-by-pairs ob)))
                                            fk-cols))))
        dist (q/distinct->sql schema)
        inner (str "select " (when dist (str dist " ")) eas "._eid"
                   (apply str (for [c cols] (str ", " eas "." (qi c) " as " (qi c))))
                   (apply str (for [[a sql] related] (str ", " sql " as " (qi a))))
                   " from " (qi (:entity/table schema)) " " eas
                   (apply str (keep #(nth % 4) related))
                   (when (seq where-all) (str " where " where-all))
                   (when (seq ords-inner)
                     (str " order by " (str/join "," (map (fn [[e d]] (str e " " d)) ords-inner))))
                   (when (:_limit args)  (str " limit "  (long (:_limit args))))
                   (when (:_offset args) (str " offset " (long (:_offset args)))))
        ord-sel (map-indexed (fn [i [e d]] [(str "__o" i) e d]) ords-rt)
        mid (str "select json_build_object(" (str/join ", " entries) ") as __o"
                 (apply str (for [[a e _] ord-sel] (str ", " e " as " a)))
                 " from (" inner ") " rt "\n" laterals)
        agg-ord (when (seq ord-sel)
                  (str " order by " (str/join "," (map (fn [[a _ d]] (str a " " d)) ord-sel))))]
    [(str "select coalesce(json_agg(__o" agg-ord "), '[]'::json) as result from ("
          mid ") __top")
     (vec (concat (mapcat #(nth % 3) related) (mapcat #(nth % 5) related) where-data ie-data rls-data lat-params))]))

(defn compile-sub-level
  "A non-root level scoped to `parent-eids`, yielding (__pe, __rels json) rows."
  [schema parent-eids]
  (let [p (gen "p")
        [laterals entries lat-params] (emit-node schema p false)
        ids (str/join "," parent-eids)]
    [(str "select " p "._eid as __pe, "
          "json_build_object(" (str/join ", " entries) ") as __rels "
          "from " (qi (:entity/table schema)) " " p "\n" laterals
          "\nwhere " p "._eid in (" ids ")")
     lat-params]))

(def ^:private json-mapper (json/object-mapper {:decode-key-fn keyword}))

(defn parse-json [v]
  (cond
    (nil? v)    nil
    (string? v) (json/read-value v json-mapper)
    :else       (json/read-value (.getValue ^org.postgresql.util.PGobject v) json-mapper)))

(defn node-has-children? [schema]
  (seq (:relations schema)))

(defn expand-tree
  "Pulls deeper levels, one statement per level, and stitches them back by _eid."
  [con schema objs]
  (reduce
   (fn [objs k]
     (let [rel-schema (get-in schema [:relations k])]
       (if-not (and rel-schema (node-has-children? rel-schema))
         objs
         (let [many?    (not= :one (:type rel-schema))
               children (if many?
                          (vec (mapcat #(get % k) objs))
                          (vec (keep #(get % k) objs)))
               eids     (distinct (map :_eid children))]
           (if (empty? eids)
             objs
             (let [[sql params] (compile-sub-level rel-schema eids)
                   started (System/currentTimeMillis)
                   rows (jdbc/execute! con (into [sql] params)
                                       {:builder-fn rs/as-unqualified-lower-maps})
                   elapsed (- (System/currentTimeMillis) started)
                   _ (log/trace {:id ::fused-sub-query
                                 :data {:phase    :sub
                                        :relation k
                                        :parent-count (count eids)
                                        :sql      sql
                                        :params   params
                                        :rows     (count rows)
                                        :elapsed-ms elapsed}}
                                "Fused sub-level query")
                   sub  (into {} (map (fn [r] [(:__pe r) (parse-json (:__rels r))])) rows)
                   expanded (expand-tree con rel-schema
                                         (mapv #(merge % (get sub (:_eid %))) children))
                   by-eid   (into {} (map (juxt :_eid identity)) expanded)]
               (mapv (fn [o]
                       (if many?
                         (update o k #(mapv (fn [c] (by-eid (:_eid c))) %))
                         (update o k #(some->> % :_eid by-eid))))
                     objs)))))))
   objs
   (keys (:relations schema))))

(def ^:private float-types #{"float" "double" "decimal" "currency" "real"})

(defn coerce-node
  "Coerces a parsed node to exactly the engine's result shape, recursively."
  [schema obj]
  (when obj
    (let [o (reduce (fn [m [f t]]
                      (if (and (some? (get m f)) (float-types (some-> t name)))
                        (update m f double)
                        m))
                    obj (:field-types schema))
          o (reduce (fn [m [f dec]]
                      (if (and (fn? dec) (some? (get m f)))
                        (update m f dec o)
                        m))
                    o (:decoders schema))
          o (reduce (fn [m [k rs]]
                      (if-not (contains? m k)
                        m
                        (let [v (get m k)]
                          (cond
                            (nil? v)                     (dissoc m k)
                            (and (vector? v) (empty? v)) (dissoc m k)
                            (vector? v) (assoc m k (mapv #(coerce-node rs %) v))
                            (map? v)    (assoc m k (coerce-node rs v))
                            :else       m))))
                    o (:relations schema))]
      o)))

(defn attach-ops
  "Merges each level's :ops into `objs`, one grouped statement per relation per level."
  [con schema objs]
  (let [values (q/pull-ops con schema (keep :_eid objs))
        objs (if values (mapv #(merge % (get values (some-> (:_eid %) long))) objs) objs)]
    (reduce
     (fn [objs [k rel-schema]]
       (if-not (q/node-has-ops? rel-schema)
         objs
         (let [many? (not= :one (:type rel-schema))
               children (if many? (mapcat #(get % k) objs) (keep #(get % k) objs))
               by-eid (into {} (map (juxt :_eid identity)) (attach-ops con rel-schema (vec children)))]
           (mapv (fn [o]
                   (cond
                     (not (contains? o k)) o
                     many? (update o k #(mapv (fn [c] (by-eid (:_eid c) c)) %))
                     :else (update o k #(some-> % :_eid (by-eid %)))))
                 objs))))
     objs
     (:relations schema))))

(defn run-search [schema]
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (let [entity-table (:entity/table schema)
          [sql params] (compile-root-level schema)
          started (System/currentTimeMillis)
          row  (jdbc/execute-one! con (into [sql] params)
                                  {:builder-fn rs/as-unqualified-lower-maps})
          elapsed (- (System/currentTimeMillis) started)
          objs (vec (parse-json (:result row)))]
      (log/trace {:id ::fused-root-query
                  :data {:phase      :root
                         :entity     entity-table
                         :sql        sql
                         :params     params
                         :rows       (count objs)
                         :elapsed-ms elapsed}}
                 "Fused root query")
      (when (seq objs)
        (attach-ops con schema (mapv #(coerce-node schema %) (expand-tree con schema objs)))))))

(defn search
  "Fused `search-entity`."
  [entity-id args selection]
  (let [eid    (id/entity entity-id)
        schema (q/selection->schema eid selection args)]
    (q/entity-accessible? eid #{:search :read})
    (run-search schema)))

(defn get-one
  "Fused `get-entity` — args wrapped to `_eq`, first row or nil."
  [entity-id args selection]
  (let [eid     (id/entity entity-id)
        eq-args (reduce-kv (fn [m k v] (assoc m k {:_eq v})) nil args)
        schema  (q/selection->schema eid selection eq-args)]
    (q/entity-accessible? eid #{:read :get})
    (first (run-search schema))))
