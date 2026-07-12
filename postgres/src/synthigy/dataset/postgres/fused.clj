(ns synthigy.dataset.postgres.fused
  "Per-level read compiler for Postgres (see memory
   project-read-path-per-level-compiler).

   ONE SQL statement PER LEVEL — never the whole tree in one statement —
   so the planner only ever sees a single junction hop and depth adds
   statements, not a mis-planned nested blob.

   Flow:
     root level — one statement: root table filtered/ordered/limited in a
                  derived table, then every direct relation as a LATERAL
                  (rows-as-json + _count + _agg fused, one scan each).
     level 2+   — for any relation whose child has relations of its own,
                  one statement per level scoped to the child _eids found
                  above, stitched back by _eid (`expand-tree`).

   Feature SQL is REUSED from the engine, not hand-rolled: predicate /
   operator / RLS-free WHERE comes from `search-stack-args` (root) and
   `relation-on-predicate` (relations) — both scoped to `:entity/as`, so
   this compiler uses each node's `:entity/as` as its SQL table alias and
   the engine fragments drop straight in.

   Invoked from the `db/ModelQueryProtocol` dispatch in
   `synthigy.dataset.postgres.query` via `requiring-resolve`. `try-search`
   / `try-get` return a 1-element vector on the fused path, or nil to
   signal fallback.

   Filtering works at every level — left relations trim the child set;
   inner relations also scope the parent (`inner-exists` adds an EXISTS to
   the parent's WHERE). Order / limit / aggregate work at every level too.

   SECURITY — RLS guards are injected per-entity via the engine's own
   `synthigy.dataset.rls/compile-guards-to-sql` (no-op for superuser or
   unconfigured entities); RBAC entity + relation access is checked
   per-entity (deny => fallback). Recursion and encrypted fields fall back
   to the engine."
  (:require [synthigy.db :as db]
            [synthigy.log :as log]
            [synthigy.dataset.id :as id]
            [synthigy.dataset.access :as access]
            [synthigy.dataset.rls :as rls]
            [synthigy.dataset.sql.query :as q]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]
            [jsonista.core :as json]))

(defn- unsupported! [reason]
  (throw (ex-info (str "fused-query unsupported: " reason)
                  {::unsupported true :reason reason})))

(defn- qi  "quote SQL identifier" [s] (str \" (str/replace (name s) "\"" "") \"))
(defn- lit "single-quoted SQL string literal" [s]
  (str \' (str/replace (name s) "'" "''") \'))

(defn- gen
  "A unique SQL alias with prefix `p`. Aliases only need uniqueness within
   one statement; `gensym` provides it."
  [p]
  (name (gensym (str "__" p))))

(def ^:private meta-keys #{:_join :_limit :_offset :_order_by :_distinct :_count :_agg})
(def ^:private agg-fns  #{:avg :sum :min :max :count})

(defn- order-by-pairs
  "Normalize an `_order_by` spec into an ordered seq of [field direction]
   pairs. Accepts a map `{:field :dir}`, a vector of `[field dir]` pairs,
   bare keywords, or a vector of single-entry maps `[{:field :dir} ...]`
   — the latter is how callers express ordered multi-column sorts, since
   map key order is not guaranteed."
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

(defn- order-clause [order-by alias]
  (for [[f d] (order-by-pairs order-by)]
    [(str alias "." (qi f)) (if (#{:desc "desc"} d) "desc nulls last" "asc nulls first")]))

(defn- has-filter?
  "True if a relation's :args carry actual field predicates."
  [args] (seq (apply dissoc args meta-keys)))

(defn- left? [args]
  (or (contains? args :_maybe)
      (boolean (#{:left :LEFT "left" "LEFT"} (:_join args)))))

(def ^:private relation-on-predicate @#'q/relation-on-predicate)

(defn- inner-exists
  "Inner-join parent scoping. For each INNER relation of `node-schema`,
   an `EXISTS (...)` predicate scoped to `node-alias` — a node is dropped
   when it has no matching child.

   Per XSQL.md line 232: `-rel` is INNER = 'drop parent if no child
   match'. Existence-of-a-child IS the filter, so we emit an EXISTS for
   every INNER relation — even when neither the relation nor any deeper
   inner relation carries an additional predicate. Predicates narrow
   the EXISTS further; the existence check itself is always present.

   Left relations never scope their parent and are skipped (they also
   stop deep-filter propagation up the chain). Returns [sql params] or
   nil when no inner relations are present."
  [node-schema node-alias]
  (let [preds
        (keep
         (fn [[_ rel]]
           (let [a (:args rel)]
             (when-not (left? a)
               (let [ex (or (:entity/as rel) (gen "ie"))
                     jx (gen "ij")
                     [fsql fdata] (when (has-filter? a)
                                    (or (relation-on-predicate rel) ["" []]))
                     [csql cdata] (inner-exists rel ex)
                     needs-target? (or (seq fsql) (seq csql))]
                 [(str "exists (select 1 from "
                       (if (:recursion? rel)
                         (str (qi (:entity/table rel)) " " ex
                              " where " ex "._eid = " node-alias "." (qi (:from/field rel)))
                         (str (qi (:relation/table rel)) " " jx
                              (when needs-target?
                                (str " join " (qi (:entity/table rel)) " " ex
                                     " on " ex "._eid = " jx "." (qi (:to/field rel))))
                              " where " jx "." (qi (:from/field rel)) " = " node-alias "._eid"))
                       (when (seq fsql) (str " and (" fsql ")"))
                       (when (seq csql) (str " and " csql)) ")")
                  (vec (concat fdata cdata))]))))
         (:relations node-schema))]
    (when (seq preds)
      [(str/join " and " (map first preds)) (vec (mapcat second preds))])))

(defn- rls-where
  "RLS predicate for `entity-id`, scoped to `alias`. Returns [sql params],
   or nil when RLS does not apply. The compiled `:rls` is read straight
   from the engine's cached runtime schema (`q/deployed-schema`) — keyed by
   entity id, an O(1) lookup. An entity with RLS enabled but no guard
   applicable to :read denies all rows (`(1=0)`)."
  [entity-id alias]
  (when (rls/should-apply-guards?)
    (when-let [{:keys [enabled guards]} (get-in (q/deployed-schema) [entity-id :rls])]
      (when enabled
        (if-let [r (and (seq guards) (rls/compile-guards-to-sql alias guards :read))]
          [(:sql r) (vec (:params r))]
          ["(1=0)" []])))))

(defn- field-entries
  "json_build_object key/value pieces for a node's scalar fields + _eid,
   referencing columns on `alias`. `_eid` is always carried (for level-
   stitching) — `coerce-node` later drops it unless the caller selected it
   — so it is excluded from the `:fields` loop to avoid a duplicate key.

   A `json`-typed (jsonb) column is cast to `::text`: otherwise
   `json_build_object` embeds it as a natively-parsed value, and then
   `coerce-node`'s per-field `<-json` decoder parses it a second time —
   a double-decode that fails on any scalar JSON value (`\"Electronics\"`
   → bare token `Electronics`). With the `::text` cast the column lands
   in the row JSON as raw JSON *text*, and the decoder parses it once."
  [schema alias]
  (let [ftypes (:field-types schema)]
    (cons (str "'_eid', " alias "._eid")
          (for [f (keys (:fields schema)) :when (not= :_eid f)]
            (str (lit f) ", " alias "." (qi f)
                 (when (= "json" (some-> (get ftypes f) name)) "::text"))))))

(defn- emit-relation
  "One relation slot (rows and/or _count and/or _agg for key `k`) as a
   single LATERAL. The child json carries ONLY scalar fields + _eid — the
   child's own relations are a separate level. Relation-level filtering is
   supported for LEFT relations (predicate trims the child set); an
   inner-join relation filter would scope the parent — unsupported here.
   Returns {:lateral :json-pair :count-pair :agg-pair :params}."
  [k schema row-alias]
  (let [rel (get-in schema [:relations k])
        cnt (get-in schema [:_count k])
        agg (get-in schema [:_agg k])
        ptr (or rel cnt agg)
        _ (when (and (:relation ptr)
                     (not (access/relation-allows? (:relation ptr) #{:read})))
            (throw (ex-info (str "Relation access forbidden: " k)
                            {:code "RELATION_FORBIDDEN" :relation (:relation ptr)})))
        junction   (:relation/table ptr)
        from-field (:from/field ptr)
        to-field   (:to/field ptr)
        child-tbl  (:entity/table ptr)
        rtype      (:type ptr)
        ;; `rel`, `cnt`, and `agg` are independent schema entries for the
        ;; same key `k` — a relation requested ONLY via `_count`/`_agg`
        ;; (no parallel plain `:relations` pull) has no `rel` at all, but
        ;; still carries its own `:args` predicate on `cnt`/`agg`. Read the
        ;; filter off whichever entry is actually present (`ptr`, already
        ;; unified above for table/field metadata) instead of hardcoding
        ;; `rel` — otherwise a count-only/agg-only relation's filter is
        ;; silently dropped (see project_agg_filter_dropped_bug memory).
        rel-args   (:args ptr)
        ;; Must match whichever entry `rel-args`/the predicate come from
        ;; (`ptr`, not `rel`) — `relation-on-predicate`/`query-selection->sql`
        ;; bake the schema's OWN `:entity/as` into the predicate text via
        ;; `prefix`, so the JOIN alias bound below must be the same value
        ;; or Postgres can't resolve it ("missing FROM-clause entry").
        ex (or (:entity/as ptr) (gen "e"))
        jx (gen "j") cx (gen "c")
        [rls-sql rls-data] (rls-where (:to ptr) ex)
        [filter-sql filter-data]
        (when (has-filter? rel-args)
          (or (relation-on-predicate ptr) ["" []]))
        [ie-sql ie-data] (inner-exists rel ex)
        child-source (str (qi junction) " " jx
                          " join " (qi child-tbl) " " ex
                          " on " ex "._eid = " jx "." (qi to-field))
        correlate (str jx "." (qi from-field) " = " row-alias "._eid"
                       (when (seq filter-sql) (str " and (" filter-sql ")"))
                       (when (seq ie-sql)  (str " and " ie-sql))
                       (when (seq rls-sql) (str " and (" rls-sql ")")))
        child-obj  (str "json_build_object(" (str/join ", " (field-entries rel ex)) ")")
        agg-spec   (when agg (select-keys agg agg-fns))
        agg-fields (distinct (for [[_ fm] agg-spec [f _] fm] f))
        agg-pair   (fn [] (str (lit k) ", json_build_object("
                               (str/join ", "
                                         (for [[fnk fm] agg-spec]
                                           (str (lit fnk) ", json_build_object("
                                                (str/join ", " (for [[f _] fm]
                                                                 (str (lit f) ", " (gensym))))
                                                ")")))
                               ")"))
        lat (gen "lat")
        ords (order-clause (:_order_by rel-args) ex)
        limit (:_limit rel-args)
        ord-cols (map-indexed (fn [i [e d]] [(str "__od" i) e d]) ords)
        rn-over (if (seq ords)
                  (str "order by " (str/join "," (map (fn [[e d]] (str e " " d)) ords)))
                  "")]
    (if (some? rel)
      (let [inner (str "select " child-obj " as __v"
                       (apply str (for [[a e _] ord-cols] (str ", " e " as " a)))
                       (apply str (for [f agg-fields] (str ", " ex "." (qi f) " as __af_" (name f))))
                       (when limit (str ", row_number() over (" rn-over ") as __rn"))
                       " from " child-source " where " correlate)
            agg-by (if (seq ord-cols)
                     (str " order by " (str/join "," (map (fn [[a _ d]] (str cx "." a " " d)) ord-cols)))
                     "")
            filt (when limit (str " filter (where " cx ".__rn <= " (long limit) ")"))
            outer (str "select coalesce(json_agg(" cx ".__v" agg-by ")" filt
                       ", '[]'::json) as __rows"
                       (when cnt ", count(*) as __cnt")
                       (apply str (for [[fnk fm] agg-spec [f _] fm]
                                    (str ", " (name fnk) "(" cx ".__af_" (name f) ") as __ag_"
                                         (name fnk) "_" (name f))))
                       " from (" inner ") " cx)]
        {:lateral (str "left join lateral (" outer ") " lat " on true")
         :json-pair  (str (lit k) ", " lat ".__rows")
         :count-pair (when cnt (str (lit k) ", " lat ".__cnt"))
         :agg-pair   (when agg
                       (str (lit k) ", json_build_object("
                            (str/join ", "
                                      (for [[fnk fm] agg-spec]
                                        (str (lit fnk) ", json_build_object("
                                             (str/join ", " (for [[f _] fm]
                                                              (str (lit f) ", " lat ".__ag_"
                                                                   (name fnk) "_" (name f))))
                                             ")")))
                            ")"))
         :params (vec (concat filter-data ie-data rls-data))})
      (let [outer (str "select"
                       (when cnt " count(*) as __cnt")
                       (when (and cnt agg) ",")
                       (when agg
                         (str " "
                              (str/join ", "
                                        (for [[fnk fm] agg-spec [f _] fm]
                                          (str (name fnk) "(" ex "." (qi f) ") as __ag_"
                                               (name fnk) "_" (name f))))))
                       " from " child-source " where " correlate)]
        {:lateral (str "left join lateral (" outer ") " lat " on true")
         :count-pair (when cnt (str (lit k) ", " lat ".__cnt"))
         :agg-pair   (when agg
                       (str (lit k) ", json_build_object("
                            (str/join ", "
                                      (for [[fnk fm] agg-spec]
                                        (str (lit fnk) ", json_build_object("
                                             (str/join ", " (for [[f _] fm]
                                                              (str (lit f) ", " lat ".__ag_"
                                                                   (name fnk) "_" (name f))))
                                             ")")))
                            ")"))
         :params (vec (concat filter-data ie-data rls-data))}))))

(defn- emit-one-relation
  "A :one relation as a LATERAL — kept separate so the :many path stays
   readable. Child json = scalar fields + _eid; LIMIT 1."
  [k schema row-alias]
  (let [rel (get-in schema [:relations k])]
    (when (or (get-in schema [:_count k]) (get-in schema [:_agg k]))
      (unsupported! (str ":one with _count/_agg on " k)))
    (when (and (:relation rel)
               (not (access/relation-allows? (:relation rel) #{:read})))
      (throw (ex-info (str "Relation access forbidden: " k)
                      {:code "RELATION_FORBIDDEN" :relation (:relation rel)})))
    (let [ex (or (:entity/as rel) (gen "e"))
          jx (gen "j") lat (gen "lat")
          [filter-sql filter-data]
          (when (has-filter? (:args rel))
            (or (relation-on-predicate rel) ["" []]))
          [ie-sql ie-data] (inner-exists rel ex)
          [rls-sql rls-data] (rls-where (:to rel) ex)
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

(defn- emit-node
  "One node's level: [laterals-sql json-entries params]. Encrypted fields
   are decrypted post-parse in `coerce-node`."
  [schema row-alias include-fields?]
  (let [rel-keys (distinct (concat (keys (:relations schema))
                                   (keys (:_count schema))
                                   (keys (:_agg schema))))
        rels (map (fn [k]
                    (if (= :one (get-in schema [:relations k :type]))
                      (emit-one-relation k schema row-alias)
                      (emit-relation k schema row-alias)))
                  rel-keys)
        count-pairs (keep :count-pair rels)
        agg-pairs   (keep :agg-pair rels)
        entries (concat
                 (when include-fields? (field-entries schema row-alias))
                 (keep :json-pair rels)
                 (when (seq count-pairs)
                   [(str "'_count', json_build_object(" (str/join ", " count-pairs) ")")])
                 (when (seq agg-pairs)
                   [(str "'_agg', json_build_object(" (str/join ", " agg-pairs) ")")]))]
    [(str/join "\n" (map :lateral rels))
     entries
     (vec (mapcat :params rels))]))

(defn- compile-root-level
  "Root level, one statement: the root table filtered/ordered/limited in a
   derived table (roots folded in — no separate roots query), then every
   direct relation as a LATERAL. Yields a json array of root objects."
  [schema]
  (let [eas (:entity/as schema)               ; engine-fragment alias
        rt  (gen "rt")                        ; derived-table alias
        [laterals entries lat-params] (emit-node schema rt true)
        [where where-data] (q/search-stack-args (dissoc schema :relations))
        [ie-sql ie-data] (inner-exists schema eas)
        [rls-sql rls-data] (rls-where (:entity schema) eas)
        where-all (str/join " and " (remove str/blank? [where ie-sql rls-sql]))
        args (:args schema)
        ob   (:_order_by args)
        ords-inner (order-clause ob eas)
        ords-rt    (order-clause ob rt)
        fk-cols (keep (fn [[_ r]]
                        (when (or (:recursion? r) (:ref? r))
                          (:from/field r)))
                      (:relations schema))
        cols (distinct (remove #{:_eid}
                               (map #(if (keyword? %) % (keyword %))
                                    (concat (keys (:fields schema))
                                            (map first (order-by-pairs ob))
                                            fk-cols))))
        dist (q/distinct->sql schema)
        inner (str "select " (when dist (str dist " ")) eas "._eid"
                   (apply str (for [c cols] (str ", " eas "." (qi c) " as " (qi c))))
                   " from " (qi (:entity/table schema)) " " eas
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
     (vec (concat where-data ie-data rls-data lat-params))]))

(defn- compile-sub-level
  "A non-root level: every relation of `schema`, scoped to `parent-eids`.
   Yields rows of (__pe parent-eid, __rels json). Parent rows come from the
   entity table itself (not a VALUES list) so a recursion relation can
   correlate its LATERAL on the parent's self-FK column."
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

(defn- parse-json [v]
  (cond
    (nil? v)    nil
    (string? v) (json/read-value v json-mapper)
    :else       (json/read-value (.getValue ^org.postgresql.util.PGobject v) json-mapper)))

(defn- node-has-children? [schema]
  (or (seq (:relations schema)) (seq (:_count schema)) (seq (:_agg schema))))

(defn- expand-tree
  "Parent objects already have their DIRECT relations populated; pull every
   relation whose child has relations of its own — one statement per level
   — and stitch results back by _eid."
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

(defn- coerce-agg
  "Match the engine's `_agg` BigDecimal scale. The engine bigdec's whatever
   JDBC handed it: `avg` returns a Double (`(bigdec (double v))` keeps the
   `.0` scale), `count` stays a Long, `sum`/`min`/`max` -> `(bigdec v)`.
   `_agg` shape is {relation-key {agg-fn {field value}}}."
  [agg]
  (reduce-kv
   (fn [m relk aggmap]
     (assoc m relk
            (reduce-kv
             (fn [m2 fnk fieldmap]
               (assoc m2 fnk
                      (reduce-kv
                       (fn [m3 fld v]
                         (assoc m3 fld
                                (cond
                                  (not (number? v)) v
                                  (= :count fnk)    v
                                  (= :avg fnk)      (bigdec (double v))
                                  :else             (bigdec v))))
                       {} fieldmap)))
             {} aggmap)))
   {} agg))

(defn- coerce-node
  "Match the engine's result shape exactly — the same query must return
   the same thing on the fused path and the old engine:
     * float-typed fields  -> Double (JSON collapses e.g. 5.0 to 5);
     * encrypted fields    -> decrypted via the engine's per-field decoder;
     * `_agg` leaves       -> BigDecimal at the engine's scale (`coerce-agg`);
     * an empty relation   -> the key is DROPPED (engine omits it).
   Recurses through relations per `schema`."
  [schema obj]
  (when obj
    (let [o (reduce (fn [m [f t]]
                      (if (and (some? (get m f)) (float-types (some-> t name)))
                        (update m f double)
                        m))
                    obj (:field-types schema))
          o (reduce (fn [m [f dec]]
                      (if (and (fn? dec) (some? (get m f)))
                        (update m f dec)
                        m))
                    o (:decoders schema))
          o (if (:_agg o) (update o :_agg coerce-agg) o)
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

(defn- run-search [schema]
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
        (mapv #(coerce-node schema %) (expand-tree con schema objs))))))

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
