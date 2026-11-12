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

(ns synthigy.dataset.sql.query
  "Database-agnostic SQL query core: schema caching, 
  ERD schema generation, selection compilation, SQL emission, 
  the pull, and the write path. Backend-specific code belongs 
  in synthigy.dataset.postgres.query / .sqlite.query / .mysql.query."
  (:require
   [buddy.hashers :as hashers]
   [clojure.core.cache :as cache]
   [clojure.set]
   [clojure.string :as str]
   [synthigy.log :as log]
   clojure.zip
   [next.jdbc :as jdbc]
   [nano-id.core :refer [nano-id]]
   [synthigy.dataset
    :refer [deployed-relation deployed-entity deployed-model]]
   [synthigy.dataset.access :as access :refer [*operation-rules*]]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.encryption :as denc]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.rls :as rls-runtime]
   [synthigy.dataset.sql.naming :refer [normalize-name
                                        normalized-enum-value
                                        entity->table-name
                                        relation->table-name
                                        entity->relation-field]]
   [synthigy.dataset.sql.protocol :as proto]
   [synthigy.dataset.sql.rls :as rls]
   [synthigy.dataset.sql.schema :as schema]
   [synthigy.db :as db :refer [*db*]]
   [synthigy.db.sql :as sql]
   synthigy.transit))

;;; ============================================================================
;;; Operation Context (Dynamic Vars)
;;; ============================================================================

;;; ============================================================================
;;; Fetch concurrency (Dynamic Vars + macros)
;;; ============================================================================
;; :parallel must never run inside a write transaction — its readers miss uncommitted writes.

(def ^:dynamic *fetch-mode*
  "Sibling-fetch concurrency for `pull-cursors`: :serial or :parallel, bound by backend protocol extensions."
  :serial)

(def ^:dynamic ^java.sql.Connection *fetch-con*
  "Connection for the current fetch task — use instead of the lexical `con` inside pull-cursors bodies."
  nil)

(defmacro fetch-on-conn
  "Schedule a leaf SQL fetch; :parallel opens and releases its own connection."
  [& body]
  `(case *fetch-mode*
     :serial   (delay ~@body)
     :parallel (future
                 (with-open [c# (jdbc/get-connection (:datasource *db*))]
                   (binding [*fetch-con* c#]
                     ~@body)))))

(defmacro parallel-task
  "Schedule a recursive pull task; acquires no connection itself."
  [& body]
  `(case *fetch-mode*
     :serial   (delay  ~@body)
     :parallel (future ~@body)))

;;; ============================================================================
;;; Schema Caching (Database-Agnostic)
;;; ============================================================================

(defonce ^:private _indexes (atom nil))
(defonce ^:private _template-cache
  (atom (cache/ttl-cache-factory {} :ttl (* 30 60 1000))))

(defn normalize-entity-name
  "Normalize an entity/label reference for index lookup."
  [name]
  (normalize-name name))

(defn build-relation-index
  "Builds {relation-uuid -> {:entity entity-name :label to-label}} from the deployed model."
  []
  (when-let [model (deployed-model)]
    (into {}
          (for [entity (core/get-entities model)
                :let [entity-name (:name entity)]
                :when entity-name
                relation (core/focus-entity-relations model entity)
                :let [relation-id (id/extract relation)
                      label (:to-label relation)]
                :when (and relation-id label)]
            [relation-id {:entity (normalize-entity-name entity-name)
                          :label (normalize-entity-name label)}]))))

(defn build-skin-reverse
  "Builds {skin -> canonical-key} from every field's :skins across the schema."
  [s]
  (let [by-skin (group-by first
                          (for [[_ entity] s
                                [_ {:keys [key skins]}] (:fields entity)
                                :when (and key skins)
                                [_ skin] skins]
                            [skin key]))]
    (doseq [[skin g] by-skin
            :when (> (count (distinct (map second g))) 1)]
      (log/warn {:id ::ambiguous-skin
                 :data {:action :deploying :subject :dataset-schema
                        :skin skin :keys (distinct (map second g))}}
                "Skin resolves to multiple keys — dropped from inbound lookup"))
    (into {} (for [[skin g] by-skin
                   :when (= 1 (count (distinct (map second g))))]
               [skin (second (first g))]))))


(comment
  (-> (deployed-schema) vals first :fields vals first :skins)
  (time (:satisfaction (build-skin-reverse (deployed-schema))))
  (map (comp :skins :fields) (vals (deployed-schema))))


(defn build-inbound-cast
  "Per-entity {received-key -> canonical-key} skins cast for inbound data."
  [{:keys [fields relations recursions]}]
  (let [pairs (for [{:keys [key skins]} (vals fields)
                    :when key
                    variant (cons key (vals skins))]
                [variant key])
        ambiguous (into #{}
                        (keep (fn [[variant vs]]
                                (when (> (count (distinct (map second vs))) 1)
                                  variant)))
                        (group-by first pairs))]
    (into {}
          (concat
           (remove (comp ambiguous first) pairs)
           (map (fn [k] [k k]) (concat (keys relations) recursions))
           [[:xid :xid] [:euuid :euuid] [:_eid :_eid]]))))

(defn deploy-schema
  "Caches the runtime schema and rebuilds the entity/relation/attribute-key/
   skin indexes; call on every model deploy."
  [s]
  (let [s (reduce-kv
           (fn [m id e] (assoc m id (assoc e :inbound-cast (build-inbound-cast e))))
           {} s)]
    (schema/set-deployed-schema! s)
    (reset! _template-cache (cache/ttl-cache-factory {} :ttl (* 30 60 1000)))
    (dk/set-skin-index! (build-skin-reverse s))
    (reset! _indexes
            {:entity (into {}
                           (for [[id {:keys [name]}] s
                                 :when name]
                             [(normalize-entity-name name) id]))
             :relation (build-relation-index)
             :attribute-key (into {}
                                  (for [[_ {:keys [fields]}] s
                                        [attr-id {field-key :key}] fields
                                        :when (and attr-id field-key)]
                                    [(keyword (str attr-id)) field-key]))})))

(defn deployed-schema
  "Returns the currently deployed runtime schema, or nil."
  []
  (schema/deployed-schema))

(defn cache-template
  "Cache a resolved template and return it."
  [template-key resolved]
  (swap! _template-cache assoc template-key resolved)
  resolved)

(defn cached-template
  "Get a cached template resolution, or nil if expired/missing."
  [template-key]
  (let [c @_template-cache]
    (when (cache/has? c template-key)
      (swap! _template-cache cache/hit template-key)
      (cache/lookup c template-key))))

(defn entity-index
  "Returns the {normalized-entity-name -> entity-id} index."
  []
  (:entity @_indexes))

(defn levenshtein
  "Compute Levenshtein edit distance between two strings."
  [a b]
  (let [m (count a) n (count b)]
    (cond
      (zero? m) n
      (zero? n) m
      :else
      (let [d (make-array Integer/TYPE (inc m) (inc n))]
        (dotimes [i (inc m)] (aset d i 0 (int i)))
        (dotimes [j (inc n)] (aset d 0 j (int j)))
        (dotimes [i m]
          (dotimes [j n]
            (let [cost (if (= (.charAt a i) (.charAt b j)) 0 1)]
              (aset d (inc i) (inc j)
                    (int (min (inc (aget d i (inc j)))
                              (inc (aget d (inc i) j))
                              (+ cost (aget d i j))))))))
        (aget d m n)))))

(defn suggest-similar
  "Pick the closest match from `candidates` for `target`, or nil."
  [target candidates]
  (when (and (string? target) (seq candidates))
    (let [[best dist] (reduce
                       (fn [[best best-d] c]
                         (let [d (levenshtein target c)]
                           (if (< d best-d) [c d] [best best-d])))
                       [nil Integer/MAX_VALUE]
                       candidates)]
      (when (and best
                 (<= dist 3)
                 (<= (count best) (* 2 (count target))))
        best))))

(defn resolve-entity
  "Resolves a user-provided entity reference (raw ID or human-readable name) to
   its entity ID; throws if not found."
  [entity-ref]
  (cond
    (contains? (schema/deployed-schema) entity-ref) entity-ref
    (uuid? entity-ref)
    (throw (ex-info (str "Unknown entity: " entity-ref)
                    {:code "UNKNOWN_ENTITY" :entity entity-ref}))
    (string? entity-ref)
    (let [normalized (normalize-entity-name entity-ref)
          entity-idx (:entity @_indexes)]
      (or (get entity-idx normalized)
          (let [hint (suggest-similar normalized (keys entity-idx))]
            (throw (ex-info (cond-> (str "Unknown entity: " entity-ref)
                              hint (str ". Did you mean \"" hint "\"?"))
                            (cond-> {:code "UNKNOWN_ENTITY" :entity entity-ref}
                              hint (assoc :hint hint)))))))
    :else
    (throw (ex-info (str "Invalid entity reference: " entity-ref)
                    {:code "UNKNOWN_ENTITY" :entity entity-ref}))))

(defn relation-index
  "Returns {relation-uuid -> {:entity name :label label}} reverse index."
  []
  (:relation @_indexes))

(defn attribute-key-index
  "Returns the {attr-xid-keyword -> field-keyword} map across all entities."
  []
  (:attribute-key @_indexes))

(defn resolve-relation
  "Resolves entity name + relation label to relation UUID, or throws."
  [entity-name label]
  (let [entity-id (resolve-entity entity-name)
        model (deployed-model)
        entity (core/get-entity model entity-id)
        relations (core/focus-entity-relations model entity)
        normalized-label (normalize-entity-name label)]
    (or (some (fn [rel]
                (when (= normalized-label
                         (normalize-entity-name (or (:to-label rel) "")))
                  (id/extract rel)))
              relations)
        (let [available (mapv (fn [rel]
                                (normalize-entity-name (or (:to-label rel) "")))
                              relations)
              hint (suggest-similar normalized-label available)]
          (throw (ex-info (cond-> (str "Unknown relation: " entity-name "." label)
                            hint (str ". Did you mean \"" entity-name "." hint "\"?")
                            (seq available) (str
                                             " Available on " entity-name ": "
                                             (clojure.string/join ", " (sort available))))
                          (cond-> {:code "UNKNOWN_RELATION"
                                   :entity entity-name
                                   :relation label
                                   :available available}
                            hint (assoc :hint hint))))))))

(defn deployed-schema-entity
  "Gets a specific entity from the deployed schema by entity id"
  [entity-id]
  (schema/deployed-schema-entity entity-id))

(defn tree-column
  "The quoted self-FK column of tree relation `on`; throws UNKNOWN_TREE_RELATION for anything else."
  [entity-id on]
  (let [k (keyword on)
        {:keys [recursions]} (deployed-schema-entity entity-id)]
    (if (contains? recursions k)
      (str \" (name k) \")
      (throw (ex-info (str "`" (name on) "` is not a tree relation of this entity")
                      {:code "UNKNOWN_TREE_RELATION"
                       :relation (name on)
                       :available (sort (map name recursions))})))))

(declare read-scope-guard)

(defn tree-source
  "[from params]: `table`, or the rows of it the caller may read, for a recursive walk to stop at."
  [entity-id table]
  (if-let [{:keys [sql params]} (read-scope-guard entity-id table)]
    [(format "(select * from \"%s\" where %s)" table sql) (vec params)]
    [table []]))

;;; ============================================================================
;;; Temporary Key Generation
;;; ============================================================================

(defn tmp-key
  "Generates a temporary key for entity tracking during mutations."
  []
  (nano-id 10))

;;; ============================================================================
;;; Delete — shared shape/identity guards (backends call these, then do SQL)
;;; ============================================================================

(def delete-batch-limit
  "Max xids per multirow delete — above this, use purge."
  1000)

(declare reference-constraint-keys)

(defn delete-unique-args
  "A single-row delete's _eq predicate: the id key when given, else the first unique group given in full; throws UNIQUE_KEY_REQUIRED."
  [entity-schema args]
  (let [fields (:fields entity-schema)
        groups (keep (fn [group] (not-empty (mapv #(get-in fields [% :key]) group)))
                     ((comp :unique :constraints) entity-schema))
        given (into #{} (keep (fn [[k v]] (when (some? v) k))) args)
        filtered-args (some->> (reference-constraint-keys groups given) (select-keys args))]
    (when (empty? filtered-args)
      (throw (ex-info
              (str "Delete on " (:name entity-schema) " needs a non-nil unique key")
              {:code "UNIQUE_KEY_REQUIRED"
               :rule "delete_identity"
               :entity-name (:name entity-schema)
               :hint (str/join ", " (map #(str/join " + " (map name %)) (cons [(id/key)] groups)))})))
    {:filtered-args filtered-args
     :predicate-args (reduce-kv (fn [acc k v] (assoc acc k {:_eq v})) nil filtered-args)}))

(defn assert-single-match!
  "Throws NON_UNIQUE_MATCH when a single-row delete predicate matched more than one row."
  [entity-schema filtered-args eids]
  (when (> (count eids) 1)
    (throw (ex-info
            (str "Delete predicate on " (:name entity-schema) " matched "
                 (count eids) " rows, expected at most 1")
            {:code "NON_UNIQUE_MATCH"
             :rule "delete_identity"
             :entity-name (:name entity-schema)
             :args filtered-args
             :matched (count eids)}))))

(defn check-delete-batch!
  "Validates and dedupes a multirow delete's xid vector."
  [xids]
  (doseq [x xids]
    (when-not (and (string? x) (not (str/blank? x)))
      (throw (ex-info "Multirow delete takes a vector of non-blank xid strings"
                      {:code "BAD_DATA_SHAPE"
                       :rule "delete_data"
                       :hint "natural keys -> single map; predicates -> purge"}))))
  (let [deduped (vec (distinct xids))]
    (when (> (count deduped) delete-batch-limit)
      (throw (ex-info (str "Multirow delete takes at most " delete-batch-limit " xids")
                      {:code "DELETE_BATCH_LIMIT"
                       :rule "delete_data"
                       :limit delete-batch-limit
                       :hint "use purge with {:xid {:_in [...]}} for more"})))
    deduped))

;;; ============================================================================
;;; Type Definitions
;;; ============================================================================

(def scalar-types
  "Scalar field types that can be selected"
  #{"boolean" "string" "int" "float" "timestamp" "enum"
    "json" "uuid" "encrypted" "hashed" "transit" "avatar"})

(defn wrap-basic-fields
  "Formats field names as SQL identifiers, optionally table-prefixed, always appending `_eid`."
  ([fields] (wrap-basic-fields fields nil))
  ([fields prefix]
   (if (not-empty prefix)
     (map #(str prefix \. \" (if (= (id/key) %) (id/field) (name %)) \")
          (conj fields "_eid"))
     (map #(str \" (if (= (id/key) %) (id/field) (name %)) \")
          (conj fields "_eid")))))

(defn extend-fields
  "Builds a comma-separated SQL field list via `wrap-basic-fields`."
  ([fields] (extend-fields fields nil))
  ([fields prefix]
   (clojure.string/join ", " (wrap-basic-fields fields prefix))))

(defn schema-zipper
  "Zipper over a schema tree's :relations, rooted at a synthetic ::ROOT entry."
  [root]
  (letfn [(branch? [[_ {:keys [relations]}]]
            (not-empty relations))
          (get-children [[_ {:keys [relations]}]]
            relations)
          (make-node [[k node] children]
            [k (update node :relations (fnil conj []) children)])]
    (clojure.zip/zipper
     branch?
     get-children
     make-node
     (clojure.lang.MapEntry. ::ROOT root))))

;;; ============================================================================
;;; Access Control (Database-Agnostic)
;;; ============================================================================

(defn throw-relation
  "Throws access-denied for relation access."
  [id [from-id _]]
  (let [{{from :name} :from
         {to :name} :to
         :keys [from-label to-label]
         :as relation} (deployed-relation id)
        [from from-label to to-label] (if (= from-id (id/extract (:from relation)))
                                        [from to-label to from-label]
                                        [to from-label from to-label])]
    (throw
     (ex-info
      (format
       "You don't have sufficient privileges to access relation [%s]%s -> %s[%s]"
       from from-label to-label to)
      {:type ::enforce-search-access
       :code "RELATION_FORBIDDEN"
       :relation id
       :roles (access/role-ids)}))))

(defn throw-entity
  "Throws access-denied for entity access."
  [id]
  (let [{entity-name :name} (deployed-entity id)]
    (throw
     (ex-info
      (format
       "You don't have sufficient privileges to access entity '%s'"
       entity-name)
      {:type ::enforce-search-access
       :code "ENTITY_FORBIDDEN"
       :entity id
       :entity-name entity-name
       :roles (access/role-ids)}))))

(defn entity-accessible?
  "True if entity is accessible with given scopes; throws otherwise."
  [entity-id scopes]
  (when-not (access/entity-allows? entity-id scopes)
    (let [{entity-name :name} (deployed-entity entity-id)]
      (log/info {:id   :synthigy.iam.access/access-denied
                 :data {:action  :denied
                        :subject :request
                        :kind    :entity
                        :entity  entity-name
                        :scopes  (vec scopes)
                        :roles   (vec (access/role-ids))}}
                "RBAC denied entity access"))
    (throw-entity entity-id))
  true)

(defn relation-accessible?
  "True if relation is accessible with given direction and scope; throws
   otherwise."
  [relation direction scope]
  (let [allowed? (access/relation-allows? relation direction scope)]
    (when-not allowed?
      (let [{{from :name} :from {to :name} :to} (deployed-relation relation)]
        (log/info {:id   :synthigy.iam.access/access-denied
                   :data {:action    :denied
                          :subject   :request
                          :kind      :relation
                          :from      from
                          :to        to
                          :direction (mapv str direction)
                          :scopes    (vec scope)
                          :roles     (vec (access/role-ids))}}
                  "RBAC denied relation access"))
      (throw-relation relation direction))
    true))

(defn throw-attribute-write-denied
  "Throws ATTRIBUTE_FORBIDDEN for a denied attribute write."
  [entity-id attribute-id]
  (let [{entity-name :name} (deployed-entity entity-id)
        attribute-name (some->> (:attributes (deployed-entity entity-id))
                                (some #(when (= (id/extract %) attribute-id) %))
                                :name)]
    (throw
     (ex-info
      (format
       "You don't have sufficient privileges to write attribute '%s' on entity '%s'"
       attribute-name entity-name)
      {:type ::attribute-write-denied
       :code "ATTRIBUTE_FORBIDDEN"
       :entity entity-id
       :entity-name entity-name
       :attribute attribute-id
       :attribute-name attribute-name
       :roles (access/role-ids)}))))

(defn attribute-writable?
  "Throws unless `attribute-id` on `entity-id` may be WRITTEN by the current
   role."
  [entity-id attribute-id]
  (when-not (access/attribute-allows? entity-id attribute-id :write)
    (log/info {:id   :synthigy.iam.access/access-denied
               :data {:action    :denied
                      :subject   :request
                      :kind      :attribute
                      :entity    entity-id
                      :attribute attribute-id
                      :roles     (vec (access/role-ids))}}
              "RBAC denied attribute write")
    (throw-attribute-write-denied entity-id attribute-id))
  true)

;;; ============================================================================
;;; Link-only classification (shared: access gate + storage)
;;; ============================================================================

(defn reference-constraint-keys
  "Constraint key vector identifying a write row keyed by `ks`, or nil."
  [constraints ks]
  (if (contains? ks (id/key))
    [(id/key)]
    (some #(when (every? ks %) %) constraints)))

(defn link-only?
  "True when a write row carries only identity/constraint keys; the one predicate shared by the access gate and storage."
  [entity-schema constraints ks]
  (let [declared (when entity-schema
                   (->> (:fields entity-schema)
                        (keep (fn [[k v]] (when-not (or (keyword? k) (:stamp v)) (:key v))))
                        set))
        recursions (:recursions entity-schema)
        ckeys (reference-constraint-keys constraints ks)]
    (boolean
     (and (some? declared)
          (not-empty ckeys)
          (not-any? (fn [k]
                      (and (or (contains? declared k)
                               (contains? recursions k))
                           (not-any? #(= % k) ckeys)))
                    (conj ks (id/key)))))))

(defn throw-reference-not-found
  "Throws when a payload reference resolves to no row the caller can see."
  [{:keys [entity/mapping constraint]} table row relation]
  (let [entity-id (get mapping table)
        {entity-name :name} (when entity-id (deployed-entity entity-id))
        ident-keys (into #{(id/key)} (flatten (get constraint table)))
        ident (into {} (filter (comp ident-keys key)) row)]
    (throw
     (ex-info
      (format "Referenced '%s' not found: %s"
              (or entity-name table)
              (pr-str ident))
      (cond-> {:type ::reference-not-found
               :code "REF_NOT_FOUND"
               :entity entity-id
               :table table
               :keys ident}
        relation (assoc :relation relation))))))

;;; ============================================================================
;;; Type Encoding/Decoding (Database-Agnostic with Protocol)
;;; ============================================================================

(defn entity-serde
  "Builds {:encoders :decoders} maps for entity fields via the TypeCodec
   protocol."
  [entity-id]
  (let [{:keys [fields field->attribute]} (get (deployed-schema) entity-id)
        field->type (reduce
                     (fn [result {f :key
                                  t :type
                                  e :enum/name}]
                       (assoc result f (or e t)))
                     nil
                     (vals fields))

        encoders (assoc
                  (reduce
                   (fn [result field]
                     (let [t (get field->type field)]
                       (case t
                         ("boolean" "string" "int" "float" "json"
                                    "timeperiod" "currency"
                                    "uuid" "avatar" "hashed" "transit" nil) result
                         (assoc result field
                                (fn [v]
                                  (proto/encode *db* t v))))))
                   nil
                   (keys field->type))
                  (id/key) id/coerce-arg)

        ;; capture *db* as a value — decoders may run in futures without the binding
        db *db*
        decoders (reduce
                  (fn [r k]
                    (let [field-type (field->type k)
                          transform (case field-type
                                      "uuid" (fn [data _] (proto/decode db "uuid" data))
                                      "encrypted" (fn [data _] (denc/unseal-cell data))
                                      "json" (fn [data _] (proto/decode db "json" data))
                                      "boolean" (fn [data _] (proto/decode db "boolean" data))
                                      "enum" (fn [data _] (proto/decode db "enum" data))
                                      "timestamp" (fn [data _] (proto/decode db "timestamp" data))
                                      (if (and field-type (not (scalar-types field-type)))
                                        (fn [data _] (proto/decode db field-type data))
                                        nil))]
                      (if transform
                        (assoc r k transform)
                        r)))
                  nil
                  (keys field->type))]
    (hash-map :encoders encoders
              :decoders decoders)))

;;; ============================================================================
;;; Selection Processing
;;; ============================================================================

(defn flatten-selection
  "Strips entity namespace from selection keys (:User/name -> :name)."
  [s]
  (reduce
   (fn [r [k v]]
     (assoc r (-> k name keyword) v))
   nil
   s))

(defn distribute-fields
  "Splits entity fields into {:field scalars :reference relation-typed fields}."
  [fields]
  (group-by
   (fn [{t :type}]
     (if (scalar-types t)
       :field
       :reference))
   fields))

;;; ============================================================================
;;; Response Construction (Database-Agnostic)
;;; ============================================================================

(defn construct-response
  "Rebuilds the nested object graph from flat DB records ([table id] references,
   self references resolved in a second pass)."
  [{:keys [entity/table recursions]
    :as schema} {:keys [ops]
                 :as db} found-records]
  (letfn [(reference? [value]
            (vector? value))
          (list-reference? [value]
            (and
             (vector? value)
             (every? vector? value)))
          (narrow [schema]
            (concat
             (keys (:fields schema))
             (keys (:relations schema))
             recursions))
          (pull-reference [[table id] schema cursor]
            (let [data (merge
                        (select-keys (get-in db [table id]) (narrow schema))
                        (get-in ops [cursor id]))
                  data' (reduce-kv
                         (fn [data' k v]
                           (cond
                             (list-reference? v)
                             (assoc data' k (mapv
                                             #(pull-reference % (get-in schema [:relations k]) (conj cursor k))
                                             (distinct v)))
                               ;;
                             (and (recursions k) (not= v id) (not= v [table id]))
                             (let [child-schema (or (get-in schema [:relations k]) schema)]
                               (if (vector? v)
                                 (assoc data' k (pull-reference v child-schema cursor))
                                 (assoc data' k (pull-reference [table v] child-schema cursor))))
                              ;;
                             (and (not (recursions k)) (reference? v))
                             (assoc data' k (pull-reference v (get-in schema [:relations k]) (conj cursor k)))
                              ;;
                             (= v [table id])
                             (assoc data' k ::self)
                              ;;
                             :else data'))
                         data
                         data)]
              (reduce-kv
               (fn [data k v]
                 (if (not= v ::self) data
                     (assoc data k data)))
               data'
               data')))]
    (let [final (mapv
                 (fn [id]
                   (pull-reference [table id] schema [::ROOT]))
                 (reduce
                  (fn [r root]
                    (if (get-in db [table root])
                      (conj r root)
                      r))
                  []
                  (get
                   found-records
                   (keyword ((sql/get-label-fn core/*return-type*) (:entity/as schema)))
                   (keys (get db table)))))]
      final)))

;;; ============================================================================
;;; Cursor System (Schema Tree Navigation)
;;; ============================================================================

(defn relations-cursor
  "Converts a cursor path (vector of relation keys) to a get-in path on the
   schema."
  [cursor]
  (if (empty? cursor)
    []
    (vec (mapcat (fn [k] [:relations k]) cursor))))

(defn schema->cursors
  "Generates all cursor paths present in the schema tree."
  ([schema]
   (schema->cursors
    (when-let [root-rels (keys (:relations schema))]
      (mapv vector root-rels))
    schema))
  ([cursors schema]
   (reduce
    (fn [acc cursor]
      (let [node (get-in schema (relations-cursor cursor))
            child-relations (:relations node)]
        (if (not-empty child-relations)
          (into
           (conj acc cursor)
           (mapcat #(schema->cursors [(conj cursor %)] schema)
                   (keys child-relations)))
          (if (or (:fields node) (:ops node))
            (conj acc cursor)
            acc))))
    []
    cursors)))

;;; ============================================================================
;;; Mutation Helper Functions (Database-Agnostic)
;;; ============================================================================

(defn group-entity-rows
  "Groups entity rows by their field-key set so same-shaped rows batch into one
   INSERT."
  [tmp-rows]
  (reduce-kv
   (fn [result tmp-id data]
     (update result (set (keys data)) (fnil conj []) [data tmp-id]))
   nil
   tmp-rows))

(defn stamp-principal
  "`analysis` with the bound principal in every user stamp of `entity-id`'s rows."
  [analysis entity-id]
  (let [eid (some-> (access/current-principal) :_eid)
        {:keys [table fields]} (get (deployed-schema) entity-id)
        ks (keep #(when (and (:stamp %) (= "user" (:type %))) (:key %)) (vals fields))]
    (if (and eid (seq ks))
      (update-in analysis [:entity table]
                 (fn [rows] (reduce-kv (fn [rows tmp-id _] (reduce #(assoc-in %1 [tmp-id %2] eid) rows ks)) rows rows)))
      analysis)))

(defn enhance-write
  "Stamps the principal into user stamps, then applies custom write enhancements, to a write analysis result."
  [tx result]
  (reduce-kv
   (fn [final _ entity-id]
     (binding [*operation-rules* #{:create :update}]
       (enhance/apply-write entity-id (stamp-principal final entity-id) tx)))
   result
   (:entity/mapping result)))

(defn project-saved-entities
  "Converts tmp ids to saved _eid/xid values for relations and recursions; throws on a dangling reference."
  [{:keys [entity :relations/one :relations/many recursion]
    :as analysis}]
  (letfn [(eid [table tmp relation]
            (when tmp
              (or (get-in entity [table tmp :_eid])
                  (throw-reference-not-found
                   analysis table (get-in entity [table tmp]) relation))))
          (xid [table tmp]
            (when tmp (get-in entity [table tmp :xid])))]
    (as-> analysis analysis
      (reduce-kv
       (fn [analysis
            {from-table :from/table
             to-table :to/table
             relation :relation
             :as table}
            ks]
         (assoc-in analysis [:relations/one table]
                   (reduce
                    (fn [result [from to]]
                      (conj result
                            [(eid from-table from relation)
                             (eid to-table to relation)
                             (xid from-table from)
                             (xid to-table to)]))
                    []
                    ks)))
       analysis
       one)
      (reduce-kv
       (fn [analysis
            {from-table :from/table
             to-table :to/table
             relation :relation
             :as table}
            ks]
         (assoc-in analysis [:relations/many table]
                   (reduce
                    (fn [result [from to]]
                      (conj result
                            [(eid from-table from relation)
                             (eid to-table to relation)
                             (xid from-table from)
                             (xid to-table to)]))
                    []
                    ks)))
       analysis
       many)
      (reduce-kv
       (fn [analysis table recursions]
         (reduce-kv
          (fn [analysis field bindings]
            (assoc-in analysis [:recursion table field]
                      (reduce-kv
                       (fn [bindings parent children]
                         (assoc bindings
                                (eid table parent nil)
                                (map #(eid table % nil) children)))
                       nil
                       bindings)))
          analysis
          recursions))
       analysis
       recursion))))

;;; ============================================================================
;;; Runtime Schema Generation (Database-Agnostic)
;;; ============================================================================

(defn model->schema
  "Converts ERDModel to the database-agnostic runtime schema used for SQL query
   generation."
  ([] (model->schema (synthigy.dataset/deployed-model)))
  ([model]
   (reduce
    (fn [schema entity]
      (let [entity-id  (id/extract entity)
            table (entity->table-name entity)
            base-fields (reduce
                         (fn [fields attr]
                           (let [attr-id (id/extract attr)
                                 aname (:name attr)
                                 t (:type attr)
                                 constraint (:constraint attr)
                                 config (:configuration attr)
                                 f (cond-> {:key (keyword (normalize-name aname))
                                            (id/key) attr-id
                                            :type t
                                            :constraint constraint
                                            :skins (dk/label->skins aname)}
                                     (core/stamp attr) (assoc :stamp (core/stamp attr)
                                                              :stamp/write (core/stamp-write attr)))]
                             (assoc fields attr-id
                                    (case t
                                      "enum"
                                      (assoc f
                                             :enum/name (normalize-name (str table \space aname))
                                             :enum/values (get-in config [:values]))
                                      (if-some [ref-entity-id (core/reference-entity-id t)]
                                        (assoc f :reference/entity ref-entity-id)
                                        f)))))
                         {}
                         (filter :active (:attributes entity)))

            fields base-fields

            {relations :relations
             recursions :recursions}
            (group-by
             (fn [{t :cardinality}]
               (case t
                 "tree" :recursions
                 :relations))
             (filter :active (core/focus-entity-relations model entity)))

            base-relations (reduce
                            (fn [relations
                                 {:keys [from to to-label cardinality]
                                  :as relation}]
                              (let [rel-id (id/extract relation)
                                    from-id (id/extract from)
                                    to-id (id/extract to)
                                    passes-check? (and (some? from) (some? to) (not-empty to-label))]
                                (if passes-check?
                                  (assoc relations (keyword (normalize-name to-label))
                                         {:relation rel-id
                                          :rbac/direction [from-id to-id]
                                          :from (core/entity-origin from)
                                          :from/field (entity->relation-field from)
                                          :from/table (entity->table-name from)
                                          :to (if (contains? (:clones model) to-id)
                                                (get-in model [:clones to-id :entity])
                                                to-id)
                                          :to/field (entity->relation-field to)
                                          :to/table (entity->table-name to)
                                          :table (relation->table-name relation)
                                          :cardinality cardinality
                                          :type (case cardinality
                                                  ("m2o" "o2o") :one
                                                  ("m2m" "o2m") :many)})
                                  relations)))
                            {}
                            relations)

            relations base-relations
            recursions (set (map (comp keyword normalize-name :to-label) recursions))
            mandatory-attributes (keep
                                  (fn [{:keys [constraint name]}]
                                    (when (core/mandatory-constraint? constraint)
                                      (keyword (normalize-name name))))
                                  (:attributes entity))
            compiled-rls (rls/compile-entity-rls model entity)
            entity-schema (cond->
                           {:table table
                            :name (:name entity)
                            :constraints (cond->
                                          (assoc (get-in entity [:configuration :constraints])
                                                 :unique (core/get-entity-unique-constraints entity))

                                           (not-empty mandatory-attributes)
                                           (assoc :mandatory mandatory-attributes))
                            :fields fields
                            :field->attribute (reduce-kv
                                               (fn [r a {field :key}]
                                                 (assoc r field a))
                                               nil
                                               fields)
                            :recursions recursions
                            :relations relations}
                            compiled-rls
                            (assoc :rls compiled-rls))]
        (assoc schema entity-id  entity-schema)))
    {}
    (core/get-entities model))))

(defn focus-order
  "Strips nested :_order_by args, keeping only the one on the schema root
   entity."
  [{{order-by :_order_by} :args
    :as schema}]
  (if (some? order-by)
    (reduce
     (fn [s c]
       (if (some? (get-in order-by c)) s
           (update-in s (relations-cursor c) update :args dissoc :_order_by)))
     schema
     (schema->cursors schema))
    schema))

(defn schema->aggregate-cursors
  "Produces cursors pointing to all connected entity tables for a single-query
   pull."
  ([{:keys [relations]
     :as schema}]
   (schema->aggregate-cursors
    (when-let [cursors (keys relations)]
      (mapv vector cursors))
    schema))
  ([cursors schema]
   (reduce
    (fn [cursors cursor]
      (let [{:keys [relations counted? aggregate]} (get-in schema (relations-cursor cursor))]
        (if (not-empty relations)
          (into
           (conj cursors cursor)
           (mapcat #(schema->cursors [(conj cursor %)] schema) (keys relations)))
          (if (or counted? (not-empty aggregate))
            (conj cursors cursor)
            cursors))))
    []
    cursors)))

(defn shave-schema-arguments
  ([schema]
   (reduce
    shave-schema-arguments
    schema
    (schema->cursors schema)))
  ([schema cursor]
   (letfn [(shave [schema]
             (->
              schema
              (dissoc :args)
              (update :fields #(zipmap (keys %) (repeat nil)))))]
     (if (some? cursor)
       (update-in schema (relations-cursor cursor) shave)
       (shave schema)))))

(defn shave-schema-relations
  ([schema]
   (let [arg-keys (set (keys (:args schema)))
         relation-keys (set (keys (:relations schema)))
         valid-keys (clojure.set/intersection arg-keys relation-keys)]
     (update schema :relations select-keys valid-keys)))
  ([schema cursor]
   (letfn [(shave [schema [current :as cursor]]
             (if-not current schema
                     (assoc-in
                      (shave-schema-relations schema)
                      [:relations current]
                      (shave (get-in schema [:relations current]) (rest cursor)))))]
     (if (not-empty cursor)
       (shave
        (update-in schema (relations-cursor cursor) dissoc :relations)
        cursor)
       (dissoc schema :relations)))))

(defn shave-schema-aggregates
  ([schema]
   (reduce
    shave-schema-aggregates
    schema
    (schema->aggregate-cursors schema)))
  ([schema cursor]
   (if (not-empty cursor)
     (let [c (butlast cursor)]
       (if-not c schema
               (recur
                (update-in schema (relations-cursor c) dissoc :counted? :pinned)
                c)))
     (dissoc schema :counted? :pinned))))

(defn analyze-data
  ([tx entity data stack?]
   (let [data' (if (sequential? data)
                 (map #(assoc % :tmp/id (tmp-key)) data)
                 (assoc data :tmp/id (tmp-key)))]
     (analyze-data
      tx
      {:root (if (sequential? data')
               (mapv :tmp/id data')
               (:tmp/id data'))
       :root/table (:table (get (deployed-schema) entity))
       :entity/id entity
       :write/stack? (boolean stack?)}
      entity
      data'
      stack?)))
  ([_ current entity data stack?]
   (let [schema (deployed-schema)
         find-entity (memoize (fn [entity] (get schema entity)))
         type-mapping (memoize
                       (fn [{:keys [fields]}]
                         (reduce-kv
                          ;; enum -> "enum", never the PG type name — that type no longer exists
                          (fn [result _ {:keys [type key]}]
                            (assoc result key type))
                          {(id/key) "uuid"}
                          fields)))
         enum-mapping (memoize
                       (fn [{:keys [fields]}]
                         (reduce-kv
                          (fn [result _ {:keys [key] vs :enum/values}]
                            (if (seq vs)
                              (assoc result key
                                     (into #{}
                                           (comp
                                            (remove #(false? (:active %)))
                                            (keep :name)
                                            (map normalized-enum-value))
                                           vs))
                              result))
                          nil
                          fields)))
         reference-mapping (memoize
                            (fn [entity]
                              (let [{:keys [fields]} (find-entity entity)]
                                (reduce
                                 (fn [result {k :key
                                              ref :reference/entity}]
                                   (if (some? ref)
                                     (assoc result k ref)
                                     result))
                                 nil
                                 (vals fields)))))
         get-constraints (memoize
                          (fn [entity]
                            (let [{:keys [fields]
                                   {:keys [unique]} :constraints} (find-entity entity)]
                              (if (or (empty? unique) (every? empty? unique))
                                [[(id/key)]]
                                (conj
                                 (mapv
                                  (fn [constraints]
                                    (mapv (fn [e] (get-in fields [e :key])) constraints))
                                  unique)
                                 [(id/key)])))))
         entity-profile
         (memoize
          (fn [entity-id]
            (let [{:keys [relations fields recursions] :as e} (find-entity entity-id)
                  writable (remove :stamp (vals fields))
                  refused-stamps (when (access/current-principal)
                                   (not-empty (set (keep #(when (= :refuse (:stamp/write %)) (:key %)) (vals fields)))))
                  {refs true scalar-fields false}
                  (try
                    (group-by #(contains? % :reference/entity) writable)
                    (catch Throwable t
                      (log/error!
                       {:id ::field-grouping-failed
                        :data {:fields (vec writable)}}
                       t)
                      (throw t)))
                  cs (get-constraints entity-id)
                  recursions-set (set recursions)
                  accessible-rel-keys
                  (into []
                        (keep (fn [k]
                                (when-not (contains? recursions-set k)
                                  (let [{:keys [relation] direction :rbac/direction} (get relations k)]
                                    (when (and relation
                                               (access/relation-allows? relation direction #{:write :owns}))
                                      k)))))
                        (keys relations))
                  scalar-field-keys (conj (mapv :key scalar-fields) (id/key))
                  field->attribute (:field->attribute e)]
              {:entity            e
               :scalar-fields     scalar-fields
               :refused-stamps    refused-stamps
               :refs              refs
               :scalar-field-keys scalar-field-keys
               :constraints       cs
               :constraint-keys   (flatten cs)
               :recursions-set    recursions-set
               :accessible-rel-keys accessible-rel-keys
               :accessible-rel-set (set accessible-rel-keys)
               :entity-write?     (access/entity-allows? entity-id #{:create :update :owns})
               :entity-read?      (access/entity-allows? entity-id #{:read :browse})
               :denied-write-keys (into #{}
                                        (filter (fn [k]
                                                  (when-let [attr-id (get field->attribute k)]
                                                    (not (access/attribute-allows? entity-id attr-id :write)))))
                                        scalar-field-keys)
               :inbound-cast      (:inbound-cast e)})))]
     (letfn [(get-indexes [data constraints]
               (remove
                empty?
                (map
                 #(select-keys data %)
                 constraints)))
             (get-id [current table indexes]
               (or
                (some
                 #(get-in current [:index table %])
                 indexes)
                (tmp-key)))
             (shallow-snake [cast data]
               (reduce-kv
                (fn [r k v]
                  (if-not k r
                          (assoc r (or (get cast k) (dk/normalize-key k)) v)))
                nil
                data))
             (transform-object
               ([entity-id data]
                (transform-object nil entity-id data nil))
               ([result entity-id data]
                (transform-object result entity-id data nil))
               ([result entity-id {:keys [tmp/id]
                                      :or {id (tmp-key)}
                                      :as data}
                 via-relation]
                (let [profile (entity-profile entity-id)
                      {:keys [entity refs
                              scalar-field-keys
                              constraints constraint-keys
                              accessible-rel-keys accessible-rel-set
                              entity-write? entity-read?
                              denied-write-keys inbound-cast refused-stamps]} profile
                      ;; the second half of this gate is below — never drop it
                      _ (when-not (or entity-write? via-relation)
                          (entity-accessible? entity-id #{:create :update :owns}))
                      {:keys [relations recursions table field->attribute]
                       {mandatory-fields :mandatory} :constraints} entity
                      data (shallow-snake inbound-cast (dissoc data :tmp/id))
                      fields-data (select-keys data scalar-field-keys)
                      _ (when-let [k (and (seq denied-write-keys)
                                          (some denied-write-keys (keys fields-data)))]
                          (attribute-writable? entity-id (get field->attribute k)))
                      _ (when-let [k (and refused-stamps (some refused-stamps (keys data)))]
                          (throw (ex-info (str "'" (name k) "' is set by the system on " (:name entity))
                                          {:code "ATTRIBUTE_FORBIDDEN"
                                           :entity (:name entity)
                                           :attribute (name k)
                                           :rule "stamp"
                                           :path [:data k]})))
                      _ (when (and (contains? data (id/key))
                                   (nil? (get data (id/key))))
                          (throw
                           (ex-info
                            (str "ID field " (name (id/key)) " cannot be nil when explicitly provided")
                            {:data fields-data
                             :id-key (id/key)})))
                      _ (when (not-empty mandatory-fields)
                          (when-let [nil-fields (not-empty
                                                 (filter
                                                  (comp nil? val)
                                                  (select-keys fields-data mandatory-fields)))]
                            (throw
                             (ex-info
                              (str
                               "Trying to set nil for mandatory fields: "
                               (str/join ", " (map (comp name key) nil-fields)))
                              {:data fields-data
                               :mandatory mandatory-fields}))))
                      type-mapping (type-mapping entity)
                      enum-mapping (enum-mapping entity)
                      fields-data
                      (reduce
                       (fn [fd k]
                         (let [t (get type-mapping k)]
                           (update
                            fd k
                            (if-let [allowed (get enum-mapping k)]
                              (fn [v]
                                (when v
                                  (let [label (normalized-enum-value (name v))]
                                    (when-not (contains? allowed label)
                                      (throw
                                       (ex-info
                                        (format "Value '%s' is not an active enum value for attribute '%s'. Allowed: %s"
                                                label (name k) (str/join ", " (sort allowed)))
                                        {:type ::invalid-enum-value
                                         :attribute k
                                         :value label
                                         :allowed allowed})))
                                    label)))
                              (case t
                                ;; "encrypted" stays plaintext here — sealed in store-entity-records
                                ("boolean" "string" "int" "float" "timeperiod" "currency" "uuid" "transit" "encrypted" nil) identity
                                "timestamp" (fn [v] (proto/encode *db* "timestamp" v))
                                "hashed" (fn [v] (when v (hashers/derive v)))
                                (fn [v]
                                  (when v
                                    (proto/encode *db* t v))))))))
                       fields-data
                       (keys fields-data))
                      encrypted-fields
                      (reduce
                       (fn [r k]
                         (if (= "encrypted" (get type-mapping k))
                           (conj (or r #{}) k)
                           r))
                       nil
                       (keys fields-data))
                      indexes (remove empty? (map #(select-keys fields-data %) constraints))
                      id (or
                          (some #(get-in result [:index table %]) indexes)
                          id)
                      {:keys [references-data resolved-references]}
                      (reduce-kv
                       (fn [r k v]
                         (if (map? v)
                           (assoc-in r [:references-data k] v)
                           (assoc-in r [:resolved-references k] v)))
                       {:references-data nil
                        :resolved-references nil}
                       (select-keys data (map :key refs)))
                      ;; a referenced relation the role can't write must throw here, never no-op
                      _ (doseq [k (keys data)]
                          (when (and (contains? relations k)
                                     (not (contains? recursions k))
                                     (not (contains? accessible-rel-set k)))
                            (let [{:keys [relation] direction :rbac/direction} (get relations k)]
                              (relation-accessible? relation direction #{:write :owns}))))
                      valid-relation-keys (filterv #(contains? data %) accessible-rel-keys)
                      relations-data (when (not-empty valid-relation-keys)
                                       (select-keys data valid-relation-keys))
                      recursions-data (select-keys data recursions)
                      [root parents-mapping]
                      (letfn [(normalize-value [v]
                                (select-keys (shallow-snake inbound-cast v) constraint-keys))]
                        (reduce-kv
                         (fn [[r c] k v]
                           (if (nil? v)
                             [(assoc r k nil) c]
                             [r (assoc c k (normalize-value v))]))
                         [nil nil]
                         recursions-data))
                      fields-data (merge fields-data root resolved-references)
                      fields-data (if (or
                                       (not-empty references-data)
                                       (not-empty (apply dissoc fields-data [:_eid (id/key)])))
                                    fields-data
                                    fields-data)
                      ;; references and recursion parents write this row — never drop these checks
                      link-only-row? (and (not entity-write?)
                                     (some? via-relation)
                                     (empty? references-data)
                                     (empty? parents-mapping)
                                     (link-only? entity constraints (set (keys fields-data))))
                      _ (when-not (or entity-write? link-only-row?)
                          (entity-accessible? entity-id #{:create :update :owns}))
                      _ (when (and link-only-row? (not entity-read?))
                          (entity-accessible? entity-id #{:read :browse}))]
                  (as->
                   (->
                    result
                    (update-in [:entity table id] (if stack? merge (fn [_ v] v)) fields-data)
                    (assoc-in [:entity/mapping table] entity-id)
                    (cond-> encrypted-fields
                      (update-in [:encrypted table] (fnil into #{}) encrypted-fields))
                    (update-in [:index table] merge (zipmap indexes (repeat id)))
                    (assoc-in [:constraint table] constraints)
                    (cond-> link-only-row? (update-in [:link-only table] (fnil conj #{}) id))
                    (cond-> (nil? via-relation)
                      (update-in [:subject table] (fnil conj #{}) id)))
                   result
                    (reduce-kv
                     (fn [result k data]
                       (let [parent-indexes (get-indexes data constraints)
                             pid (get-id result table parent-indexes)]
                         (->
                          result
                          (update-in [:recursion table k pid] (fnil conj #{}) id)
                          (update-in [:index table] merge (zipmap parent-indexes (repeat pid)))
                          (update-in [:entity table pid] merge data))))
                     result
                     parents-mapping)
                    (reduce-kv
                     (fn [result attribute data]
                       (let [reference-entity-id (get
                                                   (reference-mapping entity-id)
                                                   attribute)
                             reference-entity (find-entity reference-entity-id)
                             reference-data (some
                                              (fn [ks]
                                                (when (every? #(contains? data %) ks)
                                                  (select-keys data ks)))
                                              (get-constraints reference-entity-id))]
                         (update-in
                          result
                          [:reference
                           (:table reference-entity)
                           reference-data]
                          (fnil conj [])
                          [(:table entity) id attribute])))
                     result
                     references-data)
                    (reduce-kv
                     (fn [result k data]
                       (let [{{:keys [to]
                               to-table :to/table
                               rtype :type
                               :as relation} k} relations
                             constraints (get-constraints to)]
                         (when (if (= :many rtype) (map? data) (sequential? data))
                           (throw (ex-info (str "Relation " (name k) " is "
                                                (if (= :many rtype)
                                                  "to-many — pass an array of records"
                                                  "to-one — pass one record, not an array"))
                                           {:code "BAD_DATA_SHAPE"
                                            :rule "relation_data"
                                            :relation (name k)})))
                         (case rtype
                           :many
                           (if (or (empty? data) (nil? data))
                             (update-in result [:relations/many relation] (fnil conj #{}) [id nil])
                             (reduce
                              (fn [result data]
                                (let [relation-indexes (get-indexes data constraints)
                                      rid (get-id result to-table relation-indexes)]
                                  (transform-object
                                   (->
                                    result
                                    (update-in
                                     [:index to-table] merge
                                     (zipmap relation-indexes (repeat rid)))
                                    (update-in
                                     [:relations/many relation] (fnil conj #{})
                                     [id rid]))
                                   to
                                   (assoc data :tmp/id rid)
                                   relation)))
                              result
                              data))
                           :one
                           (if (nil? data)
                             (update-in result [:relations/one relation] (fnil conj #{}) [id nil])
                             (let [relation-indexes (get-indexes data constraints)
                                   rid (get-id result to-table relation-indexes)]
                               (transform-object
                                (->
                                 result
                                 (update-in
                                  [:index to-table] merge
                                  (zipmap relation-indexes (repeat rid)))
                                 (update-in
                                  [:relations/one relation] (fnil conj #{})
                                  [id rid]))
                                to
                                (assoc data :tmp/id rid)
                                relation))))))
                     result
                     relations-data)))))]
       (if (sequential? data)
         (reduce
          #(transform-object %1 entity %2)
          current
          data)
         (transform-object current entity data))))))

(defn join-mode
  "The mode (:left, :inner or :none) of a `:_join` value; nil is :left, an unknown value nil."
  [v]
  (if (nil? v)
    :left
    (#{:left :inner :none} v)))

(defn normalize-join
  "Args with `:_join` coerced to its mode; throws INVALID_JOIN on an unknown value or on `none` with `_maybe`."
  [entity-id args]
  (if-not (and (map? args) (contains? args :_join))
    args
    (let [v (:_join args)
          mode (join-mode v)
          entity-name (:name (deployed-schema-entity entity-id))]
      (when (or (nil? mode) (and (= :none mode) (contains? args :_maybe)))
        (throw (ex-info (str "Invalid _join " (pr-str v) " on entity " entity-name
                             (if mode ": none cannot be combined with _maybe" ". Supported: left, inner, none"))
                        {:code "INVALID_JOIN"
                         :entity entity-name
                         :value (if (keyword? v) (name v) v)
                         :supported ["left" "inner" "none"]
                         :path [:args :_join]})))
      (assoc args :_join mode))))

(def caller-access
  "Access as the bound principal: entity check, attribute rule and RLS."
  {:entity! #(entity-accessible? % *operation-rules*)
   :attribute? #(access/attribute-allows? %1 %2 :read)
   :rls? true})

(def label-access
  "No access checks: a label node is already cut to what any reader of the holding record may see."
  {:entity! (constantly nil)
   :attribute? (constantly true)
   :rls? false})

(declare label-schema)

(defn readable-args!
  "Refuses a filter, sort or distinct on a field `attribute?` does not let the caller read."
  [entity-id attribute? field->attribute fields]
  (doseq [k fields
          :let [attr-id (get field->attribute k)]
          :when (and attr-id (not (attribute? entity-id attr-id)))]
    (throw (ex-info (format "You may not filter or sort by '%s' on entity '%s'"
                            (name k) (:name (deployed-entity entity-id)))
                    {:code "ATTRIBUTE_FORBIDDEN"
                     :entity entity-id
                     :attribute (name k)
                     :path [:args]}))))

(defn filter-fields
  "Field keys an args map filters on through `_maybe`, `_not`, `_or` and `_and`."
  [args]
  (letfn [(walk [m]
            (when (map? m)
              (mapcat (fn [[k v]]
                        (cond
                          (#{:_maybe :_not} k) (walk v)
                          (#{:_or :_and} k) (mapcat walk v)
                          (str/starts-with? (name k) "_") nil
                          :else [k]))
                      m)))]
    (distinct (walk args))))

(defn aggregate-readable!
  "Refuses an operation through a relation, onto an entity or over a field the caller may not read."
  [{:keys [relation ref? recursion?] to :to direction :rbac/direction} fields args]
  (when-not (or ref? recursion?)
    (relation-accessible? relation direction #{:read}))
  (entity-accessible? to *operation-rules*)
  (readable-args! to (:attribute? caller-access) (:field->attribute (deployed-schema-entity to))
                  (concat (map keyword fields) (mapcat filter-fields args))))

(def op-field-types
  {:sum #{"int" "float"}
   :avg #{"int" "float"}
   :min #{"int" "float" "timestamp" "string"}
   :max #{"int" "float" "timestamp" "string"}})

(def uncountable-types #{"json" "hashed" "encrypted" "transit"})

(defn parse-op
  "`[fn field]` of a wire `:op` (\"count\" or `{fn field}`), or nil."
  [op]
  (cond
    (and (or (string? op) (keyword? op)) (= "count" (name op))) [:count nil]
    (and (map? op) (= 1 (count op)))
    (let [[f field] (first op)
          f (keyword (str/replace (name f) "-" "_"))]
      (when (and (#{:sum :avg :min :max :count_distinct} f)
                 (or (string? field) (keyword? field)))
        [f (keyword (name field))]))))

(defn invalid-op! [msg data]
  (throw (ex-info msg (merge {:code "INVALID_OPERATION"} data))))

(defn selection-op
  "Spec of one `{:alias :op :args}` entry on relation `rkey`; shape, type and read access checked."
  [entity-id relations rkey {:keys [alias op args]}]
  (let [ename (:name (deployed-schema-entity entity-id))
        path  [:selections rkey]
        alias (some-> alias name)
        [f field] (or (parse-op op)
                      (invalid-op! (str "Unknown operation " (pr-str op) " on " (name rkey)
                                        " — use \"count\" or {sum|avg|min|max|count_distinct: field}")
                                   {:entity ename :relation (name rkey) :path path}))
        _ (when (str/blank? alias)
            (invalid-op! (str "An operation on " (name rkey) " needs an alias")
                         {:entity ename :relation (name rkey) :path path}))
        rdata (get relations rkey)
        _ (when (or (nil? rdata) (not= :many (:type rdata)))
            (invalid-op! (str "`" (name f) "` reads a to-many relation; '" (name rkey) "' is not one on " ename)
                         {:entity ename :relation (name rkey) :alias alias :path path}))
        ;; never move below the field checks — their messages describe the target's attributes
        _ (aggregate-readable! rdata nil nil)
        target (deployed-schema-entity (:to rdata))
        tname (:name target)
        tfields (into {} (map (juxt :key identity)) (vals (:fields target)))
        ftype (when field (:type (get tfields field)))
        _ (when field
            (cond
              (nil? ftype)
              (invalid-op! (str "Unknown attribute '" (name field) "' on " tname)
                           {:code "UNKNOWN_ATTRIBUTE" :entity tname :attribute (name field) :path path})
              (= :count_distinct f)
              (when (uncountable-types ftype)
                (invalid-op! (str "count_distinct can't compare " ftype " attribute '" (name field) "'")
                             {:entity tname :attribute (name field) :alias alias :path path}))
              (not ((op-field-types f) ftype))
              (invalid-op! (str "`" (name f) "` can't read " ftype " attribute '" (name field) "'")
                           {:entity tname :attribute (name field) :alias alias :path path})))
        args (not-empty args)
        _ (doseq [k (filter-fields args)
                  :when (not (or (contains? tfields k) (#{:_eid :xid :euuid} k)))]
            (invalid-op! (str "An operation filter reads attributes of " tname "; '" (name k) "' is not one")
                         {:entity tname :attribute (name k) :alias alias :path path}))
        _ (aggregate-readable! rdata (when field [field]) [args])]
    {:alias (keyword alias)
     :fn f
     :field field
     :type ftype
     :args args
     :relation (-> rdata
                   (dissoc :relations)
                   (clojure.set/rename-keys {:table :relation/table})
                   (merge (entity-serde (:to rdata)))
                   (assoc :entity/table (:to/table rdata)
                          :field-types (update-vals tfields :type)))}))

(defn split-op-entries
  "`[selection ops-by-relation]` with the `:op` entries taken out of every relation key."
  [selection]
  (reduce-kv
   (fn [[sel ops] k v]
     (if (and (sequential? v) (some :op v))
       (let [{op-entries true rest-entries false} (group-by (comp boolean :op) v)]
         [(if (seq rest-entries) (assoc sel k (vec rest-entries)) sel)
          (assoc ops k op-entries)])
       [(assoc sel k v) ops]))
   [nil nil]
   selection))

(def id-comparisons #{:_eq :_neq :_in :_nin :_not_in})

(defn id-comparison
  "A reference compared to xids or euuids as a filter on the target's id field; nil for anything else."
  [v]
  (when (and (map? v) (seq v) (every? id-comparisons (keys v)))
    (let [ids (mapcat #(if (coll? %) % [%]) (vals v))]
      (when (and (seq ids) (every? #(or (string? %) (uuid? %)) ids))
        {(id/key) v}))))

(def args-operator-keys
  #{:_or :_and :_not :_maybe
    :_limit :_offset :_order_by :_distinct :_join
    :_eq :_neq :_lt :_lte :_le :_gt :_gte :_ge
    :_in :_nin :_not_in :_like :_ilike :_is_null :_is_not_null})

(defn null-test
  "`:none` or `:exists` when `v` only asks whether a value is there, nil otherwise."
  [v]
  (cond
    (= "is_null" v) :none
    (= "is_not_null" v) :exists
    (#{{:_is_null true} {:_is_not_null false} {:_eq nil}} v) :none
    (#{{:_is_null false} {:_is_not_null true} {:_neq nil}} v) :exists))

(defn prune-empty-lists
  "`v` without `_in`/`_nin`/`_not_in` over an empty list, so they constrain nothing, as at the root."
  [v]
  (if-not (map? v)
    v
    (reduce-kv
     (fn [m k x]
       (cond
         (and (#{:_in :_nin :_not_in} k) (coll? x) (empty? x)) m
         (= :_and k) (if-let [xs (seq (keep #(not-empty (prune-empty-lists %)) x))] (assoc m k (vec xs)) m)
         (= :_or k) (let [xs (map prune-empty-lists x)] (if (some empty? xs) m (assoc m k (vec xs))))
         (map? x) (if-let [x' (not-empty (prune-empty-lists x))] (assoc m k x') m)
         :else (assoc m k x)))
     {}
     v)))

(defn relation-key
  "The args key of a condition answered through relation `k`: `kind` is :exists or :none."
  [kind k]
  (keyword (str "relation." (name kind)) (name k)))

(defn relation-key-kind [k]
  (case (and (keyword? k) (namespace k))
    "relation.exists" :exists
    "relation.none" :none
    nil))

(defn relation-condition
  "`[key value]` of condition `v` on relation `k`: an EXISTS or NOT EXISTS key, the column itself for a to-one value test, nil when it constrains nothing."
  [k {:keys [ref? recursion?]} v]
  (let [to-one? (or ref? recursion?)]
    (if-let [t (null-test v)]
      (if to-one? [k v] [(relation-key t k) {}])
      (let [v' (prune-empty-lists v)]
        (cond
          (and (map? v) (seq v) (empty? v')) nil
          (not to-one?) [(relation-key :exists k) v']
          (id-comparison v') [(relation-key :exists k) (id-comparison v')]
          (and (map? v') (some #(not (args-operator-keys %)) (keys v'))) [(relation-key :exists k) v']
          :else (throw (ex-info (format "'%s' is a reference; compare it by xid: %s = ?xid or %s.xid = ?xid"
                                        (name k) (name k) (name k))
                                {:code "INVALID_FILTER"
                                 :rule "reference_by_xid"
                                 :relation (name k)
                                 :path [:args k]})))))))

(defn relation-args
  "`args` with every condition on a key of `relations` rewritten by `relation-condition`."
  [args relations]
  (if-not (map? args)
    args
    (reduce-kv
     (fn [m k v]
       (cond
         (#{:_and :_or} k) (assoc m k (mapv #(relation-args % relations) v))
         (#{:_maybe :_not} k) (assoc m k (relation-args v relations))
         (contains? relations k) (if-let [[k' v'] (relation-condition k (get relations k) v)] (assoc m k' v') m)
         :else (assoc m k v)))
     {}
     args)))

(defn arg-relation-constraints
  "Constraints of `args` keyed by the relation they filter through, collected across `_and`, `_or`, `_maybe` and `_not`."
  [args relation?]
  (if-not (map? args)
    nil
    (reduce-kv
     (fn [m k v]
       (cond
         (#{:_and :_or} k) (reduce #(merge-with into %1 (arg-relation-constraints %2 relation?)) m v)
         (#{:_maybe :_not} k) (merge-with into m (arg-relation-constraints v relation?))
         (relation? k v) (update m k (fnil conj []) v)
         :else m))
     nil
     args)))

(defn selection->schema
  ([entity-id selection]
   (selection->schema entity-id selection nil))
  ([entity-id selection args]
   (selection->schema entity-id selection args caller-access))
  ([entity-id selection args {:keys [entity! attribute? rls?]}]
   (entity! entity-id)
   (let [{relations :relations
          recursions :recursions
          fields :fields
          field->attribute :field->attribute
          table :table
          rls :rls} (deployed-schema-entity entity-id)
         [selection op-entries] (split-op-entries (flatten-selection selection))
         ops (reduce-kv
              (fn [ops rkey entries]
                (reduce
                 (fn [ops entry]
                   (let [{a :alias :as spec} (selection-op entity-id relations rkey entry)
                         taken? (or (contains? ops a)
                                    (some (fn [[k v]]
                                            (if (sequential? v)
                                              (some #(= a (keyword (name (or (:alias %) k)))) v)
                                              (= a k)))
                                          selection))]
                     (when taken?
                       (invalid-op! (str "Duplicate key '" (name a) "' — every operation needs its own alias")
                                    {:entity (:name (deployed-schema-entity entity-id))
                                     :alias (name a) :path [:selections rkey]}))
                     (assoc ops a spec)))
                 ops
                 entries))
              nil
              op-entries)
         args (normalize-join entity-id args)
         ;;
         {fields :field
          refs :reference} (distribute-fields (vals fields))
         ;;
         valid-fields (conj
                       (set
                        (keep
                         (fn [{t :type
                               k :key}]
                           (when (scalar-types t) k))
                         fields))
                       (id/key) :_eid)
         relation-key-set (cond-> (set (keys relations))
                            (not-empty refs) (clojure.set/union (set (map :key refs)))
                            (not-empty recursions) (clojure.set/union (set recursions)))
         selection-operator-keys #{:count}
         scalars (reduce-kv
                  (fn [r k [{args :args}]]
                    (cond
                      (and (valid-fields k)
                           (let [attr-id (get field->attribute k)]
                             (or (nil? attr-id) (attribute? entity-id attr-id))))
                      (assoc r k args)
                      (valid-fields k)            r
                      (#{:xid :euuid} k)          r
                      (selection-operator-keys k) r
                      (relation-key-set k)        r       ; legit relation — handled by objects branch
                      (#{:_count :_agg} k)
                      (throw (ex-info (str (name k) " was removed — select an operation on the relation: "
                                           "{\"ratings\": [{\"alias\": \"votes\", \"op\": \"count\"}]}")
                                      {:code "INVALID_OPERATION"
                                       :entity (:name (deployed-schema-entity entity-id))
                                       :path [:selections k]}))
                      :else
                      (throw (ex-info
                              (str "Unknown attribute " (pr-str k)
                                   " in selection on entity "
                                   (:name (deployed-schema-entity entity-id)))
                              (let [candidates (concat (map name valid-fields)
                                                       (map name relation-key-set))]
                                (cond-> {:code "UNKNOWN_ATTRIBUTE"
                                         :entity (:name (deployed-schema-entity entity-id))
                                         :attribute (name k)
                                         :rule "schema_attribute"
                                         :path [:selections]}
                                  (<= (count candidates) 200)
                                  (assoc :hint (suggest-similar (name k) candidates))))))))
                  {(id/key) nil}
                  selection)
         args (reduce-kv
               (fn [args k v]
                 (cond
                   (nil? v) args
                   (contains? args k) (-> args
                                          (dissoc k)
                                          (update :_and (fnil conj []) {k (get args k)} {k v}))
                   :else (assoc args k v)))
               args
               scalars)
         distinct-on (:_distinct args)
         order-by (:_order_by args)
         order-by-relations (reduce-kv
                             (fn [r k v]
                               (if (map? v) (conj r k) r))
                             #{}
                             order-by)
         distinct-on-relations (reduce-kv
                                (fn [r k v]
                                  (if (map? v) (conj r k) r))
                                #{}
                                distinct-on)
         valid-relations (cond-> (set (keys relations))
                           (not-empty refs)
                           (clojure.set/union (set (map :key refs)))
                           ;;
                           (not-empty order-by-relations)
                           (clojure.set/union order-by-relations)
                           ;;
                           (not-empty distinct-on-relations)
                           (clojure.set/union distinct-on-relations)
                           (not-empty recursions)
                           (clojure.set/union (set recursions)))
         type-mapping (zipmap (map :key fields) (map :type fields))
         ;; capture *db* as a value — decoders may run in futures without the binding
         db *db*
         decoders (reduce
                   (fn [r k]
                     (if (valid-fields k)
                       (let [field-type (get type-mapping k)
                             transform (case field-type
                                         "encrypted" (fn [data _] (denc/unseal-cell data))
                                         "json" (fn [data _] (proto/decode db "json" data))
                                         "boolean" (fn [data _] (proto/decode db "boolean" data))
                                         "enum" (fn [data _] (proto/decode db "enum" data))
                                         ("currency" "period") (fn [data _] (proto/decode db field-type data))
                                         "timestamp" (fn [data _] (proto/decode db "timestamp" data))
                                         (if (and field-type (not (scalar-types field-type)))
                                           (fn [data _] (proto/decode db field-type data))
                                           nil))]
                         (if transform
                           (assoc r k transform)
                           r))
                       r))
                   nil
                   (map key scalars))
         field->type (reduce
                      (fn [result {f :key t :type}]
                        (assoc result f t))
                      nil
                      fields)
         arg-fields (letfn [(join-args
                              ([args] (join-args args #{}))
                              ([args result]
                               (reduce-kv
                                (fn [result k _]
                                  (let [result'
                                        (cond
                                          (valid-fields k)        (conj result k)
                                          (args-operator-keys k)  result
                                          (relation-key-set k)    result
                                          :else
                                          (throw (ex-info
                                                  (str "Unknown attribute " (pr-str k)
                                                       " in args on entity "
                                                       (:name (deployed-schema-entity entity-id)))
                                                  (let [candidates (concat (map name valid-fields)
                                                                           (map name relation-key-set))]
                                                    (cond-> {:code "UNKNOWN_ATTRIBUTE"
                                                             :entity (:name (deployed-schema-entity entity-id))
                                                             :attribute (name k)
                                                             :rule "schema_attribute"
                                                             :path [:args]}
                                                      (<= (count candidates) 200)
                                                      (assoc :hint (suggest-similar (name k) candidates)))))))]
                                    (reduce clojure.set/union result'
                                            (concat
                                             (map join-args (vals (select-keys args [:_maybe :_not])))
                                             (mapcat #(map join-args %) (vals (select-keys args [:_or :_and])))))))
                                result
                                args)))]
                      (join-args args))
         _ (readable-args! entity-id attribute? field->attribute
                           (concat arg-fields
                                   (filter (set (map :key refs)) (filter-fields args))
                                   (keep (fn [[k v]] (when-not (map? v) k)) order-by)
                                   (map keyword (cond (sequential? distinct-on) distinct-on
                                                      (map? distinct-on) (:attributes distinct-on)))))
         encoders (cond->
                   (reduce
                    (fn [result field]
                      (let [t (get field->type field)]
                        (case t
                          ("boolean" "string" "int" "float" "json"
                                     "timeperiod" "currency"
                                     "uuid" "hashed" "transit" nil) result
                          "timestamp" (assoc result field
                                             (fn [v] (proto/encode *db* "timestamp" v)))
                          (assoc result field (fn [v] (proto/encode *db* t v))))))
                    nil
                    arg-fields)
                    true
                    (assoc (id/key) id/coerce-arg))
         objects (apply dissoc selection (keys scalars))
         all-relations (cond-> relations
                             (not-empty refs)
                             (as-> relations
                                   (reduce
                                    (fn [relations' {k :key
                                                     t :type
                                                     ref-entity :reference/entity}]
                                      (let [{ttable :table} (deployed-schema-entity ref-entity)
                                            attr-id (get field->attribute k)]
                                        (if (and attr-id (not (attribute? entity-id attr-id)))
                                          relations'
                                          (assoc relations' k
                                               {:from entity-id
                                                :from/field (name k)
                                                :from/table table
                                                :to ref-entity
                                                :to/field "_eid"
                                                :to/table ttable
                                                :table table
                                                :ref? true
                                                :reference/type t
                                                :type :one}))))
                                    relations
                                    refs))
                             ;;
                             (not-empty recursions)
                             (as-> relations
                                   (reduce
                                    (fn [relations' recursion]
                                      (assoc relations' (keyword recursion)
                                             {:args (get-in objects [(keyword recursion) 0 :args])
                                              :from entity-id
                                              :from/field (name recursion)
                                              :from/table table
                                              :to entity-id
                                              :to/field "_eid"
                                              :to/table table
                                              :table table
                                              :recursion? true
                                              :type :one}))
                                    relations
                                    recursions)))
         narrow-relations (reduce-kv
                           (fn [rs rkey rdata]
                             (if (valid-relations rkey)
                               (if (or
                                    (contains? args rkey)
                                    (contains? objects rkey)
                                    (contains? order-by rkey)
                                    (contains? distinct-on rkey))
                                 (reduce
                                  (fn [final {:keys [selections alias]
                                              new-args :args}]
                                    (let [{:keys [relation ref? recursion?] direction :rbac/direction} rdata
                                          child-args (cond-> new-args
                                                       (contains? order-by rkey)
                                                       (assoc :_order_by (get order-by rkey))
                                                       ;;
                                                       (contains? distinct-on rkey)
                                                       (assoc :_distinct (get distinct-on rkey)))]
                                      (or
                                       ref? recursion?
                                       (relation-accessible? relation direction #{:read}))
                                      (assoc final (keyword (or alias rkey))
                                             (merge
                                              (clojure.set/rename-keys rdata {:table :relation/table})
                                              {:relation/as (str (gensym "link_"))
                                               :entity/as (str (gensym "data_"))}
                                              (if-let [label (when ref? (core/reference-label (:reference/type rdata)))]
                                                (label-schema (:to rdata) label selections child-args)
                                                (selection->schema (:to rdata) selections child-args))))))
                                  rs
                                  (get objects rkey))
                                 rs)
                               rs))
                           nil
                           all-relations)
         order-relations (reduce
                          (fn [m rkey]
                            (let [rdata (get all-relations rkey)]
                              (if (or (contains? narrow-relations rkey) (nil? rdata))
                                m
                                (let [{:keys [relation ref? recursion?] direction :rbac/direction} rdata]
                                  (or ref? recursion? (relation-accessible? relation direction #{:read}))
                                  (assoc m rkey
                                         (merge
                                          (clojure.set/rename-keys rdata {:table :relation/table})
                                          {:relation/as (str (gensym "link_"))
                                           :entity/as (str (gensym "data_"))}
                                          (let [child-args {:_order_by (get order-by rkey)}]
                                            (if-let [label (when ref? (core/reference-label (:reference/type rdata)))]
                                              (label-schema (:to rdata) label nil child-args)
                                              (selection->schema (:to rdata) nil child-args)))))))))
                          nil
                          order-by-relations)
         args (relation-args args all-relations)
         filter-relations (binding [*operation-rules* #{:read}]
                          (reduce-kv
                           (fn [m rkey constraints]
                             (let [{:keys [relation ref? recursion?] direction :rbac/direction :as rdata} (get all-relations rkey)
                                   filter-args {:_and constraints}]
                               (or ref? recursion? (relation-accessible? relation direction #{:read}))
                               (assoc m rkey
                                      (merge
                                       (clojure.set/rename-keys rdata {:table :relation/table})
                                       {:relation/as (str (gensym "link_"))
                                        :entity/as (str (gensym "data_"))}
                                       (if-let [label (when ref? (core/reference-label (:reference/type rdata)))]
                                         (label-schema (:to rdata) label nil filter-args)
                                         (selection->schema (:to rdata) nil filter-args))))))
                           nil
                           (reduce-kv
                            (fn [m k cs] (update m (keyword (name k)) (fnil into []) cs))
                            nil
                            (arg-relation-constraints args (fn [k _] (relation-key-kind k))))))
         base-schema (hash-map
                            :entity entity-id
                            :entity/as (str (gensym "data_"))
                            :entity/table table
                            :fields scalars
                            :field-types field->type
                            :counted? (contains? selection :count)
                            :aggregate (reduce
                                        (fn [r {k :key}]
                                          (let [[{:keys [args selections]}] (get selection k)
                                                selection (flatten-selection selections)]
                                            (if (not-empty selections)
                                              (assoc r k {:operations (vec (map name (keys selection)))
                                                          :args args})
                                              r)))
                                        nil
                                        fields)
                            :args args
                            :decoders decoders
                            :encoders encoders
                            :relations narrow-relations
                            :recursions recursions
                            :rls (when rls? rls)
                            :ops ops
                            :order-relations order-relations
                            :filter-relations filter-relations
                            :arg-relations all-relations)]
     (enhance/apply-schema base-schema selection))))

(defn label-args!
  "Refuses args on a reference that filter, sort or distinct on anything but `allowed`."
  [entity-id allowed args]
  (let [d (:_distinct args)]
    (doseq [k (concat (filter-fields args)
                      (keys (:_order_by args))
                      (cond (sequential? d) d
                            (map? d) (concat (:attributes d) (keys (dissoc d :attributes)))))
            :let [k (keyword (name k))]
            :when (not (allowed k))]
      (throw (ex-info (format "A reference to '%s' shows only its label; '%s' can't be filtered or sorted"
                              (:name (deployed-entity entity-id)) (name k))
                      {:code "ATTRIBUTE_FORBIDDEN"
                       :entity entity-id
                       :attribute (name k)
                       :path [:args]})))))

(defn label-schema
  "Schema of a reference read as its `label`: the id and label fields only, without the target's access checks."
  [entity-id label selection args]
  (let [allowed (into label [:xid :euuid :_eid])
        {:keys [fields relations recursions]} (deployed-schema-entity entity-id)
        known (into #{:count} (concat (map :key (vals fields)) (keys relations) (map keyword recursions)))]
    (label-args! entity-id allowed args)
    (selection->schema entity-id
                       (into {} (remove (fn [[k _]] (and (known k) (not (allowed k)))))
                             (flatten-selection selection))
                       args
                       label-access)))

;;; ============================================================================
;;; SQL Generation Functions (Shared - Database-Agnostic)
;;; ============================================================================

(defn j-and
  "Joins SQL condition strings with \" and \". [\"a\" \"b\"] => \"a and b\"."
  [statements]
  (clojure.string/join " and " statements))

(defn sql-quote
  "Double-quote a SQL identifier."
  [s]
  (str \" (str/replace (name s) "\"" "") \"))

(defn distinct-fields
  "Fields of a `_distinct`: a list (the wire form) or `{:attributes [...]}` (XSQL)."
  [d]
  (not-empty (vec (cond (sequential? d) d
                        (map? d) (:attributes d)))))

(defn distinct->sql
  "Emits a `distinct on (\"f1\", \"f2\") ` prefix for the root SELECT.

   ponytail: root attributes only. Relation-scoped distinct-on isn't
   supported — the level-oriented pull builds one SELECT per level and
   never joins child tables into the root, so there's nothing to be
   distinct *on* across levels."
  [{{d :_distinct} :args}]
  (when-let [fields (distinct-fields d)]
    (str "distinct on ("
         (clojure.string/join ", " (map sql-quote fields))
         ") ")))

(defn tiebreak-columns
  "Columns that end a paged read's order so equal sort keys page the same way every time: the distinct fields, then the row id."
  [as distinct]
  (let [col (fn [c] (str (when (not-empty as) (str as \.)) c))]
    (conj (mapv (comp col sql-quote) (distinct-fields distinct)) (col "_eid"))))

(defn with-tiebreak
  "`order-sql` (an `order by …` clause or blank) ending with the tiebreak columns."
  [order-sql as distinct]
  (let [cols (str/join ", " (tiebreak-columns as distinct))]
    (if (str/blank? order-sql) (str "order by " cols) (str order-sql ", " cols))))

(defn modifiers-selection->sql
  ([schema] (modifiers-selection->sql schema nil))
  ([{operators :args
     as :entity/as
     :as schema} {:keys [tiebreak?]}]
   (let [limit (get operators :_limit)
         offset (get operators :_offset)
         limit-offset (when (or limit offset)
                        (proto/limit-offset-clause *db* limit offset))
         order-sql
         (when (contains? operators :_order_by)
              (str
               "order by "
               (clojure.string/join
                ", "
                (letfn [(process-order-by [{:keys [entity/as]
                                            :as schema} order-by]
                          (reduce-kv
                           (fn [result field order-by']
                             (let [dir (when (keyword? order-by') order-by')]
                               (if dir
                                 (do
                                   (log/trace {:id ::order-by-modifier
                                               :data {:as as :field field :dir dir}}
                                              "Order-by modifier")
                                   (conj result [as field dir]))
                                 (into
                                  result
                                  (process-order-by (get-in schema [:relations field]) order-by')))))
                           []
                           order-by))]
                  (reduce
                   (fn [result [table field order]]
                     (conj result
                           (str (when table (str table \.))
                                (sql-quote field)
                                (case order
                                  :desc " desc nulls last"
                                  :asc " asc nulls first"))))
                   []
                   (process-order-by schema (get operators :_order_by)))))))
         order-sql (if (and tiebreak? limit-offset)
                     (with-tiebreak order-sql as (:_distinct operators))
                     order-sql)]
     (str/join " " (remove str/blank? [order-sql limit-offset])))))

(def ^:dynamic *ignore-maybe* true)
(def ^:dynamic *deep* true)

(def ^:dynamic *skip-left-where* false)

(defn node-read-guard
  "[sql params] of the :read RLS a schema node carries, on `alias`; nil when it carries none or the principal bypasses it."
  [{entity-id :entity {:keys [enabled guards]} :rls} alias]
  (when (and enabled (rls-runtime/should-apply-guards? entity-id :read))
    (if-let [r (and (seq guards) (rls-runtime/compile-guards-to-sql alias guards :read))]
      [(:sql r) (vec (:params r))]
      ["1=0" []])))

(defn related-value
  "[sql params] of a correlated subquery reading `expr` through `rel` inside its read guards; to-many reads min for asc, max for desc."
  [{:keys [ref? recursion?] t :entity/as l :relation/as :as rel} parent-as expr params dir]
  (let [ref? (or ref? recursion?)
        [guard guard-params] (node-read-guard rel t)
        from (if ref?
               (format "from \"%s\" %s where %s._eid = %s.%s"
                       (:to/table rel) t t parent-as (sql-quote (:from/field rel)))
               (format "from \"%s\" %s join \"%s\" %s on %s._eid = %s.%s where %s.%s = %s._eid"
                       (:relation/table rel) l (:to/table rel) t t l (sql-quote (:to/field rel))
                       l (sql-quote (:from/field rel)) parent-as))]
    [(str "(select " (if ref? expr (str (if (= :desc dir) "max(" "min(") expr ")")) " " from
          (when guard (str " and (" guard ")"))
          (when ref? " limit 1") ")")
     (into (vec params) guard-params)]))

(declare op-order-sql)

(defn order-terms
  "[[sql direction params]] of an `_order_by` on `schema`, a related field or an operation alias as a correlated subquery."
  [{:keys [relations order-relations ops] as :entity/as} order-by]
  (reduce-kv
   (fn [terms field v]
     (cond
       (and (keyword? v) (contains? ops field))
       (let [[sql ps] (op-order-sql (get ops field) as)] (conj terms [sql v ps]))
       (keyword? v)
       (conj terms [(str as \. (sql-quote field)) v []])
       :else
       (let [rel (or (get relations field) (get order-relations field)
                     (throw (ex-info (str "Can't sort through '" (name field) "': not a relation the caller can read")
                                     {:code "INVALID_ORDER" :relation (name field) :path [:args :_order_by]})))]
         (into terms
               (map (fn [[expr dir params]]
                      (let [[sql ps] (related-value rel as expr params dir)] [sql dir ps])))
               (order-terms rel v)))))
   []
   order-by))

(defn related-order?
  [order-by]
  (some map? (vals order-by)))

(defn term-order?
  "True when `schema`'s `_order_by` needs `order-terms`: a related field or an operation alias."
  [{ops :ops {ob :_order_by} :args}]
  (boolean (and (map? ob) (or (related-order? ob) (some #(contains? ops %) (keys ob))))))

(defn related-modifiers
  "[sql params] of the ORDER BY and LIMIT/OFFSET of `schema` for an engine that reads roots without joining relations."
  [{{order-by :_order_by limit :_limit offset :_offset distinct :_distinct} :args as :entity/as :as schema}]
  (let [terms (order-terms schema order-by)
        limit-offset (when (or limit offset) (proto/limit-offset-clause *db* limit offset))
        order-sql (str "order by "
                       (str/join ", " (map (fn [[sql dir]] (str sql (case dir :desc " desc nulls last" :asc " asc nulls first"))) terms)))]
    [(str (cond-> order-sql limit-offset (with-tiebreak as distinct))
          (when limit-offset (str " " limit-offset)))
     (vec (mapcat #(nth % 2) terms))]))

(declare relation-exists)

(defn query-selection->sql
  ([schema] (query-selection->sql schema []))
  ([{operators :args
     encoders :encoders
     prefix :entity/as
     relations :relations
     field-types :field-types
     :as schema} data]
   (let [is-relation? (set (keys relations))
         left-relation? (= :left (:_join operators))]
     (reduce
      (fn [[statements data] [field constraints]]
        (let [field-type (get field-types field)
              field' (str (when (not-empty prefix) (str prefix \.)) \" (name field) \")
              field' (if (= "json" field-type)
                       (db/json-text *db* field')
                       field')
              ph (if (and field-type (not (scalar-types field-type)))
                   (db/cast-placeholder *db* field-type)
                   "?")]
           ;;
          (if-let [kind (relation-key-kind field)]
            (let [rel (get-in schema [:filter-relations (keyword (name field))])
                  [sql d] (relation-exists rel (if (not-empty prefix) prefix (sql-quote (:entity/table schema))) constraints)]
              [(conj statements (cond->> sql (= :none kind) (str "not "))) (into data d)])
          (if (boolean? constraints)
            (if (= :_distinct field)
              [statements]
              [(conj statements (str \" (name field) \" " = " constraints))])
             ;;
            (if (is-relation? field)
              (if-not *deep* [statements data]
                      (let [[statements' data'] (query-selection->sql (get-in schema [:relations field]))]
                        [(into statements statements')
                         (into data data')]))

              (let [meta-key? (or (= :_join field)
                                  (#{:_limit :_offset :_order_by :_distinct} field))
                    skip-pred? (and *skip-left-where* left-relation? (not meta-key?))]
                (cond
                  skip-pred? [statements data]
                  :else
                  (case field
                    :_maybe
                    (if *ignore-maybe* [statements data]
                        (binding [*deep* false]
                          (let [[statements' data'] (query-selection->sql
                                                     (-> schema
                                                         (assoc :args constraints)
                                                         (dissoc :relations)))]
                            (if (empty? statements')
                              [statements data]
                              [(conj statements [:or statements'])
                               (into data data')]))))
                    :_join
                    [statements data]
                     ;;
                    :_and
                    (update
                     (reduce
                      (fn [[statements data] [statements' data']]
                        [(into statements statements')
                         (into data data')])
                      [[] data]
                      (map
                       (fn [constraint]
                         (let [schema' (assoc schema :args constraint)]
                           (query-selection->sql schema')))
                       constraints))
                     0
                     (fn [statements']
                       (if (empty? statements')
                         statements
                         (conj statements
                               (str
                                "("
                                (clojure.string/join
                                 " and "
                                 statements')
                                ")")))))
                     ;;
                    :_or
                    (let [parts (map
                                 (fn [constraint]
                                   (let [[s d] (query-selection->sql (assoc schema :args constraint))]
                                     [(if (next s) [(str "(" (clojure.string/join " and " s) ")")] s) d]))
                                 constraints)]
                      (cond
                        (empty? parts) [(conj statements "1=0") data]
                        (some (comp empty? first) parts) [statements data]
                        :else [(conj statements
                                     (str "(" (clojure.string/join " or " (mapcat first parts)) ")"))
                               (into data (mapcat second parts))]))
                     ;;
                    :_not
                    (if-not *deep*
                      [statements data]
                      (let [[statements' data'] (query-selection->sql
                                                 (assoc schema :args constraints))]
                        (if (empty? statements')
                          [statements data]
                          [(conj statements
                                 (str "not ("
                                      (clojure.string/join " and " statements')
                                      ")"))
                           (into data data')])))
                    (:_limit :_offset :_order_by :_distinct)
                    [statements data]
                    (if (and (some? constraints) (not (map? constraints)))
                      (throw (ex-info (str "Unknown predicate " (pr-str constraints) " on " (name field))
                                      {:code "UNKNOWN_OPERATOR" :operator constraints :path [:args field]}))
                      (reduce-kv
                       (fn [[statements' data'] cn cv]
                         (if (or
                              (and
                               (vector? cv)
                               (not-empty cv))
                              (and
                               (not (vector? cv))
                               (some? cv)))
                           (let [statement (case cn
                                             :_in (if-not (empty? cv)
                                                    (format "%s in (%s)" field' (clojure.string/join "," (repeat (count cv) ph)))
                                                    "")
                                             (:_nin :_not_in) (if-not (empty? cv)
                                                                (format "%s not in (%s)" field' (clojure.string/join "," (repeat (count cv) ph)))
                                                                "")
                                             (:_le  :_lte) (str field' " <= " ph)
                                             (:_ge  :_gte) (str field' " >= " ph)
                                             :_eq (str field' " = " ph)
                                             (:_neq :_ne) (str field' " != " ph)
                                             :_lt (str field' " < " ph)
                                             :_gt (str field' " > " ph)
                                             :_like (str field' " like " ph " escape '\\'")
                                             :_ilike (str field' " " (proto/like-operator *db* false) " " ph " escape '\\'")
                                             :_limit (str field' " limit ?")
                                             :_offset (str field' " offset ?")
                                             :_is_null (str field' (if cv " is null" " is not null"))
                                             :_is_not_null (str field' " is not null")
                                             (do
                                               (log/error {:id ::nested-condition-error
                                                           :data {:constraint cn
                                                                  :value cv
                                                                  :schema schema}}
                                                          "Unknown operator in predicate")
                                               (throw
                                                (ex-info
                                                 (str "Unknown operator " (pr-str cn)
                                                      " in predicate. Supported: "
                                                      ":_eq :_neq :_lt :_lte :_gt :_gte "
                                                      ":_in :_nin :_not_in :_like :_ilike "
                                                      ":_is_null :_is_not_null")
                                                 {:code "UNKNOWN_OPERATOR"
                                                  :operator cn
                                                  :path [:args]}))))
                                 data (case cn
                                        (:_in :_nin :_not_in) (into data'
                                                                    (if-let [e (get encoders field)]
                                                                      (map e cv)
                                                                      cv))
                                        (:_is_null :_is_not_null) data'
                                        (conj data' (if-let [e (get encoders field)]
                                                      (e cv)
                                                      cv)))]
                             [(conj statements' statement) data])
                           (case cn
                             :_eq [(conj statements' (format "%s is null" field')) data']
                             :_neq [(conj statements' (format "%s is not null" field')) data']
                             [statements' data'])))
                       [statements data]
                       constraints))))))))))
      [[] data]
      operators))))

(defn search-stack-args
  "Function takes table pile and root entity id and produces where statement"
  ([schema] (search-stack-args schema " and "))
  ([schema j]
   (letfn [(args-stack [{:keys [relations]
                         :as schema}]
             (let [[statements data] (query-selection->sql schema)
                   [statements' data']
                   (reduce
                    (fn [[s d] r]
                      (let [[s' d'] (args-stack r)]
                        [(if (not-empty s') (into s s') s)
                         (if (not-empty d') (into d d') d)]))
                    [[] []]
                    (vals relations))]
               [((fnil into []) statements statements')
                ((fnil into []) data data')]))]
     (let [[stack data] (args-stack schema)
           [stack data] (enhance/args *db* schema [stack data])]
       (log/trace {:id ::computed-args-stack
                   :data {:stack stack :params data}}
                  "Computed args stack")
       (when (not-empty stack)
         [(str/join
           " "
           (map-indexed
            (fn [idx statement]
              (if (vector? statement)
                (let [[j statements] statement
                      op (str/join
                          (case j
                            :or " or "
                            :and " and "))]
                  (str
                   (when-not (zero? idx)
                     (str op \space))
                   (str/join op statements)))
                (str (when-not (zero? idx) j) statement)))
            stack))
          data])))))

(def ^:private meta-arg-keys
  "Args keys that are query metadata (join/paging/ordering/distinct), not field predicates."
  #{:_join :_limit :_offset :_order_by :_distinct})

(defn relation-on-predicate
  "Builds a LEFT relation's JOIN ON predicate fragment and its params."
  [relation-schema]
  (let [where-args (not-empty (apply dissoc (:args relation-schema) meta-arg-keys))]
    (when (seq where-args)
      (let [synthetic (-> relation-schema
                          (assoc :args where-args)
                          (dissoc :relations))
            [statements data] (query-selection->sql synthetic)
            stringify (fn stringify [stmt]
                        (if (vector? stmt)
                          (let [[op nested] stmt
                                op-sql (case op :and " and " :or " or " " and ")]
                            (str "(" (clojure.string/join op-sql (map stringify nested)) ")"))
                          stmt))
            sql (clojure.string/join " and " (map stringify statements))]
        (when (and sql (not= sql ""))
          [sql (vec data)])))))

;;; ============================================================================
;;; Inner-relation root scoping via EXISTS
;;; ============================================================================

(defn sql-alias
  "A statement-unique SQL alias with prefix `p` (gensym-backed)."
  [p]
  (name (gensym (str "__" p))))

(defn relation-has-filter?
  "True when a relation's args carry actual field predicates (not just
   query metadata like :_limit / :_order_by)."
  [args]
  (boolean (seq (apply dissoc args meta-arg-keys))))

(defn relation-exists
  "[sql params] of an EXISTS keeping a parent row with a related row through `rel` (to-many, reference or self-reference) that matches `constraints`, inside the target's read guard."
  [rel parent-as constraints]
  (let [constraints (relation-args constraints (:arg-relations rel))
        ex (:entity/as rel)
        jx (:relation/as rel)
        [psql pdata] (relation-on-predicate (assoc rel :args constraints))
        [gsql gdata] (node-read-guard rel ex)]
    [(str "exists (select 1 from "
          (if (or (:ref? rel) (:recursion? rel))
            (str (sql-quote (:entity/table rel)) " " ex
                 " where " ex "._eid = " parent-as "." (sql-quote (:from/field rel)))
            (str (sql-quote (:relation/table rel)) " " jx
                 " join " (sql-quote (:entity/table rel)) " " ex
                 " on " ex "._eid = " jx "." (sql-quote (:to/field rel))
                 " where " jx "." (sql-quote (:from/field rel)) " = " parent-as "._eid"))
          (when (seq psql) (str " and (" psql ")"))
          (when (seq gsql) (str " and (" gsql ")"))
          ")")
     (vec (concat pdata gdata))]))

(defn relation-left?
  "True when a relation does not scope its parent; absent `:_join` is LEFT, unconditionally."
  [args]
  (or (contains? args :_maybe)
      (= :left (join-mode (:_join args)))))

(defn read-guard
  "`[sql params]` of `entity-id`'s :read RLS at `alias`, `1=0` when RLS applies but no guard does, nil when it does not apply."
  [entity-id alias]
  (when (rls-runtime/should-apply-guards? entity-id :read)
    (when-let [{:keys [enabled guards]} (get-in (deployed-schema) [entity-id :rls])]
      (when enabled
        (if-let [r (and (seq guards) (rls-runtime/compile-guards-to-sql alias guards :read))]
          [(:sql r) (vec (:params r))]
          ["1=0" []])))))

(defn op-expr
  "`[sql params]` of op `spec` as an aggregate over target alias `ex`, its filter as FILTER."
  [{f :fn field :field args :args rel :relation} ex]
  (let [[stmts data] (when args (query-selection->sql (assoc (selection->schema (:to rel) nil args) :entity/as ex)))
        col (when field (str ex "." (sql-quote (name field))))]
    [(str (case f
            :count (str "count(" ex "._eid)")
            :count_distinct (str "count(distinct " col ")")
            (str (name f) "(" col ")"))
          (when (seq stmts) (str " filter (where " (str/join " and " stmts) ")")))
     (vec data)]))

(defn op-source
  "`[from-sql conditions params]` joining op `spec`'s junction and target at `jx`/`ex`; `conditions` hold the target's read guard and `extra`."
  [{rel :relation} jx ex & extra]
  (let [{jt :relation/table tf :to/field et :entity/table to :to} rel
        [gsql gdata] (read-guard to ex)]
    [(str " from " (sql-quote jt) " " jx
          " join " (sql-quote et) " " ex " on " ex "._eid = " jx "." (sql-quote tf))
     (cond-> (vec extra) gsql (conj (str "(" gsql ")")))
     (vec gdata)]))

(defn where-sql [conditions]
  (when (seq conditions) (str " where " (str/join " and " conditions))))

(defn op-order-sql
  "`[sql params]` of op `spec` for the row at `parent-as`, a correlated scalar subquery."
  [spec parent-as]
  (let [jx (sql-alias "sj") ex (sql-alias "se")
        ff (get-in spec [:relation :from/field])
        [expr edata] (op-expr spec ex)
        [from conds sdata] (op-source spec jx ex (str jx "." (sql-quote ff) " = " parent-as "._eid"))]
    [(str "(select " expr from (where-sql conds) ")")
     (into edata sdata)]))

(defn op-order-join
  "`[join-sql value-sql params]` sorting the rows at `parent-as` by op `spec` through one grouped derived table."
  [spec parent-as]
  (let [jx (sql-alias "gj") ex (sql-alias "ge") gx (sql-alias "g")
        ff (get-in spec [:relation :from/field])
        [expr edata] (op-expr spec ex)
        [from conds sdata] (op-source spec jx ex)]
    [(str " left join (select " jx "." (sql-quote ff) " as p, " expr " as v" from (where-sql conds)
          " group by " jx "." (sql-quote ff) ") " gx " on " gx ".p = " parent-as "._eid")
     (if (#{:count :count_distinct} (:fn spec)) (str "coalesce(" gx ".v, 0)") (str gx ".v"))
     (into edata sdata)]))

(defn ops-statement
  "`[sql & params]` computing ops `specs` (one relation) for `parents`: one grouped statement, FILTER per op."
  [specs parents]
  (let [jx (sql-alias "oj")
        ex (sql-alias "oe")
        ff (get-in (first specs) [:relation :from/field])
        cols (map-indexed
              (fn [i spec]
                (let [[expr data] (op-expr spec ex)]
                  [(str expr " as op_" i) data]))
              specs)
        [from conds sdata] (op-source (first specs) jx ex
                                      (str jx "." (sql-quote ff) " in (" (str/join "," (map long parents)) ")"))]
    (into [(str "select " jx "." (sql-quote ff) " as op_parent, "
                (str/join ", " (map first cols))
                from (where-sql conds)
                " group by " jx "." (sql-quote ff))]
          (concat (mapcat second cols) sdata))))

(defn op-value [{f :fn field :field t :type {decoders :decoders} :relation} v]
  (cond
    (#{:count :count_distinct} f) (long (or v 0))
    (nil? v) nil
    (= :avg f) (bigdec (double v))
    (= :sum f) (bigdec v)
    (= "int" t) (long v)
    (= "float" t) (double v)
    :else (if-let [dec (get decoders field)] (dec v nil) v)))

(defn node-has-ops?
  "True when `schema` or any node below it carries :ops."
  [schema]
  (boolean (or (seq (:ops schema)) (some node-has-ops? (vals (:relations schema))))))

(defn pull-ops
  "`{parent-eid {alias value}}` of node `schema`'s :ops for `parents`, one statement per relation."
  [con {ops :ops} parents]
  (when (and (seq ops) (seq parents))
    (let [parents (distinct (map long parents))
          blank (into {} (map (fn [[a spec]] [a (op-value spec nil)])) ops)]
      (reduce
       (fn [acc specs]
         (let [query (ops-statement specs parents)
               _ (log/trace {:id ::ops-query
                             :data {:action :executed :subject :sql :phase :ops
                                    :sql (first query) :params (vec (rest query))}}
                            "Sending operations query")
               rows (sql/execute! con query core/*return-type*)]
           (reduce
            (fn [acc row]
              (update acc (long (:op_parent row)) merge
                      (into {} (map-indexed (fn [i {a :alias :as spec}]
                                              [a (op-value spec (get row (keyword (str "op_" i))))]))
                            specs)))
            acc
            rows)))
       (zipmap parents (repeat blank))
       (vals (group-by #(select-keys (:relation %) [:relation/table :from/field :to/field]) (vals ops)))))))

(defn inner-exists
  "`[sql params]` (or nil) of the correlated [NOT] EXISTS scoping `schema`'s INNER and NONE relations at `alias`."
  [schema alias]
  (let [preds
        (keep
         (fn [[_ rel]]
           (let [args (:args rel)]
             (when-not (relation-left? args)
               (let [ex (or (:entity/as rel) (sql-alias "ie"))
                     jx (sql-alias "ij")
                     [fsql fdata] (when (relation-has-filter? args)
                                    (relation-on-predicate rel))
                     [csql cdata] (inner-exists rel ex)
                     [gsql gdata] (node-read-guard rel ex)
                     needs-target? (or (seq fsql) (seq csql) (seq gsql))]
                 [(str (when (= :none (join-mode (:_join args))) "not ")
                       "exists (select 1 from "
                       (if (or (:recursion? rel) (:ref? rel))
                         (str (sql-quote (:entity/table rel)) " " ex
                              " where " ex "._eid = " alias "." (sql-quote (:from/field rel)))
                         (str (sql-quote (:relation/table rel)) " " jx
                              (when needs-target?
                                (str " join " (sql-quote (:entity/table rel)) " " ex
                                     " on " ex "._eid = " jx "." (sql-quote (:to/field rel))))
                              " where " jx "." (sql-quote (:from/field rel)) " = " alias "._eid"))
                       (when (seq fsql) (str " and (" fsql ")"))
                       (when (seq csql) (str " and " csql))
                       (when (seq gsql) (str " and (" gsql ")"))
                       ")")
                  (vec (concat fdata cdata gdata))]))))
         (:relations schema))]
    (when (seq preds)
      [(str/join " and " (map first preds)) (vec (mapcat second preds))])))

(defn refuse-none-slice!
  "Throws INVALID_JOIN when a sliced relation asks for `_join none`."
  ([schema] (refuse-none-slice! schema [:args]))
  ([schema path]
   (doseq [[k {:keys [args] :as rel}] (:relations schema)
           :let [path' (conj path (keyword (name k)))]]
     (when (= :none (join-mode (:_join args)))
       (throw (ex-info (str "_join none is not supported in slice (relation " (name k) ")")
                       {:code "INVALID_JOIN"
                        :rule "join_none_slice"
                        :relation (name k)
                        :value "none"
                        :path (conj path' :_join)})))
     (refuse-none-slice! rel path'))))

(defn refuse-filter-through-cut!
  "Throws INVALID_FILTER when args filter through a relation the same slice or purge cuts."
  ([schema op] (refuse-filter-through-cut! schema op [:args]))
  ([schema op path]
   (doseq [[k rel] (:relations schema)
           :let [k' (keyword (name k))]]
     (when (contains? (:filter-relations schema) k')
       (throw (ex-info (format "A %s filter through %s also picks which %s it cuts; put the condition on the relation line: ->%s (...)"
                               (name op) (name k') (name k') (name k'))
                       {:code "INVALID_FILTER"
                        :rule "filter_through_cut"
                        :relation (name k')
                        :path (conj path k')})))
     (when (= :purge op)
       (refuse-filter-through-cut! rel op (conj path k'))))))

(defn refuse-column-slice!
  "Throws INVALID_SLICE when a slice names a reference or a parent: those are columns, not links."
  [schema]
  (doseq [[k {:keys [ref? recursion?]}] (:relations schema)
          :when (or ref? recursion?)]
    (throw (ex-info (str "Slice cuts relation links; " (name k) " is a "
                         (if ref? "reference" "parent") " column, set it to null with sync")
                    {:code "INVALID_SLICE"
                     :rule "slice_column"
                     :relation (name k)
                     :path [:selections (keyword (name k))]}))))

(defn search-stack-from
  "Builds the FROM clause by joining schema's tables; returns `[tables from-sql
   join-on-data]`, `join-on-data` carrying LEFT JOIN ON-clause params in source
   order."
  [schema]
  (letfn [(targeting-args? [args]
            (when args
              (if (vector? args)
                (some targeting-args? args)
                (let [args' (dissoc args :_offset :_limit)
                      some-constraint? (not-empty (dissoc args' :_and :_or :_maybe :_join))]
                  (if some-constraint?
                    true
                    (some
                     targeting-args?
                     ((juxt :_and :_or :_maybe) args')))))))
          (targeting-schema? [[_ {:keys [args fields pinned]}]]
            (or
             pinned
             (targeting-args? args)
             (some targeting-args? (vals fields))))
          (find-arg-locations
            [zipper]
            (loop [location zipper
                   pinned-locations #{}]
              (if (clojure.zip/end? location) pinned-locations
                  (recur
                   (clojure.zip/next location)
                   (if (targeting-schema? (clojure.zip/node location))
                     (conj pinned-locations location)
                     pinned-locations)))))
          (find-end-locations
            [zipper]
            (let [arg-locations (find-arg-locations zipper)
                  targeted (reduce
                            (fn [locations location]
                              (loop [parent (clojure.zip/up location)
                                     locations locations]
                                (if (nil? parent) locations
                                    (recur (clojure.zip/up parent) (disj locations parent)))))
                            arg-locations
                            arg-locations)]
              (if-not (empty? targeted) targeted
                      [zipper])))
          ;; INNER default is deliberate — never default to LEFT here, tree ops break
          (->join [{:keys [args]}]
            (if (or (:_maybe args) (= :left (join-mode (:_join args :inner))))
              "left"
              "inner"))]
    (let [zipper (schema-zipper schema)
          {:keys [entity/as entity/table]
           rtable :relation/table
           ras :relation/as
           ttable :to/table
           falias :from/field
           talias :to/field} schema
          ;;
          reference? (= "_eid" talias)
          ;;
          join (->join schema)
          locations (find-end-locations zipper)
          [tables stack join-data] (reduce
                                    (fn [[tables stack join-data] location]
                                      (loop [[[_ parent] [_ current] :as nodes]
                                             (conj (vec (clojure.zip/path location)) (clojure.zip/node location))
                                             ;;
                                             tables tables
                                             stack stack
                                             join-data join-data]
                                        (if (empty? current)
                                          [(conj tables (:entity/as parent)) stack join-data]
                                          (let [{:keys [entity/as
                                                        entity/table]} parent
                                                ;;
                                                {as-child :entity/as
                                                 child-table :entity/table
                                                 as-link :relation/as
                                                 ff :from/field
                                                 tf :to/field
                                                 link-table :relation/table} current
                                                join (->join current)
                                                [on-pred on-data] (when (= "left" join)
                                                                    (relation-on-predicate current))
                                                on-suffix (if on-pred (str " and " on-pred) "")
                                                link-sql (if (= table link-table)
                                                           (format
                                                            "%s join \"%s\" %s on %s.%s=%s.%s%s"
                                                            join child-table as-child as (sql-quote ff) as-child "_eid" on-suffix)
                                                           (format
                                                            "%s join \"%s\" %s on %s._eid=%s.%s\n%s join \"%s\" %s on %s.%s=%s.%s%s"
                                                            join link-table as-link as as-link (sql-quote ff)
                                                            join child-table as-child as-link (sql-quote tf) as-child "_eid" on-suffix))
                                                next-tables (conj tables as)
                                                next-stack (conj stack link-sql)
                                                next-join-data (cond-> join-data
                                                                 (seq on-data) (into on-data))]
                                            (recur
                                             (rest nodes)
                                             next-tables
                                             next-stack
                                             next-join-data)))))
                                    [[] [] []]
                                    locations)
          stack (distinct stack)]
      [(distinct (mapv keyword (into (cond-> [as] rtable (conj rtable)) tables)))
       (if rtable
         (str
          \" rtable \" " as " ras
          " " join " join " \" ttable \" \space as \space " on "
          ras \. (sql-quote (if reference? falias talias)) \= as "._eid"
          \newline (clojure.string/join "\n" stack))
         (str/join "\n" (conj
                          ; stack
                         (distinct stack)
                         (str "\"" table "\" as " as))))
       join-data])))

;;; ============================================================================
;;; Pull Query Functions (Shared - Database-Agnostic)
;;; ============================================================================

(defn pull-query
  [{:keys [entity/as
           fields]
    talias :to/field
    falias :from/field
    ras :relation/as
    table :entity/table
    :as schema}
   found-records
   parents]
  (log/trace {:id ::pull-entity-for-parents
              :data {:table table
                     :parents (vec parents)
                     :found-records (vec found-records)}}
             "Pulling entity for parents")
  (let [level (-> schema (dissoc :relations) (update :args dissoc :_join))
        [_ from maybe-data] (search-stack-from level)
        _ (log/trace {:id ::pull-where-args :data {:table table}}
                     "Looking for WHERE args")
        [where d] (search-stack-args level)
        _ (log/trace {:id ::pull-parent-args :data {:table table}}
                     "Looking for PARENT args")
        [parented pd] (if (= talias "_eid")
                        (search-stack-args
                         (-> level
                             (dissoc :rls)
                             (assoc :args {:_eid {:_in parents}}
                                    :entity/as ras)))
                        (search-stack-args
                         (-> level
                             (dissoc :rls)
                             (assoc :args {(keyword falias) {:_in parents}}
                                    :entity/as ras))))
        [exists-sql exists-data] (inner-exists schema as)
        where (clojure.string/join " and " (remove clojure.string/blank? [where parented exists-sql]))
        data (reduce into [] (remove nil? [d pd exists-data]))
        {:keys [_limit _offset]} (:args schema)
        windowed? (or _limit _offset)
        [order-sql order-data] (if (term-order? level)
                                 (let [terms (order-terms level (get-in level [:args :_order_by]))]
                                   [(str "order by "
                                         (str/join ", " (map (fn [[sql dir]] (str sql (case dir :desc " desc nulls last" :asc " asc nulls first"))) terms)))
                                    (vec (mapcat #(nth % 2) terms))])
                                 [(modifiers-selection->sql (update level :args dissoc :_limit :_offset)) []])
        order-sql (cond-> order-sql windowed? (with-tiebreak as (get-in level [:args :_distinct])))
        partition-col (if (= talias "_eid") (str ras "._eid") (str ras \. (sql-quote falias)))
        base (str "select " (if (= talias "_eid")
                              (str ras "._eid as " (sql-quote falias))
                              (str ras \. (sql-quote falias) \, ras \. (sql-quote talias)))
                  (when-not (empty? fields) (str "," (extend-fields (keys fields) as)))
                  (when windowed?
                    (str ", row_number() over (partition by " partition-col
                         (when-not (clojure.string/blank? order-sql) (str " " order-sql))
                         ") as __rn"))
                  \newline "from " from
                  (when-not (clojure.string/blank? where) (str "\nwhere " where)))
        sql (if windowed?
              (let [lo (long (or _offset 0))]
                (str "select * from (" base ") __w\nwhere __w.__rn > " lo
                     (when _limit (str " and __w.__rn <= " (+ lo (long _limit))))
                     "\norder by __w.__rn"))
              (cond-> base
                (not (clojure.string/blank? order-sql)) (str \newline order-sql)))]
    (into [sql] (if windowed?
                  (concat order-data maybe-data data)
                  (concat maybe-data data order-data)))))

(defn deep-merge
  "Deep merge maps; nil values never overwrite populated ones."
  [& maps]
  (apply merge-with
         (fn [v1 v2]
           (cond
             (nil? v2) v1
             (nil? v1) v2
             (and (map? v1) (map? v2)) (deep-merge v1 v2)
             :else v2))
         maps))

(defn pull-cursors
  [con {:keys [entity/table]
        :as schema} found-records]
  (binding [*fetch-con* (or *fetch-con* con)]
    (let [zipper (schema-zipper schema)]
      (letfn [(location->cursor [location]
                (let [[field] (clojure.zip/node location)]
                  (conj (mapv key (clojure.zip/path location)) field)))
              (maybe-pull-children [result location]
                (loop [queries []
                       current-location (clojure.zip/down location)]
                  (if (nil? current-location)
                    (if (empty? queries) result
                        (let [results (map deref queries)]
                          (apply deep-merge results)))
                    (recur
                     (conj queries (parallel-task (process-node result current-location)))
                     (clojure.zip/right current-location)))))
              (process-root [_ location]
                (let [[_ {:keys [entity/table fields
                                 decoders recursions entity/as args]}] (clojure.zip/node location)
                      expected-start-result (fetch-on-conn
                                             {table (apply hash-map
                                                           (reduce
                                                            (fn [r d]
                                                              (conj r
                                                                    (:_eid d)
                                                                    (reduce
                                                                     (fn [data [k t]] (update data k t d))
                                                                     d
                                                                     decoders)))
                                                            []
                                                            (let [root-query (format
                                                                              "select %s from \"%s\"%s %s"
                                                                              (extend-fields (concat (keys fields) (map name recursions)))
                                                                              table
                                                                              (if-let [records (get found-records (keyword as))]
                                                                                (format
                                                                                 " where \"%s\"._eid in (%s) "
                                                                                 table
                                                                                 (clojure.string/join ", " records))
                                                                                "")
                                                                              (str
                                                                               (when (= found-records {})
                                                                                 (modifiers-selection->sql {:args args}))))
                                                                  result (sql/execute!
                                                                          *fetch-con*
                                                                          [root-query]
                                                                          core/*return-type*)]
                                                              (log/trace {:id ::root-query
                                                                          :data {:action :executed
                                                                                 :subject :sql
                                                                                 :phase :root
                                                                                 :table table
                                                                                 :sql root-query}}
                                                                         "Root query")
                                                              result)))})
                      start-result @expected-start-result
                      ops (when (seq (:ops (second (clojure.zip/node location))))
                            (fetch-on-conn
                             (pull-ops *fetch-con* (second (clojure.zip/node location))
                                       (keys (get start-result table)))))]
                  (->
                   (cond->
                    start-result
                     ops (update-in [:ops [::ROOT]] merge @ops))
                   (maybe-pull-children location))))
            ;;
              (process-related [result location]
                (let [[field {etable :entity/table
                              falias :from/field
                              talias :to/field
                              decoders :decoders
                              args :args
                              as :entity/as
                              ftable :from/table
                              cardinality :type
                              :as schema}] (clojure.zip/node location)
                      parents (keys (get result ftable))
                      cursor (location->cursor location)]
                  (log/trace {:id ::cursor-position
                              :data {:table table
                                     :cursor cursor
                                     :from-table ftable
                                     :parents (vec parents)}}
                             "Cursor position")
                  (if (not-empty parents)
                    (let [expected-result
                          (fetch-on-conn
                           (let [[_ {ptable :entity/table}] (clojure.zip/node (clojure.zip/up location))
                                 query (pull-query schema (get found-records (keyword as)) parents)
                                 _ (log/trace {:id ::pull-query
                                               :data {:action :executed
                                                      :subject :sql
                                                      :phase :pull
                                                      :table table
                                                      :sql (first query)
                                                      :params (vec (rest query))}}
                                              "Sending pull query")
                                 relations (cond->> (sql/execute! *fetch-con* query core/*return-type*)
                                              (or (:_limit args) (:_offset args))
                                              (mapv #(dissoc % :__rn)))

                                 talias' (keyword talias)
                                 falias' (keyword falias)
                                 data (reduce
                                       (fn [r d]
                                         (assoc r (get d talias')
                                                (reduce-kv
                                                 (fn [data k t] (update data k t d))
                                                 (dissoc d talias' falias')
                                                 decoders)))
                                       nil
                                       relations)
                                 result' (cond-> (update result etable
                                                         (fn [table]
                                                           (merge-with merge table data)))
                                           (seq (:ops schema))
                                           (update-in [:ops cursor] merge
                                                      (pull-ops *fetch-con* schema (map #(get % talias') relations))))]
                             (reduce
                              (fn [r {t talias'
                                      f falias'}]
                                (case cardinality
                                  :many
                                  (update-in r [ptable f field] (fnil conj []) [etable t])
                                  :one
                                  (assoc-in r [ptable f field] [etable t])))
                              result'
                              relations)))]
                      (maybe-pull-children @expected-result location))
                    (do
                      (log/trace {:id ::no-parents-found
                                  :data {:entity-table etable :result result}}
                                 "Couldn't find parents for relation")
                      result))))
              (process-node [result location]
                (if (= ::ROOT (key (clojure.zip/node location)))
                  (process-root result location)
                  (process-related result location)))]
        (let [result (doall (process-node nil zipper))]
          result)))))

(defn pull-roots [con schema found-records]
  ; (log/tracef "[%s] Found records\n%s" "fieoqj" (pprint found-records))
  (binding [*ignore-maybe* false]
    (let [db (pull-cursors con schema found-records)]
      (construct-response schema db found-records))))

(defn pull-references [tx reference-table references]
  (let [table-constraint-mapping
        (reduce-kv
         (fn [result constraints _]
           (update result
                   (set (keys constraints))
                   (fnil conj [])
                   constraints))
         nil
         references)]
    (reduce-kv
     (fn [result constraint-keys values]
       (let [multi? (> (count constraint-keys) 1)
             pattern (if multi?
                       (str \( (clojure.string/join ", " (repeat (count constraint-keys) \?)) \))
                       "?")
              ;; order is not guaranteed
             columns (map name constraint-keys)
             query (str
                    "select " (str/join ", " (conj columns "_eid"))
                    " from " \" reference-table \" " where "
                    \( (clojure.string/join "," columns) \)
                    " in (" (clojure.string/join ", " (repeat (count values) pattern)) ")")
             values' (if multi?
                       (map (apply juxt constraint-keys) values)
                       (map #(get % (first constraint-keys)) values))]
         (log/trace {:id ::pulling-references
                     :data {:reference-table reference-table
                            :constraint-keys (vec constraint-keys)
                            :values (vec values')
                            :sql query}}
                    "Pulling references")
         (let [data (sql/execute!
                     tx (into [query] values')
                     core/*return-type*)
               data' (reduce
                      (fn [r d]
                        (assoc r (dissoc d :_eid) (:_eid d)))
                      nil
                      data)]
            ; (log/tracef "Normalized reference data\n%s" (pprint data'))
           (reduce
            (fn [result constraint-data]
              (assoc result constraint-data (get data' constraint-data)))
            result
            values))))
     nil
     table-constraint-mapping)))

(declare execute-multi-row!)

(defn resolve-one-satellites
  "Resolves identity-less `:one` children to the already-linked row."
  [tx analysis]
  (let [idk (id/key)
        idcol (name idk)]
    (reduce-kv
     (fn [analysis {ltable :table
                    ftable :from/table
                    ttable :to/table
                    ffield :from/field
                    tfield :to/field} pairs]
       (let [indexed-children (set (vals (get-in analysis [:index ttable])))
             ;; parent tmp -> the constraint values that identified it
             tmp->cvals (reduce-kv
                         (fn [m cvals tmp]
                           (if (contains? m tmp) m (assoc m tmp cvals)))
                         {}
                         (get-in analysis [:index ftable]))
             candidates (keep
                         (fn [[ptmp ctmp]]
                           (when (and ctmp
                                      (not (contains? indexed-children ctmp))
                                      (nil? (get-in analysis [:entity ttable ctmp idk])))
                             (let [prow (get-in analysis [:entity ftable ptmp])
                                   pident (if (some? (get prow idk))
                                            {idk (get prow idk)}
                                            (get tmp->cvals ptmp))]
                               (when (and (seq pident)
                                          (every? some? (vals pident)))
                                 [pident ctmp]))))
                         pairs)
             by-shape (group-by (comp vec sort keys first) candidates)]
         (reduce-kv
          (fn [analysis shape entries]
            (let [cols (mapv name shape)
                  rows (mapv (fn [[pident _]] (mapv pident shape)) entries)
                  group-? (str "("
                               (clojure.string/join
                                " and " (map #(str "p.\"" % "\" = ?") cols))
                               ")")
                  select-p (clojure.string/join
                            ", " (map-indexed
                                  (fn [i c] (str "p.\"" c "\" as __p" i)) cols))
                  resolved (execute-multi-row!
                            tx rows (count cols)
                            (fn [n]
                              (str "select c.\"" idcol "\" as __cid, " select-p
                                   " from \"" ftable "\" p"
                                   " join \"" ltable "\" l on l.\"" ffield "\" = p.\"_eid\""
                                   " join \"" ttable "\" c on c.\"_eid\" = l.\"" tfield "\""
                                   " where " (clojure.string/join " or " (repeat n group-?)))))
                  cid-by-vals (reduce
                               (fn [m row]
                                 (assoc m
                                        (mapv #(get row (keyword (str "__p" %)))
                                              (range (count cols)))
                                        (get row :__cid)))
                               {}
                               resolved)]
              (reduce
               (fn [analysis [pident ctmp]]
                 (if-some [cid (get cid-by-vals (mapv pident shape))]
                   (do
                     (log/trace {:id ::satellite-resolved
                                 :data {:table ttable :tmp ctmp :id cid}}
                                "Resolved identity-less :one child to linked row")
                     (assoc-in analysis [:entity ttable ctmp idk] cid))
                   analysis))
               analysis
               entries)))
          analysis
          by-shape)))
     analysis
     (:relations/one analysis))))

(defn prepare-references
  "Resolves reference attribute values to the referenced rows' `_eid`s."
  [tx {:keys [reference]
       :as analysis}]
  (reduce
   (fn [analysis [reference-table pulled-references]]
     (log/trace {:id ::pulled-references
                 :data {:reference-table reference-table
                        :pulled pulled-references}}
                "Pulled references for table")
     (reduce-kv
      (fn [analysis constraint value]
        (let [rows (get-in analysis [:reference reference-table constraint])]
          (reduce
           (fn [analysis row]
             (log/trace {:id ::updating-row-reference
                         :data {:row row :value value}}
                        "Updating row reference")
             (assoc-in analysis (concat [:entity] row) value))
           analysis
           rows)))
      analysis
      pulled-references))
   analysis
   (mapv
    (fn [[reference-table references]]
      [reference-table (pull-references tx reference-table references)])
    reference)))

(defn chunk-rows-for
  [cols]
  (let [budget (- (proto/max-bind-params *db*) 64)]
    (max 1 (quot budget (max 1 cols)))))

(defn execute-multi-row!
  ([tx rows cols sql-for]
   (execute-multi-row! tx rows cols sql-for nil))
  ([tx rows cols sql-for trailing-params]
   (into []
         (mapcat (fn [chunk]
                   (let [sql (sql-for (count chunk))
                         params (cond-> (into [sql] cat chunk)
                                  (seq trailing-params) (into trailing-params))]
                     (log/trace {:id ::multi-row-chunk
                                 :data {:rows (count chunk) :sql sql}}
                                "Executing multi-row statement chunk")
                     (sql/execute! tx params :raw))))
         (partition-all (chunk-rows-for cols) rows))))

(defn read-scope-guard
  "Compiled :read RLS guard for `entity-id`'s table, or nil when the caller's
   read window is the whole table."
  [entity-id table]
  (when entity-id
    (let [{{:keys [enabled guards]} :rls} (deployed-schema-entity entity-id)]
      (when (and enabled (rls-runtime/should-apply-guards? entity-id :read))
        (or (rls-runtime/compile-guards-to-sql (str \" table \") guards :read)
            {:sql "1=0" :params []})))))

(defn write-mode
  "Statement shape `entity-id`'s CRUDOB grants permit: :upsert, :create-only or :update-only."
  [entity-id]
  (let [c? (access/entity-allows? entity-id #{:create :owns})
        u? (access/entity-allows? entity-id #{:update :owns})]
    (cond
      (and c? u?) :upsert
      c?          :create-only
      u?          :update-only
      :else       :upsert)))

(defn delete-by-from-side!
  "Deletes a link table's from-side rows (and `evict` targets' other holders) within the caller's guards; never widen the scope."
  ([tx table from current] (delete-by-from-side! tx table from current nil))
  ([tx table from current {to-field :to/field to-table :to/table guard :guard evict :evict
                           evict-table :evict/table evict-guards :evict/guards}]
   (let [scope (when guard
                 (str " and \"" to-field "\" in (select \"_eid\" from \"" to-table
                      "\" where " (:sql guard) ")"))
         evict-scope (when (seq evict-guards)
                       (str " and \"" from "\" in (select \"_eid\" from \"" evict-table "\" where "
                            (clojure.string/join " and " (map #(str "(" (:sql %) ")") evict-guards)) ")"))
         params (vec (:params guard))
         evict-params (into [] (mapcat :params) evict-guards)
         budget (max 1 (- (chunk-rows-for 1) (count params) (count evict-params)))
         evict (vec (distinct evict))
         in-list (fn [n] (str "(" (clojure.string/join ", " (repeat n \?)) ")"))
         delete! (fn [where args]
                   (let [sql (str "delete from \"" table "\" where " where)]
                     (log/trace {:id ::delete-from-side-chunk
                                 :data {:table table :rows (count args) :sql sql}}
                                "Deleting from-side rows (chunk)")
                     (sql/execute! tx (into [sql] args))))]
     (doseq [chunk (partition-all budget current)]
       (delete! (str "\"" from "\" in " (in-list (count chunk)) scope) (concat chunk params)))
     (doseq [chunk (partition-all budget evict)]
       (delete! (str "\"" to-field "\" in " (in-list (count chunk)) evict-scope)
                (concat chunk evict-params))))))

(defn occupied-from-side
  "From-side values still linked after a scoped delete — i.e. holding an
   occupant outside the caller's read window."
  [tx table from froms]
  (into #{}
        (mapcat (fn [chunk]
                  (let [sql (str "select \"" from "\" from \"" table "\" where \"" from "\" in ("
                                 (clojure.string/join ", " (repeat (count chunk) \?))
                                 ")")]
                    (map (comp first vals)
                         (sql/execute! tx (into [sql] chunk) :raw)))))
        (partition-all (chunk-rows-for 1) (distinct froms))))

(defn write-scope-guard
  "Compiled :write RLS guard for `entity-id`'s table; 1=0 when RLS is on and no
   guard covers :write."
  [entity-id table]
  (when entity-id
    (let [{{:keys [enabled guards]} :rls} (deployed-schema-entity entity-id)]
      (when (and enabled (rls-runtime/should-apply-guards? entity-id :write))
        (or (rls-runtime/compile-guards-to-sql (str \" table \") guards :write)
            {:sql "1=0" :params []})))))

(defn row-writable?
  "Whether `entity-id`'s row `id` is inside the bound principal's :write scope."
  [entity-id id]
  (let [{:keys [table]} (deployed-schema-entity entity-id)]
    (if-let [{:keys [sql params]} (write-scope-guard entity-id table)]
      (boolean
       (seq (sql/execute!
             (into [(str "select 1 from \"" table "\" where \"" (id/field) "\" = ? and (" sql ")") id]
                   params)
             :raw)))
      true)))

(defn readable-rows
  "The `ids` of `entity-id` whose rows pass the bound principal's :read guards."
  [entity-id ids]
  (let [{:keys [table] {:keys [enabled guards]} :rls} (deployed-schema-entity entity-id)]
    (if (and enabled (rls-runtime/should-apply-guards? entity-id :read))
      (let [{guard :sql params :params} (rls-runtime/compile-guards-to-sql "t" guards :read)
            by-arg (into {} (comp (remove nil?) (map (juxt (comp str id/coerce-arg) identity))) ids)
            budget (max 1 (- (chunk-rows-for 1) (count params)))]
        (if-not (and guard (seq by-arg))
          #{}
          (into #{}
                (mapcat (fn [chunk]
                          (keep (comp by-arg str first vals)
                                (sql/execute!
                                 (into (into [(str "select t.\"" (id/field) "\" from \"" table "\" t where t.\"" (id/field) "\" in ("
                                                   (str/join ", " (repeat (count chunk) \?)) ") and (" guard ")")]
                                             (map id/coerce-arg chunk))
                                       params)
                                 :raw))))
                (partition-all budget (vals by-arg)))))
      (set ids))))

(defn row-readable?
  "Whether `entity-id`'s row `id` passes the bound principal's :read guards."
  [entity-id id]
  (contains? (readable-rows entity-id [id]) id))

(defn rows-outside
  "The `ids` of `table` that fail `guard`; a guard evaluating to NULL fails it."
  [tx table ids guard]
  (let [sql (:sql guard)
        params (vec (:params guard))
        ids (into [] (comp (remove nil?) (distinct)) ids)
        budget (max 1 (- (chunk-rows-for 1) (count params)))]
    (into []
          (mapcat (fn [chunk]
                    (let [q (str "select _eid from \"" table "\" where _eid in ("
                                 (clojure.string/join ", " (repeat (count chunk) \?))
                                 ") and (" sql ") is not true")]
                      (map (comp first vals) (sql/execute! tx (into (into [q] chunk) params) :raw)))))
          (partition-all budget ids))))

(defn without-references
  "`schema` without its reference nodes, at every level."
  [schema]
  (update schema :relations
          (fn [rs] (not-empty (into {} (keep (fn [[k rel]] (when-not (:ref? rel) [k (without-references rel)]))) rs)))))

(defn purge-found
  "Rows a purge of `schema` deletes, {table {eid row}}: `found` without what only a reference reached."
  [con schema roots found]
  (if (some :ref? (tree-seq map? (comp vals :relations) schema))
    (pull-cursors con (without-references schema) roots)
    found))

(defn purge-deletes!
  "Runs the delete enhancement of every row a purge is about to remove; `found` is {table {eid row}}."
  [tx schema found]
  (let [entities (into {} (keep (fn [{t :entity/table e :entity}] (when (and t e) [t e])))
                       (tree-seq map? (comp vals :relations) schema))]
    (doseq [[table rows] found
            :let [entity-id (get entities table)]
            :when (and entity-id (enhance/delete-hook? entity-id))
            chunk (partition-all (chunk-rows-for 1) (keys rows))
            x (map (comp first vals)
                   (sql/execute! tx (into [(str "select \"" (id/field) "\" from \"" table "\" where _eid in ("
                                                (clojure.string/join ", " (repeat (count chunk) \?)) ")")]
                                          chunk)
                                 :raw))]
      (enhance/apply-delete entity-id {(id/key) x} tx))))

(defn slice-hooks!
  "Runs the slice enhancement at both ends of every relation a slice cuts."
  [{:keys [entity relations]} tx]
  (let [cut (reduce (fn [acc {:keys [relation] to :entity}]
                      (cond-> (update acc entity (fnil conj #{}) relation)
                        to (update to (fnil conj #{}) relation)))
                    {}
                    (vals relations))]
    (doseq [[entity-id relation-ids] cut]
      (enhance/apply-slice entity-id relation-ids tx))))

(defn assert-from-side-writable!
  "Throws ROW_FORBIDDEN unless every from-side row passes the origin's :write
   guard."
  [tx table from-table froms guard]
  (when-let [denied (seq (rows-outside tx from-table froms guard))]
    (log/info {:id   :synthigy.iam.access/access-denied
               :data {:action :denied :subject :request :kind :relation
                      :table table :from-table from-table
                      :rows (count denied)}}
              "RLS denied relation write — origin outside write scope")
    (throw
     (ex-info
      (str "You don't have sufficient privileges to write links of '"
           from-table "' — " (count denied) " row(s) outside your write scope")
      {:type ::enforce-link-access
       :code "ROW_FORBIDDEN"
       :table table
       :from-table from-table
       :denied (count denied)}))))

(defn throw-slot-occupied
  "Throws when a to-one slot's current occupant is outside the caller's read
   window — never silently evict it."
  [table]
  (throw
   (ex-info
    (format "Cannot assign relation '%s': its current value is outside your access scope" table)
    {:type ::slot-occupied
     :code "SLOT_OCCUPIED"
     :table table})))

(defn lock-rows!
  "Locks `ids` of `table` until commit where the engine has row locks; sorted, so two writers never deadlock."
  [tx table ids]
  (when-let [clause (proto/row-lock-clause *db*)]
    (doseq [chunk (partition-all (chunk-rows-for 1) (sort (distinct ids)))]
      (sql/execute! tx (into [(str "select _eid from \"" table "\" where _eid in ("
                                    (clojure.string/join ", " (repeat (count chunk) \?))
                                    ") order by _eid" clause)]
                             chunk)))))

(defn slot-owners
  "[owner target] pairs holding one of `tos` that `own?` does not claim."
  [tx table from to own? tos]
  (into []
        (comp (mapcat (fn [chunk]
                        (sql/execute! tx (into [(str "select \"" from "\" as owner, \"" to "\" as target"
                                                     " from \"" table "\" where \"" to "\" in ("
                                                     (clojure.string/join ", " (repeat (count chunk) \?)) ")")]
                                               chunk))))
              (map (juxt :owner :target))
              (remove own?))
        (partition-all (chunk-rows-for 1) (distinct tos))))

(defn evict-other-parents!
  "Unlinks each one-to-many child in `pairs` from every parent but its new one; throws for a parent the caller cannot read or write."
  [tx table from to from-entity from-table pairs]
  (let [rg (read-scope-guard from-entity from-table)
        wg (write-scope-guard from-entity from-table)
        guards (filterv some? [rg wg])
        scope (when (seq guards)
                (str " and \"" from "\" in (select \"_eid\" from \"" from-table "\" where "
                     (clojure.string/join " and " (map #(str "(" (:sql %) ")") guards)) ")"))
        scope-params (into [] (mapcat :params) guards)
        budget (max 1 (quot (- (chunk-rows-for 1) (count scope-params)) 3))]
    (doseq [chunk (partition-all budget pairs)]
      (sql/execute! tx (-> [(str "delete from \"" table "\" where \"" to "\" in ("
                                 (clojure.string/join ", " (repeat (count chunk) \?))
                                 ") and (\"" from "\", \"" to "\") not in (values "
                                 (clojure.string/join ", " (repeat (count chunk) "(?, ?)"))
                                 ")" scope)]
                           (into (map second) chunk)
                           (into cat chunk)
                           (into scope-params))))
    (when (seq guards)
      (when-let [kept (seq (map first (slot-owners tx table from to (set pairs) (map second pairs))))]
        (when (and rg (seq (rows-outside tx from-table kept rg)))
          (log/info {:id ::slot-occupied
                     :data {:table table}}
                    "One-to-many child held by a parent outside the caller's read window")
          (throw-slot-occupied table))
        (assert-from-side-writable! tx table from-table kept (or wg {:sql "1=0" :params []}))))))

(defn verify-links!
  "Throws when one write gives a one-to-one target or a one-to-many child two owners; answers the analysis."
  [{one :relations/one many :relations/many entity :entity :as analysis}]
  (doseq [[{:keys [cardinality] to-entity :to to-table :to/table} links] (concat one many)
          :when (#{"o2o" "o2m"} cardinality)
          [target holders] (group-by second links)
          :when (and target (< 1 (count (distinct (map first holders)))))]
    (let [named (or (id/extract (get-in entity [to-table target])) "one record")
          o2o? (= "o2o" cardinality)]
      (throw
       (ex-info
        (format "One write links %s %s to more than one row — %s"
                (or (:name (deployed-entity to-entity)) "the target") named
                (if o2o? "a one-to-one link has one owner" "a one-to-many child has one parent"))
        {:code "BAD_DATA_SHAPE"
         :rule (if o2o? "one_to_one_target" "one_to_many_child")
         :target named}))))
  analysis)

(defn assert-links-writable!
  "Throws ROW_FORBIDDEN unless every existing from-side row of this write's links is inside the caller's :write scope; one query per table."
  [tx {one :relations/one many :relations/many :keys [created guarded] :as analysis}]
  (doseq [[[from-entity from-table] froms] (reduce-kv (fn [m {from-entity :from from-table :from/table} bindings]
                                                        (update m [from-entity from-table] (fnil into #{}) (map first bindings)))
                                                      {}
                                                      (merge one many))
          :let [wg (write-scope-guard from-entity from-table)
                existing (when wg (seq (remove (into (get created from-table #{}) (get guarded from-table)) froms)))]
          :when existing]
    (assert-from-side-writable! tx from-table from-table existing wg))
  analysis)

(defn link-relations
  ([tx analysis] (link-relations tx analysis true))
  ([tx analysis stack?]
   (as-> analysis result
     ;; Link recursions
     (let [{:keys [:recursion]} result]
       (reduce-kv
        (fn [result table mapping]
          (reduce-kv
           (fn [result field bindings]
             (reduce-kv
              (fn [result parent children]
                (try
                  (let [sql (str "UPDATE \"" table "\" SET \"" (name field) "\" = ? WHERE \"_eid\" = ?")
                        bindings (partition 2 (interleave (repeat parent) children))
                        statement (sql/prepare tx [sql])]
                    (log/trace {:id ::adding-recursions
                                :data {:table table
                                       :bindings (apply str bindings)
                                       :sql sql}}
                               "Adding new recursions")
                    (sql/execute-batch! statement bindings core/*return-type*)
                    result)
                  (catch Throwable e
                    (log/error! {:id ::recursion-link-failed
                                 :msg "Couldn't set recursion references"
                                 :data {:table table
                                        :parent parent
                                        :children children
                                        :recursion recursion}}
                                e)
                    (throw e))))
              result
              bindings))
           result
           mapping))
        result
        recursion))
     ;; Link single relations — delete on from side, a one-to-one target moves
     ;; to the writer (last write wins), then link through execute multi
     (let [{:keys [:relations/one]} result
           xid-mode? (= :xid (id/key))]
       (reduce-kv
        (fn [result {:keys [table cardinality]
                     to :to/field
                     from :from/field
                     from-entity :from
                     from-table :from/table
                     to-entity :to
                     to-table :to/table} bindings]
          (let [current (set (map first bindings))
                cols (->> (if xid-mode? [from to "from_xid" "to_xid"] [from to])
                          (map #(str \" % \"))
                          (clojure.string/join ", "))
                row-? (if xid-mode? "(?, ?, ?, ?)" "(?, ?)")
                new (if xid-mode?
                      (->> bindings
                           (filter second)
                           (mapv (fn [[f t fx tx]] [f t fx tx])))
                      (->> bindings
                           (filter second)
                           (mapv (fn [[f t]] [f t]))))
                scope (when-let [guard (read-scope-guard to-entity to-table)]
                        {:to/field to :to/table to-table :guard guard})
                o2o? (and (not-empty new) (= "o2o" cardinality))
                targets (when o2o? (mapv second new))
                rg (when o2o? (read-scope-guard from-entity from-table))
                wg (write-scope-guard from-entity from-table)]
            (when o2o? (lock-rows! tx to-table targets))
            (log/trace {:id ::delete-old-one-relations
                        :data {:table table :from-side-count (count current) :targets (count targets)}}
                       "Deleting old one-relations")
            ;; the eviction is guarded in the DELETE itself — a holder outside the caller's window is never touched
            (delete-by-from-side! tx table from current
                                  (cond-> scope
                                    o2o? (assoc :to/field to :evict targets
                                                :evict/table from-table
                                                :evict/guards (filterv some? [rg wg]))))
            (when (and o2o? (or rg wg))
              (when-let [kept (seq (map first (slot-owners tx table from to (comp current first) targets)))]
                (when (and rg (seq (rows-outside tx from-table kept rg)))
                  (log/info {:id ::slot-occupied
                             :data {:table table}}
                            "One-to-one target held by a row outside the caller's read window")
                  (throw-slot-occupied table))
                (assert-from-side-writable! tx table from-table kept (or wg {:sql "1=0" :params []}))))
            (when (and scope (seq new))
              (when (seq (occupied-from-side tx table from (map first new)))
                (log/info {:id ::slot-occupied
                           :data {:table table}}
                          "To-one slot holds a row outside the caller's read window")
                (throw-slot-occupied table)))
            (execute-multi-row!
             tx new (if xid-mode? 4 2)
             (fn [n]
               (str "insert into \"" table "\" (" cols ") values "
                    (clojure.string/join ", " (repeat n row-?)))))
            result))
        result
        one))
     ;; Link many relations — a one-to-many child moves to the parent listing
     ;; it (last write wins), then one multi-row INSERT per chunk, as above.
     (let [{:keys [:relations/many]} result
           xid-mode? (= :xid (id/key))]
       (reduce-kv
        (fn [result {:keys [table cardinality]
                     to :to/field
                     from :from/field
                     from-entity :from
                     from-table :from/table
                     to-entity :to
                     to-table :to/table} bindings]
          (let [current (set (map first bindings))
                cols (->> (if xid-mode? [from to "from_xid" "to_xid"] [from to])
                          (map #(str \" % \"))
                          (clojure.string/join ", "))
                row-? (if xid-mode? "(?, ?, ?, ?)" "(?, ?)")
                suffix (when stack? " on conflict do nothing")
                new (if xid-mode?
                      (->> bindings
                           (filter second)
                           (mapv (fn [[f t fx tx]] [f t fx tx])))
                      (->> bindings
                           (filter second)
                           (mapv (fn [[f t]] [f t]))))
                pairs (when (= "o2m" cardinality)
                        (into [] (comp (map (fn [[f t]] [f t])) (distinct)) new))]
            (when (seq pairs) (lock-rows! tx to-table (map second pairs)))
            ;; sync replaces only the caller-visible window — never delete unscoped
            (when-not stack?
              (log/debug {:id ::delete-many-relations-overwrite
                          :data {:table table :from-side-count (count current)}}
                         "Deleting many-relations (overwrite mode)")
              (delete-by-from-side!
               tx table from current
               (when-let [guard (read-scope-guard to-entity to-table)]
                 {:to/field to :to/table to-table :guard guard})))
            (when (seq pairs)
              (evict-other-parents! tx table from to from-entity from-table pairs))
            (execute-multi-row!
             tx new (if xid-mode? 4 2)
             (fn [n]
               (str "insert into \"" table "\" (" cols ") values "
                    (clojure.string/join ", " (repeat n row-?))
                    suffix)))
            result))
        result
        many)))
   analysis))

(defn existing-keys
  "Constraint-key tuples of `rows` already in `table`, unscoped: hidden is not absent."
  [tx table constraint-keys rows]
  (let [cols (map #(str \" (name %) \") constraint-keys)
        tuple-? (str \( (str/join ", " (repeat (count constraint-keys) "?")) \))]
    (into #{}
          (map (apply juxt constraint-keys))
          (execute-multi-row!
           tx (mapv (apply juxt constraint-keys) rows) (count constraint-keys)
           (fn [n]
             (str "SELECT " (str/join ", " cols) " FROM \"" table "\" WHERE ("
                  (str/join ", " cols) ") IN (" (str/join ", " (repeat n tuple-?)) ")"))))))

(defn assert-created-writable!
  "Throws ROW_FORBIDDEN unless every row this write created, links included, is inside the caller's :write scope."
  [tx {:keys [created] :as analysis}]
  (doseq [[table eids] created
          :let [entity-id (get-in analysis [:entity/mapping table])
                guard (write-scope-guard entity-id table)]
          :when guard
          :let [denied (rows-outside tx table eids guard)]
          :when (seq denied)]
    (log/info {:id :synthigy.iam.access/access-denied
               :data {:action :denied :subject :request :kind :row :code "ROW_FORBIDDEN"
                      :entity entity-id :table table :rows (count denied)
                      :roles (vec (access/role-ids))}}
              "Denied row create")
    (throw (ex-info (str "You don't have sufficient privileges to create " (count denied)
                         " row(s) of '" table "' — outside your write scope")
                    {:type ::enforce-write-access
                     :code "ROW_FORBIDDEN"
                     :entity entity-id
                     :table table
                     :denied (count denied)})))
  analysis)

(defn store-entity-records
  [tx {:keys [entity constraint encrypted]
       :as analysis}]
  ; (def analysis analysis)
  (reduce-kv
   (fn [analysis entity-table rows]
     (reduce-kv
      (fn [analysis ks rows]
        (log/debug {:id ::storing-entity-rows
                    :data {:entity-table entity-table
                           :ks (vec ks)
                           :rows (vec rows)}}
                   "Storing entity table rows")

        (if (empty? rows)
          analysis

          (let [table-encrypted (get encrypted entity-table)
                rows-with-id (map (fn [[row-data tmp-id]]
                                    (let [entity-id (or (id/extract row-data)
                                                        (id/generate))
                                          entity-id (id/coerce-arg entity-id)
                                          row-data (assoc row-data (id/key) entity-id)
                                          ;; seal here, not via TypeCodec — the backends refuse encrypted cells
                                          row-data (if table-encrypted
                                                     (reduce
                                                      (fn [rd field]
                                                        (if (some? (get rd field))
                                                          (update rd field denc/seal-cell)
                                                          rd))
                                                      row-data
                                                      table-encrypted)
                                                     row-data)]
                                      [row-data tmp-id entity-id]))
                                  rows)

                id->tmpid (into {}
                                (map (fn [[_ tmp-id entity-id]] [entity-id tmp-id])
                                     rows-with-id))

                constraint-keys (reference-constraint-keys
                                 (get constraint entity-table) ks)

                constraint->tmpid (reduce
                                   (fn [m [row tmp-id _]]
                                     (let [cvals (select-keys row constraint-keys)]
                                       (if (and (not-empty constraint-keys)
                                                (not-empty cvals)
                                                (every? some? (vals cvals)))
                                         (assoc m cvals tmp-id)
                                         m)))
                                   {}
                                   rows-with-id)

                ks' (if (contains? ks (id/key))
                      ks
                      (conj (vec ks) (id/key)))

                row-data (if (empty? ks')
                           (repeat (count rows-with-id) [])
                           (map (apply juxt ks')
                                (map first rows-with-id)))

                columns-fn #(str \" (name %) \")
                ks-quoted (map columns-fn ks')

                entity-id (get-in analysis [:entity/mapping entity-table])
                entity-schema (when entity-id
                                (deployed-schema-entity entity-id))
                field-types (when entity-schema
                              (reduce-kv
                               (fn [m _ {:keys [key type]}]
                                 (assoc m key type))
                               {}
                               (:fields entity-schema)))

                id-field (id/field)

                placeholder-fn (fn [k]
                                 (let [field-type (get field-types k)]
                                   (if (and field-type
                                            (not (core/reference-type? field-type)))
                                     (proto/placeholder-for-type *db* field-type)
                                     "?")))
                values-? (str \( (str/join ", " (map placeholder-fn ks')) \))

                return-cols (distinct (concat [:_eid (id/key)] constraint-keys))
                return-sql (str/join ", " (map columns-fn return-cols))

                fields-to-update (remove
                                  (fn [k]
                                    (let [kname (name k)]
                                      (or (= kname id-field)
                                          (some #(= (name %) kname) constraint-keys))))
                                  ks')

                resolve-only? (link-only?
                               entity-schema
                               (get constraint entity-table) ks)
                link-only-gated? (boolean
                                 (when-let [gated (get-in analysis [:link-only entity-table])]
                                   (some gated (map second rows))))
                create-guard (write-scope-guard entity-id entity-table)
                existing (when (and create-guard (not resolve-only?) (seq constraint-keys)
                                    (= :upsert (write-mode entity-id)))
                           (existing-keys tx entity-table constraint-keys (map first rows-with-id)))
                [result created]
                (if resolve-only?
                  (let [key-cols   (map columns-fn constraint-keys)
                        tuple-?    (str \( (str/join ", " (repeat (count constraint-keys) "?")) \))
                        ckey-vals  (mapv (apply juxt constraint-keys)
                                         (map first rows-with-id))
                        {:keys [enabled guards]} (:rls entity-schema)
                        rls-read (when (and enabled (rls-runtime/should-apply-guards? entity-id :read))
                                   (or (rls-runtime/compile-guards-to-sql
                                        (str \" entity-table \")
                                        guards
                                        :read)
                                       {:sql "1=0" :params []}))
                        found (execute-multi-row!
                               tx ckey-vals (count constraint-keys)
                               (fn [n]
                                 (str "SELECT " return-sql
                                      " FROM \"" entity-table "\" WHERE ("
                                      (str/join ", " key-cols) ") IN ("
                                      (str/join ", " (repeat n tuple-?))
                                      ")"
                                      (when rls-read (str " AND (" (:sql rls-read) ")"))))
                               (:params rls-read))
                        found-set (set (map (fn [r] (mapv #(get r %) constraint-keys)) found))
                        missing (remove
                                 (fn [[row]]
                                   (contains? found-set
                                              (mapv #(get row %) constraint-keys)))
                                 rows-with-id)
                        ;; id-addressed misses mint stubs on purpose — deploy relies on forward references
                        ;; never create when RLS scoped the lookup — hidden is not absent
                        create? (and (not link-only-gated?) (nil? rls-read)
                                     (not= :update-only (write-mode entity-id)))
                        refused (when (and (not create?)
                                           (not= constraint-keys [(id/key)]))
                                  (let [subjects (get-in analysis [:subject entity-table] #{})]
                                    (filterv (fn [[_ tmp-id]] (contains? subjects tmp-id))
                                             missing)))
                        _ (when (seq refused)
                            (log/info {:id :synthigy.iam.access/access-denied
                                       :data {:action :denied
                                              :subject :request
                                              :kind :row
                                              :entity entity-id
                                              :table entity-table
                                              :roles (vec (access/role-ids))}}
                                      "Denied row create")
                            (throw (ex-info
                                    (str "You don't have privileges to create row(s) of '"
                                         entity-table "'")
                                    {:type ::enforce-write-access
                                     :code (if rls-read "ROW_FORBIDDEN" "CREATE_FORBIDDEN")
                                     :entity entity-id
                                     :table entity-table
                                     :rows (mapv (fn [[row]] (select-keys row constraint-keys))
                                                 refused)})))
                        created (when (and (seq missing) create?)
                                  (execute-multi-row!
                                   tx (map (apply juxt ks') (map first missing))
                                   (count ks')
                                   (fn [n]
                                     (str "INSERT INTO \"" entity-table "\" ("
                                          (str/join ", " ks-quoted) ") VALUES "
                                          (str/join ", " (repeat n values-?))
                                          " RETURNING " return-sql))))]
                    (log/trace {:id ::resolve-entity-group
                                :data {:entity-table entity-table
                                       :resolved (count found)
                                       :created (count (or created []))}}
                               "Resolved entity group — pointers, no upsert")
                    [(concat found created) created])
                  (let [mode (write-mode entity-id)
                        on-values (map columns-fn constraint-keys)
                        do-set (if (empty? fields-to-update)
                                 (str (columns-fn (keyword id-field)) "="
                                      (proto/excluded-ref *db* (str \" id-field \")))
                                 (str/join ", "
                                           (map (fn [col]
                                                  (let [quoted (columns-fn col)]
                                                    (str quoted "=" (proto/excluded-ref *db* quoted))))
                                                fields-to-update)))
                        {:keys [enabled guards]} (:rls entity-schema)
                        rls-write
                        (when (and enabled
                                   (not-empty constraint-keys)
                                   (rls-runtime/should-apply-guards? entity-id :write))
                          (or (rls-runtime/compile-guards-to-sql
                               (str \" entity-table \")
                               guards
                               :write)
                              {:sql "1=0" :params []}))
                        rls-where (when rls-write
                                    (str " WHERE " (:sql rls-write)))
                        rls-params (:params rls-write)]
                    ;; no constraint key = nothing to match = every row is a create
                    (when (and (= :update-only mode) (empty? constraint-keys))
                      (throw (ex-info
                              (str "You don't have privileges to create row(s) of '"
                                   entity-table "'")
                              {:type ::enforce-write-access
                               :code "CREATE_FORBIDDEN"
                               :entity entity-id
                               :table entity-table
                               :attempted (count row-data)
                               :written 0})))
                    (log/trace {:id ::store-entity-group
                                :data {:entity-table entity-table
                                       :constraint-keys (vec constraint-keys)
                                       :rows (count row-data)
                                       :mode mode
                                       :rls-where rls-where}}
                               "Storing entity group with order-independent mapping")
                    (let [qualified (str \" entity-table \" \.)
                          written
                          (case mode
                            ;; the conflict IS the denial — existing rows never return
                            :create-only
                            (execute-multi-row!
                             tx row-data (count ks')
                             (fn [n]
                               (str
                                "INSERT INTO \"" entity-table "\" ("
                                (str/join ", " ks-quoted) ") VALUES "
                                (str/join ", " (repeat n values-?))
                                (when (not-empty constraint-keys)
                                  (str " ON CONFLICT (" (str/join ", " on-values)
                                       ") DO NOTHING"))
                                " RETURNING " return-sql)))

                            ;; never an INSERT — absent rows never return
                            :update-only
                            (execute-multi-row!
                             tx row-data (count ks')
                             (fn [n]
                               (str
                                "WITH v (" (str/join ", " ks-quoted) ") AS (VALUES "
                                (str/join ", " (repeat n values-?)) ")"
                                " UPDATE \"" entity-table "\" SET "
                                (str/join ", "
                                          (map (fn [col]
                                                 (let [q (columns-fn col)]
                                                   (str q "=v." q)))
                                               (or (seq fields-to-update)
                                                   [(keyword id-field)])))
                                " FROM v"
                                " WHERE " (str/join
                                           " AND "
                                           (map (fn [k]
                                                  (let [q (columns-fn k)]
                                                    (str qualified q "=v." q)))
                                                constraint-keys))
                                (when rls-write (str " AND (" (:sql rls-write) ")"))
                                " RETURNING "
                                (str/join ", " (map #(str qualified (columns-fn %))
                                                    return-cols))))
                             rls-params)

                            (execute-multi-row!
                             tx row-data (count ks')
                             (fn [n]
                               (str
                                "INSERT INTO \"" entity-table "\" ("
                                (str/join ", " ks-quoted) ") VALUES "
                                (str/join ", " (repeat n values-?))
                                (when (not-empty constraint-keys)
                                  (str " ON CONFLICT (" (str/join ", " on-values)
                                       ") DO UPDATE SET " do-set
                                       rls-where))
                                " RETURNING " return-sql))
                             rls-params))
                          short (- (count row-data) (count written))]
                      ;; the absent rows ARE the denial — never look up which ones
                      (when (and (pos? short)
                                 (or rls-where (not= :upsert mode)))
                        (let [code (case mode
                                     :create-only "UPDATE_FORBIDDEN"
                                     :update-only (if rls-where
                                                    "ROW_FORBIDDEN"
                                                    "CREATE_FORBIDDEN")
                                     "ROW_FORBIDDEN")]
                          (log/info {:id   :synthigy.iam.access/access-denied
                                     :data {:action  :denied
                                            :subject :request
                                            :kind    :row
                                            :code    code
                                            :entity  entity-id
                                            :table   entity-table
                                            :roles   (vec (access/role-ids))}}
                                    "Denied row write")
                          (throw (ex-info
                                  (str "You don't have sufficient privileges to write "
                                       short " row(s) of '" entity-table "'")
                                  {:type ::enforce-write-access
                                   :code code
                                   :entity entity-id
                                   :table entity-table
                                   :attempted (count row-data)
                                   :written (count written)}))))
                      [written (case mode
                                 :create-only written
                                 :update-only nil
                                 (remove #(contains? existing (mapv % constraint-keys)) written))])))

                _ (log/trace {:id ::store-entity-result
                              :data {:entity-table entity-table :result result}}
                             "Stored entity group result")

                mapping (reduce
                         (fn [m result-row]
                           (let [entity-id (id/extract result-row)
                                 cvals (when (not-empty constraint-keys)
                                         (select-keys result-row constraint-keys))
                                 cvals-normalized (when cvals
                                                    (reduce-kv
                                                     (fn [m k v]
                                                       (assoc m k
                                                              (if (= k (id/key))
                                                                (id/coerce-arg v)
                                                                v)))
                                                     {}
                                                     cvals))
                                 tmp-id (or
                                         (get id->tmpid (id/coerce-arg entity-id))
                                         (get constraint->tmpid cvals)
                                         (get constraint->tmpid cvals-normalized))]
                             (if tmp-id
                               (do
                                 (log/trace {:id ::mapped-result
                                             :data {:entity-table entity-table
                                                    :id entity-id
                                                    :constraint cvals
                                                    :tmp-id tmp-id}}
                                            "Mapped result to tmp-id")
                                 (assoc m tmp-id result-row))
                               (do
                                 (log/error {:id ::map-result-failed
                                             :data {:entity-table entity-table
                                                    :id entity-id
                                                    :id-type (str (type entity-id))
                                                    :constraint cvals}}
                                            "CRITICAL: Failed to map result to tmp-id")
                                 (throw
                                  (ex-info
                                   (format "[%s] Failed to map database result to tmp-id" entity-table)
                                   {:entity-table entity-table
                                    :id entity-id
                                    :constraint cvals
                                    :result-row result-row}))))))
                         {}
                         result)]

            (log/trace {:id ::final-mapping-size
                        :data {:entity-table entity-table :size (count mapping)}}
                       "Final mapping size")
            (reduce-kv
             (fn [analysis tmp-id data]
               (log/trace {:id ::merging-updated-data
                           :data {:entity-table entity-table :tmp-id tmp-id :row data}}
                          "Merging updated data")
               (update-in analysis [:entity entity-table tmp-id] merge data))
             (cond-> analysis
               (and create-guard (seq created))
               (update-in [:created entity-table] (fnil into #{}) (map :_eid created))
               (and create-guard (not resolve-only?))
               (update-in [:guarded entity-table] (fnil into #{}) (map :_eid result)))
             mapping))))
      analysis
      (group-entity-rows rows)))
   analysis
   entity))

(defn set-entity
  "Sets (creates/updates) entity data."
  ([entity-id data]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (jdbc/with-transaction [tx connection]
       (set-entity tx entity-id data true))))
  ([entity-id data stack?]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (jdbc/with-transaction [tx connection]
       (set-entity tx entity-id data stack?))))
  ([tx entity-id data stack?]
   (letfn [(pull-roots [{:keys [root entity root/table]}]
             (log/trace {:id ::pull-root-after-mutation
                         :data {:entity-id entity-id :root root}}
                        "Pulling root entity after mutation")
             (if (sequential? root)
               (mapv #(get-in entity [table %]) root)
               (get-in entity [table root])))]
     (let [analysis (analyze-data tx entity-id data stack?)]
       (log/trace {:id ::store-analysis :data {:analysis analysis}}
                  "Storing based on analysis")
       (as-> analysis result
         (verify-links! result)
         (resolve-one-satellites tx result)
         (prepare-references tx result)
         (enhance-write tx result)
         (store-entity-records tx result)
         (project-saved-entities result)
         ;; never gate after link-relations — a denied write would still evict
         (assert-links-writable! tx result)
         (link-relations tx result stack?)
         (assert-created-writable! tx result)
         (pull-roots result))))))
