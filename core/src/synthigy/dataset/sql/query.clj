;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

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
;; See docs/core/synthigy/dataset/sql/query.md "Operation context & fetch
;; concurrency" — :parallel must never run inside a write transaction.

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
  "Schedule a recursive pull task; controls thread fanout only, acquires no
   connection itself."
  [& body]
  `(case *fetch-mode*
     :serial   (delay  ~@body)
     :parallel (future ~@body)))

;;; ============================================================================
;;; Schema Caching (Database-Agnostic)
;;; ============================================================================

;; Three deploy-coupled indexes (:entity :relation :attribute-key) share ONE
;; atom so a redeploy swaps them atomically — avoids a torn read across a
;; stale/fresh pair. See docs/core/synthigy/dataset/sql/query.md.
(defonce ^:private _indexes (atom nil))
(defonce ^:private _template-cache
  (atom (cache/ttl-cache-factory {} :ttl (* 30 60 1000))))

(defn normalize-entity-name
  "Normalize an entity/label reference for index lookup — delegates to
   `naming/normalize-name`, the same fn DDL derives table/column names from."
  [name]
  (normalize-name name))

(defn build-relation-index
  "Builds {relation-uuid -> {:entity entity-name :label to-label}} from the
   deployed model, or nil if no model."
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
    ;; Single reset! publishes all three indexes atomically — avoids a torn
    ;; read (see _indexes above).
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
  "Cache a resolved template. Returns the cached value.
   TTL 30 minutes. Cleared on schema deploy."
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
  "Returns {normalized-entity-name -> entity-id} index.
   Built automatically when schema is deployed."
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
  "Pick the closest match from `candidates` for `target`, if any is within
   3 edits AND the candidate is at most 2x the target's length. Returns
   nil when no good match exists."
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
    ;; Already a schema ID (UUID in :euuid mode, XID string in :xid mode)
    (contains? (schema/deployed-schema) entity-ref) entity-ref
    ;; UUID not found in deployed schema
    (uuid? entity-ref)
    (throw (ex-info (str "Unknown entity: " entity-ref)
                    {:code "UNKNOWN_ENTITY" :entity entity-ref}))
    ;; Human-readable name — normalize and look up
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
  "Returns {attr-xid-keyword -> field-keyword} flat map across all entities,
   used by translate-delta."
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

(defn delete-unique-args
  "Filters args to entity-schema's unique-constraint keys (+ id key) and
   builds the _eq predicate map for a single-row delete. Throws
   UNIQUE_KEY_REQUIRED when no unique key survives or a surviving value is
   nil — a delete predicate must uniquely name a row, never drop silently."
  [entity-schema args]
  (let [uniques (set (flatten ((comp :unique :constraints) entity-schema)))
        unique-attribute-keys (as-> (:fields entity-schema) result
                                (select-keys result uniques)
                                (vals result)
                                (conj (map :key result) (id/key)))
        filtered-args (select-keys args unique-attribute-keys)]
    (when (or (empty? filtered-args) (some nil? (vals filtered-args)))
      (throw (ex-info
              (str "Delete on " (:name entity-schema) " needs a non-nil unique key")
              {:code "UNIQUE_KEY_REQUIRED"
               :rule "delete_identity"
               :entity-name (:name entity-schema)
               :hint (str/join ", " (map name unique-attribute-keys))})))
    {:filtered-args filtered-args
     :predicate-args (reduce-kv (fn [acc k v] (assoc acc k {:_eq v})) nil filtered-args)}))

(defn assert-single-match!
  "Throws NON_UNIQUE_MATCH when a single-row delete's predicate matched more
   than one row — partial composite-unique args, or any future drift in nil
   predicate compilation, must never delete more than the one row named."
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
  "Validates + dedupes a vector-of-xids multirow delete. Throws
   BAD_DATA_SHAPE on a non-string/blank element, DELETE_BATCH_LIMIT above
   delete-batch-limit."
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
    ;; "transit" kept for back-compat only — no longer encoded/decoded, plain
    ;; string

(defn wrap-basic-fields
  "Formats field names as SQL identifiers, optionally table-prefixed, always
   appending `_eid`; double-quote quoting (see docs re: MySQL ANSI_QUOTES)."
  ([fields] (wrap-basic-fields fields nil))
  ([fields prefix]
   (if (not-empty prefix)
     (map #(str (when prefix (str prefix \.))
                (if (= (id/key) %) (id/field) (name %)))
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
  "Throws when the current role tries to WRITE a denied attribute (write side
   fails closed, unlike read-side silent strip)."
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
  "True when a write row carries only identity/constraint keys — the caller is
   linking an existing row, not writing one. ONE predicate for the access gate
   and for storage's resolve-only path; if they disagree, the gate admits rows
   storage then upserts."
  [entity-schema constraints ks]
  (let [declared (when entity-schema
                   (->> (:fields entity-schema)
                        (keep (fn [[k v]] (when-not (keyword? k) (:key v))))
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

        encoders (reduce
                  (fn [result field]
                    (let [t (get field->type field)]
                      (case t
                        ("boolean" "string" "int" "float" "json"
                                   "timestamp" "timeperiod" "currency"
                                   "uuid" "avatar" "hashed" "transit" nil) result
                        (assoc result field
                               (fn [v]
                                 (proto/encode *db* t v))))))
                  nil
                  (keys field->type))

        ;; *db* captured as a value — decoders may run in futures where the
        ;; dynamic binding isn't conveyed.
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
    :as schema} {:keys [counts numerics]
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
          (get-counts [cursor parent]
            (get-in counts [cursor parent]))
          (get-numerics [cursor parent]
            (when-some [data (get-in numerics [cursor parent])]
              {:_agg data}))
          (pull-reference [[table id] schema cursor]
            (let [data (merge
                        (select-keys (get-in db [table id]) (narrow schema))
                        (get-counts cursor id)
                        (get-numerics cursor id))
                  data' (reduce-kv
                         (fn [data' k v]
                           (cond
                             (list-reference? v)
                             (assoc data' k (mapv
                                             #(pull-reference % (get-in schema [:relations k]) (conj cursor k))
                                             (distinct v)))
                               ;;
                             (and (recursions k) (not= v id) (not= v [table id]))
                             ;; recursion's own schema (:relations k), not the
                             ;; parent's fields
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
          (if (or (:fields node) (:_count node))
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

(defn enhance-write
  "Applies audit enhancement (infrastructure), then custom write enhancements
   (domain), to a write analysis result."
  [tx result]
  (let [final (reduce-kv
               (fn [final _ entity-id]
                 (binding [*operation-rules* #{:create :update}]
                   (let [audited (enhance/apply-audit *db* entity-id final tx)
                         current (enhance/apply-write entity-id audited tx)]
                     current)))
               result
               (:entity/mapping result))]
    final))

(defn project-saved-entities
  "Converts temporary IDs to actual _eid/xid values for relations and recursions
   after database save. A tmp-id that never resolved is a DANGLING reference and
   throws here — downstream `(filter second)` would drop it silently."
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
                                 f {:key (keyword (normalize-name aname))
                                    (id/key) attr-id
                                    :type t
                                    :constraint constraint
                                    :skins (dk/label->skins aname)}]
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

            audit-augmentation (enhance/augment-schema *db* entity)
            fields (core/deep-merge base-fields (:fields audit-augmentation))

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
                                          :from from-id
                                          :from/field (entity->relation-field from)
                                          :from/table (entity->table-name from)
                                          :to (if (contains? (:clones model) to-id)
                                                (get-in model [:clones to-id :entity])
                                                to-id)
                                          :to/field (entity->relation-field to)
                                          :to/table (entity->table-name to)
                                          :table (relation->table-name relation)
                                          :type (case cardinality
                                                  ("m2o" "o2o") :one
                                                  ("m2m" "o2m") :many)})
                                  relations)))
                            {}
                            relations)

            relations (core/deep-merge base-relations (:relations audit-augmentation))
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
             ;; If there is no current cursor return schema
             (if-not current schema
                     (assoc-in
                 ;; Otherwise keep relations that have arguments
                      (shave-schema-relations schema)
                 ;; And associate current schema
                      [:relations current]
                 ;; With shaved schema for rest of cursor
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
       :entity/id entity}
      entity
      data'
      stack?)))
  ([_ current entity data stack?]
   (let [schema (deployed-schema)
         find-entity (memoize (fn [entity] (get schema entity)))
         type-mapping (memoize
                       (fn [{:keys [fields]}]
                         (reduce-kv
                          ;; enum -> "enum", NEVER the PG type name: since
                          ;; dataset 1.4.0 enums are TEXT and the type was
                          ;; dropped, so a `?::<enum_type>` cast now fails.
                          (fn [result _ {:keys [type key]}]
                            (assoc result key type))
                          {(id/key) "uuid"}
                          fields)))
         ;; Enum enforcement lives HERE (dataset 1.4.0, enum = TEXT) — see docs.
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
         ;; Per-call memoization of stable (entity, IAM-context) derivations
         ;; so transform-object reads precomputed values per row — see docs.
         entity-profile
         (memoize
          (fn [entity-id]
            (let [{:keys [relations fields recursions] :as e} (find-entity entity-id)
                  {refs true scalar-fields false}
                  (try
                    (group-by #(contains? % :reference/entity)
                              (vals (dissoc fields :modified_by :modified_on)))
                    (catch Throwable t
                      (log/error!
                       {:id ::field-grouping-failed
                        :data {:fields (vec (vals (dissoc fields :modified_by :modified_on)))}}
                       t)
                      (throw t)))
                  cs (get-constraints entity-id)
                  recursions-set (set recursions)
                  accessible-rel-keys
                  (into []
                        (keep (fn [k]
                                (when-not (contains? recursions-set k)
                                  (let [{:keys [relation to from]} (get relations k)]
                                    (when (and relation
                                               (access/relation-allows? relation [from to] #{:write :owns}))
                                      k)))))
                        (keys relations))
                  scalar-field-keys (conj (mapv :key scalar-fields) (id/key))
                  field->attribute (:field->attribute e)]
              {:entity            e
               :scalar-fields     scalar-fields
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
                              denied-write-keys inbound-cast]} profile
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
                                ;; "encrypted" is left as plaintext here on purpose — it is
                                ;; sealed at the storage boundary (store-entity-records);
                                ;; the backends refuse it as a TypeCodec type.
                                ("boolean" "string" "int" "float" "timeperiod" "currency" "uuid" "transit" "encrypted" nil) identity
                                "timestamp" (fn [v] (proto/encode *db* "timestamp" v))
                                "hashed" (fn [v] (when v (hashers/derive v)))
                                (fn [v]
                                  (when v
                                    (proto/encode *db* t v))))))))
                       fields-data
                       (keys fields-data))
                      ;; encrypted fields PRESENT in this payload — carried on `result`
                      ;; as :encrypted so store-entity-records knows what to seal.
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
                      ;; accessible-rel-keys is memoized per entity, unaware of THIS
                      ;; payload — a relation the role can't write and the payload
                      ;; never mentions must stay silent (see valid-relation-keys),
                      ;; but one the payload DOES reference must throw here, not
                      ;; disappear as a no-op write the caller believes succeeded
                      _ (doseq [k (keys data)]
                          (when (and (contains? relations k)
                                     (not (contains? recursions k))
                                     (not (contains? accessible-rel-set k)))
                            (let [{:keys [relation to from]} (get relations k)]
                              (relation-accessible? relation [from to] #{:write :owns}))))
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
                      ;; references resolve INTO this row and recursion parents
                      ;; UPDATE its FK — neither is a pointer, never drop these
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
                    ;; Take result and
                   (->
                    result
                      ;; in case of stack merge all collected fields data, or in case of sync replace
                      ;; last one wins
                    (update-in [:entity table id] (if stack? merge (fn [_ v] v)) fields-data)
                      ;; update entity table name to entity id
                    (assoc-in [:entity/mapping table] entity-id)
                      ;; encrypted fields on this table
                    (cond-> encrypted-fields
                      (update-in [:encrypted table] (fnil into #{}) encrypted-fields))
                      ;; update indexes
                    (update-in [:index table] merge (zipmap indexes (repeat id)))
                      ;; set constraints for this table for further processing
                    (assoc-in [:constraint table] constraints)
                      ;; check if this is link operation
                    (cond-> link-only-row? (update-in [:link-only table] (fnil conj #{}) id))
                      ;; object the caller submitted itself, not one the walk
                      ;; descended into — the only place this is knowable
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

(defn selection->schema
  ([entity-id selection]
   (selection->schema entity-id selection nil))
  ([entity-id selection args]
   (entity-accessible? entity-id *operation-rules*)
   (let [{relations :relations
          recursions :recursions
          fields :fields
          field->attribute :field->attribute
          _agg :_agg
          table :table
          rls :rls} (deployed-schema-entity entity-id)
         selection (flatten-selection selection)
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
         selection-operator-keys #{:count :_count :_agg}
         scalars (reduce-kv
                  (fn [r k [{args :args}]]
                    (cond
                      (and (valid-fields k)
                           (let [attr-id (get field->attribute k)]
                             (or (nil? attr-id)
                                 (access/attribute-allows? entity-id attr-id :read))))
                      (assoc r k args)
                      ;; attribute RBAC denial: silent strip, not an error (see
                      ;; docs)
                      (valid-fields k)            r
                      ;; alternate id key, wire-allowed no-op (see docs)
                      (#{:xid :euuid} k)          r
                      (selection-operator-keys k) r
                      (relation-key-set k)        r       ; legit relation — handled by objects branch
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
                 (if (some? v)
                   (assoc args k v)
                   args))
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
         ;; *db* captured as a value — decoders may run in futures where the
         ;; dynamic binding isn't conveyed.
         db *db*
         decoders (reduce
                   (fn [r k]
                     (if (valid-fields k)
                       (let [field-type (get type-mapping k)
                             ;; every transform takes (data row); all but a few
                             ;; ignore the row the apply sites always pass.
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
         ;; enum -> "enum" (a scalar type), so predicates bind a plain
         ;; string; the PG enum type no longer exists (dataset 1.4.0).
         field->type (reduce
                      (fn [result {f :key t :type}]
                        (assoc result f t))
                      nil
                      fields)
         ;; Per-field condition operators (`:_eq` etc.) are listed here
         ;; defensively though this walk never reaches them — see docs.
         args-operator-keys #{:_where :_or :_and :_not :_maybe :_count :_agg
                              :_limit :_offset :_order_by :_distinct :_join
                              :_eq :_neq :_lt :_lte :_le :_gt :_gte :_ge
                              :_in :_nin :_not_in :_like :_ilike :_is_null :_is_not_null}
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
                                             (map join-args (vals (select-keys args [:_where :_maybe :_count :_agg])))
                                             (mapcat #(map join-args %) (vals (select-keys args [:_or :_and])))))))
                                result
                                args)))]
                      (join-args args))
         ;; A "uuid" ATTRIBUTE type stays pass-through (backend-unsafe to
         ;; coerce blanket — uuid column on PG, TEXT on SQLite; the
         ;; structural id key below is handled separately). See docs.
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
         narrow-relations (reduce-kv
                           (fn [rs rkey rdata]
                             (if (valid-relations rkey)
                               (if (or
                                    (contains? args rkey)
                                    (contains? objects rkey)
                                    (contains? order-by rkey)
                                    (contains? distinct-on rkey)
                                    (contains? _agg rkey))
                                 (reduce
                                  (fn [final {:keys [selections alias]
                                              new-args :args}]
                                    (let [{:keys [relation from to ref? recursion?]} rdata]
                                      (or
                                       ref? recursion?
                                       (relation-accessible? relation [from to] #{:read}))
                                      (assoc final (keyword (or alias rkey))
                                             (merge
                                              (clojure.set/rename-keys rdata {:table :relation/table})
                                              {:relation/as (str (gensym "link_"))
                                               :entity/as (str (gensym "data_"))}
                                              (selection->schema
                                               (:to rdata) selections
                                               (cond-> new-args
                                                 (and
                                                  (not= (:from rdata) (:to rdata))
                                                  (contains? args rkey))
                                                 (merge (get args rkey))
                                                 ;;
                                                 (contains? order-by rkey)
                                                 (assoc :_order_by (get order-by rkey))
                                                 ;;
                                                 (contains? distinct-on rkey)
                                                 (assoc :_distinct (get distinct-on rkey))))))))
                                  rs
                                  (get objects rkey))
                                 rs)
                               rs))
                           nil
                           ;; Get all possible relations
                           (cond-> relations
                             (not-empty refs)
                             (as-> relations
                                   (reduce
                                    (fn [relations' {k :key
                                                     ref-entity :reference/entity}]
                                      (let [{ttable :table} (deployed-schema-entity ref-entity)
                                            alias-key (keyword (get-in objects [k 0 :alias] k))]
                                        (assoc relations' alias-key
                                               {:args (get-in objects [k 0 :args])
                                                :from entity-id
                                                :from/field (name k)
                                                :from/table table
                                                :to ref-entity
                                                :to/field "_eid"
                                                :to/table ttable
                                                :table table
                                                :ref? true
                                                :type :one})))
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
                                    recursions))))
         aggregate-keys [:_count :_agg]
         ;; typed error instead of the old `:relation/table nil` NPE (see docs)
         count-agg-rdata (fn [rkey]
                           (or (get relations rkey)
                               (throw (ex-info
                                       (str "Unknown relation " (pr-str rkey)
                                            " in selection on entity "
                                            (:name (deployed-schema-entity entity-id)))
                                       (let [candidates (map name (keys relations))]
                                         (cond-> {:code "UNKNOWN_ATTRIBUTE"
                                                  :entity (:name (deployed-schema-entity entity-id))
                                                  :attribute (name rkey)
                                                  :rule "schema_attribute"
                                                  :path [:selections]}
                                           (<= (count candidates) 200)
                                           (assoc :hint (suggest-similar (name rkey) candidates))))))))
         base-schema (as-> (hash-map
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
                            :rls rls) schema
                       ;; Handle aggregates
                       (if-not (some #(contains? selection %) aggregate-keys)
                         schema
                         (reduce-kv
                          (fn [schema operation fields]
                            (case operation
                              :_count
                              (reduce
                               (fn [schema {operations :selections}]
                                 (reduce-kv
                                  (fn [schema relation specifics]
                                    (let [rkey (keyword (name relation))
                                          rdata (count-agg-rdata rkey)
                                          relation (->
                                                    rdata
                                                    (dissoc :_count)
                                                    (dissoc :relations)
                                                    (clojure.set/rename-keys {:table :relation/table})
                                                    (merge (entity-serde (:to rdata)))
                                                    (assoc
                                                     :pinned true
                                                     :entity/table (:to/table rdata)
                                                     :relation/as (str (gensym "link_"))
                                                     :entity/as (str (gensym "data_"))))]
                                      (case specifics
                                        (nil [nil]) (assoc-in schema [:_count rkey] relation)
                                        (reduce
                                         (fn [schema {:keys [alias args]}]
                                           (let [akey (or alias rkey)]
                                             (assoc-in schema [:_count akey]
                                                       (cond-> relation
                                                         args (assoc :args (clojure.set/rename-keys args {:_where :_maybe}))))))
                                         schema
                                         specifics))))
                                  schema
                                  operations))
                               schema
                               fields)
                              ;; Aggregate Hoist principle (same as :_count) —
                              ;; see docs.
                              :_agg
                              (reduce
                               (fn [schema {operations :selections}]
                                 (reduce-kv
                                  (fn [schema relation entries]
                                    (let [rkey (keyword (name relation))
                                          rdata (count-agg-rdata rkey)
                                          base-rel (->
                                                    rdata
                                                    (dissoc :_agg)
                                                    (dissoc :relations)
                                                    (clojure.set/rename-keys {:table :relation/table})
                                                    (merge (entity-serde (:to rdata)))
                                                    (assoc
                                                     :pinned true
                                                     :entity/table (:to/table rdata)
                                                     :relation/as (str (gensym "link_"))
                                                     :entity/as (str (gensym "data_"))))]
                                      (reduce
                                       (fn [schema {entry-alias :alias
                                                    entry-args  :args
                                                    agg-specifics :selections}]
                                         (let [akey (keyword (or entry-alias rkey))
                                               ;; Same gensym as siblings; own
                                               ;; predicate.
                                               entry-rel (-> base-rel
                                                             (assoc :args (cond-> {:_join :LEFT}
                                                                            (seq entry-args) (merge entry-args))))
                                               schema    (assoc-in schema [:_agg akey] entry-rel)]
                                           (reduce-kv
                                            (fn [schema fkey fn-entries]
                                              (reduce
                                               (fn [schema fn-entry]
                                                 (let [operation (ffirst (:selections fn-entry))
                                                       field fkey]
                                                   (assoc-in schema [:_agg akey operation field] [fkey nil])))
                                               schema
                                               fn-entries))
                                            schema
                                            agg-specifics)))
                                       schema
                                       entries)))
                                  schema
                                  operations))
                               schema
                               fields)
                              ;;default
                              (reduce
                               (fn [schema {:keys [selections]}]
                                 (reduce-kv
                                  (fn [schema fkey selections]
                                    (let [fkey (keyword (name fkey))]
                                      (reduce
                                       (fn [schema {:keys [alias args]}]
                                         (let [field (or alias fkey)]
                                           (assoc-in schema [operation field]
                                                     [fkey (when args {:args args})])))
                                       schema
                                       selections)))
                                  schema
                                  selections))
                               schema
                               fields)))
                          schema
                          (select-keys selection aggregate-keys))))]
     ;; Absent-:_join is interpreted at read time by `relation-left?`, not
     ;; stamped here — see the FLAT-LEFT DECREE in docs.
     (enhance/apply-schema base-schema selection))))

;;; ============================================================================
;;; SQL Generation Functions (Shared - Database-Agnostic)
;;; ============================================================================

(defn j-and
  "Joins SQL condition strings with \" and \". [\"a\" \"b\"] => \"a and b\"."
  [statements]
  (clojure.string/join " and " statements))

(defn distinct->sql
  "Emits a `distinct on (\"f1\", \"f2\") ` prefix for the root SELECT.

   ponytail: root attributes only. Relation-scoped distinct-on isn't
   supported — the level-oriented pull builds one SELECT per level and
   never joins child tables into the root, so there's nothing to be
   distinct *on* across levels."
  [{{fields :_distinct} :args}]
  (when (seq fields)
    (str "distinct on ("
         (clojure.string/join ", " (map #(str \" (name %) \") fields))
         ") ")))

(defn modifiers-selection->sql
  ([{operators :args
     :as schema}]
   (let [limit (get operators :_limit)
         offset (get operators :_offset)
         limit-offset (when (or limit offset)
                        (proto/limit-offset-clause *db* limit offset))
         s (cond-> (list)
             limit-offset
             (conj limit-offset)
             ;;
             (contains? operators :_order_by)
             (conj
              (str
               "order by "
               (clojure.string/join
                ", "
                (letfn [(process-order-by [{:keys [entity/as]
                                            :as schema} order-by]
                          (reduce-kv
                           (fn [result field order-by']
                             (let [dir (cond (keyword? order-by') order-by'
                                             (string? order-by')  (keyword order-by')
                                             :else nil)]
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
                                (name field)
                                (case order
                                  :desc " desc nulls last"
                                  :asc " asc nulls first"))))
                   []
                   (process-order-by schema (get operators :_order_by))))))))]
     (if (empty? s) "" (clojure.string/join " " s)))))

(def ^:dynamic *ignore-maybe* true)
(def ^:dynamic *deep* true)

;; When true, skips `:_where` for LEFT relations already embedded in the
;; JOIN ON clause — re-emitting in WHERE would double-apply and turn LEFT
;; into effective INNER via NULL-rejection. See docs.
(def ^:dynamic *skip-left-where* false)

(defn query-selection->sql
  ([schema] (query-selection->sql schema []))
  ([{operators :args
     encoders :encoders
     prefix :entity/as
     relations :relations
     field-types :field-types
     :as schema} data]
   (let [is-relation? (set (keys relations))
         left-relation? (= "left"
                           (some-> (:_join operators)
                                   ((fn [v] (if (keyword? v) (name v) v)))
                                   str/lower-case))]
     (reduce
      (fn [[statements data] [field constraints]]
        (let [field-type (get field-types field)
              field' (if (not-empty prefix) (str prefix \. (name field)) (name field))
              field' (if (= "json" field-type)
                       (db/json-text *db* field')
                       field')
               ;; typed placeholders on backends where JDBC would otherwise
               ;; bind a plain varchar — see db/Dialect.
              ph (if (and field-type (not (scalar-types field-type)))
                   (db/cast-placeholder *db* field-type)
                   "?")]
           ;;
          (if (boolean? constraints)
             ;; Check if boolean constraint refers to _distinct
             ;; if it does ignore that operator
            (if (= :_distinct field)
              [statements]
              [(conj statements (str (name field) " = " constraints))])
             ;;
            (if (is-relation? field)
              (if-not *deep* [statements data]
                      (let [[statements' data'] (query-selection->sql (get-in schema [:relations field]))]
                        [(into statements statements')
                         (into data data')]))

               ;; LEFT relation + skip mode: drop everything but meta-keys —
               ;; predicates already moved to JOIN ON (see *skip-left-where*).
              (let [meta-key? (or (= :_join field)
                                  (#{:_limit :_offset :_order_by :_distinct} field))
                    skip-pred? (and *skip-left-where* left-relation? (not meta-key?))]
                (cond
                  skip-pred? [statements data]
                  :else
                  (case field
                     ;;
                    :_where
                    (cond
                      (not *deep*) [statements data]
                      :else
                      (let [[statements' data'] (query-selection->sql (assoc schema :args constraints))]
                        [(conj statements [:and statements'])
                         (into data data')]))
                     ;; Ignore for now...
                    :_maybe
                    (if *ignore-maybe* [statements data]
                        (binding [*deep* false]
                          (let [[statements' data'] (query-selection->sql
                                                     (-> schema
                                                         (assoc :args constraints)
                                                         (dissoc :relations)))]
                            [(conj statements [:or statements'])
                             (into data data')])))
                     ;; Ignore join
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
                       (conj statements
                             (str
                              "("
                              (clojure.string/join
                               " and "
                               statements')
                              ")"))))
                     ;;
                    :_or
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
                       (conj statements
                             (str
                              "("
                              (clojure.string/join
                               " or "
                               statements')
                              ")"))))
                     ;;
                    :_not
                     ;; negates the AND of its clause map; SQL 3-valued logic
                     ;; applies
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
                     ;; Ignore limit distinct offset
                    (:_limit :_offset :_order_by :_distinct)
                    [statements data]
                     ;; Default handlers
                    (if (keyword? constraints)
                      (case constraints
                        :is_null [(conj statements (format "%s is null" field')) data]
                        :is_not_null [(conj statements (format "%s is not null" field')) data])
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
                                             :_like (str field' " like " ph)
                                             :_ilike (str field' " " (proto/like-operator *db* false) " " ph)
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
                                                  :path [:_where]}))))
                                 data (case cn
                                        (:_in :_nin :_not_in) (into data'
                                                                    (if-let [e (get encoders field)]
                                                                      (map e cv)
                                                                      cv))
                                          ;; IS [NOT] NULL has no placeholder —
                                          ;; bind nothing.
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
                       constraints)))))))))
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
  #{:_join :_limit :_offset :_order_by :_distinct :_count :_agg})

(defn relation-on-predicate
  "Builds a LEFT relation's JOIN ON predicate fragment + data values; without it
   WHERE's null-rejection would collapse LEFT into INNER."
  [relation-schema]
  (let [args      (:args relation-schema)
        ;; legacy :_where wrapper unwrapped for back-compat
        legacy    (:_where args)
        flat      (apply dissoc args (cons :_where meta-arg-keys))
        where-args (cond
                     (and (seq flat) (seq legacy)) (merge legacy flat)
                     (seq flat) flat
                     (seq legacy) legacy
                     :else nil)]
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
;; Root-finding must NOT join through constrained relations (row
;; multiplication breaks _limit/_order_by) — EXISTS scopes without
;; multiplying. See docs "inner-exists" for full rationale.

(defn sql-quote
  "Double-quote a SQL identifier."
  [s]
  (str \" (str/replace (name s) "\"" "") \"))

(defn sql-alias
  "A statement-unique SQL alias with prefix `p` (gensym-backed)."
  [p]
  (name (gensym (str "__" p))))

(defn relation-has-filter?
  "True when a relation's args carry actual field predicates (not just
   query metadata like :_limit / :_order_by)."
  [args]
  (boolean (seq (apply dissoc args meta-arg-keys))))

(defn relation-left?
  "True when a relation does not scope its parent — see THE FLAT-LEFT DECREE in
   docs; absent `:_join` is LEFT, unconditionally. PUBLIC and the single source
   of truth."
  [args]
  (or (contains? args :_maybe)
      (nil? (:_join args))
      (boolean (#{:left :LEFT "left" "LEFT"} (:_join args)))))

(defn inner-exists
  "`[sql params]` (or nil) of EXISTS-scoped correlated subqueries for `schema`'s
   INNER relations at `alias` — see EXISTS root-scoping perf note in docs."
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
                     ;; Join through to target only if referenced — see EXISTS
                     ;; root-scoping perf note in docs.
                     needs-target? (or (seq fsql) (seq csql))]
                 [(str "exists (select 1 from "
                       (if (:recursion? rel)
                         ;; tree-cardinality: self-FK column, no junction
                         (str (sql-quote (:entity/table rel)) " " ex
                              " where " ex "._eid = " alias "." (sql-quote (:from/field rel)))
                         (str (sql-quote (:relation/table rel)) " " jx
                              (when needs-target?
                                (str " join " (sql-quote (:entity/table rel)) " " ex
                                     " on " ex "._eid = " jx "." (sql-quote (:to/field rel))))
                              " where " jx "." (sql-quote (:from/field rel)) " = " alias "._eid"))
                       (when (seq fsql) (str " and (" fsql ")"))
                       (when (seq csql) (str " and " csql)) ")")
                  (vec (concat fdata cdata))]))))
         (:relations schema))]
    (when (seq preds)
      [(str/join " and " (map first preds)) (vec (mapcat second preds))])))

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
                      some-constraint? (not-empty (dissoc args' :_and :_or :_where :_maybe :_join))]
                  (if some-constraint?
                    true
                    (some
                     targeting-args?
                     ((juxt :_and :_or :_where :_maybe) args')))))))
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
          ;; INNER default deliberately NOT touched by the FLAT-LEFT decree —
          ;; see docs "search-stack-from's ->join default".
          (->join [{:keys [args]}]
            (if (:_maybe args) "left"
                (str/lower-case (name (:_join args :INNER)))))]
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
                                          ;; Return final result
                                          [(conj tables (:entity/as parent)) stack join-data]
                                          ;; Otherwise recur
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
                                                ;; LEFT-relation predicates land here,
                                                ;; embedded in the JOIN ON clause so
                                                ;; the LEFT semantic survives.
                                                [on-pred on-data] (when (= "left" join)
                                                                    (relation-on-predicate current))
                                                on-suffix (if on-pred (str " and " on-pred) "")
                                                link-sql (if (= table link-table)
                                                           (format
                                                            "%s join \"%s\" %s on %s.%s=%s.%s%s"
                                                            join child-table as-child as ff as-child "_eid" on-suffix)
                                                           (format
                                                            "%s join \"%s\" %s on %s._eid=%s.%s\n%s join \"%s\" %s on %s.%s=%s.%s%s"
                                                            join link-table as-link as as-link ff
                                                            join child-table as-child as-link tf as-child "_eid" on-suffix))
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
          ras \. (if reference? falias talias) \= as "._eid"
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
        order-sql (modifiers-selection->sql (update level :args dissoc :_limit :_offset))
        partition-col (if (= talias "_eid") (str ras "._eid") (str ras \. falias))
        base (str "select " (if (= talias "_eid")
                              (str ras "._eid as " falias)
                              (str ras \. falias \, ras \. talias))
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
    (into [sql] ((fnil into []) maybe-data data))))

(defn deep-merge
  "Deep merge multiple maps. nil values do not overwrite populated
   ones — important when parallel futures merge their results and one
   future found no rows (yielding nil) while another found data."
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
              (pull-counts [result location]
                (let [[_ {as :entity/as
                          counted :_count
                          :as schema}] (clojure.zip/node location)
                      root? (nil? (clojure.zip/up location))
                      parents (let [ptable (if root?
                                             (-> location clojure.zip/node
                                                 second :entity/table)
                                             (-> location clojure.zip/up
                                                 clojure.zip/node
                                                 second :from/table))]
                                (keys (get result ptable)))]
                  (if-not (contains? schema :_count)
                    nil
                    (fetch-on-conn
                     (let [;; Aggregate Hoist: group :_count entries sharing a gensym pair into one LEFT JOIN — see docs.
                           join-groups (group-by (fn [[_ s]]
                                                   [(:relation/as s) (:entity/as s)])
                                                 counted)
                          ;; One rep per group, forced :_join :LEFT (count
                          ;; preserves parents-with-zero); per-entry predicates
                          ;; become case-when, not JOIN ON.
                           join-relations (into {}
                                                (map (fn [[_ entries]]
                                                       (let [[k entry-schema] (first entries)]
                                                         [k (-> entry-schema
                                                                (assoc :pinned true)
                                                                (assoc-in [:args :_join] :LEFT))])))
                                                join-groups)
                           schema (-> schema
                                      (assoc :relations join-relations)
                                      (dissoc :_count :fields)
                                      (cond-> (and parents (not root?))
                                        (update :args assoc-in [:_eid :_in] parents)))
                           [_ from] (search-stack-from schema)
                          ;; SELECT columns walk every entry, not the deduped
                          ;; join set; predicate stmts AND-joined for the
                          ;; case-when condition.
                           [count-selections from-data]
                           (reduce-kv
                            (fn [[statements data] as {etable :entity/as :as entry-schema}]
                              (let [t (name as)
                                    [stmts stmt-data] (query-selection->sql entry-schema)
                                    cond-stmt (case (count stmts)
                                                0 nil
                                                1 (first stmts)
                                                (str "(" (str/join " and " stmts) ")"))]
                                [(conj statements
                                       (if (nil? cond-stmt)
                                         (format "count(distinct %s._eid) as %s"
                                                 etable t)
                                         (format "count(distinct case when %s then %s._eid end) as %s"
                                                 cond-stmt etable t)))
                                 (if (seq stmt-data) (into data stmt-data) data)]))
                            [[] []]
                            counted)
                          ;; [where where-data]  (search-stack-args schema)
                          ;; TODO - ignore where for now, as it should be part
                          ;; of count-selections
                          ;; Root count has no parent JOIN, so matched root
                          ;; _eids become an explicit IN predicate — see docs.
                           [where where-data] (if (and root? (seq parents))
                                                [(str as "._eid in ("
                                                      (str/join "," (repeat (count parents) "?"))
                                                      ")")
                                                 (vec parents)]
                                                [nil nil])
                          ;;
                           [query-string :as query]
                           (as->
                            (format
                             "select %s._eid as parent_id, %s\nfrom %s"
                             as (str/join ", " count-selections) from)
                            query
                            ;;
                             (if-not where
                               query
                               (str query \newline "where " where))
                            ;;
                             (str query \newline
                                  (format "group by %s._eid" as))
                            ;;
                             (reduce into [query] (remove nil? [from-data where-data])))
                          ;;
                           _ (log/trace {:id ::counts-aggregate-query
                                         :data {:action :executed
                                                :subject :sql
                                                :phase :counts-aggregate
                                                :table table
                                                :sql query-string
                                                :from-data from-data
                                                :where-data where-data}}
                                        "Sending counts aggregate query")
                           result (sql/execute! *fetch-con* query core/*return-type*)]
                       (reduce
                        (fn [r {:keys [parent_id]
                                :as data}]
                          (assoc r parent_id {:_count (dissoc data :parent_id)}))
                        nil
                        result))))))
              (pull-numerics
                [result location]
                (let [[_ {as :entity/as
                          :as schema}] (clojure.zip/node location)
                      schema (dissoc schema :fields)
                      numerics (get schema :_agg)
                      root? (nil? (clojure.zip/up location))
                      parents (when root?
                                (keys (get result (-> location clojure.zip/node
                                                      second :entity/table))))]
                  (cond
                    (empty? numerics) nil
                    :else
                    (fetch-on-conn
                     (let [join-groups (group-by
                                        (fn [[_ rdata]]
                                          [(:relation/as rdata) (:entity/as rdata)])
                                        numerics)
                          ;; One rep per group with bare JOIN ON; predicates
                          ;; stay on each entry for case-when below.
                           join-relations (into {}
                                                (map (fn [[_ entries]]
                                                       (let [[k rdata] (first entries)]
                                                         [k (assoc rdata :args {:_join :LEFT})])))
                                                join-groups)
                           aggregate-schema (-> schema
                                                (assoc :relations join-relations)
                                                (dissoc :_agg :_count))
                           [numerics-selections numerics-data]
                           (reduce-kv
                            ;; SELECT columns walk every entry (akey),
                            ;; not the deduped join set — each entry
                            ;; contributes its own per-fn-per-attr columns
                            ;; with its own case-when condition derived
                            ;; from its own :args.
                            (fn [result rkey {ent-as :entity/as
                                              :as rdata}]
                              (reduce-kv
                               (fn [result operation definition]
                                 (reduce-kv
                                  (fn [[statements data] target-key [field-key _field-args]]
                                    (let [[stmts stmt-data] (query-selection->sql rdata)
                                          cond-stmt (case (count stmts)
                                                      0 nil
                                                      1 (first stmts)
                                                      (str "(" (str/join " and " stmts) ")"))]
                                      [(conj statements
                                             (format
                                              "%s(%s) as %s$%s$%s"
                                              (case operation
                                                :min "min"
                                                :max "max"
                                                :avg "avg"
                                                :sum "sum")
                                              (if (nil? cond-stmt)
                                                (str ent-as "." (name field-key))
                                                (str "case when " cond-stmt
                                                     " then " ent-as "." (name field-key)
                                                     " else null end"))
                                              (name rkey) (name operation) (name target-key)))
                                       (if (seq stmt-data) (into data stmt-data) data)]))
                                  result
                                  definition))
                               result
                               (select-keys rdata [:min :max :avg :sum])))
                            [[] []]
                            numerics)
                           [_ from] (search-stack-from aggregate-schema)
                          ;; Root `_agg` scoped to matched root _eids; nested
                          ;; `_agg` keeps its relation-args WHERE.
                           [where where-data] (if (and root? (seq parents))
                                                [(str as "._eid in ("
                                                      (str/join "," (repeat (count parents) "?"))
                                                      ")")
                                                 (vec parents)]
                                                (search-stack-args aggregate-schema))
                           ;;
                           [query-string :as query]
                           (as->
                            (format
                             "select %s._eid as parent_id, %s\nfrom %s"
                             as (str/join ", " numerics-selections) from)
                            query
                            ;;
                             (if-not where
                               query
                               (str query \newline "where " where))
                             (str query \newline
                                  (format "group by %s._eid" as))
                             (reduce into [query] (remove nil? [numerics-data where-data])))
                           ;;
                           _ (log/trace {:id ::numerics-aggregate-query
                                         :data {:action :executed
                                                :subject :sql
                                                :phase :numerics-aggregate
                                                :table table
                                                :sql query-string
                                                :numerics-data numerics-data
                                                :where-data where-data}}
                                        "Sending numerics aggregate query")
                           result (sql/execute! *fetch-con* query core/*return-type*)]
                       (reduce
                        (fn [r {:keys [parent_id]
                                :as data}]
                          (assoc r parent_id
                                 (reduce-kv
                                  (fn [r k v]
                                    (let [[rkey operation k] (str/split (name k) #"\$")
                                          v' (if (number? v) (bigdec v) v)]
                                      (assoc-in r [(keyword rkey) (keyword k) (keyword operation)] v')))
                                  nil
                                  (dissoc data :parent_id))))
                        nil
                        result))))))
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
                                                                               ;; TODO - test if this
                                                                               ;; is necessary
                                                                               ;; This maybe obsolete
                                                                               ;; since we already
                                                                               ;; know what records
                                                                               ;; to pull and in which
                                                                               ;; order
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
                      counts (pull-counts start-result location)
                      numerics (pull-numerics start-result location)]
                  (->
                   (cond->
                    start-result
                     counts (update-in [:counts [::ROOT]] merge @counts)
                     numerics (update-in [:numerics [::ROOT]] merge @numerics))
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
                                                ;; TODO - Transform data here
                                                (reduce-kv
                                                 (fn [data k t] (update data k t d))
                                                 (dissoc d talias' falias')
                                                 decoders)))
                                       nil
                                       relations)
                                 result' (update result etable
                                                 (fn [table]
                                                   (merge-with merge table data)))]
                             (reduce
                              (fn [r {t talias'
                                      f falias'}]
                                (case cardinality
                                  :many
                                  (update-in r [ptable f field] (fnil conj []) [etable t])
                                  :one
                                  (assoc-in r [ptable f field] [etable t])))
                              result'
                              relations)))
                          expected-counts (pull-counts result location)
                          expected-numerics (pull-numerics result location)]
                      (cond->
                       (maybe-pull-children @expected-result location)
                        expected-counts (update-in [:counts cursor] merge @expected-counts)
                        expected-numerics (update-in [:numerics cursor] merge @expected-numerics)))
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
  "Function used to determine o2o related data xid if no
  unique constraint pair was provided."
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
  "Looks up referenced releation. User, group, role etc..."
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
  "Statement shape `entity-id`'s CRUDOB grants permit: :upsert (C+U),
   :create-only or :update-only. Holding neither keeps :upsert — that row only
   reaches the writer as link-only rows, which mint pointers under
   :read by design."
  [entity-id]
  (let [c? (access/entity-allows? entity-id #{:create :owns})
        u? (access/entity-allows? entity-id #{:update :owns})]
    (cond
      (and c? u?) :upsert
      c?          :create-only
      u?          :update-only
      :else       :upsert)))

(defn delete-by-from-side!
  "Deletes a link table's from-side rows; with `guard` only the rows whose
   to-side is inside the caller's read window (never widen this — an unscoped
   delete destroys links the caller cannot see)."
  ([tx table from current] (delete-by-from-side! tx table from current nil))
  ([tx table from current {to-field :to/field to-table :to/table guard :guard}]
   (let [scope (when guard
                 (str " and \"" to-field "\" in (select \"_eid\" from \"" to-table
                      "\" where " (:sql guard) ")"))
         params (vec (:params guard))
         budget (max 1 (- (chunk-rows-for 1) (count params)))]
     (doseq [chunk (partition-all budget current)]
       (let [sql (str "delete from \"" table "\" where \"" from "\" in ("
                      (clojure.string/join ", " (repeat (count chunk) \?))
                      ")" scope)]
         (log/trace {:id ::delete-from-side-chunk
                     :data {:table table :rows (count chunk) :sql sql}}
                    "Deleting from-side rows (chunk)")
         (sql/execute! tx (into (into [sql] chunk) params)))))))

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
  "Whether `entity-id`'s row `id` is inside the bound principal's :write scope;
   true whenever no guard applies. Row scope only — RBAC is entity-allows?."
  [entity-id id]
  (let [{:keys [table]} (deployed-schema-entity entity-id)]
    (if-let [{:keys [sql params]} (write-scope-guard entity-id table)]
      (boolean
       (seq (sql/execute!
             (into [(str "select 1 from \"" table "\" where \"" (id/field) "\" = ? and (" sql ")") id]
                   params)
             :raw)))
      true)))

(defn assert-from-side-writable!
  "Throws ROW_FORBIDDEN unless every from-side row passes the origin's :write
   guard."
  [tx table from-table froms guard]
  (let [sql (:sql guard)
        params (vec (:params guard))
        froms (into [] (comp (remove nil?) (distinct)) froms)
        budget (max 1 (- (chunk-rows-for 1) (count params)))]
    (doseq [chunk (partition-all budget froms)]
      (let [q (str "select _eid from \"" from-table "\" where _eid in ("
                   (clojure.string/join ", " (repeat (count chunk) \?))
                   ") and not (" sql ")")]
        (when-let [denied (seq (sql/execute! tx (into (into [q] chunk) params) :raw))]
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
             :denied (count denied)})))))))

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
     ;; Link single relations — delete on from side
     ;; then link through execute multi
     (let [{:keys [:relations/one]} result
           xid-mode? (= :xid (id/key))]
       (reduce-kv
        (fn [result {:keys [table]
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
                           ;; bindings are [from_eid to_eid from_xid to_xid] —
                           ;; xids carried in-memory by project-saved-entities.
                           (mapv (fn [[f t fx tx]] [f t fx tx])))
                      (->> bindings
                           (filter second)
                           (mapv (fn [[f t]] [f t]))))
                scope (when-let [guard (read-scope-guard to-entity to-table)]
                        {:to/field to :to/table to-table :guard guard})]
            ;; never gate after the delete — a denied write would still evict
            (when-let [wg (write-scope-guard from-entity from-table)]
              (assert-from-side-writable! tx table from-table current wg))
            (log/trace {:id ::delete-old-one-relations
                        :data {:table table :from-side-count (count current)}}
                       "Deleting old one-relations")
            (delete-by-from-side! tx table from current scope)
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
     ;; Link many relations — one multi-row INSERT per chunk, as above.
     (let [{:keys [:relations/many]} result
           xid-mode? (= :xid (id/key))]
       (reduce-kv
        (fn [result {:keys [table]
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
                           ;; bindings are [from_eid to_eid from_xid to_xid].
                           (mapv (fn [[f t fx tx]] [f t fx tx])))
                      (->> bindings
                           (filter second)
                           (mapv (fn [[f t]] [f t]))))]
            ;; never gate after the delete — a denied sync would still evict
            (when-let [wg (write-scope-guard from-entity from-table)]
              (assert-from-side-writable! tx table from-table current wg))
            ;; sync replaces only the caller-VISIBLE window of the edge set —
            ;; an unscoped delete destroys links to rows they cannot read.
            (when-not stack?
              (log/debug {:id ::delete-many-relations-overwrite
                          :data {:table table :from-side-count (count current)}}
                         "Deleting many-relations (overwrite mode)")
              (delete-by-from-side!
               tx table from current
               (when-let [guard (read-scope-guard to-entity to-table)]
                 {:to/field to :to/table to-table :guard guard})))
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
                                          ;; Seal HERE, not via TypeCodec — the backends
                                          ;; explicitly refuse to touch encrypted cells.
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

                ;; Dual mapping: id->tmpid primary, constraint-values->tmpid
                ;; fallback (NULLs skipped) — see docs.
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

                ;; Generate placeholders using protocol (PostgreSQL: ?::type,
                ;; SQLite: ?)
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

                ;; Resolve-only = pointer rows, no write, no deadlock-prone lock
                ;; contention — see docs "Resolve-only classification".
                resolve-only? (link-only?
                               entity-schema
                               (get constraint entity-table) ks)
                link-only-gated? (boolean
                                 (when-let [gated (get-in analysis [:link-only entity-table])]
                                   (some gated (map second rows))))
                result
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
                        ;; id-addressed miss CREATES a stub on purpose — forward
                        ;; references during import: link now, row arrives later,
                        ;; same-id upsert fills it (deploy itself relies on this)
                        ;; never create when RLS scoped the lookup — hidden is not absent
                        create? (and (not link-only-gated?) (nil? rls-read)
                                     (not= :update-only (write-mode entity-id)))
                        ;; a refused create on a row the CALLER submitted by
                        ;; NATURAL KEY is a denied write — that shape is
                        ;; get-or-create, so "not found" means it would have
                        ;; been minted. Addressing by id asserts the row
                        ;; already exists, so an unresolved one stays
                        ;; REF_NOT_FOUND at link phase, as do nested pointers.
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
                                     ;; hidden is not absent — a guard-scoped
                                     ;; lookup cannot claim the row is missing
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
                    (concat found created))
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
                        ;; RLS write-guard rides ON CONFLICT DO UPDATE's WHERE —
                        ;; see docs "Upsert path" / RLS write guard.
                        {:keys [enabled guards]} (:rls entity-schema)
                        rls-write
                        (when (and enabled
                                   (not-empty constraint-keys)
                                   ;; entity-aware: O/B role grants bypass row
                                   ;; scope (CRUDOB) — O for writes here
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
                                "UPDATE \"" entity-table "\" SET "
                                (str/join ", "
                                          (map (fn [col]
                                                 (let [q (columns-fn col)]
                                                   (str q "=v." q)))
                                               (or (seq fields-to-update)
                                                   [(keyword id-field)])))
                                " FROM (VALUES " (str/join ", " (repeat n values-?))
                                ") AS v (" (str/join ", " ks-quoted) ")"
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
                      ;; declined rows return silently — the ABSENT rows ARE the
                      ;; denial; never look up which ones, see docs.
                      (when (and (pos? short)
                                 (or rls-where (not= :upsert mode)))
                        (let [code (case mode
                                     :create-only "UPDATE_FORBIDDEN"
                                     ;; with a guard, absent and denied are
                                     ;; indistinguishable without a lookup
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
                      written)))

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
                                 ;; id->tmpid keys are coerced above; normalize
                                 ;; the lookup key the same way.
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
             analysis
             mapping))))
      analysis
      (group-entity-rows rows)))
   analysis
   entity))

(defn set-entity
  "Sets (creates/updates) entity data — see docs Write path pipeline."
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
         (resolve-one-satellites tx result)
         (prepare-references tx result)
         (enhance-write tx result)
         (store-entity-records tx result)
         (project-saved-entities result)
         (link-relations tx result stack?)
         (pull-roots result))))))
