(ns synthigy.dataset.sql.query
  "Database-agnostic SQL query utilities.

  Provides shared utilities for all database implementations:
  - Schema caching and management
  - Runtime schema generation from ERD models
  - Common type definitions
  - Mutation helper functions
  - Cursor navigation for nested schemas

  This namespace contains ONLY code that is reused across multiple
  database implementations (PostgreSQL, SQLite, MySQL, etc.).

  Database-specific code belongs in respective namespaces:
  - synthigy.dataset.postgres.query
  - synthigy.dataset.sqlite.query (future)
  - synthigy.dataset.mysql.query (future)"
  (:require
    [buddy.hashers :as hashers]
    [camel-snake-kebab.core :as csk]
    [clojure.core.async :as async]
    [clojure.pprint]
    [clojure.set]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [clojure.walk :as walk]
    clojure.zip
    [next.jdbc :as jdbc]
    [nano-id.core :refer [nano-id]]
    [synthigy.dataset
     :refer [deployed-relation deployed-entity]]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.encryption :refer [decrypt-data]]
    [synthigy.dataset.enhance :as enhance]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming :refer [normalize-name
                                          entity->table-name
                                          relation->table-name
                                          entity->relation-field]]
    [synthigy.dataset.sql.protocol :as proto]
    [synthigy.dataset.sql.rls :as rls]
    [synthigy.db :refer [*db*]]
    [synthigy.db.sql :as sql]
    [synthigy.json :refer [<-json]]
    [synthigy.transit :refer [<-transit ->transit]]))

;;; ============================================================================
;;; Operation Context (Dynamic Vars)
;;; ============================================================================

(def ^:dynamic *operation-rules*
  "Dynamic var indicating current operation context.

  Used to differentiate permission checks and schema building
  based on whether we're doing a read, write, or delete operation.

  Values: #{:read}, #{:write}, #{:delete}, #{:read :owns}, etc.

  Copied from EYWA: neyho.eywa.dataset.postgres.query/*operation-rules*

  Example:
    (binding [*operation-rules* #{:delete}]
      (selection->schema entity-id selection args))")

;;; ============================================================================
;;; Schema Caching (Database-Agnostic)
;;; ============================================================================

(defonce ^:private _deployed-schema (atom nil))

(defn deploy-schema
  "Caches the runtime schema (from model->schema) for fast access.

  This should be called when a new model is deployed to avoid
  regenerating the schema on every query."
  [schema]
  (reset! _deployed-schema schema))

(defn deployed-schema
  "Returns the currently deployed runtime schema.

  Returns nil if no schema has been deployed yet."
  []
  @_deployed-schema)

(defn deployed-schema-entity
  "Gets a specific entity from the deployed schema by UUID.

  Throws if the schema hasn't been deployed or entity doesn't exist."
  [entity-id]
  (if-some [entity (get (deployed-schema) entity-id)]
    entity
    (throw
      (ex-info
        (str "Entity " entity-id " not found in deployed schema")
        {:type ::entity-not-found
         :entity entity-id
         :available (keys (deployed-schema))}))))

;;; ============================================================================
;;; Temporary Key Generation
;;; ============================================================================

(defn tmp-key
  "Generates a temporary key for entity tracking during mutations.

  Uses nano-id for shorter, more readable identifiers."
  []
  (nano-id 10))

;;; ============================================================================
;;; Type Definitions
;;; ============================================================================

(def scalar-types
  "Scalar field types that can be selected"
  #{"boolean" "string" "int" "float" "timestamp" "enum"
    "json" "uuid" "encrypted" "hashed" "transit" "avatar"})

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn pprint
  "Pretty-prints data to a string (for logging/debugging).

  Args:
    data - Any Clojure data structure

  Returns:
    Pretty-printed string representation"
  [data]
  (with-out-str (clojure.pprint/pprint data)))

(defn j-and
  "Joins SQL statements with ' and ' separator.

  Args:
    statements - Collection of SQL statement strings

  Returns:
    Joined SQL string

  Example:
    (j-and [\"age > 18\" \"active = true\"])
    => \"age > 18 and active = true\""
  [statements]
  (clojure.string/join " and " statements))

(defn freeze
  "Encodes data to transit format (used for transit type fields).

  If data is already a string, returns as-is (idempotent).
  Otherwise encodes to transit string representation.

  Args:
    data - Clojure data structure or string

  Returns:
    Transit-encoded string"
  [data]
  (if (string? data) data (->transit data)))

(defn wrap-basic-fields
  "Wraps database field names with optional table prefix and quoting.

  Adds '_eid' to the field list and handles 'euuid' field name specially.
  Uses double-quote SQL identifier quoting (standard SQL).

  Args:
    fields - Collection of field keywords
    prefix - Optional table alias for prefixing (e.g., \"t1\")

  Returns:
    Sequence of formatted field strings

  Examples:
    (wrap-basic-fields [:name :age])
    => (\"\\\"name\\\"\" \"\\\"age\\\"\" \"\\\"_eid\\\"\")

    (wrap-basic-fields [:name] \"t1\")
    => (\"t1.name\" \"t1._eid\")

  Note:
    Double-quote quoting is standard SQL but MySQL < 8.0 may need
    ANSI_QUOTES mode or backtick quoting instead."
  ([fields] (wrap-basic-fields fields nil))
  ([fields prefix]
   (if (not-empty prefix)
     (map #(str (when prefix (str prefix \.))
                (if (= (id/key) %) (id/field) (name %)))
          (conj fields "_eid"))
     (map #(str \" (if (= (id/key) %) (id/field) (name %)) \")
          (conj fields "_eid")))))

(defn extend-fields
  "Builds comma-separated SQL field list from field collection.

  Wraps fields with quoting/prefixing via wrap-basic-fields and joins
  with commas for use in SELECT clauses.

  Args:
    fields - Collection of field keywords
    prefix - Optional table alias for prefixing

  Returns:
    Comma-separated SQL field list string

  Example:
    (extend-fields [:name :email] \"u\")
    => \"u.name, u.email, u._eid\""
  ([fields] (extend-fields fields nil))
  ([fields prefix]
   (clojure.string/join ", " (wrap-basic-fields fields prefix))))

(defn schema-zipper
  "Creates a zipper for navigating schema tree structure.

  Enables tree traversal of nested schema relations using clojure.zip.
  The root is wrapped in a synthetic ::ROOT map entry.

  Args:
    root - Root schema map with :relations

  Returns:
    Zipper over schema tree

  Example:
    (def z (schema-zipper {:relations {:posts {...}}}))
    (-> z zip/down zip/node)
    => [:posts {...}]"
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
  "Throws access denied exception for relation access.

  Args:
    id - Relation UUID
    [from-euuid _] - Direction vector [from-entity to-entity]

  Throws:
    ExceptionInfo with access denied message"
  [id [from-euuid _]]
  (let [{{from :name} :from
         {to :name} :to
         :keys [from-label to-label]
         :as relation} (deployed-relation id)
        [from from-label to to-label] (if (= from-euuid (id/extract (:from relation)))
                                        [from to-label to from-label]
                                        [to from-label from to-label])]
    (throw
      (ex-info
        (format
          "You don't have sufficent privilages to access relation [%s]%s -> %s[%s]"
          from from-label to-label to)
        {:type ::enforce-search-access
         :roles (access/current-roles)}))))

(defn throw-entity
  "Throws access denied exception for entity access.

  Args:
    id - Entity UUID

  Throws:
    ExceptionInfo with access denied message"
  [id]
  (let [{entity-name :name} (deployed-entity id)]
    (throw
      (ex-info
        (format
          "You don't have sufficent privilages to access entity '%s'"
          entity-name)
        {:type ::enforce-search-access
         :roles (access/current-roles)}))))

(defn entity-accessible?
  "Checks if entity is accessible with given scopes.

  Args:
    entity-id - Entity UUID
    scopes - Set of access scopes (e.g., #{:read}, #{:write :owns})

  Returns:
    true if accessible

  Throws:
    ExceptionInfo if not accessible"
  [entity-id scopes]
  (when-not (access/entity-allows? entity-id scopes)
    (throw-entity entity-id))
  true)

(defn relation-accessible?
  "Checks if relation is accessible with given scope and direction.

  Args:
    relation - Relation UUID
    direction - Direction vector [from-entity to-entity]
    scope - Access scope set (e.g., #{:read})

  Returns:
    true if accessible

  Throws:
    ExceptionInfo if not accessible"
  [relation direction scope]
  (let [allowed? (access/relation-allows? relation direction scope)]
    (when-not allowed?
      (throw-relation relation direction))
    true))

;;; ============================================================================
;;; Type Encoding/Decoding (Database-Agnostic with Protocol)
;;; ============================================================================

(defn entity-serde
  "Builds encoder and decoder maps for entity fields (database-agnostic).

  Uses TypeCodec protocol for database-specific type encoding/decoding.
  This allows the same code to work with PostgreSQL (PGobject),
  SQLite (TEXT/INTEGER), MySQL, etc.

  Args:
    entity-uuid - Entity UUID to build encoders/decoders for

  Returns:
    Map with :encoders and :decoders
      :encoders - Map of field-key → encoder-fn (Clojure → Database)
      :decoders - Map of field-key → decoder-fn (Database → Clojure)

  Example:
    (entity-serde user-uuid)
    => {:encoders {:status (fn [v] (proto/encode *db* \"user_status\" v))}
        :decoders {:status keyword
                   :metadata json->data}}"
  [entity-uuid]
  (let [{:keys [fields]} (get (deployed-schema) entity-uuid)
        field->type (reduce
                      (fn [result {f :key
                                   t :type
                                   e :enum/name}]
                        (assoc result f (or e t)))
                      nil
                      (vals fields))

        ;; Build encoders: Clojure values → Database representation
        ;; Uses proto/encode for database-specific encoding
        encoders (reduce
                   (fn [result field]
                     (let [t (get field->type field)]
                       (case t
                         ;; These types pass through unchanged (native DB support)
                         ("boolean" "string" "int" "float" "json"
                                    "timestamp" "timeperiod" "currency"
                                    "uuid" "avatar" "hashed" nil) result
                         ;; Transit: encode to string
                         "transit" (update result field freeze)
                         ;; Default: enum type - use protocol encoder
                         (assoc result field
                                (fn [v]
                                  ;; Database-agnostic: delegates to TypeCodec protocol
                                  ;; PostgreSQL: creates PGobject
                                  ;; SQLite: converts to TEXT
                                  (proto/encode *db* t v))))))
                   nil
                   (keys field->type))

        ;; Build decoders: Database representation → Clojure values
        ;; All non-trivial types delegate to TypeCodec protocol for database-agnostic decoding
        ;; NOTE: We capture `*db*` value here because decoders may be called in futures
        ;; where the dynamic binding is not conveyed
        db *db*
        decoders (reduce
                   (fn [r k]
                     (let [field-type (field->type k)
                           transform (case field-type
                                       ;; Complex types - delegate to TypeCodec protocol
                                       "uuid" (fn [data] (proto/decode db "uuid" data))
                                       "transit" (fn [data] (proto/decode db "transit" data))
                                       "encrypted" (fn [data] (proto/decode db "encrypted" data))
                                       "json" (fn [data] (proto/decode db "json" data))
                                       "boolean" (fn [data] (proto/decode db "boolean" data))
                                       ;; enum - convert to keyword
                                       "enum" (fn [data] (proto/decode db "enum" data))
                                       ;; If not a scalar type, it's a custom enum - delegate to protocol
                                       (if (and field-type (not (scalar-types field-type)))
                                         (fn [data] (proto/decode db field-type data))
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
  "Normalizes GraphQL selection format by removing entity namespace from keys.

  Converts namespaced keys to simple keywords:
    :User/name  → :name
    :Post/title → :title

  Example:
    Input:  {:User/name [{:args nil :selections nil}]
             :User/email [{:args {:_where {...}} :selections nil}]}
    Output: {:name [{:args nil :selections nil}]
             :email [{:args {:_where {...}} :selections nil}]}"
  [s]
  (reduce
    (fn [r [k v]]
      (assoc r (-> k name keyword) v))
    nil
    s))

(defn distribute-fields
  "Separates entity fields into scalars and references.

  Args:
    fields - Vector of field maps from entity schema
             Each field has :key, :type, :euuid, :constraint

  Returns:
    Map with :field (scalars) and :reference (relation fields)

  Example:
    Input:  [{:key :email :type \"string\"}
             {:key :owner :type \"user\"}
             {:key :age :type \"int\"}]
    Output: {:field [{:key :email :type \"string\"}
                     {:key :age :type \"int\"}]
             :reference [{:key :owner :type \"user\"}]}"
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
  "Constructs nested response structure from flat database records.

  Takes flat database records with reference IDs and reconstructs the
  nested object graph according to the schema structure.

  Args:
    schema - Query schema with entity/table, relations, fields, recursions
    db - Database map with ::counts and ::numerics for aggregations
    found-records - Flat records from database query

  Returns:
    Vector of nested response objects

  Example:
    Input schema:  {:entity/table \"users\"
                    :relations {:roles {...}}
                    :fields {:name nil :email nil}}
    Input db:      {\"users\" {1 {:name \"Alice\" :roles [[\"roles\" 5]]}}
                    \"roles\" {5 {:name \"Admin\"}}}
    Output:        [{:name \"Alice\" :roles [{:name \"Admin\"}]}]"
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
                              (if (vector? v)
                                (assoc data' k (pull-reference v schema cursor))
                                (assoc data' k (pull-reference [table v] schema cursor)))
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
  "Converts cursor path to get-in path for navigating schema tree.

  A cursor is a vector of relation keys forming a path through the schema tree.
  This function converts it to the format needed for get-in on the schema.

  Examples:
    []                    → []
    [:roles]              → [:relations :roles]
    [:roles :permissions] → [:relations :roles :relations :permissions]

  Args:
    cursor - Vector of relation keywords

  Returns:
    Vector suitable for (get-in schema ...)"
  [cursor]
  (if (empty? cursor)
    []
    (vec (mapcat (fn [k] [:relations k]) cursor))))

(defn schema->cursors
  "Generates all cursor paths present in the schema tree.

  Recursively traverses the schema to find all relation paths.
  Each cursor represents a unique path from root to a relation node.

  Examples:
    Schema: {:relations {:roles {:relations {:permissions {}}}}}
    Returns: [[:roles] [:roles :permissions]]

  Args:
    schema - Query schema with nested :relations

  Returns:
    Vector of cursor paths (each cursor is a vector of keywords)"
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
          ;; Has children - expand cursor recursively
           (into
             (conj acc cursor)
             (mapcat #(schema->cursors [(conj cursor %)] schema)
                     (keys child-relations)))
          ;; Leaf node - include if it has fields or is counted
           (if (or (:fields node) (:_count node))
             (conj acc cursor)
             acc))))
     []
     cursors)))

;;; ============================================================================
;;; Mutation Helper Functions (Database-Agnostic)
;;; ============================================================================

(defn group-entity-rows
  "Groups entity rows by their field keys for batch insertion.

  Rows with the same set of fields can be inserted in a single INSERT statement.

  Args:
    tmp-rows - Map of {tmp-id entity-data}

  Returns:
    Map of {field-key-set [[data tmp-id] ...]}"
  [tmp-rows]
  (reduce-kv
    (fn [result tmp-id data]
      (update result (set (keys data)) (fnil conj []) [data tmp-id]))
    nil
    tmp-rows))

(defn enhance-write
  "Orchestrates enhancement system for write operations.

  Applies enhancements in two phases:
  1. Infrastructure layer - audit fields (created_by, modified_on, etc.)
  2. Domain layer - custom write enhancements (business logic)

  This function is database-agnostic and delegates to the enhancement
  protocol for database-specific behavior.

  Args:
    tx - Database transaction context
    result - Analysis result map with :entity/mapping and entity data

  Returns:
    Enhanced result map with audit and custom enhancements applied

  Example:
    (enhance-write tx {:entity/mapping {\"users\" user-entity-uuid}
                       :entity {\"users\" {\"tmp123\" {:name \"Alice\"}}}})
    => {:entity {\"users\" {\"tmp123\" {:name \"Alice\"
                                        :created_by user-id
                                        :created_on timestamp}}}}"
  [tx result]
  (let [final (reduce-kv
                (fn [final _ entity-id]
                  (binding [*operation-rules* #{:write}]
                   ;; FIRST: Apply audit enhancement (infrastructure layer)
                    (let [audited (enhance/apply-audit *db* entity-id final tx)
                         ;; THEN: Apply custom write enhancements (domain layer)
                          current (enhance/apply-write entity-id audited tx)]
                      current)))
                result
                (:entity/mapping result))]
    final))

(defn project-saved-entities
  "Projects entity references after database save.

  Converts temporary IDs to actual _eid values for relations and recursions.

  Args:
    analysis - Analysis map with :entity, :relations/one, :relations/many, :recursion

  Returns:
    Updated analysis with _eid values projected"
  [{:keys [entity :relations/one :relations/many recursion]
    :as analysis}]
  (as-> analysis analysis
    ;; Project to one relations
    (reduce-kv
      (fn [analysis
           {from-table :from/table
            to-table :to/table
            :as table}
           ks]
        (assoc-in analysis [:relations/one table]
                  (reduce
                    (fn [result [from to]]
                      (conj result
                            [(get-in entity [from-table from :_eid])
                             (get-in entity [to-table to :_eid])]))
                    []
                    ks)))
      analysis
      one)
    ;; Project to many relations
    (reduce-kv
      (fn [analysis
           {from-table :from/table
            to-table :to/table
            :as table}
           ks]
        (assoc-in analysis [:relations/many table]
                  (reduce
                    (fn [result [from to]]
                      (conj result
                            [(get-in entity [from-table from :_eid])
                             (get-in entity [to-table to :_eid])]))
                    []
                    ks)))
      analysis
      many)
    ;; Project to recursions
    (reduce-kv
      (fn [analysis table recursions]
        ;; focus on recursions
        (reduce-kv
          ;; that are distributed as field parent children depth
          (fn [analysis field bindings]
            ;; Replace current temp ids with real :_eids
            (assoc-in analysis [:recursion table field]
                      (reduce-kv
                        (fn [bindings parent children]
                          (assoc bindings
                            (get-in entity [table parent :_eid])
                            (map #(get-in entity [table % :_eid]) children)))
                        nil
                        bindings)))
          analysis
          recursions))
      analysis
      recursion)))

;;; ============================================================================
;;; Runtime Schema Generation (Database-Agnostic)
;;; ============================================================================

(defn model->schema
  "Converts ERDModel to runtime schema format for SQL query generation.

  The schema is database-agnostic and works for PostgreSQL, SQLite, MySQL, etc.
  Each database implementation interprets the schema according to its features.

  Schema structure per entity:
  {:table \"user\"
   :name \"User\"
   :fields {uuid {:key :email :euuid uuid :type \"string\" :constraint \"mandatory\"}
            :modified_by {:key :modified_by :type \"user\" :reference/entity <user-uuid>}
            :modified_on {:key :modified_on :type \"timestamp\"}}
   :field->attribute {:field-name uuid}
   :relations {:posts {:relation uuid :from uuid :to uuid :type :many ...}
               :modified_by {:from uuid :to <user-uuid> :type :one ...}}
   :recursions #{:parent}}"
  ([] (model->schema (synthigy.dataset/deployed-model)))
  ([model]
   (reduce
     (fn [schema entity]
       (let [euuid (id/extract entity)
             table (entity->table-name entity)
             ;; Build base fields from domain model attributes only
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
                                      :constraint constraint}]
                               (assoc fields attr-id
                                      (case t
                                        "enum"
                                        ;; Store canonical enum name and values (database-agnostic)
                                        ;; Values include :euuid for migration tracking (rename detection)
                                        ;; Each DB implementation will map this appropriately:
                                        ;;   PostgreSQL: CREATE TYPE "table_field" AS ENUM (...)
                                        ;;   SQLite: CHECK (field IN (...))
                                        ;;   MySQL: field ENUM('val1', 'val2')
                                        (assoc f
                                          :enum/name (normalize-name (str table \space aname))
                                          :enum/values (get-in config [:values]))
                                        ;; Check if type is a reference (matches *reference-mapping*)
                                        ;; If yes, add :reference/entity with the entity UUID
                                        (if-some [ref-entity-uuid (core/reference-entity-uuid t)]
                                          (assoc f :reference/entity ref-entity-uuid)
                                          f)))))
                           {}  ; Start with empty map - audit comes from protocol
                           ;; Only include active attributes (filter out deprecated/removed fields)
                           (filter :active (:attributes entity)))

             ;; Get audit enhancement from protocol (infrastructure layer)
             audit-augmentation (enhance/augment-schema *db* entity)
            ; _ (def entity entity)
            ; _ (throw (Exception. "HEH"))

             ;; Merge audit fields into base fields
             fields (core/deep-merge base-fields (:fields audit-augmentation))

             {relations :relations
              recursions :recursions}
             (group-by
               (fn [{t :cardinality}]
                 (case t
                   "tree" :recursions
                   :relations))
               ;; Only include active relations (filter out deprecated/removed)
               (filter :active (core/focus-entity-relations model entity)))

             ;; Build relations from ERD model
             base-relations (reduce
                              (fn [relations
                                   {:keys [from to to-label from-label cardinality]
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

             ;; Merge audit relations from protocol (e.g., modified_by -> user)
             relations (core/deep-merge base-relations (:relations audit-augmentation))
             recursions (set (map (comp keyword normalize-name :to-label) recursions))
             mandatory-attributes (keep
                                    (fn [{:keys [constraint name]}]
                                      (when (#{"mandatory" "unique+mandatory"} constraint)
                                        (keyword (normalize-name name))))
                                    (:attributes entity))
             ;; Compile RLS guards (if enabled for this entity)
             compiled-rls (rls/compile-entity-rls model entity)
             ;; Build entity schema (audit fields and relations already merged above)
             entity-schema (cond->
                             {:table table
                              :name (:name entity)
                              :constraints (cond->
                                             (get-in entity [:configuration :constraints])

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
                             ;; Add :rls only if entity has RLS configured
                             compiled-rls
                             (assoc :rls compiled-rls))]
         (assoc schema euuid entity-schema)))
     {}
     (core/get-entities model))))

(defn focus-order
  "Function will remove nested :_order_by arguments
  and keep only ones defined in schema root entity"
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
  "Given election schema produces cursors that point
  to all connected entity tables. This is a way point to
  pull linked data from db with single query"
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
        :entity/euuid entity}
       entity
       data'
       stack?)))
  ([_ current entity data stack?]
   (let [schema (deployed-schema)
         find-entity (memoize (fn [entity] (get schema entity)))
         type-mapping (memoize
                        (fn [{:keys [fields]}]
                          (reduce-kv
                            (fn [result _ {:keys [type key]
                                           ename :enum/name}]
                              (assoc result key (or ename type)))
                            {(id/key) "uuid"}
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
                                   [(id/key)])))))]
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
             (shallow-snake [data]
               (reduce-kv
                 (fn [r k v]
                   (if-not k r
                           (assoc r (csk/->snake_case_keyword k :separator #"[\s\-]") v)))
                 nil
                 data))
             (transform-object
               ([entity-euuid data]
                (transform-object nil entity-euuid data))
               ([result entity-euuid {:keys [tmp/id]
                                      :or {id (tmp-key)}
                                      :as data}]
                (entity-accessible? entity-euuid #{:write :owns})
                (let [{:keys [relations fields recursions table]
                       {mandatory-fields :mandatory} :constraints
                       :as entity} (find-entity entity-euuid)
                      {references true
                       fields false} (try
                                       (group-by
                                         (fn [definition]
                                           (log/trace "Field definition: " definition)
                                           (contains? definition :reference/entity))
                                         (vals (dissoc fields :modified_by :modified_on)))
                                       (catch Throwable e
                                         (log/errorf
                                           "Fields:%s"
                                           (vals (dissoc fields :modified_by :modified_on)))
                                         (throw e)))
                      data (shallow-snake (dissoc data :tmp/id))
                      fields-data (select-keys
                                    data
                                    (conj
                                      (map :key fields)
                                      (id/key)))
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
                      fields-data
                      (reduce
                        (fn [fd k]
                          (let [t (get type-mapping k)]
                            (update
                              fd k
                              (case t
                                ("boolean" "string" "int" "float" "timeperiod" "currency" "uuid" nil) identity
                                ;; timestamp - encode via protocol (handles string → Instant conversion)
                                "timestamp" (fn [v] (proto/encode *db* "timestamp" v))
                                "hashed" hashers/derive
                                "transit" freeze
                                (fn [v]
                                  (when v
                                    (proto/encode *db* t v)))))))
                        fields-data
                        (keys fields-data))
                      constraints (get-constraints entity-euuid)
                      indexes (remove empty? (map #(select-keys fields-data %) constraints))
                      id (or
                           (some #(get-in result [:index table %]) indexes)
                           id)
                      constraint-keys (flatten constraints)
                      {:keys [references-data resolved-references]}
                      (reduce-kv
                        (fn [r k v]
                          (if (map? v)
                            (assoc-in r [:references-data k] v)
                            (assoc-in r [:resolved-references k] v)))
                        {:references-data nil
                         :resolved-references nil}
                        (select-keys data (map :key references)))
                      valid-relation-keys (let [recursions (set recursions)]
                                            (reduce
                                              (fn [result field]
                                                (if (contains? recursions field)
                                                  result
                                                  (let [{:keys [relation to from]} (get relations field)]
                                                    (cond
                                                      (not relation) result
                                                      (not (contains? data field)) result
                                                      (relation-accessible? relation [from to] #{:write :owns})
                                                      (conj result field)))))
                                              []
                                              (keys relations)))
                      relations-data (when (not-empty valid-relation-keys)
                                       (select-keys data valid-relation-keys))
                      recursions-data (select-keys data recursions)
                      [root parents-mapping]
                      (letfn [(normalize-value [v]
                                (select-keys (shallow-snake v) constraint-keys))]
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
                                    fields-data)]
                  (as->
                    (->
                      result
                      (update-in [:entity table id] (if stack? merge (fn [_ v] v)) fields-data)
                      (assoc-in [:entity/mapping table] entity-euuid)
                      (update-in [:index table] merge (zipmap indexes (repeat id)))
                      (assoc-in [:constraint table] constraints))
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
                        (let [reference-entity-euuid (get
                                                       (reference-mapping entity-euuid)
                                                       attribute)
                              reference-entity (find-entity reference-entity-euuid)
                              reference-data (some
                                               (fn [ks]
                                                 (when (every? #(contains? data %) ks)
                                                   (select-keys data ks)))
                                               (get-constraints reference-entity-euuid))]
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
                                      (assoc data :tmp/id rid))))
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
                                  (assoc data :tmp/id rid)))))))
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
   (entity-accessible? entity-id #{:read :owns})
   (let [{relations :relations
          recursions :recursions
          fields :fields
          _agg :_agg
          table :table} (deployed-schema-entity entity-id)
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
         scalars (reduce-kv
                   (fn [r k [{args :args}]]
                     (if (valid-fields k)
                       (assoc r k args)
                       r))
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
                           ;; If there are some recursions add that relations as well
                           (not-empty recursions)
                           (clojure.set/union (set recursions)))
         type-mapping (zipmap (map :key fields) (map :type fields))
         ;; Build decoders - delegate to TypeCodec protocol for database-agnostic decoding
         ;; NOTE: We capture `*db*` value here because decoders may be called in futures
         ;; where the dynamic binding is not conveyed
         db *db*
         decoders (reduce
                    (fn [r k]
                      (if (valid-fields k)
                        (let [field-type (get type-mapping k)
                              transform (case field-type
                                          ;; Complex types - delegate to TypeCodec protocol
                                          "transit" (fn [data] (proto/decode db "transit" data))
                                          "encrypted" (fn [data] (proto/decode db "encrypted" data))
                                          "json" (fn [data] (proto/decode db "json" data))
                                          "boolean" (fn [data] (proto/decode db "boolean" data))
                                          ;; enum - convert to keyword
                                          "enum" (fn [data] (proto/decode db "enum" data))
                                          ;; currency/period are scalar pass-throughs
                                          ("currency" "period") (fn [data] (proto/decode db field-type data))
                                          ;; If not a scalar type, it's a custom enum - delegate to protocol
                                          (if (and field-type (not (scalar-types field-type)))
                                            (fn [data] (proto/decode db field-type data))
                                            nil))]
                          (if transform
                            (assoc r k transform)
                            r))
                        r))
                    nil
                    (map key scalars))
         field->type (reduce
                       (fn [result {f :key
                                    t :type
                                    e :enum/name}]
                         (assoc result f (or e t)))
                       nil
                       fields)
         arg-fields (letfn [(join-args
                              ([args] (join-args args #{}))
                              ([args result]
                               (reduce-kv
                                 (fn [result k _]
                                   (reduce clojure.set/union
                                           (if (valid-fields k) (conj result k) result)
                                           (concat
                                             (map join-args (vals (select-keys args [:_where :_maybe :_count :_agg])))
                                             (mapcat #(map join-args %) (vals (select-keys args [:_or :_and]))))))
                                 result
                                 args)))]
                      (join-args args))
         ;; Build encoders for query arguments using TypeCodec protocol
         encoders (reduce
                    (fn [result field]
                      (let [t (get field->type field)]
                        (case t
                         ;; Native types - no encoding needed
                          ("boolean" "string" "int" "float" "json"
                                     "timeperiod" "currency"
                                     "uuid" "hashed" nil) result
                         ;; Timestamp - use protocol for database-specific encoding
                         ;; PostgreSQL: pass Instant directly (JDBC handles conversion)
                         ;; SQLite: convert to ISO-8601 string (text comparison works)
                          "timestamp" (assoc result field
                                             (fn [v] (proto/encode *db* "timestamp" v)))
                         ;; Transit - custom encoding
                          "transit" (update result field freeze)
                         ;; Default: enum types - use protocol
                          (assoc result field (fn [v] (proto/encode *db* t v))))))
                    nil
                    arg-fields)
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
                                        (assoc final (or alias rkey)
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
                                              alias-key (get-in objects [k 0 :alias] k)]
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
         ;; Build base schema
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
                             :recursions recursions) schema
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
                                             rdata (get relations rkey)
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
                              ;; ... rest of aggregate handling ...
                               :_agg
                               (reduce
                                 (fn [schema {operations :selections}]
                                   (reduce-kv
                                     (fn [schema relation [{specifics :selections}]]
                                       (let [rkey (keyword (name relation))
                                             rdata (get relations rkey)
                                             relation (->
                                                        rdata
                                                        (dissoc :_agg)
                                                        (dissoc :relations)
                                                        (clojure.set/rename-keys {:table :relation/table})
                                                        (merge (entity-serde (:to rdata)))
                                                        (assoc
                                                          :pinned true
                                                          :entity/table (:to/table rdata)
                                                          :relation/as (str (gensym "link_"))
                                                          :entity/as (str (gensym "data_"))))
                                             schema (assoc-in schema [:_agg rkey] relation)]
                                         (reduce-kv
                                           (fn [schema fkey aggregate-selections]
                                             (reduce
                                               (fn [schema {aggregate-alias :alias
                                                            args :args
                                                            :as data}]
                                                 (let [operation (ffirst (:selections data))
                                                       field (or aggregate-alias fkey)]
                                                   (if (nil? args)
                                                     (assoc-in schema [:_agg rkey operation field] [fkey nil])
                                                     (assoc-in schema [:_agg rkey operation field] [fkey (when args {:args args})]))))
                                               schema
                                               aggregate-selections))
                                           schema
                                           specifics)))
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
     ;; NEW: Apply access enhancement
     ;; The enhancement system uses dynamic bindings *user*, *roles*, *groups*
     (enhance/apply-schema base-schema selection))))

;;; ============================================================================
;;; SQL Generation Functions (Shared - Database-Agnostic)
;;; ============================================================================

(defn distinct->sql
  ([{{args :_distinct} :args
     :as schema}]
   (when args
     (str
       "distinct on ("
       (clojure.string/join
         ", "
         (letfn [(process-distinct [{:keys [entity/as]
                                     :as schema} {:keys [attributes]
                                                  :as args}]
                   (reduce-kv
                     (fn [result field distinct-on]
                       (if (empty? distinct-on) result
                           (into
                             result
                             (process-distinct (get-in schema [:relations field]) distinct-on))))
                     (mapv #(vector as %) attributes)
                     (dissoc args :attributes)))]
           (reduce
             (fn [result [table field]]
               (conj result (str (when table (str table \.)) (name field))))
             []
             (process-distinct schema args))))
       \)))))

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
                                 (if (keyword? order-by')
                                   (do
                                     (log/tracef
                                       "[%s] Modifier %s selection to SQL: %s"
                                       as field order-by')
                                     (conj result [as field order-by']))
                                   (into
                                     result
                                     (process-order-by (get-in schema [:relations field]) order-by'))))
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

(defn query-selection->sql
  ([schema] (query-selection->sql schema []))
  ([{operators :args
     encoders :encoders
     prefix :entity/as
     relations :relations
     field-types :field-types
     :as schema} data]
   (let [is-relation? (set (keys relations))
         postgres? (instance? synthigy.db.Postgres *db*)]
     (reduce
       (fn [[statements data] [field constraints]]
         (let [field-type (get field-types field)
              ;; For JSON fields on PostgreSQL, use ->> to extract as text for string comparisons
              ;; This extracts the actual string value from JSON, not the JSON representation
              ;; SQLite stores JSON as TEXT so no special handling needed
               field' (if (not-empty prefix) (str prefix \. (name field)) (name field))
               field' (if (and postgres? (= "json" field-type))
                        (str "(" field' " #>> '{}')")
                        field')]
           ;;
           (if (boolean? constraints)
             ;; Check if boolean constraint refers to _distinct
             ;; if it does ignore that operator
             (if (= :_distinct field)
               [statements]
               [(conj statements (str (name field) " = " constraints))])
             ;;
             (if (is-relation? field)
               ;; When specified field is nested relation
               (if-not *deep* [statements data]
                       (let [[statements' data'] (query-selection->sql (get-in schema [:relations field]))]
                         [(into statements statements')
                          (into data data')]))

               ;; Handle fields
               (case field
                ;;
                 :_where
                 (if-not *deep*
                   [statements data]
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
                                                  (format "%s in (%s)" field' (clojure.string/join "," (repeat (count cv) \?)))
                                                  "")
                                           :_not_in (if-not (empty? cv)
                                                      (format "%s not in (%s)" field' (clojure.string/join "," (repeat (count cv) \?)))
                                                      "")
                                           :_le (str field' " <= ?")
                                           :_ge (str field' " >= ?")
                                           :_eq (str field' " = ?")
                                           :_neq (str field' " != ?")
                                           :_lt (str field' " < ?")
                                           :_gt (str field' " > ?")
                                           :_like (str field' " like ?")
                                           :_ilike (str field' " " (proto/like-operator *db* false) " ?")
                                           :_limit (str field' " limit ?")
                                           :_offset (str field' " offset ?")
                                           :_boolean (str field' " "
                                                          (case cv
                                                            ("NOT_TRUE" :NOT_TRUE) " is not true"
                                                            ("NOT_FALSE" :NOT_FALSE) " is not false"
                                                            ("TRUE" :TRUE) " is true"
                                                            ("FALSE" :FALSE) " is false"
                                                            ("NULL" :NULL) " is null"))
                                         ;; If nested condition than
                                           (do
                                             (log/errorf
                                               "Nested condition error:\nConstraint: %s\nValue: %s\nSchema:\n%s"
                                               cn
                                               cv
                                               (pprint schema))
                                             (throw (Exception. "Nested problem"))))
                               data (case cn
                                      (:_boolean) data'
                                      (:_in :_not_in) (into data'
                                                            (if-let [e (get encoders field)]
                                                              (map e cv)
                                                              cv))
                                      (conj data' (if-let [e (get encoders field)]
                                                    (e cv)
                                                    cv)))]
                           [(conj statements' statement) data])
                         (case cn
                           :_eq [(conj statements' (format "%s is null" field')) data']
                           :_neq [(conj statements' (format "%s is not null" field')) data']
                           [statements' data'])))
                     [statements data]
                     constraints)))))))
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
       (log/tracef
         "Computed args stack:\nStack:\n%s\nData:\n%s"
         stack (pprint data))
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

(defn search-stack-from
  "For given schema function will return FROM statement
  by joining tables in schema based on args available
  in schema. Intended for locating root records that can
  later be pulled. Returned result is vector of two elements.
  First is sequence of table aliases, and second one is FROM
  statment itself."
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
          [tables stack] (reduce
                           (fn [[tables stack] location]
                             (loop [[[_ parent] [_ current] :as nodes]
                                    (conj (vec (clojure.zip/path location)) (clojure.zip/node location))
                                    ;;
                                    tables tables
                                    stack stack]
                               (if (empty? current)
                                 ;; Return final result
                                 [(conj tables (:entity/as parent)) stack]
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
                                       link-sql (if (= table link-table)
                                                  (format
                                                    "%s join \"%s\" %s on %s.%s=%s.%s"
                                                    join child-table as-child as ff as-child "_eid")
                                                  (format
                                                    "%s join \"%s\" %s on %s._eid=%s.%s\n%s join \"%s\" %s on %s.%s=%s.%s"
                                                    join link-table as-link as as-link ff
                                                    join child-table as-child as-link tf as-child "_eid"))
                                       next-tables (conj tables as)
                                       next-stack (conj stack link-sql)]
                                   (recur
                                     (rest nodes)
                                     next-tables
                                     next-stack)))))
                           [[] []]
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
                          (str "\"" table "\" as " as))))])))

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
  (log/tracef
    "[%s] Pulling entity for parents\n[%s]\nwith found records [%s]"
    table (str/join ", " parents) (str/join ", " found-records))
  (let [[_ from maybe-data] (search-stack-from schema)
        _ (log/tracef "[%s] Looking for WHERE args" table)
        [where d] (search-stack-args schema)
        ; _ (log/tracef "[%s] Looking for FOUND args" table)
        ; [found fd] (when-some [found-records (not-empty (keep #(when (some? %) %) found-records))]
        ;              (search-stack-args
        ;               (assoc schema :args
        ;                      ; {:_eid {:_in found-records}})))
        ;                      {:_eid {:_in parents}})))
        _ (log/tracef "[%s] Looking for PARENT args" table)
        [parented pd] (if (= talias "_eid")
                        ;; If direct binding (in entity table)
                        (search-stack-args
                          (assoc schema
                            :args {:_eid {:_in parents}}
                            :entity/as ras))
                        ;; Otherwise
                        (search-stack-args
                          (assoc schema
                            :args {(keyword falias) {:_in parents}}
                            :entity/as ras)))
        ;; TODO - When using found records it breaks when _limit is set prior in query
        ;; hierarchy... To the point... This will not work if lets say some search query
        ;; is sent that has _limit: 100, because it will return 100 root records with
        ;; and if there are some _eids in related data it will be limited to 100 in
        ;; found records
        ;; ignore found records so that search is restarted
        ; [where data] [(clojure.string/join " and " (remove nil? [where found parented]))
        ;               (reduce into [] (remove nil? [d fd pd]))]
        [where data] [(clojure.string/join " and " (remove nil? [where parented]))
                      (reduce into [] (remove nil? [d pd]))]
        modifiers (modifiers-selection->sql schema)]
    (into
      [(str "select " (if (= talias "_eid")
                        (str ras "._eid as " falias)
                        (str ras \. falias \, ras \. talias))
            (when-not (empty? fields) (str "," (extend-fields (keys fields) as)))
            \newline "from " from
            (when where (str "\nwhere " where))
            (when modifiers (str \newline modifiers)))]
      ((fnil into []) maybe-data data))))

(defn deep-merge
  "Deep merge multiple maps"
  [& maps]
  (apply merge-with
         (fn [v1 v2]
           (if (and (map? v1) (map? v2))
             (deep-merge v1 v2)
             v2))
         maps))

(defn pull-cursors
  [con {:keys [entity/table]
        :as schema} found-records]
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
                    (conj queries (future (process-node result current-location)))
                    (clojure.zip/right current-location)))))
            (pull-counts [result location]
              (let [[_ {as :entity/as
                        counted :_count
                        :as schema}] (clojure.zip/node location)
                    parents (when-let [parent (clojure.zip/up location)]
                              (keys (get result (-> parent
                                                    clojure.zip/node
                                                    second
                                                    :from/table))))]
                (if-not (contains? schema :_count)
                  nil
                  (future
                    (let [schema (as-> schema schema
                                   (assoc schema :relations
                                          (reduce-kv
                                            (fn [r k _]
                                              (->
                                                r
                                                (assoc-in [k :pinned] true)
                                                (assoc-in [k :args :_join] :LEFT)))
                                            (:_count schema)
                                            (:_count schema)))
                                   (dissoc schema :_count :fields)
                                   (if-not parents schema
                                           (update schema :args assoc-in [:_eid :_in] parents)))
                          [_ from] (search-stack-from schema)
                          ;;
                          [count-selections from-data]
                          (reduce-kv
                            (fn [[statements data] as {etable :entity/as
                                                       :as schema}]
                              (let [t (name as)
                                    [[[_ [statement]]] statement-data] (binding [*ignore-maybe* false]
                                                                         (-> schema
                                                                             query-selection->sql))]
                                [(conj statements (if (empty? statement)
                                                    (format
                                                      "count(distinct %s._eid) as %s"
                                                      etable t)
                                                    (format
                                                      "count(distinct case when %s then %s._eid end) as %s"
                                                      statement etable t)))
                                 (if statement-data (into data statement-data)
                                     data)]))
                            [[] []]
                            counted)
                          ;;
                          ;; [where where-data]  (search-stack-args schema)
                          ;; TODO - ignore where for now, as it should be part of
                          ;; count-selections
                          [where where-data] [nil nil]
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
                          _ (log/tracef
                              "[%s] Sending counts aggregate query:\n%s\n%s\n%s"
                              table query-string from-data where-data)
                          result (sql/execute! con query core/*return-type*)]
                      (reduce
                        (fn [r {:keys [parent_id]
                                :as data}]
                          (assoc r parent_id {:_count (dissoc data :parent_id)}))
                        nil
                        result))))))
            (pull-numerics
              [_ location]
              (let [[_ {as :entity/as
                        :as schema}] (clojure.zip/node location)
                    schema (dissoc schema :fields)
                    numerics (get schema :_agg)]
                (cond
                  (empty? numerics) nil
                  :else
                  (future
                    ;; Replace regular relations with ones collected in aggregates
                    (let [aggregate-schema (-> schema
                                               (assoc :relations numerics)
                                               (dissoc :_agg :_count))
                          [numerics-selections numerics-data]
                          (reduce-kv
                            ;; then for every relation field
                            ;; select keys that are aggregates :min :max :sum :avg
                            (fn [result rkey {as :entity/as
                                              :as rdata}]
                              (reduce-kv
                                ;; And for each of this "operations" definitions find
                                (fn [result operation definition]
                                  (reduce-kv
                                    ;; Find target field in relation that is aggregated
                                    ;; and target field operation
                                    (fn [[statements data] target-key [field-key field-args]]
                                      (let [[statement-stack statement-data]
                                            (binding [*ignore-maybe* false]
                                              (let [relation-schema (get-in aggregate-schema [:relations rkey])
                                                    operation-args (get relation-schema operation)]
                                                (-> relation-schema
                                                    (merge operation-args)
                                                    (deep-merge field-args)
                                                    query-selection->sql)))
                                            j :and
                                            statement (when (not-empty statement-stack)
                                                        (str/join
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
                                                            statement-stack)))]
                                        [(conj statements
                                               (format
                                                 "%s(%s) as %s$%s$%s"
                                                 (case operation
                                                   :min "min"
                                                   :max "max"
                                                   :avg "avg"
                                                   :sum "sum")
                                                 (if (empty? statement)
                                                   (str as "." (name field-key))
                                                   (str "case when " statement
                                                        " then " as "." (name field-key)
                                                        " else null end"))
                                                 (name rkey) (name operation) (name target-key)))
                                         (if-not statement-data
                                           data
                                           (into data statement-data))]))
                                    result
                                    definition))
                                result
                                (select-keys rdata [:min :max :avg :sum])))
                            [[] []]
                            numerics)
                          [_ from] (search-stack-from aggregate-schema)
                          [where where-data] (search-stack-args aggregate-schema)
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
                            ;;
                            (str query \newline
                                 (format "group by %s._eid" as))
                            ;;
                            (reduce into [query] (remove nil? [numerics-data where-data])))
                          _ (log/tracef
                              "[%s] Sending numerics aggregate query:\n%s\n%s\n%s"
                              table query-string numerics-data where-data)
                          result (sql/execute! con query core/*return-type*)]
                      (reduce
                        (fn [r {:keys [parent_id]
                                :as data}]
                          (assoc r parent_id
                                 (reduce-kv
                                   (fn [r k v]
                                     (let [[rkey operation k] (str/split (name k) #"\$")
                                           ;; Coerce numeric values to BigDecimal for SQLite/PostgreSQL parity
                                           v' (if (number? v) (bigdec v) v)]
                                       (assoc-in r [(keyword rkey) (keyword operation) (keyword k)] v')))
                                   nil
                                   (dissoc data :parent_id))))
                        nil
                        result))))))
            (process-root [_ location]
              (let [[_ {:keys [entity/table fields
                               decoders recursions entity/as args]}] (clojure.zip/node location)
                    expected-start-result (future
                                            {table (apply array-map
                                                          (reduce
                                                            (fn [r d]
                                                              (conj r
                                                                    (:_eid d)
                                                                    (reduce
                                                                      (fn [data [k t]] (update data k t))
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
                                                                               ;; TODO - test if this is necessary
                                                                               ;; This maybe obsolete since we already know what records
                                                                               ;; to pull and in which order
                                                                               (str
                                                                                 (when (= found-records {})
                                                                                   (modifiers-selection->sql {:args args}))))
                                                                  result (sql/execute!
                                                                           con
                                                                           [root-query]
                                                                           core/*return-type*)]
                                                              (log/tracef "[%s] Root query:\n%s" table root-query)
                                                              result)))})
                    counts (pull-counts nil location)
                    numerics (pull-numerics nil location)]
                (->
                  (cond->
                    @expected-start-result
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
                (log/tracef
                  "[%s] Cursor position %s from table %s. Parents:\n%s"
                  table cursor ftable (str/join ", " parents))
                (if (not-empty parents)
                  (let [expected-result
                        (future
                          (let [[_ {ptable :entity/table}] (clojure.zip/node (clojure.zip/up location))

                                ;;
                                query (pull-query
                                        (update schema :args dissoc :_limit :_offset)
                                        (get found-records (keyword as)) parents)
                                _ (log/tracef
                                    "[%s] Sending pull query:\n%s\n%s"
                                    table (first query) (rest query))
                                relations (cond->
                                            (sql/execute! con query core/*return-type*)
                                            ;;
                                            (some #(contains? args %) [:_offset :_limit])
                                            (as-> relations
                                                  (let [grouping (group-by (keyword falias) relations)]
                                                    (vec
                                                      (mapcat
                                                        #(cond->> %
                                                           (:_offset args) (drop (:_offset args))
                                                           (:_limit args) (take (:_limit args)))
                                                        (vals grouping))))))

                                talias' (keyword talias)
                                falias' (keyword falias)
                                data (reduce
                                       (fn [r d]
                                         (assoc r (get d talias')
                                                ;; TODO - Transform data here
                                                (reduce-kv
                                                  (fn [data k t] (update data k t))
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
                        ;;
                        expected-counts (pull-counts result location)
                        expected-numerics (pull-numerics result location)]
                    (cond->
                      (maybe-pull-children @expected-result location)
                      expected-counts (update-in [:counts cursor] merge @expected-counts)
                      expected-numerics (update-in [:numerics cursor] merge @expected-numerics)))
                  (do
                    (log/tracef
                      "[%s] Couldn't find parents for: %s"
                      etable result)
                    result))))
            ;;
            (process-node [result location]
              (if (= ::ROOT (key (clojure.zip/node location)))
                (process-root result location)
                (process-related result location)))]
      ;;
      (let [result (doall (process-node nil zipper))]
        result))))

(defn pull-roots [con schema found-records]
  ; (log/tracef "[%s] Found records\n%s" "fieoqj" (pprint found-records))
  (binding [*ignore-maybe* false]
    (let [db (pull-cursors con schema found-records)]
      (construct-response schema db found-records))))

;;; ============================================================================
;;; Write Operations (Shared - Database-Agnostic)
;;; ============================================================================

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
          (log/tracef
            "[%s]Pulling references %s for values %s\nQuery: %s"
            reference-table
            (str/join ", " constraint-keys)
            (str/join ", " values')
            query)
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

(defn prepare-references
  [tx {:keys [reference]
       :as analysis}]
  (reduce
    (fn [analysis [reference-table pulled-references]]
      (log/tracef "Pulled references for table: %s\n%s" reference-table (pprint pulled-references))
      (reduce-kv
        (fn [analysis constraint value]
          (let [rows (get-in analysis [:reference reference-table constraint])]
            (reduce
              (fn [analysis row]
                (log/tracef "[%s]Updating row reference %s" row value)
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
                       (log/tracef
                         "[%s]Adding new recursions %s\n%s"
                         table (apply str bindings) sql)
                       (sql/execute-batch! statement bindings core/*return-type*)
                       result)
                     (catch Throwable e
                       (log/errorf
                         e
                         "[%s]Couldn't set entity %s references %s.\nRecursion map:\n%s"
                         table
                         parent
                         children
                         recursion)
                       (throw e))))
                 result
                 bindings))
             result
             mapping))
         result
         recursion))
     ;; Link single relations
     (let [{:keys [:relations/one]} result]
       (reduce-kv
         (fn [result {:keys [table]
                      to :to/field
                      from :from/field} bindings]
           (let [current (set (map first bindings))
                 sql-delete (str
                              "delete from \"" table "\" where \"" from "\" in ("
                              (clojure.string/join ", " (repeat (count current) \?))
                              ")")
                 sql-add (sql/prepare
                           tx
                           [(str "insert into \"" table "\" (\"" from "\", \"" to "\") values (?,?)")])
                 new (filter second bindings)
                 deleted (sql/execute! tx (into [sql-delete] current))]
             (log/tracef
               "[%s]Deleting old relations\n%s\nDeleted:\n%s"
               table sql-delete deleted)
             (log/tracef
               "[%s]Adding new relations\n%s\nIDS:\n%s"
               table sql-add new)
             (sql/execute-batch! sql-add new core/*return-type*)
             result))
         result
         one))
     ;; Link many relations
     (let [{:keys [:relations/many]} result]
       (reduce-kv
         (fn [result {:keys [table]
                      to :to/field
                      from :from/field} bindings]
           (let [current (set (map first bindings))
                 sql-add (sql/prepare
                           tx
                           [(str "insert into \"" table "\" (\"" from "\", \"" to "\") values (?,?)"
                                 (when stack?
                                   " on conflict do nothing"))])
                 new (filter second bindings)]
             (when-not stack?
               (let [query (str
                             "delete from \"" table "\" where \"" from "\" in ("
                             (clojure.string/join ", " (repeat (count current) \?))
                             ")")
                     deleted (sql/execute! tx (into [query] current))]
                 (log/debugf
                   "[%s]Deleting relations\n%s\nDeleted:\n%s"
                   table query deleted)))
             (log/tracef
               "[%s]Adding new relations\n%s\n%s"
               table sql-add new)
             (sql/execute-batch! sql-add new core/*return-type*)
             result))
         result
         many)))
   analysis))

(defn publish-delta
  [{many-relations :relations/many
    one-relations :relations/one
    entities :entity
    entity-mapping :entity/mapping
    :as analysis}]
  (async/go
    (let [find-record (memoize
                        (fn [table eid]
                          (some
                            (fn [{:keys [_eid]
                                  :as data}]
                              (when (= _eid eid)
                                data))
                            (vals (get entities table)))))]
      (when (not-empty many-relations)
        (doseq [[{:keys [relation from to]
                  from-table :from/table
                  to-table :to/table} delta] many-relations]
          (log/debugf "[Datasets] Publishing relation delta: %s" relation)
          (async/put!
            core/*delta-client*
            {:element relation
             :delta {:type :link
                     :from from
                     :to to
                     :data (map
                             (fn [[fid tid]]
                               [(find-record from-table fid)
                                (find-record to-table tid)])
                             delta)}})))
      (when (not-empty one-relations)
        (doseq [[{:keys [relation from to]
                  from-table :from/table
                  to-table :to/table} delta] one-relations]
          (log/debugf "[Datasets] Publishing relation delta: %s" relation)
          (async/put!
            core/*delta-client*
            {:element relation
             :delta {:from from
                     :to to
                     :type :link
                     :data (map
                             (fn [[fid tid]]
                               [(find-record from-table fid)
                                (find-record to-table tid)])
                             delta)}}))))
    (doseq [[table delta] entities]
      (log/debugf "[Datasets] Publishing entity delta: %s" (get entity-mapping table))
      (async/put!
        core/*delta-client*
        {:element (get entity-mapping table)
         :delta {:type :change
                 :data (vals delta)}})))
  analysis)

;;; ============================================================================
;;; Unified Store Entity Records (Database-Agnostic via Protocol)
;;; ============================================================================

(defn store-entity-records
  "Stores entity records in database with order-independent mapping.

  This unified implementation works for all SQL databases by using the
  SQLDialect protocol for database-specific syntax:
  - placeholder-for-type: PostgreSQL uses ?::type for enums, SQLite uses ?
  - excluded-ref: PostgreSQL uses EXCLUDED.col, SQLite uses excluded.col

  ID Handling:
  - Generates ID for records that don't have one
  - Uses returned ID to build mapping (order-independent)
  - Falls back to constraint-based mapping for updates

  Mapping Reconstruction:
  - Builds id->tmpid lookup before INSERT
  - Builds constraint->tmpid lookup for fallback
  - After INSERT, uses returned id first, then constraint values
  - This approach works regardless of database result ordering

  Database Compatibility:
  - PostgreSQL: Works
  - Aurora: Works
  - Cockroach: Works
  - SQLite: Works"
  [tx {:keys [entity constraint]
       :as analysis}]
  (def analysis analysis)
  (reduce-kv
    (fn [analysis entity-table rows]
      (reduce-kv
        (fn [analysis ks rows]
          (log/debugf
            "[%s] Storing entity table rows %s\n%s"
            entity-table (str/join ", " ks) (str/join "\n" rows))

          ;; === EARLY RETURN: Handle empty rows ===
          (if (empty? rows)
            analysis

            (let [;; === STEP 1: Generate IDs for rows that don't have them ===
                  rows-with-id (map (fn [[row-data tmp-id]]
                                      (let [entity-id (or (id/extract row-data)
                                                          (id/generate))]
                                        [(assoc row-data (id/key) entity-id) tmp-id entity-id]))
                                    rows)

                  ;; === STEP 2: Build Dual Mappings (id + constraint) ===
                  ;; Primary mapping: id -> tmpid
                  id->tmpid (into {}
                                  (map (fn [[_ tmp-id entity-id]] [entity-id tmp-id])
                                       rows-with-id))

                  ;; Determine constraint for this batch
                  constraint-keys (if (contains? ks (id/key))
                                    [(id/key)]
                                    (some #(when (every? ks %) %)
                                          (get constraint entity-table)))

                  ;; Fallback mapping: constraint-values -> tmpid (skip NULLs)
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

                  ;; === STEP 3: Ensure ID in Columns ===
                  ks' (if (contains? ks (id/key))
                        ks
                        (conj (vec ks) (id/key)))

                  ;; === STEP 4: Extract Row Values (using ks' WITH id) ===
                  row-data (if (empty? ks')
                             (repeat (count rows-with-id) [])
                             (map (apply juxt ks')
                                  (map first rows-with-id)))

                  columns-fn #(str \" (name %) \")
                  ks-quoted (map columns-fn ks')

                  ;; Get field types for enum casting (database-specific via protocol)
                  entity-uuid (get-in analysis [:entity/mapping entity-table])
                  entity-schema (when entity-uuid
                                  (deployed-schema-entity entity-uuid))
                  field-types (when entity-schema
                                (reduce-kv
                                  (fn [m _ {:keys [key type]
                                            ename :enum/name}]
                                    (if ename
                                      (assoc m key ename)  ; Store enum type name
                                      (assoc m key type)))
                                  {}
                                  (:fields entity-schema)))

                  id-field (id/field)

                  ;; Generate placeholders using protocol (PostgreSQL: ?::type, SQLite: ?)
                  placeholder-fn (fn [k]
                                   (let [field-type (get field-types k)]
                                     (if (and field-type
                                              (not (core/reference-type? field-type)))
                                       (proto/placeholder-for-type *db* field-type)
                                       "?")))
                  values-? (str \( (str/join ", " (map placeholder-fn ks')) \))

                  ;; === STEP 5: Build Query with Order-Independent RETURNING ===
                  on-values (map columns-fn constraint-keys)

                  ;; Dedupe RETURNING columns
                  return-cols (distinct (concat [:_eid (id/key)] constraint-keys))
                  return-sql (str/join ", " (map columns-fn return-cols))

                  ;; Determine fields to update (exclude id and constraint columns)
                  fields-to-update (remove
                                     (fn [k]
                                       (let [kname (name k)]
                                         (or (= kname id-field)
                                             (some #(= (name %) kname) constraint-keys))))
                                     ks')

                  ;; Build UPDATE SET clause using protocol (EXCLUDED vs excluded)
                  do-set (if (empty? fields-to-update)
                           (str (columns-fn (keyword id-field)) "=" (proto/excluded-ref *db* (str \" id-field \")))
                           (str/join ", "
                                     (map (fn [col]
                                            (let [quoted (columns-fn col)]
                                              (str quoted "=" (proto/excluded-ref *db* quoted))))
                                          fields-to-update)))

                  query (str
                          "INSERT INTO \"" entity-table "\" ("
                          (str/join ", " ks-quoted) ") VALUES "
                          (str/join ", " (repeat (count row-data) values-?))
                          (when (not-empty constraint-keys)
                            (let [on-sql (str/join ", " on-values)]
                              (str " ON CONFLICT (" on-sql ") DO UPDATE SET " do-set)))
                          " RETURNING " return-sql)

                  ;; === STEP 6: Execute Query ===
                  _ (log/tracef
                      "[%s] Storing entity group with order-independent mapping\nConstraint: %s\nID mapping size: %d\nConstraint mapping size: %d\nQuery:\n%s"
                      entity-table
                      constraint-keys
                      (count id->tmpid)
                      (count constraint->tmpid)
                      query)

                  _ (do
                      (def query query)
                      (def row-data row-data))
                  result (sql/execute!
                           tx (into [query] (flatten row-data))
                           :edn)

                  _ (log/tracef
                      "[%s] Stored entity group result:\n%s"
                      entity-table (pprint result))

                  ;; === STEP 7: Reconstruct Mapping (Order-Independent with Fallback) ===
                  mapping (reduce
                            (fn [m result-row]
                              (let [entity-id (id/extract result-row)
                                    cvals (when (not-empty constraint-keys)
                                            (select-keys result-row constraint-keys))
                                    ;; Normalize constraint values
                                    cvals-normalized (when cvals
                                                       (reduce-kv
                                                         (fn [m k v]
                                                           (assoc m k
                                                                  (if (and (string? v)
                                                                           (= k (id/key))
                                                                           (= :euuid (id/key)))
                                                                    (try
                                                                      (java.util.UUID/fromString v)
                                                                      (catch Exception _ v))
                                                                    v)))
                                                         {}
                                                         cvals))
                                    ;; Try id lookup with type normalization
                                    tmp-id (or
                                             (get id->tmpid entity-id)
                                             (when (and (string? entity-id) (= :euuid (id/key)))
                                               (try
                                                 (get id->tmpid (java.util.UUID/fromString entity-id))
                                                 (catch Exception _ nil)))
                                             (when (and (instance? java.util.UUID entity-id) (= :euuid (id/key)))
                                               (get id->tmpid (str entity-id)))
                                             (get constraint->tmpid cvals)
                                             (get constraint->tmpid cvals-normalized))]
                                (if tmp-id
                                  (do
                                    (log/tracef
                                      "[%s] Mapped result id=%s constraint=%s -> tmp-id=%s"
                                      entity-table entity-id cvals tmp-id)
                                    (assoc m tmp-id result-row))
                                  (do
                                    (log/errorf
                                      "[%s] CRITICAL: Failed to map result to tmp-id! id=%s (type=%s) constraint=%s"
                                      entity-table entity-id (type entity-id) cvals)
                                    (throw
                                      (ex-info
                                        (format "[%s] Failed to map database result to tmp-id" entity-table)
                                        {:entity-table entity-table
                                         :id entity-id
                                         :constraint cvals
                                         :result-row result-row}))))))
                            {}
                            result)]

              ;; === STEP 8: Merge Results Back ===
              (log/tracef "[%s] Final mapping size: %d" entity-table (count mapping))
              (reduce-kv
                (fn [analysis tmp-id data]
                  (log/tracef "[%s] Merging updated data %s=%s" entity-table tmp-id data)
                  (update-in analysis [:entity entity-table tmp-id] merge data))
                analysis
                mapping))))
        analysis
        (group-entity-rows rows)))
    analysis
    entity))

;;; ============================================================================
;;; Unified Tree Operations (Database-Agnostic via Protocol)
;;; ============================================================================

(defn build-recursive-cte
  "Builds a recursive CTE query for tree operations.

  Uses SQLDialect protocol for database-specific syntax:
  - array-init-expr: PostgreSQL array[x], SQLite ',' || x || ','
  - array-append-expr: PostgreSQL path || x, SQLite path || x || ','
  - array-contains-clause: PostgreSQL x=any(path), SQLite INSTR(path, x) > 0
  - cycle-literal: PostgreSQL true/false, SQLite 1/0

  Args:
    db - Database connection (for protocol dispatch)
    opts - Map with :table, :on (field), :root-ids, :id-field, :select-fields

  Returns:
    SQL query string for the recursive CTE"
  [{:keys [table on root-ids id-field select-fields]}]
  (let [on' (name on)
        select-cols (str/join ", " (conj select-fields "_eid" id-field))]
    (if (not-empty root-ids)
      (format
        "WITH RECURSIVE tree AS (
           SELECT %s, %s as path, %s as is_cycle
           FROM \"%s\"
           WHERE %s IN (%s)
           UNION ALL
           SELECT g.%s, %s, %s
           FROM \"%s\" g
           INNER JOIN tree t ON t._eid = g.%s
           WHERE NOT %s
         )
         SELECT %s FROM tree WHERE is_cycle = %s"
        select-cols
        (proto/array-init-expr *db* "_eid")
        (proto/cycle-literal *db* false)
        table
        id-field
        (str/join ", " (map #(str "'" % "'") root-ids))
        (str/join ", g." (str/split select-cols #", "))
        (proto/array-append-expr *db* "t.path" "g._eid")
        (proto/cycle-literal *db* true)
        table
        on'
        (proto/array-contains-clause *db* "t.path" "g._eid")
        select-cols
        (proto/cycle-literal *db* false))
      (format "SELECT %s FROM \"%s\"" select-cols table))))

(defn build-in-clause
  "Builds an IN clause for multiple values using the protocol.

  PostgreSQL: column=any(?) with array parameter
  SQLite: column IN (?,?,?) with individual parameters

  Args:
    column - Column name
    values - Collection of values

  Returns:
    Map with :sql and :params"
  [column values]
  (let [{:keys [sql params-fn]} (proto/in-clause *db* column (count values))]
    {:sql sql
     :params (params-fn values)}))



;;; ============================================================================
;;; Unified Query Operations (Database-Agnostic)
;;; ============================================================================

(defn search-entity-roots
  "Search for entity root records based on schema criteria.

  This is the core function for finding records that match query arguments.
  Used by search-entity, purge-entity, and other query functions."
  ([schema]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (search-entity-roots connection schema)))
  ([connection schema]
   (let [focused-schema (focus-order schema)
         [[root-table :as tables] from] (search-stack-from focused-schema)
         [where data] (search-stack-args focused-schema)
         selected-ids (str/join
                        ", "
                        (map
                          #(str (name %) "._eid as " (name %))
                          tables))
         distinct-on (or (distinct->sql schema)
                         (when (and
                                 (contains? (:args focused-schema) :_limit)
                                 (not (contains? (:args focused-schema) :_order_by)))
                           (format "distinct on (%s._eid)" (name root-table))))
         modifiers (modifiers-selection->sql schema)
         query (as-> (format "select %s %s from %s" (str distinct-on) selected-ids from) query
                 (if (not-empty where) (str query \newline "where " where) query)
                 (if modifiers (str query " " modifiers) query))
         [r :as ids] (if (and
                           (empty? distinct-on)
                           (empty? where)
                           (empty? data)
                           (empty? (get-in schema [:args :_order_by]))
                           (= 1 (count tables)))
                       nil
                       (do
                         (log/tracef
                           "Query for roots:\n%s\nData:\n%s"
                           query (pprint data))
                         (sql/execute!
                           connection (into [query] data)
                           core/*return-type*)))]
     (if (not-empty r)
       (let [ks (keys r)]
         (reduce
           (fn [ids' k]
             (assoc ids' k (distinct (map #(get % k) ids))))
           nil
           ks))
       (if (nil? ids) {} nil)))))

(defn search-entity
  "Search for entities matching the given arguments and selection."
  ([entity-id args selection]
   (search-entity entity-id args selection #{:search :read}))
  ([entity-id args selection operations]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (search-entity connection entity-id args selection operations)))
  ([connection entity-id args selection operations]
   (entity-accessible? entity-id operations)
   (def entity-id entity-id)
   (def args args)
   (def selection selection)
   (binding [*operation-rules* operations]
     (let [schema (selection->schema entity-id selection args)
           _ (log/tracef "Searching for entity\n%s" schema)
           roots (search-entity-roots connection schema)]
       (when (some? roots)
         (log/tracef "[%s] Found roots: %s" entity-id (str/join ", " roots))
         (pull-roots connection schema roots))))))

(defn get-entity
  "Get a single entity by its unique identifier(s)."
  ([entity-id args selection]
   (get-entity entity-id args selection #{:read :get}))
  ([entity-id args selection operations]
   (assert (some? args) "No arguments to get entity for...")
   (entity-accessible? entity-id operations)
   (log/debugf
     "[%s] Getting entity\nArgs:\n%s\nSelection:\n%s"
     entity-id (pprint args) (pprint selection))
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

(defn purge-entity
  "Purge (delete) entities matching the given criteria.

  Uses proto/in-clause for database-agnostic IN clause generation."
  ([entity-id args selection]
   (with-open [connection (jdbc/get-connection (:datasource *db*))]
     (let [schema (selection->schema entity-id selection args)
           enforced-schema (as-> nil _
                             (binding [*operation-rules* #{:owns}]
                               (selection->schema entity-id selection args))
                             (binding [*operation-rules* #{:delete}]
                               (selection->schema entity-id selection args)))]
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
               (letfn [(construct-statement [table _eids]
                         (log/debugf "[%s]Constructing purge for eids #%d: %s" table (count _eids) (str/join ", " _eids))
                         ;; Use protocol for database-agnostic IN clause
                         (let [{:keys [sql params-fn]} (proto/in-clause *db* "_eid" (count _eids))]
                           (into [(str "delete from \"" table "\" where " sql)] (params-fn _eids))))
                       (process-statement [r k v]
                         (conj r (construct-statement k (keys v))))]
                 (let [db (pull-cursors connection schema roots)
                       response (construct-response schema db roots)
                       delete-statements (reduce-kv process-statement [] db)]
                   (doseq [query delete-statements]
                     (log/debugf "[%s]Purging entity rows with %s" entity-id query)
                     (sql/execute! connection query core/*return-type*))
                   (async/put! core/*delta-client* {:element entity-id
                                                    :delta {:type :purge
                                                            :data response}})
                   response))
               []))))))))

(defn set-entity
  "Set (create/update) entity data.

  This is the main write operation that handles:
  - Analyzing data structure
  - Preparing references
  - Storing entity records
  - Linking relations
  - Publishing deltas"
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
         (project-saved-entities result)
         (link-relations tx result stack?)
         (publish-delta result)
         (pull-roots result))))))
