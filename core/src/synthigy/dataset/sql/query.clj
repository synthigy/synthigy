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
   [clojure.core.cache :as cache]
   [clojure.pprint]
   [clojure.set]
   [clojure.string :as str]
   [synthigy.log :as log]
   clojure.zip
   [next.jdbc :as jdbc]
   [nano-id.core :refer [nano-id]]
   [synthigy.dataset
    :refer [deployed-relation deployed-entity deployed-model]]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.rls :as rls-runtime]
   [synthigy.dataset.sql.naming :refer [normalize-name
                                        entity->table-name
                                        relation->table-name
                                        entity->relation-field]]
   [synthigy.dataset.sql.protocol :as proto]
   [synthigy.dataset.sql.rls :as rls]
   [synthigy.dataset.sql.schema :as schema]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]
   [synthigy.transit :refer [->transit]]))

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
;;; Fetch concurrency (Dynamic Vars + macros)
;;; ============================================================================
;;
;; `pull-cursors` walks a schema and fires multiple sibling SQL queries — root,
;; counts, numerics, related, children. How those siblings execute is
;; controlled by `*fetch-mode*`:
;;
;;   :serial    Body wrapped in `delay`. Runs on the deref'ing thread using
;;              the shared `con`. Safe inside a write transaction (visible
;;              to the tx, snapshot consistent). No pool churn.
;;
;;   :parallel  Body wrapped in `future`. Each leaf SQL acquires a fresh
;;              connection via `(jdbc/get-connection (:datasource *db*))`
;;              and releases it on completion. Real DB-level parallelism.
;;              NEVER use inside a write transaction — parallel readers on
;;              fresh connections won't see uncommitted writes from the
;;              outer tx.
;;
;; Cited rationale:
;;   pgjdbc: "The driver makes no guarantees that methods on connections
;;   are synchronized. It will be up to the caller to synchronize calls to
;;   the driver."  https://jdbc.postgresql.org/documentation/thread/
;;
;;   SQLite (SERIALIZED mode, default): "SQLite uses mutexes to serialize
;;   access to each object."  https://www.sqlite.org/threadsafe.html
;;
;; Default is `:serial` (safe). Backends opt into `:parallel` at their
;; read-only protocol entry points (Postgres only; SQLite coerces to
;; `:serial` to avoid pool=1 deadlocks).

(def ^:dynamic *fetch-mode*
  "Sibling-fetch concurrency for `pull-cursors`. :serial or :parallel.
   Bound by backend `ModelQueryProtocol` extensions at the entry point.
   See header comment for semantics."
  :serial)

(def ^:dynamic ^java.sql.Connection *fetch-con*
  "The connection a fetch task should use for SQL. In :serial mode this
   equals the `con` passed into `pull-cursors`. In :parallel mode each
   leaf fetch binds it to its own fresh pool connection.

   Inside `pull-cursors` bodies, use *fetch-con* — never the lexical `con`
   from the surrounding closure."
  nil)

(defmacro fetch-on-conn
  "Schedule a leaf SQL fetch. Returns a deref-able.

     :serial   → wraps body in `delay`; uses *fetch-con*.
     :parallel → wraps body in `future`; opens a fresh connection from
                 `(:datasource *db*)`, binds *fetch-con*, runs body,
                 releases connection on exit."
  [& body]
  `(case *fetch-mode*
     :serial   (delay ~@body)
     :parallel (future
                 (with-open [c# (jdbc/get-connection (:datasource *db*))]
                   (binding [*fetch-con* c#]
                     ~@body)))))

(defmacro parallel-task
  "Schedule a recursive pull task. Controls thread fanout only — does NOT
   acquire a new connection itself. Use for `maybe-pull-children`-style
   branching where the leaves further down will acquire their own
   connections via `fetch-on-conn`.

     :serial   → wraps body in `delay` (sequential).
     :parallel → wraps body in `future` (concurrent fanout)."
  [& body]
  `(case *fetch-mode*
     :serial   (delay  ~@body)
     :parallel (future ~@body)))

;;; ============================================================================
;;; Schema Caching (Database-Agnostic)
;;; ============================================================================

;; The three deploy-coupled indexes live in ONE atom so a redeploy swaps
;; them atomically — a reader can never observe a fresh :entity index against
;; a stale :relation/:attribute-key one (the prior four separate reset!s left
;; a torn-read window). Keys:
;;   :entity        {normalized-entity-name -> entity-id}
;;   :relation      {relation-uuid -> {:entity name :label label}}
;;   :attribute-key {attr-xid-keyword -> field-keyword} — built at deploy time
;;                  from the schema's :fields entries; used by subscription
;;                  translate-delta to rewrite before/after maps from
;;                  attribute-xid keys (substrate-captured) to user-facing
;;                  attribute keys (what /data uses).
(defonce ^:private _indexes (atom nil))
(defonce ^:private _template-cache
  (atom (cache/ttl-cache-factory {} :ttl (* 30 60 1000))))

(defn- normalize-entity-name
  "Normalize entity name for index lookup: camelCase → snake_case, lowercase."
  [name]
  (-> name
      (str/replace #"([a-z])([A-Z])" "$1_$2")
      str/lower-case
      (str/replace #"[\s]+" "_")))

(defn- build-relation-index
  "Build reverse index {relation-uuid -> {:entity entity-name :label to-label}}
  from the deployed model. Returns nil if no model is available."
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

(defn deploy-schema
  "Caches the runtime schema (from model->schema) for fast access.
  Also rebuilds the entity-name, relation reverse, and attribute-key
  indices.

  This should be called when a new model is deployed to avoid
  regenerating the schema on every query."
  [s]
  (schema/set-deployed-schema! s)
  (reset! _template-cache (cache/ttl-cache-factory {} :ttl (* 30 60 1000)))
  ;; Build all three indexes, then publish them in a single reset! so readers
  ;; see a consistent set (see _indexes). The attribute-key map flattens every
  ;; entity's :fields into one {attr-xid-keyword -> field-key} map; the attr-xid
  ;; is keyword-ified to line up with the substrate's before/after envelope
  ;; shape (the trigger writes attr-xids as JSON string keys, but
  ;; synthigy.json/<-json runs them through pkey-fn which keywordizes any string
  ;; that has letters and isn't UUID-shaped, so envelopes arrive with
  ;; :before { :<attr-xid> v, ... }).
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
                                  [(keyword (str attr-id)) field-key]))}))

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

(defn- levenshtein
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

(defn- suggest-similar
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
  "Resolve user-provided entity reference to entity ID from deployed schema.

   Accepts:
     - Raw entity ID (UUID or XID string) — returned as-is if present in deployed schema
     - Human-readable name: 'Dataset Version', 'dataset_version', 'DatasetVersion'

   Returns entity ID or throws if not found."
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
  "Returns {relation-uuid -> {:entity name :label label}} reverse index.
   Built automatically when schema is deployed."
  []
  (:relation @_indexes))

(defn attribute-key-index
  "Returns {attr-xid-keyword -> field-keyword} flat map across all
   entities. Built automatically when schema is deployed. Used by the SSE
   translate-delta to rewrite substrate-captured before/after maps
   (attr-xid-keyed, post-pkey-fn keywordization) into user-facing
   attribute-key shape."
  []
  (:attribute-key @_indexes))

(defn resolve-relation
  "Resolve entity name + relation label to relation UUID.

   Returns relation UUID or throws if not found."
  [entity-name label]
  (let [entity-uuid (resolve-entity entity-name)
        model (deployed-model)
        entity (core/get-entity model entity-uuid)
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
  "Gets a specific entity from the deployed schema by UUID.
  Delegates to synthigy.dataset.sql.schema/deployed-schema-entity."
  [entity-id]
  (schema/deployed-schema-entity entity-id))

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
    ;; NOTE: "transit" kept in scalar-types for backwards compatibility
    ;; but no longer decoded/encoded — treated as plain string

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
       :roles (access/role-ids)}))))

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
       :code "ENTITY_FORBIDDEN"
       :entity id
       :entity-name entity-name
       :roles (access/role-ids)}))))

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
                                   "uuid" "avatar" "hashed" "transit" nil) result
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
                             ;; Use the recursion's OWN schema (from :relations k)
                             ;; so its :fields come from the caller's sub-selection
                             ;; on this recursion — not from the parent's fields,
                             ;; which would silently intersect and drop columns.
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
                           (get-in entity [to-table to :_eid])
                           ;; carry the in-memory xids so link-relations can
                           ;; denormalize from_xid/to_xid without a per-row
                           ;; correlated subselect.
                           (get-in entity [from-table from :xid])
                           (get-in entity [to-table to :xid])]))
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
                           (get-in entity [to-table to :_eid])
                           ;; carry the in-memory xids so link-relations can
                           ;; denormalize from_xid/to_xid without a per-row
                           ;; correlated subselect.
                           (get-in entity [from-table from :xid])
                           (get-in entity [to-table to :xid])]))
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
                                          ;; :unique via the active-aware accessor —
                                          ;; a composite unique key whose attribute was
                                          ;; deactivated is dropped (all-or-nothing), so
                                          ;; get/sync matching never keys on a dead combo.
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
                                 [(id/key)])))))
         ;; Bundle every (entity, IAM-context)-stable derivation up so the
         ;; per-row `transform-object` loop reads precomputed values instead
         ;; of rederiving from schema on every row. Memoized per analyze-data
         ;; call → cache lives for the batch + nested-relation walks, gets
         ;; dropped when the call returns. IAM context is request-scoped, so
         ;; relation-accessible? results are safe to bundle here.
         entity-profile
         (memoize
          (fn [entity-euuid]
            (let [{:keys [relations fields recursions] :as e} (find-entity entity-euuid)
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
                  cs (get-constraints entity-euuid)
                  recursions-set (set recursions)
                  ;; IAM-filtered relation keys: relation-accessible? throws on
                  ;; deny, so this acts as an assertion. Once cached, per-row
                  ;; cost is just the (contains? data k) check.
                  accessible-rel-keys
                  (into []
                        (keep (fn [k]
                                (when-not (contains? recursions-set k)
                                  (let [{:keys [relation to from]} (get relations k)]
                                    (when (and relation
                                               (relation-accessible? relation [from to] #{:write :owns}))
                                      k)))))
                        (keys relations))]
              {:entity            e
               :scalar-fields     scalar-fields
               :refs              refs
               :scalar-field-keys (conj (mapv :key scalar-fields) (id/key))
               :constraints       cs
               :constraint-keys   (flatten cs)
               :recursions-set    recursions-set
               :accessible-rel-keys accessible-rel-keys})))]
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
                (let [profile (entity-profile entity-euuid)
                      {:keys [entity refs
                              scalar-field-keys
                              constraints constraint-keys
                              accessible-rel-keys]} profile
                      {:keys [relations recursions table]
                       {mandatory-fields :mandatory} :constraints} entity
                      data (shallow-snake (dissoc data :tmp/id))
                      fields-data (select-keys data scalar-field-keys)
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
                              ("boolean" "string" "int" "float" "timeperiod" "currency" "uuid" "transit" nil) identity
                                ;; timestamp - encode via protocol (handles string → Instant conversion)
                              "timestamp" (fn [v] (proto/encode *db* "timestamp" v))
                              "hashed" (fn [v] (when v (hashers/derive v)))
                              (fn [v]
                                (when v
                                  (proto/encode *db* t v)))))))
                       fields-data
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
                      valid-relation-keys (filterv #(contains? data %) accessible-rel-keys)
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
         ;; Pre-compute the relation-key set so the scalar walk below can
         ;; tell a typo (`{movies: …}` when the entity has `movie`) from
         ;; a legitimate relation key (which gets filtered into the
         ;; `objects` branch at line 1615). Both `relations` keys and
         ;; `refs` :key fields are valid here; `valid-relations` lower
         ;; down (line 1537) widens the same set further with order-by /
         ;; distinct-on relations, but those are args-side only.
         relation-key-set (cond-> (set (keys relations))
                            (not-empty refs) (clojure.set/union (set (map :key refs)))
                            (not-empty recursions) (clojure.set/union (set recursions)))
         ;; Selection-level operators consumed by base-schema below
         ;; (`:count` → :counted?; `:_count`/`:_agg` → aggregate folds).
         ;; They appear as keys in the selection map but are NOT attributes,
         ;; so the scalar walk passes them through as no-ops.
         selection-operator-keys #{:count :_count :_agg}
         scalars (reduce-kv
                  (fn [r k [{args :args}]]
                    (cond
                      (valid-fields k)            (assoc r k args)
                      ;; Alternate id key (`:euuid` in xid mode, `:xid` in euuid mode) —
                      ;; wire-allowed but not a real column. The xid-structural
                      ;; projection at line 1743 dual-projects both, so accepting it
                      ;; here as a no-op preserves backwards-compat with selections
                      ;; that include both id keys.
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
                                   ;; Skip Levenshtein on pathological-sized entities (>200 candidates).
                                   ;; Wire still has entity + attribute so the client can act.
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
         ;; Operator + modifier keys recognized at any level of an args
         ;; walk. Used to distinguish a legitimate args modifier from a
         ;; typo'd attribute below. Per-field condition operators
         ;; (`:_eq`, `:_in`, …) appear one level deeper than this walk
         ;; reaches and are validated in process-where-conditions
         ;; (UNKNOWN_OPERATOR throw at line 2099) — they're listed here
         ;; defensively so a future change that walks deeper doesn't
         ;; mistake them for typo'd attributes.
         args-operator-keys #{:_where :_or :_and :_not :_maybe :_count :_agg
                              :_limit :_offset :_order_by :_distinct :_join
                              :_eq :_neq :_lt :_lte :_le :_gt :_gte :_ge
                              :_in :_nin :_not_in :_like :_ilike :_is_null}
         arg-fields (letfn [(join-args
                              ([args] (join-args args #{}))
                              ([args result]
                               (reduce-kv
                                (fn [result k _]
                                  (let [result'
                                        (cond
                                          (valid-fields k)        (conj result k)
                                          (args-operator-keys k)  result
                                          ;; Nested-relation args (`{movies: {_where ...}}`) — let the
                                          ;; nested selection->schema call handle its own validation.
                                          (relation-key-set k)    result
                                          ;; Plain attribute key at args top-level (the `{xid: "X"}` case)
                                          ;; or any other unknown — typo. Throw with entity + attribute
                                          ;; + hint so the client sees a typed error instead of an empty
                                          ;; result from a silently-dropped predicate.
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
         ;; Build encoders for query arguments using TypeCodec protocol
         encoders (cond->
                   (reduce
                    (fn [result field]
                      (let [t (get field->type field)]
                        (case t
                          ;; Native types - no encoding needed. NOTE: a "uuid"
                          ;; ATTRIBUTE type is left as pass-through here — it is a
                          ;; uuid column on PG (string args would hit `uuid =
                          ;; varchar`) but TEXT on SQLite, so a blanket coercion
                          ;; is backend-unsafe. The structural id key is handled
                          ;; below (euuid-gated). uuid-typed user attributes on PG
                          ;; remain a separate, pre-existing gap.
                          ("boolean" "string" "int" "float" "json"
                                     "timeperiod" "currency"
                                     "uuid" "hashed" "transit" nil) result
                          ;; Timestamp - use protocol for database-specific encoding
                          ;; PostgreSQL: pass Instant directly (JDBC handles conversion)
                          ;; SQLite: convert to ISO-8601 string (text comparison works)
                          "timestamp" (assoc result field
                                             (fn [v] (proto/encode *db* "timestamp" v)))
                          ;; Default: enum types - use protocol
                          (assoc result field (fn [v] (proto/encode *db* t v))))))
                    nil
                    arg-fields)
                   ;; The structural id column isn't a model attribute (no
                   ;; field->type entry), so add its coercion explicitly via the
                   ;; id seam. `id/coerce-arg` is a no-op in xid mode (varchar);
                   ;; in euuid mode it binds a uuid (PG refuses uuid = varchar).
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
                              ;; `_agg` schema build, `_count`-pattern parity:
                              ;;
                              ;;   :_agg [{:selections {<rel>
                              ;;           [{:alias? :args? :selections {<attr> [{:selections {<fn> nil}} ...]}}
                              ;;            ...]}}]
                              ;;
                              ;; Each entry stores its own filtered/aliased view of
                              ;; the same source relation. Entries on the same
                              ;; relation SHARE one gensym pair (`link_X` /
                              ;; `data_X`); per-entry predicates ride on each
                              ;; entry's :args and surface only as SELECT-side
                              ;; case-when material in `pull-numerics`. This is
                              ;; the same Aggregate Hoist principle we use for
                              ;; `_count`.
                              :_agg
                              (reduce
                               (fn [schema {operations :selections}]
                                 (reduce-kv
                                  (fn [schema relation entries]
                                    (let [rkey (keyword (name relation))
                                          rdata (get relations rkey)
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
                                               ;; Same gensym as siblings; own predicate.
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

;; When TRUE, `query-selection->sql` skips `:_where` clauses for
;; relations that declare `:_join "left"` — those predicates have
;; already been embedded in the JOIN ON clause by `search-stack-from`,
;; so re-emitting them in WHERE would double-apply (and turn LEFT
;; into effective INNER via NULL-rejection).
;;
;; The root-query path (`search-entity-roots` etc.) binds this true.
;; The pull-query path leaves it false — pull runs a separate SELECT
;; against the relation's table, with no parent JOIN, so the relation's
;; `:_where` belongs in WHERE there.
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
         ;; Both Postgres and Cockroach store JSON as jsonb and need
         ;; `column #>> '{}'` to extract a JSON string back to text for
         ;; equality predicates. SQLite stores JSON as TEXT so no
         ;; unwrapping needed.
         postgres? (or (instance? synthigy.db.Postgres *db*)
                       (instance? synthigy.db.Cockroach *db*))
         ;; LEFT-joined relations: their `:_where` predicates belong in
         ;; the JOIN ON clause (handled by `search-stack-from`), not in
         ;; the global WHERE — otherwise NULL-rejection from the WHERE
         ;; turns a LEFT JOIN into an effective INNER. We skip `:_where`
         ;; here when the schema declares `:_join "left"`.
         left-relation? (= "left"
                           (some-> (:_join operators)
                                   ((fn [v] (if (keyword? v) (name v) v)))
                                   str/lower-case))]
     (reduce
      (fn [[statements data] [field constraints]]
        (let [field-type (get field-types field)
              ;; For JSON fields on PostgreSQL, use ->> to extract as text for string comparisons
              ;; This extracts the actual string value from JSON, not the JSON representation
              ;; SQLite stores JSON as TEXT so no special handling needed
              field' (if (not-empty prefix) (str prefix \. (name field)) (name field))
              field' (if (and postgres? (= "json" field-type))
                       (str "(" field' " #>> '{}')")
                       field')
              ;; PG enum predicates need an explicit `::enum_type` cast on the
              ;; placeholder. Without it JDBC binds the value as varchar and PG
              ;; sees `enum = varchar` / `enum IN (varchar, ...)` with no
              ;; matching operator (ERROR: operator does not exist). Enum field
              ;; types carry the canonical PG enum name (e.g. "project_task_priority")
              ;; — anything not in `scalar-types` is a custom enum. SQLite has
              ;; no enums so `ph` stays `?`.
              ph (if (and postgres? field-type (not (scalar-types field-type)))
                   (str "?::" field-type)
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
               ;; When specified field is nested relation
              (if-not *deep* [statements data]
                      (let [[statements' data'] (query-selection->sql (get-in schema [:relations field]))]
                        [(into statements statements')
                         (into data data')]))

               ;; Handle fields. When LEFT relation + skip mode, drop
               ;; everything except meta-keys — predicates and combinators
               ;; (whether `:_where`/`:_or`/`:_and` or flat field keys)
               ;; have moved to JOIN ON.
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
                                             :_not_in (if-not (empty? cv)
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
                                             ;; Unknown operator — structured error so callers
                                             ;; can branch and see what went wrong.
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
                                                      ":_in :_not_in :_like :_ilike :_is_null")
                                                 {:code "UNKNOWN_OPERATOR"
                                                  :operator cn
                                                  :path [:_where]}))))
                                 data (case cn
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
  "Args keys that are NOT field predicates — they're metadata about the
   query (join type, paging, ordering, distinct). Everything else in
   args is either a field-name predicate or an explicit predicate
   combinator (`:_or` / `:_and` / legacy `:_where` / `:_maybe`)."
  #{:_join :_limit :_offset :_order_by :_distinct :_count :_agg})

(defn- relation-on-predicate
  "Build a SQL predicate fragment + data values from a LEFT relation's
   args, scoped to its `:entity/as` alias. Reads field predicates flat
   from the args (everything except meta-keys); also accepts the legacy
   `:_where` wrapper if present. Returns `[sql-fragment data-vec]`, or
   nil if no predicate.

   The fragment lands in the JOIN ON clause so LEFT semantics actually
   keep parents without a matching joined row — without this, `WHERE`'s
   null-rejection (`x = TRUE` is null when the LEFT join produced no
   row) collapses LEFT into effective INNER."
  [relation-schema]
  (let [args      (:args relation-schema)
        ;; Backward compat: if a client/test still wraps in :_where,
        ;; unwrap it. New compile output emits flat.
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
;;
;; Root-finding must NOT join through constrained relations: a JOIN
;; multiplies root rows (one per matching child), so `_limit` then slices
;; the *joined* set and yields fewer than N distinct roots. The historic
;; `DISTINCT ON` hotfix only collapsed those duplicates when no `_order_by`
;; was present (Postgres requires the DISTINCT ON expr to lead ORDER BY).
;; `EXISTS` correlated subqueries scope the root without multiplying rows,
;; so `_limit` + `_order_by` are exact and no DISTINCT is needed.

(defn- sql-quote
  "Double-quote a SQL identifier."
  [s]
  (str \" (str/replace (name s) "\"" "") \"))

(defn- sql-alias
  "A statement-unique SQL alias with prefix `p` (gensym-backed)."
  [p]
  (name (gensym (str "__" p))))

(defn- relation-left?
  "True when a relation's args mark it as a LEFT join — a left relation
   never scopes its parent, so it contributes no root predicate."
  [args]
  (or (contains? args :_maybe)
      (boolean (#{:left :LEFT "left" "LEFT"} (:_join args)))))

(defn- relation-has-filter?
  "True when a relation's args carry actual field predicates (not just
   query metadata like :_limit / :_order_by)."
  [args]
  (boolean (seq (apply dissoc args meta-arg-keys))))

(defn inner-exists
  "For each INNER (non-left) relation of `schema`, an `EXISTS (...)`
   correlated subquery scoped to `alias`: the parent row is kept only
   when a matching child exists.

   Per XSQL.md line 232: `-rel` is INNER = 'drop parent if no child
   match'. Existence-of-a-child IS the filter — so we emit an EXISTS
   for every INNER relation, even when neither the relation nor any
   deeper inner relation carries an additional predicate. Predicates
   on the relation or on deeper inner relations narrow the EXISTS
   further; the existence check itself is always present.

   LEFT relations never scope their parent and are skipped. Returns
   `[sql params]`, or nil when no inner relations are present.

   Used by `search-entity-roots` to filter roots without JOINing through
   relations — no row multiplication, so `_limit`/`_order_by` are exact."
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
                     ;; The junction row alone proves existence. Only join
                     ;; through to the target entity (`ex`) when something
                     ;; actually references it: a predicate ON the target
                     ;; (`fsql`) or a deeper inner-exists correlated to `ex`
                     ;; (`csql`). For a bare INNER relation (no filter, no
                     ;; nested inner) the join makes Postgres materialize the
                     ;; whole junction⨝target instead of an index-only semi
                     ;; join — see EXISTS root-scoping perf notes.
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
  "For given schema function will return FROM statement
  by joining tables in schema based on args available
  in schema. Returns `[tables from-sql join-on-data]` —
  `join-on-data` carries `?` parameter values for predicates
  embedded in LEFT JOIN ON clauses (positional, in source order),
  to be prepended to the WHERE-clause data when binding."
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
  ;; Pull-query runs a separate SELECT per relation. There's no parent
  ;; JOIN here, so `:_join "left"` on the relation's args is meaningless
  ;; at this layer; strip it so the relation's own FROM uses the default
  ;; INNER JOIN (link → target table). The relation's `:_where` stays
  ;; in `:args` and lands in WHERE — which is correct here, since pull
  ;; is filtering "give me only the joined rows that match."
  (let [schema (update schema :args dissoc :_join)
        [_ from maybe-data] (search-stack-from schema)
        _ (log/trace {:id ::pull-where-args :data {:table table}}
                     "Looking for WHERE args")
        [where d] (search-stack-args schema)
        ; _ (log/tracef "[%s] Looking for FOUND args" table)
        ; [found fd] (when-some [found-records (not-empty (keep #(when (some? %) %) found-records))]
        ;              (search-stack-args
        ;               (assoc schema :args
        ;                      ; {:_eid {:_in found-records}})))
        ;                      {:_eid {:_in parents}})))
        _ (log/trace {:id ::pull-parent-args :data {:table table}}
                     "Looking for PARENT args")
        ;; RLS guards are already applied via [where d] above against the
        ;; entity's own alias. Strip :rls here so enhance/args doesn't
        ;; re-apply them against the junction alias (`ras` = link_NNN),
        ;; which has no _eid column and explodes with "no such column".
        [parented pd] (if (= talias "_eid")
                        ;; If direct binding (in entity table)
                        (search-stack-args
                         (-> schema
                             (dissoc :rls)
                             (assoc :args {:_eid {:_in parents}}
                                    :entity/as ras)))
                        ;; Otherwise
                        (search-stack-args
                         (-> schema
                             (dissoc :rls)
                             (assoc :args {(keyword falias) {:_in parents}}
                                    :entity/as ras))))
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
                     (let [;; Aggregate Hoist: entries in :_count that target the same
                          ;; source relation share gensym aliases (selection->schema
                          ;; assigns one :relation/as / :entity/as pair per source
                          ;; relation, then reuses it across all entries pointing at
                          ;; it). Group by that pair so the FROM walk emits exactly
                          ;; one LEFT JOIN per group; the per-entry predicate moves
                          ;; entirely into the SELECT-side `case when`.
                           join-groups (group-by (fn [[_ s]]
                                                   [(:relation/as s) (:entity/as s)])
                                                 counted)
                          ;; One representative per group → :relations for the FROM.
                          ;; We force :_join :LEFT so search-stack-from emits LEFT
                          ;; (the count must preserve parents-with-zero); we do NOT
                          ;; carry per-entry predicate args here — those become
                          ;; case-when conditions in the SELECT, never JOIN ON.
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
                          ;;
                          ;; SELECT columns walk every entry (not the deduped set):
                          ;; one count column per entry, sharing the join with its
                          ;; group-mates via the common etable. query-selection->sql
                          ;; renders predicate args as flat per-field stmts now —
                          ;; no `_maybe` wrapping — so we AND-join them ourselves
                          ;; for the case-when condition.
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
                          ;;
                          ;; [where where-data]  (search-stack-args schema)
                          ;; TODO - ignore where for now, as it should be part of
                          ;; count-selections
                          ;; A nested count is scoped by its parent JOIN; a root
                          ;; count has no such JOIN, so the matched root _eids
                          ;; become an explicit predicate — otherwise the count
                          ;; aggregates the entire related table.
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
                    ;; Aggregate Hoist for `_agg`: entries that share a
                    ;; gensym pair (set in selection->schema once per
                    ;; source relation) collapse into ONE LEFT JOIN.
                    ;; Per-entry predicates surface only as SELECT-side
                    ;; case-when, never in JOIN ON. Mirrors what
                    ;; `pull-counts` does for `_count`.
                     (let [join-groups (group-by
                                        (fn [[_ rdata]]
                                          [(:relation/as rdata) (:entity/as rdata)])
                                        numerics)
                          ;; One representative per group → :relations for the FROM
                          ;; clause. Strip any per-entry predicate from the rep so
                          ;; the JOIN ON is bare; predicates stay on each entry's
                          ;; own rdata for case-when emission below.
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
                          ;; Root `_agg` is scoped to the matched root _eids; a
                          ;; nested `_agg` keeps its relation-args WHERE.
                           [where where-data] (if (and root? (seq parents))
                                                [(str as "._eid in ("
                                                      (str/join "," (repeat (count parents) "?"))
                                                      ")")
                                                 (vec parents)]
                                                (search-stack-args aggregate-schema))
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
                      expected-start-result (fetch-on-conn
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

                                ;;
                                 query (pull-query
                                        (update schema :args dissoc :_limit :_offset)
                                        (get found-records (keyword as)) parents)
                                 _ (log/trace {:id ::pull-query
                                               :data {:action :executed
                                                      :subject :sql
                                                      :phase :pull
                                                      :table table
                                                      :sql (first query)
                                                      :params (vec (rest query))}}
                                              "Sending pull query")
                                 relations (cond->
                                            (sql/execute! *fetch-con* query core/*return-type*)
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
                      (log/trace {:id ::no-parents-found
                                  :data {:entity-table etable :result result}}
                                 "Couldn't find parents for relation")
                      result))))
            ;;
              (process-node [result location]
                (if (= ::ROOT (key (clojure.zip/node location)))
                  (process-root result location)
                  (process-related result location)))]
      ;;
        (let [result (doall (process-node nil zipper))]
          result)))))

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

(defn prepare-references
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

(defn- chunk-rows-for
  "Rows per multi-row statement so the total bound-parameter count stays
   under the backend's hard cap (`proto/max-bind-params`). `cols` is the
   number of bound parameters one row contributes.

   Each statement is filled as close to the cap as a whole row allows.
   Multi-row INSERT / IN-list throughput climbs with batch size and then
   plateaus far below any realistic cap, so the largest safe chunk also
   issues the fewest round trips — fewest round trips is the fast path."
  [cols]
  (let [;; tiny headroom against driver miscounts; cost is ~0.1% of the cap
        budget (- (proto/max-bind-params *db*) 64)]
    (max 1 (quot budget (max 1 cols)))))

(defn- execute-multi-row!
  "Run a multi-row statement in parameter-bounded chunks. `rows` is a seq
   of equal-length parameter vectors; `cols` is the parameter count per
   row; `sql-for` takes a chunk's row count and returns the statement
   text carrying that many placeholder tuples. Every chunk runs on the
   same `tx`; returns the concatenated `:edn` result rows.

   Optional `trailing-params` is a vector of bind values appended once per
   chunk after the per-row params. Used to thread constant params (e.g.,
   RLS WHERE bind values on ON CONFLICT DO UPDATE) into the statement.

   This is what keeps bulk writes correct on every backend: an unchunked
   VALUES list overruns SQLite's 32766-param cap (and Postgres' 65535)
   on large batches or wide tables."
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
                     (sql/execute! tx params :edn))))
         (partition-all (chunk-rows-for cols) rows))))

(defn- delete-by-from-side!
  "Chunked counterpart to `execute-multi-row!` for the pre-DELETE that
   link-relations runs before re-inserting a from-side's links. The
   inline `IN (?, ?, …)` list carries one bound parameter per id, so a
   large `current` set overruns the same per-statement parameter cap
   the INSERT path already respects. Splits `current` into chunks sized
   by `chunk-rows-for` and runs one DELETE per chunk on the same `tx`."
  [tx table from current]
  (doseq [chunk (partition-all (chunk-rows-for 1) current)]
    (let [sql (str "delete from \"" table "\" where \"" from "\" in ("
                   (clojure.string/join ", " (repeat (count chunk) \?))
                   ")")]
      (log/trace {:id ::delete-from-side-chunk
                  :data {:table table :rows (count chunk) :sql sql}}
                 "Deleting from-side rows (chunk)")
      (sql/execute! tx (into [sql] chunk)))))

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
     ;; Link single relations — one multi-row INSERT per chunk (same shape as
     ;; store-entity-records; NOT JDBC addBatch / execute-batch!). from_xid/
     ;; to_xid are denormalized onto the relation row from the in-memory
     ;; analysis (no per-row subselect) so the relation-audit substrate's
     ;; cascade-DELETE triggers can read OLD.from_xid. In :euuid mode the
     ;; from_xid/to_xid columns are omitted — same as pre-1.3.0 installs.
     (let [{:keys [:relations/one]} result
           xid-mode? (= :xid (id/key))]
       (reduce-kv
        (fn [result {:keys [table]
                     to :to/field
                     from :from/field} bindings]
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
                           (mapv (fn [[f t]] [f t]))))]
            (log/trace {:id ::delete-old-one-relations
                        :data {:table table :from-side-count (count current)}}
                       "Deleting old one-relations")
            (delete-by-from-side! tx table from current)
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
                     from :from/field} bindings]
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
            (when-not stack?
              (log/debug {:id ::delete-many-relations-overwrite
                          :data {:table table :from-side-count (count current)}}
                         "Deleting many-relations (overwrite mode)")
              (delete-by-from-side! tx table from current))
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

;; ----------------------------------------------------------------------------
;; Relation-audit helpers (shared between Postgres and SQLite drainers)
;; ----------------------------------------------------------------------------

;; The app-path `publish-delta` that used to fan synthetic `:change`/
;; `:link` envelopes is gone. Live notifications flow exclusively through
;; the trigger substrate now: PG/SQLite triggers → `__entity_delta_queue`/
;; `__relation_delta_queue` → drainer → `delta/dispatch!`. See
;; `synthigy.dataset.postgres.audit` and `synthigy.dataset.sqlite.audit`.

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

          ;; === EARLY RETURN: Handle empty rows ===
        (if (empty? rows)
          analysis

          (let [;; === STEP 1: Generate IDs for rows that don't have them ===
                rows-with-id (map (fn [[row-data tmp-id]]
                                    (let [entity-id (or (id/extract row-data)
                                                        (id/generate))
                                          ;; The id arrives as a string (wire JSON
                                          ;; has no uuid type; bootstrap/seed
                                          ;; constants too). `id/coerce-arg` binds
                                          ;; a uuid in euuid mode (PG refuses the
                                          ;; implicit varchar→uuid cast); xid stays
                                          ;; a string (no-op).
                                          entity-id (id/coerce-arg entity-id)]
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

                  ;; Declared (ERD) user-field keys. Declared attributes are
                  ;; keyed in :fields by their attribute id; audit / augmented
                  ;; fields are keyword-keyed. Classifying resolve-vs-write by
                  ;; *declared* fields means audit stamps from enhance-write
                  ;; never make a pure pointer look like a dirty row.
                declared-keys (when entity-schema
                                (->> (:fields entity-schema)
                                     (keep (fn [[k v]]
                                             (when-not (keyword? k) (:key v))))
                                     set))

                id-field (id/field)

                  ;; Generate placeholders using protocol (PostgreSQL: ?::type, SQLite: ?)
                placeholder-fn (fn [k]
                                 (let [field-type (get field-types k)]
                                   (if (and field-type
                                            (not (core/reference-type? field-type)))
                                     (proto/placeholder-for-type *db* field-type)
                                     "?")))
                values-? (str \( (str/join ", " (map placeholder-fn ks')) \))

                  ;; === STEP 5: Classify group — resolve-only vs write ===
                  ;; Dedupe RETURNING columns
                return-cols (distinct (concat [:_eid (id/key)] constraint-keys))
                return-sql (str/join ", " (map columns-fn return-cols))

                  ;; Fields to update on conflict (exclude id + constraint
                  ;; columns). Empty ⇒ the group's rows carry only identity /
                  ;; constraint keys — pure pointers, nothing to write.
                fields-to-update (remove
                                  (fn [k]
                                    (let [kname (name k)]
                                      (or (= kname id-field)
                                          (some #(= (name %) kname) constraint-keys))))
                                  ks')

                  ;; Recursion (tree) FK columns — e.g. `mother`/`father`.
                  ;; They live ON the entity table but are not in `:fields`,
                  ;; so `declared-keys` misses them. A row carrying one is a
                  ;; real write (`{:mother nil}` clears the FK), not a pure
                  ;; pointer — count it as a user field.
                recursion-keys (when entity-schema (:recursions entity-schema))

                  ;; Declared user fields the row actually carries, outside
                  ;; its identifying constraint. Computed from declared-keys
                  ;; (+ recursion FK columns), so audit columns and other
                  ;; injected fields don't count.
                user-fields (filter (fn [k]
                                      (and (or (and declared-keys (declared-keys k))
                                               (and recursion-keys (recursion-keys k)))
                                           (not (some #(= % k) constraint-keys))))
                                    ks')

                  ;; A resolve-only group is pointer rows resolvable by a
                  ;; unique key — no declared field data to write. SELECT them
                  ;; instead of upserting: no write to shared parent tables,
                  ;; hence no cross-transaction lock contention (deadlock
                  ;; source). Pointers that don't resolve fall through to a
                  ;; create INSERT (find-or-create). No schema ⇒ can't
                  ;; classify ⇒ fall back to the write/upsert path.
                resolve-only? (and (some? declared-keys)
                                   (empty? user-fields)
                                   (not-empty constraint-keys))

                  ;; === STEP 6: Execute — SELECT for resolve, INSERT for write ===
                  ;; Every multi-row statement runs through execute-multi-row!
                  ;; so the bound-parameter count is chunked under the
                  ;; backend cap — large batches / wide tables can't overrun
                  ;; SQLite's 32766 or Postgres' 65535.
                result
                (if resolve-only?
                  (let [key-cols   (map columns-fn constraint-keys)
                        tuple-?    (str \( (str/join ", " (repeat (count constraint-keys) "?")) \))
                        ckey-vals  (mapv (apply juxt constraint-keys)
                                         (map first rows-with-id))
                        found      (execute-multi-row!
                                    tx ckey-vals (count constraint-keys)
                                    (fn [n]
                                      (str "SELECT " return-sql
                                           " FROM \"" entity-table "\" WHERE ("
                                           (str/join ", " key-cols) ") IN ("
                                           (str/join ", " (repeat n tuple-?))
                                           ")")))
                        found-set  (set (map (fn [r] (mapv #(get r %) constraint-keys))
                                             found))
                        missing    (remove
                                    (fn [[row]]
                                      (contains? found-set
                                                 (mapv #(get row %) constraint-keys)))
                                    rows-with-id)
                        created    (when (seq missing)
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
                  (let [on-values (map columns-fn constraint-keys)
                        do-set (if (empty? fields-to-update)
                                 (str (columns-fn (keyword id-field)) "="
                                      (proto/excluded-ref *db* (str \" id-field \")))
                                 (str/join ", "
                                           (map (fn [col]
                                                  (let [quoted (columns-fn col)]
                                                    (str quoted "=" (proto/excluded-ref *db* quoted))))
                                                fields-to-update)))
                        ;; RLS write-guard on ON CONFLICT DO UPDATE. When the
                        ;; upsert matches an existing row, this WHERE gates
                        ;; whether the principal may modify it. References the
                        ;; existing row via the table name (PG/SQLite/CRDB
                        ;; semantics). No principal / superuser / RLS disabled
                        ;; → no injection (fast path). RLS enabled but no
                        ;; :write guards applicable → 1=0 (fail-closed),
                        ;; mirroring enhance-args.
                        {:keys [enabled guards]} (:rls entity-schema)
                        rls-write
                        (when (and enabled
                                   (not-empty constraint-keys)
                                   (rls-runtime/should-apply-guards?))
                          (or (rls-runtime/compile-guards-to-sql
                               (str \" entity-table \")
                               guards
                               :write)
                              {:sql "1=0" :params []}))
                        rls-where (when rls-write
                                    (str " WHERE " (:sql rls-write)))
                        rls-params (:params rls-write)]
                    (log/trace {:id ::store-entity-group
                                :data {:entity-table entity-table
                                       :constraint-keys (vec constraint-keys)
                                       :rows (count row-data)
                                       :rls-where rls-where}}
                               "Storing entity group with order-independent mapping")
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
                     rls-params)))

                _ (log/trace {:id ::store-entity-result
                              :data {:entity-table entity-table :result result}}
                             "Stored entity group result")

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
                                                              (if (= k (id/key))
                                                                (id/coerce-arg v)
                                                                v)))
                                                     {}
                                                     cvals))
                                    ;; Try id lookup with type normalization
                                 ;; id->tmpid keys are coerced (STEP 1 above), so
                                 ;; normalize the lookup key through the same seam.
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

              ;; === STEP 8: Merge Results Back ===
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
   ;; CONSIDERING (2026-06-03): drop pull-roots in favor of returning a
   ;; thin `{tmp-id {_eid X :xid Y}}` mapping. Rationale: query-driven
   ;; frontends never consume the fat record — they refetch the
   ;; view-driving query or read deltas off the subscription substrate;
   ;; either flow delivers the canonical post-write state. The only
   ;; callers that *need* fields back from the write itself are
   ;; programmatic non-reactive ones (CLIs, scripts, ETL, server-side
   ;; chain-of-writes that can't use nested syntax), and they can do
   ;; an explicit get-entity / search-entity follow-up. Decision pending
   ;; SDK + lacinia-removal alignment; do not change yet.
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
         (prepare-references tx result)
         (enhance-write tx result)
         (store-entity-records tx result)
         (project-saved-entities result)
         (link-relations tx result stack?)
         (pull-roots result))))))
