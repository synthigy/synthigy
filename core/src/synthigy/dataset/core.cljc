(ns synthigy.dataset.core
  "Core dataset protocols and records for entity-relationship modeling.

  This namespace contains:
  - Protocol declarations (shared contracts for frontend/backend)
  - Record definitions (ERDEntity, ERDRelation, ERDModel)
  - Type conversion validation system (shared logic)
  - Core utility functions

  Implementations are extended in separate namespaces:
  - synthigy.dataset.projection: Projection protocol implementations
  - synthigy.dataset.operations: Model operations (join, merge, etc.)"
  (:require
   #?(:cljs [goog.string :as gstring])
   #?(:cljs [goog.string.format])
   clojure.data
   clojure.set
   [clojure.string :as str]
   [synthigy.dataset.id :as id]))

(defn deep-merge
  "Recursively merges maps."
  [& maps]
  (letfn [(m [& xs]
            (if (some #(and (map? %) (not (record? %))) xs)
              (apply merge-with m xs)
              (last xs)))]
    (reduce m maps)))

(defonce ^:dynamic *return-type* :raw)
;; The delta pub/sub vars + lifecycle moved to `synthigy.dataset.delta`.

;;; Type Conversion Validation System (Shared Frontend/Backend)

;; Forward declaration for reference-types (defined below *reference-mapping*)
(declare reference-types)

(defn type-families
  "Returns type family groupings. Reference types derived from *reference-mapping*.
   Used to determine safe type conversions."
  []
  {:text #{"string" "avatar" "transit" "hashed"}
   :json #{"json" "encrypted"}
   :numeric #{"int" "float"}
   :reference (reference-types)  ;; DYNAMIC - derived from *reference-mapping*
   :temporal #{"timestamp"}
   :boolean #{"boolean"}
   :enum #{"enum"}})

(def legacy-type-mapping
  "Maps legacy attribute types to their modern equivalents.
   Applied during deploy to keep old models valid."
  {"avatar" "json"})

(defn normalize-legacy-types
  "Replaces legacy attribute types in a model with their modern equivalents."
  [model]
  (update model :entities
          (fn [entities]
            (reduce-kv
             (fn [acc eid entity]
               (assoc acc eid
                      (update entity :attributes
                              (fn [attrs]
                                (mapv (fn [attr]
                                        (if-let [new-type (legacy-type-mapping (:type attr))]
                                          (assoc attr :type new-type)
                                          attr))
                                      attrs)))))
             {}
             entities))))

(defonce ^{:dynamic true
           :doc "Maps attribute type strings to reference metadata.

  Structure: {type-name {:entity-uuid UUID :table-fn (fn [] table-name)}}

  When an attribute has a type that exists in this mapping, it's treated
  as a reference to that entity (scalar UUID pointer) rather than a nested
  relation or scalar type.

  Reference fields:
  - Store only a UUID value (not nested data)
  - Point to another entity's record
  - Are tracked in the :reference section during mutation analysis
  - Require the referenced entity to exist for validation

  This mapping is extensible - add entries for any entity that should be
  referenceable via scalar UUID fields.

  Backward compatible: Also supports old format {type-name UUID}."}
  *reference-mapping*
  {})

;;; Reference Type API

(defn reference-types
  "Returns set of registered reference type names."
  []
  (set (keys *reference-mapping*)))

(defn reference-type?
  "Returns true if type is a registered reference type."
  [type-name]
  (contains? *reference-mapping* type-name))

(defn reference-entity-uuid
  "Returns entity UUID for a reference type, or nil."
  [type-name]
  (let [v (get *reference-mapping* type-name)]
    (if (uuid? v) v (:entity-uuid v))))

(defn reference-table-fn
  "Returns table-fn for a reference type, or nil."
  [type-name]
  (when-let [v (get *reference-mapping* type-name)]
    (when (map? v) (:table-fn v))))

(defn register-reference-type!
  "Registers a reference type with metadata.

  type-name   - String type name (e.g. \"user\")
  entity-uuid - UUID of the entity
  opts        - {:table-fn (fn [] table-name)}"
  [type-name entity-uuid opts]
  #?(:clj (alter-var-root
           #'*reference-mapping*
           (fn [m] (assoc m type-name (merge {:entity-uuid entity-uuid} opts))))))

;;; End Reference Type API

(defn get-type-family
  "Returns the family keyword for a given type, or nil if not in a family"
  [type]
  (some (fn [[family types]]
          (when (contains? types type)
            family))
        (type-families)))

(defn same-family?
  "Check if two types are in the same family (safe conversion)"
  [from-type to-type]
  (when-let [from-family (get-type-family from-type)]
    (= from-family (get-type-family to-type))))

(defn validate-type-conversion
  "Validates a type conversion and returns:
   - {:safe true} if conversion is always safe
   - {:warning \"message\"} if conversion might lose data
   - {:error \"message\" :type ::error-type :suggestion \"hint\"} if conversion is forbidden

   This function is shared between frontend and backend to ensure consistent validation."
  [from-type to-type]
  (cond
    ;; Same type - no conversion needed
    (= from-type to-type)
    {:safe true}

    ;; SPECIFIC WARNINGS - These must come BEFORE general family checks

    ;; float → int (precision loss warning)
    (and (= from-type "float") (= to-type "int"))
    {:warning "Converting float to int will truncate decimal values. Precision loss may occur."}

    ;; Within reference family (risky - EIDs might not exist)
    (and (reference-type? from-type)
         (reference-type? to-type))
    {:warning (str "Converting " from-type " to " to-type " assumes all entity IDs exist in the target table. Invalid references will violate foreign key constraints.")}

    ;; timestamp → string (losing temporal semantics)
    (and (= from-type "timestamp") (= to-type "string"))
    {:warning "Converting timestamp to string will lose temporal semantics and indexing capabilities. Consider carefully if this is necessary."}

    ;; encrypted → non-json (data is encrypted)
    (and (= from-type "encrypted")
         (not (contains? (:json (type-families)) to-type))
         (not= to-type "string"))
    {:error "Cannot convert encrypted data to non-JSON/string type: Data is encrypted and cannot be directly converted."
     :type ::forbidden-conversion
     :suggestion "Decrypt data first or keep as encrypted/json type."}

    ;; GENERAL SAFE CONVERSIONS

    ;; Any type can be converted to string (after specific checks above)
    (= to-type "string")
    {:safe true}

    ;; Within same family - safe (same DB type) - after specific warnings
    (same-family? from-type to-type)
    {:safe true}

    ;; int → float (widening)
    (and (= from-type "int") (= to-type "float"))
    {:safe true}

    ;; enum → string (enum values are strings)
    (and (= from-type "enum") (= to-type "string"))
    {:safe true}

    ;; RISKY CONVERSIONS (data-dependent)

    ;; string → numeric (risky - depends on data)
    (and (= from-type "string") (contains? #{"int" "float"} to-type))
    {:warning (str "Converting string to " to-type " requires all values to be valid numbers. Invalid values will cause the conversion to fail.")}

    ;; string → boolean (risky - depends on data)
    (and (= from-type "string") (= to-type "boolean"))
    {:warning "Converting string to boolean requires all values to be 't', 'f', 'true', 'false', 'yes', 'no', '1', '0'. Invalid values will cause the conversion to fail."}

    ;; string → timestamp (risky - depends on data)
    (and (= from-type "string") (= to-type "timestamp"))
    {:warning "Converting string to timestamp requires all values to be valid timestamp formats. Invalid values will cause the conversion to fail."}

    ;; string → json (lossy - invalid JSON becomes NULL)
    (and (= from-type "string") (= to-type "json"))
    {:warning "Converting string to json will set non-JSON values to NULL. This may result in data loss."}

    ;; string → enum (risky - values must be in enum set)
    (and (= from-type "string") (= to-type "enum"))
    {:warning "Converting string to enum requires all values to be valid enum values. Invalid values will cause the conversion to fail."}

    ;; LEGACY: avatar ↔ json/string (avatar type removed, data preserved as-is)
    (and (= from-type "avatar") (contains? #{"json" "string"} to-type))
    {:safe true}

    (and (contains? #{"json" "string"} from-type) (= to-type "avatar"))
    {:safe true}

    ;; LEGACY: transit → json (transit deprecated; data is syntactically valid
    ;; JSON, so jsonb cast in DDL preserves it lossless). transit → string is
    ;; already covered by the general "to string is safe" rule above.
    (and (= from-type "transit") (= to-type "json"))
    {:safe true}

    ;; FORBIDDEN: json → numeric
    (and (contains? (:json (type-families)) from-type)
         (contains? #{"int" "float"} to-type))
    {:error (str "Cannot convert " from-type " to " to-type ": No meaningful automatic conversion exists.")
     :type ::forbidden-conversion
     :suggestion "Extract numeric fields from JSON manually before converting."}

    ;; FORBIDDEN: json → boolean
    (and (contains? (:json (type-families)) from-type)
         (= to-type "boolean"))
    {:error (str "Cannot convert " from-type " to boolean: No meaningful automatic conversion exists.")
     :type ::forbidden-conversion
     :suggestion "Extract boolean fields from JSON manually before converting."}

    ;; FORBIDDEN: timestamp → int (semantic mismatch)
    (and (= from-type "timestamp") (= to-type "int"))
    {:error "Cannot convert timestamp to int: Use explicit epoch conversion if needed."
     :type ::forbidden-conversion
     :suggestion "Create a new attribute and populate it with epoch timestamps explicitly."}

    ;; FORBIDDEN: boolean → numeric
    (and (= from-type "boolean") (contains? #{"int" "float"} to-type))
    {:error (str "Cannot convert boolean to " to-type ": Semantic mismatch.")
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need '0'/'1' representation, or create explicit mapping logic."}

    ;; FORBIDDEN: reference → non-reference (losing referential integrity)
    (and (reference-type? from-type)
         (not (reference-type? to-type))
         (not= to-type "string"))
    {:error (str "Cannot convert " from-type " to " to-type ": This would lose referential integrity.")
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need to preserve entity IDs."}

    ;; Default: Unknown/unsupported conversion
    :else
    {:error (str "Unsupported type conversion from " from-type " to " to-type ".")
     :type ::unsupported-conversion
     :suggestion "This conversion path has not been validated. Please review the type compatibility matrix."}))

(defn can-convert-type?
  "Returns true if the type conversion is allowed (safe or warning), false if forbidden.
   Use this for quick yes/no checks. For detailed info, use validate-type-conversion."
  [from-type to-type]
  (let [result (validate-type-conversion from-type to-type)]
    (not (:error result))))

(defn get-conversion-level
  "Returns the risk level of a type conversion: :safe, :warning, or :error"
  [from-type to-type]
  (let [result (validate-type-conversion from-type to-type)]
    (cond
      (:error result) :error
      (:warning result) :warning
      :else :safe)))

(defn all-types
  "All available attribute types including registered references."
  []
  (vec (concat
        ["string" "avatar" "transit" "hashed" ;; text family
         "json" "encrypted" ;; json family
         "int" "float" ;; numeric family
         "timestamp" ;; temporal
         "boolean" ;; boolean
         "enum"] ;; enum
        (reference-types))))

(defn get-allowed-conversions
  "Returns a map of all possible target types grouped by safety level.

   Returns:
   {:safe [types that are safe to convert to]
    :warning [types that might work but are risky]
    :forbidden [types that are blocked]}

   Useful for populating UI dropdowns with visual indicators.

   Example:
   (get-allowed-conversions \"avatar\")
   => {:safe [\"string\" \"transit\" \"hashed\" \"avatar\"]
       :warning []
       :forbidden [\"json\" \"int\" \"float\" ...]}"
  [from-type]
  (reduce
   (fn [acc to-type]
     (let [level (get-conversion-level from-type to-type)]
       (update acc level (fnil conj []) to-type)))
   {:safe []
    :warning []
    :forbidden []}
   (all-types)))

(defn get-convertible-types
  "Returns only the types that CAN be converted to (safe or warning, but not forbidden).
   This is useful for filtering dropdown options to show only valid choices.

   Options:
   - :include-warnings? true (default) - includes both safe and risky conversions
   - :include-warnings? false - only safe conversions

   Example:
   (get-convertible-types \"avatar\")
   => [\"string\" \"transit\" \"hashed\" \"avatar\"]

   (get-convertible-types \"avatar\" :include-warnings? false)
   => [\"string\" \"transit\" \"hashed\" \"avatar\"]"
  ([from-type]
   (get-convertible-types from-type {:include-warnings? true}))
  ([from-type {:keys [include-warnings?]
               :or {include-warnings? true}}]
   (let [allowed (get-allowed-conversions from-type)]
     (if include-warnings?
       (concat (:safe allowed) (:warning allowed))
       (:safe allowed)))))

(defn get-type-conversion-info
  "Returns detailed information about a type conversion for UI display.

   Returns:
   {:level :safe|:warning|:error
    :allowed? true|false
    :badge-color \"green\"|\"yellow\"|\"red\"
    :icon \"✓\"|\"⚠\"|\"✗\"
    :message \"Human readable message\"
    :warning \"Warning message\" (if level is :warning)
    :error \"Error message\" (if level is :error)
    :suggestion \"Suggestion for forbidden conversions\" (if level is :error)}

   Example:
   (get-type-conversion-info \"string\" \"int\")
   => {:level :warning
       :allowed? true
       :badge-color \"yellow\"
       :icon \"⚠\"
       :warning \"Converting string to int requires all values...\"
       :message \"Converting string to int requires all values...\"}

   (get-type-conversion-info \"avatar\" \"json\")
   => {:level :error
       :allowed? false
       :badge-color \"red\"
       :icon \"✗\"
       :error \"Cannot convert avatar to json...\"
       :message \"Cannot convert avatar to json...\"
       :suggestion \"Convert to string first...\"}"
  [from-type to-type]
  (let [validation (validate-type-conversion from-type to-type)
        level (get-conversion-level from-type to-type)]
    (case level
      :safe
      {:level :safe
       :allowed? true
       :badge-color "green"
       :icon "✓"
       :message (str "Safe conversion from " from-type " to " to-type)}

      :warning
      {:level :warning
       :allowed? true
       :badge-color "yellow"
       :icon "⚠"
       :warning (:warning validation)
       :message (:warning validation)}

      :error
      {:level :error
       :allowed? false
       :badge-color "red"
       :icon "✗"
       :error (:error validation)
       :message (:error validation)
       :suggestion (:suggestion validation)})))

;;; End Type Conversion Validation System

;;; Core Protocols

(defprotocol EntityConstraintProtocol
  (set-entity-unique-constraints [this constraints])
  (update-entity-unique-constraints [this function])
  (get-entity-unique-constraints [this]))

(defprotocol AuditConfigurationProtocol
  (set-who-field [this name])
  (get-who-field [this])
  (set-when-field [this name])
  (get-when-field [this]))

(defprotocol ERDEntityAttributeProtocol
  (add-attribute [this attribute])
  (set-attribute [this attribute])
  (get-attribute [this id])
  (update-attribute [this id f])
  (remove-attribute [this attribute]))

(defprotocol ERDModelActions
  (get-entity [this] [this id] "Returns node in model with name if provided, otherwise it returns last entity")
  (get-entities [this] "Returns vector of entities")
  (add-entity [this entity] "Adds new entity to model")
  (set-entity [this entity] "Sets entity in model ignoring previous state")
  (update-entity [this id function] "Sets entity in model ignoring previous state")
  (remove-entity [this entity] "Removes node from model")
  (replace-entity
    [this entity replacement]
    "Repaces entity in model with replacement and reconects all previous connections")
  (get-entity-relations
    [this entity]
    "Returns all relations for given entity where relations
    are returned in such maner that input entity is always in :from field")
  (get-relation [this id] "Returns relation between entities")
  (get-relations [this] "Returns vector of relations")
  (get-relations-between [this entity1 entity2] "Returns all found relations that exist between entity1 entity2")
  (add-relation [this relation])
  (create-relation [this from to] [this from to type] [this from to type path] [id this from to type path] "Creates relation from entity to entity")
  (set-relation [this relation] "Sets relation in model ignoring previous values")
  (update-relation [this id function] "Updates relation in model by merging new values upon old ones")
  (remove-relation [this relation] "Removes relation between entities"))

(defprotocol ERDModelReconciliationProtocol
  (reconcile
    [this model]
    "Function reconciles this with that. Starting point should be reconcilation
    of some 'this' with ERDModel, and that might lead to reconiliation of relations
    and entities with 'this'. Therefore reconcile this with that"))

(defprotocol DatasetProtocol
  (deploy!
    [this version]
    "Deploys dataset version")
  (recall!
    [this version]
    "Deletes a specific dataset version by {:id version-id}. Only works on deployed versions.
     If it's the only deployed version, cleans up and returns.
     If it's the most recent (but not only), rolls back to previous version.
     Otherwise just deletes it.")
  (destroy!
    [this dataset]
    "Nuclear delete: removes ALL dataset versions and all dataset data. Affects DB as well. All is gone")
  (get-model
    [this]
    "Returns all entities and relations for given account")
  (mount
    [this module]
    "Mounts module in EYWA by storing its dataset and special handlers")
  (reload
    [this]
    [this module]
    "Reloads module. If module is not specified, than whole dataset is reloaded")
  (unmount
    [this module]
    "Removes module from EYWA by removing all data for that module")
  (get-last-deployed
    [this] [this offset]
    "⚠️ MANUAL RECOVERY ONLY - Reads model from __deploy_history audit table.
     NOT used in normal bootstrap (use reload instead).
     Use this to recover from corrupted dataset tables.")
  (backup
    [this options]
    "Backups dataset for given target based on provided options"))

(defprotocol ERDModelProjectionProtocol
  (added? [this] "Returns true if this is added or false otherwise")
  (removed? [this] "Returns true if this is removed or false otherwise")
  (diff? [this] "Returns true if this has diff or false otherwise")
  (diff [this] "Returns diff content")
  (mark-added [this] "Marks this ass added")
  (mark-removed [this] "Marks this as removed")
  (mark-diff [this diff] "Adds diff content")
  (suppress [this] "Returns this before projection")
  (project
    [this that]
    "Returns projection of this on that updating each value in nested structure with keys:
    * added?
    * removed?
    * diff
    * active")
  (clean-projection-meta [this] "Returns "))

;;; Core Records

(defrecord ERDRelation [euuid xid from to from-label to-label cardinality path configuration active claimed-by])
(defrecord NewERDRelation [euuid xid entity type])
(defrecord ERDEntityAttribute [euuid xid seq name constraint type configuration active])

;;; Attribute Name Validation

(defn find-attribute-by-normalized-name
  "Finds an attribute whose name matches (case-insensitive). Returns attribute or nil."
  [attributes name]
  (let [normalized (str/lower-case name)]
    (some #(when (= normalized (str/lower-case (:name %))) %) attributes)))

(defn check-attribute-name-conflict!
  "Throws if attribute name conflicts with existing attribute (different ID, same name ignoring case)."
  [attributes new-attribute]
  (when-let [found (find-attribute-by-normalized-name attributes (:name new-attribute))]
    (let [found-id (id/extract found)
          new-id (id/extract new-attribute)]
      (when (and (some? found-id)
                 (some? new-id)
                 (not= found-id new-id))
        (throw
         (ex-info
          (#?(:clj format :cljs gstring/format)
           "Attribute name conflict: '%s' already exists with different ID"
           (:name new-attribute))
          {:type ::attribute-name-conflict
           :new-attribute new-attribute
           :existing-attribute found
           :normalized-name (str/lower-case (:name new-attribute))}))))))

(defn cloned? [{:keys [clone]}] clone)
(defn original [{:keys [original]}] original)

(defrecord ERDEntity [euuid xid position width height name attributes type configuration clone original active claimed-by]
  EntityConstraintProtocol
  (set-entity-unique-constraints [this constraints]
    (assoc-in this [:configuration :constraints :unique] constraints))
  (update-entity-unique-constraints [this f]
    (update-in this [:configuration :constraints :unique] f))
  (get-entity-unique-constraints [this]
    ;; All-or-nothing: a composite unique key is dropped entirely if ANY of its
    ;; attributes is inactive/removed — never silently narrowed (a,b)->(a),
    ;; which would impose a different, stricter constraint the modeler didn't
    ;; declare. Mirrors RLS guard pruning: removing what a rule depends on drops
    ;; the rule. Returns the surviving groups, compacted (for schema use).
    (let [active-attributes (set (map id/extract (filter :active (:attributes this))))]
      (reduce
       (fn [r constraint-group]
         (if (and (seq constraint-group) (every? active-attributes constraint-group))
           (conj r (vec constraint-group))
           r))
       []
       (get-in this [:configuration :constraints :unique]))))

  ERDEntityAttributeProtocol
  (add-attribute [{:keys [attributes]
                   :as this} {:as attribute}]
    {:pre [(instance? ERDEntityAttribute attribute)]}
    (let [attribute (map->ERDEntityAttribute attribute)
          ;; Model node: euuid-first dual id when missing (not native generate).
          attribute (if (id/extract attribute) attribute (merge attribute (id/new-model-node-id)))
          attribute-id (id/extract attribute)
          ;; Validate no duplicate name
          _ (check-attribute-name-conflict! attributes
                                            (assoc attribute (id/key) attribute-id))
          entity (update this :attributes (fnil conj [])
                         (assoc attribute
                                (id/key) attribute-id
                                :seq (count attributes)))]
      (if (= "unique" (:constraint attribute))
        (update-entity-unique-constraints
         entity
         (fnil
          (fn [current]
            (update current 0 (comp distinct conj) attribute-id))
          [[]]))
        entity)))
  (get-attribute [{:keys [attributes]} id]
    (if-let [attribute (some #(when (= id (id/extract %)) %) attributes)]
      attribute
      (throw
       (ex-info
        (str "Couldn't find attribute with id" id)
        {:id id
         :attributes attributes
         :ids (map id/extract attributes)}))))
  (set-attribute [{:keys [attributes]
                   :as this}
                  {ct :constraint
                   :as attribute}]
    (let [id (id/extract attribute)
          p (.indexOf (mapv id/extract attributes) id)]
      (if (neg? p)
        (throw
         (ex-info
          "Attribute not found"
          {:attribute attribute
           :attributes attributes}))
        (let [;; Validate name conflict (excluding current attribute)
              other-attributes (vec (concat (subvec attributes 0 p)
                                            (subvec attributes (inc p))))
              _ (check-attribute-name-conflict! other-attributes attribute)
              {pt :constraint} (get attributes p)
              entity (assoc-in this [:attributes p] attribute)]
          (cond
            ;; If once was unique and currently isn't
            (and (= "unique" pt) (not= "unique" ct))
            (update-entity-unique-constraints
             entity
             (fn [constraints]
               (mapv #(vec (remove #{id} %)) constraints)))
            ;; If now is unique and previously wasn't
            (and (= "unique" ct) (not= "unique" pt))
            (update-entity-unique-constraints
             entity
             (fnil #(update % 0 conj id) [[]]))
            ;; Otherwise return changed entity
            :else entity)))))
  (update-attribute [{:keys [attributes]
                      :as this} id f]
    (if-let [{pt :constraint
              :as attribute} (some #(when (= id (id/extract %)) %) attributes)]
      (let [{ct :constraint
             :as attribute'} (f attribute)
            entity (set-attribute this attribute')]
        (cond
          ;; If once was unique and currently isn't
          (and (= "unique" pt) (not= "unique" ct))
          (update-entity-unique-constraints
           entity
           (fn [constraints]
             (mapv #(vec (remove #{id} %)) constraints)))
          ;; If now is unique and previously wasn't
          (and (= "unique" ct) (not= "unique" pt))
          (update-entity-unique-constraints
           entity
           (fnil #(update % 0 conj id) [[]]))
          ;; Otherwise return changed entity
          :else entity))
      (throw (ex-info (str "Couldn't find attribute with id" id)
                      {:id id
                       :ids (map id/extract attributes)}))))
  (remove-attribute [{:keys [attributes]
                      :as this} attribute]
    (let [id (id/extract attribute)]
      (->
       this
       (assoc :attributes
              (vec
               (keep-indexed
                (fn [idx a] (assoc a :seq idx))
                (remove #(= id (id/extract %)) attributes))))
       (update update-entity-unique-constraints
               (fn [unique-bindings]
                 (reduce
                  (fn [r group]
                    (let [group' (vec
                                  (remove
                                   (some-fn
                                    #{id}
                                    string?)
                                   group))]
                      (if (empty? group') r (conj r group'))))
                  []
                  unique-bindings)))))))

(defrecord ERDModel [id-key entities relations configuration clones version]
  AuditConfigurationProtocol
  (set-who-field
    [this name]
    (assoc-in this [:configuration :audit :who] name))
  (get-who-field [this]
    (get-in this [:configuration :audit :who]))
  (set-when-field
    [this name]
    (assoc-in this [:configuration :audit :when] name))
  (get-when-field [this]
    (get-in this [:configuration :audit :when])))

(extend-protocol ERDModelActions
  nil
  (get-entities [_] nil)
  (get-entity [_ _] nil)
  (get-relations [_] nil)
  (get-relation [_ _] nil)
  (get-entity-relations [_ _] nil)
  (add-entity [_ _] nil)
  (remove-entity [_ _] nil)
  (replace-entity [_ _ _] nil))

;;; Relation Helpers

(defn invert-relation [relation]
  (with-meta
    (-> relation
        (clojure.set/rename-keys
         {:from :to
          :from-label :to-label
          :to :from
          :to-label :from-label})
        (assoc :cardinality
               (case (:cardinality relation)
                 "o2m" "m2o"
                 "o2o" "o2o"
                 "m2m" "m2m"
                 "m2o" "o2m"
                 relation))
        map->ERDRelation)
    (merge
     (meta relation)
     {:dataset.relation/inverted? true})))

(defn inverted-relation? [relation] (:dataset.relation/inverted? (meta relation)))

(defn normalize-relation
  [relation]
  (if (inverted-relation? relation)
    (with-meta
      (invert-relation relation)
      (dissoc (meta relation) :dataset.relation/inverted?))
    relation))

(defn direct-relation-from
  [entity {:keys [from to to-label]
           :as relation}]
  (if (= from to)
    (if (not-empty to-label)
      relation
      (invert-relation relation))
    (if (= (id/extract entity) (id/extract from)) relation
        (invert-relation relation))))

(defn direct-relations-from
  [entity relations]
  (map #(direct-relation-from entity %) relations))

(defn focus-entity-relations
  "Function returns entity rel focused on entity, inverting
  all relations that are not outgoing from input entity"
  ([model entity]
   (direct-relations-from entity (get-entity-relations model entity)))
  ([model entity entity']
   (direct-relations-from entity (get-relations-between model entity entity'))))

(defn align-relations
  "Function aligns two relations. By comparing source and
  target node. If needed second relation will be inverted"
  [relation1 relation2]
  (if (= (id/extract relation1) (id/extract relation2))
    (if (= (get-in relation1 [:from (id/key)])
           (get-in relation2 [:from (id/key)]))
      [relation1 relation2]
      (if (= (get-in relation1 [:from (id/key)])
             (get-in relation2 [:to (id/key)]))
        [relation1 (invert-relation relation2)]
        (throw
         (ex-info
          "Cannot align relations that connect different entities"
          {:relations [relation1 relation2]}))))
    (throw
     (ex-info
      "Cannot align different relations"
      {:relations [relation1 relation2]}))))

(defn same-relations?
  "Function returns true if two relations are the same, by comparing
  relation1 to relation2 and inverted version of relation2"
  [relation1 relation2]
  (if (= (id/extract relation1) (id/extract relation2))
    (let [[relation1' relation2' relation2'']
          (map
           #(->
             %
             (select-keys [:to-label :from-label :cardinality :to :from])
             (update :to (id/key))
             (update :from (id/key)))
           [relation1 relation2 (invert-relation relation2)])
          same? (boolean
                 (or
                  (= relation1' relation2')
                  (= relation1' relation2'')))]
      same?)
    false))

;;; Model Operations

(defn- merge-entity-attributes
  "Merges attributes from two entities, accumulating all historical attributes.
   Attributes in entity2 are marked :active true, attributes only in entity1 are marked :active false.
   This implements 'last deployed wins' at the entity level for attribute active flags."
  [entity1 entity2]
  (let [attrs1 (or (:attributes entity1) [])
        attrs2 (or (:attributes entity2) [])
        ;; Build maps by attribute UUID for fast lookup
        attrs1-by-id (into {} (map (juxt id/extract identity) attrs1))
        attrs2-by-id (into {} (map (juxt id/extract identity) attrs2))
        ;; Get all unique attribute UUIDs
        all-attr-uuids (clojure.set/union (set (keys attrs1-by-id))
                                          (set (keys attrs2-by-id)))
        ;; Merge attributes: model2 wins for properties, but accumulate all
        merged-attrs (vec
                      (for [attr-uuid all-attr-uuids]
                        (if-let [attr2 (get attrs2-by-id attr-uuid)]
                          ;; Attribute in model2: use it with :active true
                          (assoc attr2 :active true)
                          ;; Attribute only in model1: keep it with :active false
                          (assoc (get attrs1-by-id attr-uuid) :active false))))]
    ;; Return entity2 as base with merged attributes
    (assoc entity2 :attributes merged-attrs)))

(defn- merge-entity-rls
  "Merge RLS config from two entities (entity1 = accumulated model so far,
   entity2 = the model being folded in — newer in the deploy-order fold).

   - Guards UNION by :id (entity2 wins on id collisions). The union is what
     lets guards from different datasets/versions layer onto a shared entity,
     e.g. Resource Planning adding a cross-dataset guard to Project Task.
   - `:enabled` is LAST-DEPLOYMENT-WINS, mirroring the `:active` rule used for
     attributes and entities: the newer entity decides on/off WHEN it carries
     an `:rls` block; a model silent on an entity's `:rls` leaves the prior
     `:enabled` unchanged. (Previously `(or e1 e2)`, which made `:enabled`
     sticky-on across the whole deploy history — an entity could never be
     toggled back off in-place because some earlier deployed version still
     carried enabled=true.)"
  [entity1 entity2]
  (let [rls1 (get-in entity1 [:configuration :rls])
        rls2 (get-in entity2 [:configuration :rls])]
    (if (or rls1 rls2)
      (let [guards1 (or (:guards rls1) [])
            guards2 (or (:guards rls2) [])
            guards1-by-id (into {} (keep (fn [g] (when (:id g) [(:id g) g]))) guards1)
            guards2-by-id (into {} (keep (fn [g] (when (:id g) [(:id g) g]))) guards2)
            all-ids (clojure.set/union (set (keys guards1-by-id))
                                       (set (keys guards2-by-id)))
            merged-guards (vec (for [gid all-ids]
                                 (or (get guards2-by-id gid)
                                     (get guards1-by-id gid))))
            enabled (if (some? rls2)
                      (boolean (:enabled rls2))
                      (boolean (:enabled rls1)))]
        (assoc-in entity2 [:configuration :rls]
                  {:enabled enabled :guards merged-guards}))
      entity2)))

(defn join-models [model1 model2]
  (->
   model1
   ;; Handled by ensure active attributes
   (update :configuration deep-merge (:configuration model2))
   (update :clones deep-merge (:clones model2))
   ;; Ensure active attributes
   (as-> joined-model
         ;; Merge entities: handle both claimed-by AND attributes
         (reduce
          (fn [m entity]
            (let [id (id/extract entity)
                  entity1 (get-entity model1 id)
                  entity2 (get-entity model2 id)
                  claims-1 (get entity1 :claimed-by #{})
                  claims-2 (get entity2 :claimed-by #{})
                  claims (clojure.set/union claims-1 claims-2)
                  ;; Entity is active if present in model2 (last deployment wins)
                  entity-active? (some? entity2)
                  ;; Merge attributes, configuration, and RLS when both
                  ;; entities exist. :configuration uses the same
                  ;; deep-merge pattern as the model-level merge above
                  ;; (entity1 as base, entity2 on top) so keys entity2
                  ;; omits stay alive — audit actions, audit/persist,
                  ;; constraints, etc. RLS merge runs last so its
                  ;; guard-union semantics still layer on top.
                  merged-entity (if (and entity1 entity2)
                                  (as-> (merge-entity-attributes entity1 entity2) e
                                    (assoc e :configuration
                                           (deep-merge (:configuration entity1)
                                                       (:configuration entity2)))
                                    (merge-entity-rls entity1 e))
                                  entity)]
              (set-entity m (assoc merged-entity
                                   :claimed-by claims
                                   :active entity-active?))))
          joined-model
          (mapcat get-entities [model1 model2]))
     ;; Merge relations: handle claimed-by AND active
     (reduce
      (fn [m relation]
        (let [id (id/extract relation)
              relation1 (get-relation model1 id)
              relation2 (get-relation model2 id)
              claims-1 (get relation1 :claimed-by #{})
              claims-2 (get relation2 :claimed-by #{})
              claims (clojure.set/union claims-1 claims-2)
              ;; Relation is active if present in model2 (last deployment wins)
              relation-active? (some? relation2)]
          (set-relation m (assoc relation
                                 :claimed-by claims
                                 :active relation-active?))))
      joined-model
      (mapcat get-relations [model1 model2])))))

(defn activate-model
  ([model] (activate-model model (constantly true)))
  ([model is-deployed-fn]
   (as-> model m
     (reduce
      (fn [m entity]
        (set-entity m (assoc entity :active (is-deployed-fn (set (:claimed-by entity))))))
      m
      (get-entities m))
     (reduce
      (fn [m relation]
        (set-relation m (assoc relation :active (is-deployed-fn (set (:claimed-by relation))))))
      m
      (get-relations m)))))

(defn disjoin-model [model1 model2]
  (reduce
   (fn [final entity]
     (remove-entity final entity))
   model1
   (get-entities model2)))

(defn add-claims
  "Adds version-id as a claim to all entities and relations in the provided model"
  ([model version-id]
   (letfn [(add-claim [model object-id]
             (cond
               ;; Check if it's an entity
               (get-in model [:entities object-id])
               (update-in model [:entities object-id :claimed-by]
                          (fnil conj #{}) version-id)
               ;; Check if it's a relation
               (get-in model [:relations object-id])
               (update-in model [:relations object-id :claimed-by]
                          (fnil conj #{}) version-id)
               ;; Not found
               :else model))]
     (as-> model gm
       (reduce
        (fn [gm entity]
          (add-claim gm (id/extract entity)))
        gm
        (get-entities model))
       (reduce
        (fn [gm relation]
          (add-claim gm (id/extract relation)))
        gm
        (get-relations model))))))

(defn find-exclusive-entities
  "Returns entities that are ONLY claimed by the provided version-uuids"
  [model version-uuids]
  (let [version-set (set version-uuids)]
    (filter
     (fn [entity]
       (let [claims (get entity :claimed-by #{})]
          ;; Skip entities without claims (legacy system entities)
          ;; Exclusive if all claims are within version-uuids
         (and (not-empty claims)
              (empty? (clojure.set/difference claims version-set)))))
     (get-entities model))))

(defn find-exclusive-relations
  "Returns relations that are ONLY claimed by the provided version-uuids"
  [model version-uuids]
  (let [version-set (set version-uuids)]
    (filter
     (fn [relation]
       (let [claims (get relation :claimed-by #{})]
          ;; Skip relations without claims (legacy system relations)
          ;; Exclusive if all claims are within version-uuids
         (and (not-empty claims)
              (empty? (clojure.set/difference claims version-set)))))
     (get-relations model))))

;;; Projection Helper Functions

(defn projection-data [x] (:dataset/projection (meta x)))

(defn attribute-has-diff?
  [attribute]
  (boolean (not-empty (:diff (projection-data attribute)))))

(defn new-attribute? [attribute] (boolean (:added? (projection-data attribute))))

(defn removed-attribute? [attribute] (boolean (:removed? (projection-data attribute))))

(def attribute-changed? (some-fn new-attribute? removed-attribute? attribute-has-diff?))
(def attribute-not-changed? (complement attribute-changed?))

(defn entity-has-diff?
  [{:keys [attributes]
    :as entity}]
  (let [{:keys [diff added?]} (projection-data entity)]
    (and
     (not added?)
     (or
      (not-empty (dissoc diff :width :height))
      (some attribute-changed? attributes)))))

(defn new-entity? [e] (boolean (:added? (projection-data e))))

(def entity-changed? (some-fn new-entity? entity-has-diff?))
(def entity-not-changed? (complement entity-changed?))

(defn new-relation? [r] (boolean (:added? (projection-data r))))
(defn relation-has-diff? [r] (some? (:diff (projection-data r))))

(def relation-changed? (some-fn new-relation? relation-has-diff?))
(def relation-not-changed? (complement relation-changed?))

(defn recursive-relation? [relation]
  (boolean (#{"tree"} (:cardinality relation))))

(defn setup
  "Setup dataset for given DB target.

  Validates the database is supported."
  [db]
  db)

;; =============================================================================
;; RBAC Configuration Helpers
;; =============================================================================

(defn rbac-enabled?
  "Check if RBAC is enabled for entity"
  [entity]
  (get-in entity [:configuration :rbac :enabled] false))

(defn set-rbac-enabled
  "Enable or disable RBAC for entity"
  [entity enabled]
  (assoc-in entity [:configuration :rbac :enabled] enabled))

(defn audit-actions
  "Get audit actions set from entity configuration.
  Returns #{:modified :created} or nil."
  [entity]
  (get-in entity [:configuration :audit :actions]))

(defn audit-modified?
  "Check if :modified audit is enabled for entity"
  [entity]
  (contains? (audit-actions entity) :modified))

(defn audit-created?
  "Check if :created audit is enabled for entity"
  [entity]
  (contains? (audit-actions entity) :created))

(defn audit-persist?
  "True iff this entity opts into audit-substrate persistence.
   Set as `[:configuration :audit/persist]` on the entity. Independent of
   `[:configuration :audit]` which configures schema augmentation (:who /
   :when / :actions). When false (the default), mutations still fire
   triggers and flow through the drainer for live delta notifications,
   but the audit provider skips persisting them — so /history will have
   no rows for the entity. The SYNTHIGY_AUDIT_ALL env var overrides
   per-entity choices and persists everything."
  [entity]
  (boolean (get-in entity [:configuration :audit/persist])))

(defn set-audit-persist
  "Flip the audit-substrate opt-in flag on the entity. Persists to
   `[:configuration :audit/persist]`. When toggled on (and the model is
   redeployed) the audit provider will start writing every mutation of
   this entity into `__audit_entity`, queryable via `/history`."
  [entity enabled]
  (assoc-in entity [:configuration :audit/persist] (boolean enabled)))

(defn relation-rbac-enabled?
  "Check if RBAC is enabled for relation in given direction [from-id to-id]"
  [relation direction]
  (get-in relation [:configuration :rbac direction :enabled] false))

(defn set-relation-rbac-enabled
  "Enable or disable RBAC for relation in given direction [from-id to-id]"
  [relation direction enabled]
  (assoc-in relation [:configuration :rbac direction :enabled] enabled))

(defn unique-constraints-indexed
  "Index-stable view of an entity's composite unique-key groups for DDL.

   Returns a vector the SAME length as the raw `:configuration :constraints
   :unique`, with each group either kept (all member attributes active) or
   replaced by `nil` (any member inactive/removed → the whole combo is dropped).

   The DDL names unique constraints by position (`_eucg_<idx>`), so positions
   must be preserved: a deactivated attribute nils its group in place, which the
   backend transform turns into a DROP CONSTRAINT for that index. Pairs with
   `get-entity-unique-constraints` (the compacted, schema-facing view)."
  [entity]
  (let [active (set (map id/extract (filter :active (:attributes entity))))]
    (mapv (fn [group]
            (when (and (seq group) (every? active group))
              (vec group)))
          (get-in entity [:configuration :constraints :unique]))))

;; =============================================================================
;; RLS Configuration Helpers
;; =============================================================================

(defn get-rls-config
  "Get RLS configuration from entity"
  [entity]
  (get-in entity [:configuration :rls]))

(defn set-rls-config
  "Set RLS configuration on entity"
  [entity rls-config]
  (assoc-in entity [:configuration :rls] rls-config))

(defn rls-enabled?
  "Check if RLS is enabled for entity"
  [entity]
  (get-in entity [:configuration :rls :enabled] false))

(defn set-rls-enabled
  "Enable or disable RLS for entity"
  [entity enabled]
  (assoc-in entity [:configuration :rls :enabled] enabled))

(defn get-rls-guards
  "Get RLS guards from entity"
  [entity]
  (get-in entity [:configuration :rls :guards] []))

(defn set-rls-guards
  "Set RLS guards on entity"
  [entity guards]
  (assoc-in entity [:configuration :rls :guards] guards))

(defn add-rls-guard
  "Add a new RLS guard to entity"
  [entity guard]
  (update-in entity [:configuration :rls :guards]
             (fnil conj [])
             guard))

(defn remove-rls-guard
  "Remove an RLS guard by id"
  [entity guard-id]
  (update-in entity [:configuration :rls :guards]
             (fn [guards]
               (vec (remove #(= (:id %) guard-id) guards)))))

(defn find-guard-index
  "Find the index of a guard by id"
  [guards guard-id]
  (first (keep-indexed
          (fn [idx g] (when (= (:id g) guard-id) idx))
          guards)))

(defn toggle-rls-operation
  "Toggle a Read/Write operation on a guard"
  [entity guard-id operation]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)]
    (if guard-idx
      (let [current-ops (get-in guards [guard-idx :operation] #{})
            new-ops (if (contains? current-ops operation)
                      (disj current-ops operation)
                      (conj current-ops operation))]
        (assoc-in entity [:configuration :rls :guards guard-idx :operation] new-ops))
      entity)))

(defn- path->condition
  "Convert a discovered path to a minimal condition for storage.
   Only stores ids (active form via the seam) - no names that can go stale.
   Key names are format-DECOUPLED (`:relation-id`/`:entity-id`/`:attribute`),
   the same name regardless of whether values are euuid or xid.

   Stored structure:
   - :ref      {:type :ref :attribute <id>}
   - :relation {:type :relation :steps [{:relation-id <id> :entity-id <id>}]}
   - :hybrid   {:type :hybrid :steps [{:relation-id <id> :entity-id <id>}] :attribute <id>}"
  [path]
  (case (:type path)
    :ref
    {:type :ref
     :attribute (:attribute-id path)}

    :relation
    {:type :relation
     :steps (mapv #(select-keys % [:relation-id :entity-id])
                  (:steps path))}

    :hybrid
    {:type :hybrid
     :steps (mapv #(select-keys % [:relation-id :entity-id])
                  (:steps path))
     :attribute (:attribute-id path)}))

(defn condition-matches-path?
  "Check if a stored condition matches a discovered path by comparing UUIDs.
   This is stable across model changes that don't affect the actual path structure."
  [condition path]
  (case (:type condition)
    :ref
    (= (:attribute condition) (:attribute-id path))

    :relation
    (let [condition-steps (mapv :relation-id (:steps condition))
          path-steps (mapv :relation-id (:steps path))]
      (= condition-steps path-steps))

    :hybrid
    (and (= (:attribute condition) (:attribute-id path))
         (let [condition-steps (mapv :relation-id (:steps condition))
               path-steps (mapv :relation-id (:steps path))]
           (= condition-steps path-steps)))

    ;; Legacy: fallback to path-id for old configs
    (= (:path-id condition) (:id path))))

(defn toggle-rls-condition
  "Toggle a path condition on a guard. If guard doesn't exist, creates a new one.
   Auto-removes guard if all conditions are removed.
   Matches conditions by structure (UUIDs), not ephemeral path-id."
  [entity guard-id path]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)]
    (if guard-idx
      ;; Toggle condition on existing guard
      (let [conditions (get-in guards [guard-idx :conditions] [])
            matching-condition (some #(when (condition-matches-path? % path) %) conditions)
            new-conditions (if matching-condition
                             (vec (remove #(condition-matches-path? % path) conditions))
                             (conj conditions (path->condition path)))]
        ;; Auto-remove guard if no conditions left
        (if (empty? new-conditions)
          (remove-rls-guard entity guard-id)
          (assoc-in entity [:configuration :rls :guards guard-idx :conditions] new-conditions)))
      ;; Guard not found - create new guard with this condition
      (let [new-guard {:id (id/generate)
                       :operation #{}
                       :conditions [(path->condition path)]}]
        (add-rls-guard entity new-guard)))))

;;; RLS Guards - Modal UI Support Functions

(defn paths->id-set
  "Convert paths to a set of identifying ids (active form) for comparison.
   Used for duplicate detection - two path selections are duplicates
   if they produce the same id set."
  [paths]
  (set
   (map
    (fn [path]
      (case (:type path)
        :ref (:attribute-id path)
        :relation (mapv :relation-id (:steps path))
        :hybrid [(:attribute-id path) (mapv :relation-id (:steps path))]))
    paths)))

(defn conditions->id-set
  "Convert stored conditions to id set (active form) for comparison."
  [conditions]
  (set
   (map
    (fn [condition]
      (case (:type condition)
        :ref (:attribute condition)
        :relation (mapv :relation-id (:steps condition))
        :hybrid [(:attribute condition) (mapv :relation-id (:steps condition))]))
    conditions)))

(defn guard-matches-paths?
  "Check if a guard's conditions match exactly the given paths.
   Used for duplicate detection when adding/editing guards."
  [guard paths]
  (= (conditions->id-set (:conditions guard))
     (paths->id-set paths)))

(defn- entity-active-attribute?
  "Is `attr-id` an ACTIVE attribute of `entity`? nil-safe on both."
  [entity attr-id]
  (boolean
   (some #(and (:active %) (= attr-id (id/extract %)))
         (:attributes entity))))

(defn validate-guard-paths
  "Validate guard conditions against current model state.
   Returns a map with:
   - :valid - vector of valid conditions
   - :invalid - vector of invalid conditions (referencing removed entities/relations/attributes)

   A condition is invalid if:
   - :ref type: attribute no longer exists (or is inactive) on entity
   - :relation type: any relation in the path no longer exists
   - :hybrid type: any relation in the path, the final entity, or the final
     entity's attribute no longer exists. Conditions store the traversed
     `:entity-id` per step, so the final entity is `(last steps)` — no
     graph traversal needed.

   A dangling condition is a real hazard, not cosmetic: the deploy-time RLS
   compiler drops the WHOLE guard (fail-safe), which fail-closes any
   operation only that guard granted — writes silently no-op."
  [guard model entity]
  ;; Relation activeness: only an EXPLICIT :active false counts as inactive.
  ;; Authored (not-yet-deployed) models leave :active nil on relations —
  ;; absent is NOT inactive (path discovery never checks it either); requiring
  ;; truthy here false-flagged every relation condition in the modeler.
  (let [model-relations (set (map id/extract
                                  (remove #(false? (:active %)) (get-relations model))))]
    (reduce
     (fn [acc condition]
       (let [valid?
             (case (:type condition)
               :ref
               (entity-active-attribute? entity (:attribute condition))

               :relation
               (and (seq (:steps condition))
                    (every? #(contains? model-relations (:relation-id %))
                            (:steps condition)))

               :hybrid
               (and (seq (:steps condition))
                    ;; all relations in the path exist
                    (every? #(contains? model-relations (:relation-id %))
                            (:steps condition))
                    ;; final entity + its referenced attribute exist
                    (let [final-entity (get-entity model (:entity-id (last (:steps condition))))]
                      (and final-entity
                           (entity-active-attribute? final-entity (:attribute condition)))))

               ;; Unknown type - treat as invalid
               false)]
         (update acc (if valid? :valid :invalid) conj condition)))
     {:valid []
      :invalid []}
     (:conditions guard))))

(defn remove-guard-condition
  "Remove one (structurally matched) condition from a guard — the drawer's
   'remove broken condition' affordance. Drops the guard entirely when its
   last condition is removed."
  [entity guard-id condition]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)]
    (if-not guard-idx
      entity
      (let [conditions (get-in guards [guard-idx :conditions] [])
            remaining (vec (remove #(= % condition) conditions))]
        (if (empty? remaining)
          (remove-rls-guard entity guard-id)
          (assoc-in entity [:configuration :rls :guards guard-idx :conditions] remaining))))))

(defn validate-unique-constraints
  "Validate an entity's composite unique-key groups against its current
   attributes. Returns {:valid [groups] :invalid [groups]} — a group is
   invalid when ANY member attribute is missing or inactive (the DDL layer
   silently drops such groups; the modeler should show them as broken
   instead). EMPTY groups are ignored: they are unfilled placeholders that
   impose no constraint — not rot (several system entities carry them)."
  [entity]
  (let [active (set (map id/extract (filter :active (:attributes entity))))]
    (reduce
     (fn [acc group]
       (if (empty? group)
         acc
         (update acc
                 (if (every? active group) :valid :invalid)
                 conj (vec group))))
     {:valid [] :invalid []}
     (get-in entity [:configuration :constraints :unique]))))

(defn model-integrity-report
  "Sweep the whole model for configuration rot — rules referencing removed or
   inactive attributes/relations. Returns a vector of per-entity findings:

     [{:entity <name> :entity-id <id>
       :invalid-guard-conditions <n>   ; RLS conditions that no longer resolve
       :invalid-unique-groups <n>}]    ; unique-key groups with dead members

   Empty vector = clean model. Run on load: rot is invisible in normal use
   (deploy silently drops broken rules) but fail-closes RLS-guarded writes."
  [model]
  (vec
   (for [entity (get-entities model)
         :let [guard-rot (when (rls-enabled? entity)
                           (reduce + (map #(count (:invalid (validate-guard-paths % model entity)))
                                          (get-rls-guards entity))))
               unique-rot (count (:invalid (validate-unique-constraints entity)))]
         :when (or (pos? (or guard-rot 0)) (pos? unique-rot))]
     {:entity (:name entity)
      :entity-id (id/extract entity)
      :invalid-guard-conditions (or guard-rot 0)
      :invalid-unique-groups unique-rot})))

(defn paths->conditions
  "Convert a collection of paths to conditions for storage."
  [paths]
  (mapv path->condition paths))

(defn add-rls-guard-with-paths
  "Add a new RLS guard with the given paths and default READ permission.
   Returns nil if paths would create a duplicate guard."
  [entity paths]
  (let [guards (get-rls-guards entity)
        ;; Check for duplicates
        duplicate? (some #(guard-matches-paths? % paths) guards)]
    (when-not duplicate?
      (let [new-guard {:id (id/generate)
                       :operation #{:read}
                       :conditions (paths->conditions paths)}]
        (add-rls-guard entity new-guard)))))

(defn update-rls-guard-paths
  "Update an existing guard's paths (conditions).
   Returns nil if the new paths would create a duplicate with another guard."
  [entity guard-id paths]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)
        ;; Check for duplicates with OTHER guards (not this one)
        other-guards (remove #(= (:id %) guard-id) guards)
        duplicate? (some #(guard-matches-paths? % paths) other-guards)]
    (when (and guard-idx (not duplicate?))
      (assoc-in entity
                [:configuration :rls :guards guard-idx :conditions]
                (paths->conditions paths)))))

;;; ============================================================================
;;; RLS Guard - Path Discovery (BFS graph traversal)
;;; ============================================================================

(def ^:private path-labels "ABCDEFGHIJKLMNOPQRSTUVWXYZ")

(defn get-path-label
  "Get letter label for path index (0 -> A, 25 -> Z, 26 -> AA, 27 -> AB, etc.)"
  [idx]
  (loop [n idx
         result ""]
    (let [remainder (mod n 26)
          char (nth path-labels remainder)
          new-result (str char result)
          quotient (quot n 26)]
      (if (zero? quotient)
        new-result
        (recur (dec quotient) new-result)))))

(defn iam-entity-type
  "Returns the IAM entity type keyword for a given entity UUID.
   Requires iam-uuids map with :user, :group, :role keys."
  [euuid iam-uuids]
  (cond
    (= euuid (:user iam-uuids)) :user
    (= euuid (:group iam-uuids)) :group
    (= euuid (:role iam-uuids)) :role
    :else nil))

(defn- discover-ref-paths*
  "Discover ref attributes (type user/group/role) as direct paths to IAM entities.
   Returns vector of ref paths with :type :ref"
  [entity iam-uuids start-idx]
  (let [ref-types #{"user" "group" "role"}
        attributes (or (:attributes entity) [])]
    (->> attributes
         (filter (fn [attr]
                   (and (:active attr)
                        (contains? ref-types (:type attr)))))
         (map-indexed
          (fn [idx attr]
            (let [attr-type (:type attr)
                  target (keyword attr-type)
                  target-id (get iam-uuids target)]
              {:id (get-path-label (+ start-idx idx))
               :type :ref
               :target target
               :target-name (case target
                              :user "User"
                              :group "UserGroup"
                              :role "UserRole")
               :target-id target-id
               :attribute-id (id/extract attr)
               :attribute-name (:name attr)
               :depth 0})))
         vec)))

(defn- discover-relation-paths*
  "Discover relation paths to IAM entities using BFS.
   Returns vector of paths with :type :relation or :type :hybrid.

   :relation - path ends at an IAM entity via relation
   :hybrid - path traverses relations then ends at a ref attribute (user/group/role type)"
  [model entity iam-uuids max-depth start-idx]
  ;; id-AGNOSTIC: track nodes by (id/extract …) (active form). The caller passes
  ;; iam-uuids already in the active form (via id/entity), so comparisons agree
  ;; in any provider. Path-struct field NAMES are format-DECOUPLED (`-id` suffix,
  ;; never `-euuid`/`-xid`); their VALUES are the active id form.
  (let [entity-id (id/extract entity)
        iam-entity-ids (set (vals iam-uuids))]
    (loop [queue [{:entity entity
                   :steps []
                   :visited #{entity-id}}]
           paths []
           path-idx start-idx]
      (if (empty? queue)
        paths
        (let [{:keys [entity steps visited]} (first queue)
              remaining (rest queue)
              current-depth (count steps)]
          (if (>= current-depth max-depth)
            (recur remaining paths path-idx)
            (let [relations (focus-entity-relations model entity)
                  new-items
                  (reduce
                   (fn [acc relation]
                     (let [target-entity (:to relation)
                           target-id (id/extract target-entity)
                           relation-label (or (:to-label relation) (:from-label relation) "")]
                       (if (contains? visited target-id)
                         acc
                         (let [new-step {:relation-id (id/extract relation)
                                         :label relation-label
                                         :entity-id target-id
                                         :entity-name (:name target-entity)}
                               new-steps (conj steps new-step)
                               new-visited (conj visited target-id)
                               ref-types #{"user" "group" "role"}
                               is-iam? (contains? iam-entity-ids target-id)
                               target-refs (when-not is-iam?
                                             (->> (:attributes target-entity)
                                                  (filter #(and (:active %)
                                                                (contains? ref-types (:type %))))))
                               iam-path (when is-iam?
                                          {:id (get-path-label (+ path-idx (count (:paths acc))))
                                           :type :relation
                                           :target (iam-entity-type target-id iam-uuids)
                                           :target-name (:name target-entity)
                                           :target-id target-id
                                           :steps new-steps
                                           :depth (count new-steps)})
                               base-offset (+ path-idx (count (:paths acc)) (if iam-path 1 0))
                               hybrid-paths (vec
                                             (map-indexed
                                              (fn [idx attr]
                                                (let [t (keyword (:type attr))]
                                                  {:id (get-path-label (+ base-offset idx))
                                                   :type :hybrid
                                                   :target t
                                                   :target-name (case t :user "User" :group "UserGroup" :role "UserRole")
                                                   :target-id (get iam-uuids t)
                                                   :steps new-steps
                                                   :attribute-id (id/extract attr)
                                                   :attribute-name (:name attr)
                                                   :depth (count new-steps)}))
                                              target-refs))
                               new-paths (cond-> (:paths acc)
                                           iam-path (conj iam-path)
                                           (seq hybrid-paths) (into hybrid-paths))
                               new-queue (conj (:queue acc)
                                               {:entity target-entity
                                                :steps new-steps
                                                :visited new-visited})]
                           (assoc acc :paths new-paths :queue new-queue)))))
                   {:paths [] :queue []}
                   relations)]
              (recur (into (vec remaining) (:queue new-items))
                     (into paths (:paths new-items))
                     (+ path-idx (count (:paths new-items)))))))))))

(defn discover-paths-to-iam
  "Discovers all paths from entity to IAM entities (User, UserGroup, UserRole).
   Finds:
   - Ref attributes (type user/group/role) as direct paths (:type :ref)
   - Relation paths via BFS traversal (:type :relation)
   - Hybrid paths: relations ending at a ref attribute (:type :hybrid)

   Arguments:
   - model: The ERD model
   - entity: The source entity to start from
   - iam-uuids: Map with :user, :group, :role keys containing entity UUIDs
   - max-depth: Maximum number of hops for relation paths (default 3)

   Returns a vector of paths, each with :id (letter label), :type, :target, :steps, etc."
  ([model entity iam-uuids]
   (discover-paths-to-iam model entity iam-uuids 3))
  ([model entity iam-uuids max-depth]
   (when (and model entity)
     (let [ref-paths (discover-ref-paths* entity iam-uuids 0)
           ref-count (count ref-paths)
           relation-paths (discover-relation-paths* model entity iam-uuids max-depth ref-count)]
       (into ref-paths relation-paths)))))

;;; ============================================================
;;; RLS Projection (dual-base projection for deploy drawer)
;;; ============================================================

(defn- conditions-equal?
  "Compare conditions as sets (order-insensitive)."
  [conds1 conds2]
  (= (set conds1) (set conds2)))

(defn- guard-changed?
  "Check if a guard has changed between old and new versions."
  [old-guard new-guard]
  (or (not= (:operation old-guard) (:operation new-guard))
      (not (conditions-equal? (:conditions old-guard) (:conditions new-guard)))))

(defn- compute-guard-diff
  "Compute what changed in a guard between old and new versions."
  [old-guard new-guard]
  (cond-> {}
    (not= (:operation old-guard) (:operation new-guard))
    (assoc :operation (:operation old-guard))
    (not (conditions-equal? (:conditions old-guard) (:conditions new-guard)))
    (assoc :conditions (:conditions old-guard))))

(defn project-rls-guards
  "Project RLS guards from old config onto new config.
   Returns guards with projection metadata:
   - :added? for guards in new but not old
   - :removed? for guards in old but not new
   - :diff for guards in both but changed
   Compares by :id, not position. Conditions compared as sets."
  [old-rls new-rls]
  (let [old-guards (or (:guards old-rls) [])
        new-guards (or (:guards new-rls) [])
        old-by-id (into {} (map (juxt :id identity) old-guards))
        new-by-id (into {} (map (juxt :id identity) new-guards))
        old-ids (set (keys old-by-id))
        new-ids (set (keys new-by-id))
        added-ids (clojure.set/difference new-ids old-ids)
        removed-ids (clojure.set/difference old-ids new-ids)
        common-ids (clojure.set/intersection old-ids new-ids)
        projected-guards
        (concat
         (for [id added-ids
               :let [guard (get new-by-id id)]]
           (vary-meta guard assoc-in [:dataset/projection :added?] true))
         (for [id removed-ids
               :let [guard (get old-by-id id)]]
           (vary-meta guard assoc-in [:dataset/projection :removed?] true))
         (for [id common-ids
               :let [old-guard (get old-by-id id)
                     new-guard (get new-by-id id)]]
           (if (guard-changed? old-guard new-guard)
             (vary-meta new-guard assoc-in [:dataset/projection :diff]
                        (compute-guard-diff old-guard new-guard))
             new-guard)))]
    {:enabled (get new-rls :enabled (get old-rls :enabled false))
     :guards (vec projected-guards)}))

(defn rls-has-changes?
  "Check if projected RLS configuration has any effective changes.
   Only added and changed guards count — removed guards persist from other versions."
  [projected-rls]
  (some (fn [guard]
          (let [proj (:dataset/projection (meta guard))]
            (or (:added? proj) (not-empty (:diff proj)))))
        (:guards projected-rls)))

(defn rls-differs-from-base?
  "Check if RLS configuration differs from base for deployability.
   Considers added, changed, removed guards, and enabled flag changes."
  [base-rls target-rls]
  (let [base-enabled (get base-rls :enabled false)
        target-enabled (get target-rls :enabled false)
        base-guards (or (:guards base-rls) [])
        target-guards (or (:guards target-rls) [])
        base-guard-ids (set (map :id base-guards))
        target-guard-ids (set (map :id target-guards))
        guards-differ? (or (not= base-guard-ids target-guard-ids)
                           (let [projected (project-rls-guards base-rls target-rls)]
                             (rls-has-changes? projected)))
        enabled-differs? (not= base-enabled target-enabled)]
    (or enabled-differs? guards-differ?)))

(defn project-entity-with-rls-base
  "Project entity with separate bases for structural and RLS projection.
   Structural changes projected against global-entity.
   RLS guards projected against rls-base-entity (last deployed version of this dataset)."
  [global-entity target-entity rls-base-entity]
  (if (nil? target-entity)
    (when global-entity (mark-removed global-entity))
    (let [structural-projection (project global-entity target-entity)
          rls-base (get-in rls-base-entity [:configuration :rls])
          rls-target (get-in target-entity [:configuration :rls])
          projected-rls (when (or rls-base rls-target)
                          (project-rls-guards rls-base rls-target))
          has-rls-diff? (rls-differs-from-base? rls-base rls-target)]
      (if has-rls-diff?
        (-> structural-projection
            (assoc-in [:configuration :rls] projected-rls)
            (vary-meta assoc-in [:dataset/projection :diff :configuration :rls] rls-base))
        ;; No RLS diff — strip the :rls slot from the configuration diff,
        ;; but DO NOT nuke :configuration wholesale. Sibling keys like
        ;; :audit and :constraints (set by the structural projection in
        ;; synthigy.dataset.projection) live under the same :configuration
        ;; map and must survive. If after removing :rls the :configuration
        ;; map is empty, drop it; if the resulting :diff map is empty,
        ;; drop that too — so `diff?` doesn't misreport.
        (vary-meta
         structural-projection
         (fn [m]
           (let [cleaned (-> (or (get-in m [:dataset/projection :diff :configuration]) {})
                             (dissoc :rls))
                 diff   (cond-> (or (get-in m [:dataset/projection :diff]) {})
                          true                  (dissoc :configuration)
                          (seq cleaned)         (assoc :configuration cleaned))]
             (if (empty? diff)
               (update m :dataset/projection dissoc :diff)
               (assoc-in m [:dataset/projection :diff] diff)))))))))

(defn has-rls-changes-from-base?
  "Check if any entity has RLS changes compared to a base model."
  [target base]
  (when base
    (some (fn [entity]
            (let [base-entity (get-entity base (id/extract entity))
                  base-rls (get-in base-entity [:configuration :rls])
                  target-rls (get-in entity [:configuration :rls])]
              (rls-differs-from-base? base-rls target-rls)))
          (get-entities target))))

(defn project-with-rls-base
  "Project model with separate bases for structural and RLS projection.
   Structural changes projected against global (union of all deployed versions).
   RLS guards projected against rls-base (last deployed version of THIS dataset).
   If rls-base is nil, falls back to normal projection."
  [global target rls-base]
  (if (nil? rls-base)
    (project global target)
    (let [rls-base-model (if (instance? ERDModel rls-base) rls-base (:model rls-base))]
      (as-> target projection
        (reduce
         (fn [m e]
           (let [id (id/extract e)
                 global-entity (get-entity global id)
                 rls-base-entity (get-entity rls-base-model id)]
             (set-entity m (project-entity-with-rls-base global-entity e rls-base-entity))))
         projection
         (get-entities projection))
        (reduce
         (fn [m r]
           (set-relation m (project (get-relation global (id/extract r)) r)))
         projection
         (get-relations projection))))))
