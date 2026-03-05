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

(defn not-initialized [msg]
  (throw (ex-info "Delta client not initialized" {:message msg})))

(defonce ^:dynamic *return-type* :raw)
(defonce ^:dynamic *delta-client* not-initialized)
(defonce ^:dynamic *delta-publisher* not-initialized)

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

    ;; FORBIDDEN: avatar → json (current implementation nulls everything)
    (and (= from-type "avatar") (= to-type "json"))
    {:error "Cannot convert avatar to json: Avatar URLs/data cannot be meaningfully converted to JSON."
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need to preserve the data, then manually transform to valid JSON if needed."}

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

(defrecord ERDRelation [euuid xid from to from-label to-label cardinality path active claimed-by])
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
    (let [active-attributes (set (map id/extract (filter :active (:attributes this))))]
      (reduce
        (fn [r constraint-group]
          (if-some [filtered-group (not-empty (filter active-attributes constraint-group))]
            (conj r (vec filtered-group))
            r))
        []
        (get-in this [:configuration :constraints :unique]))))

  ERDEntityAttributeProtocol
  (add-attribute [{:keys [attributes]
                   :as this} {:as attribute}]
    {:pre [(instance? ERDEntityAttribute attribute)]}
    (let [attribute (map->ERDEntityAttribute attribute)
          attribute-id (or (id/extract attribute) (id/generate))
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
                  ;; Merge attributes if both entities exist
                    merged-entity (if (and entity1 entity2)
                                    (merge-entity-attributes entity1 entity2)
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
   Only stores UUIDs - no names that can go stale.

   Stored structure:
   - :ref      {:type :ref :attribute <uuid>}
   - :relation {:type :relation :steps [{:relation <uuid> :entity <uuid>}]}
   - :hybrid   {:type :hybrid :steps [{:relation <uuid> :entity <uuid>}] :attribute <uuid>}"
  [path]
  (case (:type path)
    :ref
    {:type :ref
     :attribute (:attribute-euuid path)}

    :relation
    {:type :relation
     :steps (mapv #(select-keys % [:relation-euuid :entity-euuid])
                  (:steps path))}

    :hybrid
    {:type :hybrid
     :steps (mapv #(select-keys % [:relation-euuid :entity-euuid])
                  (:steps path))
     :attribute (:attribute-euuid path)}))

(defn condition-matches-path?
  "Check if a stored condition matches a discovered path by comparing UUIDs.
   This is stable across model changes that don't affect the actual path structure."
  [condition path]
  (case (:type condition)
    :ref
    (= (:attribute condition) (:attribute-euuid path))

    :relation
    (let [condition-steps (mapv :relation-euuid (:steps condition))
          path-steps (mapv :relation-euuid (:steps path))]
      (= condition-steps path-steps))

    :hybrid
    (and (= (:attribute condition) (:attribute-euuid path))
         (let [condition-steps (mapv :relation-euuid (:steps condition))
               path-steps (mapv :relation-euuid (:steps path))]
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

(defn paths->euuid-set
  "Convert paths to a set of identifying UUIDs for comparison.
   Used for duplicate detection - two path selections are duplicates
   if they produce the same euuid set."
  [paths]
  (set
    (map
      (fn [path]
        (case (:type path)
          :ref (:attribute-euuid path)
          :relation (mapv :relation-euuid (:steps path))
          :hybrid [(:attribute-euuid path) (mapv :relation-euuid (:steps path))]))
      paths)))

(defn conditions->euuid-set
  "Convert stored conditions to euuid set for comparison."
  [conditions]
  (set
    (map
      (fn [condition]
        (case (:type condition)
          :ref (:attribute condition)
          :relation (mapv :relation-euuid (:steps condition))
          :hybrid [(:attribute condition) (mapv :relation-euuid (:steps condition))]))
      conditions)))

(defn guard-matches-paths?
  "Check if a guard's conditions match exactly the given paths.
   Used for duplicate detection when adding/editing guards."
  [guard paths]
  (= (conditions->euuid-set (:conditions guard))
     (paths->euuid-set paths)))

(defn validate-guard-paths
  "Validate guard conditions against current model state.
   Returns a map with:
   - :valid - vector of valid conditions
   - :invalid - vector of invalid conditions (referencing removed entities/relations/attributes)

   A condition is invalid if:
   - :ref type: attribute no longer exists on entity
   - :relation type: any relation in the path no longer exists
   - :hybrid type: any relation or the final attribute no longer exists"
  [guard model entity]
  (let [entity-attrs (set (map :euuid (filter :active (:attributes entity))))
        model-relations (set (map :euuid (filter :active (get-relations model))))]
    (reduce
      (fn [acc condition]
        (let [valid?
              (case (:type condition)
                :ref
                (contains? entity-attrs (:attribute condition))

                :relation
                (every? #(contains? model-relations (:relation-euuid %))
                        (:steps condition))

                :hybrid
                (and
                  ;; All relations in path exist
                  (every? #(contains? model-relations (:relation-euuid %))
                          (:steps condition))
                  ;; Final entity's attribute exists
                  ;; Note: We'd need to traverse to check the final entity's attrs
                  ;; For now, just check relations - attr check requires model traversal
                  true)

                ;; Unknown type - treat as invalid
                false)]
          (update acc (if valid? :valid :invalid) conj condition)))
      {:valid []
       :invalid []}
      (:conditions guard))))

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
