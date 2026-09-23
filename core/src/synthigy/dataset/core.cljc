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

(ns synthigy.dataset.core
  "Core dataset protocols and records for entity-relationship modeling."
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

;;; Type Conversion Validation System (Shared Frontend/Backend)

(declare reference-types)

(defn type-families
  "Returns type family groupings used to determine safe conversions."
  []
  {:text #{"string" "avatar" "transit" "hashed"}
   :json #{"json" "encrypted"}
   :numeric #{"int" "float"}
   :reference (reference-types)
   :temporal #{"timestamp"}
   :boolean #{"boolean"}
   :enum #{"enum"}})

(def legacy-type-mapping
  "Maps legacy attribute types to their modern equivalents."
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
           :doc "Maps attribute type strings to reference metadata."}
  *reference-mapping*
  {})

;;; Reference Type API

(def reference-type-names
  "Attribute types that resolve to an IAM entity FK. Fixed vocabulary — the
   registry says which are CURRENTLY resolvable, this says which ever are."
  #{"user" "group" "role"})

(defn reference-types []
  (set (keys *reference-mapping*)))

(defn reference-type? [type-name]
  (contains? *reference-mapping* type-name))

(defn reference-entity-id [type-name]
  (let [v (get *reference-mapping* type-name)]
    (if (uuid? v) v (:entity-id v))))

(defn reference-table-fn [type-name]
  (when-let [v (get *reference-mapping* type-name)]
    (when (map? v) (:table-fn v))))

(defn register-reference-type!
  "Registers a reference type with metadata."
  [type-name entity-id opts]
  #?(:clj (alter-var-root
           #'*reference-mapping*
           (fn [m] (assoc m type-name (merge {:entity-id entity-id} opts))))))

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
  "Shared frontend/backend validation of a type conversion — returns {:safe
   true}, {:warning …} or {:error …}."
  [from-type to-type]
  (cond
    (= from-type to-type)
    {:safe true}

    ;; references are structural model constructs, not castable scalars —
    ;; must come before every scalar rule (notably the to-string :safe case)
    (or (reference-type? from-type) (reference-type? to-type))
    {:error (str "Cannot convert " from-type " to " to-type ": reference attributes are structural, not castable.")
     :type ::forbidden-conversion
     :suggestion "Deactivate the attribute and create a new reference attribute, then migrate the links."}

    ;; specific warnings must come BEFORE the general family checks

    (and (= from-type "float") (= to-type "int"))
    {:warning "Converting float to int will truncate decimal values. Precision loss may occur."}

    (and (= from-type "timestamp") (= to-type "string"))
    {:warning "Converting timestamp to string will lose temporal semantics and indexing capabilities. Consider carefully if this is necessary."}

    (and (= from-type "encrypted")
         (not (contains? (:json (type-families)) to-type))
         (not= to-type "string"))
    {:error "Cannot convert encrypted data to non-JSON/string type: Data is encrypted and cannot be directly converted."
     :type ::forbidden-conversion
     :suggestion "Decrypt data first or keep as encrypted/json type."}

    (= to-type "string")
    {:safe true}

    (same-family? from-type to-type)
    {:safe true}

    (and (= from-type "int") (= to-type "float"))
    {:safe true}

    (and (= from-type "enum") (= to-type "string"))
    {:safe true}

    (and (= from-type "string") (contains? #{"int" "float"} to-type))
    {:warning (str "Converting string to " to-type " requires all values to be valid numbers. Invalid values will cause the conversion to fail.")}

    (and (= from-type "string") (= to-type "boolean"))
    {:warning "Converting string to boolean requires all values to be 't', 'f', 'true', 'false', 'yes', 'no', '1', '0'. Invalid values will cause the conversion to fail."}

    (and (= from-type "string") (= to-type "timestamp"))
    {:warning "Converting string to timestamp requires all values to be valid timestamp formats. Invalid values will cause the conversion to fail."}

    (and (= from-type "string") (= to-type "json"))
    {:warning "Converting string to json will set non-JSON values to NULL. This may result in data loss."}

    (and (= from-type "string") (= to-type "enum"))
    {:warning "Converting string to enum requires all values to be valid enum values. Invalid values will cause the conversion to fail."}

    (and (= from-type "avatar") (contains? #{"json" "string"} to-type))
    {:safe true}

    (and (contains? #{"json" "string"} from-type) (= to-type "avatar"))
    {:safe true}

    (and (= from-type "transit") (= to-type "json"))
    {:safe true}

    (and (contains? (:json (type-families)) from-type)
         (contains? #{"int" "float"} to-type))
    {:error (str "Cannot convert " from-type " to " to-type ": No meaningful automatic conversion exists.")
     :type ::forbidden-conversion
     :suggestion "Extract numeric fields from JSON manually before converting."}

    (and (contains? (:json (type-families)) from-type)
         (= to-type "boolean"))
    {:error (str "Cannot convert " from-type " to boolean: No meaningful automatic conversion exists.")
     :type ::forbidden-conversion
     :suggestion "Extract boolean fields from JSON manually before converting."}

    (and (= from-type "timestamp") (= to-type "int"))
    {:error "Cannot convert timestamp to int: Use explicit epoch conversion if needed."
     :type ::forbidden-conversion
     :suggestion "Create a new attribute and populate it with epoch timestamps explicitly."}

    (and (= from-type "boolean") (contains? #{"int" "float"} to-type))
    {:error (str "Cannot convert boolean to " to-type ": Semantic mismatch.")
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need '0'/'1' representation, or create explicit mapping logic."}

    :else
    {:error (str "Unsupported type conversion from " from-type " to " to-type ".")
     :type ::unsupported-conversion
     :suggestion "This conversion path has not been validated. Please review the type compatibility matrix."}))

(defprotocol EntityConstraintProtocol
  (set-entity-unique-constraints [this constraints])
  (update-entity-unique-constraints [this function])
  (get-entity-unique-constraints [this]))

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
    "Replaces entity in model with replacement and reconnects all previous connections")
  (get-entity-relations
    [this entity]
    "Returns all relations for given entity with the input entity always in :from field")
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
    "Reconciles this with the given model, cascading to relations and entities"))

(defprotocol DatasetProtocol
  (deploy!
    [this version]
    "Deploys dataset version")
  (preview-model
    [this version]
    "Global model as it would stand after deploying `version`; nothing is written.")
  (recall!
    [this version]
    "Deletes a specific deployed dataset version, rolling back when it is the most recent.")
  (destroy!
    [this dataset]
    "Removes ALL dataset versions and all dataset data, DB included.")
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
    "MANUAL RECOVERY ONLY — reads model from __deploy_history; use reload for normal bootstrap.")
  (backup
    [this options]
    "Backups dataset for given target based on provided options"))

(defprotocol ERDModelProjectionProtocol
  (added? [this] "Returns true if this is added or false otherwise")
  (removed? [this] "Returns true if this is removed or false otherwise")
  (diff? [this] "Returns true if this has diff or false otherwise")
  (diff [this] "Returns diff content")
  (mark-added [this] "Marks this as added")
  (mark-removed [this] "Marks this as removed")
  (mark-diff [this diff] "Adds diff content")
  (suppress [this] "Returns this before projection")
  (project
    [this that]
    "Returns projection of this on that, marking nested values with added?/removed?/diff/active")
  (clean-projection-meta [this] "Returns this stripped of projection metadata"))

;;; Core Records

(defrecord ERDRelation [euuid xid from to from-label to-label cardinality path configuration active claimed-by])
(defrecord NewERDRelation [euuid xid entity type])
(defrecord ERDEntityAttribute [euuid xid seq name constraint type configuration active])

;;; Attribute Name Validation

(defn find-attribute-by-normalized-name
  "Finds an attribute whose name matches (case-insensitive). Returns attribute
   or nil."
  [attributes name]
  (let [normalized (str/lower-case name)]
    (some #(when (= normalized (str/lower-case (:name %))) %) attributes)))

(defn check-attribute-name-conflict!
  "Throws if attribute name conflicts with existing attribute (different ID,
   same name ignoring case)."
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

(def unique-constraint?
  "Attribute `:constraint` values that put the attribute in a UNIQUE group."
  #{"unique" "unique+mandatory"})

(def mandatory-constraint?
  "Attribute `:constraint` values that drive a NOT NULL column."
  #{"mandatory" "unique+mandatory"})

(defrecord ERDEntity [euuid xid position width height name attributes type configuration clone original active claimed-by]
  EntityConstraintProtocol
  (set-entity-unique-constraints [this constraints]
    (assoc-in this [:configuration :constraints :unique] constraints))
  (update-entity-unique-constraints [this f]
    (update-in this [:configuration :constraints :unique] f))
  (get-entity-unique-constraints [this]
    ;; all-or-nothing: never narrow a unique group, drop it entirely if any
    ;; member is inactive
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
          attribute (if (id/extract attribute) attribute (merge attribute (id/new-model-node-id)))
          attribute-id (id/extract attribute)
          _ (check-attribute-name-conflict! attributes
                                            (assoc attribute (id/key) attribute-id))
          entity (update this :attributes (fnil conj [])
                         (assoc attribute
                                (id/key) attribute-id
                                :seq (count attributes)))]
      (if (unique-constraint? (:constraint attribute))
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
        (let [other-attributes (vec (concat (subvec attributes 0 p)
                                            (subvec attributes (inc p))))
              _ (check-attribute-name-conflict! other-attributes attribute)
              {pt :constraint} (get attributes p)
              entity (assoc-in this [:attributes p] attribute)]
          (cond
            (and (unique-constraint? pt) (not (unique-constraint? ct)))
            (update-entity-unique-constraints
             entity
             (fn [constraints]
               (mapv #(vec (remove #{id} %)) constraints)))
            (and (unique-constraint? ct) (not (unique-constraint? pt)))
            (update-entity-unique-constraints
             entity
             (fnil #(update % 0 conj id) [[]]))
            :else entity)))))
  (update-attribute [{:keys [attributes]
                      :as this} id f]
    (if-let [{pt :constraint
              :as attribute} (some #(when (= id (id/extract %)) %) attributes)]
      (let [{ct :constraint
             :as attribute'} (f attribute)
            entity (set-attribute this attribute')]
        (cond
          (and (unique-constraint? pt) (not (unique-constraint? ct)))
          (update-entity-unique-constraints
           entity
           (fn [constraints]
             (mapv #(vec (remove #{id} %)) constraints)))
          (and (unique-constraint? ct) (not (unique-constraint? pt)))
          (update-entity-unique-constraints
           entity
           (fnil #(update % 0 conj id) [[]]))
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

(defrecord ERDModel [id-key entities relations configuration clones version])

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
  "Returns entity relations with every relation inverted to be outgoing from
   entity."
  ([model entity]
   (direct-relations-from entity (get-entity-relations model entity)))
  ([model entity entity']
   (direct-relations-from entity (get-relations-between model entity entity'))))

;;; Model Operations

(defn merge-entity-attributes
  "Accumulates all historical attributes; the newer entity decides each
   attribute's :active."
  [entity1 entity2]
  (let [attrs1 (or (:attributes entity1) [])
        attrs2 (or (:attributes entity2) [])
        attrs1-by-id (into {} (map (juxt id/extract identity) attrs1))
        attrs2-by-id (into {} (map (juxt id/extract identity) attrs2))
        all-attr-uuids (clojure.set/union (set (keys attrs1-by-id))
                                          (set (keys attrs2-by-id)))
        ;; entity2 owns its own :active; forcing true here would resurrect
        ;; soft-deleted fields
        merged-attrs (vec
                      (for [attr-uuid all-attr-uuids]
                        (if-let [attr2 (get attrs2-by-id attr-uuid)]
                          (assoc attr2 :active (not (false? (:active attr2))))
                          (assoc (get attrs1-by-id attr-uuid) :active false))))]
    (assoc entity2 :attributes merged-attrs)))

(defn union-guards
  "Guards from `guard-colls` in first-seen order; a later collection's guard
   replaces an earlier one carrying the same :id."
  [guard-colls]
  (let [guards (apply concat guard-colls)
        latest (reduce (fn [m g] (cond-> m (:id g) (assoc (:id g) g))) {} guards)]
    (second
     (reduce (fn [[seen out] g]
               (if (and (:id g) (contains? seen (:id g)))
                 [seen out]
                 [(conj seen (:id g)) (conj out (get latest (:id g) g))]))
             [#{} []]
             guards))))

(defn merge-entity-rls
  "Merges RLS config: only a model with RLS on contributes, guards union by :id."
  [entity1 entity2]
  (let [rls1 (get-in entity1 [:configuration :rls])
        rls2 (get-in entity2 [:configuration :rls])]
    (if (or rls1 rls2)
      (let [contributing (filterv :enabled [rls1 rls2])]
        (assoc-in entity2 [:configuration :rls]
                  {:enabled (boolean (seq contributing))
                   :guards (union-guards (map :guards contributing))}))
      entity2)))

(defn reconcile-rls-guards
  "Replaces every entity's RLS with what the latest deployed version of each
   dataset declares — a model with RLS off contributes nothing. No-op when
   `models` is empty."
  [model models]
  (let [declared (reduce
                  (fn [acc m]
                    (reduce-kv
                     (fn [a entity-id entity]
                       (if-let [rls (get-in entity [:configuration :rls])]
                         (cond-> a
                           (:enabled rls)
                           (update entity-id (fnil into []) (:guards rls)))
                         a))
                     acc
                     (:entities m)))
                  {}
                  models)]
    (if (empty? models)
      model
      (update model :entities
              (fn [entities]
                (reduce-kv
                 (fn [acc entity-id entity]
                   (assoc acc entity-id
                          (cond-> entity
                            (get-in entity [:configuration :rls])
                            (assoc-in [:configuration :rls]
                                      {:enabled (contains? declared entity-id)
                                       :guards (union-guards
                                                [(get declared entity-id [])])}))))
                 (empty entities)
                 entities))))))

(defn join-models [model1 model2]
  (->
   model1
   (update :configuration deep-merge (:configuration model2))
   (update :clones deep-merge (:clones model2))
   (as-> joined-model
         (reduce
          (fn [m entity]
            (let [id (id/extract entity)
                  entity1 (get-entity model1 id)
                  entity2 (get-entity model2 id)
                  claims-1 (get entity1 :claimed-by #{})
                  claims-2 (get entity2 :claimed-by #{})
                  claims (clojure.set/union claims-1 claims-2)
                  entity-active? (some? entity2)
                  ;; RLS merge must run last so guard-union layers on the
                  ;; deep-merged config
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
     (reduce
      (fn [m relation]
        (let [id (id/extract relation)
              relation1 (get-relation model1 id)
              relation2 (get-relation model2 id)
              claims-1 (get relation1 :claimed-by #{})
              claims-2 (get relation2 :claimed-by #{})
              claims (clojure.set/union claims-1 claims-2)
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

(defn add-claims
  "Adds version-id as a claim to all entities and relations in the provided
   model"
  ([model version-id]
   (letfn [(add-claim [model object-id]
             (cond
               (get-in model [:entities object-id])
               (update-in model [:entities object-id :claimed-by]
                          (fnil conj #{}) version-id)
               (get-in model [:relations object-id])
               (update-in model [:relations object-id :claimed-by]
                          (fnil conj #{}) version-id)
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

(defn fold-version
  "The global model as it stands once `model`, claimed by `version-id`, is folded
   in: join, reconcile RLS against `latest-models`, activate with `active?`."
  [global model version-id latest-models active?]
  (-> global
      (join-models (add-claims model version-id))
      (reconcile-rls-guards latest-models)
      (activate-model active?)))

(defn find-exclusive-entities
  "Returns entities that are ONLY claimed by the provided version-uuids"
  [model version-uuids]
  (let [version-set (set version-uuids)]
    (filter
     (fn [entity]
       (let [claims (get entity :claimed-by #{})]
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

(defn new-relation? [r] (boolean (:added? (projection-data r))))
(defn relation-has-diff? [r] (some? (:diff (projection-data r))))

(def relation-changed? (some-fn new-relation? relation-has-diff?))

(defn recursive-relation? [relation]
  (boolean (#{"tree"} (:cardinality relation))))

(defn setup [db]
  db)

;; =============================================================================
;; RBAC Configuration Helpers
;; =============================================================================

(defn rbac-enabled? [entity]
  (get-in entity [:configuration :rbac :enabled] false))

(defn set-rbac-enabled [entity enabled]
  (assoc-in entity [:configuration :rbac :enabled] enabled))

(def attribute-ops
  "Operations an attribute guard can deny, independently."
  [:read :write])

(defn attribute-denied-roles
  "Role xids explicitly denied `op` on this attribute; 1-arity unions across
   ops."
  ([attribute]
   (into #{} cat (vals (get-in attribute [:configuration :rbac :denied-roles]))))
  ([attribute op]
   (get-in attribute [:configuration :rbac :denied-roles op] #{})))

(defn set-attribute-denied-roles [attribute op roles]
  (assoc-in attribute [:configuration :rbac :denied-roles op] roles))

(defn attribute-allows-op?
  "Pure check whether roles may perform op on attribute; applies only when the
   entity's RBAC is enabled."
  [entity attribute op roles]
  (or (not (rbac-enabled? entity))
      (empty? (clojure.set/intersection roles (attribute-denied-roles attribute op)))))

(defn audit-actions [entity]
  (get-in entity [:configuration :audit :actions]))

(defn audit-modified? [entity]
  (contains? (audit-actions entity) :modified))

(defn audit-created? [entity]
  (contains? (audit-actions entity) :created))

(defn audit-ref-attrs
  "Synthetic user-typed attrs (modified_by/created_by) contributed by audit
   config."
  [entity]
  (let [eid (id/extract entity)
        existing (into #{} (map :name) (:attributes entity))]
    (->> (cond-> []
           (audit-modified? entity)
           (conj (merge (id/derive-id eid "modified_by")
                        {:name "modified_by" :type "user" :active true}))
           (audit-created? entity)
           (conj (merge (id/derive-id eid "created_by")
                        {:name "created_by" :type "user" :active true})))
         (remove #(contains? existing (:name %)))
         vec)))

(defn audit-persist?
  "True iff this entity opts into audit-plug persistence."
  [entity]
  (boolean (get-in entity [:configuration :audit/persist])))

(defn set-audit-persist
  "Flips the audit-plug opt-in flag on the entity."
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
  "Index-stable DDL view of unique groups — dead groups are nil'd in place,
   never compacted."
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

(defn path->condition
  "Converts a discovered path to a minimal, id-only condition for storage."
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
  "Checks if a stored condition matches a discovered path by comparing ids."
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
  "Toggles a path condition on a guard, creating it if needed and dropping it if
   empty."
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
  "Converts paths to an id set for duplicate-selection comparison."
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
  "Converts stored conditions to an id set for comparison."
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
  "Checks if a guard's conditions match exactly the given paths."
  [guard paths]
  (= (conditions->id-set (:conditions guard))
     (paths->id-set paths)))

(defn entity-active-attribute?
  "Is attr-id an active attribute of entity? Includes synthetic audit
   who-columns."
  [entity attr-id]
  (boolean
   (some #(and (:active %) (= attr-id (id/extract %)))
         (concat (:attributes entity) (audit-ref-attrs entity)))))

(defn validate-guard-paths
  "Validates guard conditions against current model state — a dangling one drops
   the whole guard at deploy."
  [guard model entity]
  ;; only an EXPLICIT :active false counts as inactive; nil (authored,
  ;; not-yet-deployed) does not
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
  "Removes one condition from a guard, dropping the guard if it was the last."
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
  "Validates unique-key groups; a group is invalid when any member attribute is
   missing or inactive."
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
  "Sweeps the model for rules referencing removed/inactive attributes or
   relations."
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
  "Adds a new RLS guard for the given paths with default READ permission; nil if
   a duplicate."
  [entity paths]
  (let [guards (get-rls-guards entity)
        duplicate? (some #(guard-matches-paths? % paths) guards)]
    (when-not duplicate?
      (let [new-guard {:id (id/generate)
                       :operation #{:read}
                       :conditions (paths->conditions paths)}]
        (add-rls-guard entity new-guard)))))

(defn update-rls-guard-paths
  "Updates an existing guard's paths; nil if it would duplicate another guard."
  [entity guard-id paths]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)
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
  "Returns the IAM entity type keyword (:user/:group/:role) for a given entity
   id."
  [id iam-ids]
  (cond
    (= id (:user iam-ids)) :user
    (= id (:group iam-ids)) :group
    (= id (:role iam-ids)) :role
    :else nil))

(defn discover-ref-paths*
  "Discovers user/group/role-typed attrs as direct :ref paths to IAM entities.
   Audit who-columns excluded — see docs."
  [entity iam-uuids start-idx]
  (let [ref-types #{"user" "group" "role"}
        attributes (or (:attributes entity) [])]
    (->> (filter (fn [attr]
                   (and (:active attr)
                        (contains? ref-types (:type attr))))
                 attributes)
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

(defn discover-relation-paths*
  "BFS for :relation paths (ending at an IAM entity) and :hybrid paths
   (relations then a ref attr)."
  [model entity iam-uuids max-depth start-idx]
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
                               ;; audit who-columns excluded as hybrid endpoints
                               ;; — nearly every entity is audited
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
  "Discovers all :ref/:relation/:hybrid paths from entity to IAM entities,
   sorted cheapest-first."
  ([model entity iam-uuids]
   (discover-paths-to-iam model entity iam-uuids 3))
  ([model entity iam-uuids max-depth]
   (when (and model entity)
     (let [ref-paths (discover-ref-paths* entity iam-uuids 0)
           relation-paths (discover-relation-paths* model entity iam-uuids max-depth (count ref-paths))
           cost (fn [{:keys [type steps]}]
                  (+ (count steps) (if (= :hybrid type) 0.5 0)))]
       (->> (into ref-paths relation-paths)
            (sort-by cost)
            (map-indexed (fn [idx p] (assoc p :id (get-path-label idx))))
            vec)))))

;;; ============================================================
;;; RLS Projection (dual-base projection for deploy drawer)
;;; ============================================================

(defn conditions-equal?
  "Compares conditions as sets, order-insensitive."
  [conds1 conds2]
  (= (set conds1) (set conds2)))

(defn guard-changed?
  "Checks if a guard changed between old and new versions."
  [old-guard new-guard]
  (or (not= (:operation old-guard) (:operation new-guard))
      (not (conditions-equal? (:conditions old-guard) (:conditions new-guard)))))

(defn compute-guard-diff
  "Computes what changed in a guard between old and new versions."
  [old-guard new-guard]
  (cond-> {}
    (not= (:operation old-guard) (:operation new-guard))
    (assoc :operation (:operation old-guard))
    (not (conditions-equal? (:conditions old-guard) (:conditions new-guard)))
    (assoc :conditions (:conditions old-guard))))

(defn project-rls-guards
  "Projects RLS guards old->new with :added?/:removed?/:diff metadata, matched
   by :id."
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
  "True if projected RLS has added/changed guards (removed guards persist from
   other versions)."
  [projected-rls]
  (some (fn [guard]
          (let [proj (:dataset/projection (meta guard))]
            (or (:added? proj) (not-empty (:diff proj)))))
        (:guards projected-rls)))

(defn rls-differs-from-base?
  "True if RLS config differs from base (guards or the enabled flag)."
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
  "Projects an entity with dual bases: structure against global-entity, RLS
   guards against rls-base-entity."
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
        ;; no RLS diff: strip only :rls from the configuration diff, sibling
        ;; keys (:audit, :constraints) must survive
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

(defn project-with-rls-base
  "Projects a model with dual bases: structure against global, RLS guards
   against rls-base; falls back to project when rls-base is nil."
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
