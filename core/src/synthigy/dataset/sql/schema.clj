(ns synthigy.dataset.sql.schema
  "Deployed runtime-schema cache.

  Holds the single atom that maps entity-id → compiled schema entry.
  Intentionally has no dependency on synthigy.dataset so both
  synthigy.dataset and synthigy.dataset.sql.query can require it
  without creating a circular dependency.")

(defonce ^:private _deployed-schema (atom nil))

(defn set-deployed-schema!
  "Replace the cached schema. Pass nil to clear."
  [schema]
  (reset! _deployed-schema schema))

(defn deployed-schema
  "Returns the currently cached runtime schema, or nil."
  []
  @_deployed-schema)

(defn deployed-schema-entity
  "Returns the compiled schema entry for entity-id.
  Throws if the schema has not been deployed or the entity is absent."
  [entity-id]
  (if-some [entity (get @_deployed-schema entity-id)]
    entity
    (throw
     (ex-info
      (str "Entity " entity-id " not found in deployed schema")
      {:type ::entity-not-found
       :entity entity-id
       :available (keys @_deployed-schema)}))))
