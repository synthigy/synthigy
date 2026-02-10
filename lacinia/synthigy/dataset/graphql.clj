(ns synthigy.dataset.graphql
  "GraphQL resolvers for dataset operations

  Copied from EYWA: neyho.eywa.dataset.graphql"
  (:require
   [clojure.core.async :as async]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [com.walmartlabs.lacinia.executor :as executor]
   [com.walmartlabs.lacinia.resolve :as resolve]
   [synthigy.dataset
    :refer [publisher
            subscription
            deployed-model]]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.core :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :refer [normalize-name]]
   [synthigy.db :as db :refer [*db*]]
   [synthigy.dataset.access :as access]))

(defn- protect-dataset
  "Filters the dataset model based on IAM permissions.

  Removes entities and relations that the current user cannot access.

  Copied from EYWA: neyho.eywa.dataset.graphql/protect-dataset"
  [model]
  (as-> model m
    (reduce
     (fn [m entity]
       (if (access/entity-allows? (id/extract entity) #{:read :write})
         m
         (dataset/remove-entity m entity)))
     m
     (dataset/get-entities m))
    (reduce
     (fn [m {from :from
             to :to
             :as relation}]
       (let [from-id (id/extract from)
             to-id (id/extract to)
             relation-id (id/extract relation)]
         (if (or
              (access/relation-allows? relation-id [from-id to-id] #{:read :write})
              (access/relation-allows? relation-id [to-id from-id] #{:read :write}))
           m
           (dataset/remove-relation m relation))))
     m
     (dataset/get-relations m))))

(defn get-deployed-model
  "Returns the currently deployed dataset model as Transit.

  Filters out entities/relations based on IAM permissions.

  Copied from EYWA: neyho.eywa.dataset.graphql/get-deployed-model"
  [_ _ _]
  (protect-dataset (deployed-model)))

(defn on-deploy
  "Subscription resolver for dataset deployment events.

  Returns a stream function for Lacinia subscriptions that sends the
  deployed model whenever it changes.

  Copied from EYWA: neyho.eywa.dataset.graphql/on-deploy"
  [{:keys [username]} _ upstream]
  (let [sub (async/chan)]
    (async/sub publisher :refreshedGlobalDataset sub)
    (async/go-loop [{:keys [data]
                     :as published} (async/<! sub)]
      (when published
        (log/tracef "Sending update of global model to user %s" username)
        (upstream data)
        (recur (async/<! sub))))
    (let [model (dataset/get-model *db*)
          protected-model (protect-dataset model)]
      (upstream
       {:name "Global"
        :model protected-model}))
    (fn []
      (async/unsub publisher :refreshedGlobalDataset sub)
      (async/close! sub))))

(defn on-delta
  "Subscription resolver for dataset delta events.

  Returns a stream function for Lacinia subscriptions that sends deltas
  (changes) for specific entities/relations.

  Copied from EYWA: neyho.eywa.dataset.graphql/on-delta"
  [{:keys [username]} {:keys [elements]} upstream]
  (let [sub (async/chan)]
    (doseq [element elements]
      (log/debugf "[DELTA SUBSCRIPTION::%s] Subscribing to dataset delta channel for: %s" username element)
      (async/sub dataset/*delta-publisher* element sub))
    ;; Start idle service that will listen on delta changes
    (async/go-loop
     [{:keys [element]
       :as data} (async/<! sub)]
      (log/debugf "[DELTA SUBSCRIPTION::%s] Received something at delta channel" username)
      (when data
        (when (or (access/entity-allows? element #{:read :write})
                  (access/relation-allows? element #{:read :write}))
          (upstream data))
        (recur (async/<! sub))))
    (fn []
      (doseq [element elements]
        (log/debugf "[DELTA SUBSCRIPTION::%s] Removing subscription to dataset delta channel for: %s" username element)
        (async/unsub dataset/*delta-publisher* element sub))
      (async/close! sub))))

(def dataset-txt-prelude
  "# Synthigy Dataset Schema

This document describes the Synthigy data model and how to interact with it through GraphQL operations.

## Schema Notation Guide

### Attribute Constraints
- `#` = **Unique** - Field must be unique across all records
- `*` = **Mandatory** - Field is required
- `o` = **Optional** - Field can be null

### Attribute Types
- Basic: `string`, `int`, `float`, `boolean`, `timestamp`
- Complex: `json`, `transit`, `uuid`, `avatar`
- Security: `hashed`, `encrypted`
- Special: `user`, `group`, `role`, `timeperiod`, `currency`
- Enum: `enum{VALUE1,VALUE2,...}` - Enumerated values

### Relationship Format
```
[Source Entity]---[relation name][cardinality]--->[Target Entity]
```

### Cardinality Types
- `o2o` = One-to-One
- `o2m` = One-to-Many
- `m2o` = Many-to-One
- `m2m` = Many-to-Many
- `tree` = Hierarchical/Self-referencing

## GraphQL Operation Patterns

Each entity automatically generates these operations:

### Queries
- `get{Entity}(euuid: \" ... \")` - Get single record by ID
- `search{Entity}(_where: {...}, _limit: 10, _offset: 0, _order_by: {...})` - Search with filters
- `search{Entity}TreeBy{Field}` - Hierarchical queries for tree relationships

### Mutations
- `sync{Entity}(data: {...})` - Upsert SINGLE entity (create or update)
- `sync{Entity}List(data: [{...}, {...}])` - Upsert MULTIPLE entities
- `stack{Entity}(data: {...})` - Create SINGLE new entity
- `stack{Entity}List(data: [{...}, {...}])` - Create MULTIPLE new entities
- `slice{Entity}(euuid: \" ... \", data: {...})` - Partial update
- `delete{Entity}(euuid: \" ... \")` - Soft delete (returns boolean)
- `purge{Entity}(euuid: \" ... \")` - Hard delete (DANGEROUS - permanent)

## Quick Examples

### From Entity to Query
```
Entity: User
Attributes:
#  Name[string]
o  Active[boolean]
```

Generates:
```graphql
query {
  searchUser(_where: {active: {_eq: true}}, _limit: 10) {
    euuid
    name
    active
  }
}
```

### From Relationship to Nested Query
```
User---roles[m2m]--->User Role
```

Enables:
```graphql
query {
  getUser(euuid: \" ... \") {
    name
    roles {  # Follow the relationship
      euuid
      name
      active
    }
  }
}
```

### From Attributes to Mutation
```graphql
mutation {
  syncUser(data: {
    name: \"john_doe\",      # Unique field
    active: true           # Optional field
  }) {
    euuid
    name
  }
}
```

## Important Notes

1. **Field Names**: GraphQL uses snake_case (e.g., `name`, `active`) even if shown with capitals in schema
2. **Enum Values**: Use unquoted values (e.g., `type: ACCESS` not `type: \"ACCESS\"`)
3. **Array Operations**: Always use `List` suffix for multiple entities:
   - `syncUserList` for updating multiple users
   - `stackProjectList` for creating multiple projects
4. **Unique Fields**: Can be used as identifiers in sync operations instead of euuid
")

(defn get-dataset-txt
  "Generates a human-readable text representation of the dataset schema.

  Returns a markdown-style document describing all entities, attributes,
  and relations in the deployed model.

  Copied from EYWA: neyho.eywa.dataset.graphql/get-dataset-txt"
  ([] (get-dataset-txt (protect-dataset (deployed-model))))
  ([model]
   (let [entities (dataset/get-entities model)
         txt-entities (reduce
                       (fn [result {:keys [attributes]
                                    entity-name :name
                                    :as entity}]
                         (let [relations (dataset/focus-entity-relations model entity)]
                           (conj result
                                 (str "Entity: " entity-name \newline
                                      "Attributes:" \newline
                                      (str/join "\n" (map (fn [{:keys [constraint name type configuration]}]
                                                            (str
                                                             (case constraint
                                                               "optional" "o"
                                                               "mandatory" "*"
                                                               "unique" "#"
                                                               "o")
                                                             "\t"
                                                             name
                                                             \[
                                                             type
                                                             \]
                                                             (when (= type "enum")
                                                               (str
                                                                \{
                                                                (str/join ","
                                                                          (keep
                                                                           (fn [{:keys [active name]}]
                                                                             (when active name))
                                                                           (:values configuration)))
                                                                \}))))
                                                          attributes))
                                      (when (not-empty relations)
                                        (str
                                         "\nRelations:\n"
                                         (str/join
                                          "\n"
                                          (keep
                                           (fn [{:keys [from to-label to cardinality]}]
                                             (when to-label
                                               (str
                                                (:name from)
                                                "---" (normalize-name to-label) \[ cardinality \] "--->"
                                                (:name to))))
                                           relations))))))))
                       []
                       entities)]
     (str dataset-txt-prelude "\n\n" (str/join "\n----\n" txt-entities)))))

(defn get-deployed-model-document
  "Returns the currently deployed dataset model as a string document.

  Generates a human-readable markdown representation of the schema.

  Copied from EYWA: neyho.eywa.dataset.graphql/get-deployed-model-document"
  [_ _ _]
  (get-dataset-txt))

;;; ============================================================================
;;; GraphQL Mutation Resolvers
;;; ============================================================================

(defn deploy-dataset
  "GraphQL resolver: Deploys a new dataset version

  Args:
    context - GraphQL context
    version - DatasetVersionInput with model data
    value   - Parent value (unused)

  Returns the deployed DatasetVersion

  Copied from EYWA: neyho.eywa.dataset/deploy-dataset"
  ([model] (deploy-dataset nil model nil))
  ([context model] (deploy-dataset context model nil))
  ([context {version :version} _]
   (let [selection (executor/selections-tree context)]
     (log/infof "User %s deploying dataset %s@%s"
                (:name (access/current-user)) (-> version :dataset :name) (:name version))
     (try
       (let [deployed (core/deploy! *db* version)
             deployed-id (id/extract deployed)]
         (async/put! subscription
                     {:topic :refreshedGlobalDataset
                      :data {:name "Global"
                             :model (deployed-model)}})
         (when (not-empty selection)
           (db/get-entity *db* :dataset/version {(id/key) deployed-id} selection)))
       (catch Throwable e
         (log/errorf e "Couldn't deploy dataset version")
         (resolve/resolve-as nil {:message (.getMessage e)}))))))

(defn import-dataset
  "GraphQL resolver: Imports a dataset from Transit format

  Args:
    context - GraphQL context
    dataset - Transit encoded dataset model
    value   - Parent value (unused)

  Returns the imported DatasetVersion

  Import is just an alias for deployment with different argument structure.

  Copied from EYWA: neyho.eywa.dataset/import-dataset"
  [context {dataset :dataset} _]
  (deploy-dataset context {:version dataset} nil))

(comment
  (def model (deployed-model))
  (def entity (dataset/get-entity model #uuid "63b2e70a-2162-423a-be36-4909d7831605"))
  (def relations (dataset/focus-entity-relations model entity))
  (spit "dataset_schema.md" (get-dataset-txt)))
