(ns synthigy.dataset.enhance
  "Schema enhancement for access control.

  Provides a mechanism to inject access control conditions into query schemas
  before they are executed. This allows for transparent enforcement of complex
  access patterns like folder-based file permissions."
  (:require
   [clojure.tools.logging :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as access]
   [synthigy.db :refer [*db*]])
  (:import
   [synthigy.db Postgres SQLite]))

;; ============================================================================
;; Audit Enhancement Protocol
;; ============================================================================

(defprotocol AuditEnhancement
  "Infrastructure enhancement for audit tracking across dataset operations.

  Audit is NOT part of the domain model - it's added at infrastructure layer.
  Allows pluggable audit implementations (IAM-based, system-based, etc.)"

  (transform-audit [db tx entities]
    "Phase 1: Add audit columns to database tables (DDL).

    Called after table creation during transform-database.

    Args:
      db       - Database instance
      tx       - Current transaction
      entities - Collection of newly created entities

    Returns: nil (side effects only - executes ALTER TABLE)")

  (augment-schema [db entity]
    "Phase 2: Add audit fields and relations to runtime schema (query building).

    Called during model->schema for each entity.

    Args:
      db     - Database instance
      entity - Entity record from ERD model

    Returns:
      Map with :fields and :relations keys to merge into entity schema:
        {:fields {:modified_by {:key :modified_by
                                :type \"user\"
                                :reference/entity <user-uuid>}
                  :modified_on {:key :modified_on
                                :type \"timestamp\"}}
         :relations {:modified_by {:from <entity-uuid>
                                   :from/field :modified_by
                                   :from/table \"table_name\"
                                   :to <user-uuid>
                                   :to/field :_eid
                                   :to/table \"user\"
                                   :table \"table_name\"
                                   :type :one}}}")

  (audit [db entity-id data tx]
    "Phase 3: Populate audit values during mutations.

    Called during mutation pipeline in enhance-write.

    Args:
      db        - Database instance
      entity-id - Entity UUID being mutated
      data      - Mutation data structure (from analyze-data)
      tx        - Current transaction

    Returns: Enhanced data with audit values populated"))

;; Default no-op implementation for Postgres and SQLite
;; Allows dataset tests to run without IAM loaded
(extend-protocol AuditEnhancement
  Postgres
  (transform-audit [_ _ _] nil)
  (augment-schema [_ _] {})
  (audit [_ _ data _] data)

  SQLite
  (transform-audit [_ _ _] nil)
  (augment-schema [_ _] {})
  (audit [_ _ data _] data))

;; ============================================================================
;; Core Enhancement Protocol
;; ============================================================================

(defmulti schema
  "Enhances a query schema with access control conditions.

   Dispatches on entity UUID to allow entity-specific access patterns.
   Uses dynamic bindings *user*, *roles*, and *groups* from context.

   Args:
     db          - database class
     schema      - The query schema to enhance
     entity-id   - UUID of the entity being queried

   Returns:
     Enhanced schema with additional access control conditions

   Copied from EYWA: neyho.eywa.dataset.enhance/schema"
  (fn dispatch
    ([db _schema selection]
     [(class db) (:entity _schema)]))
  :default ::default)

(defmethod schema ::default
  [_ _schema _]
  ;; By default, no enhancement - rely on standard entity-level access control
  _schema)

(defmulti args
  "Enhances query arguments with access control conditions at any schema depth.
   Called during search-stack-args processing.

   Args:
     db        - Database instance
     schema    - Current schema node being processed
     entity-id - Entity UUID of the current schema node
     path      - Vector of relation keys from root to current node

   Returns:
     Enhanced schema with injected args

   Copied from EYWA: neyho.eywa.dataset.enhance/args"
  (fn [db {entity-id :entity
           :as schema} [stack data]]
    [(class db) entity-id])
  :default ::default)

(defmethod args ::default
  [_ schema current-stack]
  current-stack)

(defn explain-enhancement
  "Returns a human-readable explanation of what enhancements were applied.

   Useful for debugging and audit trails.

   Copied from EYWA: neyho.eywa.dataset.enhance/explain-enhancement"
  [original-schema enhanced-schema entity-id]
  (let [original-relations (set (keys (:relations original-schema)))
        enhanced-relations (set (keys (:relations enhanced-schema)))
        added-relations (clojure.set/difference enhanced-relations original-relations)
        modified-relations (filter #(not= (get-in original-schema [:relations % :args])
                                          (get-in enhanced-schema [:relations % :args]))
                                   original-relations)]
    {:entity entity-id
     :user (access/current-user)
     :roles (access/current-roles)
     :groups (access/current-groups)
     :added-relations added-relations
     :modified-relations modified-relations
     :access-conditions (into {}
                              (for [rel (concat added-relations modified-relations)]
                                [rel (get-in enhanced-schema [:relations rel :args])]))}))

(defn log-enhancement
  "Logs enhancement details for monitoring and debugging.

   Copied from EYWA: neyho.eywa.dataset.enhance/log-enhancement"
  [schema entity-id enhanced-schema]
  (when (not= schema enhanced-schema)
    (log/debugf "Access enhancement applied for entity %s, user %s:\n%s"
                entity-id
                (access/current-user)
                (pr-str (explain-enhancement schema enhanced-schema entity-id)))))

(defn ensure-path
  "Creates a nested structure and optionally sets a value at the end.

   Examples:
   (ensure-path {} [:a :b :c] :leaf-value)
   => {:a {:b {:c :leaf-value}}}

   (ensure-path {} [:folder 0 :selections :read_users 0] {:selections {:euuid nil}})
   => {:folder [{:selections {:read_users [{:selections {:euuid nil}}]}}]}"
  ([m path]
   (ensure-path m path {}))
  ([m path leaf-value]
   (if (empty? path)
     leaf-value
     (let [[k & ks] path
           next-k (first ks)]
       (cond
         ;; Last key in path - set the value
         (empty? ks)
         (assoc m k leaf-value)

         ;; Current key is numeric - ensure we have a vector
         (number? k)
         (let [v (if (vector? m) m [])
               v' (if (< k (count v))
                    v
                    (into v (repeat (inc (- k (count v))) nil)))
               existing (get v' k)
               new-val (ensure-path existing ks leaf-value)]
           (assoc v' k new-val))

         ;; Next key is numeric - current value should be a vector
         (number? next-k)
         (assoc m k (ensure-path (get m k []) ks leaf-value))

         ;; Regular map navigation
         :else
         (assoc m k (ensure-path (get m k {}) ks leaf-value)))))))

(defmulti write
  "Checks if the current user can perform the mutation.

   Args:
     db         - Database instance
     entity-id  - Entity UUID being mutated
     data       - The mutation data
     tx         - Current transaction

   Returns:
     returns updated data ready for storage

   Copied from EYWA: neyho.eywa.dataset.enhance/write"
  (fn [db entity-id data tx]
    [(class db) entity-id])
  :default ::default)

(defmethod write ::default
  [_ _ data _]
  ;; Default allows all mutations (relies on entity-level access)
  data)

(defn apply-write
  "Applies write enhancement to mutation data.

  Copied from EYWA: neyho.eywa.dataset.enhance/apply-write"
  ([entity-id data tx] (apply-write *db* entity-id data tx))
  ([db entity-id data tx]
   (write db entity-id data tx)))

(defn apply-audit
  "Applies write enhancement to mutation data.

  Copied from EYWA: neyho.eywa.dataset.enhance/apply-write"
  ([entity-id data tx] (apply-audit *db* entity-id data tx))
  ([db entity-id data tx]
   (audit db entity-id data tx)))

(defmulti delete
  "Checks if the current user can perform the mutation.

   Args:
     db         - Database instance
     entity-id  - Entity UUID being mutated
     data       - The mutation data
     tx         - Current transaction

   Returns:
     returns updated data ready for storage

   Copied from EYWA: neyho.eywa.dataset.enhance/delete"
  (fn [db entity-id data tx]
    [(class db) entity-id])
  :default ::default)

(defmethod delete ::default
  [_ _ data _]
  ;; Default allows all mutations (relies on entity-level access)
  data)

(defn apply-delete
  "Applies delete enhancement to mutation data.

  Copied from EYWA: neyho.eywa.dataset.enhance/apply-delete"
  ([entity-id data tx] (apply-delete *db* entity-id data tx))
  ([db entity-id data tx]
   (delete db entity-id data tx)))

(defmulti purge
  "Checks if the current user can perform the mutation.

   Args:
     db         - Database instance
     entity-id  - Entity UUID being mutated
     data       - The mutation data
     tx         - Current transaction

   Returns:
     returns updated data ready for storage

   Copied from EYWA: neyho.eywa.dataset.enhance/purge"
  (fn [db entity-id data tx]
    [(class db) entity-id])
  :default ::default)

(defmethod purge ::default
  [_ _ data _]
  ;; Default allows all mutations (relies on entity-level access)
  data)

(defn apply-purge
  "Applies purge enhancement to mutation data.

  Copied from EYWA: neyho.eywa.dataset.enhance/apply-purge"
  ([entity-id data tx] (apply-purge *db* entity-id data tx))
  ([db entity-id data tx]
   (purge db entity-id data tx)))

;; ============================================================================
;; Apply Schema Enhancement
;; ============================================================================

(defn apply-schema
  "Main entry point for schema enhancement.

   Called by the query system to potentially add access control conditions
   to a schema before query execution.

   Args:
     schema    - The query schema to enhance
     selection - Original selection map

   Returns:
     Enhanced schema (or original if no enhancement needed)

   Copied from EYWA: neyho.eywa.dataset.enhance/apply-schema"
  [_schema selection]
  (if (access/current-user)
    (let [enhanced (schema *db* _schema selection)]
      (log-enhancement _schema (:entity _schema) enhanced)
      enhanced)
    _schema))
