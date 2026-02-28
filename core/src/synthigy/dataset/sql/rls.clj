(ns synthigy.dataset.sql.rls
  "RLS compilation - converts UUID-based config to SQL-ready schema.

   This namespace handles the compile-time transformation of RLS guard
   configurations from entity model UUIDs into table/column names that
   can be used at query time for SQL generation.

   Called during model->schema to pre-compile :rls data for each entity.
   Database-agnostic - works for PostgreSQL, SQLite, etc."
  (:require
    [synthigy.dataset.core :as core]
    [synthigy.dataset.sql.naming
     :refer [normalize-name
             relation->table-name
             entity->relation-field
             entity->table-name]]))


;; =============================================================================
;; IAM Entity UUIDs for Match Type Detection
;; =============================================================================

(def ^:private user-entity-uuid
  "UUID for :iam/user entity"
  #uuid "edcab1db-ee6f-4744-bfea-447828893223")

(def ^:private user-group-entity-uuid
  "UUID for :iam/user-group entity"
  #uuid "95afb558-3d28-45e5-9fbf-a2625afc5675")

(def ^:private user-role-entity-uuid
  "UUID for :iam/user-role entity"
  #uuid "4778f7b1-f946-4cb2-b356-b9cb336b4087")


;; =============================================================================
;; RLS Compilation Functions
;; =============================================================================

(defn- compile-ref-condition
  "Compile a :ref condition from UUID to SQL-ready structure.
   Returns {:type :ref :column \"name\" :match :user/:group/:role}
   or nil if attribute not found/invalid."
  [entity condition]
  (let [attr-uuid (:attribute condition)
        attr (core/get-attribute entity attr-uuid)]
    (when (and attr (:active attr))
      (let [attr-type (:type attr)]
        (when (#{"user" "group" "role"} attr-type)
          {:type :ref
           :column (normalize-name (:name attr))
           :match (keyword attr-type)})))))


(defn- compile-relation-step
  "Compile a single relation step to table/field names.
   Returns {:table \"x\" :from-field \"y\" :to-field \"z\" :from-entity <uuid> :to-entity <uuid>}
   or nil if relation not found."
  [model step from-entity-uuid]
  (let [rel-uuid (:relation-euuid step)
        relation (core/get-relation model rel-uuid)]
    (when (and relation (:active relation))
      (let [{:keys [from to]} relation
            ;; Determine direction based on which entity we're coming from
            forward? (= (:euuid from) from-entity-uuid)]
        {:table (relation->table-name relation)
         :from-field (entity->relation-field (if forward? from to))
         :to-field (entity->relation-field (if forward? to from))
         :from-entity (if forward? (:euuid from) (:euuid to))
         :to-entity (if forward? (:euuid to) (:euuid from))}))))


(defn- entity-uuid->match-type
  "Determine match type from entity UUID.
   Returns :user, :group, :role, or nil."
  [entity-uuid]
  (cond
    (= entity-uuid user-entity-uuid) :user
    (= entity-uuid user-group-entity-uuid) :group
    (= entity-uuid user-role-entity-uuid) :role
    :else nil))


(defn- compile-relation-condition
  "Compile a :relation condition from UUIDs to SQL-ready structure.
   Returns {:type :relation :match :user/:group/:role :hops [...]}
   or nil if any relation not found."
  [model entity condition]
  (let [steps (:steps condition)]
    (loop [remaining-steps steps
           current-entity-uuid (:euuid entity)
           compiled-hops []
           final-entity-uuid nil]
      (if (empty? remaining-steps)
        ;; Done - determine match type from final entity
        (when-let [match (entity-uuid->match-type final-entity-uuid)]
          {:type :relation
           :match match
           :hops compiled-hops})
        ;; Process next step
        (let [step (first remaining-steps)
              compiled-step (compile-relation-step model step current-entity-uuid)]
          (if compiled-step
            (recur (rest remaining-steps)
                   (:to-entity compiled-step)
                   (conj compiled-hops (dissoc compiled-step :from-entity :to-entity))
                   (:to-entity compiled-step))
            ;; Relation not found or inactive - abort
            nil))))))


(defn- compile-hybrid-condition
  "Compile a :hybrid condition (relation hops + final ref attribute).
   Returns {:type :hybrid :match :user/:group/:role :hops [...] :final-table \"x\" :final-column \"y\"}
   or nil if invalid."
  [model entity condition]
  (let [steps (:steps condition)
        attr-uuid (:attribute condition)]
    (loop [remaining-steps steps
           current-entity-uuid (:euuid entity)
           compiled-hops []
           final-entity-uuid (:euuid entity)]
      (if (empty? remaining-steps)
        ;; Done with hops - now resolve final attribute
        (let [final-entity (core/get-entity model final-entity-uuid)
              final-attr (core/get-attribute final-entity attr-uuid)]
          (when (and final-attr (:active final-attr))
            (let [attr-type (:type final-attr)]
              (when (#{"user" "group" "role"} attr-type)
                {:type :hybrid
                 :match (keyword attr-type)
                 :hops compiled-hops
                 :final-table (entity->table-name final-entity)
                 :final-column (normalize-name (:name final-attr))}))))
        ;; Process next step
        (let [step (first remaining-steps)
              compiled-step (compile-relation-step model step current-entity-uuid)]
          (if compiled-step
            (recur (rest remaining-steps)
                   (:to-entity compiled-step)
                   (conj compiled-hops (dissoc compiled-step :from-entity :to-entity))
                   (:to-entity compiled-step))
            ;; Relation not found or inactive - abort
            nil))))))


(defn- compile-rls-condition
  "Compile a single RLS condition based on its type."
  [model entity condition]
  (case (:type condition)
    :ref (compile-ref-condition entity condition)
    :relation (compile-relation-condition model entity condition)
    :hybrid (compile-hybrid-condition model entity condition)
    ;; Unknown type - skip
    nil))


(defn- compile-rls-guard
  "Compile a single guard's conditions."
  [model entity guard]
  (let [compiled-conditions (->> (:conditions guard)
                                 (keep #(compile-rls-condition model entity %))
                                 vec)]
    (when (seq compiled-conditions)
      {:id (:id guard)
       :operation (:operation guard)
       :conditions compiled-conditions})))


(defn compile-entity-rls
  "Compile all RLS configuration for an entity.
   Returns compiled :rls map or nil if disabled/no valid guards."
  [model entity]
  (when (core/rls-enabled? entity)
    (let [guards (core/get-rls-guards entity)
          compiled-guards (->> guards
                               (keep #(compile-rls-guard model entity %))
                               vec)]
      (when (seq compiled-guards)
        {:enabled true
         :guards compiled-guards}))))
