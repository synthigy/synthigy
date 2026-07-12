(ns synthigy.dataset.sql.rls
  "RLS compilation - converts UUID-based config to SQL-ready schema.

   This namespace handles the compile-time transformation of RLS guard
   configurations from entity model UUIDs into table/column names that
   can be used at query time for SQL generation.

   Called during model->schema to pre-compile :rls data for each entity.
   Database-agnostic - works for PostgreSQL, SQLite, etc."
  (:require
    [synthigy.log :as log]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming
     :refer [normalize-name
             relation->table-name
             entity->relation-field
             entity->table-name]]))

;; =============================================================================
;; IAM entity identification — id-AGNOSTIC, via the seam.
;;
;; RLS names no representation. It identifies the IAM user/group/role entities
;; through `(id/entity :iam/…)` (resolves to whatever the active provider uses
;; — euuid, xid, a future format) and tracks the graph by `(id/extract node)`.
;; No raw uuid constants, no euuid↔xid round-trip: swap the provider and this
;; works unchanged. Stored conditions reference ids already in the model's
;; key form, so direct `get-entity`/`get-relation` lookups need no translation.
;; =============================================================================


;; =============================================================================
;; RLS Compilation Functions
;; =============================================================================

(defn- compile-ref-condition
  "Compile a :ref condition from UUID to SQL-ready structure.
   Returns {:type :ref :column \"name\" :match :user/:group/:role}
   or nil if attribute not found/invalid."
  [entity condition]
  (let [attr-uuid (:attribute condition)
        ;; Safe lookup — a removed attribute is simply absent (or inactive),
        ;; and a dangling reference must DROP the rule, never throw. `core/
        ;; get-attribute` throws on a missing id, so resolve directly here.
        attr (some #(when (= attr-uuid (id/extract %)) %) (:attributes entity))]
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
  [model step from-entity-id]
  (let [rel-id (:relation-id step)               ; format-decoupled key; value is active id form
        relation (core/get-relation model rel-id)]
    (when (and relation (:active relation))
      (let [{:keys [from to]} relation
            ;; Direction: which endpoint matches the entity we're coming from.
            ;; All ids in the active form via the seam — no representation named.
            forward? (= (id/extract from) from-entity-id)]
        {:table (relation->table-name relation)
         :from-field (entity->relation-field (if forward? from to))
         :to-field (entity->relation-field (if forward? to from))
         :from-entity (if forward? (id/extract from) (id/extract to))
         :to-entity (if forward? (id/extract to) (id/extract from))}))))


(defn- entity-id->match-type
  "Determine match type by comparing an entity id (active form, from
   `id/extract`) against the seam-resolved IAM entity ids. `id/entity`
   returns the id in whatever form the active provider uses, so the
   comparison is representation-agnostic. Returns :user, :group, :role, nil."
  [entity-id]
  (condp = entity-id
    (id/entity :iam/user)       :user
    (id/entity :iam/user-group) :group
    (id/entity :iam/user-role)  :role
    nil))


(defn- compile-relation-condition
  "Compile a :relation condition from UUIDs to SQL-ready structure.
   Returns {:type :relation :match :user/:group/:role :hops [...]}
   or nil if any relation not found."
  [model entity condition]
  (let [steps (:steps condition)]
    (loop [remaining-steps steps
           current-entity-id (id/extract entity)
           compiled-hops []
           final-entity-id nil]
      (if (empty? remaining-steps)
        ;; Done - determine match type from final entity
        (when-let [match (entity-id->match-type final-entity-id)]
          {:type :relation
           :match match
           :hops compiled-hops})
        ;; Process next step
        (let [step (first remaining-steps)
              compiled-step (compile-relation-step model step current-entity-id)]
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
           current-entity-id (id/extract entity)
           compiled-hops []
           final-entity-id (id/extract entity)]
      (if (empty? remaining-steps)
        ;; Done with hops - resolve final attribute. final-entity-id is the
        ;; active id form (same as the model's keys), so look it up directly.
        (let [final-entity (core/get-entity model final-entity-id)
              final-attr (when final-entity (core/get-attribute final-entity attr-uuid))]
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
              compiled-step (compile-relation-step model step current-entity-id)]
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
  "Compile a single guard's conditions.

   A guard's conditions are AND'd together (see rls/guard-to-sql), so the guard
   is only meaningful if EVERY condition resolves. If ANY condition references a
   removed/inactive attribute, relation, or entity, the WHOLE guard is dropped.

   This is the fail-safe choice when a dataset removes a referenced element:
   dropping the rule (an OR-branch) only ever NARROWS access, whereas keeping a
   guard with a missing AND-term would silently BROADEN it. Removing what a rule
   depends on drops the rule; it never weakens it."
  [model entity guard]
  (let [conditions (:conditions guard)
        compiled (map #(compile-rls-condition model entity %) conditions)]
    (if (and (seq conditions) (every? some? compiled))
      {:id (:id guard)
       :operation (:operation guard)
       :conditions (vec compiled)}
      ;; Dropping is the fail-safe choice, but it must be LOUD: a dropped
      ;; :write/:delete guard fail-closes those operations (1=0) and the
      ;; symptom downstream is a write that echoes success yet changes
      ;; nothing — near-undebuggable without this trace.
      (do
        (log/warn {:id ::guard-dropped
                   :data {:action :dropped
                          :subject :rls-guard
                          :entity (:name entity)
                          :guard-id (:id guard)
                          :operation (:operation guard)
                          :conditions (mapv (fn [c cd]
                                              {:type (:type c)
                                               :attribute (:attribute c)
                                               :compiled? (some? cd)})
                                            conditions compiled)}}
                  (str "RLS guard dropped for entity '" (:name entity)
                       "' — a condition references a missing/inactive attribute or "
                       "relation. Operations " (pr-str (:operation guard))
                       " will FAIL CLOSED (silent no-op) unless another guard covers them."))
        nil))))


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
