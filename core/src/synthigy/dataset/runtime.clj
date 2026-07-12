(ns synthigy.dataset.runtime
  "Composes the runtime model — the augmented view of the deployed
   model that surfaces identity, audit, and reference-typed attrs as
   first-class entries.

   See memory: project_runtime_model_plan.md

   Three ModelEnhancement implementations live here:
     - UUIDProvider / NanoIDProvider — extended to add xid/euuid attrs.
     - AuditEnhancer (private record)  — adds audit attrs based on
       entity audit configuration.
     - ReferenceEnhancer (private record) — walks attrs and emits
       relations for any whose :type is a reference type.

   Order matters: identity first, then audit (audit attrs land in
   :attributes with :type \"user\"), then reference (sees both authored
   ref-typed attrs and audit's user-typed attrs uniformly)."
  (:require
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id])
  (:import
   [synthigy.dataset.id UUIDProvider NanoIDProvider]))

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- find-entity
  "Look up an entity by either form of its id (UUID or xid string).
   Necessary because `core/reference-entity-uuid` returns whichever form
   the registry holds — that's a UUID under EUUID provider but an xid
   string under XID provider — and entities carry both."
  [model id]
  (or (get (:entities model) id)
      (some #(when (or (= id (:euuid %)) (= id (:xid %))) %)
            (vals (:entities model)))))

(defn- append-attrs
  "Append attrs to entity.attributes, skipping ones whose name is
   already present. Idempotent."
  [entity new-attrs]
  (let [existing (into #{} (map :name) (:attributes entity))
        addins   (remove #(contains? existing (:name %)) new-attrs)]
    (cond-> entity
      (seq addins) (update :attributes (fnil into []) addins))))

(defn- enhance-each-entity [model f]
  (update model :entities
          (fn [es] (into {} (map (fn [[k v]] [k (f v)])) es))))

;; ============================================================================
;; ID Provider — emit xid/euuid attrs on every entity
;; ============================================================================

(defn- system-attrs-for
  "Emit ONLY the active provider's id attr — `xid` under NanoID, `euuid`
   under UUID. A deploy is mono-format (euuid OR xid is first-class per
   deploy); the other id is an internal dual-storage detail. Emitting both
   leaked the non-active id onto `/schema` and `/data` — so project just the
   active `(id/key*)` field here."
  [provider entity]
  (let [eid   (id/extract* provider entity)
        field (name (id/key* provider))]
    [(merge (id/derive* provider eid field)
            {:name field :type "uuid" :active true})]))

(extend-protocol enhance/ModelEnhancement
  UUIDProvider
  (enhance-model [provider model]
    (enhance-each-entity
     model
     #(append-attrs % (system-attrs-for provider %))))

  NanoIDProvider
  (enhance-model [provider model]
    (enhance-each-entity
     model
     #(append-attrs % (system-attrs-for provider %)))))

;; ============================================================================
;; Audit — emit audit attrs based on entity audit configuration
;; ============================================================================

(defn- audit-attrs-for [entity]
  (let [eid       (id/extract entity)
        modified? (core/audit-modified? entity)
        created?  (core/audit-created? entity)]
    (cond-> []
      modified?
      (into [(merge (id/derive-id eid "modified_on")
                    {:name "modified_on" :type "timestamp" :active true})
             (merge (id/derive-id eid "modified_by")
                    {:name "modified_by" :type "user" :active true})])

      created?
      (into [(merge (id/derive-id eid "created_on")
                    {:name "created_on" :type "timestamp" :active true})
             (merge (id/derive-id eid "created_by")
                    {:name "created_by" :type "user" :active true})]))))

(defrecord ^:private AuditEnhancer []
  enhance/ModelEnhancement
  (enhance-model [_ model]
    (enhance-each-entity model #(append-attrs % (audit-attrs-for %)))))

;; ============================================================================
;; Reference — emit relations for ref-typed attrs
;; ============================================================================

(defn- attr->relation
  "Build an ERDRelation from a ref-typed attr. The relation's identity
   is the attr's identity (per plan): :euuid/:xid lifted directly. From
   and To are stored as ids (matching how authored relations store)."
  [from-entity attr to-entity]
  (core/map->ERDRelation
   {:euuid         (:euuid attr)
    :xid           (:xid attr)
    :from          (id/extract from-entity)
    :to            (id/extract to-entity)
    :from-label    nil
    :to-label      (:name attr)
    :cardinality   "m2o"
    :path          nil
    :configuration nil
    :active        true
    :claimed-by    nil}))

(defn- entity-reference-relations
  "All synthetic relations contributed by `entity` — one per ref-typed
   attr whose target entity exists in the model."
  [model entity]
  (keep
   (fn [attr]
     (when-let [target-id (core/reference-entity-uuid (:type attr))]
       (when-let [to-entity (find-entity model target-id)]
         (attr->relation entity attr to-entity))))
   (:attributes entity)))

(defrecord ^:private ReferenceEnhancer []
  enhance/ModelEnhancement
  (enhance-model [_ model]
    (let [new-rels (mapcat (partial entity-reference-relations model)
                           (vals (:entities model)))]
      (reduce (fn [m rel]
                (let [k (id/extract rel)]
                  ;; Don't overwrite existing relations (idempotent).
                  (if (get-in m [:relations k])
                    m
                    (assoc-in m [:relations k] rel))))
              model
              new-rels))))

;; ============================================================================
;; Composer
;; ============================================================================

(defn build
  "Compose the runtime view of the deployed model: identity attrs +
   audit attrs + reference-typed-attrs-as-relations.

   Idempotent — re-running over an already-runtime model adds nothing
   new (existing names/relations are preserved). Returns nil for nil
   input."
  [model]
  (when model
    (->> model
         (enhance/enhance-model (id/current-provider))
         (enhance/enhance-model (->AuditEnhancer))
         (enhance/enhance-model (->ReferenceEnhancer)))))
