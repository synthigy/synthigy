(ns synthigy.dataset.patch.model
  "Model transformation utilities for EUUID to XID migration.

  This namespace provides functions to:
  1. Transform ERDModel entities/relations from euuid to xid format
  2. Patch-ready functions for upgrading/downgrading stored models
  3. XID migration utilities for data tables

  Usage:
    (require '[synthigy.dataset.patch.model :as model])

    ;; Transform a model in memory
    (model/transform-model my-model :to-xid)

    ;; In patch upgrade (MODIFIES DATABASE)
    (model/upgrade-models-to-xid!)
    (model/downgrade-models-to-euuid!)"
  (:require
    [synthigy.log :as log]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.db.sql :as sql]
    [synthigy.transit :refer [<-transit ->transit]]))


;;; ============================================================================
;;; Deploy drift-stamp (for codegen)
;;; ============================================================================

(defn- ->iso
  "Best-effort ISO-8601 string for a `deployed_on` value across backends
   (PG `Timestamp`/`Date` → instant; SQLite string/epoch → plain str).
   Isolated so a coercion hiccup never nulls the surrounding version info."
  [v]
  (try
    (if (instance? java.util.Date v)
      (str (.toInstant ^java.util.Date v))
      (str v))
    (catch Throwable _ (str v))))

(defn latest-deployed-version-info
  "Lightweight drift-stamp for codegen: a map
   `{:version <name> :version-id <id> :deployed-at <iso-string>}` describing the
   most recently deployed dataset version, or `nil` when none is deployed (or a
   legacy meta-model predates `deployed_on`). Reads `dataset_version` directly —
   cheap, no model rebuild — so it's safe to call from the public discovery doc
   and from `/schema` on every request."
  []
  (try
    (let [id-col (clojure.core/name (id/key))]            ; active id seam: "xid" (or "euuid")
      (when-let [row (first (sql/execute!
                              [(str "SELECT " id-col ", name, deployed_on
                                     FROM dataset_version
                                     WHERE deployed = true
                                     ORDER BY deployed_on DESC NULLS LAST
                                     LIMIT 1")]))]
        {:version     (:name row)
         :version-id  (str (get row (keyword id-col)))
         :deployed-at (some-> (:deployed_on row) ->iso)}))
    (catch Throwable _ nil)))


;;; ============================================================================
;;; Full Model Transformation
;;; ============================================================================

(defn transform-model
  "Transform all entities, clones, and relations in a model.

  Args:
    model     - ERDModel to transform
    direction - :to-xid or :to-euuid

  Returns:
    Transformed model with:
    - All entities/clones/relations having both euuid and xid
    - Map keys updated to target ID format
    - from/to references updated
    - unique constraints updated

  Note: Uses deterministic id/uuid->nanoid for consistency across all models.
        Same euuid always resolves to same xid."
  ([model ->to]
   (transform-model model (id/key) ->to))
  ([original-model ->from ->to]
   (if (= ->from ->to)
     original-model
     (letfn [(->new-id
               [data]
               ((case ->to
                  :xid (comp id/uuid->nanoid :euuid)
                  :euuid (comp id/nanoid->uuid :xid))
                data))
             (->new-bare-id
               ;; Translate a raw id (no surrounding record) between the
               ;; two formats. Pass-through when the id is already in the
               ;; target form or nil.
               [id]
               (cond
                 (nil? id) id
                 (= ->to :xid) (if (uuid? id) (id/uuid->nanoid id) id)
                 (= ->to :euuid) (if (string? id) (id/nanoid->uuid id) id)))
             (transform-rls-condition
               ;; RLS guard conditions reference attributes / relations /
               ;; entities by bare id under `:attribute` and step keys
               ;; `:relation-id` / `:entity-id` (format-DECOUPLED names). None
               ;; carry the surrounding record, so `->new-id` doesn't apply —
               ;; translate field-by-field. This is also the forward-migration
               ;; seam for legacy `:relation-euuid` / `:entity-euuid` step keys:
               ;; read either, write only the decoupled name.
               [condition]
               (cond-> condition
                 (contains? condition :attribute)
                 (update :attribute ->new-bare-id)

                 (contains? condition :steps)
                 (update :steps
                         (fn [steps]
                           (mapv (fn [step]
                                   (-> step
                                       (assoc :relation-id
                                              (->new-bare-id (or (:relation-id step)
                                                                 (:relation-euuid step)))
                                              :entity-id
                                              (->new-bare-id (or (:entity-id step)
                                                                 (:entity-euuid step))))
                                       (dissoc :relation-euuid :entity-euuid)))
                                 steps)))))
             (transform-rls-guards
               [entity]
               (if (seq (get-in entity [:configuration :rls :guards]))
                 (update-in entity [:configuration :rls :guards]
                            (fn [guards]
                              (mapv (fn [guard]
                                      (update guard :conditions
                                              #(mapv transform-rls-condition %)))
                                    guards)))
                 entity))
             (get-entity [{:keys [entities clones]} id]
               (if-let [e (get entities id)]
                 e
                 (when-some [{:keys [entity position]} (get clones id)]
                   (when-some [entity (get entities entity)]
                     (assoc entity
                       ->from id
                       :position position
                       :clone true
                       :original (id/extract entity))))))]

       (as-> [nil original-model] data
         ;; Migrate entities
         (reduce
           (fn [[mapping model] entity]
             (letfn [(find-attribute
                       [id]
                       (some
                         (fn [attribute]
                           (when (= (->from attribute) id)
                             attribute))
                         (:attributes entity)))]
               (let [id (->new-id entity)
                     old-id (->from entity)
                     new-entity (->
                                  entity
                                  (assoc ->to id)
                                  (update-in [:configuration :constraints :unique]
                                             (fn [uniques]
                                               (mapv
                                                 (fn [constraints]
                                                   (mapv (comp ->new-id find-attribute) constraints))
                                                 uniques)))
                                  transform-rls-guards
                                  (update :attributes
                                          (fn [as]
                                            (mapv
                                              (fn [{:keys [type]
                                                    :as attribute}]
                                                (let [new-attribute (assoc attribute ->to (->new-id attribute))]
                                                  (case type
                                                    "enum" (update-in new-attribute [:configuration :values]
                                                                      (fn [values]
                                                                        (mapv
                                                                          (fn [value]
                                                                            (assoc value ->to (->new-id value)))
                                                                          values)))
                                                    new-attribute)))
                                              as))))]
                 (when (nil? old-id)
                   (throw
                     (ex-info "Couldn't get entity original id!"
                              {:entity entity
                               :->from ->from
                               :->to ->to})))
                 [(assoc mapping old-id id)
                  (->
                    model
                    (assoc-in [:entities id] new-entity)
                    (update :entities dissoc old-id))])))
           data
           (core/get-entities (second data)))
         ;; Migrate clones
         (reduce-kv
           (fn [[mapping model] old-id {original-entity-id :entity
                                        :as old-value}]
             (let [original-entity (get-in original-model [:entities original-entity-id])
                   clone-entity (assoc original-entity ->from old-id)
                   clone-new-id (->new-id clone-entity)
                   new-original-entity-id (->new-id original-entity)]
               [(assoc mapping old-id clone-new-id)
                (assoc-in model [:clones clone-new-id] (assoc old-value :entity new-original-entity-id))]))
           (assoc-in data [1 :clones] nil)
           (get-in data [1 :clones]))
         ;;
         (reduce-kv
           (fn [[mapping model] old-relation-id {:keys [from to]
                                                 :as old-relation}]
             (let [from-entity (get-entity original-model from)
                   to-entity (get-entity original-model to)
                   new-relation-id (->new-id old-relation)
                   new-from-id (->new-id from-entity)
                   new-to-id (->new-id to-entity)
                   new-relation (->
                                  old-relation
                                  (assoc
                                    ->to new-relation-id
                                    :from new-from-id
                                    :to new-to-id))]
               (when (some nil? [old-relation-id new-from-id new-to-id])
                 (throw
                   (ex-info "Couldn't get relation original id!"
                            {:relation old-relation
                             :->from ->from
                             :->to ->to})))
               [(assoc mapping old-relation-id new-relation-id)
                (->
                  model
                  (update :relations dissoc old-relation-id)
                  (assoc-in [:relations new-relation-id] new-relation))]))
           data
           (:relations original-model))
         (second data))))))

;;; ============================================================================
;;; Patch Functions (MODIFY DATABASE)
;;; ============================================================================
;;
;; These functions are designed to be called from patcho upgrade/downgrade blocks.
;; They transform ALL deployed models in the database.
;;
;; WARNING: These functions MODIFY the database!
;; Only call them from patch definitions.


(comment
  (def version-euuid #uuid "d908a70f-a1fb-46bd-ac76-801bebe6ceed")
  (def version-euuid #uuid "8996515d-3447-4ac1-8f36-2c874967913b")
  (def direction :xid)
  (def version-record
    (sql/execute-one!
      ["SELECT euuid, name, model
                           FROM dataset_version
                           WHERE euuid = ?"
       version-euuid]))
  (def model
    (when-let [m (:model version-record)]
      (<-transit m))))

(defn transform-stored-model!
  "Transform a single stored model and save it back to the database.

  Args:
    version-euuid - UUID of the dataset_version record
    direction     - :to-xid or :to-euuid

  Returns:
    Map with :success, :version-name, :entities-count

  WARNING: This MODIFIES the database!"
  [version-euuid direction]
  (log/info {:id ::transforming-model
             :data {:version-euuid version-euuid :direction direction}}
            "Transforming model")
  (let [;; Load the version record
        version-record (sql/execute-one!
                         ["SELECT euuid, name, model
                           FROM dataset_version
                           WHERE euuid = ?"
                          version-euuid])
        model (when (:model version-record)
                (<-transit (:model version-record)))]
    (if-not model
      (do
        (log/warn {:id ::model-not-found
                   :data {:version-euuid version-euuid}}
                  "No model found for version")
        {:success false
         :error "No model found"})
      (let [;; Transform the model
            transformed (transform-model model direction)
            ;; Serialize back to Transit
            transit-data (->transit transformed)]
        ;; Update the database
        (sql/execute!
          ["UPDATE dataset_version SET model = ? WHERE euuid = ?"
           transit-data version-euuid])
        (log/info {:id ::model-transformed
                   :data {:version-name (:name version-record)
                          :entity-count (count (:entities transformed))
                          :direction direction}}
                  "Transformed model")
        {:success true
         :version-name (:name version-record)
         :entity-count (count (:entities transformed))
         :direction direction}))))

(defn transform-stored-models!
  "Transform ALL deployed models in the database.

  Args:
    direction - :to-xid or :to-euuid

  Returns:
    Map with :total, :success-count, :failures

  Note: Processes models in deployment order (oldest first) to preserve
        rebuild logic consistency. The deterministic id/uuid->nanoid ensures
        that same euuid always resolves to same xid across all models.

  WARNING: This MODIFIES the database!
  Should be called from patcho patch definitions."
  [direction]
  (log/info {:id ::transforming-all-models
             :data {:direction direction}}
            "Transforming all stored models")
  (let [versions (sql/execute!
                   ["SELECT euuid, name, deployed_on
                     FROM dataset_version
                     WHERE deployed = true
                     ORDER BY deployed_on ASC NULLS LAST"])
        results (atom [])]
    (doseq [{:keys [euuid name]} versions]
      (try
        (let [result (transform-stored-model! euuid direction)]
          (swap! results conj result))
        (catch Exception e
          (log/error! {:id ::transform-model-failed
                       :msg "Failed to transform stored model"
                       :data {:version-name name}}
                      e)
          (swap! results conj
                 {:success false
                  :version-name name
                  :error (.getMessage e)}))))
    (let [successes (filter :success @results)
          failures (remove :success @results)]
      (log/info {:id ::transform-all-complete
                 :data {:total (count @results)
                        :successful (count successes)
                        :failed (count failures)}}
                "Bulk model transformation complete")
      (when (seq failures)
        (log/warn {:id ::transform-failures
                   :data {:failures (mapv :version-name failures)}}
                  "Some models failed to transform"))
      {:total (count @results)
       :success-count (count successes)
       :failures failures
       :results @results})))
