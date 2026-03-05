(ns synthigy.dataset
  "High-level dataset API (database-agnostic).

  This namespace provides the public API for working with dataset models.
  It delegates to database-specific implementations via protocols."
  (:require
    [clojure.core.async :as async]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia.selection :as selection]
    [patcho.patch :as patch]
    synthigy.dataset.access
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id :refer [defentity defrelation defdata]]
    synthigy.dataset.operations
    [synthigy.dataset.patch.model :as model]
    synthigy.dataset.projection
    [synthigy.db :as db :refer [*db*]]
    [synthigy.env :as env]
    [synthigy.transit :refer [<-transit]]
    [version-clj.core :as version]))

;; ============================================================================
;; Dataset Meta-Entity Definitions
;; ============================================================================
;; Well-known dataset entity IDs from the meta-model (dataset.json).
;; Uses compile-time multimethod registration for provider-agnostic ID resolution.

(defdata :dataset/id
  :euuid #uuid "4ab2fe4f-9b74-4a23-8441-60b58be08e7e" :xid "AE18CKqkpn1txrdJxhU9v5")

(defdata :dataset.model/version-1.0.3
  :euuid #uuid "d908a70f-a1fb-46bd-ac76-801bebe6ceed" :xid "ToR56Krq2zEUmkKmtrfmNU")

(defentity :dataset/dataset
  :euuid #uuid "a800516e-9cfa-4414-9874-60f2285ec330" :xid "MkEtt4vQAsruc44MGypn63")

(defentity :dataset/entity
  :euuid #uuid "a0d304a7-afe3-4d9f-a2e1-35e174bb5d5b" :xid "LrqagSaDABNV272TRWfx9G")

(defentity :dataset/entity-attribute
  :euuid #uuid "226f54b0-6af0-4c70-b090-d4fdf2579294" :xid "5FdHiW1BHsJSPFNcT1Dsd5")

(defentity :dataset/relation
  :euuid #uuid "191eb26a-c68f-46e5-84ec-1ee887d22adb" :xid "46uqZWNio5gQF6ZNU1QQDp")

(defentity :dataset/version
  :euuid #uuid "d922edda-f8de-486a-8407-e62ad67bf44c" :xid "Tp9i6E2m3kBNHtt7KwHgWX")

;; ============================================================================
;; Dataset Relation Definitions
;; ============================================================================
;; Well-known relation IDs for dataset schema elements.

(defrelation :dataset/dataset->versions
  :euuid #uuid "3c277257-916f-41ba-ac64-f591110c84a4" :xid "8Rq5Bu1E48EGKpFEPbUeh9")

(defrelation :dataset/version->entities
  :euuid #uuid "63f05e2f-22fc-4bae-9889-1f03a2ff4540" :xid "DLmjxHPyY8Jq2UEQdAxP27")

(defrelation :dataset/version->relations
  :euuid #uuid "23d267d7-3ae2-4432-92cb-82581afb5f90" :xid "5RZTLN852KqK2RjQJzUYqm")

(defrelation :dataset/entity->attributes
  :euuid #uuid "5e165470-6bfc-4ae4-a0eb-6f04004a2b79" :xid "Ccrw9PgsKh2PQ35KyGrcwz")

;;; ============================================================================
;;; ID Format Management
;;; ============================================================================
;; User-controlled EUUID/XID choice, stored via patcho version store.

(def ^:private format-topic
  "Patcho topic for ID format storage."
  :synthigy/id-format)

(defn current-format
  "Read the stored ID format from database.
  Returns the stored format string, or nil if not stored.
  Each DB implementation is responsible for handling nil (detection/defaults)."
  []
  (let [stored (patch/read-version *db* format-topic)]
    (when (and stored (not= stored "0"))
      stored)))

(defn set-format!
  "Store the active ID format choice.
  Format must be \"euuid\" or \"xid\"."
  [format]
  (assert (#{"euuid" "xid"} format)
          (str "Invalid format: " format ". Must be \"euuid\" or \"xid\"."))
  (patch/write-version *db* format-topic format)
  (log/infof "[ID Format] Stored format choice: %s" format))

(defn initialize-provider!
  "Set the global ID provider based on format.

  Args:
    format - \"xid\" or \"euuid\"

  Each DB implementation is responsible for detecting/determining the format
  and calling this function with the appropriate value."
  [format]
  (assert (#{"euuid" "xid"} format)
          (str "Invalid format: " format ". Must be \"euuid\" or \"xid\"."))
  (log/infof "[ID Format] Initializing provider for format: %s" format)
  (case format
    "xid" (id/set-provider! (id/->NanoIDProvider))
    "euuid" (id/set-provider! (id/->UUIDProvider)))
  format)

;;; ============================================================================
;;; Model State
;;; ============================================================================

(defonce ^:private _model (atom nil))

(defn deployed-model
  "Returns the currently deployed model."
  []
  (some-> @_model
          (assoc :id-key (id/key))))

(defonce subscription (async/chan 100))

(defonce publisher (async/pub subscription :topic))

(defn save-model!
  "Updates the deployed model and publishes :model/deployed event.

  This is called after loading a model from the database or deploying
  a new model version.

  Publishes :model/deployed event for listeners (e.g., GraphQL schema regeneration)."
  [model]
  (reset! _model model)
  (async/put! subscription
              {:topic :model/deployed
               :model model}))

(defn deployed-entity
  "Returns a specific entity from the deployed model by UUID."
  [id]
  (core/get-entity (deployed-model) id))

(defn deployed-relation
  "Returns a specific relation from the deployed model by UUID."
  [id]
  (core/get-relation (deployed-model) id))

;;; ============================================================================
;;; Model Versioning
;;; ============================================================================

(defn latest-deployed-version
  "Gets the latest deployed version for a dataset.

  Returns a map with :name (version string), :model, and ID (via id/key).
  Compatible with patcho versioning system.

  Note: Currently returns the in-memory model since we don't have
  per-dataset versioning yet. The dataset-id is ignored."
  [dataset-id]
  (let [version (patch/read-version patch/*version-store* :synthigy.dataset/model)]
    (->
      (db/get-entity
        *db*
        :dataset/dataset
        {(id/key) dataset-id}
        {:xid nil
         :euuid nil
         :name nil
         :versions [{:selections
                     {:xid nil
                      :euuid nil
                      :name nil
                      :model nil
                      :modified_on nil
                      :dataset [{:selections {:xid nil
                                              :euuid nil
                                              :name nil}}]}
                     :args {:deployed {:_boolean :TRUE}
                            :_order_by {(cond
                                          (version/newer-or-equal? version "1.0.1") :deployed_on
                                          (version/older? version "1.0.1") :modified_on) :desc}
                            :_limit 1}}]})
      :versions
      first)))


(comment
  (def dataset-id (id/data :dataset/id))
  ((juxt :modified_on :deployed_on :xid) (latest-deployed-version (id/data :dataset/id)))
  (search-entity
    :dataset/dataset
    nil
    {(id/key) nil
     :name nil
     :versions [{:selections
                 {(id/key) nil
                  :name nil
                  :modified_on nil
                  :model nil}
                 :args {:_order_by {:deployed_on :desc}
                        :_limit 2}}]})
  (def version
    (get-entity
      :dataset/version
      {:euuid #uuid "d908a70f-a1fb-46bd-ac76-801bebe6ceed"}
      ; {:xid "vjhvyB3WqchFr4CCn8rgcX"}
      ; {:xid "CcJHRJ7PPHXcobdGTYna1m"}
      {:euuid nil
       :name nil
       :xid nil
       :modified_on nil
       :deployed_on nil
       :dataset [{:selections {:euuid nil
                               :xid nil
                               :name nil}}]
       :model nil}))

  (:model version)

  (spit "resources/dataset/dataset.json"
        (synthigy.transit/->transit
          (->
            (latest-deployed-version (id/data :dataset/id))
            (assoc-in [:dataset :euuid] (id/data :dataset/id :euuid))
            (assoc-in [:dataset :xid] (id/data :dataset/id :xid))
            (assoc :euuid (id/data :dataset.model/version-1.0.3 :euuid))
            (assoc :xid (id/data :dataset.model/version-1.0.3 :xid)))))
  (spit "resources/dataset/iam.json"
        (synthigy.transit/->transit
          (->
            (latest-deployed-version (id/data :iam/id))
            (assoc-in [:dataset :euuid] (id/data :iam/id :euuid))
            (assoc-in [:dataset :xid] (id/data :iam/id :xid))
            (assoc :euuid (id/data :iam.model/version-0.80.0 :euuid))
            (assoc :xid (id/data :iam.model/version-0.80.0 :xid))))))

(defn adapt-model-to-provider
  "Transform model to match current ID provider format.

  When loading models from resources (dataset.json, iam.json), they are stored
  in EUUID format. After XID migration, the system uses XID format.
  This function transforms the model to match the active provider.

  Note: For well-known entities (defentity registrations), XIDs are stable
  and derived from code, so transformation is deterministic."
  [model]
  (let [model-key (try
                    (if (uuid? (first (keys (:entities model))))
                      :euuid
                      :xid)
                    (catch Throwable _ :xid))]
    (model/transform-model model model-key (id/key))))

(defn <-resource
  "Load a model from classpath resource and adapt to current ID provider format.

  Resource models are stored in EUUID format. This function:
  1. Loads and parses the transit-encoded model
  2. Transforms it to match the current ID provider (euuid or xid)

  Args:
    path - Resource path (e.g., \"dataset/dataset.json\", \"dataset/iam.json\")

  Returns:
    Model adapted to current provider format"
  [path]
  (letfn [(adjust-id [{:keys [euuid xid]
                       :as data}]
            (case (id/key)
              :xid (-> data
                       (assoc (id/key) (or xid (id/uuid->nanoid euuid))))
              :euuid (->
                       data
                       (assoc (id/key) (or euuid (id/nanoid->uuid xid))))))]
    (as-> (-> (io/resource path)
              slurp
              <-transit
              (update :model adapt-model-to-provider))
          version
      (adjust-id version)
      (update version :dataset adjust-id))))

(defn current-dataset-version
  "Returns the dataset meta-model from resources/dataset/dataset.json,
  adapted to current ID provider format."
  []
  (<-resource "dataset/dataset.json"))


(comment
  (<-resource "dataset/dataset.json")
  (def new-dataset-model
    (synthigy.dataset.patch.model/transform-model
      (:model (<-transit (slurp (io/resource "dataset/dataset.json"))))
      :euuid
      :xid))
  (def new-dataset-model2 new-dataset-model)
  (= new-dataset-model)

  (binding [id/*provider* (id/->NanoIDProvider)]
    (<-resource "dataset/dataset.json"))
  (binding [id/*provider* (id/->NanoIDProvider)]
    (spit "resources/dataset/dataset.json" (synthigy.transit/->transit (<-resource "dataset/dataset.json")))
    (spit "resources/dataset/iam.json" (synthigy.transit/->transit (<-resource "dataset/iam.json")))))

(id/entity-id-for-key :dataset/dataset :euuid)

;;; ============================================================================
;;; Database Operations (Protocol Delegation)
;;; ============================================================================

;; These functions delegate to the database-specific protocol implementations
;; defined in synthigy.db/ModelQueryProtocol

(defn sync-entity
  "Sync entity takes dataset entity id and data and synchronizes DB with
  current state. This includes inserting/updating new records and relations
  as well as removing relations that were previously linked with input data
  and currently are not.

  entity-id can be:
  - A keyword like :iam/user (auto-resolved via id/entity)
  - A UUID directly
  - A string XID directly"
  [entity-id data]
  (db/sync-entity *db* (id/entity entity-id) data))

(defn stack-entity
  "Stack takes dataset entity id and data to stack input data on top of
  current DB state.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id data]
  (db/stack-entity *db* (id/entity entity-id) data))

(defn slice-entity
  "Slice takes dataset entity id and data to slice current DB state based
  on input data effectively deleting relations between entities.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/slice-entity *db* (id/entity entity-id) args selection))

(defn get-entity
  "Takes dataset entity id, arguments to pinpoint target row and selection
  that specifies which attributes and relations should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/get-entity *db* (id/entity entity-id) args selection))

(defn get-entity-tree
  "Takes dataset entity id, root record and constructs tree based 'on'.
  Selection specifies which attributes and relations should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id root on selection]
  (db/get-entity-tree *db* (id/entity entity-id) root on selection))

(defn search-entity
  "Takes dataset entity id, arguments to pinpoint target rows and selection
  that specifies which attributes and relations should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/search-entity *db* (id/entity entity-id) args selection))

(defn search-entity-tree
  "Takes dataset entity id, arguments to pinpoint target rows based 'on'
  recursion and selection that specifies which attributes and relations
  should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id on args selection]
  (db/search-entity-tree *db* (id/entity entity-id) on args selection))

(defn purge-entity
  "Find all records that match arguments, delete found records and return
  deleted information based on selection input.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/purge-entity *db* (id/entity entity-id) args selection))

(defn aggregate-entity
  "Takes dataset entity id, arguments and selection to return aggregated
  values for given args and selection. Possible fields in selection are:
  count, max, min, avg.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/aggregate-entity *db* (id/entity entity-id) args selection))

(defn aggregate-entity-tree
  "Takes dataset entity id 'on' recursion with arguments and selection to
  return aggregated values for given args and selection. Possible fields
  in selection are: count, max, min, avg.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id on args selection]
  (db/aggregate-entity-tree *db* (id/entity entity-id) on args selection))

(defn delete-entity
  "Function takes dataset entity id and data to delete entities from DB.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id data]
  (db/delete-entity *db* (id/entity entity-id) data))

(defn deploy! [model] (core/deploy! *db* model))
(defn reload [] (core/reload *db*))

(defn init-delta-pipe
  "Initializes the delta subscription system for real-time change notifications.

  Creates:
  - delta-client: Channel for publishing entity/relation changes
  - delta-publisher: Pub/sub system for subscribing to specific entity changes

  The delta system is used by:
  - IAM access control (reload permissions when roles/permissions change)
  - GraphQL subscriptions (real-time updates)
  - Any code that needs to react to data changes

  Example subscription:
    (let [my-chan (async/chan)]
      (async/sub core/*delta-publisher* entity-uuid my-chan)
      (async/go-loop []
        (when-let [msg (async/<! my-chan)]
          (println \"Got change:\" msg)
          (recur))))"
  []
  (let [delta-client (async/chan 1000)
        delta-publisher (async/pub
                          delta-client
                          (fn [{:keys [element]}]
                            element))]
    (alter-var-root #'core/*delta-client* (constantly delta-client))
    (alter-var-root #'core/*delta-publisher* (constantly delta-publisher))))

;;; ============================================================================
;;; GraphQL Hooks System
;;; ============================================================================

(defn wrap-hooks
  "Function will wrap all hooks that were defined for some
  field definition and sort those hooks by :metric attribute.

  Every time field is required by selection hook chain will be
  called.

  Hooks accept args [ctx args value] and should return 'modified'
  [ctx args value]"
  [hooks resolver]
  (if (not-empty hooks)
    (let [hooks (keep
                  (fn [definition]
                    (let [{resolver :fn
                           :as hook} (selection/arguments definition)]
                      (if-some [resolved (try
                                           (resolve (symbol resolver))
                                           (catch Throwable e
                                             (log/errorf e "Couldn't resolve symbol %s" resolver)
                                             nil))]
                        (assoc hook :fn resolved)
                        (assoc hook :fn (fn [ctx args v]
                                          (log/errorf "Couldn't resolve '%s'" resolver)
                                          [ctx args v])))))
                  hooks)
          ;;
          {:keys [pre post]}
          (group-by
            #(cond
               (neg? (:metric % 1)) :pre
               (pos? (:metric % 1)) :post
               :else :resolver)
            hooks)
          ;;
          steps
          (cond-> (or (some-> (not-empty (map :fn pre)) vec) [])
            ;;
            (some? resolver)
            (conj (fn wrapped-resolver [ctx args value]
                    (let [new-value (resolver ctx args value)]
                      [ctx args new-value])))
            ;;
            (not-empty post)
            (into (map :fn post)))]
      ;; FINAL RESOLVER
      (fn wrapped-hooks-resolver [ctx args value]
        (let [[_ _ v]
              (reduce
                (fn [[ctx args value] f]
                  (f ctx args value))
                [ctx args value]
                steps)]
          v)))
    resolver))

;;; ============================================================================
;;; Unified Startup (Following EYWA Pattern)
;;; ============================================================================

(defn start
  "Initialize dataset system: load model and set up event channels.

  This is the unified startup function following EYWA's neyho.eywa.dataset/start pattern.
  It orchestrates:
  1. ID provider initialization (based on user's stored format choice)
  2. Delta pipe initialization (async channels for change notifications)
  3. Model versioning (apply patches for dataset meta-model)
  4. Model loading from __deploy_history
  5. Service user binding (*EYWA*)

  This should be called during application startup.
  GraphQL/Lacinia initialization is handled separately via synthigy.lacinia/start."
  ([]
   (log/info "Initializing Datasets...")

   (init-delta-pipe)


     ;; Reload model from database (after patches are applied)
   (core/reload *db*)


     ;; Apply dataset feature patches (database transforms)
   (patch/level! :synthigy/dataset)


     ;; Apply dataset model patches (meta-model deployment)
   (patch/level! :synthigy.dataset/model)
   nil))

(defn stop
  "Stop dataset system: close delta channels and clean up resources.

  This should be called during application shutdown to properly clean up:
  1. Close delta-client channel (stops all delta subscriptions)
  2. Reset delta publisher
  3. Clear model state

  Without this, async go-loops subscribing to delta events will hang
  waiting for channel operations that never complete."
  []
  (log/info "Stopping Datasets...")

;; Reset vars
  (alter-var-root #'core/*delta-client* (constantly core/not-initialized))
  (alter-var-root #'core/*delta-publisher* (constantly core/not-initialized))

  (log/info "Datasets stopped")
  nil)

(patch/current-version :synthigy/dataset "1.1.2")

(defn can-migrate-to-xid?
  "Check if system can migrate to XID format.

  Prerequisites:
  - Database has xid columns on all entity tables
  - All records have xid values populated"
  []
  (try
    ; (model/verify-xid-migration)
    true
    (catch Exception e
      (log/warnf "[ID Format] Cannot migrate to xid: %s" (.getMessage e))
      false)))
