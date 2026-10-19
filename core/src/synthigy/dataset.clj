;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset
  "High-level dataset API (database-agnostic)."
  (:refer-clojure :exclude [sync])
  (:require
   [clojure.core.async :as async]
   [clojure.java.io :as io]
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor]
   [patcho.patch :as patch]
   synthigy.dataset.access
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id :refer [defentity defrelation defdata]]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.sql.naming :refer [entity->table-name]]
   [synthigy.dataset.sql.schema :as sql-schema]
   synthigy.iam.keys
   synthigy.dataset.operations
   [synthigy.dataset.patch.model :as model]
   [synthigy.dataset.projection :as projection]
   [synthigy.dataset.system :as system]
   [synthigy.db :as db :refer [*db*]]
   [synthigy.env :as env]
   [synthigy.transit :refer [<-transit ->transit]]))

;; ============================================================================
;; Dataset Meta-Entity Definitions
;; ============================================================================

(defdata :dataset/id
  :euuid #uuid "4ab2fe4f-9b74-4a23-8441-60b58be08e7e" :xid "AE18CKqkpn1txrdJxhU9v5")

(defdata :synthigy/id
  :euuid #uuid "c5c85417-0aef-4c44-9e86-8090647d6378" :xid "RRY5JcXr8MwppjvK9bhp7Z")

(defdata :dataset.model/version-1.0.5
  :euuid #uuid "5e9704b0-cc4a-4763-96a7-5c11da57c05e" :xid "CgTka9ViKjvcSz1hmuaC53")

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

(def ^:private format-topic
  :synthigy/id-format)

(defn current-format
  "Read the stored ID format from database, or nil if not stored."
  []
  (let [stored (patch/read-version *db* format-topic)]
    (when (and stored (not= stored "0"))
      stored)))

(defn set-format!
  "Store the active ID format choice."
  [format]
  (assert (#{"euuid" "xid"} format)
          (str "Invalid format: " format ". Must be \"euuid\" or \"xid\"."))
  (patch/write-version *db* format-topic format)
  (log/info {:id ::id-format-stored
             :data {:format format}}
            "Stored ID format choice"))

(defn clear-format!
  "Remove the stored ID format choice."
  []
  (patch/write-version *db* format-topic "0")
  (log/info {:id ::id-format-cleared :data {:action :cleanup :subject :id-format}}
            "Cleared stored ID format"))

(defn initialize-provider!
  "Set the global ID provider based on format."
  [format]
  (assert (#{"euuid" "xid"} format)
          (str "Invalid format: " format ". Must be \"euuid\" or \"xid\"."))
  (log/info {:id ::id-format-initializing
             :data {:format format}}
            "Initializing ID provider for format")
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
  "Updates the deployed model and publishes :model/deployed event."
  [model]
  (reset! _model model)
  (async/put! subscription
              {:topic :model/deployed
               :model model}))

(defn add-model-watch!
  "Register a watch on the deployed-model atom; idempotent per key."
  [key f]
  (add-watch _model key f))

(defn remove-model-watch!
  "Remove a previously-registered model watch by key."
  [key]
  (remove-watch _model key))

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
  "Latest deployed version (:name, :model, id) of a dataset, or nil."
  [dataset-id]
  (try
    (->
     (db/get-entity
      *db*
      :dataset/dataset
      {(id/key) dataset-id}
      {(id/key) nil
       :name nil
       :versions [{:selections
                   {(id/key) nil
                    :name nil
                    :model nil
                    :deployed_on nil
                    :dataset [{:selections {(id/key) nil
                                            :name nil}}]}
                   :args {:deployed {:_eq true}
                          :_order_by {:deployed_on :desc}
                          :_limit 1}}]})
     :versions
     first
     (as-> v (if (and v (string? (:model v)))
               (update v :model <-transit)
               v)))
    ;; legacy meta-model without deployed_on: nil lets the model patches deploy forward
    (catch clojure.lang.ExceptionInfo e
      (if (= "UNKNOWN_ATTRIBUTE" (:code (ex-data e)))
        (do (log/warn {:id ::legacy-version-unreadable
                       :data {:action :patching :subject :deployed-on}}
                      "deployed_on attribute absent (legacy meta-model); treating as unversioned so model patches deploy forward")
            nil)
        (throw e)))))

(defn deployed-versions
  "Drift stamp — `{dataset-id {:version :deployed-at}}` for every dataset's latest deployed version."
  []
  (model/deployed-versions))

(defn stamp-schema
  "Adds the `:datasets` drift stamp to a schema map."
  [schema]
  (if-let [versions (not-empty (deployed-versions))]
    (assoc schema :datasets versions)
    schema))

(comment
  (def dataset-id (id/data :dataset/id))
  ((juxt :modified_on :deployed_on :xid) (latest-deployed-version (id/data :dataset/id)))
  (search
   :dataset/dataset
   nil
   {(id/key) nil
    :name nil
    :roles [:name :active]
    :groups [:name :active {:created-by {:name nil}} :created-on]})
  (search
   :iam/user
   nil
   {(id/key) nil
    :name nil
    :roles [:name :active]
    :groups {:name nil
             :active nil
             :created-by {:name nil}}})
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
  (def model (-> *1 first :versions first))
  (type (:model model))
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

  (spit "resources/dataset/synthigy.json"
        (synthigy.transit/->transit
         (->
          (latest-deployed-version (id/data :synthigy/id))
          (assoc-in [:dataset :euuid] (id/data :synthigy/id :euuid))
          (assoc-in [:dataset :xid] (id/data :synthigy/id :xid))))))

(defn adapt-model-to-provider
  "Transform model to match the current ID provider format."
  [model]
  (let [model-key (try
                    (if (uuid? (first (keys (:entities model))))
                      :euuid
                      :xid)
                    (catch Throwable _ :xid))]
    (model/transform-model model model-key (id/key))))

(defn <-resource
  "Load a transit-encoded model resource adapted to the current ID provider."
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

(defn current-synthigy-version
  "The Synthigy system model (resources/dataset/synthigy.json), adapted to the current ID provider."
  []
  (<-resource "dataset/synthigy.json"))

(def system-ids
  "Dataset, entity and relation ids of the shipped system models (Synthigy, OAuth) for an ID provider key."
  (memoize
   (fn [_id-key]
     (let [versions [(current-synthigy-version) (<-resource "dataset/oauth_session.json")]]
       (assoc (system/model-ids (map :model versions))
              :datasets (into #{} (map (comp id/extract :dataset)) versions))))))

(defn bootstrap-version
  "Synthigy model without RLS, for compiling the meta tables before the global model exists."
  []
  (update (current-synthigy-version) :model
          (fn [model]
            (reduce #(core/set-entity %1 (update %2 :configuration dissoc :rls))
                    model
                    (core/get-entities model)))))

(defn register-reference-types!
  "Register the user/group/role attribute types; must run before any model carrying one is mounted."
  []
  (doseq [[type-name entity-key] [["user" :iam/user]
                                  ["group" :iam/user-group]
                                  ["role" :iam/user-role]]]
    (core/register-reference-type!
     type-name (id/entity entity-key)
     {:table-fn #(entity->table-name
                  (or (core/get-entity (deployed-model) (id/entity entity-key))
                      (core/get-entity (:model (current-synthigy-version)) (id/entity entity-key))))})))

(comment
  (binding [id/*provider* (id/->NanoIDProvider)]
    (<-resource "dataset/synthigy.json"))
  (binding [id/*provider* (id/->NanoIDProvider)]
    (spit "resources/dataset/synthigy.json" (synthigy.transit/->transit (<-resource "dataset/synthigy.json")))))

(id/entity-id-for-key :dataset/dataset :euuid)

;;; ============================================================================
;;; Selection Normalization
;;; ============================================================================

(defn order-by-map
  "A `_order_by` given as [field direction] pairs, the wire and XSQL form, as the array-map the query layer reads."
  [order-by]
  (if (and (sequential? order-by)
           (every? #(and (sequential? %) (= 2 (count %))) order-by))
    (apply array-map (mapcat (fn [[f d]] [(keyword (name f)) (keyword (name d))]) order-by))
    order-by))

(defn normalize-config-args [args]
  (let [args (dk/normalize-keys-deep args)]
    (cond-> args
      (contains? args :_order_by) (update :_order_by order-by-map))))

(defn normalize-selection
  "Normalize a user-friendly selection into the internal format selection->schema expects."
  [selection]
  (cond
    (or (nil? selection) (true? selection))
    nil

    ;; Vector of keywords — expand to map
    (and (vector? selection) (every? keyword? selection))
    (normalize-selection (zipmap selection (repeat nil)))

    ;; Mixed vector — keywords and maps together
    (and (vector? selection)
         (every? #(or (keyword? %) (map? %)) selection))
    (normalize-selection
     (reduce (fn [m item]
               (if (keyword? item)
                 (assoc m item nil)
                 (merge m item)))
             {}
             selection))

    (map? selection)
    (reduce-kv
     (fn [m k v]
       (let [k (dk/normalize-key k)]
         (cond
           ;; Already internal format: vector of config maps
           (and (vector? v) (seq v) (map? (first v))
                (some #(contains? (first v) %) [:selections :args :alias]))
           (assoc m k (mapv (fn [cfg]
                              (cond-> cfg
                                (:selections cfg) (update :selections normalize-selection)
                                (:args cfg) (update :args normalize-config-args)))
                            v))

           ;; nil / true — scalar field
           (or (nil? v) (true? v))
           (assoc m k nil)

           ;; Vector — nested selection shorthand (keywords, maps, or mixed)
           (and (vector? v)
                (every? #(or (keyword? %) (map? %)) v))
           (assoc m k [{:selections (normalize-selection v)}])

           ;; Plain map without :selections — nested selection
           (and (map? v) (not (contains? v :selections)))
           (assoc m k [{:selections (normalize-selection v)}])

           ;; Map with :selections — single relation config (unwrapped)
           (and (map? v) (contains? v :selections))
           (assoc m k [(cond-> v
                         (:selections v) (update :selections normalize-selection)
                         (:args v) (update :args normalize-config-args))])

           :else
           (assoc m k v))))
     {}
     selection)

    :else selection))

;;; ============================================================================
;;; Database Operations (Protocol Delegation)
;;; ============================================================================

(defn sync-entity
  "Upserts records and relations, unlinking relations the input no longer lists; empty input is a no-op."
  [entity-id data]
  (if (or (nil? data) (and (coll? data) (empty? data)))
    []
    (db/sync-entity *db* (id/entity entity-id) data)))

(defn stack-entity
  "Stacks input data on top of current DB state; empty input is a no-op."
  [entity-id data]
  (if (or (nil? data) (and (coll? data) (empty? data)))
    []
    (db/stack-entity *db* (id/entity entity-id) data)))

(defn slice-entity
  "Deletes the relations between entities that match args."
  [entity-id args selection]
  (db/slice-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn get-entity
  "Returns the one row `args` pinpoints, shaped by `selection`."
  [entity-id args selection]
  (db/get-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn get-entity-tree
  "Builds the tree below `root` along the `on` recursion."
  [entity-id root on selection]
  (db/get-entity-tree *db* (id/entity entity-id) root on (normalize-selection selection)))

(defn search-entity
  "Returns the rows matching `args`, shaped by `selection`."
  [entity-id args selection]
  (db/search-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn search-entity-tree
  "Returns rows matching `args` with their ancestors along the `on` recursion."
  [entity-id on args selection]
  (db/search-entity-tree *db* (id/entity entity-id) on (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn purge-entity
  "Deletes every row matching `args` and returns them shaped by `selection`."
  [entity-id args selection]
  (db/purge-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn delete-entity
  "Deletes by a unique-key map (one row) or a vector of xid strings (one transaction)."
  [entity-id data]
  (db/delete-entity *db* (id/entity entity-id) (cond-> data (map? data) dk/normalize-keys)))

;;; ============================================================================
;;; Dataset Lifecycle (deploy / recall / destroy)
;;; ============================================================================

(defn version-summary
  "Identifying bits of a version (or a `{:model …}` envelope) for log payloads."
  [v]
  (let [m (or (:model v) v)]
    (cond-> {}
      (:xid m)        (assoc :version-xid (:xid m))
      (:euuid m)      (assoc :version-euuid (str (:euuid m)))
      (:name m)       (assoc :version-name (:name m))
      (:version m)    (assoc :version (:version m))
      (:dataset m)    (assoc :dataset-name (:name (:dataset m))))))

(defn persisted-model
  "The model as `deploy!` stores it, so stored and candidate models compare with `=`."
  [model]
  (let [model (if (string? model) (<-transit model) model)]
    (-> model
        core/normalize-legacy-types
        core/clean-model
        adapt-model-to-provider
        (assoc :version 1)
        ->transit
        <-transit)))

(defn meta-model-loaded?
  "True once the deployed schema holds Dataset and Dataset Version, so versions can be read."
  []
  (let [schema (sql-schema/deployed-schema)]
    (every? #(contains? schema (id/entity %)) [:dataset/dataset :dataset/version])))

(defn stored-version
  "The stored row of `version` (:name, :deployed, :model, :dataset), or nil."
  [version]
  (when-let [vid (id/extract version)]
    (get-entity :dataset/version {(id/key) vid}
                {(id/key) nil :name nil :deployed nil :model nil
                 :dataset [{:selections {(id/key) nil}}]})))

(defn deployed-version-altered?
  "True when `version` names a deployed row whose stored name, model or dataset differs from it."
  ([version] (deployed-version-altered? version (stored-version version)))
  ([version stored]
   (boolean
    (and (:deployed stored)
         (or (not= (id/extract (:dataset stored)) (id/extract (:dataset version)))
             (and (contains? version :name) (not= (:name stored) (:name version)))
             (not= (persisted-model (:model version))
                   (persisted-model (:model stored))))))))

(defn log-deploy-changes!
  "Log what deploying `version` changes against the global model — the deploy drawer's diff, for every deploy path."
  [version base]
  (try
    (let [last-deployed (some-> (:dataset version) id/extract latest-deployed-version)
          changes (projection/deploy-changes (deployed-model) (:model version) last-deployed)
          cap #(vec (take 100 %))]
      (log/info {:id ::deploy-changes
                 :data (-> (merge base changes)
                           (update :entities cap)
                           (update :relations cap))}
                (str "Deploy changes " (pr-str (:summary changes)))))
    (catch Throwable e
      (log/warn {:id ::deploy-changes-failed
                 :data (assoc base :error (ex-message e))}
                "Could not compute deploy changes"))))

(def ^:dynamic *engine-write*
  "True while deploy, recall or destroy write the rows only the engine writes."
  false)

(defn engine-write?
  "True for the engine's own writes: no principal, or inside deploy, recall or destroy."
  []
  (or *engine-write* (nil? (synthigy.dataset.access/current-principal))))

(defn model-change
  "Calls `f` as the one model change in progress: holds the model lock, announces it on the model topic and marks its writes as the engine's."
  [action dataset-name version-name f]
  (let [holder (cond-> {:action action
                        :since (str (java.time.Instant/now))}
                 dataset-name (assoc :dataset dataset-name)
                 version-name (assoc :version version-name)
                 (:name (synthigy.dataset.access/current-principal))
                 (assoc :by (:name (synthigy.dataset.access/current-principal))))]
    (db/hold-model-lock
     *db* holder
     (fn [outer?]
       (when outer? (async/put! subscription {:topic :model/deployed :busy holder}))
       (try
         (binding [*engine-write* true] (f))
         (finally
           (when outer? (async/put! subscription {:topic :model/deployed :busy nil}))))))))

(defn deploy!
  "Deploy a dataset version: creates/updates schema and tables, logging deploying/deployed/deploy-failed."
  [version]
  (let [actor   (log/current-principal-xid)
        summary (version-summary version)
        base    (cond-> (assoc summary :action :deploying :subject :dataset)
                  actor (assoc :actor-xid actor))]
    (log/info {:id ::deploying :data base}
              (str "Deploying dataset version "
                   (or (:version-name summary) (:version-xid summary) "")))
    (try
      (let [version (update version :model persisted-model)
            _ (log-deploy-changes! version base)
            result (model-change :deploy (get-in version [:dataset :name]) (:name version)
                                 #(core/deploy! *db* version))]
        (log/info {:id ::deployed
                   :data (assoc base :action :deployed)}
                  (str "Deployed dataset version "
                       (or (:version-name summary) (:version-xid summary) "")))
        result)
      (catch Throwable e
        (log/error! {:id ::deploy-failed
                     :data (assoc base :action :deploy-failed)}
                    e)
        (throw e)))))

(defn recall!
  "Recall (delete) a dataset version, rolling back its schema and rebuilding the global model; true when done."
  [version]
  (let [actor   (log/current-principal-xid)
        summary (version-summary version)
        base    (cond-> (assoc summary :subject :dataset)
                  actor (assoc :actor-xid actor))
        id      (id/extract version)
        stored  (when id
                  (get-entity :dataset/version {(id/key) id} {(id/key) nil :name nil :deployed nil}))]
    (if (and stored (not (:deployed stored)))
      (do (delete-entity :dataset/version {(id/key) id})
          (log/info {:id ::recalled
                     :data (assoc base :action :recalled :deployed? false)}
                    (str "Deleted undeployed dataset version "
                         (or (:version-name summary) (:version-xid summary) "")))
          true)
      (do
        (log/info {:id ::recalling
                   :data (assoc base :action :recalling)}
                  (str "Recalling dataset version "
                       (or (:version-name summary) (:version-xid summary) "")))
        (try
          (model-change :recall nil (:name stored) #(core/recall! *db* version))
          (log/info {:id ::recalled
                     :data (assoc base :action :recalled)}
                    (str "Recalled dataset version "
                         (or (:version-name summary) (:version-xid summary) "")))
          true
          (catch Throwable e
            (log/error! {:id ::recall-failed
                         :data (assoc base :action :recall-failed)}
                        e)
            (throw e)))))))

(defn destroy!
  "Destroy a dataset — recalls all versions and removes tables, schema, versions and the dataset record."
  [dataset]
  (let [actor (log/current-principal-xid)
        base  (cond-> {:subject :dataset
                       :dataset-name (:name dataset)
                       :dataset-xid  (:xid dataset)}
                actor (assoc :actor-xid actor))]
    (log/warn {:id ::destroying
               :data (assoc base :action :destroying)}
              (str "Destroying dataset " (or (:name dataset) "(unnamed)")))
    (try
      (let [result (model-change :destroy (:name dataset) nil #(core/destroy! *db* dataset))]
        (log/warn {:id ::destroyed
                   :data (assoc base :action :destroyed)}
                  (str "Destroyed dataset " (or (:name dataset) "(unnamed)")))
        result)
      (catch Throwable e
        (log/error! {:id ::destroy-failed
                     :data (assoc base :action :destroy-failed)}
                    e)
        (throw e)))))

;;; ============================================================================
;;; Sugar API
;;; ============================================================================
(def ^:private default-key-format :kebab)

(defn resolve-key-fn
  "Resolve key-fn from opts; nil for snake (no transform)."
  [opts]
  (let [fmt (get opts :key-format default-key-format)]
    (when (not= fmt :snake)
      (dk/format->key-fn fmt))))

(defn selection-sub-map
  "Pull the nested selection map out of a relation-style selection value."
  [v]
  (when (and (vector? v) (seq v) (map? (first v)))
    (:selections (first v))))

(defn result-key->info
  "Map result-key (alias or schema key) → {:original-k k :sub-sel ...} for a normalized selection."
  [selection]
  (reduce-kv
   (fn [m k v]
     (if (and (vector? v) (seq v) (map? (first v))
              (some #(contains? (first v) %) [:selections :args :alias]))
       (reduce (fn [m {:keys [alias selections]}]
                 (let [rk (if alias (keyword alias) k)]
                   (assoc m rk {:original-k k :sub-sel selections})))
               m v)
       (assoc m k {:original-k k :sub-sel nil})))
   {}
   selection))

(defn strip-to-selection
  "Trim a query result, recursively, to the keys the normalized `selection` asked for plus the id key."
  [entity-id selection data]
  (cond
    (nil? data) nil
    (sequential? data) (mapv #(strip-to-selection entity-id selection %) data)
    (not (map? data)) data
    (or (nil? selection) (empty? selection)) data
    :else
    (let [{:keys [fields field->attribute relations recursions]}
          (sql-schema/deployed-schema-entity entity-id)
          id-k (id/key)
          rk->info (result-key->info selection)
          selected? (conj (set (keys rk->info)) id-k)]
      (reduce-kv
       (fn [m k v]
         (if-not (selected? k)
           m
           (let [{:keys [original-k sub-sel]} (get rk->info k)
                 lookup-k (or original-k k)
                 rel (get relations lookup-k)
                 attr-id (get field->attribute lookup-k)
                 attr (when attr-id (get fields attr-id))
                 ref-entity (:reference/entity attr)
                 recursion? (contains? recursions lookup-k)
                 target-id (cond
                             rel (:to rel)
                             recursion? entity-id
                             ref-entity ref-entity)]
             (assoc m k
                    (if (and target-id sub-sel)
                      (strip-to-selection target-id sub-sel v)
                      v)))))
       {}
       data))))

(defn transform-output
  "Strip a result to the selection, then apply the key format."
  [entity-id selection key-fn data]
  (when data
    (let [stripped (strip-to-selection entity-id selection data)]
      (cond
        (nil? key-fn) stripped
        (map? stripped) (dk/transform-result entity-id key-fn stripped selection)
        (sequential? stripped) (mapv #(dk/transform-result entity-id key-fn % selection) stripped)
        :else stripped))))

(defn search
  "Search for entities matching args; kebab-case keys by default (`:key-format`)."
  ([entity-id args selection]
   (search entity-id args selection nil))
  ([entity-id args selection opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)
         sel (normalize-selection selection)]
     (transform-output eid sel key-fn
                       (search-entity entity-id
                                      (dk/normalize-keys-deep args)
                                      sel)))))

(defn get-one
  "Get a single entity by unique constraint; kebab-case keys by default (`:key-format`)."
  ([entity-id args selection]
   (get-one entity-id args selection nil))
  ([entity-id args selection opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)
         sel (normalize-selection selection)]
     (transform-output eid sel key-fn
                       (get-entity entity-id
                                   (dk/normalize-keys-deep args)
                                   sel)))))

(defn sync
  "Sync (upsert) entity data; kebab-case keys by default (`:key-format`)."
  ([entity-id data]
   (sync entity-id data nil))
  ([entity-id data opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)]
     (transform-output eid nil key-fn
                       (sync-entity entity-id (dk/normalize-keys data))))))

(defn stack
  "Stack data on top of current state; kebab-case keys by default (`:key-format`)."
  ([entity-id data]
   (stack entity-id data nil))
  ([entity-id data opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)]
     (transform-output eid nil key-fn
                       (stack-entity entity-id (dk/normalize-keys data))))))

(defn delete
  "Delete by a unique-key map (boolean) or a vector of xid strings (count deleted)."
  ([entity-id data]
   (delete entity-id data nil))
  ([entity-id data opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)
         data (if (sequential? data) data (dk/normalize-keys data))]
     (transform-output eid nil key-fn (delete-entity entity-id data)))))

(defn slice
  "Slice relations from entity; kebab-case keys by default (`:key-format`)."
  ([entity-id args selection]
   (slice entity-id args selection nil))
  ([entity-id args selection opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)]
     (transform-output eid nil key-fn
                       (slice-entity entity-id
                                     (dk/normalize-keys-deep args)
                                     (normalize-selection selection))))))

(defn purge
  "Find and delete matching records, returning them; kebab-case keys by default (`:key-format`)."
  ([entity-id args selection]
   (purge entity-id args selection nil))
  ([entity-id args selection opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)]
     (transform-output eid nil key-fn
                       (purge-entity entity-id
                                     (dk/normalize-keys-deep args)
                                     (normalize-selection selection))))))

(defn reload [] (core/reload *db*))

;;; ============================================================================
;;; Unified Startup
;;; ============================================================================

(defn start
  "Initialize the dataset system: delta pipe, model load, then dataset and meta-model patches."
  ([]
   (log/info {:id ::initializing} "Initializing Datasets")

   (delta/init!)

   (register-reference-types!)
   (core/reload *db*)

   (supervisor/progress-patching! :synthigy/dataset
                                  (patch/deployed-version :synthigy/dataset)
                                  (patch/version :synthigy/dataset))
   (patch/level! :synthigy/dataset)

   (supervisor/progress-patching! :synthigy.dataset/model
                                  (patch/deployed-version :synthigy.dataset/model)
                                  (patch/version :synthigy.dataset/model))
   (patch/level! :synthigy.dataset/model)
   nil))

(defn stop
  "Stop the dataset system; without it delta-subscriber go-loops hang."
  []
  (log/info {:id ::stopping :data {:action :stopping :subject :dataset}} "Stopping Datasets")

  (delta/shutdown!)

  (log/info {:id ::stopped :data {:action :stopped :subject :dataset}} "Datasets stopped")
  nil)
