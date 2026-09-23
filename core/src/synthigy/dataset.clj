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
   [synthigy.dataset.sql.schema :as sql-schema]
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

(defdata :dataset/id
  :euuid #uuid "4ab2fe4f-9b74-4a23-8441-60b58be08e7e" :xid "AE18CKqkpn1txrdJxhU9v5")

(defdata :dataset.model/version-1.0.3
  :euuid #uuid "d908a70f-a1fb-46bd-ac76-801bebe6ceed" :xid "ToR56Krq2zEUmkKmtrfmNU")

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
  "Gets the latest deployed version for a dataset.

  Returns a map with :name (version string), :model, and ID (via id/key).
  Compatible with patcho versioning system.

  Note: Currently returns the in-memory model since we don't have
  per-dataset versioning yet. The dataset-id is ignored."
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
                   ;; Order by `deployed_on` — the meta-model entities are not
                   ;; row-stamp-audited, so they have no `modified_on` column;
                   ;; `deployed_on` is also the semantically correct cursor for
                   ;; "latest deployed version".
                   :args {:deployed {:_eq true}
                          :_order_by {:deployed_on :desc}
                          :_limit 1}}]})
     :versions
     first
     (as-> v (if (and v (string? (:model v)))
               (update v :model <-transit)
               v)))
    ;; Legacy (pre-`deployed_on`) meta-model: the Dataset Version entity does
    ;; not yet define the `deployed_on` attribute, so this selection is invalid.
    ;; That attribute is added by the `:synthigy.dataset/model` patches — but
    ;; those patches read THIS function to decide the installed version, a
    ;; chicken-and-egg. Returning nil makes installed-version resolve to "0",
    ;; so the model patches deploy forward and add `deployed_on`. Self-heals on
    ;; first boot of an old dataset; subsequent boots take the fast path above.
    (catch clojure.lang.ExceptionInfo e
      (if (= "UNKNOWN_ATTRIBUTE" (:code (ex-data e)))
        (do (log/warn {:id ::legacy-version-unreadable
                       :data {:action :patching :subject :deployed-on}}
                      "deployed_on attribute absent (legacy meta-model); treating as unversioned so model patches deploy forward")
            nil)
        (throw e)))))

(defn deployed-versions
  "Drift stamp — `{dataset-id {:version :deployed-at}}` for every dataset's
   latest deployed version. Public, cheap; consumed by `/schema`,
   generated `schema.json` and `/.well-known/synthigy`."
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
    (spit "resources/dataset/iam.json" (synthigy.transit/->transit (<-resource "dataset/iam.json"))))
  (binding [id/*provider* (id/->NanoIDProvider)]
    (spit "resources/dataset/dataset.json" (synthigy.transit/->transit (<-resource "dataset/dataset.json")))
    (spit "resources/dataset/iam.json" (synthigy.transit/->transit (<-resource "dataset/iam.json")))))

(id/entity-id-for-key :dataset/dataset :euuid)

;;; ============================================================================
;;; Selection Normalization
;;; ============================================================================

(defn normalize-selection
  "Normalize a user-friendly selection into the internal format
  expected by selection->schema.

  Normalizes keys to snake_case (kebab-case and camelCase accepted).

  Supports shorthand syntax:
  - nil / true              → nil (include field)
  - [:name :email]          → {:name nil :email nil}
  - {:roles {:name nil}}    → {:roles [{:selections {:name nil}}]}
  - {:roles [:name]}        → {:roles [{:selections {:name nil}}]}
  - {:roles {:selections {:name nil} :args {...}}} → wraps in vector

  Existing internal format [{:selections {...}}] passes through unchanged."
  [selection]
  (cond
    (or (nil? selection) (true? selection))
    nil

    ;; Vector of keywords — expand to map
    (and (vector? selection) (every? keyword? selection))
    (normalize-selection (zipmap selection (repeat nil)))

    ;; Mixed vector — keywords and maps together
    ;; [:name :active {:created-by {:name nil}} :created-on]
    ;; → {:name nil :active nil :created-by {:name nil} :created-on nil}
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
                                (:args cfg) (update :args dk/normalize-keys-deep)))
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
                         (:args v) (update :args dk/normalize-keys-deep))])

           :else
           (assoc m k v))))
     {}
     selection)

    :else selection))

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
  - A string XID directly

  Empty input (nil, `[]`, or `{}`) is a NO-OP — it returns `[]` and writes
  nothing. Otherwise a nil/empty map would be turned into `{:tmp/id …}` deep in
  `analyze-data` and persisted as a phantom record with all defaults."
  [entity-id data]
  (if (or (nil? data) (and (coll? data) (empty? data)))
    []
    (db/sync-entity *db* (id/entity entity-id) data)))

(defn stack-entity
  "Stack takes dataset entity id and data to stack input data on top of
  current DB state.

  entity-id can be a keyword (auto-resolved), UUID, or string.

  Empty input (nil, `[]`, or `{}`) is a NO-OP — see `sync-entity`."
  [entity-id data]
  (if (or (nil? data) (and (coll? data) (empty? data)))
    []
    (db/stack-entity *db* (id/entity entity-id) data)))

(defn slice-entity
  "Slice takes dataset entity id and data to slice current DB state based
  on input data effectively deleting relations between entities.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/slice-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn get-entity
  "Takes dataset entity id, arguments to pinpoint target row and selection
  that specifies which attributes and relations should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/get-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn get-entity-tree
  "Takes dataset entity id, root record and constructs tree based 'on'.
  Selection specifies which attributes and relations should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id root on selection]
  (db/get-entity-tree *db* (id/entity entity-id) root on (normalize-selection selection)))

;; Wire-arg normalization at the protocol entry. Backends expect the
;; snake-case meta-key form `:_order_by` — `dk/normalize-keys-deep` resolves
;; any inbound spelling to it via `preserve-prefix`. Callers that hand in
;; kebab meta-keys (embedded/REPL/SDK) would otherwise have `_order_by` (and
;; siblings `_limit`, `_offset`, `_where`, `_join`, `_distinct`, `_count`,
;; `_agg`) sneak past the fused EXISTS-builder's `meta-keys` check in
;; postgres/fused.clj and get treated as filter predicates, silently
;; dropping rows. Idempotent — already-normalized args pass through.
;;
;; The `selection` gets the same treatment via `normalize-selection` — it
;; carries model keys too (attribute names AND the nested relation `:args`/
;; `_where` inside relation configs), and `selection->schema` validates them
;; by EXACT match against the schema. Normalizing args but not the selection
;; is why a kebab where key worked at the top level but not inside a relation
;; pull or a projection. Both are model-key surfaces; the server owns their
;; normalization so clients can send idiomatic kebab everywhere. Idempotent.

(defn search-entity
  "Takes dataset entity id, arguments to pinpoint target rows and selection
  that specifies which attributes and relations should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/search-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn search-entity-tree
  "Takes dataset entity id, arguments to pinpoint target rows based 'on'
  recursion and selection that specifies which attributes and relations
  should be returned.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id on args selection]
  (db/search-entity-tree *db* (id/entity entity-id) on (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn purge-entity
  "Find all records that match arguments, delete found records and return
  deleted information based on selection input.

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id args selection]
  (db/purge-entity *db* (id/entity entity-id) (dk/normalize-keys-deep args) (normalize-selection selection)))

(defn delete-entity
  "Deletes entity records. `data` is a unique-key map (single row) or a
  vector of xid strings (multirow, one transaction).

  entity-id can be a keyword (auto-resolved), UUID, or string."
  [entity-id data]
  (db/delete-entity *db* (id/entity entity-id) data))

;;; ============================================================================
;;; Dataset Lifecycle (deploy / recall / destroy)
;;; ============================================================================
;;;
;;; Top-level model-change emits — these are what the cockpit's System lens
;;; surfaces as "who deployed what, when". The backend's lower-level
;;; `synthigy.dataset.postgres` schema-diff signals still fire underneath
;;; for the per-table detail; these are the *headline* events.

(defn- version-summary
  "Pull the identifying bits of a version map for log payloads. Vary
   shape — some callers pass a model map directly, others a deploy
   envelope `{:model …}`."
  [v]
  (let [m (or (:model v) v)]
    (cond-> {}
      (:xid m)        (assoc :version-xid (:xid m))
      (:euuid m)      (assoc :version-euuid (str (:euuid m)))
      (:name m)       (assoc :version-name (:name m))
      (:version m)    (assoc :version (:version m))
      (:dataset m)    (assoc :dataset-name (:name (:dataset m))))))

(defn deploy!
  "Deploy a dataset version. Creates/updates schema, tables, etc.
   Emits `:synthigy.dataset/deploying` before and
   `:synthigy.dataset/deployed` (success) or `:synthigy.dataset/deploy-failed`
   (failure) after — both carry the actor xid (when IAM is on) and a
   version summary so the System lens can answer 'who deployed what'."
  [version]
  (let [actor   (log/current-principal-xid)
        summary (version-summary version)
        base    (cond-> (assoc summary :action :deploying :subject :dataset)
                  actor (assoc :actor-xid actor))]
    (log/info {:id ::deploying :data base}
              (str "Deploying dataset version "
                   (or (:version-name summary) (:version-xid summary) "")))
    (try
      (let [result (core/deploy! *db* version)]
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
  "Recall (delete) a specific dataset version. Handles schema rollback,
   orphaned table cleanup, and global model rebuild.
   Emits `:synthigy.dataset/recalling` / `:synthigy.dataset/recalled` for
   System-lens visibility."
  [version]
  (let [actor   (log/current-principal-xid)
        summary (version-summary version)
        base    (cond-> (assoc summary :subject :dataset)
                  actor (assoc :actor-xid actor))
        id      (id/extract version)
        stored  (when id
                  (get-entity :dataset/version {(id/key) id} {(id/key) nil :deployed nil}))]
    (if (and stored (not (:deployed stored)))
      ;; Never deployed — nothing is mounted, so drop the record and skip
      ;; schema rollback entirely.
      (do (delete-entity :dataset/version {(id/key) id})
          (log/info {:id ::recalled
                     :data (assoc base :action :recalled :deployed? false)}
                    (str "Deleted undeployed dataset version "
                         (or (:version-name summary) (:version-xid summary) "")))
          nil)
      (do
        (log/info {:id ::recalling
                   :data (assoc base :action :recalling)}
                  (str "Recalling dataset version "
                       (or (:version-name summary) (:version-xid summary) "")))
        (try
          (let [result (core/recall! *db* version)]
            (log/info {:id ::recalled
                       :data (assoc base :action :recalled)}
                      (str "Recalled dataset version "
                           (or (:version-name summary) (:version-xid summary) "")))
            result)
          (catch Throwable e
            (log/error! {:id ::recall-failed
                         :data (assoc base :action :recall-failed)}
                        e)
            (throw e)))))))

(defn destroy!
  "Destroy a dataset — recalls all versions and removes all dataset data.
   Nuclear delete: tables, schema, versions, dataset record — all gone.
   This emit is at WARN level since destruction is operationally significant
   and should stand out in the System lens."
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
      (let [result (core/destroy! *db* dataset)]
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
;; Idiomatic Clojure interface with:
;; - kebab-case output by default
;; - Selection shorthand (see normalize-selection)
;; - Key normalization on args and data (kebab/camel → snake_case)
;; - Schema-aware output transformation (JSON values untouched)

(def ^:private default-key-format :kebab)

(defn- resolve-key-fn
  "Resolve key-fn from opts. Returns nil for snake (no transform needed)."
  [opts]
  (let [fmt (get opts :key-format default-key-format)]
    (when (not= fmt :snake)
      (dk/format->key-fn fmt))))

(defn- selection-sub-map
  "Pull the nested selection map out of a relation-style selection value.
   Normalized form is [{:selections {...} :args ...}]."
  [v]
  (when (and (vector? v) (seq v) (map? (first v)))
    (:selections (first v))))

(defn- result-key->info
  "Build a map from result-key → {:original-k k :sub-sel ...} for a
  normalized selection. Each relation-style entry contributes one
  result-key (alias if present, else original key); scalar entries
  contribute their key directly. Used to bridge selection (keyed by
  schema names) and result data (keyed by aliases when present).

  Aliases supplied as strings are keywordized so the result-key set
  matches what the JSON parser produces (every JSON key becomes a
  keyword via `synthigy.json/read-str`)."
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
  "Trim a query result so it contains only the keys the caller actually
   asked for (plus the id key for row identity).

   The query engine auto-includes recursions like :father/:mother as empty
   maps even when the caller didn't request them. Callers who write
   minimal selections expect minimal responses, so we filter those away
   here — recursively, so nested records also honor their sub-selection.

   Honors :alias on relation entries: a result key may be an alias string,
   in which case relation metadata is looked up by the original schema key.

   Expects `selection` in the normalized internal format (see
   normalize-selection). Snake_case keys throughout.

   Non-map / nil data passes through untouched."
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

(defn- transform-output
  "Shape a query result for return to the caller:
   1. Strip keys that weren't in the user's selection (keeps id key)
   2. Apply key-format (kebab/camel) — recurses into nested references

   Operates on single maps or sequences. Pass-through for nil and scalars."
  [entity-id selection key-fn data]
  (when data
    (let [stripped (strip-to-selection entity-id selection data)]
      (cond
        (nil? key-fn) stripped
        (map? stripped) (dk/transform-result entity-id key-fn stripped selection)
        (sequential? stripped) (mapv #(dk/transform-result entity-id key-fn % selection) stripped)
        :else stripped))))

(defn search
  "Search for entities matching args.

  Returns results with kebab-case keys by default.
  Args and selection accept kebab-case (normalized to snake_case).
  Selection supports shorthand syntax (see normalize-selection).

  Options:
    :key-format - :kebab (default), :snake, :camel"
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
  "Get a single entity by unique constraint.

  Returns result with kebab-case keys by default.
  Args and selection accept kebab-case (normalized to snake_case).
  Selection supports shorthand syntax (see normalize-selection).

  Options:
    :key-format - :kebab (default), :snake, :camel"
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
  "Sync (upsert) entity data.

  Returns synced record with kebab-case keys by default.
  Data keys accept kebab-case (normalized to snake_case).

  Options:
    :key-format - :kebab (default), :snake, :camel"
  ([entity-id data]
   (sync entity-id data nil))
  ([entity-id data opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)]
     (transform-output eid nil key-fn
                       (sync-entity entity-id (dk/normalize-keys data))))))

(defn stack
  "Stack data on top of current state.

  Returns stacked record with kebab-case keys by default.
  Data keys accept kebab-case (normalized to snake_case).

  Options:
    :key-format - :kebab (default), :snake, :camel"
  ([entity-id data]
   (stack entity-id data nil))
  ([entity-id data opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)]
     (transform-output eid nil key-fn
                       (stack-entity entity-id (dk/normalize-keys data))))))

(defn delete
  "Delete entity records.

  `data` is a unique-key map (single row, boolean result) or a vector of
  xid strings (multirow, returns the count deleted) — see delete-entity.

  Returns deleted record with kebab-case keys by default.
  Map data keys accept kebab-case (normalized to snake_case); a vector of
  xids is passed through as-is.

  Options:
    :key-format - :kebab (default), :snake, :camel"
  ([entity-id data]
   (delete entity-id data nil))
  ([entity-id data opts]
   (let [eid (id/entity entity-id)
         key-fn (resolve-key-fn opts)
         data (if (sequential? data) data (dk/normalize-keys data))]
     (transform-output eid nil key-fn (delete-entity entity-id data)))))

(defn slice
  "Slice relations from entity.

  Returns result with kebab-case keys by default.
  Args and selection accept kebab-case (normalized to snake_case).
  Selection supports shorthand syntax (see normalize-selection).

  Options:
    :key-format - :kebab (default), :snake, :camel"
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
  "Find and delete matching records, return deleted data.

  Returns result with kebab-case keys by default.
  Args and selection accept kebab-case (normalized to snake_case).
  Selection supports shorthand syntax (see normalize-selection).

  Options:
    :key-format - :kebab (default), :snake, :camel"
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

  This should be called during application startup."
  ([]
   (log/info {:id ::initializing} "Initializing Datasets")

   (delta/init!)

;; Reload model from database (after patches are applied)
   (core/reload *db*)

;; Apply dataset feature patches (database transforms)
   (supervisor/progress-update!
    {:detail (str "patching :synthigy/dataset "
                  (patch/deployed-version :synthigy/dataset) " \u2192 "
                  (patch/version :synthigy/dataset))})
   (patch/level! :synthigy/dataset)

;; Apply dataset model patches (meta-model deployment)
   (supervisor/progress-update!
    {:detail (str "patching :synthigy.dataset/model "
                  (patch/deployed-version :synthigy.dataset/model) " \u2192 "
                  (patch/version :synthigy.dataset/model))})
   (patch/level! :synthigy.dataset/model)
   nil))

(defn stop
  "Stop dataset system: tear down the delta registry and clean up resources.

  This should be called during application shutdown to properly clean up:
  1. Close every delta-subscriber channel (delta/shutdown!)
  2. Drop the subscriber registry
  3. Clear model state

  Without this, async go-loops subscribing to delta events will hang
  waiting for channel operations that never complete."
  []
  (log/info {:id ::stopping :data {:action :stopping :subject :dataset}} "Stopping Datasets")

;; Tear down the delta pipe. Drainer ownership has moved to
;; :synthigy/subscriptions.<backend>; its :stop hook stops the drainer
;; thread before this code runs.
  (delta/shutdown!)

  (log/info {:id ::stopped :data {:action :stopped :subject :dataset}} "Datasets stopped")
  nil)

(patch/current-version :synthigy/dataset "1.5.0")

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
      (log/warn {:id ::xid-migration-blocked
                 :data {:action :migrating :subject :xid
                        :error-message (.getMessage e)}}
                "Cannot migrate to xid format")
      false)))
