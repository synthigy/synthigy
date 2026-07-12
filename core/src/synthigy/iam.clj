(ns synthigy.iam
  (:require
    [buddy.core.codecs]
    [buddy.core.hash]
    [buddy.hashers :as hashers]
    clojure.pprint
    clojure.set
    [synthigy.log :as log]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.data
     :refer [*SYNTHIGY*
             *ROOT*
             *PUBLIC_ROLE*
             *PUBLIC_USER*]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.access :as dataset.access]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming :refer [entity->table-name]]
    synthigy.iam.keys  ; register well-known :iam/* ids — shared with frontend
    [synthigy.db :refer [*db*]]
    [synthigy.iam.access :as access :refer [with-principal]]
    [synthigy.iam.audit]
    [synthigy.iam.context :as context]
    synthigy.iam.encryption
    [synthigy.iam.events :as events]
    [synthigy.iam.gen :as gen]
    [synthigy.iam.patch]
    [synthigy.iam.patch.model]
    [synthigy.iam.util
     :refer [import-role
             import-api
             import-app]]))

;; ============================================================================
;; IAM Entity Definitions
;; ============================================================================
;; Well-known :iam/* entity and data identity registrations live in
;; `synthigy.iam.keys` (shared with the frontend). They are registered on
;; require (see ns declaration above). Look up via:
;;
;;   (id/entity :iam/user-role)  ; -> euuid or xid, per current provider
;;   (id/data   :iam/id)

;; ============================================================================
;; IAM Core
;; ============================================================================

;; Event publishing infrastructure moved to synthigy.iam.events
(def subscription events/subscription)
(def publisher events/publisher)
(def publish events/publish)

(defn root?
  [roles]
  (contains? (set roles) (id/extract *ROOT*)))

;; Encryption functions moved to synthigy.iam.encryption
;; OAuth code should require [synthigy.iam.encryption :as encryption] directly

(defn get-password [username]
  (:password
    (dataset/get-entity
      :iam/app
      {:name username}
      {:password nil})))

(comment
  (delete-user (get-user-details "oauth_test")))

(defn get-user-details [username]
  (some->
    (dataset/get-entity
      :iam/user
      {:name username}
      {:_eid nil
       (id/key) nil
       :name nil
       :password nil
       :active nil
       :settings nil
       :person_info [{:args {:_join :left}
                      :selections
                      {:name nil
                       :given_name nil
                       :middle_name nil
                       :nickname nil
                       :preferred_username nil
                       :profile nil
                       :picture nil
                       :website nil
                       :email nil
                       :email_verified nil
                       :gender nil
                       :birthdate nil
                       :zoneinfo nil
                       :phone_number nil
                       :phone_number_verified nil
                       :address nil}}]
       ;; LEFT joins: a user with no person_info/groups/roles must still be
       ;; returned — relation pulls here are projections, not filters.
       ;; INNER (default) would drop the parent via inner-exists.
       :groups [{:args {:_join :left} :selections {(id/key) nil}}]
       :roles  [{:args {:_join :left} :selections {(id/key) nil}}]})
    (update :roles #(set (map id/extract %)))
    (update :groups #(set (map id/extract %)))))

(defn validate-password
  [user-password password-hash]
  (hashers/check user-password password-hash))

(defn jwt-token? [token]
  (= 2 (count (re-seq #"\." token))))

(defn get-client
  [id]
  (dataset/get-entity
    :iam/app
    {:id id}
    {(id/key) nil
     :id nil
     :name nil
     :type nil
     :active nil
     :secret nil
     :settings nil}))

(defn get-clients
  [ids]
  (dataset/search-entity
    :iam/app
    {:id {:_in ids}}
    {(id/key) nil
     :id nil
     :name nil
     :type nil
     :active nil
     :secret nil
     :settings nil}))

(defn add-client [{:keys [id name secret settings type]
                   :or {id (gen/client-id)
                        type :public}}]
  (let [confidential? (#{:confidential "confidential"} type)
        raw-secret (or secret
                       (when confidential?
                         (gen/client-secret)))
        client (dataset/sync-entity
                 :iam/app
                 {:id id
                  :name name
                  :type type
                  :settings settings
                  :secret raw-secret
                  :active true})]
    ;; Auto-create linked service user for confidential clients
    (when confidential?
      (dataset/stack-entity
        :iam/user
        {:name id
         :type :SERVICE
         :active true}))
    ;; Return raw secret — DB stores the hash (attribute type: Hash)
    (assoc client :secret raw-secret)))

(defn remove-client [client]
  (dataset/delete-entity :iam/app {(id/key) (id/extract client)}))

(defn set-user
  [user-data]
  (dataset/sync-entity :iam/user user-data))

(defn delete-user
  [user-data]
  (dataset/delete-entity :iam/user {(id/key) (id/extract user-data)}))

(defn list-clients
  []
  (dataset/search-entity
    :iam/app nil
    {(id/key) nil
     :name nil
     :id nil
     :secret nil
     :type nil
     :settings nil}))

(defn ensure-public
  []
  (dataset/sync-entity
    :iam/user
    (assoc *PUBLIC_USER* :roles [*PUBLIC_ROLE*])))

(defn current-version
  "Returns the IAM model from resources, adapted to current ID provider format."
  []
  (dataset/<-resource "dataset/iam.json"))

;; Register current IAM dataset version with patcho
(patch/current-version :synthigy.iam/model (:name (current-version)))

(defn bind-service-user
  "Loads a service user from the database and binds it to a var.

  Takes a var containing a map with entity ID, queries the database
  for the full user entity, and updates the var with the loaded data.

  Used to initialize service accounts like *SYNTHIGY* or *PUBLIC_USER*.

  Copied from EYWA: neyho.eywa.dataset/bind-service-user"
  [variable]
  (let [args {(id/key) (id/extract (var-get variable))}
        data (dataset/get-entity
               :iam/user
               args
               {:_eid nil
                (id/key) nil
                :name nil
                :active nil
                :type nil})]
    (log/debug {:id ::bind-service-user
                :data {:var (str variable) :user data}}
               "Initializing service user")
    (alter-var-root variable (constantly data))))

(defn start
  []
  (log/info {:id ::initializing} "Initializing IAM")
  (try
    ;; Note: Encryption is now started separately as a dependency module
    ;; Register IAM reference types with table-fn for dynamic table name lookup
    (core/register-reference-type! "user" (id/entity :iam/user)
                                   {:table-fn #(entity->table-name (core/get-entity
                                                                     (dataset/deployed-model)
                                                                     (id/entity :iam/user)))})
    (core/register-reference-type! "group" (id/entity :iam/user-group)
                                   {:table-fn #(entity->table-name
                                                 (core/get-entity
                                                   (dataset/deployed-model)
                                                   (id/entity :iam/user-group)))})
    (core/register-reference-type! "role" (id/entity :iam/user-role)
                                   {:table-fn #(entity->table-name
                                                 (core/get-entity
                                                   (dataset/deployed-model)
                                                   (id/entity :iam/user-role)))})

    ;; Level components in dependency order:
    ;; 1. Model patches (dataset schema) MUST come first
    ;; 2. Component patches (infrastructure setup)
    ;; 3. Audit patches (database-specific schema enhancements)
    (patch/level! :synthigy.iam/model    ; Deploy IAM dataset model
                  :synthigy/iam          ; Initialize IAM component
                  :synthigy.iam/audit)   ; Add audit enhancements (implementation chosen by requires)

    ;; Initialize user context provider for audit tracking
    (context/start)

    (ensure-public)

    ;; Bind service users (load from DB and update vars)
    (binding [core/*return-type* :edn]
      (bind-service-user #'*PUBLIC_USER*)
      (bind-service-user #'synthigy.data/*SYNTHIGY*))

    ;; Bind IAM access control implementation to dataset protocol
    (alter-var-root #'dataset.access/*access-control*
                    (constantly (access/->IAMAccessControl)))
    (dataset/reload)
    (log/info {:id ::initialized} "IAM initialized")
    (catch Throwable e
      (log/error! {:id ::initialize-failed
                   :msg "Failed to initialize IAM"} e)
      (throw e))))

(defn stop
  []
  (when context/*user-context-provider*
    (context/stop! context/*user-context-provider*)
    (alter-var-root #'context/*user-context-provider* (constantly nil))))

(defn setup-schema
  "Sets up the IAM database schema.

  This loads and deploys the IAM dataset definition (entities, relations, attributes)
  that define the structure of users, roles, groups, permissions, etc.

  Should be called during initial database setup, before setup-data."
  []
  (log/info {:id ::loading-schema} "Loading IAM schema from dataset/iam.json")
  (let [iam-schema (dataset/<-resource "dataset/iam.json")]
    ;; Mount the IAM schema (create tables, etc.)
    (as-> iam-schema model
      (core/mount *db* model)
      (core/reload *db* model))
    (log/info {:id ::schema-mounted} "Mounted IAM schema")

    ;; Deploy the IAM schema to version history
    (core/deploy! *db* iam-schema)
    (log/info {:id ::schema-deployed :data {:action :deployed :subject :iam-schema}} "Deployed IAM schema to history")))

(defn setup-system-users
  "Creates essential system users (SYNTHIGY service user).

  Should be called after setup-schema but before setup-data."
  []
  (log/info {:id ::creating-synthigy-user} "Creating *SYNTHIGY* system user")
  (binding [core/*return-type* :edn]
    (dataset/sync-entity :iam/user *SYNTHIGY*)
    (bind-service-user #'synthigy.data/*SYNTHIGY*))
  (log/info {:id ::created-synthigy-user} "*SYNTHIGY* system user created"))

(defn setup-default-data
  "Imports default IAM data (apps, APIs, roles).

  Should be called as *SYNTHIGY* user after system users are created."
  []
  ;; nil principal = system superuser (no identity bound).
  (with-principal nil
    (dataset/sync-entity :iam/user-role *ROOT*)
    (dataset/sync-entity
      :iam/user
      (assoc *PUBLIC_USER* :roles [*PUBLIC_ROLE*]))
    (log/info {:id ::importing-default-data} "Importing default IAM data")
    (import-app "exports/app_synthigy_frontend.json")
    (import-app "exports/app_synthigy_components.json")
    (import-api "exports/api_synthigy_graphql.json")
    (doseq [role ["exports/role_dataset_developer.json"
                  "exports/role_dataset_modeler.json"
                  "exports/role_dataset_explorer.json"
                  "exports/role_iam_admin.json"
                  "exports/role_iam_user.json"]]
      (import-role role))
    (log/info {:id ::imported-default-data} "Default IAM data imported")))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/iam
  {:depends-on [:synthigy/dataset]
   :doc "Identity & access — users, roles, RBAC/RLS rules"
   :setup (fn []
            ;; One-time: Deploy IAM schema and create default data
            (log/info {:id ::setup-start} "Setting up IAM schema and default data")
            (setup-schema)
            (start)
            (setup-system-users)
            (setup-default-data)
            (log/info {:id ::setup-complete} "IAM schema and default data setup complete"))
   :cleanup (fn []
              ;; One-time: Purge all IAM entities (DESTRUCTIVE)
              (log/info {:id ::purging-entities} "Purging all IAM entities")
              ;; TODO: Implement IAM entity purge
              (log/info {:id ::purged-entities} "IAM entities purged"))
   :start (fn []
            ;; Runtime: Level patches, bind service users, register access control
            (log/info {:id ::lifecycle-start :data {:action :starting}} "Starting IAM")
            (start)
            (log/info {:id ::lifecycle-started :data {:action :started}} "IAM started"))
   :stop (fn []
           (log/info {:id ::lifecycle-stop :data {:action :stopping}} "Stopping IAM")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "IAM stopped"))})


(comment
  (patcho.patch/deployed-version :synthigy.dataset/model)
  (lifecycle/start! :synthigy/transit)
  (lifecycle/start! :synthigy/database)
  (lifecycle/start! :synthigy/dataset)
  (lifecycle/start! :synthigy/iam)
  (lifecycle/cleanup! :synthigy/database)

  (lifecycle/registered-modules)
  (lifecycle/started-modules)
  (lifecycle/print-system-report))
