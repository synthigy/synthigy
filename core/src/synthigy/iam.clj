(ns synthigy.iam
  (:require
    [buddy.core.codecs]
    [buddy.core.hash]
    [buddy.hashers :as hashers]
    clojure.data.json
    clojure.pprint
    clojure.set
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia.resolve :as resolve]
    [com.walmartlabs.lacinia.selection :as selection]
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
    [synthigy.dataset.id :as id :refer [defentity defdata]]
    [synthigy.dataset.sql.naming :refer [entity->table-name]]
    [synthigy.db :refer [*db*]]
    [synthigy.iam.access :as access :refer [*user*]]
    [synthigy.iam.context :as context]
    synthigy.iam.encryption
    [synthigy.iam.events :as events]
    [synthigy.iam.gen :as gen]
    [synthigy.iam.patch]
    [synthigy.iam.patch.model]
    [synthigy.iam.util
     :refer [import-role
             import-api
             import-app]]
    [synthigy.lacinia :as lacinia]))

;; ============================================================================
;; IAM Entity Definitions
;; ============================================================================
;; Well-known entity IDs for IAM schema elements.
;; Uses compile-time multimethod registration for provider-agnostic ID resolution.

(defentity :iam/user
  :euuid #uuid "edcab1db-ee6f-4744-bfea-447828893223" :xid "WN5xU8Do5pcdhTkxXvEYwt")

(defentity :iam/user-group
  :euuid #uuid "95afb558-3d28-45e5-9fbf-a2625afc5675" :xid "KV4seQm27gTAUHCxWERS12")

(defentity :iam/user-role
  :euuid #uuid "4778f7b1-f946-4cb2-b356-b9cb336b4087" :xid "9ptmMENqHWqLFbyidsyF9Y")

(defentity :iam/permission
  :euuid #uuid "6f525f5f-0504-498b-8b92-c353a0f9d141" :xid "EkJBUAxs1zQ5m2C1zXw4e4")

(defentity :iam/service-location
  :euuid #uuid "1029c9bd-dc48-436a-b7a8-2245508a4a72" :xid "2zmE3Enxdtzzs48a4DK6jB")

(defentity :iam/app
  :euuid #uuid "0757bd93-7abf-45b4-8437-2841283edcba" :xid "1ubBUFuDpcpqxY94TECvMw")

(defentity :iam/api
  :euuid #uuid "17b18b52-458e-4657-a827-eb1255de8f1e" :xid "3vhKU5p1QK9KPoyLoYkYW9")

(defentity :iam/scope
  :euuid #uuid "eb03406a-9b0c-4d61-8f96-34e8aa04f13c" :xid "W2BXVawY82expJoFJAAANX")

(defentity :iam/person-info
  :euuid #uuid "b2ccded3-9f8e-49aa-b610-8902ad7330e9" :xid "P5apo8kss7HEgtjdxXozwz")

(defentity :iam/project
  :euuid #uuid "342f54bd-5a1b-4024-8b64-566862f82627" :xid "7Skf8MvReE4XsEQXdtHXWn")

(defdata :iam/id
  :euuid #uuid "c5c85417-0aef-4c44-9e86-8090647d6378" :xid "RRY5JcXr8MwppjvK9bhp7Z")

(defdata :iam.model/version-0.80.0
  :euuid #uuid "7c9981ed-8494-47b0-9580-adc2951819f9" :xid "GPPpgwJqAuSEbRRuoMYLLg")

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
       :avatar nil
       :settings nil
       :person_info [{:selections
                      {:name nil
                       :given_name nil
                       :middle_name nil
                       :nickname nil
                       :prefered_username nil
                       :profile nil
                       :picture nil
                       :website nil
                       :email nil
                       :email_verified nil
                       :gender nil
                       :birth_date nil
                       :zoneinfo nil
                       :phone_number nil
                       :phone_number_verified nil
                       :address nil}}]
       :groups [{:selections {(id/key) nil}}]
       :roles [{:selections {(id/key) nil}}]})
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
  (let [secret (or secret
                   (when (#{:confidential "confidential"} type)
                     (gen/client-secret)))]
    (dataset/sync-entity
      :iam/app
      {:id id
       :name name
       :type type
       :settings settings
       :secret secret
       :active true})))

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

(defn wrap-protect
  [protection resolver]
  (if (not-empty protection)
    (fn wrapped-protection
      [ctx args value]
      ; (log/infof "RESOLVING: %s" resolver)
      ; (def protection protection)
      ; (def resolver resolver)
      ; (def value value)
      ; (def ctx ctx)
      ; (def args args)
      (let [{:keys [scopes roles]}
            (reduce
              (fn [result definition]
                (let [{:keys [scopes roles]} (selection/arguments definition)]
                  (->
                    result
                    (update :scopes (fnil clojure.set/union #{}) (set scopes))
                    (update :roles
                            (fnil clojure.set/union #{})
                            (map (fn [r] (if (= :euuid (id/provider-type))
                                           (parse-uuid r)
                                           r))
                                 roles)))))
              nil
              protection)]
        (if (and
              (or (empty? scopes)
                  (some access/scope-allowed? scopes))
              (or (empty? roles)
                  (access/roles-allowed? roles)))
          (resolver ctx args value)
          (resolve/resolve-as
            nil
            {:message "Access denied!"
             :code :unauthorized}))))
    resolver))

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
                :avatar nil
                :active nil
                :type nil})]
    (log/debugf "Initializing %s\n%s" variable data)
    (alter-var-root variable (constantly data))))

(defn start
  []
  (log/info "Initializing IAM...")
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
    (lacinia/add-directive :protect wrap-protect)
    (dataset/reload)
    (log/info "IAM initialized")
    (catch Throwable e
      (log/error e "Failed to initialize IAM")
      (throw e))))

(defn stop
  []
  (when context/*user-context-provider*
    (context/stop! context/*user-context-provider*)
    (alter-var-root #'context/*user-context-provider* (constantly nil)))
  (dosync
    (ref-set lacinia/compiled nil)
    (ref-set lacinia/state nil)))

(defn setup-schema
  "Sets up the IAM database schema.

  This loads and deploys the IAM dataset definition (entities, relations, attributes)
  that define the structure of users, roles, groups, permissions, etc.

  Should be called during initial database setup, before setup-data."
  []
  (log/info "Loading IAM schema from dataset/iam.json")
  (let [iam-schema (dataset/<-resource "dataset/iam.json")]
    ;; Mount the IAM schema (create tables, etc.)
    (as-> iam-schema model
      (core/mount *db* model)
      (core/reload *db* model))
    (log/info "Mounted IAM schema")

    ;; Deploy the IAM schema to version history
    (core/deploy! *db* iam-schema)
    (log/info "Deployed IAM schema to history")))

(defn setup-system-users
  "Creates essential system users (SYNTHIGY service user).

  Should be called after setup-schema but before setup-data."
  []
  (log/info "Creating *SYNTHIGY* system user")
  (binding [core/*return-type* :edn]
    (dataset/sync-entity :iam/user *SYNTHIGY*)
    (bind-service-user #'synthigy.data/*SYNTHIGY*))
  (log/info "*SYNTHIGY* system user created"))

(defn setup-default-data
  "Imports default IAM data (apps, APIs, roles).

  Should be called as *SYNTHIGY* user after system users are created."
  []
  (binding [*user* (:_eid *SYNTHIGY*)]
    (dataset/sync-entity :iam/user-role *ROOT*)
    (dataset/sync-entity
      :iam/user
      (assoc *PUBLIC_USER* :roles [*PUBLIC_ROLE*]))
    (log/info "Importing default IAM data")
    (import-app "exports/app_synthigy_frontend.json")
    (import-api "exports/api_synthigy_graphql.json")
    (doseq [role ["exports/role_dataset_developer.json"
                  "exports/role_dataset_modeler.json"
                  "exports/role_dataset_explorer.json"
                  "exports/role_iam_admin.json"
                  "exports/role_iam_user.json"]]
      (import-role role))
    (log/info "Default IAM data imported")))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/iam
  {:depends-on [:synthigy/dataset]
   :setup (fn []
            ;; One-time: Deploy IAM schema and create default data
            (log/info "[IAM] Setting up IAM schema and default data...")
            (setup-schema)
            (start)
            (setup-system-users)
            (setup-default-data)
            (log/info "[IAM] IAM schema and default data setup complete"))
   :cleanup (fn []
              ;; One-time: Purge all IAM entities (DESTRUCTIVE)
              (log/info "[IAM] Purging all IAM entities...")
              ;; TODO: Implement IAM entity purge
              (log/info "[IAM] IAM entities purged"))
   :start (fn []
            ;; Runtime: Level patches, bind service users, register access control
            (log/info "[IAM] Starting IAM...")
            (start)
            (log/info "[IAM] IAM started"))
   :stop (fn []
           ;; Runtime: Clear lacinia state
           (log/info "[IAM] Stopping IAM...")
           (stop)
           (log/info "[IAM] IAM stopped"))})


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
