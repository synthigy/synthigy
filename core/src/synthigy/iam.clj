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

(ns synthigy.iam
  (:require
   [buddy.core.codecs]
   [buddy.core.hash]
   [buddy.hashers :as hashers]
   clojure.pprint
   clojure.set
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor]
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
   [synthigy.iam.context.protocols :as context.protocols]
   synthigy.iam.encryption
   [synthigy.iam.events :as events]
   [synthigy.iam.gen :as gen]
   [synthigy.iam.patch]
   [synthigy.iam.patch.model]
   [synthigy.iam.transfer :as transfer]
   [clojure.core.async :as async]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.sql.query :as query]
   [synthigy.db.sql :as db.sql]))

(def subscription events/subscription)
(def publisher events/publisher)
(def publish events/publish)

;; Privilege checks: use access/superuser?; never a bare ROOT-role predicate —
;; it misses the nil-principal and SYNTHIGY-service-user escapes.

(comment
  (delete-user (context/get-user-details {:name "oauth_test"})))

(defn validate-password
  [user-password password-hash]
  (hashers/check user-password password-hash))

(defn jwt-token? [token]
  (= 2 (count (re-seq #"\." token))))

(def ^:private client-selections
  {(id/key) nil
   :id nil
   :name nil
   :type nil
   :active nil
   :secret nil
   :settings nil})

(defn get-client
  [id]
  (dataset/get-entity :iam/app {:id id} client-selections))

(defn get-client-by-key
  "Resolve a client by its primary id (xid/euuid) rather than the client_id
   string."
  [k]
  (dataset/get-entity :iam/app {(id/key) k} client-selections))

(defn add-client [{:keys [id name secret settings type apis]
                   :or {id (gen/client-id)
                        type :public}}]
  (let [confidential? (#{:confidential "confidential"} type)
        created? (nil? (get-client id))
        raw-secret (or secret
                       (when (and confidential? created?)
                         (gen/client-secret)))
        client (dataset/stack-entity
                :iam/app
                (cond-> {:id id
                         :name name
                         :type type
                         :settings settings
                         :active true}
                  raw-secret (assoc :secret raw-secret)
                  (seq apis) (assoc :apis apis)))]
    (when confidential?
      (dataset/stack-entity
       :iam/user
       {:name id
        :type :SERVICE
        :active true}))
    (cond-> (assoc client :created? created?)
      raw-secret (assoc :secret raw-secret))))

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
  ;; preserve :active across boots — the operator's public-access toggle
  ;; must survive a restart; only a missing row gets the inactive default
  (let [existing (dataset/get-entity
                  :iam/user
                  {(id/key) (id/extract *PUBLIC_USER*)}
                  {(id/key) nil :active nil})]
    (dataset/sync-entity
     :iam/user
     (assoc *PUBLIC_USER*
            :active (boolean (:active existing))
            :roles [*PUBLIC_ROLE*]))))

(defn current-version
  "Returns the IAM model from resources, adapted to current ID provider format."
  []
  (dataset/<-resource "dataset/iam.json"))

(patch/current-version :synthigy.iam/model (:name (current-version)))

(defn bind-service-user
  "Load a service user from the database and rebind the var to it."
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

(def ^:private profile-cleanup-sub-key ::orphan-profile-cleanup)

(defn cleanup-orphaned-profiles!
  "Delete profile satellite rows that no longer link to any user; returns {label
   deleted-count}."
  []
  (reduce
   (fn [result label]
     (let [{link :table
            profile-table :to/table
            to-field :to/field}
           (get-in (query/deployed-schema)
                   [(id/entity :iam/user) :relations label])]
       (if-not link
         result
         (let [{n :next.jdbc/update-count}
               (db.sql/execute-one!
                [(format "delete from \"%s\" p where not exists (select 1 from \"%s\" l where l.%s = p._eid)"
                         profile-table link to-field)])]
           (when (pos? (or n 0))
             (log/info {:id ::orphaned-profiles-cleaned
                        :data {:action :deleted :subject label :count n}}
                       "Cleaned orphaned profile rows"))
           (assoc result label (or n 0))))))
   {}
   [:person_info :public_profile]))

(defn debounced
  "Return a delta handler that runs `f` once, `ms` after the last delta in a
   burst."
  [ms f]
  (let [latest (atom 0)]
    (fn [_env]
      (let [tick (swap! latest inc)]
        (async/go
          (async/<! (async/timeout ms))
          (when (= tick @latest)
            (try (f)
                 (catch Throwable e
                   (log/error! {:id ::profile-cleanup-failed} e)))))))))

(defn register-reference-types!
  "Registers the user/group/role attribute types. Must run before ANY model
   carrying one is mounted — an unregistered type compiles to a bare `user`
   column."
  []
  (doseq [[type-name entity-key] [["user" :iam/user]
                                  ["group" :iam/user-group]
                                  ["role" :iam/user-role]]]
    (core/register-reference-type!
     type-name (id/entity entity-key)
     {:table-fn #(entity->table-name
                  (or (core/get-entity (dataset/deployed-model) (id/entity entity-key))
                      (core/get-entity (:model (current-version)) (id/entity entity-key))))})))

(defn start
  []
  (log/info {:id ::initializing} "Initializing IAM")
  (try
    (register-reference-types!)
    ;; dataset/start already compiled the schema before reference types existed — recompile now, or any ref-typed field (e.g. Owner Group) resolves :reference/entity nil until the reload below.
    (dataset/reload)

    (supervisor/progress-update!
     {:detail (str "patching :synthigy.iam/model "
                   (patch/deployed-version :synthigy.iam/model) " \u2192 "
                   (patch/version :synthigy.iam/model))})
    (patch/level! :synthigy.iam/model
                  :synthigy/iam
                  :synthigy.iam/audit)

    (context/start)

    (ensure-public)

    (binding [core/*return-type* :edn]
      (bind-service-user #'*PUBLIC_USER*)
      (bind-service-user #'synthigy.data/*SYNTHIGY*))

    (alter-var-root #'dataset.access/*access-control*
                    (constantly (access/->IAMAccessControl)))
    (dataset/reload)

    (delta/subscribe!
     profile-cleanup-sub-key
     {:entity-xids #{(str (id/entity :iam/user))}
      :ops #{:delete}}
     (debounced 10000 cleanup-orphaned-profiles!))

    (log/info {:id ::initialized} "IAM initialized")
    (catch Throwable e
      (log/error! {:id ::initialize-failed
                   :msg "Failed to initialize IAM"} e)
      (throw e))))

(defn stop
  []
  (delta/unsubscribe! profile-cleanup-sub-key)
  (when context/*user-context-provider*
    (context.protocols/stop! context/*user-context-provider*)
    (alter-var-root #'context/*user-context-provider* (constantly nil))))

(defn setup-schema
  "Deploy the IAM dataset schema and the OAuth store dataset."
  []
  (log/info {:id ::loading-schema} "Loading IAM schema from dataset/iam.json")
  ;; setup runs BEFORE start — the IAM model carries user/group-typed
  ;; attributes, and mounting it unregistered emits a bare `user` column type
  (register-reference-types!)
  (let [iam-schema (dataset/<-resource "dataset/iam.json")]
    (as-> iam-schema model
      (core/mount *db* model)
      (core/reload *db* model))
    (log/info {:id ::schema-mounted} "Mounted IAM schema")

    (core/deploy! *db* iam-schema)
    (log/info {:id ::schema-deployed :data {:action :deployed :subject :iam-schema}} "Deployed IAM schema to history"))

  ;; OAuth store must deploy before setup-default-data imports apps/APIs into
  ;; it.
  (log/info {:id ::loading-oauth-store} "Deploying OAuth store schema from dataset/oauth_session.json")
  (dataset/deploy! (dataset/<-resource "dataset/oauth_session.json"))
  (dataset/reload))

(defn setup-system-users
  "Create the *SYNTHIGY* system user."
  []
  (log/info {:id ::creating-synthigy-user} "Creating *SYNTHIGY* system user")
  (binding [core/*return-type* :edn]
    (dataset/sync-entity :iam/user *SYNTHIGY*)
    (bind-service-user #'synthigy.data/*SYNTHIGY*))
  (log/info {:id ::created-synthigy-user} "*SYNTHIGY* system user created"))

(defn setup-default-data
  "Import default IAM apps, APIs, and roles as the system principal."
  []
  (with-principal nil
    (dataset/sync-entity :iam/user-role *ROOT*)
    (dataset/sync-entity
     :iam/user
     (assoc *PUBLIC_USER* :roles [*PUBLIC_ROLE*]))
    (log/info {:id ::importing-default-data} "Importing default IAM data")
    (transfer/import-app "exports/app_synthigy_tools.json")
    (transfer/import-api "exports/api_synthigy.json")
    (doseq [role ["exports/role_dataset_developer.json"
                  "exports/role_dataset_modeler.json"
                  "exports/role_dataset_explorer.json"
                  "exports/role_iam_admin.json"
                  "exports/role_user_admin.json"
                  "exports/role_role_admin.json"
                  ;; App Developer + Identity Admin: deferred presets, not
                  ;; seeded — see docs iam/default-roles.md. Files stay for
                  ;; historical patches.
                  "exports/role_iam_auditor.json"
                  "exports/role_internal_member.json"
                  "exports/role_access_admin.json"
                  "exports/role_department_admin.json"
                  "exports/role_user_provisioner.json"]]
      (transfer/import-role role))
    (log/info {:id ::imported-default-data} "Default IAM data imported")))

(lifecycle/register-module!
 :synthigy/iam
 {:depends-on [:synthigy/dataset]
  :headline true
  :doc "Identity & access — users, roles, RBAC/RLS rules"
  :setup (fn []
           (log/info {:id ::setup-start} "Setting up IAM schema and default data")
           (supervisor/progress-update! {:detail "IAM schema"})
           (setup-schema)
           (start)
           (supervisor/progress-update! {:detail "IAM system users"})
           (setup-system-users)
           (supervisor/progress-update! {:detail "IAM default roles"})
           (setup-default-data)
           (log/info {:id ::setup-complete} "IAM schema and default data setup complete"))
  :cleanup (fn []
             (log/info {:id ::purging-entities} "Purging all IAM entities")
              ;; TODO: Implement IAM entity purge
             (log/info {:id ::purged-entities} "IAM entities purged"))
  :start (fn []
           (log/info {:id ::lifecycle-start :data {:action :starting}} "Starting IAM")
           (supervisor/progress-update! {:phase "starting :synthigy/iam" :detail nil})
           (start)
           ;; Keep access/start out of `start` — the :setup path would run it
           ;; before setup-default-data has imported the roles.
           (access/start)
           (log/info {:id ::lifecycle-started :data {:action :started}} "IAM started"))
  :stop (fn []
          (log/info {:id ::lifecycle-stop :data {:action :stopping}} "Stopping IAM")
          (access/stop)
          (stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "IAM stopped"))})

(comment
  (comment
    (dataset/sync-entity
     :iam/user-role
     {:name "Internal Member"
      :description "This role is used for internal members so that it has access to
       entities protected by RBAC that are important organisation wide. Like access to
       users, user groups and user roles in project management or process management.

       Delegating tasks to other departments requires user to see those. That is why
       every employee/organisation member should be marked as internal"
      :active true})
    (dataset/sync-entity
     :iam/user-role
     {:name "External Guest"
      :description "Role that is given to external guest to see redacted and protected part
       of your data!"
      :active true}))
  (patcho.patch/deployed-version :synthigy.dataset/model)
  (lifecycle/start! :synthigy/transit)
  (lifecycle/start! :synthigy/database)
  (lifecycle/start! :synthigy/dataset)
  (lifecycle/start! :synthigy/iam)

  (lifecycle/registered-modules)
  (lifecycle/started-modules)
  (lifecycle/print-system-report))
