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

(ns synthigy.iam.context
  "Pluggable user-context provider with a cached default implementation."
  (:require
   [synthigy.log :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.iam.keys]
   [synthigy.iam.context.protocols :as protocols
    :refer [UserContextProvider lookup-user invalidate-user start! stop!]]
   [synthigy.iam.access :as access]))

(defonce ^{:dynamic true
           :doc "The current user context provider.

  Can be temporarily overridden with with-user-context-provider macro.
  Set the default provider with set-user-context-provider!

  See UserContextProvider protocol for implementation details."}
  *user-context-provider*
  nil)

(defn set-user-context-provider!
  "Set the default user context provider, stopping the old one and starting the
   new one."
  [provider]
  (when-let [old-provider *user-context-provider*]
    (try
      (stop! old-provider)
      (catch Exception e
        (log/error! {:id ::stop-old-provider-failed
                     :msg "Error stopping old user-context provider"} e))))

  (alter-var-root #'*user-context-provider* (constantly provider))

  (try
    (start! provider)
    (catch Exception e
      (log/error! {:id ::start-new-provider-failed
                   :msg "Error starting new user-context provider"} e)
      (throw e)))

  nil)

(defmacro with-user-context-provider
  "Temporarily override the user context provider for the scope of `body`."
  [provider & body]
  `(binding [*user-context-provider* ~provider]
     ~@body))

(defn group-roles-live?
  "True when the User Group→User Role relation is deployed and active."
  []
  (boolean
   (:active
    (core/get-relation (dataset/deployed-model) (id/relation :iam/group->roles)))))

(defn public-profile-live?
  "True when the User→User Public Profile relation is deployed and active."
  []
  (boolean
   (:active
    (core/get-relation (dataset/deployed-model) (id/relation :oauth/user->public-profile)))))

(def ^:private public-profile-selection
  {:name nil
   :given_name nil
   :family_name nil
   :nickname nil
   :preferred_username nil
   :profile nil
   :picture nil
   :website nil
   :zone_info nil})

;; Selecting a missing/inactive key throws and this resolves the principal —
;; optional keys must stay gated by the liveness probes or login breaks.
(defn user-details-selection
  []
  (cond-> {:_eid nil
           (id/key) nil
           :name nil
           :password nil
           :active nil
           :settings nil
           :person_info [{:args {:_join :left}
                          :selections
                          {:middle_name nil
                           :email nil
                           :email_verified nil
                           :gender nil
                           :birthdate nil
                           :phone_number nil
                           :phone_number_verified nil
                           :address nil}}]
           :groups [{:args {:_join :left}
                     :selections (cond-> {(id/key) nil :_eid nil :name nil}
                                   (group-roles-live?)
                                   (assoc :roles [{:args {:_join :left}
                                                   :selections {(id/key) nil :_eid nil :name nil}}]))}]
           :roles  [{:args {:_join :left} :selections {(id/key) nil :_eid nil :name nil}}]}

    (public-profile-live?)
    (assoc :public_profile [{:args {:_join :left}
                             :selections public-profile-selection}])))

(defn get-user-details
  "Fetch and materialize the principal map for a user; strips `:password` and
   folds group-conferred roles into `:roles`."
  [args]
  (let [materialize (fn [entries]
                      (into {} (map (juxt id/extract identity)) entries))
        group-roles (fn [groups]
                      (into {}
                            (for [g groups
                                  r (:roles g)]
                              [(id/extract r) (assoc r :role/via (id/extract g))])))]
    (some->
     (dataset/get-entity :iam/user args (user-details-selection))
     (dissoc :password)
     (as-> user
           (let [direct (into {}
                              (map (juxt id/extract #(assoc % :role/via :direct)))
                              (:roles user))]
             (assoc user
                    ;; direct last — never relabel a directly-held role as
                    ;; group-derived
                    :roles  (merge (group-roles (:groups user)) direct)
                    :groups (materialize (map #(dissoc % :roles) (:groups user)))))))))

(defn ->CachedUserContextProvider
  "Create a cached user context provider with delta-driven cache invalidation."
  []
  (let [cache (atom {})
        skey  (fn [base] (keyword "synthigy.iam.context"
                                  (str (name base) "-" (System/identityHashCode cache))))
        k-user       (skey "user-cache-invalidator")
        k-membership (skey "user-membership-cache-invalidator")
        k-catalog    (skey "role-catalog-cache-invalidator")]
    (reify UserContextProvider

      (lookup-user [_ identifier]
        (let [user-id (cond
                        (uuid? identifier)
                        identifier

                        (string? identifier)
                        (or
                         (when (get @cache identifier) identifier)
                         (get-in @cache [::name->id identifier])
                         (when-let [user (try
                                           (get-user-details {(id/key) identifier})
                                           (catch Exception e
                                             (log/error! {:id ::lookup-by-id-string-failed
                                                          :msg "Failed to load user by ID"
                                                          :data {:identifier identifier}}
                                                         e)
                                             nil))]
                           (let [uid (id/extract user)]
                             (swap! cache (fn [c]
                                            (-> c
                                                (assoc uid user)
                                                (assoc-in [::name->id (:name user)] uid)
                                                (assoc-in [::eid->id (:_eid user)] uid))))
                             uid))
                         (when-let [user (try
                                           (get-user-details {:name identifier})
                                           (catch Exception e
                                             (log/error! {:id ::lookup-by-name-failed
                                                          :msg "Failed to load user by name"
                                                          :data {:name identifier}}
                                                         e)
                                             nil))]
                           (let [uid (id/extract user)]
                             (swap! cache (fn [c]
                                            (-> c
                                                (assoc uid user)
                                                (assoc-in [::name->id (:name user)] uid)
                                                (assoc-in [::eid->id (:_eid user)] uid))))
                             uid)))

                        (integer? identifier)
                        (or (get-in @cache [::eid->id identifier])
                            (when-let [user (try
                                              (get-user-details {:_eid identifier})
                                              (catch Exception e
                                                (log/error! {:id ::lookup-by-eid-failed
                                                             :msg "Failed to load user by _eid"
                                                             :data {:eid identifier}}
                                                            e)
                                                nil))]
                              (let [uid (id/extract user)]
                                (swap! cache (fn [c]
                                               (-> c
                                                   (assoc uid user)
                                                   (assoc-in [::name->id (:name user)] uid)
                                                   (assoc-in [::eid->id (:_eid user)] uid))))
                                uid)))

                        :else
                        (do
                          (log/warn {:id ::unknown-identifier-type
                                     :data {:identifier identifier
                                            :type (str (type identifier))}}
                                    "Unknown user-context identifier type")
                          nil))]

          (when user-id
            (or (get @cache user-id)
                (when-let [user (try
                                  (get-user-details {(id/key) user-id})
                                  (catch Exception e
                                    (log/error! {:id ::lookup-by-resolved-id-failed
                                                 :msg "Failed to load user by resolved ID"
                                                 :data {:user-id user-id}}
                                                e)
                                    nil))]
                  (swap! cache (fn [c]
                                 (-> c
                                     (assoc user-id user)
                                     (assoc-in [::name->id (:name user)] user-id)
                                     (assoc-in [::eid->id (:_eid user)] user-id))))
                  user)))))

      (invalidate-user [_ identifier]
        (let [user-id (cond
                        (uuid? identifier) identifier
                        (string? identifier)
                        (or (when (get @cache identifier) identifier)
                            (get-in @cache [::name->id identifier]))
                        (integer? identifier) (get-in @cache [::eid->id identifier])
                        :else nil)]

          (when user-id
            (when-let [user (get @cache user-id)]
              (swap! cache (fn [c]
                             (-> c
                                 (dissoc user-id)
                                 (update ::name->id dissoc (:name user))
                                 (update ::eid->id dissoc (:_eid user)))))
              (log/debug {:id ::invalidated-user
                          :data {:user-id user-id}}
                         "Invalidated cached user"))))
        nil)

      (clear-cache [_]
        (reset! cache {})
        (log/info {:id ::cache-cleared} "Cleared all cached user contexts")
        nil)

      (start! [this]
        ;; No TTL — every source feeding the cached principal must invalidate
        ;; it, or a revoked role keeps working forever.
        (let [user-xid (id/entity :iam/user)
              user->roles  (str (id/relation :iam/user->roles))
              user->groups (str (id/relation :iam/user->groups))
              group->roles (str (id/relation :iam/group->roles))]

          ;; precise: User row changed
          (delta/subscribe!
           k-user
           {:entity-xids #{user-xid}}
           (fn [env]
             (when-let [record-xid (some-> env :delta :data :record-xid)]
               (log/debug {:id ::user-changed-invalidating
                           :data {:record-xid record-xid}}
                          "User changed; invalidating cache entry")
               (invalidate-user this record-xid))))

          ;; precise: membership junction writes never touch the User row
          (delta/subscribe!
           k-membership
           {:relation-xids #{user->roles user->groups}}
           (fn [env]
             (let [{:keys [from-xid to-xid]} (some-> env :delta :data)]
               (log/debug {:id ::membership-changed-invalidating
                           :data {:from-xid from-xid :to-xid to-xid}}
                          "Role/group membership changed; invalidating principal")
               (doseq [x [from-xid to-xid] :when x]
                 (invalidate-user this x)))))

          ;; coarse: catalog edits don't name the affected users
          (delta/subscribe!
           k-catalog
           {:relation-xids #{group->roles}
            :entity-xids   #{(id/entity :iam/user-role) (id/entity :iam/user-group)}}
           (fn [_env]
             (log/info {:id ::role-catalog-changed-clearing
                        :data {:action :cleanup :subject :connector-cache}}
                       "Role/group catalog changed; clearing user context cache")
             (reset! cache {}))))
        (log/info {:id ::invalidation-loop-started
                   :data {:action :started :subject :invalidation-loop}}
                  "Started automatic cache invalidation for user entity changes")
        nil)

      (stop! [_]
        (delta/unsubscribe! k-user)
        (delta/unsubscribe! k-membership)
        (delta/unsubscribe! k-catalog)
        (log/info {:id ::invalidation-loop-stopped
                   :data {:action :stopped :subject :invalidation-loop}}
                  "Cache invalidation stopped")
        nil))))

(deftype AtomUserContextProvider [users]
  UserContextProvider

  (lookup-user [_ identifier]
    (let [user-id (cond
                    (uuid? identifier) identifier
                    (string? identifier)
                    (or
                     (when (get @users identifier) identifier)
                     (some (fn [[id user]]
                             (when (= (:name user) identifier)
                               id))
                           @users))
                    (integer? identifier)
                    (some (fn [[id user]]
                            (when (= (:_eid user) identifier)
                              id))
                          @users)
                    :else nil)]
      (when user-id
        (get @users user-id))))

  (invalidate-user [_ _]
    nil)

  (clear-cache [_]
    nil)

  (start! [_]
    nil)

  (stop! [_]
    nil))

(defn get-user-context
  "Retrieve user context for an identifier using the current provider."
  [identifier]
  (when-not *user-context-provider*
    (throw (IllegalStateException. "No user context provider configured. Call set-user-context-provider! first.")))
  (lookup-user *user-context-provider* identifier))

(defmacro with-user-context
  "Bind access/*principal* to the materialized user map for `identifier` and
   execute `body`."
  [identifier & body]
  `(let [user-ctx# (get-user-context ~identifier)]
     (when-not user-ctx#
       (throw (IllegalArgumentException. (str "User not found: " ~identifier))))
     (access/with-principal user-ctx#
       ~@body)))

(defn start
  "Install the default cached user context provider."
  []
  (set-user-context-provider! (->CachedUserContextProvider)))
