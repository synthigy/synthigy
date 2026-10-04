;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.pages.users
  (:require
   [synthigy.server.console.data :as data]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.xsql.console :as cx]))

(def role-link
  [:roles "Roles" :user_role :shield
   "Roles the account holds directly. Group-inherited roles are not shown here — edit the group to change those."
   {:default "Internal Member" :sub :description}])

(def group-link
  [:groups "Groups" :user_group :users
   "Groups the account belongs to. A group's roles apply on top of the ones above."])

(defn sessions-panel
  [row _panel-notice]
  (widgets/sessions-table (data/my-sessions (:xid row) nil) true))

(defn identities-panel
  [row _panel-notice]
  (widgets/identities-table (data/my-identities (:xid row)) nil true))

(defn profile-panel
  [row _panel-notice]
  (let [profile (data/get-profile (:xid row))]
    [:ty-scroll-container {:custom-scrollbar true :shadow true}
     (widgets/profile-fields "Public profile" :user data/public-profile-fields profile)
     (widgets/profile-fields "Personal info" :lock data/person-info-fields profile)]))

(defn managed-by-app
  [row]
  (when-let [{:keys [xid name]} (:authorized-client row)]
    [:span "Managed by app "
     [:a {:href (str "/console/iam/apps/" xid)} name]
     " — edit its roles and groups there. It is active only while the app is "
     "Confidential and allows Client Credentials."]))

(def spec
  {:slug "users" :entity :user :key :iam/user :label "Users" :icon :user
   :watch [:oauth_session]
   :table {:query cx/users-table
           :watch cx/watch-users-table
           :chips {:groups "groups" :roles "roles"}
           :sortable #{"name" "active" "sessions" "groups" "roles"}}
   :filters [[:groups "Groups" :user_group :users]
             [:roles "Roles" :user_role :shield]]
   :create {:fields [[:name "Name" :text {:required true}]
                     [:type "Type" :choices
                      {:required true :keyword? true
                       :choices [["PERSON" "Person"] ["SERVICE" "Service"] ["ROBOT" "Robot"]]}]
                     [:active "Active" :switch {:default true}]]
            :links [role-link group-link]}
   :delete {:warning (str "Their sessions and sign-in methods stay in the database "
                          "but become unreachable — not a full cleanup. This can't "
                          "be undone.")
            :guard (fn [row principal]
                     (when (= (:xid row) (:xid principal))
                       "You can't delete your own account from here."))}
   :subtitle (str "Every account this deployment knows about. The list is "
                  "governed by your own IAM grants — filtered server-side, not hidden.")
   :columns [[:name "Name" :name] [:active "Status" :status] [:type "Type" :type]
             [:roles "Roles" :agg :shield] [:groups "Groups" :agg :users]
             [:sessions "Sessions" :agg :monitor]]
   :selection-extra [{:authorized-client [{:args {:_join :left} :selections [:xid :name]}]}]
   :detail {:locked managed-by-app
            :fields [[:name   "Name"   :text]
                     [:active "Active" :switch]
                     [:type   "Type"   :enum]]
            :links  [role-link group-link]
            :panels [["Profile" profile-panel]
                     ["Sessions" sessions-panel]
                     ["Sign-in methods" identities-panel]]}})
