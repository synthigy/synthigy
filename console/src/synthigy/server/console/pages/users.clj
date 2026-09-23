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
   :detail {:fields [[:name   "Name"   :text]
                     [:active "Active" :switch]
                     [:type   "Type"   :enum]]
            :links  [role-link group-link]
            :panels [["Profile" profile-panel]
                     ["Sessions" sessions-panel]
                     ["Sign-in methods" identities-panel]]}})
