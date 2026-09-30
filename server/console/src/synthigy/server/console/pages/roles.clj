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

(ns synthigy.server.console.pages.roles
  (:require [synthigy.xsql.console :as cx]))

(def scopes-link
  [:scopes "Scopes" :oauth_scope :microscope
   "What this role actually grants."
   {:tag :api :sub :description}])

(def users-link
  [:users "Users" :user :user
   (str "Accounts holding this role directly. Bulk-assign here rather than "
        "editing one account at a time — group membership is a separate, "
        "larger-grained way to grant it.")])

(def spec
  {:slug "roles" :entity :user_role :key :iam/user-role :label "Roles" :icon :shield
   :table {:query cx/roles-table
           :watch cx/watch-roles-table
           :sortable #{"name" "active" "users" "scopes"}}
   :subtitle (str "A role is a bundle of scopes. Users and groups hold roles; "
                  "roles are what the engine actually checks.")
   :create {:fields [[:name "Name" :text {:required true}]
                     [:description "Description" :textarea]
                     [:active "Active" :switch {:default true}]]
            :links [scopes-link users-link]}
   :delete {:warning "Every user and group holding it loses the grant immediately."}
   :columns [[:name "Name" :name] [:active "Status" :status]
             [:scopes "Scopes" :agg :microscope] [:users "Users" :agg :user]]
   :detail {:fields [[:name        "Name"        :text]
                     [:active      "Active"      :switch]
                     [:description "Description" :textarea]]
            :links  [scopes-link users-link]}})
