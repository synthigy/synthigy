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

(ns synthigy.server.console.pages.groups
  (:require [synthigy.xsql.console :as cx]))

(def roles-link
  [:roles "Roles" :user_role :shield
   "Roles every member of this group inherits."
   {:sub :description}])

(def users-link
  [:users "Users" :user :user
   "Accounts that belong to this group."])

(def spec
  {:slug "groups" :entity :user_group :key :iam/user-group :label "Groups" :icon :users
   :table {:query cx/groups-table
           :watch cx/watch-groups-table
           :sortable #{"name" "active" "users" "roles"}}
   :subtitle (str "Groups collect users so a role is granted once rather than "
                  "account by account.")
   :create {:fields [[:name "Name" :text {:required true}]
                     [:type "Type" :choices
                      {:required true :keyword? true
                       :choices [["ACCESS" "Access"] ["ROBOTICS" "Robotics"] ["SERVICE" "Service"]]}]
                     [:active "Active" :switch {:default true}]]
            :links [roles-link users-link]}
   :delete {:warning "Every member loses whatever this group granted them, immediately."}
   :columns [[:name "Name" :name] [:active "Status" :status] [:type "Type" :type]
             [:users "Users" :agg :user] [:roles "Roles" :agg :shield]]
   :detail {:fields [[:name   "Name"   :text]
                     [:active "Active" :switch]
                     [:type   "Type"   :enum]]
            :links  [roles-link users-link]}})
