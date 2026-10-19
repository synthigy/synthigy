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
