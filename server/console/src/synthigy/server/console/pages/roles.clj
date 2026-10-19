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
