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

(ns synthigy.server.console.pages.apis
  (:require
   [synthigy.iam.gen :as iam.gen]
   [synthigy.xsql.console :as cx]))

(def scopes-link
  [:scopes "Scopes" :oauth_scope :microscope
   "Scopes an app may request against this audience."
   {:sub :description
    :flag :confidential-only
    :create
    {:parent :api
     :validate (fn [{:keys [name]}]
                 (when-not (iam.gen/valid-scope-name? (str name))
                   iam.gen/scope-name-message))
     :fields [[:name "Name" :text]
              [:description "Description" :textarea]
              [:confidential_only "Confidential clients only" :switch]]}}])

(def spec
  {:slug "apis" :entity :oauth_api :key :iam/api :label "APIs" :icon :layers
   :table {:query cx/apis-table
           :watch cx/watch-apis-table
           :sortable #{"name" "apps" "scopes"}}
   :subtitle (str "Token audiences. An API owns the scopes apps may request "
                  "for it.")
   :create {:fields [[:name "Name" :text {:required true}]
                     [:audience "Audience" :text {:required true}]
                     [:description "Description" :textarea]]
            :owned [scopes-link]}
   :delete {:warning (str "Its scopes are deleted with it, and every role that "
                          "grants them loses them. Apps holding this audience can "
                          "no longer get a token for it.")}
   :columns [[:name "Name" :name] [:audience "Audience" :mono]
             [:scopes "Scopes" :agg :microscope] [:apps "Apps" :agg :box]]
   :detail {:fields [[:name        "Name"        :text]
                     [:audience    "Audience"    :text]
                     [:description "Description" :textarea]]
            :links  [scopes-link]}})
