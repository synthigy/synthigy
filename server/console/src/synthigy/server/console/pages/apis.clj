;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
     :delete-warning "Every role that grants it loses it."
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
