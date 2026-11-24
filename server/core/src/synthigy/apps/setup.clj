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

(ns synthigy.apps.setup
  "The Synthigy Apps model and App roles the module ships."
  (:require
   [patcho.patch :as patch]
   [synthigy.apps.model]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam.access :as access]
   [synthigy.iam.transfer :as transfer]
   [synthigy.log :as log]
   [version-clj.core :as version]))

(def model-resource "dataset/synthigy_apps.json")

(def role-resources
  ["exports/role_app_deployer.json"
   "exports/role_app_program.json"
   "exports/role_app_developer.json"])

(defn shipped-version [] (dataset/<-resource model-resource))

(defn import-roles!
  "Import the App roles and reload IAM's rules now; its own reload waits for a debounced delta."
  []
  (access/with-principal nil
    (doseq [r role-resources]
      (transfer/import-role r)))
  (access/load-rules))

(defn level-model!
  "Deploy the shipped Synthigy Apps version when none is deployed or the deployed one is older, then stack the client roles onto it."
  []
  (access/with-principal nil
    (let [{shipped :name :as v} (shipped-version)
          {deployed :name} (dataset/latest-deployed-version (id/data :apps/dataset))]
      (when (or (nil? deployed) (version/newer? shipped deployed))
        (log/info {:id ::deploying :data {:action :deploying :subject :apps-model
                                          :shipped shipped :deployed deployed}}
                  "Deploying the Synthigy Apps model")
        (dataset/deploy! v)
        (dataset/reload)
        (import-roles!)))))

(patch/current-version :synthigy/apps "1.5.0")
