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

(ns synthigy.iam.patch.model
  "Declarative patches for the IAM dataset model (iam.json); level via
   (patch/level! :synthigy.iam/model)."
  (:require
    [synthigy.log :as log]
    [patcho.patch :as patch]
    [synthigy.data :refer [*SYNTHIGY*]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.iam.access :as access :refer [with-principal]]
    [synthigy.iam.transfer :as transfer]))

(defn current-iam-model
  "The IAM model from resources/dataset/iam.json, adapted to the current ID
   provider format."
  []
  (dataset/<-resource "dataset/iam.json"))

(patch/current-version :synthigy.iam/model
                       (:name (current-iam-model)))

(patch/installed-version :synthigy.iam/model
                         (or (some-> (dataset/latest-deployed-version (synthigy.dataset.id/data :iam/id))
                                     :name
                                     str)
                             "0"))

(patch/upgrade :synthigy.iam/model
               "0.80.0"
               (log/info {:id ::deploying-0-80-0 :data {:action :deploying :subject :iam-model :version "0.80.0"}}
                         "Deploying IAM dataset v0.80.0")
               (dataset/deploy! (current-iam-model))
               (log/info {:id ::importing-default-data :data {:action :importing :subject :iam-defaults}}
                         "Importing default OAuth apps and roles")
               (with-principal nil
                 (transfer/import-app "exports/app_synthigy_tools.json")
                 (transfer/import-api "exports/api_synthigy.json")
                 (doseq [role ["exports/role_dataset_developer.json"
                               "exports/role_dataset_modeler.json"
                               "exports/role_dataset_explorer.json"
                               "exports/role_iam_admin.json"
                               "exports/role_iam_user.json"]]
                   (transfer/import-role role))))

(patch/upgrade :synthigy.iam/model
               "0.80.1"
               (log/info {:id ::deploying-0-80-1 :data {:action :deploying :subject :iam-model :version "0.80.1"}}
                         "Deploying IAM dataset v0.80.1")
               (dataset/deploy! (current-iam-model)))

(patch/upgrade :synthigy.iam/model
               "0.80.3"
               (log/info {:id ::deploying-0-80-3 :data {:action :deploying :subject :iam-model :version "0.80.3"}}
                         "Deploying IAM dataset v0.80.3 (audit config on IAM entities)")
               (dataset/deploy! (current-iam-model)))

(patch/upgrade :synthigy.iam/model
               "0.81.0"
               (log/info {:id ::deploying-0-81-0 :data {:action :deploying :subject :iam-model :version "0.81.0"}}
                         "Deploying IAM dataset v0.81.0 (Auth Connector entity)")
               (dataset/deploy! (current-iam-model)))

(patch/upgrade :synthigy.iam/model
               "0.85.3"
               (log/info {:id ::deploying-0-85-3 :data {:action :deploying :subject :iam-model :version "0.85.3"}}
                         "Deploying IAM dataset v0.85.3 (Dataset entity + User Role/Dataset RLS guards)")
               (dataset/deploy! (current-iam-model)))

(patch/upgrade :synthigy.iam/model
               "0.85.4"
               (log/info {:id ::deploying-0-85-4 :data {:action :deploying :subject :iam-model :version "0.85.4"}}
                         "Deploying IAM dataset v0.85.4 (Dataset RLS write guard + Dataset Version entity/guard)")
               (dataset/deploy! (current-iam-model)))

(patch/upgrade :synthigy.iam/model
               "0.85.8"
               (log/info {:id ::deploying-0-85-8 :data {:action :deploying :subject :iam-model :version "0.85.8"}}
                         "Deploying IAM dataset v0.85.8 (User role guard removed; User Role group-scoped)")
               (dataset/deploy! (current-iam-model)))

(defn copy-create-grants-to-update!
  "CRUDOB migration: every :create grant also becomes :update, so no existing
   role narrows on upgrade. `create entities` IS the old `write entities`
   renamed, so its links are the pre-split write set."
  []
  (let [create-field (access/grant-field :iam/role->create-entities)
        update-field (access/grant-field :iam/role->update-entities)
        {:keys [cardinality]} (dataset/deployed-relation
                               (id/relation :iam/role->update-entities))]
    (when-not (and create-field update-field)
      (throw (ex-info "CRUDOB grant relations are not both deployed"
                      {:create create-field :update update-field})))
    ;; a to-one link table keeps ONE grant per role — the copy would look like
    ;; it worked and leave every role with a single update entity
    (when-not (= "m2m" cardinality)
      (throw (ex-info "`update entities` must be m2m before grants can be copied"
                      {:cardinality cardinality})))
    (let [roles (dataset/search-entity
                 :iam/user-role nil
                 {(id/key) nil :name nil
                  create-field [{:selections {(id/key) nil}}]})
          copied (reduce
                  (fn [n role]
                    (if-let [grants (seq (get role create-field))]
                      (do (dataset/stack-entity
                           :iam/user-role
                           {(id/key) (id/extract role)
                            update-field (mapv #(select-keys % [(id/key)]) grants)})
                          (inc n))
                      n))
                  0
                  roles)]
      (log/info {:id ::crudob-grants-copied
                 :data {:action :migrated :subject :role-access
                        :roles copied :of (count roles)}}
                "Copied create grants to update")
      copied)))

(patch/upgrade :synthigy.iam/model
               "0.85.11"
               (log/info {:id ::deploying-0-85-11 :data {:action :deploying :subject :iam-model :version "0.85.11"}}
                         "Deploying IAM dataset v0.85.11 (CRUDOB: write entities split into create + update)")
               (dataset/deploy! (current-iam-model))
               (with-principal nil (copy-create-grants-to-update!)))

;; Data-only — no dataset/deploy!, the model shape doesn't change. Links
;; Synthigy Tools (the CLI/frontend OAuth client) to the Synthigy API row
;; it has always actually called, so client-audience (token.clj) has an
;; entitlement to resolve — see docs/plans/PLAN-AUDIENCE-BINDING.md step 2.
;; import-app is :mode :stack (additive-only, re-import-safe): re-running
;; it against an already-migrated instance only adds the new :apis ref.
(patch/upgrade :synthigy.iam/model
               "0.85.12"
               (log/info {:id ::deploying-0-85-12 :data {:action :deploying :subject :iam-model :version "0.85.12"}}
                         "Deploying IAM dataset v0.85.12 (link Synthigy Tools to the Synthigy API)")
               (with-principal nil (transfer/import-app "exports/app_synthigy_tools.json")))

(patch/upgrade :synthigy.iam/model
               "0.85.13"
               (log/info {:id ::deploying-0-85-13 :data {:action :deploying :subject :iam-model :version "0.85.13"}}
                         "Deploying IAM dataset v0.85.13 (User Owner Group + owner-group RLS guard)")
               (dataset/deploy! (current-iam-model))
               (with-principal nil (transfer/import-role "exports/role_user_provisioner.json")))
