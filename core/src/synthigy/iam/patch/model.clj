;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.iam.patch.model
  "Declarative patches for the Synthigy system model (synthigy.json); level via
   (patch/level! :synthigy/model)."
  (:require
    [synthigy.log :as log]
    [patcho.patch :as patch]
    [synthigy.data :refer [*SYNTHIGY*]]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.iam.access :as access :refer [with-principal]]
    [synthigy.iam.transfer :as transfer]))

(patch/current-version :synthigy/model
                       (:name (dataset/current-synthigy-version)))

(patch/installed-version :synthigy/model
                         (or (some-> (dataset/latest-deployed-version (id/data :synthigy/id))
                                     :name
                                     str)
                             "0"))

(patch/upgrade :synthigy/model
               "0.80.0"
               (log/info {:id ::deploying-0-80-0 :data {:action :deploying :subject :iam-model :version "0.80.0"}}
                         "Deploying IAM dataset v0.80.0")
               (dataset/deploy! (dataset/current-synthigy-version))
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

(patch/upgrade :synthigy/model
               "0.80.1"
               (log/info {:id ::deploying-0-80-1 :data {:action :deploying :subject :iam-model :version "0.80.1"}}
                         "Deploying IAM dataset v0.80.1")
               (dataset/deploy! (dataset/current-synthigy-version)))

(patch/upgrade :synthigy/model
               "0.80.3"
               (log/info {:id ::deploying-0-80-3 :data {:action :deploying :subject :iam-model :version "0.80.3"}}
                         "Deploying IAM dataset v0.80.3 (audit config on IAM entities)")
               (dataset/deploy! (dataset/current-synthigy-version)))

(patch/upgrade :synthigy/model
               "0.81.0"
               (log/info {:id ::deploying-0-81-0 :data {:action :deploying :subject :iam-model :version "0.81.0"}}
                         "Deploying IAM dataset v0.81.0 (Auth Connector entity)")
               (dataset/deploy! (dataset/current-synthigy-version)))

(patch/upgrade :synthigy/model
               "0.85.3"
               (log/info {:id ::deploying-0-85-3 :data {:action :deploying :subject :iam-model :version "0.85.3"}}
                         "Deploying IAM dataset v0.85.3 (Dataset entity + User Role/Dataset RLS guards)")
               (dataset/deploy! (dataset/current-synthigy-version)))

(patch/upgrade :synthigy/model
               "0.85.4"
               (log/info {:id ::deploying-0-85-4 :data {:action :deploying :subject :iam-model :version "0.85.4"}}
                         "Deploying IAM dataset v0.85.4 (Dataset RLS write guard + Dataset Version entity/guard)")
               (dataset/deploy! (dataset/current-synthigy-version)))

(patch/upgrade :synthigy/model
               "0.85.8"
               (log/info {:id ::deploying-0-85-8 :data {:action :deploying :subject :iam-model :version "0.85.8"}}
                         "Deploying IAM dataset v0.85.8 (User role guard removed; User Role group-scoped)")
               (dataset/deploy! (dataset/current-synthigy-version)))

(defn copy-create-grants-to-update!
  "CRUDOB migration: copy every :create grant to :update so no role narrows on upgrade."
  []
  (let [create-field (access/grant-field :iam/role->create-entities)
        update-field (access/grant-field :iam/role->update-entities)
        {:keys [cardinality]} (dataset/deployed-relation
                               (id/relation :iam/role->update-entities))]
    (when-not (and create-field update-field)
      (throw (ex-info "CRUDOB grant relations are not both deployed"
                      {:create create-field :update update-field})))
    ;; a to-one link table keeps ONE grant per role — the copy would silently lose grants
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

(patch/upgrade :synthigy/model
               "0.85.11"
               (log/info {:id ::deploying-0-85-11 :data {:action :deploying :subject :iam-model :version "0.85.11"}}
                         "Deploying IAM dataset v0.85.11 (CRUDOB: write entities split into create + update)")
               (dataset/deploy! (dataset/current-synthigy-version))
               (with-principal nil (copy-create-grants-to-update!)))

(patch/upgrade :synthigy/model
               "0.85.12"
               (log/info {:id ::deploying-0-85-12 :data {:action :deploying :subject :iam-model :version "0.85.12"}}
                         "Deploying IAM dataset v0.85.12 (link Synthigy Tools to the Synthigy API)")
               (with-principal nil (transfer/import-app "exports/app_synthigy_tools.json")))

(patch/upgrade :synthigy/model
               "0.85.13"
               (log/info {:id ::deploying-0-85-13 :data {:action :deploying :subject :iam-model :version "0.85.13"}}
                         "Deploying IAM dataset v0.85.13 (User Owner Group + owner-group RLS guard)")
               (dataset/deploy! (dataset/current-synthigy-version))
               (with-principal nil (transfer/import-role "exports/role_user_provisioner.json")))

(patch/upgrade :synthigy/model
               "0.90.0"
               (log/info {:id ::deploying-0-90-0 :data {:action :deploying :subject :synthigy-model :version "0.90.0"}}
                         "Deploying Synthigy model v0.90.0 (absorbs the Datasets meta-model; retire follows in iam/start)")
               (dataset/deploy! (dataset/current-synthigy-version)))
