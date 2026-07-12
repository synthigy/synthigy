(ns synthigy.iam.patch.model
  "IAM model version patches.

  Patches for the IAM dataset model (iam.json) that define users, roles, groups, etc.

  This handles DATASET/MODEL patches (declarative, model-driven):
  - Model loaded from resources/dataset/iam.json
  - Deployed via dataset/deploy! (which diffs and migrates)

  To level this model:
    (require '[patcho.patch :as patch])
    (patch/level! :synthigy.iam/model)"
  (:require
    [synthigy.log :as log]
    [patcho.patch :as patch]
    [synthigy.data :refer [*SYNTHIGY*]]
    [synthigy.dataset :as dataset]
    [synthigy.iam.access :refer [with-principal]]
    [synthigy.iam.util :refer [import-role import-api import-app]]))



;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn current-iam-model
  "Returns the IAM model from resources/dataset/iam.json,
  adapted to the current ID provider format."
  []
  (dataset/<-resource "dataset/iam.json"))

;; ============================================================================
;; Dataset Model Version Registration
;; ============================================================================

;; Current version from resource file (:name field in model)
(patch/current-version :synthigy.iam/model
                       (:name (current-iam-model)))

;; Installed version from dataset's __deploy_history__ table
(patch/installed-version :synthigy.iam/model
                         (or (some-> (dataset/latest-deployed-version (synthigy.dataset.id/data :iam/id))
                                     :name
                                     str)
                             "0"))

;; ============================================================================
;; Dataset Model Patches
;; ============================================================================

;; Patch 0.80.0 - Deploy IAM model and import OAuth apps/roles
(patch/upgrade :synthigy.iam/model
               "0.80.0"
               (log/info {:id ::deploying-0-80-0 :data {:action :deploying :subject :iam-model :version "0.80.0"}}
                         "Deploying IAM dataset v0.80.0")
               (dataset/deploy! (current-iam-model))
               (log/info {:id ::importing-default-data :data {:action :importing :subject :iam-defaults}}
                         "Importing default OAuth apps and roles")
               (with-principal nil
                 (import-app "exports/app_synthigy_frontend.json")
                 (import-api "exports/api_synthigy_graphql.json")
                 (doseq [role ["exports/role_dataset_developer.json"
                               "exports/role_dataset_modeler.json"
                               "exports/role_dataset_explorer.json"
                               "exports/role_iam_admin.json"
                               "exports/role_iam_user.json"]]
                   (import-role role))))

;; Patch 0.80.1 - Client secret hashing (attribute type changed to Hash)
(patch/upgrade :synthigy.iam/model
               "0.80.1"
               (log/info {:id ::deploying-0-80-1 :data {:action :deploying :subject :iam-model :version "0.80.1"}}
                         "Deploying IAM dataset v0.80.1")
               (dataset/deploy! (current-iam-model)))

;; Patch 0.80.3 - Audit columns on IAM entities (User, Role, Group, OAuth Client/API/Scope, PersonInfo, Project)
(patch/upgrade :synthigy.iam/model
               "0.80.3"
               (log/info {:id ::deploying-0-80-3 :data {:action :deploying :subject :iam-model :version "0.80.3"}}
                         "Deploying IAM dataset v0.80.3 (audit config on IAM entities)")
               (dataset/deploy! (current-iam-model)))
