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
    [clojure.tools.logging :as log]
    [patcho.patch :as patch]
    [synthigy.data :refer [*SYNTHIGY*]]
    [synthigy.dataset :as dataset]
    [synthigy.iam.access :refer [*user*]]
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
               (log/info "[IAM Model] Deploying IAM dataset v0.80.0")
               (dataset/deploy! (current-iam-model))
               (log/info "[IAM Model] Importing default OAuth apps and roles")
               (binding [*user* *SYNTHIGY*]
                 (import-app "exports/app_synthigy_frontend.json")
                 (import-api "exports/api_synthigy_graphql.json")
                 (doseq [role ["exports/role_dataset_developer.json"
                               "exports/role_dataset_modeler.json"
                               "exports/role_dataset_explorer.json"
                               "exports/role_iam_admin.json"
                               "exports/role_iam_user.json"]]
                   (import-role role))))
