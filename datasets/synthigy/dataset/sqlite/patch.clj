(ns synthigy.dataset.sqlite.patch
  "SQLite schema patches and migrations.

  This namespace will contain version-based patches for SQLite schema evolution.
  Currently empty - patches will be added as needed."
  (:require
    [clojure.tools.logging :as log]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]))

(log/info "[SQLITE] Patch system loaded (no patches registered yet)")



(patch/current-version :synthigy.dataset/model (:name (dataset/current-dataset-version)))

;; Installed model version from __deploy_history
(patch/installed-version
  :synthigy.dataset/model
  (or (some-> (dataset/latest-deployed-version (id/data :dataset/id))
              :name
              str)
      "0"))
