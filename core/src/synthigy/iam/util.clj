(ns synthigy.iam.util
  (:require
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.json :refer [<-json]]))

(defn import-data
  [path entity]
  (let [{entity-name :name} (dataset/deployed-entity (id/entity entity))]
    (when-some [data (<-json (slurp (io/resource path)) {:keyfn keyword})]
      (log/infof "[IAM] Importing \"%s\" name %s" entity-name (:name data))
      (dataset/stack-entity entity data))))

(defn import-role [path] (import-data path :iam/user-role))
(defn import-api [path] (import-data path :iam/api))
(defn import-app [path] (import-data path :iam/app))

(comment
  ()
  (<-json (slurp (io/resource "exports/app_synthigy_frontend.json")) {:keyfn keyword})
  (import-role "roles/role_dataset_developer.json"))
