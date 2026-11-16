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

(ns synthigy.files.setup
  "The File System model and file roles the module ships."
  (:require
   [babashka.fs :as fs]
   [clojure.string :as str]
   [environ.core :refer [env]]
   [next.jdbc :as jdbc]
   [patcho.patch :as patch]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db]
   [synthigy.files.model :as model]
   [synthigy.files.storage :as storage]
   [synthigy.files.storage.local :as local]
   [synthigy.iam.access :as access]
   [synthigy.iam.transfer :as transfer]
   [synthigy.log :as log]
   [version-clj.core :as version]))

(def model-resource "dataset/file_system.json")

(def role-resources
  ["exports/role_file_admin.json"
   "exports/role_file_user.json"
   "exports/role_file_reader.json"])

(defn shipped-version [] (dataset/<-resource model-resource))

(defn import-roles!
  "Import the file roles and reload IAM's rules now; its own reload waits for a debounced delta."
  []
  (access/with-principal nil
    (doseq [r role-resources]
      (transfer/import-role r)))
  (access/load-rules))

(defn level-model!
  "Deploy the shipped File System version when none is deployed or the deployed one is older, then stack the file roles onto it."
  []
  (access/with-principal nil
    (let [{shipped :name :as v} (shipped-version)
          {deployed :name} (dataset/latest-deployed-version (id/data :files/dataset))]
      (when (or (nil? deployed) (version/newer? shipped deployed))
        (log/info {:id ::deploying :data {:action :deploying :subject :files-model
                                          :shipped shipped :deployed deployed}}
                  "Deploying the File System model")
        (dataset/deploy! v)
        (dataset/reload)
        (import-roles!)))))

(defn local-storage? []
  (contains? #{nil "local"} (some-> (env :synthigy-files-storage) str/trim str/lower-case not-empty)))

(def sharded-key #"([A-Za-z0-9]{22})-([A-Za-z0-9]{22})")

(defn move-shards!
  "Move `<2 chars>/<file>-<version>` blobs under `root` to `<file>/<version>`, any other key to `<key>`; answers how many moved."
  [root]
  (let [shards (when (fs/directory? root)
                 (filterv #(and (fs/directory? %) (= 2 (count (str (fs/file-name %))))) (fs/list-dir root)))
        moved (volatile! 0)]
    (doseq [shard shards
            blob (vec (fs/list-dir shard))
            :when (fs/regular-file? blob)
            :let [n (str (fs/file-name blob))
                  [_ f v] (re-matches sharded-key n)]]
      (let [target (if f (fs/path root f v) (fs/path root n))]
        (storage/private-dirs! (fs/parent target))
        (fs/move blob target {:atomic-move true})
        (vswap! moved inc)))
    (doseq [shard shards :when (fs/exists? shard)]
      (try (fs/delete shard) (catch java.nio.file.DirectoryNotEmptyException _)))
    @moved))

(defn rekey-rows!
  "Rewrite File storage keys `<file>-<version>` to `<file>/<version>`; answers the row count."
  []
  (if-not (model/deployed?)
    0
    (let [t (naming/entity->table-name (model/entity :files/file))
          c (str \" (model/column :files/file :files.file/storage-key) \")]
      (-> (jdbc/execute-one! (:datasource db/*db*)
                             [(format "UPDATE \"%s\" SET %s = substr(%s, 1, 22) || '/' || substr(%s, 24) WHERE length(%s) = 45 AND substr(%s, 23, 1) = '-'"
                                      t c c c c c)])
          :next.jdbc/update-count))))

(defn rekey-local!
  "Local store only: blobs to `<file>/<version>`, File rows to the matching keys."
  []
  (when (local-storage?)
    (let [blobs (move-shards! (fs/path local/root))
          rows (rekey-rows!)]
      (log/info {:id ::rekeyed :data {:action :migrated :subject :files-storage :blobs blobs :rows rows}}
                "Local file store moved to <file>/<version>"))))

(patch/current-version :synthigy/files "1.1.0")

(patch/upgrade :synthigy/files "1.0.0"
               (import-roles!))

(patch/upgrade :synthigy/files "1.1.0"
               (rekey-local!))
