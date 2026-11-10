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

(ns synthigy.files.model
  "Ids of the File System dataset the files module owns."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id :refer [defdata defentity defrelation]]
   [synthigy.dataset.sql.naming :as naming]))

(defdata :files/dataset
  :euuid #uuid "4502962a-088f-4c33-b0f2-9b1ce09f9c96" :xid "9XFsUy7kbgxJdBxdSjDdbs")

(defdata :files/root-folder
  :euuid #uuid "87ce50d8-5dfa-4008-a265-053e727ab793" :xid "Hmevac9kcS24ukE6HFBYqg")

(defentity :files/file
  :euuid #uuid "63b2e70a-2162-423a-be36-4909d7831605" :xid "DK41BPRFuMKYtMzdomVQLU")

(defentity :files/folder
  :euuid #uuid "f03d4db6-da1a-46d2-b3cb-a4211d811729" :xid "WfcjKW6boVAH5LFGehEgKr")

(defrelation :files/file->folder
  :euuid #uuid "4cc0f585-7d95-4bba-aecc-af759845d6bd" :xid "AUib64hVvqcsuEevMb4sSt")

(defrelation :files/folder->parent
  :euuid #uuid "c6286f14-73ea-4eb3-bc49-fcbd9e9ac92c" :xid "RUE2CYwxiT7xcsmEtUwWyh")

(defrelation :files/folder-read-users
  :euuid #uuid "21fcca7e-e67d-48da-9596-f252d2030fbb" :xid "5CRRt4afDgTn8Wi7DgBwf8")

(defrelation :files/folder-write-users
  :euuid #uuid "18624b99-fc7c-42d6-8f2c-4644ca373270" :xid "41e8GqLS9nz6b9Vu9HzuFD")

(defrelation :files/folder-delete-users
  :euuid #uuid "e4ad2b4b-05bb-4314-9d62-fcf0615bda9b" :xid "VEoSL1SgcH4pnTqmJNMAwC")

(defrelation :files/folder-read-groups
  :euuid #uuid "66766a93-20e6-45b3-bbad-20d88e0ae882" :xid "Der4MByKSAX3ZoN2SYesQm")

(defrelation :files/folder-write-groups
  :euuid #uuid "5a177337-c027-42b3-be3e-89ad54b52ae8" :xid "C8F9Xd8CFNFGcHtPzukzUo")

(defrelation :files/folder-delete-groups
  :euuid #uuid "2bcacf23-fbee-4f8c-bd7a-2879a1a1dfa4" :xid "6QeLVLKwj4athvZTz7i9tF")

(def acl-relations
  "Folder access lists: [access kind relation-key]."
  [[:read :users :files/folder-read-users]
   [:write :users :files/folder-write-users]
   [:delete :users :files/folder-delete-users]
   [:read :groups :files/folder-read-groups]
   [:write :groups :files/folder-write-groups]
   [:delete :groups :files/folder-delete-groups]])

(defdata :files.file/name
  :euuid #uuid "dae0d6c3-b18d-4170-b8d7-e5b4f482bd8c" :xid "U2dGaep11h1D1A4MwugxAo")

(defdata :files.file/content-type
  :euuid #uuid "5368a605-e842-46c8-a485-e8656f0eb917" :xid "BJP6djSzq6EYcxCkoG52Fx")

(defdata :files.file/uploaded-by
  :euuid #uuid "ec36acd2-5b39-4994-9bd2-da04a874e739" :xid "WAnNSNBrQ6yCKhGTfGT22U")

(defdata :files.file/storage-key
  :euuid #uuid "81bfd3c8-e2a2-4d10-8ab4-d9af1be5c6a9" :xid "H2H1S8acFYnpJT8RCq57X6")

(defdata :files.file/hash
  :euuid #uuid "d22d52e7-e4fe-470e-9ab0-0f24b1e4a903" :xid "SxJmZ12TZBc7Mf79W6xe4S")

(defdata :files.file/status
  :euuid #uuid "0322b2a0-be72-44bd-a6dc-b91c2856c948" :xid "1PTWLm6WGctcP9r1vuP4Vu")

(defdata :files.file/size
  :euuid #uuid "ec8f5824-1e0a-4ea1-92c6-72914e397fd8" :xid "WDGFUmmMa2nFLWKtMUxaas")

(defdata :files.file/uploaded-at
  :euuid #uuid "78bbeea8-1188-46b0-896a-176f962d8b2b" :xid "Fui5W4EkuxVFsWAAwypqvJ")

(defdata :files.folder/name
  :euuid #uuid "a2cdb986-b03f-459f-a943-978caacf1f97" :xid "M71o1gMCM9iiNab9oicX82")

(defdata :files.folder/path
  :euuid #uuid "5cd7b371-084a-4d5f-a427-54ef9cf30b75" :xid "CTwucGjdgvNnokFyZYuNo6")

(defdata :files.folder/public
  :euuid #uuid "1271cc0b-92cf-4c6c-8ca8-25f98f5a9682" :xid "3H6se2au8ZiTF3w4TPoMEq")

(defdata :files.folder/cache
  :euuid #uuid "b384adbf-be2f-4f7d-9a34-1713e824155a" :xid "PAj5nWDB1x5fNxkC4JkPZT")

(defdata :files.folder/can-link
  :euuid #uuid "80ff8d9f-0202-414a-ada7-22e456fa5170" :xid "Gvu1hAtKVkqeazSx5qx9qR")

(defentity :files/link
  :euuid #uuid "2f8bbcf4-4e59-4fce-9ced-14ba752d09ce" :xid "6sXbeaD2ZZMcbTjYVUVsim")

(defrelation :files/link->file
  :euuid #uuid "ddedb2af-dfa6-48b4-868e-612fd729a29f" :xid "UQULb5Yn13rqBGdseCAv7Q")

(defdata :files.link/token-hash
  :euuid #uuid "b5c17bf9-86b3-4baa-bedc-51944d6478d2" :xid "PSkYyvaLiicVDsa7YSMZAy")

(defdata :files.link/created-by
  :euuid #uuid "e0b93173-9c80-4408-8085-473df49d500c" :xid "UkVLvCPRKV9gEKch5N67vT")

(defdata :files.link/expires-at
  :euuid #uuid "4f428fbd-8c1d-4380-94cb-2fb4e6a4c9db" :xid "Anfh3zoExRGMpBCSdA44ve")

(defdata :files.link/max-downloads
  :euuid #uuid "3044ec25-658f-4e86-a0ee-f33b36c11182" :xid "6xi69JZip5ixq94pPQHHFf")

(defdata :files.link/downloads
  :euuid #uuid "d61f7cd4-e7e6-40ca-9f1d-ebbf97b04055" :xid "TSZvQqzqb3V3yy5GsPGe7E")

(defdata :files.link/disposition
  :euuid #uuid "264ab0dc-1153-4a77-a791-c6f7e08d53bd" :xid "5jFSRDmNxi5nTwsvsozcxL")

(defn entity
  ([entity-key] (entity (dataset/deployed-model) entity-key))
  ([model entity-key] (core/get-entity model (id/entity entity-key))))

(defn relation
  ([relation-key] (relation (dataset/deployed-model) relation-key))
  ([model relation-key] (core/get-relation model (id/relation relation-key))))

(defn column
  "Column name of attribute `attr-key` on entity `entity-key`, from the deployed model."
  ([entity-key attr-key] (column (dataset/deployed-model) entity-key attr-key))
  ([model entity-key attr-key]
   (let [aid (id/data attr-key)]
     (some (fn [a] (when (= aid (id/extract a)) (naming/normalize-name (:name a))))
           (:attributes (entity model entity-key))))))

(defn deployed?
  ([] (deployed? (dataset/deployed-model)))
  ([model] (boolean (and (entity model :files/file) (entity model :files/folder)
                         (relation model :files/file->folder)))))

(defn folder-field
  "Folder-side wire key of relation `relation-key`, from the deployed model."
  ([relation-key] (folder-field (dataset/deployed-model) relation-key))
  ([model relation-key]
   (let [r (relation model relation-key)
         folder-side? (= (id/entity :files/folder) (id/extract (:to r)))]
     (keyword (naming/normalize-name (if folder-side? (:from-label r) (:to-label r)))))))

(defn acl-fields
  "{[access kind] wire-key} of the folder access lists."
  []
  (into {} (map (fn [[access kind k]] [[access kind] (folder-field k)])) acl-relations))

(defn name-problem
  "Why `n` cannot name a file or folder, or nil."
  [n]
  (cond
    (str/blank? n) "The name is empty"
    (> (count n) 255) "The name is longer than 255 characters"
    (re-find #"[/\\\x00-\x1f\x7f]" n) "The name contains / or \\ or a control character"
    (#{"." ".."} n) "The name cannot be . or .."
    (not= n (str/trim n)) "The name starts or ends with a space"))
