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

(ns synthigy.files.guard
  "The data API reads files, folders and their links and links other records to files and folders; only /files creates, changes or deletes them."
  (:require
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.access.protocol :as p]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.files.model :as model]))

(def ^:dynamic *module-write*
  "True while /files writes; the principal stays bound so audit records the user."
  false)

(defrecord ModuleAccess [inner]
  p/AccessControl
  (entity-allows? [_ _ _] true)
  (relation-allows? [_ _ _] true)
  (relation-allows? [_ _ _ _] true)
  (attribute-allows? [_ _ _ _] true)
  (scope-allowed? [_ _] true)
  (roles-allowed? [_ _] true)
  (superuser? [_] true)
  (get-principal [_] (p/get-principal inner))
  (principal-eid [_] (p/principal-eid inner))
  (role-ids [_] (p/role-ids inner))
  (role-eids [_] (p/role-eids inner))
  (group-eids [_] (p/group-eids inner)))

(defn as-module
  "Call `f` as the module: every check passes, the principal stays the actor audit records. Check first, as the caller."
  [f]
  (binding [*module-write* true
            daccess/*access-control* (->ModuleAccess daccess/*access-control*)]
    (f)))

(defn as-endpoint
  "Call `f` as /files on the caller's behalf: this guard lets it through, the caller's grants and row rules still apply."
  [f]
  (binding [*module-write* true]
    (f)))

(def link-hint
  "Link your record to the file or folder by its xid alone: {\"<relation>\": {\"xid\": \"<xid>\"}} to one, {\"<relation>\": [{\"xid\": \"<xid>\"}]} to many.")

(defn files-problem
  "A `files_read_only` refusal: what was refused and how /files does it."
  [message hint & {:as extra}]
  (merge {:code "ENGINE_OWNED" :rule "files_read_only" :message message :hint hint} extra))

(defn problem-ex [{:keys [message] :as problem}]
  (ex-info message (dissoc problem :message)))

(defn module-write? []
  (or *module-write* (dataset/engine-write?)))

(defn readable-xids
  "The xids among `xids` of `entity-key` rows the caller may read; one answer for unknown and unreadable."
  [entity-key xids]
  (if (empty? xids)
    #{}
    (into #{} (map :xid) (dataset/search-entity (id/entity entity-key) {:xid {:_in (vec xids)}} {:xid nil}))))

(defn attribute-columns
  "Columns a client could write on `entity-key`'s rows; stamps are the engine's, so a link carries them without writing them."
  [entity-key]
  (into #{} (comp (remove core/stamp) (map #(keyword (naming/normalize-name (:name %)))))
        (:attributes (model/entity entity-key))))

(defn entity-label [entity-key] (:name (model/entity entity-key)))

(defn create-problem [entity-key]
  (case entity-key
    :files/file (files-problem "Files are created through /files, by uploading them (POST /files?name=…); /data links existing ones by xid." link-hint
                               :entity (entity-label entity-key))
    :files/folder (files-problem "Folders are created through /files (POST /files/folders {\"name\", \"parent\"}); /data links existing ones by xid."
                                 link-hint :entity (entity-label entity-key))
    (files-problem "Download links are created through /files (POST /files/<file xid>/links)."
                   "Create the link with POST /files/<file xid>/links and share the URL it returns." :entity (entity-label entity-key))))

(defn not-found-problem
  "A nested reference to a row the caller can't see, unknown or unreadable alike, as storage reports it."
  [entity-key xid]
  {:code "REF_NOT_FOUND"
   :message (str "Referenced '" (entity-label entity-key) "' not found: " (pr-str {:xid xid}))
   :hint (case entity-key
           :files/file "Check the xid, or upload the file with POST /files first; a file you can't read counts as not found."
           "Check the xid; a record you can't read counts as not found.")
   :entity (entity-label entity-key) :xids [xid]})

(defn unknown-problem
  "A row naming an xid no such row has: /data would create it, which only /files does."
  [entity-key xid]
  (files-problem (str "No " (entity-label entity-key) " has xid \"" xid "\"; "
                      (case entity-key
                        :files/file "files are created through /files: upload with POST /files?name=… and link the xid it returns."
                        :files/folder "folders are created through /files (POST /files/folders); link the xid it returns."
                        "download links are created through /files (POST /files/<file xid>/links)."))
                 link-hint :entity (entity-label entity-key) :xids [xid]))

(defn attribute-problem [entity-key column]
  (files-problem (str "A " (entity-label entity-key) " row in a /data write carries only its xid; `" (name column) "` is set through /files"
                      (case entity-key
                        :files/file " (PATCH /files/<xid>)"
                        :files/folder " (PATCH /files/folders/<xid>)"
                        "")
                      ".")
                 link-hint :entity (entity-label entity-key) :attribute (name column)))

(defn owned-relation-problems
  "Relation id → the refusal naming the /files call that writes it."
  []
  (merge
   {(id/relation :files/file->folder)
    (files-problem "A file's folder is changed through /files (PATCH /files/<xid> {\"folder\": \"<folder xid>\"}); /data can't link it."
                   "Move the file with PATCH /files/<xid>; /data reads the folder." :relation "folder")
    (id/relation :files/folder->parent)
    (files-problem "A folder's parent is changed through /files (PATCH /files/folders/<xid> {\"parent\": \"<folder xid>\"}); /data can't link it."
                   "Move the folder with PATCH /files/folders/<xid>; /data reads the parent." :relation "parent")
    (id/relation :files/link->file)
    (files-problem "A download link's file is set when /files creates the link (POST /files/<file xid>/links)."
                   "Create the link with POST /files/<file xid>/links." :relation "file")}
   (into {} (map (fn [[_ _ k]]
                   [(id/relation k)
                    (files-problem "Folder access is changed through /files (POST /files/folders/<xid>/share or /unshare); /data can't link it."
                                   "Share with POST /files/folders/<xid>/share {\"access\", \"users\" | \"groups\"}.")]))
         model/acl-relations)))

(defn parent-problem []
  (get (owned-relation-problems) (id/relation :files/folder->parent)))

(defn write-problem
  "The first /files rule `analysis` breaks, as a refusal, or nil; a reference to a row the caller can read passes."
  [analysis]
  (or
   (some (fn [entity-key]
           (when-let [e (model/entity entity-key)]
             (let [table (naming/entity->table-name e)
                   rows (get-in analysis [:entity table])
                   subject (get-in analysis [:subject table] #{})
                   link-only (get-in analysis [:link-only table] #{})
                   columns (attribute-columns entity-key)
                   checked (remove (fn [[id _]] (link-only id)) rows)
                   readable (readable-xids entity-key (distinct (keep (comp :xid second) checked)))]
               (or (when (some (fn [[_ row]] (not (:xid row))) rows) (create-problem entity-key))
                   (some (fn [[_ row]] (some #(when (columns %) (attribute-problem entity-key %)) (keys row))) rows)
                   (some (fn [[id {:keys [xid]}]]
                           (when (and xid (not (readable xid)))
                             (if (subject id) (unknown-problem entity-key xid) (not-found-problem entity-key xid))))
                         checked)))))
         [:files/file :files/folder :files/link])
   (let [folders (naming/entity->table-name (model/entity :files/folder))
         owned (owned-relation-problems)]
     (or (when (seq (get-in analysis [:recursion folders])) (parent-problem))
         (some (fn [[{:keys [relation]} pairs]] (when (seq pairs) (get owned relation)))
               (concat (:relations/one analysis) (:relations/many analysis)))))))

(defn assert-write!
  "Refuse a principal's write that creates or changes a file, folder or link or one of the module's relations; links to existing files and folders pass."
  [analysis]
  (when-not (module-write?)
    (when-let [problem (write-problem analysis)]
      (throw (problem-ex problem))))
  analysis)

(def delete-problem
  (files-problem "Files, folders and download links are deleted through /files (DELETE /files/<xid>, /files/folders/<xid> or /files/links/<xid>)."
                 "To unlink a record from a file, write its relation as null; the file stays."))

(defn assert-delete!
  "Refuse a principal's delete or purge of a file, folder or link."
  [data]
  (when-not (module-write?)
    (throw (problem-ex delete-problem)))
  data)

(defn assert-slice!
  "Refuse a principal's slice of one of the module's relations."
  [relations]
  (when-not (module-write?)
    (when-let [problem (some (owned-relation-problems) relations)]
      (throw (problem-ex problem))))
  relations)

(defmethod daccess/client-writes (id/entity :files/file :xid) [_] "link")
(defmethod daccess/client-writes (id/entity :files/folder :xid) [_] "link")
(defmethod daccess/client-writes (id/entity :files/link :xid) [_] "none")

(defmethod enhance/write [Object (id/entity :files/folder :xid)] [_ _ analysis _]
  (assert-write! analysis))

(defmethod enhance/write [Object (id/entity :files/file :xid)] [_ _ analysis _]
  (assert-write! analysis))

(defmethod enhance/delete [Object (id/entity :files/folder :xid)] [_ _ data _]
  (assert-delete! data))

(defmethod enhance/delete [Object (id/entity :files/file :xid)] [_ _ data _]
  (assert-delete! data))

(defmethod enhance/slice [Object (id/entity :files/folder :xid)] [_ _ relations _]
  (assert-slice! relations))

(defmethod enhance/slice [Object (id/entity :files/file :xid)] [_ _ relations _]
  (assert-slice! relations))

(defmethod enhance/write [Object (id/entity :files/link :xid)] [_ _ analysis _]
  (assert-write! analysis))

(defmethod enhance/delete [Object (id/entity :files/link :xid)] [_ _ data _]
  (assert-delete! data))

(defmethod enhance/slice [Object (id/entity :files/link :xid)] [_ _ relations _]
  (assert-slice! relations))
