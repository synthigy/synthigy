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
  "The data API reads files and folders and links other records to them; only /files creates, changes or deletes them."
  (:require
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.access.protocol :as p]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db]
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

(def read-only-problem
  {:code "ENGINE_OWNED"
   :rule "files_read_only"
   :message "Files and folders are created, changed and deleted through /files; the data API reads them and links records to them."})

(defn problem-ex [{:keys [message] :as problem}]
  (ex-info message (dissoc problem :message)))

(defn module-write? []
  (or *module-write* (dataset/engine-write?)))

(defn existing-xids
  [tx table xids]
  (if (empty? xids)
    #{}
    (into #{} (map :xid)
          (jdbc/execute! (or tx (:datasource db/*db*)) (into [(format "SELECT xid FROM \"%s\" WHERE xid IN (%s)"
                                           table (str/join "," (repeat (count xids) "?")))]
                                  xids)
                         {:builder-fn rs/as-unqualified-lower-maps}))))

(defn owned-relations
  "Relation ids only the module links and unlinks: a file's folder, a folder's parent and the access lists."
  []
  (into #{} (map id/relation) (into [:files/file->folder :files/folder->parent] (map peek) model/acl-relations)))

(defn attribute-columns [entity-key]
  (into #{} (map #(keyword (naming/normalize-name (:name %)))) (:attributes (model/entity entity-key))))

(defn rows-changed?
  "True when `analysis` creates a file or folder or writes one of their attributes."
  [analysis tx]
  (some (fn [entity-key]
          (let [table (naming/entity->table-name (model/entity entity-key))
                rows (vals (get-in analysis [:entity table]))
                columns (attribute-columns entity-key)
                xids (keep :xid rows)]
            (or (some #(some columns (keys %)) rows)
                (some (complement :xid) rows)
                (not= (count (set xids)) (count (existing-xids tx table (distinct xids)))))))
        [:files/file :files/folder]))

(defn relations-written?
  "True when `analysis` links or clears a file's folder, a folder's parent or an access list."
  [analysis]
  (let [owned (owned-relations)
        folders (naming/entity->table-name (model/entity :files/folder))]
    (or (seq (get-in analysis [:recursion folders]))
        (some (fn [[{:keys [relation]} pairs]] (and (owned relation) (seq pairs)))
              (concat (:relations/one analysis) (:relations/many analysis))))))

(defn assert-write!
  "Refuse a principal's write that creates or changes a file or folder or one of the module's relations; links to existing ones pass."
  [analysis tx]
  (when (and (not (module-write?))
             (or (rows-changed? analysis tx) (relations-written? analysis)))
    (throw (problem-ex read-only-problem)))
  analysis)

(defn assert-delete!
  "Refuse a principal's delete or purge of a file or folder."
  [data]
  (when-not (module-write?)
    (throw (problem-ex read-only-problem)))
  data)

(defn assert-slice!
  "Refuse a principal's slice of one of the module's relations."
  [relations]
  (when (and (not (module-write?)) (some (owned-relations) relations))
    (throw (problem-ex read-only-problem)))
  relations)

(defmethod enhance/write [Object (id/entity :files/folder :xid)] [_ _ analysis tx]
  (assert-write! analysis tx))

(defmethod enhance/write [Object (id/entity :files/file :xid)] [_ _ analysis tx]
  (assert-write! analysis tx))

(defmethod enhance/delete [Object (id/entity :files/folder :xid)] [_ _ data _]
  (assert-delete! data))

(defmethod enhance/delete [Object (id/entity :files/file :xid)] [_ _ data _]
  (assert-delete! data))

(defmethod enhance/slice [Object (id/entity :files/folder :xid)] [_ _ relations _]
  (assert-slice! relations))

(defmethod enhance/slice [Object (id/entity :files/file :xid)] [_ _ relations _]
  (assert-slice! relations))
