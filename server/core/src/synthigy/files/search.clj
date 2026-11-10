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

(ns synthigy.files.search
  "Folders and files whose paths hold the searched words in order, shortest path first."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.dataset.sql.template :as template]
   [synthigy.files.folders :as folders]
   [synthigy.files.model :as model]))

(defn like-escape [s] (str/replace s #"[!%_]" "!$0"))

(defn words [q] (remove str/blank? (str/split (str q) #"\s+")))

(defn pattern
  "The like pattern for `q`: its words in order with anything between, under folder path `scope` when given."
  [q scope]
  (str (some-> scope like-escape) "%" (str/join "%" (map like-escape (words q))) "%"))

(defn template-name [entity-key] (naming/normalize-name (:name (model/entity entity-key))))

(defn template
  "One search over folders by path and files by full path, shortest first; each CTE gets its own row rules."
  [folder-label]
  (let [d (template-name :files/folder)
        f (template-name :files/file)
        path (name (folders/path-field))
        dp (str "{" d "." path "}")
        n (str "{" f "." (model/column :files/file :files.file/name) "}")
        fp (str "{" f "->" folder-label "." path "}")
        full (str "coalesce(" fp ", '') || " n)]
    (str "with d as (select 'folder' as type, {" d "." (name (id/key)) "} as xid, " dp " as path,"
         " " dp " as sort, length(" dp ") as len"
         " where lower(" dp ") like lower(?) escape '!'),"
         " f as (select 'file' as type, {" f "." (name (id/key)) "} as xid, " fp " || " n " as path,"
         " " full " as sort, length(" full ") as len"
         " where lower(" full ") like lower(?) escape '!')"
         " select type, xid, path, len, sort from d union all select type, xid, path, len, sort from f"
         " order by len, sort, xid limit ? offset ?")))

(defn matches
  "A page of `limit` folders and files matching `q` under folder path `scope` from `offset`, shortest path first: [{:type :xid :path}], and whether more matched."
  [q scope limit offset folder-label]
  (let [like (pattern q scope)
        found (template/execute-template (template folder-label) [like like (inc limit) offset] {:cached true})]
    {:matches (map (fn [{:keys [type xid path]}] {:type (keyword type) :xid (str xid) :path path}) (take limit found))
     :truncated (> (count found) limit)}))

(defn name-page
  "Xids of one page of the files whose names hold the words of `q` in order, filtered and ordered as GET /files lists."
  [q {:keys [folder folderless file-name order limit offset]} folder-label]
  (let [f (template-name :files/file)
        col #(str "{" f "." % "}")
        n (col (model/column :files/file :files.file/name))
        at (col (model/column :files/file :files.file/uploaded-at))
        in-folder (str "{" f "->" folder-label "." (name (id/key)) "}")
        sql (str "select " (col (name (id/key))) " as xid"
                 " where lower(" n ") like lower(?) escape '!'"
                 (when file-name (str " and " n " = ?"))
                 (when folder (str " and " in-folder " = ?"))
                 (when folderless (str " and " in-folder " is null"))
                 " order by " (case order "name" (str n " asc") "oldest" (str at " asc") (str at " desc"))
                 ", " (col (name (id/key))) " limit ? offset ?")
        params (cond-> [(pattern q nil)]
                 file-name (conj file-name)
                 folder (conj (id/coerce-arg folder))
                 true (conj limit offset))]
    (mapv (comp str :xid) (template/execute-template sql params {:cached true}))))
