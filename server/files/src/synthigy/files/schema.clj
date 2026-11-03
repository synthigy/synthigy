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

(ns synthigy.files.schema
  "Module-owned DDL: file names unique per folder, enforced by the database."
  (:require
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db]
   [synthigy.files.model :as model]
   [synthigy.log :as log])
  (:import
   [synthigy.db Postgres SQLite]))

(def name-column "file_name")

(defn names
  "Table and column names the uniqueness DDL needs, from `model`."
  [model]
  (when (model/deployed? model)
    (let [file (model/entity model :files/file)
          rel (model/relation model :files/file->folder)
          file-first? (= (id/extract file) (id/extract (:from rel)))]
      {:file (naming/entity->table-name file)
       :file-name (model/column model :files/file :files.file/name)
       :link (naming/relation->table-name rel)
       :link-file (naming/entity->relation-field (if file-first? (:from rel) (:to rel)))
       :link-folder (naming/entity->relation-field (if file-first? (:to rel) (:from rel)))})))

(defn q [s] (str \" s \"))

(defmulti statements
  "DDL that adds the name column, its triggers and the unique indexes; idempotent."
  (fn [db _] (class db)))

(defmethod statements Postgres
  [_ {:keys [file file-name link link-file]}]
  [(format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s text COLLATE \"C\"" (q link) name-column)
   (format "CREATE OR REPLACE FUNCTION synthigy_files_link_name() RETURNS trigger AS $$
BEGIN NEW.%s := (SELECT %s FROM %s WHERE _eid = NEW.%s); RETURN NEW; END $$ LANGUAGE plpgsql"
           name-column (q file-name) (q file) (q link-file))
   (format "DROP TRIGGER IF EXISTS synthigy_files_link_name ON %s" (q link))
   (format "CREATE TRIGGER synthigy_files_link_name BEFORE INSERT OR UPDATE OF %s ON %s
FOR EACH ROW EXECUTE FUNCTION synthigy_files_link_name()" (q link-file) (q link))
   (format "CREATE OR REPLACE FUNCTION synthigy_files_rename() RETURNS trigger AS $$
BEGIN UPDATE %s SET %s = NEW.%s WHERE %s = NEW._eid; RETURN NEW; END $$ LANGUAGE plpgsql"
           (q link) name-column (q file-name) (q link-file))
   (format "DROP TRIGGER IF EXISTS synthigy_files_rename ON %s" (q file))
   (format "CREATE TRIGGER synthigy_files_rename AFTER UPDATE OF %s ON %s
FOR EACH ROW WHEN (OLD.%s IS DISTINCT FROM NEW.%s) EXECUTE FUNCTION synthigy_files_rename()"
           (q file-name) (q file) (q file-name) (q file-name))
   (format "UPDATE %s l SET %s = f.%s FROM %s f WHERE f._eid = l.%s AND l.%s IS DISTINCT FROM (f.%s COLLATE \"C\")"
           (q link) name-column (q file-name) (q file) (q link-file) name-column (q file-name))])

(defmethod statements SQLite
  [db {:keys [file file-name link link-file]}]
  (let [fill (format "UPDATE %s SET %s = (SELECT %s FROM %s WHERE _eid = NEW.%s) WHERE rowid = NEW.rowid"
                     (q link) name-column (q file-name) (q file) (q link-file))]
    (cond-> []
      (not (db/column-exists? db link name-column))
      (conj (format "ALTER TABLE %s ADD COLUMN %s TEXT COLLATE BINARY" (q link) name-column))
      :always
      (into [(format "DROP TRIGGER IF EXISTS synthigy_files_link_ins")
             (format "CREATE TRIGGER synthigy_files_link_ins AFTER INSERT ON %s BEGIN %s; END" (q link) fill)
             (format "DROP TRIGGER IF EXISTS synthigy_files_link_upd")
             (format "CREATE TRIGGER synthigy_files_link_upd AFTER UPDATE OF %s ON %s BEGIN %s; END"
                     (q link-file) (q link) fill)
             (format "DROP TRIGGER IF EXISTS synthigy_files_rename")
             (format "CREATE TRIGGER synthigy_files_rename AFTER UPDATE OF %s ON %s BEGIN UPDATE %s SET %s = NEW.%s WHERE %s = NEW._eid; END"
                     (q file-name) (q file) (q link) name-column (q file-name) (q link-file))
             (format "UPDATE %s SET %s = (SELECT %s FROM %s WHERE _eid = %s.%s)"
                     (q link) name-column (q file-name) (q file) (q link) (q link-file))]))))

(defn index-statements [{:keys [link link-file link-folder]}]
  [(format "CREATE UNIQUE INDEX IF NOT EXISTS synthigy_files_one_folder ON %s (%s)" (q link) (q link-file))
   (format "CREATE UNIQUE INDEX IF NOT EXISTS synthigy_files_name_per_folder ON %s (%s, %s)"
           (q link) (q link-folder) name-column)])

(defn numbered
  "`name` with ` (n)` before its extension."
  [name n]
  (let [[_ base ext] (re-matches #"(.+?)(\.[^.]*)?" name)]
    (str base " (" n ")" ext)))

(defn duplicates
  "Folder links whose file name repeats inside the folder, all but the oldest file of each group."
  [tx {:keys [link link-file link-folder]}]
  (jdbc/execute! tx [(format "SELECT l.%s AS folder, l.%s AS file, l.%s AS name FROM %s l
WHERE l.%s IS NOT NULL AND EXISTS (SELECT 1 FROM %s o WHERE o.%s = l.%s AND o.%s = l.%s AND o.%s < l.%s)
ORDER BY l.%s, l.%s"
                             (q link-folder) (q link-file) name-column (q link)
                             name-column (q link) (q link-folder) (q link-folder) name-column name-column
                             (q link-file) (q link-file) (q link-folder) (q link-file))]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn rename-duplicates!
  "Rename repeated names inside a folder to `name (2).ext`, `name (3).ext`, ...; returns the renames."
  [tx {file-table :file :keys [file-name link link-folder] :as n}]
  (let [taken? (fn [folder candidate]
                 (seq (jdbc/execute! tx [(format "SELECT 1 FROM %s WHERE %s = ? AND %s = ?"
                                                 (q link) (q link-folder) name-column)
                                         folder candidate])))]
    (vec
     (for [{:keys [folder file name] :as dup} (duplicates tx n)
           :let [fresh (first (remove #(taken? folder %) (map #(numbered name %) (iterate inc 2))))]]
       (do
         (jdbc/execute! tx [(format "UPDATE %s SET %s = ? WHERE _eid = ?" (q file-table) (q file-name)) fresh file])
         (assoc dup :renamed-to fresh))))))

(defn ensure!
  "Install or refresh the name-uniqueness DDL for the deployed File System model."
  ([] (ensure! db/*db* (dataset/deployed-model)))
  ([db model]
   (if-let [n (names model)]
     (if (get-method statements (class db))
       (jdbc/with-transaction [tx (:datasource db)]
         (doseq [s (statements db n)] (jdbc/execute! tx [s]))
         (let [renamed (rename-duplicates! tx n)]
           (when (seq renamed)
             (log/warn {:id ::duplicates-renamed :data {:action :migrated :subject :files :renamed renamed}}
                       "Renamed files whose names repeated inside a folder")))
         (doseq [s (index-statements n)] (jdbc/execute! tx [s]))
         n)
       (log/warn {:id ::unsupported-backend :data {:subject :files :backend (.getSimpleName (class db))}}
                 "File name uniqueness is not enforced on this database"))
     (log/info {:id ::model-missing :data {:subject :files}}
               "File System dataset not deployed; name uniqueness not installed"))))

(defn unique-violation?
  "True when `ex` is the database refusing a second file of the same name in a folder."
  [ex]
  (boolean (some #(let [m (str (ex-message %))]
                   (or (str/includes? m "synthigy_files_name_per_folder")
                       (and (str/includes? m "UNIQUE constraint failed") (str/includes? m (str "." name-column)))))
                 (take-while some? (iterate ex-cause ex)))))
