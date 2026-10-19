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

(ns synthigy.dataset.sqlite.collation
  "The HUMAN collation on SQLite: column swaps, conversion of existing columns and ICU version upkeep."
  (:require
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.sql.naming :refer [normalize-name entity->table-name]]
   [synthigy.db :refer [*db*]]
   [synthigy.log :as log]))

(def human-types #{"string" "enum"})

(def human-ddl "TEXT COLLATE HUMAN")

(defn rows
  [tx sql-params]
  (jdbc/execute! tx sql-params {:builder-fn rs/as-unqualified-lower-maps}))

(defn quoted [identifier] (str \" identifier \"))

(defn split-top-level
  "Splits `s` on commas outside parentheses and quotes."
  [s]
  (loop [[c & more :as cs] (seq s)
         depth 0
         quote-char nil
         current (StringBuilder.)
         parts []]
    (cond
      (empty? cs) (conj parts (str current))
      quote-char (recur more depth (when (not= c quote-char) quote-char) (.append current c) parts)
      (#{\" \' \`} c) (recur more depth c (.append current c) parts)
      (= c \[) (recur more depth \] (.append current c) parts)
      (= c \() (recur more (inc depth) nil (.append current c) parts)
      (= c \)) (recur more (dec depth) nil (.append current c) parts)
      (and (= c \,) (zero? depth)) (recur more depth nil (StringBuilder.) (conj parts (str current)))
      :else (recur more depth nil (.append current c) parts))))

(defn column-definitions
  "Column name → its definition text, read from a CREATE TABLE statement."
  [create-sql]
  (let [body (subs create-sql (inc (str/index-of create-sql "(")) (str/last-index-of create-sql ")"))]
    (into {}
          (keep (fn [part]
                  (let [part (str/trim part)]
                    (when-not (re-find #"(?i)^(constraint|primary\s+key|unique|check|foreign\s+key)\b" part)
                      (when-let [[_ q1 q2 q3 bare] (re-find #"^(?:\"((?:[^\"]|\"\")+)\"|`([^`]+)`|\[([^\]]+)\]|(\S+))" part)]
                        [(or (some-> q1 (str/replace "\"\"" "\"")) q2 q3 bare) part])))))
          (split-top-level body))))

(defn human-collated?
  [definition]
  (boolean (re-find #"(?i)\bcollate\s+[\"`\[]?human\b" definition)))

(defn table-create-sql
  [tx table]
  (:sql (first (rows tx ["select sql from sqlite_master where type = 'table' and name = ?" table]))))

(defn swap-columns!
  "Recreates the named columns of `table` with new type DDL, keeping values, indexes and triggers; runs in the caller's tx."
  [tx table column->ddl]
  (let [objects (sort-by :type (rows tx ["select type, name, sql from sqlite_master
                                          where tbl_name = ? and type in ('index', 'trigger') and sql is not null"
                                         table]))
        swap-name #(str % "__swap")]
    ;; never copy with the triggers in place — the copy UPDATE would bump modified_on and emit deltas for every row
    (doseq [{:keys [type name]} objects]
      (jdbc/execute-one! tx [(format "drop %s if exists %s" type (quoted name))]))
    (doseq [[column ddl] column->ddl]
      (jdbc/execute-one! tx [(format "alter table %s add column %s %s" (quoted table) (quoted (swap-name column)) ddl)]))
    (jdbc/execute-one! tx [(format "update %s set %s" (quoted table)
                                   (str/join ", " (map #(format "%s = %s" (quoted (swap-name %)) (quoted %))
                                                       (keys column->ddl))))])
    (doseq [column (keys column->ddl)]
      (jdbc/execute-one! tx [(format "alter table %s drop column %s" (quoted table) (quoted column))])
      (jdbc/execute-one! tx [(format "alter table %s rename column %s to %s"
                                     (quoted table) (quoted (swap-name column)) (quoted column))]))
    (doseq [{:keys [sql]} objects]
      (jdbc/execute-one! tx [sql]))
    {:table table
     :columns (vec (keys column->ddl))
     :restored (mapv :name objects)}))

(defn pending-columns
  "table → columns of the deployed model's string/enum attributes not declared COLLATE HUMAN."
  [tx]
  (reduce
   (fn [pending entity]
     (let [table (entity->table-name entity)
           definitions (some-> (table-create-sql tx table) column-definitions)
           columns (->> (:attributes entity)
                        (filter (comp human-types :type))
                        (map (comp normalize-name :name))
                        (filter #(when-let [definition (get definitions %)]
                                   (not (human-collated? definition))))
                        distinct
                        vec)]
       (cond-> pending
         (seq columns) (update table (fnil into []) columns))))
   {}
   (core/get-entities (dataset/deployed-model))))

(defn collate-columns!
  "Swaps every pending string/enum column to COLLATE HUMAN in one transaction; returns table → columns."
  []
  (jdbc/with-transaction [tx (:datasource *db*)]
    (let [pending (pending-columns tx)]
      (doseq [[table columns] pending]
        (swap-columns! tx table (zipmap (distinct columns) (repeat human-ddl)))
        (log/debug {:id ::table-collated :data {:table table :columns columns}} "Collated columns HUMAN"))
      pending)))

(def versions-ddl
  "CREATE TABLE IF NOT EXISTS __collation_versions__ (name TEXT PRIMARY KEY, version TEXT NOT NULL)")

(defn check-version!
  "Rebuilds the HUMAN indexes when the driver's ICU differs from the one recorded, then records it."
  []
  (with-open [conn (jdbc/get-connection (:datasource *db*))]
    (jdbc/execute-one! conn [versions-ddl])
    (let [actual (:v (first (rows conn ["select human_version() as v"])))
          recorded (:version (first (rows conn ["select version from __collation_versions__ where name = 'HUMAN'"])))]
      (when (and recorded (not= recorded actual))
        (log/warn {:id ::version-changed
                   :data {:action :verifying :subject :collation :recorded recorded :actual actual}}
                  "ICU version of the HUMAN collation changed; rebuilding its indexes")
        (jdbc/execute-one! conn ["REINDEX HUMAN"]))
      (when (not= recorded actual)
        (jdbc/execute-one! conn ["insert into __collation_versions__ (name, version) values ('HUMAN', ?)
                                  on conflict (name) do update set version = excluded.version"
                                 actual]))
      actual)))
