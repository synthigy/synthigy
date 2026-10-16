;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.sqlite.patch
  "SQLite bodies of the :synthigy/dataset feature patches."
  (:require
    [clojure.string]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.plug.sqlite :as plug]
    [synthigy.db :refer [*db*]]
    [synthigy.log :as log]))

(log/info {:id ::patch-system-loaded} "Patch system loaded")

(patch/upgrade :synthigy/dataset
               "1.2.0"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::v120-installing-relation-audit
                            :data {:action :installing :subject :dataset-features :version "1.2.0"}}
                           "Installing relation-audit triggers (SQLite)")
                 (try
                   (let [model (dataset/deployed-model)
                         schema (sql-query/model->schema model)
                         relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
                         unique-tables (set (keep :table relations))]
                     (plug/reconcile-relations! *db* (:datasource *db*) relations)
                     (log/info {:id ::v120-installed
                                :data {:action :installed :subject :dataset-features :version "1.2.0"
                                       :relation-count (count unique-tables)}}
                               "Relation-audit triggers installed (SQLite)"))
                   (catch Throwable e
                     (log/error! {:id ::v120-failed
                                  :data {:action :installing :subject :dataset-features :version "1.2.0"}} e)
                     (throw e)))))

(patch/upgrade :synthigy/dataset
               "1.3.0"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::v130-relation-xid-migration
                            :data {:action :upgrading :subject :dataset-features :version "1.3.0"}}
                           "Migrating relation tables: from_xid / to_xid denormalization (SQLite)")
                 (try
                   (let [model (dataset/deployed-model)
                         schema (sql-query/model->schema model)
                         relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
                         unique-tables (set (keep :table relations))]
                     (plug/reconcile-relations! *db* (:datasource *db*) relations)
                     (log/info {:id ::v130-installed
                                :data {:action :upgraded :subject :dataset-features :version "1.3.0"
                                       :relation-count (count unique-tables)}}
                               "Relation xid denormalization complete (SQLite)"))
                   (catch Throwable e
                     (log/error! {:id ::v130-failed
                                  :data {:action :upgrading :subject :dataset-features :version "1.3.0"}} e)
                     (throw e)))))

(patch/upgrade :synthigy/dataset
               "1.4.0"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::v140-enum-to-text
                            :data {:action :upgraded :subject :dataset-features :version "1.4.0"}}
                           "Enum-to-TEXT: no-op on SQLite (already TEXT)")))

(def eucg-constraint-clause
  ;; matches `, constraint "<name>" unique(cols)` as generate-entity-ddl wrote it
  #"(?i),\s*constraint\s+\"([^\"]+)\"\s+unique\s*\(([^)]*)\)")

(defn strip-constraint-clauses
  "The table's own CREATE TABLE text with inline UNIQUE-group clauses removed."
  [create-sql]
  (clojure.string/replace create-sql eucg-constraint-clause ""))

(defn table-create-sql
  [tx table]
  (:sql (jdbc/execute-one! tx ["select sql from sqlite_master where type='table' and name=?" table]
                           {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps})))

(defn table-objects
  "The table's own triggers or explicit indexes as [name create-sql]; auto-indexes are excluded."
  [tx table kind]
  (->> (jdbc/execute! tx ["select name, sql from sqlite_master where type=? and tbl_name=? and sql is not null"
                          kind table]
                      {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps})
       (mapv (juxt :name :sql))))

(defn rebuild-table-without-constraints!
  "Rebuilds `table` from its own sqlite_master SQL without inline unique groups; caller owns the tx and foreign_keys pragma."
  [tx table]
  (let [create-sql (table-create-sql tx table)
        groups (map (fn [[_ nm cols]] {:name nm :columns cols})
                    (re-seq eucg-constraint-clause create-sql))
        indexes (table-objects tx table "index")
        triggers (table-objects tx table "trigger")
        tmp (str table "__eucg_rebuild")
        columns (mapv :name (jdbc/execute! tx [(format "PRAGMA table_info(\"%s\")" table)]
                                           {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps}))
        col-list (clojure.string/join "," (map #(format "\"%s\"" %) columns))]
    (jdbc/execute-one! tx [(-> create-sql
                               strip-constraint-clauses
                               (clojure.string/replace-first
                                (re-pattern (str "(?i)create\\s+table\\s+\"?" (java.util.regex.Pattern/quote table) "\"?"))
                                (format "create table \"%s\"" tmp)))])
    (jdbc/execute-one! tx [(format "insert into \"%s\" (%s) select %s from \"%s\""
                                   tmp col-list col-list table)])
    (jdbc/execute-one! tx [(format "drop table \"%s\"" table)])
    (jdbc/execute-one! tx [(format "alter table \"%s\" rename to \"%s\"" tmp table)])
    (doseq [{nm :name cols :columns} groups]
      (jdbc/execute-one! tx [(format "create unique index if not exists \"%s\" on \"%s\"(%s)"
                                     nm table cols)]))
    (let [group-names (set (map :name groups))]
      (doseq [[nm sql] indexes
              :when (not (group-names nm))]
        (jdbc/execute-one! tx [sql])))
    (doseq [[_ sql] triggers]
      (jdbc/execute-one! tx [sql]))
    {:groups (vec groups)
     :indexes (mapv first indexes)
     :triggers (mapv first triggers)}))

(defn user-tables
  [tx]
  (mapv :name (jdbc/execute! tx ["select name from sqlite_master where type='table' and name not like 'sqlite_%' and name not like '\\_\\_%' escape '\\'"]
                             {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps})))

(patch/upgrade :synthigy/dataset
               "1.5.0"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::v150-unique-index-migration
                            :data {:action :upgrading :subject :dataset-features :version "1.5.0"}}
                           "Migrating unique groups from inline constraints to indexes (SQLite)")
                 (try
                   (let [rebuilt
                         ;; foreign_keys=off only works in autocommit; =on must restore this SAME connection
                         (with-open [conn (jdbc/get-connection (:datasource *db*))]
                           (jdbc/execute-one! conn ["PRAGMA foreign_keys=off"])
                           (try
                             (jdbc/with-transaction [tx conn]
                               (let [pre-existing (set (jdbc/execute! tx ["PRAGMA foreign_key_check"]))
                                     _ (when (seq pre-existing)
                                         (log/warn {:id ::v150-preexisting-fk-orphans
                                                    :data {:action :verifying :subject :dataset-features :version "1.5.0"
                                                           :count (count pre-existing)
                                                           :violations (vec (take 20 pre-existing))}}
                                                   "Pre-existing FK violations in database before unique-index rebuild — not caused by, and not fixed by, this migration"))
                                     done (reduce
                                           (fn [acc table]
                                             (if (re-find eucg-constraint-clause (table-create-sql tx table))
                                               (do (rebuild-table-without-constraints! tx table)
                                                   (conj acc table))
                                               acc))
                                           []
                                           (user-tables tx))]
                                 (let [violations (remove pre-existing (jdbc/execute! tx ["PRAGMA foreign_key_check"]))]
                                   (when (seq violations)
                                     (throw (ex-info "Foreign key violations after unique-index rebuild"
                                                     {:type ::v150-fk-violations
                                                      :violations (vec violations)}))))
                                 done))
                             (finally
                               (jdbc/execute-one! conn ["PRAGMA foreign_keys=on"]))))]
                     (log/info {:id ::v150-installed
                                :data {:action :upgraded :subject :dataset-features :version "1.5.0"
                                       :rebuilt (count rebuilt) :tables rebuilt}}
                               "Unique groups migrated to indexes (SQLite)"))
                   (catch Throwable e
                     (log/error! {:id ::v150-failed
                                  :data {:action :upgrading :subject :dataset-features :version "1.5.0"}} e)
                     (throw e)))))

