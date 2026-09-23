;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.sqlite.patch
  "SQLite schema patches and migrations.

  This namespace will contain version-based patches for SQLite schema evolution.
  Currently empty - patches will be added as needed."
  (:require
    [clojure.string]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.plug.sqlite :as plug]
    [synthigy.db :refer [*db*]]
    [synthigy.log :as log]))

(log/info {:id ::patch-system-loaded} "Patch system loaded")

;; Patch 1.2.0 - Relation Audit Triggers (SQLite only)
;; Mirror of postgres/patch.clj's 1.2.0. Installs the relation delta queue +
;; _ctx tables + per-relation triggers from the deployed model.
;; (Queue was originally named __delta_queue; renamed to __relation_delta_queue
;; in 1.4.0. New installs land on the new name directly via the DDL constant.)
;; Idempotent — re-runs on every level! to reconcile new relations.
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

;; Patch 1.3.0 - Xid denormalization for relation tables (SQLite only)
;; Mirror of postgres patch.clj's 1.3.0. Adds from_xid / to_xid TEXT
;; columns via ALTER TABLE (PRAGMA-checked for idempotency), backfills,
;; reinstalls triggers with the updated payload. SQLite has no BEFORE
;; INSERT populate trigger (NEW is immutable) — the app-path
;; link-relations subselects are authoritative.
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

;; Patch 1.4.0 - Enum columns to TEXT (no-op on SQLite)
;; SQLite has always stored enum attributes as TEXT — 1.4.0 makes the other
;; backends match it. Nothing to migrate here; the marker keeps the feature
;; version aligned across backends.
(patch/upgrade :synthigy/dataset
               "1.4.0"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::v140-enum-to-text
                            :data {:action :upgraded :subject :dataset-features :version "1.4.0"}}
                           "Enum-to-TEXT: no-op on SQLite (already TEXT)")))

;; Patch 1.5.0 - Unique groups: inline table constraints → unique INDEXES
;; (SQLite only). SQLite has no ADD/DROP CONSTRAINT, so a group inlined in
;; CREATE TABLE can never be changed; and its auto-index is undroppable, so a
;; `drop index` against the _eucg_ name silently succeeds while the constraint
;; keeps enforcing. The table must be rebuilt to shed it. See
;; docs/core/synthigy/dataset/sqlite.md and docs/plans/PLAN-SQLITE-UNIQUE-INDEX.md.

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
  "The table's own triggers and explicitly-created indexes, as their CREATE
   text. `DROP TABLE` takes both with it, so a rebuild must replay them.
   Auto-indexes (constraint/PK-born) carry a NULL `sql` and are excluded —
   they are recreated by the new table definition itself."
  [tx table kind]
  (->> (jdbc/execute! tx ["select name, sql from sqlite_master where type=? and tbl_name=? and sql is not null"
                          kind table]
                      {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps})
       (mapv (juxt :name :sql))))

(defn rebuild-table-without-constraints!
  "SQLite's documented table-rebuild, narrowed to shedding inline UNIQUE-group
   constraints. Rebuilds from the table's OWN `sqlite_master.sql` minus those
   clauses — never from the model — so the migration is semantics-preserving
   and cannot be thrown off by model drift, and each dropped constraint comes
   back as an identically-named index over identical columns.

   Triggers and user indexes are captured BEFORE the drop and replayed after,
   so the table comes out of this with exactly what it went in with. Notably
   the delta/audit plug triggers are NOT reinstalled from the model — a
   bare-server deployment deliberately runs without them, and reconciling
   against the model here would install a trigger tax that config opted out of.

   Caller owns the transaction and the foreign_keys pragma."
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
    ;; Groups first: a captured index sharing a _eucg_ name must not double-create.
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
                         ;; foreign_keys=off MUST run in autocommit mode (no-op inside a
                         ;; transaction) and =on MUST restore the SAME connection before
                         ;; it returns to the pool — connectionInitSql runs only at
                         ;; physical-connection creation.
                         (with-open [conn (jdbc/get-connection (:datasource *db*))]
                           (jdbc/execute-one! conn ["PRAGMA foreign_keys=off"])
                           (try
                             (jdbc/with-transaction [tx conn]
                               ;; foreign_key_check scans the WHOLE db — pre-existing
                               ;; orphans (dev DBs have them) must not fail the patch,
                               ;; only violations the rebuild itself introduces.
                               (let [pre-existing (set (jdbc/execute! tx ["PRAGMA foreign_key_check"]))
                                     _ (when (seq pre-existing)
                                         (log/warn {:id ::v150-preexisting-fk-orphans
                                                    :data {:action :verifying :subject :dataset-features :version "1.5.0"
                                                           :count (count pre-existing)
                                                           :violations (vec (take 20 pre-existing))}}
                                                   "Pre-existing FK violations in database before unique-index rebuild — not caused by, and not fixed by, this migration"))
                                     done (reduce
                                           (fn [acc table]
                                             ;; rebuild only tables whose create SQL carries a
                                             ;; named eucg constraint clause — exactly the tables
                                             ;; the rebuild would change. Relation tables'
                                             ;; anonymous unique(...) and the xid column UNIQUE
                                             ;; never match.
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

;; Patch :synthigy.dataset/model 1.0.5 — Dataset + Dataset Version opt
;; into principal-aware audit. Resource at dataset.json was carrying the
;; new opt-in (`:audit {:actions #{:created :modified}}`) before this
;; bump, but the deployed snapshot in `dataset_version` stayed at 1.0.4
;; and never picked it up. Re-runs `dataset/deploy!` with the bumped
;; resource — after this, `audit-modified?` returns true for those
;; entities and the SQL validator accepts modified-on / modified-by in
;; selections.
(patch/upgrade :synthigy.dataset/model
               "1.0.5"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::model-v105-deploying
                            :data {:action :deploying :subject :dataset-model :version "1.0.5"}}
                           "Deploying meta-model v1.0.5 — audit opt-in for Dataset + Dataset Version")
                 (dataset/deploy! (dataset/current-dataset-version))
                 (log/info {:id ::model-v105-complete
                            :data {:action :upgraded :subject :dataset-model :version "1.0.5"}}
                           "Meta-model v1.0.5 deployed; audit fields now resolvable in selections")))

(patch/upgrade :synthigy.dataset/model
               "1.0.6"
               (when (instance? synthigy.db.SQLite *db*)
                 (log/info {:id ::model-v106-deploying
                            :data {:action :deploying :subject :dataset-model :version "1.0.6"}}
                           "Deploying meta-model v1.0.6 — RBAC opt-in on meta-entities")
                 (dataset/deploy! (dataset/current-dataset-version))
                 (log/info {:id ::model-v106-complete
                            :data {:action :upgraded :subject :dataset-model :version "1.0.6"}}
                           "Meta-model v1.0.6 deployed")))

(patch/current-version :synthigy.dataset/model (:name (dataset/current-dataset-version)))

;; Installed model version from __deploy_history
(patch/installed-version
  :synthigy.dataset/model
  (or (some-> (dataset/latest-deployed-version (id/data :dataset/id))
              :name
              str)
      "0"))
