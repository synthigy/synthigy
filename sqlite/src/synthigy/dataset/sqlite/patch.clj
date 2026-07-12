(ns synthigy.dataset.sqlite.patch
  "SQLite schema patches and migrations.

  This namespace will contain version-based patches for SQLite schema evolution.
  Currently empty - patches will be added as needed."
  (:require
    [next.jdbc :as jdbc]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.substrate.sqlite :as substrate]
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
                     (substrate/reconcile-relations! *db* (:datasource *db*) relations)
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
                     (substrate/reconcile-relations! *db* (:datasource *db*) relations)
                     (log/info {:id ::v130-installed
                                :data {:action :upgraded :subject :dataset-features :version "1.3.0"
                                       :relation-count (count unique-tables)}}
                               "Relation xid denormalization complete (SQLite)"))
                   (catch Throwable e
                     (log/error! {:id ::v130-failed
                                  :data {:action :upgrading :subject :dataset-features :version "1.3.0"}} e)
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

(patch/current-version :synthigy.dataset/model (:name (dataset/current-dataset-version)))

;; Installed model version from __deploy_history
(patch/installed-version
  :synthigy.dataset/model
  (or (some-> (dataset/latest-deployed-version (id/data :dataset/id))
              :name
              str)
      "0"))
