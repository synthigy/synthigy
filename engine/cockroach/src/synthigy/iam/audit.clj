;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.iam.audit
  "CockroachDB principal-aware audit: `*_by`/`*_on` columns only, no triggers."
  (:require
   [next.jdbc :as jdbc]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.dataset :refer [deployed-model deployed-entity]]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
   [synthigy.db :refer [*db*]]
   [synthigy.db.cockroach]  ; Load Cockroach JDBCBackend implementation
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.log :as log]))

(defn enhance-audit-data
  "Stamps modified_by/created_by from the bound principal, per the entity's audit config."
  [entity-id data]
  (let [entity (deployed-entity entity-id)
        current-user-eid (some-> (access/current-principal) :_eid)
        table (entity->table-name entity)]
    (if-not current-user-eid
      data
      (let [modified? (core/audit-modified? entity)
            created? (core/audit-created? entity)]
        (if-not (or modified? created?)
          data
          (update-in data [:entity table]
                     (fn [mapping]
                       (reduce-kv
                        (fn [data tmp-id _]
                          (cond-> data
                            modified? (assoc-in [tmp-id :modified_by] current-user-eid)
                            created? (assoc-in [tmp-id :created_by] current-user-eid)))
                        mapping
                        mapping))))))))

(defn user-table-exists?
  [tx]
  (let [result (execute-one! tx
                             ["SELECT COUNT(*) as cnt FROM information_schema.tables
                               WHERE table_name = 'user' AND table_schema = 'public'"])]
    (pos? (:cnt result 0))))

(defn transform-audit-impl
  "Adds the configured audit columns to new entity tables; no triggers on CRDB."
  [_ tx entities]
  (let [has-user-table (user-table-exists? tx)]
    (log/info {:id ::transform-audit-starting
               :data {:user-table (if has-user-table "exists" "not yet created")
                      :mode :crdb-app-layer-fill}}
              "Adding audit columns (no triggers — CRDB app-layer enforcement)")

    (doseq [{:as entity} entities
            :let [table (entity->table-name entity)
                  modified? (core/audit-modified? entity)
                  created? (core/audit-created? entity)]
            :when (or modified? created?)]

      (when modified?
        (execute! tx
                  [(if has-user-table
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                             table)
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint"
                             table))])
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp"
                           table)]))

      (when created?
        (execute! tx
                  [(if has-user-table
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                             table)
                     (format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint"
                             table))])
        (execute! tx
                  [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp"
                           table)]))

      (log/debug {:id ::table-audited
                  :data {:table table :modified modified? :created created?
                         :mode :columns-only}}
                 "Added audit columns to table (CRDB: no triggers)"))))

(defn augment-schema-impl
  "Audit fields and relations for the runtime schema, per the entity's audit config."
  [_ entity]
  (let [modified? (core/audit-modified? entity)
        created? (core/audit-created? entity)]
    (if-not (or modified? created?)
      {}
      (let [entity-id (id/extract entity)
            entity-table (entity->table-name entity)
            user-entity (core/reference-entity-id "user")
            user-table (when user-entity
                         (some-> (deployed-model)
                                 (core/get-entity user-entity)
                                 entity->table-name))]
        (cond-> {:fields {}}
          modified?
          (assoc-in [:fields :modified_on] {:key :modified_on :type "timestamp"})

          created?
          (assoc-in [:fields :created_on] {:key :created_on :type "timestamp"})

          (and modified? user-entity user-table)
          (-> (assoc-in [:fields :modified_by]
                        {:key :modified_by :type "user" :reference/entity user-entity})
              (assoc-in [:relations :modified_by]
                        {:from entity-id
                         :from/field :modified_by
                         :from/table entity-table
                         :to user-entity
                         :to/field :_eid
                         :to/table user-table
                         :table entity-table
                         :type :one}))

          (and created? user-entity user-table)
          (-> (assoc-in [:fields :created_by]
                        {:key :created_by :type "user" :reference/entity user-entity})
              (assoc-in [:relations :created_by]
                        {:from entity-id
                         :from/field :created_by
                         :from/table entity-table
                         :to user-entity
                         :to/field :_eid
                         :to/table user-table
                         :table entity-table
                         :type :one})))))))

(extend-protocol enhance/AuditEnhancement
  synthigy.db.Cockroach
  (transform-audit [db tx entities] (transform-audit-impl db tx entities))
  (augment-schema  [db entity]      (augment-schema-impl db entity))
  (audit           [_ entity-id data _] (enhance-audit-data entity-id data)))

(defn setup!
  "Idempotently retrofits the configured audit columns and their FKs onto existing tables."
  [db model]
  (log/info {:id ::setup-starting}
            "Adding audit columns to existing entity tables")

  (let [entities (core/get-entities model)
        results (atom {:added-columns 0
                       :added-triggers 0})]

    (with-open [conn (jdbc/get-connection (:datasource db))]
      (doseq [{:as entity} entities
              :let [table (entity->table-name entity)
                    modified? (core/audit-modified? entity)
                    created? (core/audit-created? entity)]
              :when (or modified? created?)]

        (try
          (when modified?
            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_by bigint"
                               table)])
            (try
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD CONSTRAINT \"%s_modified_by_fkey\" FOREIGN KEY (modified_by) REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                 table table)])
              (catch Exception _ nil))
            (swap! results update :added-columns inc)

            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp"
                               table)])
            (swap! results update :added-columns inc))

          (when created?
            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_by bigint"
                               table)])
            (try
              (execute! conn
                        [(format "ALTER TABLE \"%s\" ADD CONSTRAINT \"%s_created_by_fkey\" FOREIGN KEY (created_by) REFERENCES \"user\"(_eid) ON DELETE SET NULL"
                                 table table)])
              (catch Exception _ nil))
            (swap! results update :added-columns inc)

            (execute! conn
                      [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp"
                               table)])
            (swap! results update :added-columns inc))

          (log/info {:id ::table-setup-complete
                     :data {:table table :modified modified? :created created?
                            :mode :columns-only}}
                    "Added audit to table (CRDB: app-layer enforcement)")

          (catch Exception e
            (log/error! {:id ::table-setup-failed :data {:table table}} e)))))

    (log/info {:id ::setup-complete :data @results} "Setup complete")
    @results))

(patch/current-version :synthigy.iam/audit "1.0.2")

(comment
  (patch/read-version *db* :synthigy.iam/audit))

(patch/upgrade :synthigy.iam/audit
               "1.0.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::installing-v100 :data {:action :installing :subject :iam-audit :version "1.0.0"}}
                           "Installing audit enhancement v1.0.0")
                 (setup! *db* (deployed-model))))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::upgrading-v101 :data {:action :upgrading :subject :iam-audit :version "1.0.1"}}
                           "Upgrading to v1.0.1: adding created_by and created_on fields")
                 (setup! *db* (deployed-model))
                 (log/info {:id ::backfilling-created-by :data {:action :migrating :subject :created-by}}
                           "Backfilling created_by from modified_by")
                 (let [entities (core/get-entities (deployed-model))
                       backfill-count (atom 0)]
                   (with-open [conn (jdbc/get-connection (:datasource *db*))]
                     (doseq [{:as entity} entities
                             :let [table     (entity->table-name entity)
                                   modified? (core/audit-modified? entity)
                                   created?  (core/audit-created? entity)]
                             ;; never loosen to `or` — the UPDATE reads modified_by AND writes created_by
                             :when (and modified? created?)]
                       (try
                         (let [{result :jdbc.next/update-count}
                               (execute-one! conn
                                             [(format "UPDATE \"%s\" SET created_by = modified_by WHERE created_by IS NULL"
                                                      table)])]
                           (swap! backfill-count + (or (first result) 0))
                           (log/debug {:id ::table-backfilled
                                       :data {:rows (or (first result) 0) :table table}}
                                      "Backfilled records"))
                         (catch Exception e
                           (log/error! {:id ::table-backfill-failed :data {:table table}} e)))))
                   (log/info {:id ::backfill-complete :data {:action :migrated :subject :created-by :total @backfill-count}}
                             "Backfilled total records with created_by"))

                 (log/info {:id ::v101-complete :data {:action :upgraded :subject :iam-audit :version "1.0.1"}} "v1.0.1 upgrade complete")))

(patch/upgrade :synthigy.iam/audit
               "1.0.2"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::upgrading-v102 :data {:action :upgrading :subject :iam-audit :version "1.0.2"}}
                           "Upgrading to v1.0.2: adding FK constraints to audit columns")
                 (setup! *db* (deployed-model))
                 (log/info {:id ::v102-complete :data {:action :upgraded :subject :iam-audit :version "1.0.2"}} "v1.0.2 upgrade complete")))

(lifecycle/register-module!
 :synthigy.iam/audit
 {:depends-on [:synthigy/iam]
  :doc "Audit trail — principal _by columns + change capture"
  :setup (fn []
           (log/info {:id ::lifecycle-setup-starting :data {:action :setup :subject :audit-fields}}
                     "Retrofitting principal-aware audit columns onto existing tables")
           (setup! *db* (deployed-model))
           (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete :subject :audit-fields}}
                     "Audit retrofit complete"))
  :start (fn []
           (log/info {:id ::lifecycle-started :data {:action :started :subject :principal-audit}}
                     "Principal-aware audit enhancement active")
           (core/reload *db*))
  :stop (fn []
          (log/info {:id ::lifecycle-stopped :data {:action :stopped :subject :principal-audit}}
                    "Reverted to timestamp-only audit enhancement"))})
