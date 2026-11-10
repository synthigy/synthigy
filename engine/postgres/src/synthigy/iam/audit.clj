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

(ns synthigy.iam.audit
  "Postgres stamp DDL: the columns, defaults and triggers that keep stamp attributes."
  (:require
   [next.jdbc :as jdbc]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.dataset :refer [deployed-model]]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
   [synthigy.db :refer [*db*]]
   [synthigy.db.postgres]  ; Load Postgres JDBCBackend implementation
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.log :as log]))

(defn user-table-exists?
  [tx]
  (let [result (execute-one! tx
                             ["SELECT COUNT(*) as cnt FROM information_schema.tables
                               WHERE table_name = 'user' AND table_schema = 'public'"])]
    (pos? (:cnt result 0))))

(defn stamp-columns
  "[[column type stamp] …] of `entity`'s stamp attributes."
  [entity]
  (mapv (fn [a] [(normalize-name (:name a)) (:type a) (core/stamp a)]) (core/stamp-attributes entity)))

(defn stamp-trigger-body
  "The BEFORE UPDATE statements that keep `stamps`, or nil when none needs one."
  [stamps]
  (not-empty
   (apply str
          (for [[c t s] stamps]
            (case [t s]
              ["timestamp" :modify] (str "NEW.\"" c "\" = localtimestamp; ")
              ["timestamp" :create] (str "NEW.\"" c "\" = OLD.\"" c "\"; ")
              ["user" :create] (str "NEW.\"" c "\" = COALESCE(OLD.\"" c "\", NEW.\"" c "\"); ")
              nil)))))

(defn transform-audit-impl
  "Ensures every stamp column of `entities`, the timestamp defaults and one stamp trigger per table."
  [_ tx entities]
  (let [user-table? (user-table-exists? tx)
        stamped (filter (comp seq core/stamp-attributes) entities)]
    (log/info {:id ::transform-audit-starting
               :data {:entities (count entities) :stamped (count stamped)}}
              "Ensuring stamp columns and triggers")
    (doseq [entity entities
            :let [table (entity->table-name entity)
                  trigger (str "stamp_" table)
                  stamps (stamp-columns entity)
                  body (stamp-trigger-body stamps)]]
      (doseq [[c t _] stamps]
        (if (= "timestamp" t)
          (do (execute! tx [(str "ALTER TABLE \"" table "\" ADD COLUMN IF NOT EXISTS \"" c "\" timestamp")])
              (execute! tx [(str "ALTER TABLE \"" table "\" ALTER COLUMN \"" c "\" SET DEFAULT localtimestamp")]))
          (do (execute! tx [(str "ALTER TABLE \"" table "\" ADD COLUMN IF NOT EXISTS \"" c "\" bigint")])
              (when user-table?
                (execute! tx [(str "DO $$ BEGIN ALTER TABLE \"" table "\" ADD CONSTRAINT \"" table "_" c "_fkey\""
                                   " FOREIGN KEY (\"" c "\") REFERENCES \"user\"(_eid) ON DELETE SET NULL;"
                                   " EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL; END $$")])))))
      (if body
        (do (execute! tx [(str "CREATE OR REPLACE FUNCTION \"" trigger "\"() RETURNS TRIGGER AS $$ BEGIN "
                               body "RETURN NEW; END; $$ LANGUAGE plpgsql")])
            (execute! tx [(str "DROP TRIGGER IF EXISTS \"" trigger "\" ON \"" table "\"")])
            (execute! tx [(str "CREATE TRIGGER \"" trigger "\" BEFORE UPDATE ON \"" table
                               "\" FOR EACH ROW EXECUTE FUNCTION \"" trigger "\"()")]))
        (execute! tx [(str "DROP FUNCTION IF EXISTS \"" trigger "\"() CASCADE")]))
      (when (seq stamps)
        (log/debug {:id ::table-audited :data {:table table :stamps (mapv first stamps)}}
                   "Ensured stamps on table")))))

(extend-protocol enhance/AuditEnhancement
  synthigy.db.Postgres
  (transform-audit [db tx entities] (transform-audit-impl db tx entities)))

(defn setup!
  "Idempotently ensures the stamps of every entity in `model`."
  [db model]
  (with-open [conn (jdbc/get-connection (:datasource db))]
    (transform-audit-impl db conn (core/get-entities model))))

(patch/current-version :synthigy.iam/audit "1.1.0")

(comment
  (patch/read-version *db* :synthigy.iam/audit))

(patch/upgrade :synthigy.iam/audit
               "1.0.0"
               (when (instance? synthigy.db.Postgres *db*)
                 (setup! *db* (deployed-model))))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (when (instance? synthigy.db.Postgres *db*)
                 (setup! *db* (deployed-model))
                 (with-open [conn (jdbc/get-connection (:datasource *db*))]
                   (doseq [entity (core/get-entities (deployed-model))
                           :let [columns (set (map first (stamp-columns entity)))]
                           :when (every? columns ["modified_by" "created_by"])]
                     (execute-one! conn [(format "UPDATE \"%s\" SET created_by = modified_by WHERE created_by IS NULL"
                                                 (entity->table-name entity))])))
                 (log/info {:id ::v101-complete :data {:action :upgraded :subject :iam-audit :version "1.0.1"}}
                           "Backfilled created_by from modified_by")))

(patch/upgrade :synthigy.iam/audit
               "1.0.2"
               (when (instance? synthigy.db.Postgres *db*)
                 (setup! *db* (deployed-model))))

(patch/upgrade :synthigy.iam/audit
               "1.1.0"
               (when (instance? synthigy.db.Postgres *db*)
                 (with-open [conn (jdbc/get-connection (:datasource *db*))]
                   (execute! conn ["DROP FUNCTION IF EXISTS update_modified_on() CASCADE"])
                   (execute! conn ["DROP FUNCTION IF EXISTS preserve_created_audit() CASCADE"]))
                 (setup! *db* (deployed-model))
                 (log/info {:id ::v110-complete :data {:action :upgraded :subject :iam-audit :version "1.1.0"}}
                           "Audit triggers replaced by stamp triggers")))

(lifecycle/register-module!
 :synthigy.iam/audit
 {:depends-on [:synthigy/iam]
  :doc "Stamps — the database keeps stamp attributes (who and when)"
  :setup (fn []
           (log/info {:id ::lifecycle-setup-starting :data {:action :setup :subject :audit-fields}}
                     "Ensuring stamp columns and triggers on existing tables")
           (setup! *db* (deployed-model))
           (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete :subject :audit-fields}}
                     "Stamps ensured"))
  :start (fn []
           (log/info {:id ::lifecycle-started :data {:action :started :subject :principal-audit}}
                     "Stamps active")
           (core/reload *db*))
  :stop (fn []
          (log/info {:id ::lifecycle-stopped :data {:action :stopped :subject :principal-audit}}
                    "Stamps module stopped"))})
