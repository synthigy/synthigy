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
  "SQLite stamp DDL: the columns and triggers that keep stamp attributes."
  (:require
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.dataset :refer [deployed-model]]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.sql.naming :refer [entity->table-name normalize-name]]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.db.sqlite]  ; Load SQLite JDBCBackend implementation
   [synthigy.log :as log]))

(def now-sql "strftime('%Y-%m-%d %H:%M:%f', 'now')")

(defn table-columns
  [tx table]
  (try
    (->> (jdbc/execute! tx [(format "PRAGMA table_info(\"%s\")" table)])
         (map :name)
         (into #{}))
    (catch Exception _ #{})))

(defn user-table-exists?
  [tx]
  (boolean
   (seq (jdbc/execute! tx ["SELECT name FROM sqlite_master
                            WHERE type = 'table' AND name = 'user'"]))))

(defn stamp-columns
  "[[column type stamp] …] of `entity`'s stamp attributes."
  [entity]
  (mapv (fn [a] [(normalize-name (:name a)) (:type a) (core/stamp a)]) (core/stamp-attributes entity)))

(defn stamp-update-sets
  "The SET terms the AFTER UPDATE trigger applies to keep `stamps`."
  [stamps]
  (keep (fn [[c t s]]
          (case [t s]
            ["timestamp" :modify] (str "\"" c "\" = " now-sql)
            (["timestamp" :create] ["user" :create]) (str "\"" c "\" = COALESCE(OLD.\"" c "\", NEW.\"" c "\")")
            nil))
        stamps))

(defn transform-audit-impl
  "Ensures every stamp column of `entities` and the triggers that keep them."
  [_db tx entities]
  (let [user-table? (user-table-exists? tx)
        stamped (filter (comp seq core/stamp-attributes) entities)]
    (log/info {:id ::transform-audit-starting
               :data {:entities (count entities) :stamped (count stamped)}}
              "Ensuring stamp columns and triggers")
    (doseq [entity entities
            :let [table (entity->table-name entity)
                  stamps (stamp-columns entity)
                  columns (when (seq stamps) (table-columns tx table))
                  timestamps (keep (fn [[c t _]] (when (= "timestamp" t) c)) stamps)
                  sets (stamp-update-sets stamps)
                  update-trigger (str "stamp_" table)
                  insert-trigger (str "stamp_" table "_insert")]]
      (doseq [[c t _] stamps
              :when (not (contains? columns c))]
        (execute! tx [(str "ALTER TABLE \"" table "\" ADD COLUMN \"" c "\" "
                           (if (= "timestamp" t)
                             "TEXT"
                             (str "INTEGER" (when user-table? " REFERENCES \"user\"(_eid) ON DELETE SET NULL"))))]))
      (execute! tx [(str "DROP TRIGGER IF EXISTS \"" insert-trigger "\"")])
      (execute! tx [(str "DROP TRIGGER IF EXISTS \"" update-trigger "\"")])
      ;; fires only for a column added to a filled table, which can't carry the CREATE TABLE default
      (when (seq timestamps)
        (execute! tx [(str "CREATE TRIGGER \"" insert-trigger "\" AFTER INSERT ON \"" table "\" FOR EACH ROW"
                           " WHEN " (str/join " OR " (map #(str "NEW.\"" % "\" IS NULL") timestamps))
                           " BEGIN UPDATE \"" table "\" SET "
                           (str/join ", " (map #(str "\"" % "\" = COALESCE(NEW.\"" % "\", " now-sql ")") timestamps))
                           " WHERE _eid = NEW._eid; END")]))
      (when (seq sets)
        (execute! tx [(str "CREATE TRIGGER \"" update-trigger "\" AFTER UPDATE ON \"" table "\" FOR EACH ROW"
                           " BEGIN UPDATE \"" table "\" SET " (str/join ", " sets) " WHERE _eid = NEW._eid; END")]))
      (when (seq stamps)
        (log/debug {:id ::table-audited :data {:table table :stamps (mapv first stamps)}}
                   "Ensured stamps on table")))))

(extend-protocol enhance/AuditEnhancement
  synthigy.db.SQLite
  (transform-audit [db tx entities] (transform-audit-impl db tx entities)))

(defn setup!
  "Idempotently ensures the stamps of every deployed entity."
  []
  (with-open [conn (jdbc/get-connection (:datasource *db*))]
    (transform-audit-impl *db* conn (core/get-entities (deployed-model)))))

(patch/current-version :synthigy.iam/audit "1.1.0")

(comment
  (patch/read-version *db* :synthigy.iam/audit))

(patch/upgrade :synthigy.iam/audit
               "1.0.0"
               (setup!))

(patch/upgrade :synthigy.iam/audit
               "1.0.1"
               (setup!)
               (with-open [conn (jdbc/get-connection (:datasource *db*))]
                 (doseq [entity (core/get-entities (deployed-model))
                         :let [columns (set (map first (stamp-columns entity)))]
                         :when (every? columns ["modified_by" "created_by"])]
                   (execute-one! conn [(format "UPDATE \"%s\" SET created_by = modified_by WHERE created_by IS NULL"
                                               (entity->table-name entity))])))
               (log/info {:id ::v101-complete :data {:action :upgraded :subject :iam-audit :version "1.0.1"}}
                         "Backfilled created_by from modified_by"))

(patch/upgrade :synthigy.iam/audit
               "1.1.0"
               (with-open [conn (jdbc/get-connection (:datasource *db*))]
                 (doseq [{trigger :name} (jdbc/execute! conn ["SELECT name FROM sqlite_master WHERE type = 'trigger'
                                                               AND (name LIKE 'update\\_%\\_modified\\_on' ESCAPE '\\'
                                                                    OR name LIKE 'preserve\\_%\\_created\\_audit' ESCAPE '\\')"])]
                   (execute! conn [(str "DROP TRIGGER IF EXISTS \"" trigger "\"")])))
               (setup!)
               (log/info {:id ::v110-complete :data {:action :upgraded :subject :iam-audit :version "1.1.0"}}
                         "Audit triggers replaced by stamp triggers"))

(lifecycle/register-module!
 :synthigy.iam/audit
 {:depends-on [:synthigy/iam]
  :doc "Stamps — the database keeps stamp attributes (who and when)"
  :setup (fn []
           (log/info {:id ::lifecycle-setup-starting :data {:action :setup :subject :audit-fields}}
                     "Ensuring stamp columns and triggers (SQLite)")
           (setup!)
           (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete :subject :audit-fields}}
                     "Stamps ensured (SQLite)"))
  :start (fn []
           (log/info {:id ::lifecycle-started :data {:action :started :subject :principal-audit}}
                     "Stamps active (SQLite)")
           (core/reload *db*))
  :stop (fn []
          (log/info {:id ::lifecycle-stopped :data {:action :stopped :subject :principal-audit}}
                    "Stamps module stopped (SQLite)"))})
