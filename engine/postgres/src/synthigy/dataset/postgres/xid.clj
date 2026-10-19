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

(ns synthigy.dataset.postgres.xid
  "PostgreSQL legacy euuid → xid import and boot-time ID-format detection."
  (:require
    [clojure.string :as str]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.patch.model :as model]
    [synthigy.dataset.postgres.patch :as postgres.patch]
    [synthigy.dataset.sql.naming :as naming :refer [entity->table-name relation->table-name]]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.db.sql :as sql]
    [synthigy.env :as env]
    [synthigy.log :as log]
    [synthigy.supervisor :as supervisor]
    [synthigy.transit :refer [<-transit ->transit]]))


(defn table-exists?
  [table-name]
  (let [result (sql/execute-one!
                 ["SELECT COUNT(*) as cnt FROM information_schema.tables
                   WHERE table_name = ? AND table_schema = 'public'"
                  table-name])]
    (pos? (:cnt result 0))))

(defn get-all-entity-tables
  "Entity tables named by the deployed schema; may include tables missing from the DB."
  []
  (let [schema (sql-query/deployed-schema)]
    (vec (distinct (keep :table (vals schema))))))

(defn get-existing-entity-tables
  []
  (let [all-tables (get-all-entity-tables)
        existing (filterv table-exists? all-tables)
        missing (remove (set existing) all-tables)]
    (when (seq missing)
      (log/warn {:id ::missing-tables-skipped
                 :data {:count (count missing) :tables (vec missing)}}
                "Skipping missing tables"))
    existing))

(defn get-all-relation-tables
  []
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)]
    (vec (distinct (map relation->table-name relations)))))

(defn rename-relation-tables!
  "Renames relation tables from euuid- to xid-based names; call BEFORE switching the provider."
  []
  (log/info {:id ::relation-tables-rename-starting}
            "Renaming relation tables")
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)
        renamed (atom 0)]
    (doseq [relation relations]
      (let [relation-with-xid (if (:xid relation)
                                relation
                                (assoc relation :xid (id/uuid->nanoid (:euuid relation))))
            old-name (naming/relation->table-name-for-id relation-with-xid :euuid)
            new-name (naming/relation->table-name-for-id relation-with-xid :xid)]
        (when (and new-name (not= old-name new-name))
          (when (table-exists? old-name)
            (log/info {:id ::relation-table-renamed
                       :data {:from old-name :to new-name}}
                      "Renaming relation table")
            (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" old-name new-name)])
            (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                   (str old-name "_fidx") (str new-name "_fidx"))])
            (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                   (str old-name "_tidx") (str new-name "_tidx"))])
            (swap! renamed inc)))))
    (log/info {:id ::relation-tables-rename-complete
               :data {:renamed @renamed :relations (count relations)}}
              "Relation tables renamed")
    {:renamed @renamed
     :relations (count relations)}))

(defn revert-relation-tables!
  "Reverts relation table names from xid- back to euuid-based names (migration reset)."
  []
  (log/info {:id ::relation-tables-revert-starting}
            "Reverting relation table names")
  (let [model (dataset/deployed-model)
        relations (core/get-relations model)
        reverted (atom 0)]
    (doseq [relation relations]
      (let [xid (or (:xid relation) (id/uuid->nanoid (:euuid relation)))
            relation-with-xid (assoc relation :xid xid)]
        (when xid
          (let [xid-name (naming/relation->table-name-for-id relation-with-xid :xid)
                euuid-name (naming/relation->table-name-for-id relation-with-xid :euuid)]
            (when (not= xid-name euuid-name)
              (when (table-exists? xid-name)
                (log/info {:id ::relation-table-reverted
                           :data {:from xid-name :to euuid-name}}
                          "Reverting relation table")
                (sql/execute! [(format "ALTER TABLE \"%s\" RENAME TO \"%s\"" xid-name euuid-name)])
                (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                       (str xid-name "_fidx") (str euuid-name "_fidx"))])
                (sql/execute! [(format "ALTER INDEX IF EXISTS \"%s\" RENAME TO \"%s\""
                                       (str xid-name "_tidx") (str euuid-name "_tidx"))])
                (swap! reverted inc)))))))
    (log/info {:id ::relation-tables-revert-complete
               :data {:reverted @reverted :relations (count relations)}}
              "Relation tables reverted")
    {:reverted @reverted
     :relations (count relations)}))

(defn column-exists?
  [table-name column-name]
  (let [result (sql/execute-one!
                 [(str "SELECT COUNT(*) as cnt FROM information_schema.columns "
                       "WHERE table_name = ? AND column_name = ? AND table_schema = 'public'")
                  table-name column-name])]
    (pos? (:cnt result 0))))

(defn add-xid-column!
  [table-name]
  (if (column-exists? table-name "xid")
    (do
      (log/debug {:id ::xid-column-exists :data {:table table-name}}
                 "xid column already exists")
      :exists)
    (do
      (log/info {:id ::xid-column-added :data {:table table-name}}
                "Adding xid column to table")
      (sql/execute! [(format "ALTER TABLE \"%s\" ADD COLUMN xid VARCHAR(64)" table-name)])
      :added)))

(defn add-xid-columns-to-all-tables!
  []
  (let [tables (get-existing-entity-tables)
        results (doall (map (fn [table]
                              (try
                                [table (add-xid-column! table)]
                                (catch Exception e
                                  (log/error! {:id ::xid-column-add-failed
                                               :data {:table table}}
                                              e)
                                  [table :error])))
                            tables))
        grouped (group-by second results)]
    {:added (count (:added grouped))
     :existed (count (:exists grouped))
     :errors (count (:error grouped))
     :tables tables}))


(def ^:private default-batch-size
  "Default `_eid` stride per backfill UPDATE."
  50000)

(def uuid->nanoid-plpgsql
  "In-DB twin of id/uuid->nanoid — must stay byte-identical or the backfill corrupts ids silently."
  "CREATE OR REPLACE FUNCTION uuid_to_nanoid(u uuid)
RETURNS text AS $$
DECLARE
  alphabet text := '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  hexchars text := '0123456789abcdef';
  hex text;
  n numeric := 0;
  i int;
  d int;
  r int;
  s text := '';
BEGIN
  IF u IS NULL THEN
    RETURN NULL;
  END IF;
  hex := replace(u::text, '-', '');
  -- hex (big-endian) → numeric via Horner
  FOR i IN 1..32 LOOP
    d := position(substr(hex, i, 1) in hexchars) - 1;
    n := n * 16 + d;
  END LOOP;
  -- numeric → Base58, most-significant digit first
  WHILE n > 0 LOOP
    r := (n % 58)::int;
    s := substr(alphabet, r + 1, 1) || s;
    n := div(n, 58);
  END LOOP;
  -- left-pad with the zero digit ('1') to a fixed 22 chars
  RETURN lpad(s, 22, '1');
END;
$$ LANGUAGE plpgsql IMMUTABLE;")

(defn install-uuid->nanoid-fn!
  []
  (sql/execute! [uuid->nanoid-plpgsql])
  (log/debug {:id ::uuid-to-nanoid-fn-installed}
             "Installed server-side uuid_to_nanoid function"))

(defn generate-xids-for-table!
  "Backfills missing xids server-side in `_eid`-range chunks; the table needs `_eid` and `euuid`."
  ([table-name]
   (generate-xids-for-table! table-name {}))
  ([table-name {:keys [batch-size]
                :or {batch-size default-batch-size}}]
   (log/info {:id ::xid-generation-starting
              :data {:table table-name :batch-size batch-size}}
             "Generating XIDs for table")
   (supervisor/progress-update! {:detail table-name :done nil :total nil})
   (install-uuid->nanoid-fn!)
   (let [{:keys [lo hi]} (sql/execute-one!
                           [(format "SELECT min(_eid) AS lo, max(_eid) AS hi FROM \"%s\" WHERE xid IS NULL"
                                    table-name)])]
     (if (nil? lo)
       (do
         (log/info {:id ::xid-generation-complete
                    :data {:table table-name :updated 0 :batches 0}}
                   "Generated XIDs for table")
         {:table table-name :updated 0 :batches 0})
       (let [update-sql (format "UPDATE \"%s\" SET xid = uuid_to_nanoid(euuid) WHERE xid IS NULL AND _eid >= ? AND _eid < ?"
                                table-name)]
         (loop [start lo
                total 0
                batches 0]
           (if (> start hi)
             (do
               (log/info {:id ::xid-generation-complete
                          :data {:table table-name :updated total :batches batches}}
                         "Generated XIDs for table")
               {:table table-name
                :updated total
                :batches batches})
             (let [end (+ start batch-size)
                   res (sql/execute-one! [update-sql start end])
                   updated (:next.jdbc/update-count res 0)]
               (log/debug {:id ::xid-batch-updated
                           :data {:table table-name :batch (inc batches) :records updated}}
                          "Batch updated")
               (supervisor/progress-update! {:done (min (- end lo) (- (inc hi) lo))
                                  :total (- (inc hi) lo)})
               (recur end
                      (+ total updated)
                      (inc batches))))))))))

(defn get-all-tables-with-xid
  "Every public table with an xid column, not just those in the deployed schema."
  []
  (let [results (sql/execute!
                  ["SELECT table_name FROM information_schema.columns
                    WHERE column_name = 'xid' AND table_schema = 'public'"])]
    (mapv :table_name results)))

(defn add-xid-unique-constraint!
  [table-name]
  (let [constraint-name (str table-name "_xid_unique")]
    (try
      (sql/execute! [(format "ALTER TABLE \"%s\" ADD CONSTRAINT %s UNIQUE (xid)"
                             table-name constraint-name)])
      (log/info {:id ::xid-unique-constraint-added :data {:table table-name}}
                "Added UNIQUE constraint on xid")
      :added
      (catch Exception e
        (if (str/includes? (.getMessage e) "already exists")
          (do
            (log/debug {:id ::xid-unique-constraint-exists :data {:table table-name}}
                       "UNIQUE constraint already exists on xid")
            :exists)
          (throw e))))))

(defn add-xid-not-null-constraint!
  [table-name]
  (try
    (sql/execute! [(format "ALTER TABLE \"%s\" ALTER COLUMN xid SET NOT NULL" table-name)])
    (log/info {:id ::xid-not-null-constraint-added :data {:table table-name}}
              "Added NOT NULL constraint on xid")
    :added
    (catch Exception e
      (if (str/includes? (.getMessage e) "already set")
        (do
          (log/debug {:id ::xid-not-null-constraint-exists :data {:table table-name}}
                     "NOT NULL constraint already set on xid")
          :exists)
        (throw e)))))

(defn add-xid-index!
  [table-name]
  (let [index-name (str table-name "_xid_idx")]
    (try
      (sql/execute! [(format "CREATE INDEX IF NOT EXISTS %s ON \"%s\" (xid)"
                             index-name table-name)])
      (log/info {:id ::xid-index-added :data {:table table-name :index index-name}}
                "Added xid index")
      :added
      (catch Exception e
        (if (str/includes? (.getMessage e) "already exists")
          :exists
          (throw e))))))

(defn add-constraints!
  "Adds the xid UNIQUE constraint and index to every table with xid; run after the backfill."
  []
  (log/info {:id ::constraints-starting} "Adding XID constraints")
  (let [tables (get-all-tables-with-xid)
        results (doall
                  (map (fn [table]
                         (try
                           {:table table
                            :unique (add-xid-unique-constraint! table)
                            :index (add-xid-index! table)}
                           (catch Exception e
                             (log/error! {:id ::constraint-add-failed
                                          :data {:table table}}
                                         e)
                             {:table table
                              :error (.getMessage e)})))
                       tables))]
    {:tables results
     :count (count tables)}))

(defn postgres-xid-trigger-function
  []
  "CREATE OR REPLACE FUNCTION prevent_xid_update()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.xid IS NOT NULL AND NEW.xid IS DISTINCT FROM OLD.xid THEN
    -- Keep the old value instead of raising an exception
    NEW.xid := OLD.xid;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;")

(defn postgres-xid-trigger
  "SQL for the per-table trigger that silently reverts any xid UPDATE."
  [table-name]
  (format
    "CREATE OR REPLACE TRIGGER prevent_xid_update_trigger_%s
  BEFORE UPDATE ON \"%s\"
  FOR EACH ROW
  EXECUTE FUNCTION prevent_xid_update();"
    (str/replace table-name #"[^a-zA-Z0-9_]" "_")
    table-name))

(defn create-xid-immutability-triggers!
  []
  (let [tables (get-all-tables-with-xid)]
    (log/info {:id ::immutability-triggers-starting
               :data {:tables (count tables)}}
              "Creating XID immutability triggers")

    (sql/execute! [(postgres-xid-trigger-function)])
    (log/debug {:id ::trigger-function-created}
               "Created PostgreSQL trigger function for xid")

    (doseq [table tables]
      (try
        (sql/execute! [(postgres-xid-trigger table)])
        (log/debug {:id ::xid-trigger-created :data {:table table}}
                   "Created xid trigger on table")
        (catch Exception e
          (log/warn {:id ::xid-trigger-failed
                     :data {:table table :error (.getMessage e)}
                     :error e}
                    "Could not create xid trigger"))))

    {:created (count tables)
     :tables tables}))

(defn migrate-meta-table!
  [table-name]
  (log/info {:id ::meta-table-migrating :data {:action :migrating :subject :meta-table :table table-name}}
            "Migrating meta-table")

  (let [column-added (if (column-exists? table-name "xid")
                       false
                       (do
                         (sql/execute! [(format "ALTER TABLE %s ADD COLUMN xid VARCHAR(64)" table-name)])
                         true))
        res (sql/execute-one!
              [(format "UPDATE %s SET xid = uuid_to_nanoid(euuid) WHERE xid IS NULL" table-name)])
        updated (:next.jdbc/update-count res 0)]

    (log/info {:id ::meta-table-migrated
               :data {:action :migrated :subject :meta-table
                      :table table-name
                      :column (if column-added "added" "existed")
                      :updated updated}}
              "Meta-table migrated")

    {:table table-name
     :added-column column-added
     :updated updated}))

(defn migrate-meta-tables!
  "Adds and backfills xid on dataset_entity, dataset_relation and dataset_entity_attribute."
  []
  (log/info {:id ::meta-tables-migration-starting :data {:action :migrating :subject :meta-tables}}
            "Migrating meta-tables")
  (install-uuid->nanoid-fn!)
  (let [results {:entity (migrate-meta-table! "dataset_entity")
                 :relation (migrate-meta-table! "dataset_relation")
                 :attribute (migrate-meta-table! "dataset_entity_attribute")}
        total-updated (+ (get-in results [:entity :updated] 0)
                         (get-in results [:relation :updated] 0)
                         (get-in results [:attribute :updated] 0))]
    (log/info {:id ::meta-tables-migration-complete
               :data {:action :migrated :subject :meta-tables :total-updated total-updated}}
              "Meta-tables migrated")
    results))

(defn audited-table-names
  "Public tables carrying a `modified_by` column."
  []
  (set (map :table_name
            (sql/execute! ["SELECT DISTINCT table_name FROM information_schema.columns
                            WHERE table_schema = 'public' AND column_name = 'modified_by'"]))))

(defn normalize-stored-audit-config!
  "Unions #{:modified :created} into the audit actions of stored entities whose table has `modified_by`."
  []
  (log/info {:id ::audit-config-normalize-starting :data {:action :patching :subject :model}}
            "Normalizing legacy audit config across stored models")
  (let [audited (audited-table-names)
        versions (sql/execute! ["SELECT euuid, name, model FROM dataset_version
                                 WHERE deployed = true"])
        stats (atom {:versions 0 :entities 0})]
    (doseq [{:keys [euuid name model]} versions]
      (when model
        (let [m (<-transit model)
              changed (atom 0)
              entities' (reduce-kv
                          (fn [acc eid entity]
                            (let [audited? (and (string? (:name entity))
                                                (contains? audited (entity->table-name entity)))
                                  actions (get-in entity [:configuration :audit :actions])]
                              (if (and audited?
                                       (not (and (contains? actions :modified)
                                                 (contains? actions :created))))
                                (do (swap! changed inc)
                                    (assoc acc eid
                                           (assoc-in entity [:configuration :audit :actions]
                                                     (into #{:modified :created} actions))))
                                (assoc acc eid entity))))
                          {}
                          (:entities m))]
          (when (pos? @changed)
            (sql/execute! ["UPDATE dataset_version SET model = ? WHERE euuid = ?"
                           (->transit (assoc m :entities entities')) euuid])
            (swap! stats #(-> % (update :versions inc) (update :entities + @changed)))
            (log/info {:id ::audit-config-normalized
                       :data {:action :patching :subject :model
                              :version name :entities @changed}}
                      "Normalized audit config for stored model")))))
    (log/info {:id ::audit-config-normalize-complete
               :data {:action :patched :subject :model
                      :versions (:versions @stats) :entities (:entities @stats)}}
              "Normalized legacy audit config across stored models")
    @stats))

(defn migrate-all-tables!
  "Adds xid columns to data tables, backfills them, then adds the UNIQUE constraint and index."
  []
  (log/info {:id ::data-tables-migration-starting :data {:action :migrating :subject :data-tables}}
            "Starting XID migration on data tables")

  (log/info {:id ::data-tables-step-1} "Step 1: Adding xid columns")
  (let [column-result (add-xid-columns-to-all-tables!)]
    (log/info {:id ::data-tables-columns-added
               :data {:added (:added column-result)
                      :existed (:existed column-result)}}
              "Added xid columns")

    ;; never drop the _eid/euuid filter — xid-native infra tables would fail the backfill
    (log/info {:id ::data-tables-step-2} "Step 2: Generating XIDs for all records")
    (let [tables (->> (get-all-tables-with-xid)
                      (filterv (fn [t] (and (column-exists? t "_eid")
                                            (column-exists? t "euuid")))))
          gen-results (doall (map generate-xids-for-table! tables))
          total-updated (reduce + (map :updated gen-results))]
      (log/info {:id ::data-tables-xids-generated
                 :data {:total-updated total-updated :tables (count tables)}}
                "Generated XIDs across tables")

      (log/info {:id ::data-tables-step-3} "Step 3: Adding UNIQUE constraints")
      (let [constraint-results (doall
                                 (map (fn [table]
                                        (try
                                          {:table table
                                           :unique (add-xid-unique-constraint! table)
                                           :index (add-xid-index! table)}
                                          (catch Exception e
                                            (log/error! {:id ::constraint-add-failed
                                                         :data {:table table}}
                                                        e)
                                            {:table table :error (.getMessage e)})))
                                      tables))]
        (log/info {:id ::data-tables-constraints-added
                   :data {:tables (count tables)}}
                  "Added constraints to tables")

        {:columns column-result
         :generated {:total total-updated
                     :tables gen-results}
         :constraints constraint-results}))))

(defn migrate-to-xid!
  "Full deterministic euuid → xid migration, ending with the provider switch."
  []
  (log/info {:id ::migration-starting :data {:action :migrating :subject :xid}}
            "Starting complete XID migration (deterministic)")

  (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 1 :of 6 :name "migrate-meta-tables"}}
            "Migrating meta-tables")
  (supervisor/progress-step! 1 6 "migrate-meta-tables")
  (let [meta-result (migrate-meta-tables!)]

    (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 2 :of 6 :name "transform-stored-models"}}
              "Transforming stored models")
    (supervisor/progress-step! 2 6 "transform-stored-models")
    (let [model-result (model/transform-stored-models! :xid)]

      (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 3 :of 6 :name "migrate-data-tables"}}
                "Migrating data tables")
      (supervisor/progress-step! 3 6 "migrate-data-tables")
      (let [data-result (migrate-all-tables!)]

        ;; must run BEFORE the provider switch
        (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 4 :of 6 :name "rename-relation-tables"}}
                  "Renaming relation tables")
        (supervisor/progress-step! 4 6 "rename-relation-tables")
        (let [rename-result (rename-relation-tables!)]

          (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 5 :of 6 :name "add-immutability-triggers"}}
                    "Adding immutability triggers")
          (supervisor/progress-step! 5 6 "add-immutability-triggers")
          (create-xid-immutability-triggers!)

          (log/info {:id ::migration-step :data {:action :migrating :subject :xid :step 6 :of 6 :name "switch-provider"}}
                    "Switching to NanoID provider")
          (supervisor/progress-step! 6 6 "switch-provider")
          (dataset/set-format! "xid")
          (id/set-provider! (id/->NanoIDProvider))
          (dataset/save-model! nil)
          (dataset/reload)

          (log/info {:id ::migration-complete :data {:action :migrated :subject :xid}}
                    "XID migration complete")

          {:meta-tables meta-result
           :models model-result
           :data-tables data-result
           :relation-tables rename-result})))))

(defn verify-xid-migration
  "Throws unless every row of every existing entity table has an xid."
  []
  (log/info {:id ::verification-starting :data {:action :verifying :subject :xid-migration}} "Verifying XID migration")
  (let [tables (get-existing-entity-tables)
        results (doall
                  (map (fn [table]
                         (let [missing (sql/execute-one!
                                         [(format "SELECT COUNT(*) as cnt FROM \"%s\" WHERE xid IS NULL"
                                                  table)])]
                           {:table table
                            :missing (:cnt missing 0)}))
                       tables))
        missing-tables (filter #(pos? (:missing %)) results)]
    (if (empty? missing-tables)
      (do
        (log/info {:id ::verification-complete :data {:tables (count tables)}}
                  "XID migration verified: all records have XIDs")
        {:verified true
         :tables (count tables)})
      (do
        (log/error {:id ::verification-incomplete
                    :data {:tables-with-missing (count missing-tables)}}
                   "XID migration incomplete")
        (doseq [{:keys [table missing]} missing-tables]
          (log/error {:id ::verification-table-missing
                      :data {:table table :missing missing}}
                     "Records missing XIDs in table"))
        (throw (ex-info "Missing XIDs"
                        {:missing missing-tables
                         :verified false}))))))

(defn drop-xid-column!
  [table-name]
  (if (column-exists? table-name "xid")
    (do
      (log/info {:id ::xid-column-dropped :data {:table table-name}}
                "Dropping xid column")
      (sql/execute! [(format "ALTER TABLE \"%s\" DROP COLUMN xid" table-name)])
      :dropped)
    :not-exists))

(defn reset-xid-migration!
  "DESTRUCTIVE: fully reverts the xid import (models, relation names, xid columns, stamp, provider)."
  []
  (log/warn {:id ::reset-starting}
            "RESETTING XID MIGRATION")

  (log/info {:id ::reset-step :data {:step 1 :of 5 :name "revert-stored-models"}}
            "Reverting stored models to euuid format")
  (let [model-result (model/transform-stored-models! :euuid)]

    ;; must run BEFORE dropping xid columns
    (log/info {:id ::reset-step :data {:step 2 :of 5 :name "revert-relation-tables"}}
              "Reverting relation table names")
    (let [revert-result (revert-relation-tables!)]

      (log/info {:id ::reset-step :data {:step 3 :of 5 :name "drop-xid-columns"}}
                "Dropping xid columns")
      (let [tables-with-xid (get-all-tables-with-xid)
            _ (log/info {:id ::reset-tables-found
                         :data {:count (count tables-with-xid)}}
                        "Found tables with xid column")
            drop-results (doall
                           (map (fn [table]
                                  {:table table
                                   :result (drop-xid-column! table)})
                                tables-with-xid))
            dropped-count (count (filter #(= :dropped (:result %)) drop-results))]

        ;; never skip — a stale "xid" stamp boots the reverted DB as migrated
        (log/info {:id ::reset-step :data {:step 4 :of 5 :name "clear-format-stamp"}}
                  "Clearing stored format stamp")
        (dataset/clear-format!)

        ;; never skip — the import gate reads the ambient provider; a rerun would skip as :already-synthigy
        (log/info {:id ::reset-step :data {:step 5 :of 5 :name "restore-euuid-provider"}}
                  "Restoring euuid provider")
        (id/set-provider! (id/->UUIDProvider))
        (dataset/save-model! nil)
        (dataset/reload)

        (log/info {:id ::reset-complete
                   :data {:models-reverted (:total model-result 0)
                          :relation-tables-renamed (:reverted revert-result 0)
                          :xid-columns-dropped dropped-count}}
                  "Reset complete")

        {:models model-result
         :relation-tables revert-result
         :tables {:dropped dropped-count
                  :total (count tables-with-xid)
                  :details drop-results}}))))

(defn ensure-id-trigger-function!
  "Creates `prevent_<id>_update()` for the active id field; per-table triggers need it."
  []
  (log/debug {:id ::id-trigger-function-ensuring :data {:id-key (id/key)}}
             "Ensuring id immutability trigger function exists")
  (sql/execute-one! [(postgres.patch/postgres-id-trigger-function)]))

(defn detect-format
  "Detect ID format for Postgres when no format stamp is stored: \"xid\" for a
   fresh install, \"euuid\" for a legacy layout."
  []
  (cond
    (not (table-exists? "dataset_version"))
    (do
      (when (= "euuid" env/id-format)
        (log/warn {:id ::euuid-fresh-deploy-unsupported}
                  "SYNTHIGY_ID_FORMAT=euuid ignored — fresh installs are xid-only; euuid exists only as a legacy source to import. Deploying xid."))
      (log/info {:id ::id-format-fresh :data {:format "xid"}}
                "Fresh Postgres (no dataset_version), using xid")
      "xid")

    :else
    (do
      (log/info {:id ::id-format-legacy :data {:format "euuid"}}
                "Legacy Postgres (euuid only), using euuid")
      "euuid")))

(defn partial-migration?
  "True when dataset_version carries xid VALUES but no \"xid\" stamp — column presence alone is not a signal."
  []
  (and (not= "xid" (dataset/current-format))
       (table-exists? "dataset_version")
       (column-exists? "dataset_version" "xid")
       (pos? (:cnt (sql/execute-one!
                    ["SELECT count(xid) AS cnt FROM dataset_version"])
                   0))))

(comment
  ;; Simulate both import-crash windows on a healthy dev DB (restores itself):
  (do (dataset/clear-format!)                     ; window A: no stamp at all
      [(partial-migration?)                       ; => true
       (try (detect-and-set-provider!) :no-throw
            (catch clojure.lang.ExceptionInfo e (:state (ex-data e))))]) ; => :partial-migration
  (do (dataset/set-format! "euuid")               ; window B: stale legacy stamp
      (partial-migration?))                       ; => true
  (do (dataset/set-format! "xid")                 ; restore
      (partial-migration?)))                      ; => false

(defn legacy-deployment?
  "True when dataset_version exists but no ID format is stored."
  []
  (and (nil? (dataset/current-format))
       (table-exists? "dataset_version")))

(defn detect-and-set-provider!
  "Resolve the ID format (stored stamp, else detection), set the provider, and
   store the format. Throws on a half-migrated DB — never boot over one."
  []
  (when (partial-migration?)
    (throw (ex-info
            (str "INTERRUPTED XID MIGRATION DETECTED — dataset_version has an xid "
                 "column but the migration never stamped completion. Run "
                 "(synthigy.dataset.postgres.xid/reset-xid-migration!) and then rerun "
                 "(synthigy.dataset.postgres.xid/migrate->synthigy!). If this "
                 "database is in fact fully migrated, stamp it with "
                 "(synthigy.dataset/set-format! \"xid\") and restart.")
            {:action :not-initialized :subject :id-format :state :partial-migration})))
  (if-let [stored (dataset/current-format)]
    (do
      (log/info {:id ::id-format-stored-used :data {:format stored}}
                "Using stored ID format")
      (dataset/initialize-provider! stored)
      (ensure-id-trigger-function!))

    (let [format (detect-format)]
      (log/info {:id ::id-format-detected :data {:format format}}
                "Detected ID format")
      (dataset/initialize-provider! format)
      (ensure-id-trigger-function!)
      (dataset/set-format! format))))

(defn reconcile-legacy-meta-columns!
  "Idempotently adds and backfills `deployed_on`/`active` on a legacy meta-schema; must run before patcho."
  []
  (doseq [stmt ["ALTER TABLE dataset_version  ADD COLUMN IF NOT EXISTS deployed_on timestamp"
                "ALTER TABLE dataset_entity   ADD COLUMN IF NOT EXISTS active boolean"
                "ALTER TABLE dataset_relation ADD COLUMN IF NOT EXISTS active boolean"
                "UPDATE dataset_version  SET deployed_on = modified_on WHERE deployed = true AND deployed_on IS NULL"
                "UPDATE dataset_entity   SET active = true WHERE active IS NULL"
                "UPDATE dataset_relation SET active = true WHERE active IS NULL"]]
    (try
      (sql/execute-one! [stmt])
      (catch Throwable ex
        (log/warn {:id ::legacy-meta-reconcile-skip
                   :data {:action :patching :subject :meta-table :sql stmt}}
                  (.getMessage ex))))))

(defn reconcile-legacy-audit-columns!
  "Adds missing `created_by`/`created_on` to every legacy table carrying `modified_by`."
  []
  (try
    (sql/execute-one!
      ["DO $$
        DECLARE t text;
        BEGIN
          FOR t IN SELECT table_name FROM information_schema.columns
                   WHERE table_schema = 'public' AND column_name = 'modified_by'
          LOOP
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS created_by bigint', t);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp', t);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp', t);
          END LOOP;
        END $$;"])
    (log/info {:id ::legacy-audit-reconciled :data {:action :patching :subject :data-tables}}
              "Reconciled legacy audit columns (added created_by/created_on where missing)")
    (catch Throwable ex
      (log/warn {:id ::legacy-audit-reconcile-failed :data {:action :patching :subject :data-tables}}
                (.getMessage ex)))))

(defn legacy-euuid-layout?
  "True when the active id format is euuid, i.e. a legacy database awaiting import."
  []
  (= :euuid (id/key)))

(defn migrate->synthigy!
  "One-time, idempotent import of a legacy euuid Postgres database into the xid layout."
  []
  (if-not (legacy-euuid-layout?)
    (do (log/info {:id ::migrate-skip :data {:action :migrating :subject :xid}}
                  "Native Synthigy (xid) layout — no migration needed")
        {:skipped :already-synthigy})
    (do (log/warn {:id ::migrate-starting :data {:action :migrating :subject :xid}}
                  "Legacy database detected — migrating to Synthigy xid layout")
        ;; must run BEFORE migrating — transform-stored-models! reads deployed_on
        (reconcile-legacy-meta-columns!)
        (let [result (migrate-to-xid!)]
          (verify-xid-migration)
          (log/info {:id ::migrate-complete :data {:action :migrated :subject :xid}}
                    "Legacy → Synthigy xid migration complete and verified")
          (assoc result :migrated true)))))

(supervisor/register-method! "migrate.xid"
  (fn [_]
    ;; never drop this guard — serve! runs each frame on its own thread; a second click would migrate concurrently
    (if-not (supervisor/progress-begin! :migration "starting")
      (throw (ex-info "A migration is already running on this engine."
                      {:code -32000 :running (supervisor/progress)}))
      (try
        (migrate->synthigy!)
        (finally (supervisor/progress-finish!))))))
