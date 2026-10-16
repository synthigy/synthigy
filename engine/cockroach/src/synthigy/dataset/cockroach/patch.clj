;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.cockroach.patch
  "CockroachDB :synthigy/dataset feature patches and meta-model patches."
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.plug.cockroach :as plug]
    [synthigy.dataset.sql.naming
     :as naming
     :refer [normalize-name
             relation->table-name
             entity->relation-field
             entity->table-name]]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.db :refer [*db*]]
    [synthigy.db.cockroach]  ; Load Cockroach JDBCBackend implementation
    [synthigy.db.sql :as sql :refer [execute-one!]]
    [synthigy.log :as log]))

(defn id-trigger-function
  "SQL for `prevent_<id>_update()`, which REJECTS an id change (PG silently keeps the old id)."
  []
  (format
    "CREATE OR REPLACE FUNCTION prevent_%s_update()
RETURNS TRIGGER AS $$
BEGIN
  IF (OLD).%s IS NOT NULL AND (NEW).%s IS DISTINCT FROM (OLD).%s THEN
    RAISE EXCEPTION '%s is immutable';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;"
    (id/field) (id/field) (id/field) (id/field) (id/field)))

(defn id-trigger
  "Plain CREATE TRIGGER (no OR REPLACE on CRDB); run `drop-id-trigger` first."
  [table-name]
  (let [safe-table (clojure.string/replace table-name #"[^a-zA-Z0-9_]" "_")]
    (format
      "CREATE TRIGGER prevent_%s_update_trigger_%s
    BEFORE UPDATE ON \"%s\"
    FOR EACH ROW
    EXECUTE FUNCTION prevent_%s_update();"
      (id/field) safe-table table-name (id/field))))

(defn drop-id-trigger
  "DROP for `id-trigger`; the trigger names must match."
  [table-name]
  (let [safe-table (clojure.string/replace table-name #"[^a-zA-Z0-9_]" "_")]
    (format
      "DROP TRIGGER IF EXISTS prevent_%s_update_trigger_%s ON \"%s\";"
      (id/field) safe-table table-name)))

(defn drop-id-trigger-function
  []
  (format "DROP FUNCTION IF EXISTS prevent_%s_update() CASCADE;" (id/field)))

(defn sqlite-id-trigger
  [table-name]
  (format
    "CREATE TRIGGER IF NOT EXISTS prevent_%s_update_trigger
  BEFORE UPDATE ON \"%s\"
  FOR EACH ROW
  WHEN NEW.%s != OLD.%s
BEGIN
  SELECT RAISE(ABORT, 'Cannot modify %s value');
END;"
    (id/field) table-name (id/field) (id/field) (id/field)))

(defn drop-sqlite-id-trigger
  [table-name]
  (format
    "DROP TRIGGER IF EXISTS prevent_%s_update_trigger;"
    (id/field)))

(defn get-entity-tables
  []
  (let [schema (sql-query/deployed-schema)]
    (vec (distinct (keep :table (vals schema))))))

(defn create-id-immutability-triggers!
  "Installs the id immutability trigger on every deployed entity table."
  ([]
   (let [tables (get-entity-tables)]

     (log/info {:id ::creating-id-triggers
                :data {:table-count (count tables) :column (id/field)}}
               "Creating ID immutability triggers")
     ;; never CREATE OR REPLACE here — CRDB refuses while triggers depend on the fn
     (let [exists? (boolean
                    (seq (sql/execute!
                          [(format "select 1 from pg_proc where proname = 'prevent_%s_update'"
                                   (id/field))])))]
       (if exists?
         (log/debug {:id ::trigger-function-already-exists}
                    "prevent_<id>_update() already present; skipping CREATE")
         (do (sql/execute! [(id-trigger-function)])
             (log/debug {:id ::trigger-function-created} "Created trigger function"))))
     (let [created (atom 0)]
       (doseq [table tables]
         (try
           (sql/execute! [(drop-id-trigger table)])
           (sql/execute! [(id-trigger table)])
           (swap! created inc)
           (log/debug {:id ::trigger-created :data {:table table}} "Created trigger on table")
           (catch Throwable ex
             (log/error! {:id ::trigger-create-failed :data {:table table}} ex))))
       {:created @created
        :tables tables
        :type :postgres}))))

(defn remove-id-immutability-triggers!
  "Removes the id immutability triggers and function (rollback/testing)."
  []
  (let [tables (get-entity-tables)]

    (log/info {:id ::removing-id-triggers
               :data {:table-count (count tables) :column (id/field)}}
              "Removing ID immutability triggers")
    (doseq [table tables]
      (sql/execute! *db* [(drop-id-trigger table)])
      (log/debug {:id ::trigger-dropped :data {:table table}} "Dropped trigger from table"))
    (sql/execute! *db* [(drop-id-trigger-function)])
    (log/debug {:id ::trigger-function-dropped} "Dropped PostgreSQL/CockroachDB trigger function")

    {:removed (count tables)
     :tables tables
     :type :postgres}))

(defn is-mandatory-attribute?
  [attribute]
  (= "mandatory" (:constraint attribute)))

(defn get-column-constraints
  [table-name column-name]
  (try
    (execute-one! ["SELECT column_name, is_nullable, data_type
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ? AND table_schema = 'public'"
                   table-name column-name])
    (catch Exception _
      (log/warn {:id ::column-constraints-lookup-failed
                 :data {:table table-name :column column-name}}
                "Could not get column constraints")
      nil)))

(defn get-relation-indexes
  [relation]
  (let [table (relation->table-name relation)]
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (let [metadata (.getMetaData conn)
            indexes (.getIndexInfo metadata nil "public" table false false)]
        (loop [col indexes
               result []]
          (let [idx (.next col)]
            (if-not idx result
                    (recur col (conj result
                                     {:index (.getString col "INDEX_NAME")
                                      :column (.getString col "COLUMN_NAME")
                                      :type (if (.getBoolean col "NON_UNIQUE") :non-unique :unique)})))))))))

(defn get-column-type
  [table-name column-name]
  (try
    (execute-one! ["SELECT data_type
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ? AND table_schema = 'public'"
                   table-name column-name])
    (catch Exception _
      (log/warn {:id ::column-type-lookup-failed
                 :data {:table table-name :column column-name}}
                "Could not get column type")
      nil)))

(defn fix-on-reference-indexes
  []
  (let [statements (let [model (dataset/deployed-model)
                         relations (core/get-relations model)]
                     (reduce
                       (fn [r {:keys [from to]
                               :as relation}]
                         (let [table (relation->table-name relation)
                               from-field (entity->relation-field from)
                               to-field (entity->relation-field to)
                               indexes (set (map :index (get-relation-indexes relation)))]
                           (cond-> r
                             (not (indexes (str table \_ "fidx")))
                             (conj (format "create index %s_fidx on \"%s\" (%s);" table table from-field))
                               ;;
                             (not (indexes (str table \_ "tidx")))
                             (conj (format "create index %s_tidx on \"%s\" (%s);" table table to-field)))))
                       []
                       relations))]
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info {:id ::reference-index-created :data {:sql statement}}
                    "Created reference index")
          (catch Throwable ex
            (log/error! {:id ::reference-index-failed :data {:sql statement}} ex)))))))

(defn fix-on-delete-set-null-to-references
  []
  (let [model (dataset/deployed-model)
        entities (core/get-entities model)]
    (reduce
      (fn [r entity]
        (let [user-table-name "user"
              entity-table (entity->table-name entity)
              modified_by (str entity-table \_ "modified_by_fkey")
              refered-attributes (filter
                                   (comp
                                     #{"user" "group" "role"}
                                     :type)
                                   (:attributes entity))
                ;;
              current-result
              (conj r
                    (format "alter table \"%s\" drop constraint \"%s\"" entity-table modified_by)
                    (format
                      "alter table \"%s\" add constraint \"%s\" foreign key (modified_by) references \"%s\"(_eid) on delete set null"
                      entity-table modified_by user-table-name))]
          (reduce
            (fn [r {attribute-name :name
                    attribute-type :type}]
              (let [attribute-column (normalize-name attribute-name)
                    constraint-name (str entity-table \_ attribute-column "_fkey")
                    refered-table (case attribute-type
                                    "user" "user"
                                    "group" "user_group"
                                    "role" "user_role")]
                (conj
                  r
                  (format "alter table \"%s\" drop constraint %s" entity-table constraint-name)
                  (format
                    "alter table \"%s\" add constraint \"%s\" foreign key (%s) references \"%s\"(_eid) on delete set null"
                    entity-table constraint-name attribute-column refered-table))))
            current-result
            refered-attributes)))
      []
      entities)))

(defn fix-mandatory-constraints
  "Drops NOT NULL from mandatory columns; mandatory stays enforced in the model."
  []
  (let [model (dataset/deployed-model)
        entities (core/get-entities model)
        statements (reduce
                     (fn [statements entity]
                       (let [entity-table (entity->table-name entity)
                             mandatory-attributes (filter is-mandatory-attribute? (:attributes entity))
                             attribute-statements
                             (reduce
                               (fn [attr-statements {:keys [name]}]
                                 (let [column-name (normalize-name name)
                                       column-info (get-column-constraints entity-table column-name)
                                       has-not-null? (and column-info (= "NO" (:is_nullable column-info)))]
                                   (if has-not-null?
                                     (conj attr-statements
                                           (format "ALTER TABLE \"%s\" ALTER COLUMN %s DROP NOT NULL"
                                                   entity-table column-name))
                                     attr-statements)))
                               []
                               mandatory-attributes)]

                         (concat statements attribute-statements)))
                     []
                     entities)]
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info {:id ::mandatory-constraint-dropped :data {:sql statement}}
                    "Dropped NOT NULL constraint")
          (catch Throwable ex
            (log/error! {:id ::mandatory-constraint-failed :data {:sql statement}} ex)))))))

(defn fix-int-types
  "Converts integer columns of int attrs, user/group/role refs and modified_by to bigint."
  []
  (let [model (dataset/deployed-model)
        entities (core/get-entities model)
        statements (reduce
                     (fn [statements entity]
                       (let [entity-table (entity->table-name entity)
                             modified-by-type (get-column-type entity-table "modified_by")
                             modified-by-statements
                             (if (and modified-by-type (= "integer" (:data_type modified-by-type)))
                               [(format "ALTER TABLE \"%s\" ALTER COLUMN modified_by TYPE bigint" entity-table)]
                               [])
                             attribute-statements
                             (reduce
                               (fn [attr-statements {:keys [name type]}]
                                 (if-not (contains? #{"user" "group" "role" "int"} type)
                                   attr-statements
                                   (let [column-name (normalize-name name)
                                         current-type (get-column-type entity-table column-name)
                                         needs-conversion? (and current-type (= "integer" (:data_type current-type)))]
                                     (if needs-conversion?
                                       (conj attr-statements
                                             (format "ALTER TABLE \"%s\" ALTER COLUMN %s TYPE bigint"
                                                     entity-table column-name))
                                       attr-statements))))
                               []
                               (:attributes entity))]

                         (concat statements modified-by-statements attribute-statements)))
                     []
                     entities)]
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info {:id ::int-type-converted :data {:sql statement}}
                    "Converted integer column to bigint")
          (catch Throwable ex
            (log/error! {:id ::int-type-conversion-failed :data {:sql statement}} ex)))))))

(defn fix-deployed-on
  []
  (let [model (dataset/deployed-model)
        dataset-version-entity (core/get-entity model (id/entity :dataset/version))
        table-name (entity->table-name dataset-version-entity)]
    (try
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (execute-one! con [(format "update \"%s\" set deployed_on = modified_on" table-name)]))
      (catch Throwable ex
        (log/error! {:id ::deployed-on-patch-failed :data {:action :patching :subject :deployed-on}} ex)))))

(patch/upgrade :synthigy.dataset/model
               "1.0.0"
               (log/info {:id ::model-v100-deploying :data {:action :deploying :subject :dataset-model :version "1.0.0"}}
                         "Deploying meta-model v1.0.0 (removing Dataset Entity/Relation)")
               (dataset/deploy! (dataset/current-dataset-version)))

(patch/upgrade :synthigy.dataset/model
               "1.0.2"
               (log/info {:id ::model-v102-deploying :data {:action :deploying :subject :dataset-model :version "1.0.2"}}
                         "Deploying meta-model v1.0.2 (removing UI layout attributes)")
               (log/info {:id ::model-v102-ui-inactivation :data {:action :upgrading :subject :ui-layout :version "1.0.2"}}
                         "Width/Height/Position/Type/Path will become inactive")
               (dataset/deploy! (dataset/current-dataset-version))
               (log/info {:id ::model-v102-complete :data {:action :upgraded :subject :dataset-model :version "1.0.2"}}
                         "Migration complete - UI attributes preserved as inactive columns"))

(patch/upgrade :synthigy.dataset/model
               "1.0.3"
               (log/info {:id ::model-v103-deploying :data {:action :deploying :subject :dataset-model :version "1.0.3"}}
                         "Deploying meta-model v1.0.3 (removing UI layout attributes)")
               (log/info {:id ::model-v103-adding-deployed-on :data {:action :upgrading :subject :deployed-on :version "1.0.3"}}
                         "Adding deployed_on to dataset versions")
               (dataset/deploy! (dataset/current-dataset-version))
               (fix-deployed-on)
               (log/info {:id ::model-v103-complete :data {:action :upgraded :subject :dataset-model :version "1.0.3"}} "Migration complete"))

(patch/upgrade :synthigy.dataset/model
               "1.0.5"
               (log/info {:id ::model-v105-deploying
                          :data {:action :deploying :subject :dataset-model :version "1.0.5"}}
                         "Deploying meta-model v1.0.5 — audit opt-in for Dataset + Dataset Version")
               (dataset/deploy! (dataset/current-dataset-version))
               (log/info {:id ::model-v105-complete
                          :data {:action :upgraded :subject :dataset-model :version "1.0.5"}}
                         "Meta-model v1.0.5 deployed; audit fields now resolvable in selections"))

(patch/upgrade :synthigy.dataset/model
               "1.0.6"
               (log/info {:id ::model-v106-deploying
                          :data {:action :deploying :subject :dataset-model :version "1.0.6"}}
                         "Deploying meta-model v1.0.6 — RBAC opt-in on meta-entities")
               (dataset/deploy! (dataset/current-dataset-version))
               (log/info {:id ::model-v106-complete
                          :data {:action :upgraded :subject :dataset-model :version "1.0.6"}}
                         "Meta-model v1.0.6 deployed"))

(patch/upgrade :synthigy/dataset
               "0.5.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::v050-fix-int-types-starting :data {:action :upgrading :subject :int-types :version "0.5.0"}}
                           "Fixing integer types (integer to bigint)")
                 (fix-int-types)
                 (log/info {:id ::v050-fix-mandatory-starting :data {:action :upgrading :subject :mandatory-constraints :version "0.5.0"}}
                           "Removing NOT NULL constraints from mandatory fields")
                 (fix-mandatory-constraints)))

(patch/upgrade :synthigy/dataset
               "1.0.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::v100-initialized :data {:action :initialized :subject :dataset-features :version "1.0.0"}}
                           "PostgreSQL dataset features initialized at v1.0.0")))

(patch/upgrade :synthigy/dataset
               "1.0.1"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::v101-installing-id-triggers :data {:action :installing :subject :id-triggers :version "1.0.1"}}
                           "Installing ID immutability triggers")
                 (log/info {:id ::v101-rationale}
                           "Enables order-independent mapping for CockroachDB/SQLite compatibility")
                 (try
                   (let [result (create-id-immutability-triggers!)]
                     (log/info {:id ::v101-triggers-created
                                :data {:count (:created result)
                                       :type (name (:type result))}}
                               "Created ID immutability triggers")
                     (log/info {:id ::v101-protected-tables
                                :data {:tables (:tables result)}}
                               "Protected tables"))
                   (catch Exception e
                     (log/error! {:id ::v101-trigger-creation-failed} e)
                     (throw e)))
                 (log/info {:id ::v101-complete}
                           "EUUID immutability migration complete")))

(patch/upgrade :synthigy/dataset
               "1.2.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::v120-installing-relation-audit
                            :data {:action :installing :subject :dataset-features :version "1.2.0"}}
                           "Installing relation-audit triggers")
                 (try
                   (let [model (dataset/deployed-model)
                         schema (sql-query/model->schema model)
                         relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
                         unique-tables (set (keep :table relations))]
                     (plug/reconcile-relations! *db* (:datasource *db*) relations)
                     (log/info {:id ::v120-installed
                                :data {:action :installed :subject :dataset-features :version "1.2.0"
                                       :relation-count (count unique-tables)}}
                               "Relation-audit triggers installed"))
                   (catch Throwable e
                     (log/error! {:id ::v120-failed
                                  :data {:action :installing :subject :dataset-features :version "1.2.0"}} e)
                     (throw e)))))

(patch/upgrade :synthigy/dataset
               "1.3.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::v130-relation-xid-migration
                            :data {:action :upgrading :subject :dataset-features :version "1.3.0"}}
                           "Migrating relation tables: from_xid / to_xid denormalization")
                 (try
                   (let [model (dataset/deployed-model)
                         schema (sql-query/model->schema model)
                         relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
                         unique-tables (set (keep :table relations))]
                     (plug/reconcile-relations! *db* (:datasource *db*) relations)
                     (log/info {:id ::v130-installed
                                :data {:action :upgraded :subject :dataset-features :version "1.3.0"
                                       :relation-count (count unique-tables)}}
                               "Relation xid denormalization complete"))
                   (catch Throwable e
                     (log/error! {:id ::v130-failed
                                  :data {:action :upgrading :subject :dataset-features :version "1.3.0"}} e)
                     (throw e)))))

(patch/upgrade :synthigy/dataset
               "1.4.0"
               (when (instance? synthigy.db.Cockroach *db*)
                 (log/info {:id ::v140-enum-to-text
                            :data {:action :upgrading :subject :dataset-features :version "1.4.0"}}
                           "Migrating enum columns to TEXT")
                 (try
                   (let [enum-columns (sql/execute!
                                       ["select c.table_name, c.column_name
                                         from information_schema.columns c
                                         join pg_type t on t.typname = c.udt_name
                                         join pg_namespace n on n.oid = t.typnamespace
                                         where t.typtype = 'e' and n.nspname = 'public'
                                           and c.table_schema = 'public'"])
                         enum-types (sql/execute!
                                     ["select t.typname
                                       from pg_type t
                                       join pg_namespace n on n.oid = t.typnamespace
                                       where t.typtype = 'e' and n.nspname = 'public'"])]
                     (doseq [row enum-columns
                             :let [table (or (:columns/table_name row) (:table_name row))
                                   column (or (:columns/column_name row) (:column_name row))]]
                       (execute-one!
                        [(format "alter table \"%s\" alter column \"%s\" type text using \"%s\"::text"
                                 table column column)]))
                     (doseq [row enum-types
                             :let [type-name (or (:pg_type/typname row) (:typname row))]]
                       (try
                         (execute-one!
                          [(format "drop type if exists \"%s\"" type-name)])
                         (catch Throwable e
                           (log/warn {:id ::v140-drop-type-best-effort
                                      :data {:type type-name}}
                                     (.getMessage e)))))
                     (log/info {:id ::v140-complete
                                :data {:action :upgraded :subject :dataset-features :version "1.4.0"
                                       :columns (count enum-columns)
                                       :types (count enum-types)}}
                               "Enum columns migrated to TEXT, enum types dropped"))
                   (catch Throwable e
                     (log/error! {:id ::v140-failed
                                  :data {:action :upgrading :subject :dataset-features :version "1.4.0"}} e)
                     (throw e)))))

(patch/upgrade :synthigy/dataset
               "1.5.0"
               (log/info {:id ::v150-unique-index
                          :data {:action :upgraded :subject :dataset-features :version "1.5.0"}}
                         "Unique-groups-as-indexes: no-op on CockroachDB (constraint DDL is supported)"))
