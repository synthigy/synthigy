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

(ns synthigy.dataset.postgres
  (:require
   [buddy.hashers :as hashers]
   clojure.data
   clojure.set
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   next.jdbc.date-time
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   synthigy.dataset.operations
   [synthigy.dataset.postgres.collation :as collation]
   ;; required for its load-time patch registrations (no direct refs)
   synthigy.dataset.postgres.patch
   [synthigy.dataset.postgres.xid :as xid]
   synthigy.dataset.projection
   [synthigy.dataset.sql.naming
    :as naming
    :refer [column-name
            relation->table-name
            entity->relation-field
            entity->table-name
            SQLNameResolution]]
   [synthigy.dataset.sql.errors :as errors]
   [synthigy.dataset.sql.protocol :as proto]
   [synthigy.dataset.sql.query :as query]
   [synthigy.dataset.sql.rls :as rls]
   [synthigy.dataset.system :as system]
   [synthigy.db
    :refer [*db*
            sync-entity
            delete-entity]]
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.json :refer [<-json ->json]]
   [synthigy.log :as log]
   ;; loaded for its lifecycle registration — :synthigy/dataset depends on :synthigy/log.config
   [synthigy.log.config]
   ;; loaded for their registrations — patches dataset/start levels, the meta-model write hooks
   [synthigy.dataset.patch]
   [synthigy.dataset.meta]
   [synthigy.timestamp :as ts]
   [synthigy.transit
    :refer [<-transit ->transit]])
  (:import
   [org.postgresql.util PGobject]
   [synthigy.db Postgres]))

(defn reference-table
  "Table name for a reference type, or nil (with a warning) when unregistered."
  [type-name]
  (if-let [table-fn (core/reference-table-fn type-name)]
    (table-fn)
    (do
      (log/warn {:id ::reference-type-unregistered :data {:type-name type-name}}
                "No table-fn registered for reference type")
      nil)))

(defn check-type-conversion!
  "Throws when core/validate-type-conversion forbids the type change; logs warnings."
  [entity attribute old-type new-type]
  (let [validation (core/validate-type-conversion old-type new-type)]
    (cond
      (:safe validation)
      true

      (:warning validation)
      (do
        (log/warn {:id ::type-conversion-warning
                   :data {:entity (:name entity)
                          :attribute (:name attribute)
                          :from-type old-type
                          :to-type new-type
                          :warning (:warning validation)}}
                  "Type conversion warning")
        true)

      (:error validation)
      (throw
       (ex-info
        (format "Forbidden type conversion for %s.%s: %s → %s\n%s"
                (:name entity)
                (:name attribute)
                old-type
                new-type
                (:error validation))
        {:type (or (:type validation) :dataset/forbidden-conversion)
         :code "TYPE_CONVERSION_FORBIDDEN"
         :hint (:suggestion validation)
         :entity (:name entity)
         :attribute (:name attribute)
         :from-type old-type
         :to-type new-type
         :suggestion (:suggestion validation)}))

      :else
      (throw
       (ex-info
        (format "Unknown validation result for %s.%s: %s → %s"
                (:name entity)
                (:name attribute)
                old-type
                new-type)
        {:validation validation})))))

(defn type->ddl
  "Converts type to DDL syntax"
  [t]
  (try
    (if (core/reference-type? t)
      (if-let [table (reference-table t)]
        (str "bigint references \"" table "\"(_eid) on delete set null")
        (throw (ex-info
                (format "Cannot generate DDL for unregistered reference type '%s'" t)
                {:type ::unregistered-reference-type
                 :phase :ddl-generation
                 :reference-type t})))
      (if (core/reference-type-names t)
        ;; never fall through to the case — it emits a bare `user` column type
        (throw (ex-info
                (format "Reference type '%s' is not registered; register IAM reference types before mounting a model that uses them" t)
                {:type ::unregistered-reference-type
                 :phase :ddl-generation
                 :reference-type t}))
        (case t
        "currency" "jsonb"
        ("string" "enum") collation/human-ddl
        ("avatar" "hashed") "text"
        "timestamp" "timestamp"
        ("json" "encrypted" "timeperiod") "jsonb"
        "transit" "text"
        "int" "bigint"
        t)))
    (catch Throwable e
      (throw (ex-info
              (format "Failed to generate DDL for type '%s'" t)
              {:type ::type-ddl-generation-error
               :phase :ddl-generation
               :attribute-type t}
              e)))))

(defn attribute->ddl
  [_entity {n :name
            t :type}]
  (clojure.string/join
   " "
   (remove
    empty?
    [(column-name n)
     (type->ddl t)])))

(defn normalized-enum-value [value]
  (clojure.string/replace value #"-|\s" "_"))

(defn generate-entity-ddl
  "For given model and entity returns entity table DDL"
  [{n :name
    as :attributes
    {cs :constraints} :configuration
    :as entity}]
  (try
    (let [table (entity->table-name entity)
          as' (keep #(attribute->ddl entity %) as)
          pk ["_eid bigserial not null primary key"
              (str (id/field) " varchar(64) not null unique")]
          cs' (keep-indexed
               (fn [idx ids]
                 (when (not-empty ids)
                   (format
                    "constraint \"%s\" unique(%s)"
                    (str table "_eucg_" idx)
                    (clojure.string/join
                     ","
                     (map
                      (fn [id]
                        (log/info {:id ::constraint-generating
                                   :data {:entity n :attribute-id id}}
                                  "Generating constraint")
                        (->
                         (core/get-attribute entity id)
                         :name
                         column-name))
                      ids)))))
               (:unique cs))
          audit-cols (cond-> []
                       (core/audit-modified? entity)
                       (conj "modified_on timestamp not null default localtimestamp")
                       (core/audit-created? entity)
                       (conj "created_on timestamp not null default localtimestamp"))
          rows (concat pk as' audit-cols cs')]
      (format
       "create table \"%s\" (\n  %s\n)"
       table
       (clojure.string/join ",\n  " rows)))
    (catch Throwable e
      (throw (ex-info
              (format "Failed to generate CREATE TABLE DDL for entity '%s'" n)
              {:type ::entity-ddl-generation-error
               :phase :ddl-generation
               :entity-name n
               :entity-id (id/extract entity)
               :table-name (try (entity->table-name entity) (catch Throwable _ nil))
               :attribute-count (count as)
               :has-constraints (some? cs)}
              e)))))

(defn generate-relation-ddl
  "Relation table DDL, including the from_xid/to_xid audit columns."
  [_ {f :from
      t :to
      :as relation}]
  (try
    (let [table (relation->table-name relation)
          from-table (entity->table-name f)
          to-table (entity->table-name t)
          from-field (entity->relation-field f)
          to-field (entity->relation-field t)]
      (format
       "create table if not exists \"%s\"(\n %s\n)"
       table
       (if (= f t)
         (clojure.string/join
          ",\n "
          [(str to-field " bigint not null references \"" to-table "\"(_eid) on delete cascade")
           "from_xid text"
           "to_xid text"
           (str "unique(" to-field ")")])
         (clojure.string/join
          ",\n "
          [(str from-field " bigint not null references \"" from-table "\"(_eid) on delete cascade")
           (str to-field " bigint not null references \"" to-table "\"(_eid) on delete cascade")
           "from_xid text"
           "to_xid text"
           (str "unique(" from-field "," to-field ")")]))))
    (catch Throwable e
      (throw (ex-info
              (format "Failed to generate CREATE TABLE DDL for relation between '%s' and '%s'"
                      (:name f) (:name t))
              {:type ::relation-ddl-generation-error
               :phase :ddl-generation
               :from-entity (:name f)
               :from-entity-id (id/extract f)
               :to-entity (:name t)
               :to-entity-id (id/extract t)
               :relation-id (id/extract relation)
               :is-recursive (= f t)}
              e)))))

(defn generate-relation-indexes-ddl
  [_ {f :from
      t :to
      :as relation}]
  (let [table (relation->table-name relation)
        from-field (entity->relation-field f)
        to-field (entity->relation-field t)]
    [(format "create index if not exists \"%s_fidx\" on \"%s\" (%s);" table table from-field)
     (format "create index if not exists \"%s_tidx\" on \"%s\" (%s);" table table to-field)]))

(defn analyze-projection
  [projection]
  (let [new-entities
        (filter
         core/added?
         (core/get-entities projection))
        reactivated-entities
        (filter
         (fn [entity]
           (and (not (core/added? entity))
                (false? (:active (core/suppress entity)))
                (true? (:active entity))))
         (core/get-entities projection))
        changed-enties
        (filter
         core/diff?
         (core/get-entities projection))
        new-relations
        (filter
         (fn [relation]
           (core/added? relation))
         (core/get-relations projection))
        changed-relations
        (filter
         (fn [relation]
           (core/diff? relation))
         (core/get-relations projection))
        {nrr true
         nr false} (group-by core/recursive-relation? new-relations)
        {crr true
         cr false} (group-by core/recursive-relation? changed-relations)]
    {:new/entities new-entities
     :reactivated/entities reactivated-entities
     :changed/entities changed-enties
     :new/relations nr
     :new/recursive-relations nrr
     :changed/relations cr
     :changed/recursive-relations crr}))

(defn attribute-delta->ddl
  [entity
   {:keys [name type]
    {:keys [values]} :configuration
    :as attribute}]
  (let [oentity (core/suppress entity)
        old-table (entity->table-name oentity)
        diff (core/diff attribute)]
    (if (core/new-attribute? attribute)
      (do
        (log/debug {:id ::attribute-added :data {:attribute name :table old-table}}
                   "Adding attribute to table")
        [(str "alter table \"" old-table "\" add column if not exists " (column-name name) " " (type->ddl type))])
      (when (not-empty (dissoc diff :pk))
        (let [{dn :name
               dt :type
               dconfig :configuration} diff
              old-column (column-name (or dn name))
              column (column-name name)]
          (when dt (check-type-conversion! entity attribute dt type))
          (cond-> []
            (not= old-column column)
            (conj
             (do
               (log/debug {:id ::column-renamed
                           :data {:table old-table :from old-column :to column}}
                          "Renaming table column")
               (format
                "alter table \"%s\" rename column %s to %s"
                old-table old-column column)))
            dt
            (as-> statements
                  (do
                    (log/debug {:id ::column-type-changed
                                :data {:table old-table :column column :from-type dt :to-type type}}
                               "Changing table column type")
                    (conj
                     statements
                     (let [alter-stmt
                           (cond->
                            (format
                             "alter table \"%s\" alter column %s type %s"
                             old-table column
                             (type->ddl type))
                             (= "int" type) (str (if (= "float" dt)
                                                  (str " using(round(" column ")::bigint)")
                                                  (str " using(trim(" column "::text)::bigint)")))
                             (= "float" type) (str " using(trim(" column "::text)::double precision)")
                             (= "string" type) (str " using(" column "::text)")
                             (= "json" type) (str " using\n case"
                                                  "\n  when " column " is null then null"
                                                  "\n  when " column " ~ '^\\s*[\\{\\[]' then " column "::jsonb"
                                                  "\n  else to_jsonb(" column ")"
                                                  "\nend")
                             (= "transit" type) (str " using(" column "::text)")
                             (= "avatar" type) (str " using(" column "::text)")
                             (= "encrypted" type) (str " using(" column "::text)")
                             (= "hashed" type) (str " using(" column "::text)")
                             (= "boolean" type) (str " using(trim(" column "::text)::boolean)")
                             (= "enum" type) (str " using(" column "::text)"))]
                   ;; never run the json USING guard on an already-jsonb column — jsonb ~ text has no operator
                       (if (= "json" type)
                         (format
                          (str "do $$ begin\n"
                               "if exists (select 1 from information_schema.columns\n"
                               "           where table_name = '%s' and column_name = '%s'\n"
                               "             and table_schema = 'public' and data_type <> 'jsonb') then\n"
                               "  execute '%s';\n"
                               "end if;\nend $$")
                          old-table
                          (str/replace column "\"" "")
                          (str/replace alter-stmt "'" "''"))
                         alter-stmt)))))
            (and (= type "enum") (nil? dt) (not-empty dconfig))
            (as-> statements
                  (let [[ov nv _] (clojure.data/diff
                                   (reduce
                                    (fn [r [idx {n :name
                                                 :as row}]]
                                      (let [id (id/extract row)]
                                        (assoc r
                                               (or id (get-in values [idx (id/key)]))
                                               (or n (get-in values [idx :name])))))
                                    nil
                                    (map-indexed (fn [idx v] (vector idx v)) (:values dconfig)))
                                   (zipmap
                                    (map (id/key) values)
                                    (map :name values)))
                        column (column-name name)
                        removed (reduce-kv
                                 (fn [r k v] (if (contains? nv k) r (conj r v)))
                                 []
                                 ov)
                        renames (reduce-kv
                                 (fn [r id old-name]
                                   (let [new-name (get nv id)]
                                     (if (and new-name old-name (not= old-name new-name))
                                       (conj r [old-name new-name])
                                       r)))
                                 []
                                 ov)]
                    (log/trace {:id ::enum-diff
                                :data {:config dconfig :old-enums ov :new-enums nv}}
                               "Diffing enum values")
                    (cond-> statements
                      (seq removed)
                      (conj
                       (format
                        "do $$ begin if exists (select 1 from \"%s\" where %s::text in (%s)) then raise exception 'Cannot remove enum value(s) %s from %s.%s - rows still reference them; deactivate the value instead'; end if; end $$;"
                        old-table column
                        (str/join ", " (map #(str \' (normalized-enum-value %) \') removed))
                        (str/join ", " removed) old-table column))
                      (seq renames)
                      (into
                       (map (fn [[old-name new-name]]
                              (format "update \"%s\" set %s = '%s' where %s::text = '%s'"
                                      old-table column (normalized-enum-value new-name)
                                      column (normalized-enum-value old-name)))
                            renames)))))))))))

(defn orphaned-attribute->drop-ddl
  "DROP COLUMN ... CASCADE for an attribute no remaining deployed version references."
  [{:keys [entity attribute]}]
  (let [table-name (entity->table-name entity)
        column-name (column-name (:name attribute))]
    [(format "ALTER TABLE \"%s\" DROP COLUMN IF EXISTS %s CASCADE"
             table-name column-name)]))

(defn entity-delta->ddl
  [{:keys [attributes]
    :as entity}]
  (assert (core/diff? entity) "This entity is already synced with DB")
  (let [diff (core/diff entity)
        old-entity (core/suppress entity)
        old-table (entity->table-name old-entity)
        old-constraints (get-in old-entity [:configuration :constraints :unique])
        ;; never diff against the old side — `suppress` carries the NEW :active flags
        new-unique (core/unique-constraints-indexed entity)
        raw-groups (max (count (get-in entity [:configuration :constraints :unique]))
                        (count old-constraints))
        group-members (into #{}
                            (comp cat cat)
                            [(get-in entity [:configuration :constraints :unique])
                             old-constraints])
        table (entity->table-name entity)
        attributes' (keep #(attribute-delta->ddl entity %) attributes)]
    #_(do
        (def attributes' attributes')
        (def diff diff)
        (def entity entity)
        (def attributes attributes)
        (def old-entity old-entity)
        (def old-table old-table)
        (def table table)
        (def old-constraints old-constraints))
    (cond-> (->> attributes'
                 (reduce into [])
                 (remove #(or (nil? %) (and (string? %) (str/blank? %))))
                 vec)
      (:name diff)
      (into
       (cond->
        [(format "alter table \"%s\" rename to \"%s\"" old-table table)
         (format "alter table \"%s\" rename constraint \"%s_pkey\" to \"%s_pkey\"" table old-table table)
         (format "alter table \"%s\" rename constraint \"%s_%s_key\" to \"%s_%s_key\"" table old-table (id/field) table (id/field))
         (format "alter sequence \"%s__eid_seq\" rename to \"%s__eid_seq\"" old-table table)]
         (some? old-constraints)
         (into
          (map-indexed
           (fn [idx _]
             (let [constraint (str "_eucg_" idx)]
               (format
                "alter table \"%s\" rename constraint \"%s%s\" to \"%s%s\""
                table old-table constraint
                table constraint)))
           old-constraints))))
      ;; second arm is load-bearing: deactivation leaves the constraints config untouched
      (or (-> diff :configuration :constraints)
          (some #(group-members (id/extract %)) (:attributes diff)))
      (into
       (reduce
        (fn [statements idx]
          (let [n (get new-unique idx)
                constraint (str table "_eucg_" idx)
                statements (conj statements
                                 (format "alter table \"%s\" drop constraint if exists %s"
                                         table constraint))]
            (cond-> statements
              (seq n)
              (conj (format "alter table \"%s\" add constraint %s unique(%s)"
                            table constraint
                            (str/join "," (map #(-> (core/get-attribute entity %)
                                                    :name
                                                    column-name)
                                               n)))))))
        []
        (range raw-groups))))))

(defn transform-relation
  [tx {:keys [from to]
       :as relation}]
  (try
    (let [diff (core/diff relation)
          _ (log/trace {:id ::relation-transforming
                        :data {:diff diff :from (pr-str from) :to (pr-str to)}}
                       "Transforming relation")
          from-diff (:from diff)
          to-diff (:to diff)
          old-from (core/suppress from)
          old-to (core/suppress to)
          old-relation (core/suppress relation)
          old-name (relation->table-name old-relation)
          new-name (relation->table-name relation)]

      (when (not= old-name new-name)
        (let [sql (format
                   "alter table \"%s\" rename to \"%s\""
                   old-name new-name)]
          (log/debug {:id ::relation-renamed
                      :data {:from-table old-name :to-table new-name :sql sql}}
                     "Renaming relation table")
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (errors/relation-ddl-error relation
                      (format "Failed to rename relation table from '%s' to '%s' (relation: %s → %s)"
                              old-name new-name (:name from) (:name to))
                      {:type ::relation-rename-error
                       :phase :ddl-execution
                       :operation :rename-table
                       :from-entity (:name from)
                       :from-entity-id (id/extract from)
                       :to-entity (:name to)
                       :to-entity-id (id/extract to)
                       :relation-id (id/extract relation)
                       :old-table-name old-name
                       :new-table-name new-name
                       :sql sql}
                      e)))))
        (let [old-fidx (str old-name "_fidx")
              new-fidx (str new-name "_fidx")
              old-tidx (str old-name "_tidx")
              new-tidx (str new-name "_tidx")]
          (let [sql (format "alter index if exists \"%s\" rename to \"%s\"" old-fidx new-fidx)]
            (log/debug {:id ::relation-from-index-renamed
                        :data {:from-index old-fidx :to-index new-fidx :sql sql}}
                       "Renaming relation from-index")
            (try
              (execute-one! tx [sql])
              (catch Throwable e
                (throw (errors/relation-ddl-error relation
                        (format "Failed to rename relation from-index from '%s' to '%s'" old-fidx new-fidx)
                        {:type ::relation-index-rename-error
                         :phase :ddl-execution
                         :operation :rename-from-index
                         :relation-id (id/extract relation)
                         :old-index-name old-fidx
                         :new-index-name new-fidx
                         :sql sql}
                        e)))))
          (let [sql (format "alter index if exists \"%s\" rename to \"%s\"" old-tidx new-tidx)]
            (log/debug {:id ::relation-to-index-renamed
                        :data {:from-index old-tidx :to-index new-tidx :sql sql}}
                       "Renaming relation to-index")
            (try
              (execute-one! tx [sql])
              (catch Throwable e
                (throw (errors/relation-ddl-error relation
                        (format "Failed to rename relation to-index from '%s' to '%s'" old-tidx new-tidx)
                        {:type ::relation-index-rename-error
                         :phase :ddl-execution
                         :operation :rename-to-index
                         :relation-id (id/extract relation)
                         :old-index-name old-tidx
                         :new-index-name new-tidx
                         :sql sql}
                        e)))))))
      (when (:name to-diff)
        (let [o (entity->relation-field old-to)
              n (entity->relation-field to)
              sql (format
                   "alter table \"%s\" rename column %s to %s"
                   new-name o n)]
          (log/debug {:id ::relation-to-column-renamed
                      :data {:table new-name :from-column o :to-column n :sql sql}}
                     "Renaming relation to-column")
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (errors/relation-ddl-error relation
                      (format "Failed to rename 'to' column in relation table '%s' from '%s' to '%s' (relation: %s → %s)"
                              new-name o n (:name from) (:name to))
                      {:type ::relation-column-rename-error
                       :phase :ddl-execution
                       :operation :rename-to-column
                       :from-entity (:name from)
                       :from-entity-id (id/extract from)
                       :to-entity (:name to)
                       :to-entity-id (id/extract to)
                       :relation-id (id/extract relation)
                       :table-name new-name
                       :old-column-name o
                       :new-column-name n
                       :sql sql}
                      e))))))
      (when (:name from-diff)
        (let [o (entity->relation-field old-from)
              n (entity->relation-field from)
              sql (format
                   "alter table \"%s\" rename column %s to %s"
                   new-name o n)]
          (log/debug {:id ::relation-from-column-renamed
                      :data {:table new-name :from-column o :to-column n :sql sql}}
                     "Renaming relation from-column")
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (errors/relation-ddl-error relation
                      (format "Failed to rename 'from' column in relation table '%s' from '%s' to '%s' (relation: %s → %s)"
                              new-name o n (:name from) (:name to))
                      {:type ::relation-column-rename-error
                       :phase :ddl-execution
                       :operation :rename-from-column
                       :from-entity (:name from)
                       :from-entity-id (id/extract from)
                       :to-entity (:name to)
                       :to-entity-id (id/extract to)
                       :relation-id (id/extract relation)
                       :table-name new-name
                       :old-column-name o
                       :new-column-name n
                       :sql sql}
                      e)))))))
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Throwable e
      (throw (ex-info
              (format "Failed to transform relation: %s → %s"
                      (:name from) (:name to))
              {:type ::relation-transformation-error
               :phase :ddl-execution
               :from-entity (:name from)
               :from-entity-id (id/extract from)
               :to-entity (:name to)
               :to-entity-id (id/extract to)
               :relation-id (id/extract relation)}
              e)))))

(defn column-exists?
  [tx table column]
  (let [sql (str
             "select exists ("
             "select 1 from pg_attribute where attrelid = '" table "'::regclass"
             " and attname = '" column "'"
             " and attnum > 0"
             " and not attisdropped"
             ");")]
    (:exists (execute-one! tx [sql]))))

(defn transform-database [ds projection configuration]
  (log/debug {:id ::database-transforming :data {:config configuration}}
             "Transforming database")
  (let [{ne :new/entities
         re :reactivated/entities
         nr :new/relations
         nrr :new/recursive-relations
         ce :changed/entities
         cr :changed/relations
         crr :changed/recursive-relations} (analyze-projection projection)]
    (log/trace {:id ::projection-analyzed
                :data {:new {:entities (map :name ne)
                             :relations (map (juxt :from-label :to-label) nr)
                             :recursive (map (juxt :from-label :to-label) nrr)}
                       :reactivated {:entities (map :name re)}
                       :changed {:entities (map :name ce)
                                 :relations (map (juxt :from-label :to-label) cr)
                                 :recursive (map (juxt :from-label :to-label) crr)}}}
               "Transform projection analysis")
    (jdbc/with-transaction [tx ds]
      (when (not-empty re)
        (log/info {:id ::entities-reactivating :data {:count (count re)}}
                  "Reactivating entities (tables already exist)")
        (doseq [{:keys [name]} re]
          (log/debug {:id ::entity-reactivated :data {:entity name}}
                     "Reactivated entity")))
      (let [ne (core/creation-order ne)]
        (when (not-empty ne)
          (log/info {:id ::entities-generating
                     :data {:entities (mapv :name ne)}}
                    "Generating new entities"))
        (doseq [{n :name
                 :as entity} ne
                :let [table-sql (generate-entity-ddl entity)
                      table (entity->table-name entity)]]
          (try
            (log/debug {:id ::entity-added
                        :data {:entity n :sql table-sql}}
                       "Adding entity to DB")
            (try
              (execute-one! tx [table-sql])
              (execute! tx [(xid/postgres-xid-trigger table)])
              (log/debug {:id ::id-immutability-trigger-created
                          :data {:table table :id-key (id/key)}}
                         "Created ID immutability trigger")
              (catch Throwable e
                (throw (errors/deploy-ddl-error
                        (format "Failed to create table for entity '%s'" n)
                        {:type ::entity-table-creation-error
                         :phase :ddl-execution
                         :operation :create-table
                         :entity-name n
                         :entity-id (id/extract entity)
                         :table-name table
                         :sql table-sql}
                        entity nil e))))
            (catch clojure.lang.ExceptionInfo e
              (throw e))
            (catch Throwable e
              (throw (ex-info
                      (format "Unexpected error while creating entity '%s'" n)
                      {:type ::entity-creation-error
                       :phase :ddl-execution
                       :entity-name n
                       :entity-id (:id/extract entity)
                       :table-name table}
                      e))))))

      (when (not-empty ne)
        (enhance/transform-audit *db* tx ne))

      (when (not-empty ce)
        (log/info {:id ::changed-entities-checking :data {:count (count ce)}}
                  "Checking changed entities"))
      (doseq [{n :name
               :as entity} ce
              :let [sql (entity-delta->ddl entity)]]
        (log/debug {:id ::entity-changing :data {:entity n}}
                   "Changing entity")
        (doseq [statement sql]
          (log/debug {:id ::entity-statement-executing
                      :data {:entity n :sql statement}}
                     "Executing statement")
          (try
            (execute-one! tx [statement])
            (catch Throwable e
              (throw (errors/deploy-ddl-error
                      (format "Failed to execute DDL statement for entity '%s'" n)
                      {:type ::entity-change-error
                       :phase :ddl-execution
                       :operation :alter-entity
                       :entity-name n
                       :entity-id (id/extract entity)
                       :table-name (entity->table-name entity)
                       :sql statement}
                      entity statement e))))))
      (when (not-empty cr)
        (log/info {:id ::changed-relations-checking :data {:count (count cr)}}
                  "Checking changed trans entity relations"))
      (doseq [r cr] (transform-relation tx r))
      (when (not-empty crr)
        (log/info {:id ::changed-recursive-relations-checking :data {:count (count crr)}}
                  "Checking changed recursive relations"))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label
               diff :diff
               :as r} crr
              :let [id (id/extract r)
                    table (entity->table-name e)
                    previous-column (when-some [label (not-empty (:to-label diff))]
                                      (column-name label))]]
        (when-not (and (some? tl) (not-empty tl))
          (throw
           (ex-info
            (str "Can't change recursive relation for entity " tname " that has empty label")
            {:entity e
             :relation {(id/key) id
                        :label (:to-label diff)}
             :type ::core/error-recursive-no-label})))
        (if (empty? previous-column)
          (do
            (log/debug {:id ::recursive-relation-no-prior-label
                        :data {:relation-id id :entity tname}}
                       "Previous deploy didn't have to-label for recursive relation")
            (when tl
              (when-not (column-exists? tx table tl)
                (let [sql (format
                           "alter table %s add %s bigint references \"%s\"(_eid) on delete cascade"
                           table tl table)]
                  (log/debug {:id ::recursive-relation-creating
                              :data {:entity tname :sql sql}}
                             "Creating recursive relation for entity")
                  (try
                    (execute-one! tx [sql])
                    (catch Throwable ex
                      (throw (errors/relation-ddl-error r
                              (format "Failed to create recursive relation column for entity '%s'" tname)
                              {:type ::recursive-relation-creation-error
                               :phase :ddl-execution
                               :operation :add-recursive-column
                               :entity-name tname
                               :entity-id (id/extract e)
                               :relation-id id
                               :table-name table
                               :column-name tl
                               :sql sql}
                              ex))))))))
          (when diff
            (let [sql (format
                       "alter table %s rename column %s to %s"
                       table previous-column (column-name tl))]
              (log/debug {:id ::recursive-relation-updating
                          :data {:entity tname :sql sql}}
                         "Updating recursive relation for entity")
              (try
                (execute-one! tx [sql])
                (catch Throwable ex
                  (throw (errors/relation-ddl-error r
                          (format "Failed to rename recursive relation column for entity '%s'" tname)
                          {:type ::recursive-relation-rename-error
                           :phase :ddl-execution
                           :operation :rename-recursive-column
                           :entity-name tname
                           :entity-id (id/extract e)
                           :relation-id id
                           :table-name table
                           :old-column-name previous-column
                           :new-column-name (column-name tl)
                           :sql sql}
                          ex))))))))
      (when (not-empty nr)
        (log/info {:id ::new-relations-generating :data {:count (count nr)}}
                  "Generating new relations"))
      (doseq [{{tname :name
                :as to-entity} :to
               {fname :name
                :as from-entity} :from
               :as relation} nr
              :let [sql (generate-relation-ddl projection relation)
                    [from-idx to-idx] (generate-relation-indexes-ddl projection relation)]]
        (try
          (log/debug {:id ::relation-connecting
                      :data {:from fname :to tname :sql sql}}
                     "Connecting entities")
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (errors/relation-ddl-error relation
                      (format "Failed to create relation table between '%s' and '%s'" fname tname)
                      {:type ::relation-creation-error
                       :phase :ddl-execution
                       :operation :create-relation-table
                       :from-entity fname
                       :from-entity-id (id/extract from-entity)
                       :to-entity tname
                       :to-entity-id (id/extract to-entity)
                       :relation-id (id/extract relation)
                       :table-name (relation->table-name relation)
                       :sql sql}
                      e))))
          (when from-idx
            (log/debug {:id ::relation-from-index-creating
                        :data {:from fname :to tname :sql from-idx}}
                       "Creating from-index for relation")
            (try
              (execute-one! tx [from-idx])
              (catch Throwable e
                (throw (errors/relation-ddl-error relation
                        (format "Failed to create 'from' index for relation between '%s' and '%s'" fname tname)
                        {:type ::relation-index-creation-error
                         :phase :ddl-execution
                         :operation :create-from-index
                         :from-entity fname
                         :from-entity-id (id/extract from-entity)
                         :to-entity tname
                         :to-entity-id (id/extract to-entity)
                         :relation-id (id/extract relation)
                         :table-name (relation->table-name relation)
                         :sql from-idx}
                        e)))))
          (when to-idx
            (log/debug {:id ::relation-to-index-creating
                        :data {:from fname :to tname :sql to-idx}}
                       "Creating to-index for relation")
            (try
              (execute-one! tx [to-idx])
              (catch Throwable e
                (throw (errors/relation-ddl-error relation
                        (format "Failed to create 'to' index for relation between '%s' and '%s'" fname tname)
                        {:type ::relation-index-creation-error
                         :phase :ddl-execution
                         :operation :create-to-index
                         :from-entity fname
                         :from-entity-id (id/extract from-entity)
                         :to-entity tname
                         :to-entity-id (id/extract to-entity)
                         :relation-id (id/extract relation)
                         :table-name (relation->table-name relation)
                         :sql to-idx}
                        e)))))
          (catch clojure.lang.ExceptionInfo e
            (throw e))
          (catch Throwable e
            (throw (ex-info
                    (format "Unexpected error while creating relation between '%s' and '%s'" fname tname)
                    {:type ::relation-creation-error
                     :phase :ddl-execution
                     :from-entity fname
                     :from-entity-id (id/extract from-entity)
                     :to-entity tname
                     :to-entity-id (id/extract to-entity)
                     :relation-id (id/extract relation)}
                    e)))))
      (when (not-empty nrr)
        (log/info {:id ::new-recursive-relations-adding :data {:count (count nrr)}}
                  "Adding new recursive relations"))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label
               :as r} nrr
              :when (not-empty tl)
              :let [rel-id (id/extract r)
                    table (entity->table-name e)
                    sql (format
                         "alter table %s add %s bigint references \"%s\"(_eid) on delete cascade"
                         table (column-name tl) table)]]
        (log/debug {:id ::new-recursive-relation-creating
                    :data {:entity tname :sql sql}}
                   "Creating new recursive relation for entity")
        (try
          (execute-one! tx [sql])
          (catch Throwable ex
            (throw (errors/relation-ddl-error r
                    (format "Failed to add new recursive relation column for entity '%s'" tname)
                    {:type ::recursive-relation-creation-error
                     :phase :ddl-execution
                     :operation :add-new-recursive-column
                     :entity-name tname
                     :entity-id (id/extract e)
                     :relation-id rel-id
                     :table-name table
                     :column-name (column-name tl)
                     :sql sql}
                    ex))))))))

(defn get-dataset-versions
  "All versions of a dataset by id or :name, newest deployed_on first."
  [{:keys [name]
    :as dataset-version}]
  (let [id (id/extract dataset-version)]
    (:versions
     (dataset/get-entity
      :dataset/dataset
      (case
       id {(id/key) xid}
       name {:name name})
      {:name nil
       (id/key) nil
       :versions
       [{:selections
         {(id/key) nil
          :name nil
          :model nil
          :deployed nil
          :deployed_on nil}
         :args {:_order_by {:deployed_on :desc}}}]}))))

(defn decode-version-model
  [version]
  (update version :model (fn [m] (if (string? m) (<-transit m) m))))

(defn encode-version-model
  [version]
  (update version :model (fn [m] (if (string? m) m (->transit m)))))

(defn deployed-versions
  []
  ;; never order by modified_on — meta-model entities don't carry it
  (mapv decode-version-model
        (dataset/search-entity
         :dataset/version
         {:deployed {:_eq true}
          :_order_by {:deployed_on :asc}}
         {(id/key) nil
          :model nil
          :name nil
          :dataset [{:selections {(id/key) nil
                                  :name nil}}]})))

#_(defn get-version
    [id]
    (dataset/get-entity
     :dataset/version
     {(id/key) id}
     {(id/key) nil
      :model nil
      :modified_on nil
      :dataset [{:selections {(id/key) nil}}]}))

(defn last-deployed-version-per-dataset
  []
  (let [datasets (dataset/search-entity
                  :dataset/dataset
                  nil
                  {(id/key) nil
                   :versions [{:selections
                               {(id/key) nil
                                :name nil
                                :dataset [{:selections {(id/key) nil}}]}
                               :args {:deployed {:_eq true}
                                      :_order_by {:deployed_on :desc}
                                      :_limit 1}}]})]
    (mapcat :versions datasets)))

(comment
  (dataset/deployed-model)

  (time
   (dataset/search-entity
    :dataset/version
    nil
    {:name nil
     (id/key) nil
     :dataset [{:selections {:name nil}}]
     :entities [{:selections {:name nil}}]
     :modified_by [{:selections {:name nil}}]}))
  (time
   (dataset/search-entity
    :iam/user
    nil
    {(id/key) nil
     :name nil
     :groups [{:selections {:name nil}}]
     :roles [{:selections {:name nil}}]})))

(defn read-versions
  "`(read)`, or `empty` before the meta-model is deployed or on a legacy meta-model without deployed_on."
  [read empty]
  (if-not (dataset/meta-model-loaded?)
    empty
    (try
      (read)
      (catch clojure.lang.ExceptionInfo e
        (if (= "UNKNOWN_ATTRIBUTE" (:code (ex-data e)))
          empty
          (throw e))))))

(defn deployed-version-per-dataset-ids
  ([] (deployed-version-per-dataset-ids nil))
  ([superseded-dataset]
   (read-versions
    #(set (map id/extract (cond->> (last-deployed-version-per-dataset)
                            superseded-dataset (remove (fn [v] (= superseded-dataset (some-> v :dataset id/extract)))))))
    #{})))

(defn version-rls-model
  "The model of `version`, without RLS on system entities unless its dataset is a system dataset."
  [{:keys [model dataset]}]
  (let [system-ids (dataset/system-ids (id/key))]
    (cond-> model
      (not (contains? (:datasets system-ids) (id/extract dataset)))
      (system/without-system-rls system-ids))))

(defn latest-version-models
  "Models of the latest deployed version of every dataset, plus `include-version`
   when given, each as `version-rls-model`."
  ([] (latest-version-models nil))
  ([include-version]
   (let [prior (read-versions
                #(let [latest (deployed-version-per-dataset-ids)
                       superseded (some-> include-version :dataset id/extract)]
                   (->> (deployed-versions)
                        (filter (fn [v] (contains? latest (id/extract v))))
                        (remove (fn [v] (= superseded (some-> v :dataset id/extract))))
                        (mapv (fn [v] (version-rls-model (update v :model dataset/adapt-model-to-provider))))))
                [])]
     (cond-> prior
       include-version (conj (version-rls-model include-version))))))

(defn gen-activation-filter
  "Filter for activate-model; `include-version` replaces its dataset's latest deployed version."
  ([] (gen-activation-filter nil))
  ([include-version]
   (let [deployed-versions (cond-> (deployed-version-per-dataset-ids (some-> include-version :dataset id/extract))
                             include-version (conj (id/extract include-version)))]
     (if (empty? deployed-versions)
       (constantly true)
       (fn [claims]
         (boolean
          (not-empty
           (clojure.set/intersection
            claims
            deployed-versions))))))))

(defn rebuild-global-model
  "Rebuilds the global model, with :active flags, from ALL deployed versions; does not save."
  ([] (rebuild-global-model (deployed-versions)))
  ([all-versions]
   (comment
     (def all-versions (deployed-versions))
     (map (juxt id/extract :name (comp :name :dataset)) all-versions)
     (def version
       (some
        #(when (= (:euuid %) #uuid "d908a70f-a1fb-46bd-ac76-801bebe6ceed")
           %)
        all-versions))
     (def model (:model version))
     (def x (rebuild-global-model)))
   (let [initial-model (core/map->ERDModel {:entities {}
                                            :relations {}})
         final-model
         (reduce
          (fn [global version]
            (core/join-models
             global
             (core/add-claims
              (version-rls-model (update version :model dataset/adapt-model-to-provider))
              (id/extract version))))
          initial-model
          all-versions)]
     (-> final-model
         (core/reconcile-rls-guards (latest-version-models))
         (core/activate-model (gen-activation-filter))))))

(defn check-entity-name-conflicts!
  "Throws exception if new-model contains entities with conflicting names"
  [global new-model]
  (let [entities-by-table (reduce
                           (fn [r entity]
                             (assoc r (entity->table-name entity) entity))
                           {}
                           (core/get-entities global))]
    (doseq [new-entity (core/get-entities new-model)
            :let [table (entity->table-name new-entity)
                  found-entity (get entities-by-table table)]]
      (when (and
             (some? found-entity)
             (not= ((id/key) found-entity) ((id/key) new-entity)))
        (throw
         (ex-info
          (format "Entity name conflict: '%s' already exists with different ID" (:name new-entity))
          {:type ::entity-name-conflict
           :new-entity new-entity
           :existing-entity (get entities-by-table table)
           :table-name (entity->table-name new-entity)}))))))

(defn create-deploy-history!
  []
  (execute-one!
   [(format
     "create table if not exists __deploy_history (
           \"deployed_on\" timestamp not null default localtimestamp,
           \"model\" text)")]))

(defn add-to-deploy-history!
  [model]
  (execute-one!
   ["insert into __deploy_history (model) values (?)"
    (->transit model)]))

(extend-protocol core/DatasetProtocol
  synthigy.db.Postgres
  (core/preview-model [_ version]
    (let [version (decode-version-model version)
          model (dataset/adapt-model-to-provider (:model version))
          version (assoc version :model model)
          global (or (dataset/deployed-model)
                     (core/map->ERDModel {:entities {} :relations {}}))]
      (core/fold-version global (version-rls-model version) (id/extract version)
                         (latest-version-models version)
                         (gen-activation-filter version))))
  (core/preview-recall [_ version-ids]
    (let [recalled (set version-ids)]
      (rebuild-global-model (remove #(contains? recalled (id/extract %)) (deployed-versions)))))
  (core/deploy! [this version]
    (let [version (decode-version-model version)
          {:keys [model]} version]
      (try
        (let [model (dataset/adapt-model-to-provider model)
              version (assoc version :model model)
              global (or (dataset/deployed-model)
                         (core/map->ERDModel {:entities {}
                                              :relations {}}))

              _ (check-entity-name-conflicts! global model)

            ;; refuse before any DDL if the result would drop a guard — the op fail-closes silently
              _ (rls/assert-guards-compile! (core/preview-model this version) model)

              _ (core/mount this (update version :model #(core/with-merged-config global %)))

              version'' (encode-version-model
                         (assoc version
                                :model (assoc model :version 1)
                                :deployed_on (java.util.Date.)
                                :deployed true))]
          (sync-entity this :dataset/version version'')
          (core/reload this version'')

          (let [updated-model (assoc (rebuild-global-model) :version 1)]
            (dataset/save-model! updated-model)
            (add-to-deploy-history! updated-model)
            (let [all-entities (vec (core/get-entities updated-model))]
              (enhance/transform-audit *db* (:datasource *db*) all-entities))
            updated-model))
        (catch Throwable e
          (log/error! {:id ::deploy-failed
                       :data {:action :deploying :subject :dataset
                              :version (:name version)
                              :dataset (:name (:dataset version))}}
                      e)
          (throw e)))))
  (core/recall!
    [this record]
    (assert (id/extract record) "Version id is required")

    ;; never skip decode-version-model — :model comes back as a transit string
    (let [id (id/extract record)
          version (decode-version-model
                   (dataset/get-entity
                    :dataset/version
                    {(id/key) id}
                    {(id/key) nil
                     :name nil
                     :model nil
                     :deployed nil
                     :dataset [{:selections {(id/key) nil
                                             :name nil}}]}))

          _ (when-not version
              (throw (ex-info (format "Version %s not found" id)
                              {:type :version-not-found
                               (id/key) id})))
          _ (when-not (:deployed version)
              (throw (ex-info (format "Version %s was never deployed, cannot recall" id)
                              {:type :version-not-deployed
                               (id/key) id
                               :version version})))

          dataset (:dataset version)
          dataset-name (:name dataset)
          dataset-id (id/extract dataset)

          all-deployed-versions (filter :deployed (get-dataset-versions dataset))

          only-version? (= 1 (count all-deployed-versions))

          most-recent-version (first all-deployed-versions)
          is-most-recent? (= id (id/extract most-recent-version))

          global (dataset/deployed-model)
          version-uuids #{id}

          exclusive-entities (core/find-exclusive-entities global version-uuids)
          exclusive-relations (core/find-exclusive-relations global version-uuids)]

      #_(do
          (def global global)
          (def exclusive-entities exclusive-entities)
          (def exclusive-relations exclusive-relations)
          (def dataset dataset)
          (def all-deployed-versions all-deployed-versions)
          (def only-version? only-version?)
          (def version version)
          (def id id)
          (def version-uuids #{id})
          (def most-recent-version (first all-deployed-versions))
          (def is-most-recent? (= id (id/extract most-recent-version)))
          (def updated-model (rebuild-global-model (remove #(= (id/extract%) id) (deployed-versions)))))

      (log/info {:id ::version-recalling
                 :data {:dataset dataset-name
                        :version (:name version)
                        :only? only-version?
                        :most-recent? is-most-recent?}}
                "Recalling version")

      (when (or (not-empty exclusive-entities) (not-empty exclusive-relations))
        (log/info {:id ::exclusive-pruning
                   :data {:entity-count (count exclusive-entities)
                          :relation-count (count exclusive-relations)}}
                  "Pruning exclusive entities and relations")
        (with-open [con (jdbc/get-connection (:datasource *db*))]
          (doseq [relation exclusive-relations
                  :let [{:keys [from to]
                         :as relation} (core/get-relation global (id/extract relation))
                        table-name (relation->table-name relation)
                        sql (format "drop table if exists \"%s\"" table-name)]]
            (execute-one! con [sql])
            (log/trace {:id ::relation-table-removed
                        :data {:from (:name from) :to (:name to) :table table-name}}
                       "Removed relation table")
            (dataset/delete-entity :dataset/relation relation))
          (doseq [entity exclusive-entities
                  :let [entity (core/get-entity global (id/extract entity))]
                  :when (some? entity)]
            (let [table-name (entity->table-name entity)
                  sql (format "drop table if exists \"%s\"" table-name)]
              (log/trace {:id ::entity-removing
                          :data {:entity (:name entity) :table table-name}}
                         "Removing entity")
              (execute-one! con [sql])
              (dataset/delete-entity :dataset/relation entity)))))

      (let [updated-model (rebuild-global-model (remove #(= (id/extract %) id) (deployed-versions)))
            recalled-model (:model version)

            projection (core/project updated-model recalled-model)

            exclusive-entity-ids (set (map id/extract exclusive-entities))

            orphaned-attrs (for [entity (core/get-entities projection)
                                 :when (not (contains? exclusive-entity-ids (id/extract entity)))
                                 attr (:attributes entity)
                                 :when (core/new-attribute? attr)]
                             {:entity entity
                              :attribute attr})]

        (when (not-empty orphaned-attrs)
          (log/info {:id ::orphaned-columns-dropping
                     :data {:count (count orphaned-attrs)}}
                    "Dropping orphaned attribute columns from recalled version")
          (with-open [con (jdbc/get-connection (:datasource *db*))]
            (doseq [orphan orphaned-attrs]
              (let [ddl-statements (orphaned-attribute->drop-ddl orphan)]
                (doseq [sql ddl-statements]
                  (execute-one! con [sql])
                  (log/debug {:id ::orphaned-column-dropped :data {:sql sql}}
                             "Dropped orphaned column"))))))

        (cond
          only-version?
          (do
            (log/info {:id ::version-recalled-rebuilding}
                      "Recalled most recent version, rebuilding from remaining versions")
            (core/mount this {:model updated-model})
            (log/info {:id ::version-record-deleting
                       :data {:dataset dataset-name :version (:name version)}}
                      "Deleting version record")
            (delete-entity this :dataset/version {(id/key) id})
            (log/info {:id ::dataset-removing :data {:dataset dataset-name}}
                      "Last version deleted, removing dataset")
            (dataset/delete-entity :dataset/dataset {(id/key) dataset-id}))
          is-most-recent?
          (do
            (log/info {:id ::version-recalled-rebuilding}
                      "Recalled most recent version, rebuilding from remaining versions")
            (core/mount this {:model updated-model})
            (log/info {:id ::version-record-deleting
                       :data {:dataset dataset-name :version (:name version)}}
                      "Deleting version record")
            (delete-entity this :dataset/version {(id/key) id}))
          :else
          (log/info {:id ::older-version-recalled
                     :data {:dataset dataset-name :version (:name version)}}
                    "Recalled older version, rebuilding model"))
        (dataset/delete-entity :dataset/version {(id/key) id})
        (let [final-model (rebuild-global-model)
              final-schema (query/model->schema final-model)]
          (dataset/save-model! final-model)
          (query/deploy-schema final-schema)
          (add-to-deploy-history! final-model)
          (let [all-entities (vec (core/get-entities final-model))]
            (enhance/transform-audit *db* (:datasource *db*) all-entities))))))
  (core/destroy! [this {:keys [name]
                        :as record}]
    (assert (or name (id/extract record)) "Specify dataset name or id!")
    (comment
      (def id #uuid "ffffffff-0000-0000-0000-000000000000")
      (dataset/search-entity
       :dataset/dataset
       nil
       {:euuid nil
        :name nil}))
    (let [id (id/extract record)
          {all-versions :versions} (dataset/get-entity
                                    :dataset/dataset
                                    (if id
                                      {(id/key) id}
                                      {:name name})
                                    {:name nil
                                     (id/key) nil
                                     :versions [{:args {:_order_by [{:deployed_on :asc}]
                                                        :_where {:deployed {:_eq true}}}
                                                 :selections {:name nil
                                                              (id/key) nil
                                                              :model nil}}]})]
      (log/info {:id ::dataset-destroying
                 :data {:dataset (or name id) :version-count (count all-versions)}}
                "Destroying dataset: recalling versions in reverse order")
      (doseq [version all-versions]
        (core/recall! this {(id/key) (id/extract version)}))))
  (core/get-model
    [_]
    (dataset/deployed-model))
  (core/reload
    ([this]

     (when-not (dataset/deployed-model)
       (let [{:keys [model]
              :as dataset-version} (dataset/bootstrap-version)]
         (core/reload this dataset-version)
         (query/deploy-schema (query/model->schema model))
         model))
     (let [model (rebuild-global-model)
           model' (-> (or model (core/map->ERDModel {:entities {}
                                                     :relations {}}))
                      (assoc :version 1))
           model'' (core/activate-model model' (gen-activation-filter))
           schema (query/model->schema model'')]
       (dataset/save-model! model'')
       (query/deploy-schema schema)
       model''))
    ([this record]
     (let [{:keys [model] :as record} (decode-version-model record)
           model (dataset/adapt-model-to-provider model)
           record (assoc record :model model)
           version-id (id/extract record)
           global (or
                   (core/get-model this)
                   (core/map->ERDModel nil))
           model'' (core/fold-version global model version-id
                                      (latest-version-models record)
                                      (gen-activation-filter record))
           schema (query/model->schema model'')]
       (query/deploy-schema schema)
       (dataset/save-model! model'')
       model'')))
  (core/mount
    [this version]
    (let [{model :model :as version} (decode-version-model version)
          model (dataset/adapt-model-to-provider model)
          version (assoc version :model model)]
      (log/debug {:id ::version-mounting
                  :data {:version (:name version)
                         :dataset (get-in version [:dataset :name])}}
                 "Mounting dataset version")
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (let [global (or
                      (core/get-model this)
                      (core/map->ERDModel nil))
              projection (core/project global model)]
          (transform-database con projection nil)
          version))))
  (core/unmount
    [this version]
    (let [{:keys [model]} (decode-version-model version)
          global (core/get-model this)]
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (doseq [relation (core/get-relations model)
                :let [{:keys [from to]
                       :as relation} (core/get-relation global (id/extract relation))
                      sql (format "drop table if exists \"%s\"" (relation->table-name relation))]]
          (try
            (execute-one! con [sql])
            (log/trace {:id ::relation-removing
                        :data {:from (:name from) :to (:name to) :sql sql}}
                       "Removing relation")
            (delete-entity this :dataset/relation {(id/key) (id/extract relation)})
            (catch Throwable e
              (log/error! {:id ::relation-table-remove-failed
                           :data {:table (relation->table-name relation)}}
                          e))))
        (doseq [entity (core/get-entities model)
                :let [{:keys [attributes]
                       :as entity} (core/get-entity global (id/extract entity))]
                :when (some? entity)]
          (try
            (let [sql (format "drop table if exists \"%s\"" (entity->table-name entity))]
              (log/trace {:id ::unmount-entity-removing
                          :data {:entity (:name entity) :sql sql}}
                         "Removing entity")
              (execute-one! con [sql])
              (delete-entity this :dataset/entity {(id/key) (id/extract entity)})
              (doseq [attribute attributes]
                (delete-entity this :dataset/entity-attribute (id/extract attribute))))
            (catch Throwable e
              (log/error! {:id ::entity-table-remove-failed
                           :data {:table (entity->table-name entity)}}
                          e))))))
    (core/reload this))
  (core/get-last-deployed
    ([this]
     ;; manual recovery only — never part of bootstrap (reload handles that)
     (log/warn {:id ::manual-recovery-invoked}
               "get-last-deployed called - this is for manual recovery only")
     (core/get-last-deployed this 0))
    ([_ offset]
     (when-let [m (execute-one!
                   [(cond->
                     "select model from __deploy_history order by deployed_on desc"
                      offset (str " offset " offset))])]
       (let [model (-> m :model <-transit)
             clean-model (reduce
                          (fn [m entity]
                            (if (empty? (:attributes entity))
                              (core/remove-entity m entity)
                              m))
                          model
                          (core/get-entities model))
             clean-schema (query/model->schema clean-model)]
         (with-meta clean-model {:dataset/schema clean-schema}))))))

(lifecycle/register-module!
 :synthigy/dataset
 {:depends-on [:synthigy/log :synthigy/database :synthigy.dataset/encryption :synthigy/log.config]
  :headline true
  :doc "ERD model — schema deploy, /data query engine"
  :setup (fn []
           ;; never deploy before this — every string/enum column is collated human
           (collation/ensure-human-collation!)
            ;; legacy check BEFORE detect-and-set-provider! — it stores the format
           (let [legacy? (xid/legacy-deployment?)]

             (xid/detect-and-set-provider!)
             (dataset/register-reference-types!)

             (if legacy?
               (do
                 (log/info {:id ::legacy-setup-starting}
                           "Legacy Postgres deployment detected, loading existing schema")
                 ;; must run BEFORE any version-detection reads the meta columns
                 (xid/reconcile-legacy-meta-columns!)
                 (create-deploy-history!)
                 (core/reload *db*)
                 (log/info {:id ::legacy-setup-complete}
                           "Legacy setup complete"))

               (do
                 (log/info {:id ::fresh-setup-starting}
                           "Fresh Postgres, deploying Synthigy model")
                 (dataset/save-model! (core/map->ERDModel nil))
                 (log/info {:id ::fresh-setup-database
                            :data {:database (dissoc *db* :password :datasource)}}
                           "Using database")

                 (create-deploy-history!)
                 (log/info {:id ::deploy-history-created :data {:action :created :subject :deploy-history}}
                           "Created __deploy_history")

                 (log/info {:id ::synthigy-model-loading :data {:action :loading :subject :dataset-model}}
                           "Loading Synthigy model from dataset/synthigy.json")
                 (let [version (dataset/current-synthigy-version)]
                   (core/mount *db* version)
                   (core/reload *db* version)
                   (log/info {:id ::synthigy-model-mounted :data {:action :mounted :subject :dataset-model}}
                             "Mounted Synthigy model")
                   (dataset/deploy! version))

                 (log/info {:id ::model-reloading :data {:action :reloading :subject :model}}
                           "Reloading model")
                 (core/reload *db*)

                 (log/info {:id ::deployed-model-recording :data {:action :recording :subject :deployed-model}}
                           "Adding deployed model to history")
                 (add-to-deploy-history! (core/get-model *db*))

                 (log/info {:id ::setup-complete :data {:action :setup-complete :subject :dataset}}
                           "Setup complete")))))

  :cleanup (fn []
             (log/info {:id ::cleanup-starting}
                       "Cleaning up dataset tables")
              ;; TODO: Drop dataset-specific tables if needed
             (log/info {:id ::cleanup-complete}
                       "Dataset cleanup complete"))

  :start (fn []
           (log/info {:id ::lifecycle-starting :data {:action :starting}}
                     "Starting dataset system")
           (xid/detect-and-set-provider!)
           ;; never auto-migrate on boot — legacy import is operator-run
           (when (xid/legacy-euuid-layout?)
             (log/warn {:id ::legacy-euuid-detected
                        :data {:action :not-initialized :subject :id-format :format "euuid"}}
                       (str "LEGACY (euuid) DATABASE DETECTED. Synthigy is xid-native — "
                            "run (synthigy.dataset.postgres.xid/migrate->synthigy!) to convert "
                            "this database to the Synthigy xid layout. Until then the system "
                            "runs against the legacy euuid layout.")))
           (dataset/start)
           (collation/check-version!)
           (log/info {:id ::lifecycle-started :data {:action :started}}
                     "Dataset system started"))

  :stop (fn []
          (log/info {:id ::lifecycle-stopping :data {:action :stopping}}
                    "Stopping dataset system")
          (dataset/stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}}
                    "Dataset system stopped"))})

(extend-protocol SQLNameResolution
  synthigy.db.Postgres
  (table [_ value]
    (let [schema (query/deployed-schema)]
      (get-in schema [value :table])))
  (relation [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :table])
        (some
         (fn [[_ {:keys [relation table]}]]
           (when (= relation value)
             table))
         relations))))
  (related-table [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :to/table])
        (some
         (fn [[_ {:keys [relation table]}]]
           (when (= relation value)
             table))
         relations))))
  (relation-from-field [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :from/field])
        (some
         (fn [[_ {:keys [relation :from/field]}]]
           (when (= relation value)
             field))
         relations))))
  (relation-to-field [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :to/field])
        (some
         (fn [[_ {:keys [relation :to/field]}]]
           (when (= relation value)
             field))
         relations)))))

(defn get-pg-tables
  []
  (let [system-tables #{"__deks" "__version_history"}]
    (set (remove system-tables
                 (map :table_name
                      (execute! ["SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                AND table_name NOT LIKE '__deploy%'"]))))))

(defn get-pg-columns
  [table-name]
  (set (map (comp keyword :column_name)
            (execute! ["SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?"
                       table-name]))))

(defn get-pg-enum-types
  []
  (set (map :typname
            (execute! ["SELECT typname FROM pg_type
            WHERE typtype = 'e'"]))))

(defn healthcheck-database
  "Compares the deployed schema with the live database: :healthy?, :expected/*, :actual/*, :missing/*, :extra/*."
  []
  (let [schema (query/deployed-schema)

        pg-tables (get-pg-tables)

        expected-entity-tables (set (keep (fn [[_ entity-data]]
                                            (:table entity-data))
                                          schema))

        expected-relation-tables (set (mapcat
                                       (fn [[_ entity-data]]
                                         (map :table (vals (:relations entity-data))))
                                       schema))

        expected-tables (clojure.set/union expected-entity-tables expected-relation-tables)

        missing-tables (clojure.set/difference expected-tables pg-tables)

        extra-tables (clojure.set/difference pg-tables expected-tables)

        missing-columns
        (for [[entity-uuid entity-data] schema
              :let [table-name (:table entity-data)
                    expected-cols (set (concat
                                        [:_eid (id/key) :modified_by :modified_on]
                                        (map :key (vals (:fields entity-data)))))
                    actual-cols (when (pg-tables table-name)
                                  (get-pg-columns table-name))
                    missing (when actual-cols
                              (clojure.set/difference expected-cols actual-cols))]
              :when (and missing (not-empty missing))]
          {:entity (:name entity-data)
           :entity-id entity-uuid
           :table table-name
           :missing-columns missing})

        missing-relations (filter #(not (pg-tables %)) expected-relation-tables)]

    {:healthy? (and (empty? missing-tables)
                    (empty? missing-columns)
                    (empty? missing-relations))

     :expected/tables (count expected-tables)
     :expected/entities (count expected-entity-tables)
     :expected/relations (count expected-relation-tables)

     :actual/tables (count pg-tables)

     :missing/tables {:count (count missing-tables)
                      :items missing-tables}
     :missing/columns {:count (count missing-columns)
                       :items missing-columns}
     :missing/relations {:count (count missing-relations)
                         :items missing-relations}

     :extra/tables {:count (count extra-tables)
                    :items extra-tables}}))

(defn healthcheck-report
  "Healthcheck as {:healthy? :message :data}, with a human-readable message."
  ([] (healthcheck-report (healthcheck-database)))
  ([health]
   (let [lines (atom [])
         println! (fn [& args]
                    (swap! lines conj (apply str args)))

         missing-count (+ (get-in health [:missing/tables :count])
                          (get-in health [:missing/columns :count])
                          (get-in health [:missing/relations :count]))]

     (println! "=== Database Health Check ===")
     (println! "")
     (if (:healthy? health)
       (println! "✅ Database is HEALTHY - schema matches deployed model")
       (println! "❌ Database has ISSUES - schema does not match deployed model"))

     (println! "")
     (println! "Expected (from schema):")
     (println! (format "  Tables:    %d (%d entities + %d relations)"
                       (:expected/tables health)
                       (:expected/entities health)
                       (:expected/relations health)))

     (println! "")
     (println! "Actual (in database):")
     (println! (format "  Tables:    %d" (:actual/tables health)))

     (when (pos? missing-count)
       (println! "")
       (println! (format "⚠️  Missing %d item(s):" missing-count)))

     (when (pos? (get-in health [:missing/tables :count]))
       (println! "")
       (println! (format "  Tables (%d):" (get-in health [:missing/tables :count])))
       (doseq [table (get-in health [:missing/tables :items])]
         (println! (format "    - %s" table))))

     (when (pos? (get-in health [:missing/relations :count]))
       (println! "")
       (println! (format "  Relations (%d):" (get-in health [:missing/relations :count])))
       (doseq [table (get-in health [:missing/relations :items])]
         (println! (format "    - %s" table))))

     (when (pos? (get-in health [:missing/columns :count]))
       (println! "")
       (println! (format "  Columns (%d):" (get-in health [:missing/columns :count])))
       (doseq [{:keys [entity table missing-columns]} (get-in health [:missing/columns :items])]
         (println! (format "    Entity '%s' (table: %s)" entity table))
         (doseq [col missing-columns]
           (println! (format "      • %s" col)))))

     (when (pos? (get-in health [:extra/tables :count]))
       (println! "")
       (println! (format "ℹ️  Extra tables in DB (%d) - could be inactive/historical:"
                         (get-in health [:extra/tables :count])))
       (doseq [table (take 20 (get-in health [:extra/tables :items]))]
         (println! (format "    - %s" table)))
       (when (> (get-in health [:extra/tables :count]) 20)
         (println! (format "    ... and %d more" (- (get-in health [:extra/tables :count]) 20)))))

     (println! "")
     (println! "=========================")

     {:healthy? (:healthy? health)
      :message (clojure.string/join "\n" @lines)
      :data health})))

(defn healthcheck-print
  []
  (let [{:keys [message]} (healthcheck-report)]
    (println message)))

(comment
  ;; Get structured report (for APIs, programmatic use)
  (def report (healthcheck-report))
  (:healthy? report) ;; => true/false
  (println (:message report)) ;; => "=== Database Health Check ===\n..."
  (:data report) ;; => {:expected/tables 216 ...}

  ;; Print report to stdout
  (healthcheck-print)

  ;; Get raw health data directly
  (def health (healthcheck-database))
  (:healthy? health)
  (get-in health [:missing/relations :count])

  ;; Previous comment block
  (def deployed (deployed-versions))
  (def without-dataset
    (filter
     (fn [{dataset :dataset}]
       (nil? (id/extract dataset)))
     deployed))
  (doseq [version without-dataset]
    (println (dataset/delete-entity :dataset/version {(id/key) (id/extract version)})))
  (def versions
    (dataset/get-entity
     :dataset/dataset
     {(id/key) :dataset/dataset}
     {:versions [{:selections
                  {(id/key) nil
                   :name nil}}]})))

(extend-type Postgres
  proto/TypeCodec

  (encode [_db type value]
    (case type
      ("string" "int" "float" "boolean" "timeperiod" "currency" "uuid" "avatar")
      value

      "timestamp"
      (when (some? value) (ts/->utc-local value))

      "json"
      (when value
        (doto (PGobject.)
          (.setType "jsonb")
          (.setValue (->json value))))

      "encrypted"
      (throw (ex-info
              (str "\"encrypted\" cannot be encoded via TypeCodec — it seals "
                   "at the storage boundary (synthigy.dataset.encryption/"
                   "seal-cell), never here.")
              {:code :encrypted-not-a-codec-type}))

      "hashed"
      (when value
        (hashers/derive value))

      "transit"
      value

      (when value
        (name value))))

  (decode [_db type value]
    (case type
      ("string" "int" "float" "boolean" "timeperiod" "currency" "uuid" "avatar")
      value

      "timestamp"
      (when (some? value) (ts/->date value))

      ;; opaque user data — never coerce keys or values
      "json"
      (when value
        (cond
          (or (map? value) (vector? value)) value
          (instance? PGobject value) (<-json (.getValue ^PGobject value) {:keyfn identity :valfn nil})
          (string? value) (<-json value {:keyfn identity :valfn nil})
          :else value))

      "encrypted"
      (throw (ex-info
              (str "\"encrypted\" cannot be decoded via TypeCodec — it unseals "
                   "at the storage boundary (synthigy.dataset.encryption/"
                   "unseal-cell), never here.")
              {:code :encrypted-not-a-codec-type}))

      "hashed"
      value

      "transit"
      value

      (when value
        (cond
          (keyword? value) value
          (instance? PGobject value) (keyword (.getValue ^PGobject value))
          (string? value) (keyword value)
          :else value)))))

(extend-type Postgres
  proto/SQLDialect

  (like-operator [_db case-sensitive?]
    (if case-sensitive? "LIKE" "ILIKE"))

  (limit-offset-clause [_db limit offset]
    (cond
      (and limit offset) (str "LIMIT " limit " OFFSET " offset)
      limit (str "LIMIT " limit)
      offset (str "OFFSET " offset)
      :else nil))

  (placeholder-for-type [_db _field-type]
    "?")

  (excluded-ref [_db column]
    (format "EXCLUDED.%s" column))

  (max-bind-params [_db]
    65535)

  (row-lock-clause [_db]
    " for no key update"))

(extend-type Postgres
  proto/SchemaManager

  (get-tables [_db]
    (let [system-tables #{"__deks" "__version_history"}
          results (execute!
                   ["SELECT table_name FROM information_schema.tables
                     WHERE table_schema = 'public'
                     AND table_type = 'BASE TABLE'
                     AND table_name NOT LIKE '__deploy%'"]
                   :raw)]
      (vec (remove system-tables (map :table_name results)))))

  (get-columns [_db table]
    (let [results (execute!
                   ["SELECT column_name, data_type, is_nullable, column_default
                     FROM information_schema.columns
                     WHERE table_schema = 'public' AND table_name = ?
                     ORDER BY ordinal_position"
                    table]
                   :raw)]
      (mapv (fn [row]
              {:name (:column_name row)
               :type (:data_type row)
               :nullable (= "YES" (:is_nullable row))
               :default (:column_default row)})
            results)))

  (get-enums [_db]
    (let [enum-types (execute!
                      ["SELECT t.typname as enum_name,
                               e.enumlabel as enum_value
                        FROM pg_type t
                        JOIN pg_enum e ON t.oid = e.enumtypid
                        WHERE t.typtype = 'e'
                        ORDER BY t.typname, e.enumsortorder"]
                      :raw)]
      (when (seq enum-types)
        (vec
         (map (fn [[enum-name values]]
                {:name enum-name
                 :values (vec (map :enum_value values))})
              (group-by :enum_name enum-types))))))

  (table-exists? [db table]
    (contains? (set (proto/get-tables db)) table))

  (column-exists? [db table column]
    (let [columns (proto/get-columns db table)]
      (some #(= column (:name %)) columns)))

  (list-tables-like [_db pattern]
    (execute!
     ["SELECT tablename FROM pg_tables
       WHERE schemaname='public' AND tablename LIKE ?"
      pattern]
     :raw))

  (drop-table! [_db table]
    (try
      (execute-one! [(str "DROP TABLE IF EXISTS " table " CASCADE")])
      true
      (catch Throwable _ false)))

  (drop-tables-like! [db pattern]
    (let [tables (proto/list-tables-like db pattern)]
      (doseq [{:keys [tablename]} tables]
        (proto/drop-table! db tablename))
      (count tables)))

  (truncate-table! [_db table]
    (execute-one! [(str "TRUNCATE TABLE " table " CASCADE")]))

  (list-types-like [_db pattern]
    (execute!
     ["SELECT typname FROM pg_type WHERE typname LIKE ?"
      pattern]
     :raw))

  (drop-type! [_db type-name]
    (try
      (execute-one! [(str "DROP TYPE IF EXISTS " type-name " CASCADE")])
      true
      (catch Throwable _ false)))

  (drop-types-like! [db pattern]
    (let [types (proto/list-types-like db pattern)]
      (doseq [{:keys [typname]} types]
        (proto/drop-type! db typname))
      (count types))))
