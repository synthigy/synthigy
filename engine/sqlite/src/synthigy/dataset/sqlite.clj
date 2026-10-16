;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.sqlite
  (:require
   [buddy.hashers :as hashers]
   clojure.data
   clojure.set
   clojure.string
   [next.jdbc :as jdbc]
   next.jdbc.date-time
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.errors :as errors]
   [synthigy.dataset.sql.naming
    :as naming
    :refer [normalize-name
            column-name
            relation->table-name
            entity->relation-field
            entity->table-name
            SQLNameResolution]]
   [synthigy.dataset.sql.protocol :as proto]
   [synthigy.dataset.sql.query :as query]
   [synthigy.dataset.sql.rls :as rls]
   [synthigy.dataset.system :as system]
    ;; required: extends ModelQueryProtocol for SQLite (no direct refs)
   synthigy.dataset.sqlite.query
    ;; required for its load-time patch registrations (no direct refs)
   synthigy.dataset.sqlite.patch
   [synthigy.db
    :refer [*db*
            sync-entity
            delete-entity]]
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.db.sqlite :as sqlite]
   [synthigy.json :refer [<-json ->json]]
   [synthigy.log :as log]
   ;; loaded for its lifecycle registration — :synthigy/dataset depends on :synthigy/log.config
   [synthigy.log.config]
   ;; loaded for their registrations — patches dataset/start levels, the meta-model write hooks
   [synthigy.dataset.patch]
   [synthigy.dataset.meta]
   [synthigy.transit
    :refer [<-transit ->transit]]
   [synthigy.timestamp :as ts])
  (:import
   [synthigy.db SQLite]))

(declare add-to-deploy-history!)

(defn reference-table
  "Table name for a reference type, or nil (with a warning) when unregistered."
  [type-name]
  (if-let [table-fn (core/reference-table-fn type-name)]
    (table-fn)
    (do
      (log/warn {:id ::reference-type-unregistered :data {:type type-name}}
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
  "Converts type to DDL syntax (SQLite)"
  [t]
  (try
    (if (core/reference-type? t)
      (if-let [table (reference-table t)]
        (str "INTEGER references \"" table "\"(_eid) on delete set null")
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
        "currency" "TEXT"
        ("avatar" "string" "hashed") "TEXT"
        "timestamp" "TEXT"
        ("json" "encrypted" "timeperiod") "TEXT"
        "transit" "TEXT"
        "int" "INTEGER"
        "enum" "TEXT"
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
          pk ["_eid INTEGER PRIMARY KEY"
              (str (id/field) " TEXT NOT NULL UNIQUE")]
          ;; never inline unique groups — an inline constraint can't be dropped or changed on SQLite
          rows (concat pk as')]
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

(defn unique-index-name
  [table idx]
  (str table "_eucg_" idx))

(defn unique-index-ddl
  "CREATE UNIQUE INDEX for one unique group, or nil for an empty/dead group."
  [entity table idx ids]
  (when (not-empty ids)
    (format
     "create unique index if not exists \"%s\" on \"%s\"(%s)"
     (unique-index-name table idx)
     table
     (clojure.string/join
      ","
      (map #(-> (core/get-attribute entity %) :name column-name) ids)))))

(defn generate-entity-index-ddl
  "Unique-group indexes for a new entity table."
  [entity]
  (let [table (entity->table-name entity)]
    (vec
     (keep-indexed
      (fn [idx ids] (unique-index-ddl entity table idx ids))
      (core/unique-constraints-indexed entity)))))

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
       "create table %s(\n %s\n)"
       table
       (if (= f t)
         (clojure.string/join
          ",\n "
          [(str to-field " INTEGER not null references \"" to-table "\"(_eid) on delete cascade")
           "from_xid TEXT"
           "to_xid TEXT"
           (str "unique(" to-field ")")])
         (clojure.string/join
          ",\n "
          [(str from-field " INTEGER not null references \"" from-table "\"(_eid) on delete cascade")
           (str to-field " INTEGER not null references \"" to-table "\"(_eid) on delete cascade")
           "from_xid TEXT"
           "to_xid TEXT"
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
    [(format "create index %s_fidx on \"%s\" (%s);" table table from-field)
     (format "create index %s_tidx on \"%s\" (%s);" table table to-field)]))

(defn analyze-projection
  [projection]
  (let [new-entities
        (filter
         core/added?
         (core/get-entities projection))
        ;;
        reactivated-entities
        (filter
         (fn [entity]
           (and (not (core/added? entity))
                (false? (:active (core/suppress entity)))
                (true? (:active entity))))
         (core/get-entities projection))
        ;;
        changed-enties
        (filter
         core/diff?
         (core/get-entities projection))
        ;;
        new-relations
        (filter
         (fn [relation]
           (core/added? relation))
         (core/get-relations projection))
        ;;
        changed-relations
        (filter
         (fn [relation]
           (core/diff? relation))
         (core/get-relations projection))
        ;;
        {nrr true
         nr false} (group-by core/recursive-relation? new-relations)
        ;;
        {crr true
         cr false} (group-by core/recursive-relation? changed-relations)]
    {:new/entities new-entities
     :reactivated/entities reactivated-entities
     :changed/entities changed-enties
     :new/relations nr
     :new/recursive-relations nrr
     :changed/relations cr
     :changed/recursive-relations crr}))

(def boolean-literals
  #{"t" "true" "y" "yes" "on" "1" "f" "false" "n" "no" "off" "0"})

(defn convertible?
  [to-type ^String v]
  (let [v (clojure.string/trim v)]
    (try
      (case to-type
        "int"     (do (Long/parseLong v) true)
        "float"   (do (Double/parseDouble v) true)
        "boolean" (contains? boolean-literals (clojure.string/lower-case v))
        true)
      (catch NumberFormatException _ false))))

(defn check-stored-values!
  "Throw INVALID_VALUE when stored text values won't convert to the new type; SQLite has no cast to fail on its own."
  [conn entity attribute table column to-type]
  (when (#{"int" "float" "boolean"} to-type)
    (let [q    [(format "select %s as v from \"%s\" where %s is not null and typeof(%s) = 'text'"
                        column table column column)]
          bad  (remove #(convertible? to-type (:v %))
                       (if conn (execute! conn q) (execute! q)))]
      (when (seq bad)
        (throw (errors/invalid-conversion-error entity attribute to-type
                                                (count bad) (:v (first bad))))))))

(defn column-exists?
  [tx table column]
  (let [sql (format "PRAGMA table_info(\"%s\")" table)
        columns (execute! tx [sql] :raw)]
    (boolean (some #(= column (:name %)) columns))))

(defn attribute-delta->ddl
  "DDL for one changed attribute; `conn` is the deploy's own transaction."
  ([entity attribute] (attribute-delta->ddl entity attribute nil))
  ([entity
    {:keys [name type]
     {:keys [values]} :configuration
     :as attribute}
    conn]
  (let [oentity (core/suppress entity)
        old-table (entity->table-name oentity)
        diff (core/diff attribute)]
    (if (core/new-attribute? attribute)
      ;; SQLite has no ADD COLUMN IF NOT EXISTS — a reactivated attribute's column is still there
      (when-not (and conn (column-exists? conn old-table (normalize-name name)))
        (log/debug {:id ::attribute-added :data {:attribute name :table old-table}}
                   "Adding attribute to table")
        [(str "alter table \"" old-table "\" add column " (column-name name) " " (type->ddl type))])
      ;;
      (when (not-empty (dissoc diff :pk))
        (let [{dn :name
               dt :type
               dconfig :configuration} diff
              column (column-name (or dn name))]
          (when dt (check-type-conversion! entity attribute dt type))
          (cond-> []
            (and dn (not= (column-name dn) (column-name name)))
            (conj
             (do
               (log/debug {:id ::column-renamed
                           :data {:table old-table
                                  :from column
                                  :to (column-name name)}}
                          "Renaming table column")
               (format
                "alter table \"%s\" rename column %s to %s"
                old-table column (column-name name))))
            dt
            (as-> statements
                  (do
                    (check-stored-values! conn entity attribute old-table column type)
                    (log/debug {:id ::column-type-change-no-ddl
                                :data {:table old-table :column column
                                       :from-type dt :to-type type}}
                               "No DDL needed for type change (dynamic typing)")
                    statements))
            (and (= type "enum") (nil? dt))
            (as-> statements
                  (let [id-key (id/key)
                        [ov nv] (clojure.data/diff
                                 (reduce
                                  (fn [r [idx v]]
                                    (let [n (:name v)
                                          e (get v id-key)]
                                      (if (not-empty n)
                                        (assoc r (or e (get-in values [idx id-key])) n)
                                        r)))
                                  nil
                                  (map-indexed vector (:values dconfig)))
                                 (zipmap
                                  (map id-key values)
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
                    (log/trace {:id ::enum-diffed
                                :data {:diff-config dconfig
                                       :old-enums ov
                                       :new-enums nv}}
                               "Diffing enum values")
                    (when (seq removed)
                      (let [in-clause (clojure.string/join
                                       ", "
                                       (map #(str \' (normalized-enum-value %) \') removed))
                            q [(format "select count(*) as cnt from \"%s\" where %s in (%s)"
                                       old-table column in-clause)]
                            [{cnt :cnt}] (if conn (execute! conn q) (execute! q))]
                        (when (pos? (or cnt 0))
                          (throw (ex-info
                                  (format "Cannot remove enum value(s) %s from attribute '%s' - %d row(s) still reference them; deactivate the value instead"
                                          (clojure.string/join ", " removed) name cnt)
                                  {:type ::enum-value-removal-forbidden
                                   :code "GUARD_VIOLATION"
                                   :hint "Deactivate the value instead of removing it."
                                   :entity-name name
                                   :table-name old-table
                                   :column column
                                   :removed removed
                                   :rows cnt})))))
                    (cond-> statements
                      (seq renames)
                      (into
                       (map (fn [[old-name new-name]]
                              (format "update \"%s\" set %s = '%s' where %s = '%s'"
                                      old-table column (normalized-enum-value new-name)
                                      column (normalized-enum-value old-name)))
                            renames))))))))))))

(defn orphaned-attribute->drop-ddl
  "DROP COLUMN for an attribute no remaining deployed version references."
  [{:keys [entity attribute]}]
  (let [table-name (entity->table-name entity)
        column-name (column-name (:name attribute))]
    [(format "ALTER TABLE \"%s\" DROP COLUMN %s"
             table-name column-name)]))

(defn entity-delta->ddl
  ([entity] (entity-delta->ddl entity nil))
  ([{:keys [attributes]
     :as entity}
    conn]
  (assert (core/diff? entity) "This entity is already synced with DB")
  (let [diff (core/diff entity)
        old-entity (core/suppress entity)
        old-table (entity->table-name old-entity)
        old-constraints (get-in old-entity [:configuration :constraints :unique])
        new-unique (core/unique-constraints-indexed entity)
        raw-groups (max (count (get-in entity [:configuration :constraints :unique]))
                        (count old-constraints))
        group-members (into #{}
                            (comp cat cat)
                            [(get-in entity [:configuration :constraints :unique])
                             old-constraints])
        table (entity->table-name entity)
        attributes' (keep #(attribute-delta->ddl entity % conn) attributes)]
    #_(do
        (def attributes' attributes')
        (def diff diff)
        (def entity entity)
        (def attributes attributes)
        (def old-entity old-entity)
        (def old-table old-table)
        (def table table)
        (def old-constraints old-constraints))
    (cond-> (reduce into [] attributes')
      (:name diff)
      (into
       [(format "alter table \"%s\" rename to \"%s\"" old-table table)])
      ;; second arm is load-bearing: deactivation leaves the constraints config untouched
      (or (-> diff :configuration :constraints)
          (some #(group-members (id/extract %)) (:attributes diff)))
      (into
       (reduce
        (fn [statements idx]
          (let [statements (conj statements
                                 (format "drop index if exists \"%s\""
                                         (unique-index-name table idx)))]
            (cond-> statements
              (seq (get new-unique idx))
              (conj (unique-index-ddl entity table idx (get new-unique idx))))))
        []
        (range raw-groups)))))))

(defn transform-relation
  [tx {:keys [from to]
       :as relation}]
  (try
    (let [diff (core/diff relation)
          _ (log/trace {:id ::transforming-relation
                        :data {:diff diff
                               :from (pr-str from)
                               :to (pr-str to)}}
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
                   "alter table %s rename to %s"
                   old-name new-name)]
          (log/debug {:id ::relation-table-renamed
                      :data {:from old-name :to new-name :sql sql}}
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
                      e))))))
      (when (:name to-diff)
        (let [o (entity->relation-field old-to)
              n (entity->relation-field to)
              sql (format
                   "alter table %s rename column %s to %s"
                   new-name o n)]
          (log/debug {:id ::relation-to-column-renamed
                      :data {:table new-name
                             :from-column o
                             :to-column n
                             :sql sql}}
                     "Renaming relation 'to' column")
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
                   "alter table %s rename column %s to %s"
                   new-name o n)]
          (log/debug {:id ::relation-from-column-renamed
                      :data {:table new-name
                             :from-column o
                             :to-column n
                             :sql sql}}
                     "Renaming relation 'from' column")
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

(defn transform-database [ds projection configuration]
  (log/debug {:id ::transforming-database :data {:configuration configuration}}
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
          (log/info {:id ::new-entities-generating
                     :data {:entities (map :name ne)}}
                    "Generating new entities"))
        (doseq [{n :name
                 :as entity} ne
                :let [table-sql (generate-entity-ddl entity)
                      table (entity->table-name entity)]]
          (try
            (log/debug {:id ::entity-added :data {:entity n :sql table-sql}}
                       "Adding entity to DB")
            (try
              (execute-one! tx [table-sql])
              (doseq [index-sql (generate-entity-index-ddl entity)]
                (execute-one! tx [index-sql]))
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
                        entity nil e tx))))
            (catch clojure.lang.ExceptionInfo e
              (throw e))
            (catch Throwable e
              (throw (ex-info
                      (format "Unexpected error while creating entity '%s'" n)
                      {:type ::entity-creation-error
                       :phase :ddl-execution
                       :entity-name n
                       :entity-id (id/extract entity)
                       :table-name table}
                      e))))))

      (when (not-empty ne)
        (enhance/transform-audit *db* tx ne))

      (when (not-empty ce) (log/info {:id ::checking-changed-entities} "Checking changed entities"))
      (doseq [{n :name
               :as entity} ce
              :let [sql (entity-delta->ddl entity tx)]]
        (log/debug {:id ::entity-changing :data {:entity n}} "Changing entity")
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
                      entity statement e tx))))))
      (when (not-empty cr)
        (log/info {:id ::checking-changed-relations}
                  "Checking changed trans entity relations"))
      (doseq [r cr] (transform-relation tx r))
      (when (not-empty crr)
        (log/info {:id ::checking-changed-recursive-relations}
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
            (log/debug {:id ::recursive-relation-no-previous-label
                        :data {:relation-id id :entity tname}}
                       "Previous deploy didn't have to-label for recursive relation")
            (when tl
              (when-not (column-exists? tx table tl)
                (let [sql (format
                           "alter table %s add %s INTEGER references \"%s\"(_eid) on delete cascade"
                           table tl table)]
                  (log/debug {:id ::recursive-relation-created
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
              (log/debug {:id ::recursive-relation-updated
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
      (when (not-empty nr) (log/info {:id ::generating-new-relations} "Generating new relations"))
      (doseq [{{tname :name
                :as to-entity} :to
               {fname :name
                :as from-entity} :from
               :as relation} nr
              :let [sql (generate-relation-ddl projection relation)
                    [from-idx to-idx] (generate-relation-indexes-ddl projection relation)]]
        (try
          (log/debug {:id ::entities-connected
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
            (log/debug {:id ::relation-from-index-created
                        :data {:from fname :to tname :sql from-idx}}
                       "Creating from index for relation")
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
            (log/debug {:id ::relation-to-index-created
                        :data {:from fname :to tname :sql to-idx}}
                       "Creating to index for relation")
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
        (log/info {:id ::adding-new-recursive-relations}
                  "Adding new recursive relations"))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label
               :as r} nrr
              :when (not-empty tl)
              :let [rel-id (id/extract r)
                    table (entity->table-name e)
                    sql (format
                         "alter table %s add %s INTEGER references \"%s\"(_eid) on delete cascade"
                         table (column-name tl) table)]]
        (log/debug {:id ::recursive-relation-created
                    :data {:entity tname :sql sql}}
                   "Creating recursive relation for entity")
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
  "All versions of a dataset by id or :name, newest modified_on first."
  [{:keys [name]
    :as args}]
  (let [dataset-id (id/extract args)]
    (:versions
     (dataset/get-entity
      :dataset/dataset
      (if dataset-id
        {(id/key) dataset-id}
        {:name name})
      {:name nil
       (id/key) nil
       :versions
       [{:selections
         {(id/key) nil
          :name nil
          :model nil
          :deployed nil
          :deployed_on nil}
         :args {:_order_by {:modified_on :desc}}}]}))))

(defn decode-version-model
  [version]
  (update version :model (fn [m] (if (string? m) (<-transit m) m))))

(defn encode-version-model
  [version]
  (update version :model (fn [m] (if (string? m) m (->transit m)))))

(defn deployed-versions
  "Deployed versions, oldest deployed_on first, so a redeployed version wins the rebuild."
  []
  (mapv decode-version-model
        (dataset/search-entity
         :dataset/version
         {:deployed {:_eq true}
          :_order_by {:deployed_on :asc}}
         {(id/key) nil
          :model nil
          :deployed_on nil
          :dataset [{:selections {(id/key) nil}}]})))

(defn get-version
  [version-id]
  (decode-version-model
   (dataset/get-entity
    :dataset/version
    {(id/key) version-id}
    {(id/key) nil
     :model nil
     :modified_on nil
     :dataset [{:selections {(id/key) nil}}]})))

(comment
  (def version (get-version #uuid "b54595c9-3759-470f-9657-f4b0bc19e294"))
  (dataset/sync-entity :dataset/version {(id/key) (id/extract version)
                                         :deployed true}))

(defn last-deployed-version-per-dataset
  []
  (let [datasets (dataset/search-entity
                  :dataset/dataset
                  nil
                  {(id/key) nil
                   :name nil
                   :versions [{:selections
                               {(id/key) nil
                                :model nil
                                :deployed_on nil
                                :dataset [{:selections {(id/key) nil}}]}
                               :args {:deployed {:_eq true}
                                      :_order_by {:deployed_on :desc}
                                      :_limit 1}}]})]
    (mapv decode-version-model (mapcat :versions datasets))))

(comment
  (dataset/search-entity
   :dataset/version
   nil
   {:name nil
    :entities [{:selections {:name nil}}]
    :modified_by [{:selections {:name nil}}]})
  (dataset/search-entity
   :iam/user
   nil
   {(id/key) nil
    :name nil
    :groups [{:selections {:name nil}}]
    :roles [{:selections {:name nil}}]}))

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
   (let [initial-model (core/map->ERDModel {:entities {}
                                            :relations {}})
         final-model
         (reduce
          (fn [global version]
            (core/join-models global (core/add-claims (version-rls-model version) (id/extract version))))
          initial-model
          all-versions)]
     (-> final-model
         (core/reconcile-rls-guards (latest-version-models))
         (core/activate-model (gen-activation-filter))))))

(defn last-deployed-model
  "Global model of every deployed version, with claims and :active flags."
  []
  (rebuild-global-model))

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

(extend-protocol core/DatasetProtocol
  synthigy.db.SQLite
  (core/preview-model [_ version]
    (let [version (-> version decode-version-model (update :model core/normalize-legacy-types))
          global (or (try
                       (last-deployed-model)
                       (catch Throwable _ nil))
                     (core/map->ERDModel {:entities {} :relations {}}))]
      (core/fold-version global (version-rls-model version) (id/extract version)
                         (latest-version-models version)
                         (gen-activation-filter version))))
  (core/preview-recall [_ version-ids]
    (let [recalled (set version-ids)]
      (rebuild-global-model (remove #(contains? recalled (id/extract %)) (deployed-versions)))))
  (core/deploy!
    [this version]
    (let [version (-> version decode-version-model (update :model core/normalize-legacy-types))
          {:keys [model]} version]
      (try
        (let [global (or (try
                           (last-deployed-model)
                           (catch Throwable _
                             (log/warn {:id ::deploy-empty-model-fallback :data {:action :deploying :subject :dataset}}
                                       "Cannot query dataset entities during deploy, starting with empty model")
                             nil))
                         (core/map->ERDModel {:entities {}
                                              :relations {}}))

              _ (check-entity-name-conflicts! global model)

            ;; refuse before any DDL if the result would drop a guard — the op fail-closes silently
              _ (rls/assert-guards-compile! (core/preview-model this version) model)

              _ (core/mount this (assoc version :model (core/with-merged-config global model)))

              version'' (encode-version-model
                         (assoc version
                                :model (assoc model :version 1)
                                :deployed_on (java.util.Date.)
                                :deployed true))]
          (sync-entity this :dataset/version version'')
          (core/reload this (assoc version :model model))

          (let [updated-model (assoc (rebuild-global-model) :version 1)]
            (dataset/save-model! updated-model)
            (add-to-deploy-history! this updated-model)
            (let [all-entities (vec (core/get-entities updated-model))]
              (enhance/transform-audit *db* (:datasource *db*) all-entities)))

          (log/info {:id ::preparing-model :data {:action :preparing :subject :model}} "Preparing model for DB")
          version'')
        (catch Throwable e
          (log/error! {:id ::deploy-failed
                       :data {:action :deploying :subject :dataset
                              :version (:name version)
                              :dataset (:name (:dataset version))}}
                      e)
          (throw e)))))
  ;;
  (core/recall!
    [this version-ref]
    (let [version-id (id/extract version-ref)]
      (assert version-id "Version ID is required")

      (let [version (-> (dataset/get-entity
                         :dataset/version
                         {(id/key) version-id}
                         {(id/key) nil
                          :name nil
                          :model nil
                          :deployed nil
                          :dataset [{:selections {(id/key) nil
                                                  :name nil}}]})
                        decode-version-model)

            _ (when-not version
                (throw (ex-info (format "Version %s not found" version-id)
                                {:type :version-not-found
                                 (id/key) version-id})))
            _ (when-not (:deployed version)
                (throw (ex-info (format "Version %s was never deployed, cannot recall" version-id)
                                {:type :version-not-deployed
                                 (id/key) version-id
                                 :version version})))

            dataset (:dataset version)
            dataset-name (:name dataset)
            dataset-id (id/extract dataset)

            all-deployed-versions (filter :deployed (get-dataset-versions dataset))

            only-version? (= 1 (count all-deployed-versions))

            most-recent-version (first all-deployed-versions)
            is-most-recent? (= version-id (id/extract most-recent-version))

            global (dataset/deployed-model)
            version-uuids #{version-id}

            exclusive-entities (core/find-exclusive-entities global version-uuids)
            exclusive-relations (core/find-exclusive-relations global version-uuids)]

        (log/info {:id ::version-recalling
                   :data {:dataset dataset-name
                          :version (:name version)
                          :only? only-version?
                          :most-recent? is-most-recent?}}
                  "Recalling version")

        (when (or (not-empty exclusive-entities) (not-empty exclusive-relations))
          (log/info {:id ::exclusives-pruning
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
                         "Removed relation table"))
            (doseq [entity exclusive-entities
                    :let [entity (core/get-entity global (id/extract entity))]
                    :when (some? entity)]
              (let [table-name (entity->table-name entity)
                    sql (format "drop table if exists \"%s\"" table-name)]
                (log/trace {:id ::entity-removing
                            :data {:entity (:name entity) :table table-name}}
                           "Removing entity")
                (execute-one! con [sql])))))

        (let [updated-model (rebuild-global-model (remove #(= (id/extract %) version-id) (deployed-versions)))
              recalled-model (core/normalize-legacy-types (:model version))

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
              (delete-entity this :dataset/version {(id/key) version-id})
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
              (delete-entity this :dataset/version {(id/key) version-id}))
            :else
            (log/info {:id ::older-version-recalled
                       :data {:dataset dataset-name :version (:name version)}}
                      "Recalled older version, rebuilding model"))
          (dataset/delete-entity :dataset/version {(id/key) version-id})
          (let [final-model (rebuild-global-model)]
            (dataset/save-model! final-model)
            (query/deploy-schema (query/model->schema final-model))
            (add-to-deploy-history! this final-model))))))
  ;;
  (core/destroy! [this record]
    (assert (or (:name record) (id/extract record)) "Specify dataset name or id!")
    (let [dataset-id (id/extract record)
          {all-versions :versions} (dataset/get-entity
                                    :dataset/dataset
                                    (if dataset-id
                                      {(id/key) dataset-id}
                                      {:name (:name record)})
                                    {:name nil
                                     (id/key) nil
                                     :versions [{:args {:_order_by [{:modified_on :asc}]}
                                                 :selections {:name nil
                                                              (id/key) nil
                                                              :deployed nil
                                                              :model nil}}]})]
      (dataset/search-entity :dataset/dataset nil {:name nil :xid nil})
      (log/info {:id ::dataset-destroying
                 :data {:dataset (or (:name record) dataset-id)
                        :version-count (count all-versions)}}
                "Destroying dataset, recalling versions in reverse order")
      (doseq [{:keys [deployed] :as version} all-versions]
        (if deployed
          (core/recall! this {(id/key) (id/extract version)})
          (dataset/delete :dataset/version {(id/key) (id/extract version)})))
      (dataset/delete :dataset/dataset (select-keys record [:name (id/key)]))))
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
           schema (query/model->schema model')]
       (dataset/save-model! model')
       (query/deploy-schema schema)
       model'))
    ([this version]
     (let [{:keys [model]} (decode-version-model version)
           global (or
                   (core/get-model this)
                   (core/map->ERDModel nil))
           version-id (id/extract version)
           model'' (core/fold-version global model version-id
                                      (latest-version-models version)
                                      (gen-activation-filter version))
           schema (query/model->schema model'')]
       (query/deploy-schema schema)
       (dataset/save-model! model'')
       model'')))
  (core/mount
    [this version]
    (let [{model :model :as version} (decode-version-model version)]
      (log/debug {:id ::version-mounting
                  :data {:version (:name version)
                         :dataset (get-in version [:dataset :name])}}
                 "Mounting dataset version")
      (let [global (or
                    (core/get-model this)
                    (core/map->ERDModel nil))
            projection (core/project global model)]
        (transform-database (:datasource *db*) projection nil)
        version)))
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
              (log/trace {:id ::entity-removing
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
     (log/warn {:id ::get-last-deployed-manual-recovery :data {:action :recovering :subject :deploy-history}}
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

(defn create-deploy-history!
  []
  (execute-one!
   [(format
     "create table __deploy_history (
           \"deployed_on\" timestamp not null default CURRENT_TIMESTAMP,
           \"model\" text)")]))

(defn add-to-deploy-history!
  ([model]
   (execute-one!
    ["insert into __deploy_history (model) values (?)"
     (->transit model)]))
  ([_db model]
   (add-to-deploy-history! model)))

(lifecycle/register-module!
 :synthigy/dataset
 {:depends-on [:synthigy/log :synthigy/database :synthigy.dataset/encryption :synthigy/log.config]
  :headline true
  :doc "ERD model — schema deploy, /data query engine"
  :setup (fn []
           (log/info {:id ::lifecycle-setup-starting :data {:action :setup}} "Setting up dataset meta-model")

           (synthigy.transit/init)
           (log/info {:id ::transit-handlers-initialized} "Transit handlers initialized")

           (let [db-config (sqlite/from-env)
                 db (sqlite/connect db-config)]

             (alter-var-root #'synthigy.db/*db* (constantly db))

             (log/info {:id ::dataset-tables-initializing
                        :data {:config (pr-str db-config)}}
                       "Initializing dataset tables for SQLite database")

             (create-deploy-history!)
             (log/info {:id ::deploy-history-created :data {:action :created :subject :deploy-history}} "Created __deploy_history")

             (log/info {:id ::synthigy-model-loading :data {:action :loading :subject :dataset-model}}
                       "Loading Synthigy model from dataset/synthigy.json")
             (dataset/register-reference-types!)
             (let [version (dataset/current-synthigy-version)]
               (dataset/save-model! nil)
               (core/mount db version)
               (core/reload db version)
               (log/info {:id ::synthigy-model-mounted :data {:action :mounted :subject :dataset-model}}
                         "Mounted Synthigy model")
               (dataset/deploy! version))

             (log/info {:id ::model-reloading :data {:action :reloading :subject :model}} "Reloading model")
             (core/reload db)

             (log/info {:id ::model-history-adding :data {:action :recording :subject :deployed-model}} "Adding deployed model to history")
             (add-to-deploy-history! (core/get-model db))

             (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete}} "Dataset system setup complete")))

  :cleanup (fn []
             (log/info {:id ::lifecycle-cleanup-starting :data {:action :cleanup}} "Clearing dataset caches")
             (dataset/save-model! nil)
             (query/deploy-schema nil)
             (log/info {:id ::lifecycle-cleanup-complete :data {:action :cleanup-complete}} "Dataset cleanup complete"))

  :start (fn []
           (log/info {:id ::lifecycle-starting :data {:action :starting}} "Starting dataset system")
           (dataset/start)
           (log/info {:id ::lifecycle-started :data {:action :started}} "Dataset system started"))

  :stop (fn []
          (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "Stopping dataset system")
          (dataset/stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "Dataset system stopped"))})

(extend-protocol SQLNameResolution
  synthigy.db.SQLite
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

(comment
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
     {(id/key) #uuid "743a9023-3980-405a-8340-3527d91064d8"}
     {:versions [{:selections
                  {(id/key) nil
                   :name nil}}]})))

(extend-type SQLite
  proto/TypeCodec

  (encode [_db type value]
    (case type
      ("string" "int" "float" "timeperiod" "currency" "uuid" "avatar")
      value

      "timestamp"
      (when (some? value) (ts/->sortable-text value))

      "boolean"
      (if (nil? value) nil (if value 1 0))

      "json"
      (when value
        (->json value))

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
      ("string" "int" "float" "timeperiod" "currency" "uuid" "avatar")
      value

      "timestamp"
      (when (some? value) (ts/->date value))

      "boolean"
      (when value
        (cond
          (boolean? value) value
          (number? value) (not (zero? value))
          :else value))

      ;; opaque user data — never coerce keys or values
      "json"
      (when value
        (cond
          (or (map? value) (vector? value)) value
          (string? value) (try
                            (<-json value {:keyfn identity :valfn nil})
                            (catch Exception _
                              value))
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
          (string? value) (keyword value)
          :else value)))))

(extend-type SQLite
  proto/SQLDialect

  (like-operator [_db case-sensitive?]
    (if case-sensitive?
      "GLOB"
      "LIKE"))

  (limit-offset-clause [_db limit offset]
    (cond
      (and limit offset) (str "LIMIT " limit " OFFSET " offset)
      limit (str "LIMIT " limit)
      offset (str "LIMIT -1 OFFSET " offset)
      :else ""))

  (placeholder-for-type [_db _field-type]
    "?")

  (excluded-ref [_db column]
    (format "excluded.%s" column))

  (max-bind-params [_db]
    32766)

  (row-lock-clause [_db]
    nil))

(extend-type SQLite
  proto/SchemaManager

  (get-tables [db]
    (let [results (execute!
                   ["SELECT name FROM sqlite_master
                     WHERE type = 'table'
                     AND name NOT LIKE 'sqlite_%'
                     AND name NOT LIKE '__deploy%'
                     AND name NOT LIKE '__component_versions%'
                     AND name NOT LIKE '__lifecycle_state%'
                     ORDER BY name"]
                   :raw)]
      (mapv :name results)))

  (get-columns [db table]
    (let [results (execute!
                   [(str "PRAGMA table_info(\"" table "\")")]
                   :raw)]
      (mapv (fn [row]
              {:name (:name row)
               :type (:type row)
               :nullable (zero? (:notnull row))
               :default (:dflt_value row)})
            results)))

  (get-enums [db]
    nil)

  (table-exists? [db table]
    (let [result (execute!
                  ["SELECT COUNT(*) as count FROM sqlite_master
                    WHERE type = 'table' AND name = ?"
                   table]
                  :raw)]
      (pos? (:count (first result)))))

  (column-exists? [db table column]
    (let [columns (proto/get-columns db table)
          column-names (set (map :name columns))]
      (contains? column-names column)))

  (list-tables-like [_db pattern]
    (execute!
     ["SELECT name as tablename FROM sqlite_master
       WHERE type = 'table'
       AND name LIKE ?
       ORDER BY name"
      pattern]
     :raw))

  (drop-table! [_db table]
    (try
      (execute-one! [(str "DROP TABLE IF EXISTS \"" table "\"")])
      true
      (catch Throwable _
        false)))

  (drop-tables-like! [db pattern]
    (let [tables (proto/list-tables-like db pattern)]
      (doseq [{:keys [tablename]} tables]
        (proto/drop-table! db tablename))
      (count tables)))

  (truncate-table! [_db table]
    (execute-one! [(str "DELETE FROM \"" table "\"")]))

  (list-types-like [_db pattern]
    [])

  (drop-type! [_db type-name]
    false)

  (drop-types-like! [_db pattern]
    0))
