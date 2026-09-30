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

(ns synthigy.dataset.sqlite
  (:require
   [buddy.hashers :as hashers]
   clojure.data
    ;; JSON via synthigy.json (jsonista)
   [clojure.java.io :as io]
   clojure.set
   clojure.string
   [next.jdbc :as jdbc]
   ; [clojure.pprint :refer [pprint]]
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
    ;; IMPORTANT: Load SQLite-specific query namespace to ensure ModelQueryProtocol
    ;; is extended for synthigy.db.SQLite. Without this, requiring synthigy.dataset.sqlite
    ;; directly (e.g., from tests) would register the SQLite lifecycle module but lack
    ;; the protocol implementations needed for dataset operations.
   synthigy.dataset.sqlite.query
    ;; Load patch registration for :synthigy.dataset/model
   synthigy.dataset.sqlite.patch
   [synthigy.db
    :refer [*db*
            sync-entity
            delete-entity]]
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.db.sqlite :as sqlite]
   [synthigy.json :refer [<-json ->json]]
   [synthigy.log :as log]
   ;; loaded for its lifecycle registration — :synthigy/dataset depends on
   ;; :synthigy/log.config so the data plane always carries DB-backed log routing
   [synthigy.log.config]
   [synthigy.transit
    :refer [<-transit ->transit]]
   [synthigy.timestamp :as ts])
  (:import
   [synthigy.db SQLite]))

;;; Forward declarations
(declare add-to-deploy-history!)

(defn reference-table
  "Returns table name for a reference type, or nil if not registered.
   Logs a warning when reference type is not registered."
  [type-name]
  (if-let [table-fn (core/reference-table-fn type-name)]
    (table-fn)
    (do
      (log/warn {:id ::reference-type-unregistered :data {:type type-name}}
                "No table-fn registered for reference type")
      nil)))

;;; Type Conversion Validation (Backend-specific wrapper)
;;; The core validation logic is in synthigy.dataset.core (shared with frontend)

(defn check-type-conversion!
  "Backend-specific wrapper that validates type conversion and throws exception if forbidden.
   The actual validation logic is in synthigy.dataset.core/validate-type-conversion
   which is shared between frontend and backend."
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

;;; End Type Conversion Validation

(defn type->ddl
  "Converts type to DDL syntax (SQLite)"
  [t]
  (try
    (if (core/reference-type? t)
      ;; Dynamic reference type - look up table from *reference-mapping*
      (if-let [table (reference-table t)]
        (str "INTEGER references \"" table "\"(_eid) on delete set null")
        (throw (ex-info
                (format "Cannot generate DDL for unregistered reference type '%s'" t)
                {:type ::unregistered-reference-type
                 :phase :ddl-generation
                 :reference-type t})))
      (if (core/reference-type-names t)
        ;; never fall through to the case — an unregistered reference type
        ;; emits a bare `user` column type, which SQLite accepts silently
        (throw (ex-info
                (format "Reference type '%s' is not registered; register IAM reference types before mounting a model that uses them" t)
                {:type ::unregistered-reference-type
                 :phase :ddl-generation
                 :reference-type t}))
        ;; Standard types
        (case t
        "currency" "TEXT"
        ("avatar" "string" "hashed") "TEXT"
        "timestamp" "TEXT"
        ("json" "encrypted" "timeperiod") "TEXT"
        "transit" "TEXT"
        "int" "INTEGER"
        ;; enum - TEXT everywhere since dataset 1.4.0; value list enforced at
        ;; the write path, not by the database
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
  "Function converts attribute to DDL syntax (SQLite - enums stored as TEXT)"
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
          ;; Unique groups are NOT inlined here — they ship as separate unique
          ;; INDEXES (`generate-entity-index-ddl`). An inline constraint can
          ;; never be dropped or changed on SQLite. See
          ;; docs/core/synthigy/dataset/sqlite.md.
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
  "Unique-group indexes for a NEW entity table. SQLite cannot alter a table
   constraint, so groups live as indexes from the start."
  [entity]
  (let [table (entity->table-name entity)]
    (vec
     (keep-indexed
      ;; active-aware + index-stable: a group whose attribute is inactive nils
      ;; out at its position — never index a deactivated/removed column.
      (fn [idx ids] (unique-index-ddl entity table idx ids))
      (core/unique-constraints-indexed entity)))))

(defn generate-relation-ddl
  "Returns relation table DDL for given model and target relation.
   Includes from_xid / to_xid TEXT columns for the relation-audit
   plug (denormalized at link time via link-relations subselects).
   On SQLite the app-path is authoritative for populating these — no
   BEFORE INSERT trigger fallback because SQLite can't modify NEW."
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
        ;; Create table
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
  "Returns relation table DDL for given model and target relation"
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
     :reactivated/entities reactivated-entities ; NEW
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

;;
(defn attribute-delta->ddl
  "DDL for one changed attribute; `conn` is the deploy's own transaction."
  ([entity attribute] (attribute-delta->ddl entity attribute nil))
  ([entity
    {:keys [name type]
     {:keys [values]} :configuration
     :as attribute}
    conn]
  ;; Don't look at primary key since that is EYWA problem
  (let [oentity (core/suppress entity)
        old-table (entity->table-name oentity)
        diff (core/diff attribute)]
    (if (core/new-attribute? attribute)
      ;;
      (do
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
            ;; Change attribute name
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
            ;; SQLite: Enum is TEXT, no custom types needed
            ;; (no-op - enum type changes don't need DDL)
            ;; Type has changed
            ;; Reference conversions rejected upstream (check-type-conversion!).
            ;; Scalar changes need no DDL — SQLite uses dynamic typing;
            ;; string→json works because decoder is tolerant of plain strings.
            dt
            (as-> statements
                  (do
                    (check-stored-values! conn entity attribute old-table column type)
                    (log/debug {:id ::column-type-change-no-ddl
                                :data {:table old-table :column column
                                       :from-type dt :to-type type}}
                               "No DDL needed for type change (dynamic typing)")
                    statements))
            ;; Enum value changes are pure data operations (dataset 1.4.0,
            ;; identical semantics on every backend):
            ;;   add    - no statement at all
            ;;   rename - UPDATE rewriting old label to new
            ;;   remove - allowed only when no rows reference the label;
            ;;            otherwise the deploy fails - deactivate instead.
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
                    ;; SQLite cannot RAISE outside triggers, so the removal
                    ;; guard runs here at generation time instead of as a SQL
                    ;; statement. SQLite's single-writer lock makes
                    ;; check-then-deploy effectively race-free.
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
  "Generates DROP COLUMN DDL for an orphaned attribute.
   An orphaned attribute exists only in a recalled/destroyed version,
   not in any remaining deployed versions.

   Returns a vector of DDL statements to execute.

   Note: SQLite DROP COLUMN doesn't support IF EXISTS, but the column
   should always exist since we're dropping from a recalled version.
   SQLite has no enum types (stored as TEXT), so no type cleanup needed."
  [{:keys [entity attribute]}]
  (let [table-name (entity->table-name entity)
        column-name (column-name (:name attribute))]
    [(format "ALTER TABLE \"%s\" DROP COLUMN %s"
             table-name column-name)]))

;; 1. Change attributes by calling attribute-delta->ddl
;; 2. Rename table if needed
;; 3. Change constraints
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
        ;; DESIRED groups only — index-stable, active-aware. Reconcile is
        ;; declarative (drop-then-create), so it never has to know what the
        ;; database currently holds. Mirrors postgres.clj.
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
      ;; Renaming occured
      (:name diff)
      (into
        ;; SQLite: Just rename the table - constraints, indexes, and triggers move automatically
        ;; No need for RENAME CONSTRAINT (doesn't exist in SQLite) or ALTER SEQUENCE (no sequences)
       [(format "alter table \"%s\" rename to \"%s\"" old-table table)])
      ;; Reconcile unique groups whenever the constraints config changed OR an
      ;; attribute participating in one did — deactivating an attribute leaves
      ;; the config untouched and KEEPS the column, so nothing else would drop
      ;; the dependent index. SQLite has no constraint DDL: groups are INDEXES,
      ;; which is the only form that can be dropped and recreated. See
      ;; docs/core/synthigy/dataset/sqlite.md.
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
          ;; Assoc old from and to entities
          ;; This will be handled latter
          new-name (relation->table-name relation)]

      ;; When name has changed
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
      ;; when to name has changed than change table column
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
      ;; when from name has changed than change table column
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
      ;; Re-throw ex-info with preserved context
      (throw e))
    (catch Throwable e
      ;; Catch any other errors during relation transformation
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
  "Check if a column exists in a SQLite table using PRAGMA table_info"
  [tx table column]
  (let [sql (format "PRAGMA table_info(\"%s\")" table)
        columns (execute! tx [sql] :raw)]
    (boolean (some #(= column (:name %)) columns))))

;; 1. Generate new entities by creating tables
;;  - Create new types if needed by enum attributes
;; 2. Add audit attributes if present (modified_by,modified_on)
;; 3. Check if model has changed attributes
;;  - If so try to resolve changes by calling entity-delta->ddl
;; 4. Check if model has changed relations
;;  - If so try to resolve changes by calling transform-relation
;; 5. Generate new relations by connecting entities
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
      ;; Generate new entities
      (let [entity-priority {:iam/user -100}
            ne (sort-by
                (fn [e]
                  (get entity-priority (id/extract e) 0))
                ne)]
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
              ;; Re-throw ex-info with preserved context
              (throw e))
            (catch Throwable e
              ;; Catch any other unexpected errors during entity creation
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

      ;; Change entities
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
      ;; Change relations
      (when (not-empty cr)
        (log/info {:id ::checking-changed-relations}
                  "Checking changed trans entity relations"))
      (doseq [r cr] (transform-relation tx r))
      ;; Change recursive relation
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
                    ; _ (log/debug {:id ::recursive-relation-diff :data {:diff diff}} "RECURSIVE RELATION")
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
          ;; Apply changes
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
      ;; Generate new relations
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
            ;; Re-throw ex-info with preserved context
            (throw e))
          (catch Throwable e
            ;; Catch any other unexpected errors during relation creation
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
      ;; Add new recursive relations
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

(defn- get-dataset-versions
  "Gets all versions for a dataset by its ID or :name, ordered by modified_on desc"
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

(defn- decode-version-model
  "Decode the :model field from transit string to ERDModel record."
  [version]
  (update version :model (fn [m] (if (string? m) (<-transit m) m))))

(defn- encode-version-model
  "Encode the :model field to transit string for DB storage."
  [version]
  (update version :model (fn [m] (if (string? m) m (->transit m)))))

(defn deployed-versions
  "Returns all deployed versions ordered by deployed_on (most recently deployed last).
   This ensures that when a version is redeployed, it becomes the 'latest' and
   its model takes precedence in rebuild-global-model."
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

(defn deployed-version-per-dataset-ids
  []
  (try
    (set (map id/extract (last-deployed-version-per-dataset)))
    (catch Throwable _ #{})))

(defn latest-version-models
  "Models of the latest deployed version of every dataset, plus `include-version`
   when given."
  ([] (latest-version-models nil))
  ([include-version]
   (let [;; the meta-model is not queryable during the first deploy — it creates it
         prior (try
                 (let [latest (deployed-version-per-dataset-ids)
                       superseded (some-> include-version :dataset id/extract)]
                   (->> (deployed-versions)
                        (filter #(contains? latest (id/extract %)))
                        (remove #(= superseded (some-> % :dataset id/extract)))
                        (mapv (comp dataset/adapt-model-to-provider :model))))
                 (catch Throwable _ []))]
     (cond-> prior
       include-version (conj (:model include-version))))))

(defn gen-activation-filter
  "Returns a filter function for activate-model.
   Bootstrap behavior: if no versions deployed, everything is active.
   Optional: include-version-id to include a version being deployed (not yet visible in DB query)."
  ([] (gen-activation-filter nil))
  ([include-version-id]
   (let [deployed-versions (cond-> (deployed-version-per-dataset-ids)
                             include-version-id (conj include-version-id))]
     (if (empty? deployed-versions)
       (constantly true)
       (fn [claims]
         (boolean
          (not-empty
           (clojure.set/intersection
            claims
            deployed-versions))))))))

(defn rebuild-global-model
  "Utility function to rebuild the global model from ALL deployed dataset versions.
   Use this as a backup/recovery mechanism if __deploy_history is corrupted.

   Algorithm:
   1. Get ALL deployed versions (not just latest) in ascending order
   2. Build global model with claims via reduce + join-models
   3. Determine which entities/relations are in LATEST version per dataset
   4. Compute :active flags based on step 3
   5. Return global model (does NOT save to __deploy_history)"
  ([] (rebuild-global-model (deployed-versions)))
  ([all-versions]
   (let [;; Build global model with ALL claims
         ;; join-models handles active flags via "last deployment wins"
         initial-model (core/map->ERDModel {:entities {}
                                            :relations {}})
         final-model
         (reduce
          (fn [global version]
            (let [version-id (id/extract version)
                  model (:model version)]
              (core/join-models global (core/add-claims model version-id))))
          initial-model
          all-versions)]
     (-> final-model
         (core/reconcile-rls-guards (latest-version-models))
         (core/activate-model (gen-activation-filter))))))

(defn last-deployed-model
  "Returns the global model with ALL entities from ALL deployed versions.
   Entities have :claimed-by sets and :active flags properly computed.

   This rebuilds from ALL deployed versions to ensure historical entities
   are included. Use this as the standard way to get the current global state."
  []
  (rebuild-global-model))

;; Helper functions for claims-based deployment
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
      (core/fold-version global (:model version) (id/extract version)
                         (latest-version-models version)
                         (gen-activation-filter (id/extract version)))))
  (core/deploy!
    [this version]
    (let [version (-> version decode-version-model (update :model core/normalize-legacy-types))
          {:keys [model]} version]
      (try
        (let [;; Get current global model WITH CLAIMS (or empty if first deployment)
            ;; MUST use last-deployed-model (not fallback) because we need claims
              global (or (try
                           (last-deployed-model)
                           (catch Throwable _
                             (log/warn {:id ::deploy-empty-model-fallback :data {:action :deploying :subject :dataset}}
                                       "Cannot query dataset entities during deploy, starting with empty model")
                             nil))
                         (core/map->ERDModel {:entities {}
                                              :relations {}}))

            ;; 1. Check for entity name conflicts (throws on conflict)
              _ (check-entity-name-conflicts! global model)

            ;; 2. Refuse before any DDL runs if the resulting model would drop a
            ;;    guard — the dropped op fail-closes with no other signal
              _ (rls/assert-guards-compile! (core/preview-model this version) model)

            ;; 3. Call mount to transform database with updated global model
              dataset' (core/mount this (assoc version :model model))

            ;; 4. Prepare version metadata for saving (with original model v1, not global)
              version'' (encode-version-model
                         (assoc version
                                :model (assoc model :version 1)
                                :deployed_on (java.util.Date.)
                                :deployed true))]
        ;; Mark version as deployed in database
          (sync-entity this :dataset/version version'')
        ;; Reload current projection so that you can sync data for new model
          (core/reload this dataset')

        ;; Rebuild global model from ALL deployed versions to compute correct :active flags
        ;; This ensures entities not in latest versions are marked as inactive
          (let [updated-model (assoc (rebuild-global-model) :version 1)]
            (dataset/save-model! updated-model)
            (add-to-deploy-history! this updated-model)
            ;; IAM-audit columns (created_by/modified_by/_on) — reconcile
            ;; against EVERY current entity. Flipping :audit ON for an
            ;; existing entity adds the columns + triggers; transform-audit
            ;; is idempotent on SQLite (PRAGMA probe).
            ;;
            ;; Plug trigger reconcile (entity + relation delta queues)
            ;; is owned by :synthigy/subscriptions.sqlite via
            ;; add-model-watch! on *deployed-model*. When that module is
            ;; started, save-model! above fires the watch synchronously and
            ;; the plug is installed/refreshed. Bare-server config
            ;; doesn't start that module — plug stays uninstalled.
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

      ;; 1. Query the version to get its information
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
            ;; 2. Check if version was deployed
            _ (when-not (:deployed version)
                (throw (ex-info (format "Version %s was never deployed, cannot recall" version-id)
                                {:type :version-not-deployed
                                 (id/key) version-id
                                 :version version})))

            dataset (:dataset version)
            dataset-name (:name dataset)
            dataset-id (id/extract dataset)

          ;; 3. Get all deployed versions for this dataset
            all-deployed-versions (filter :deployed (get-dataset-versions dataset))

          ;; 4. Determine if this is the only deployed version
            only-version? (= 1 (count all-deployed-versions))

          ;; 5. Determine if this is the most recent version
            most-recent-version (first all-deployed-versions)
            is-most-recent? (= version-id (id/extract most-recent-version))

          ;; 6. Get global model with claims
            global (dataset/deployed-model)
            version-uuids #{version-id}

          ;; 7. Find exclusive entities/relations for this version
            exclusive-entities (core/find-exclusive-entities global version-uuids)
            exclusive-relations (core/find-exclusive-relations global version-uuids)]

        (log/info {:id ::version-recalling
                   :data {:dataset dataset-name
                          :version (:name version)
                          :only? only-version?
                          :most-recent? is-most-recent?}}
                  "Recalling version")

      ;; 8. Unmount ONLY exclusive entities/relations
        (when (or (not-empty exclusive-entities) (not-empty exclusive-relations))
          (log/info {:id ::exclusives-pruning
                     :data {:entity-count (count exclusive-entities)
                            :relation-count (count exclusive-relations)}}
                    "Pruning exclusive entities and relations")
          (with-open [con (jdbc/get-connection (:datasource *db*))]
          ;; Drop relation tables
            (doseq [relation exclusive-relations
                    :let [{:keys [from to]
                           :as relation} (core/get-relation global (id/extract relation))
                          table-name (relation->table-name relation)
                          sql (format "drop table if exists \"%s\"" table-name)]]
              (execute-one! con [sql])
              (log/trace {:id ::relation-table-removed
                          :data {:from (:name from) :to (:name to) :table table-name}}
                         "Removed relation table"))
          ;; Drop entity tables
            (doseq [entity exclusive-entities
                    :let [entity (core/get-entity global (id/extract entity))]
                    :when (some? entity)]
              (let [table-name (entity->table-name entity)
                    sql (format "drop table if exists \"%s\"" table-name)]
                (log/trace {:id ::entity-removing
                            :data {:entity (:name entity) :table table-name}}
                           "Removing entity")
                (execute-one! con [sql])))))

      ;; 9. Drop orphaned attribute columns and rebuild model
      ;; After deleting the version, find attributes that exist ONLY in the recalled version
      ;; and drop their database columns
        (let [;; Rebuild global model from remaining deployed versions
              updated-model (rebuild-global-model (remove #(= (id/extract %) version-id) (deployed-versions)))
              recalled-model (core/normalize-legacy-types (:model version))

            ;; Project to find what recalled version would ADD to updated global
            ;; Attributes marked :added? don't exist in updated-model → orphaned
              projection (core/project updated-model recalled-model)

            ;; Create set of exclusive entity ids for efficient lookup
              exclusive-entity-ids (set (map id/extract exclusive-entities))

            ;; Find all orphaned attributes, excluding those from exclusive entities
            ;; (exclusive entities have their entire tables dropped, so no need to drop individual columns)
              orphaned-attrs (for [entity (core/get-entities projection)
                                   :when (not (contains? exclusive-entity-ids (id/extract entity)))
                                   attr (:attributes entity)
                                   :when (core/new-attribute? attr)]
                               {:entity entity
                                :attribute attr})]

        ;; Drop orphaned columns from database
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

        ;; 10. Handle three cases: only version, most recent, or older version
        ;; All cases rebuild from updated-model (from step 9.5)
          (cond
            ;; Case 1: Only deployed version - delete the dataset itself
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
            ;; Case 2: Most recent version - SIMPLIFIED (no redeploy!)
            is-most-recent?
            (do
              (log/info {:id ::version-recalled-rebuilding}
                        "Recalled most recent version, rebuilding from remaining versions")
              (core/mount this {:model updated-model})
              (log/info {:id ::version-record-deleting
                         :data {:dataset dataset-name :version (:name version)}}
                        "Deleting version record")
              (delete-entity this :dataset/version {(id/key) version-id}))
            ;; Case 3: Older version - just reload model without this version
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
    ;; Get all versions for this dataset - query fresh from DB with deployed filter
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
      ;; Recall each version in reverse chronological order (most recent first)
      ;; This ensures proper cleanup and rollback behavior
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
              :as dataset-version} (<-transit (slurp (io/resource "dataset/dataset.json")))]
         (core/reload this dataset-version)
         (query/deploy-schema (query/model->schema model))
         model))
     (let [model (rebuild-global-model)
           ;; Convert claims to :active flags
           ;; Ensure we always have an ERDModel, even if database is empty
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
           ;; Use id/extract to get the version ID in the current provider format (xid or euuid)
           ;; This ensures claims match deployed-version-per-dataset-ids which also uses id/extract
           version-id (id/extract version)
           ;; Pass version-id to the filter so it's included even if not yet visible in DB query
           model'' (core/fold-version global model version-id
                                      (latest-version-models version)
                                      (gen-activation-filter version-id))
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
    ;; USE Global model to reference true DB state
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
     ;; ⚠️ MANUAL RECOVERY ONLY - Read from __deploy_history audit table
     ;; This is NOT part of normal bootstrap flow - reload handles that
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

;;; ============================================================================
;;; Private Helper Functions
;;; ============================================================================

(defn- create-deploy-history!
  "Creates __deploy_history table for tracking model deployments.
   Private helper used during lifecycle setup."
  []
  (execute-one!
   [(format
     "create table __deploy_history (
           \"deployed_on\" timestamp not null default CURRENT_TIMESTAMP,
           \"model\" text)")]))

(defn- add-to-deploy-history!
  "Records a model version in __deploy_history.
   Private helper used during lifecycle setup."
  ([model]
   (execute-one!
    ["insert into __deploy_history (model) values (?)"
     (->transit model)]))
  ([_db model]
   ;; 2-arity version for calls from protocol methods
   (add-to-deploy-history! model)))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
 :synthigy/dataset
 {:depends-on [:synthigy/log :synthigy/database :synthigy.dataset/encryption :synthigy/log.config]
  :headline true
  :doc "ERD model — schema deploy, /data query engine"
  :setup (fn []
           ;; One-time: Create database file, deploy dataset meta-model
           (log/info {:id ::lifecycle-setup-starting :data {:action :setup}} "Setting up dataset meta-model")

           (synthigy.transit/init)
           (log/info {:id ::transit-handlers-initialized} "Transit handlers initialized")

           (let [db-config (sqlite/from-env)
                 db (sqlite/connect db-config)]

             ;; Set as default database
             (alter-var-root #'synthigy.db/*db* (constantly db))

             (log/info {:id ::dataset-tables-initializing
                        :data {:config (pr-str db-config)}}
                       "Initializing dataset tables for SQLite database")

             ;; Create deployment history table
             (create-deploy-history!)
             (log/info {:id ::deploy-history-created :data {:action :created :subject :deploy-history}} "Created __deploy_history")

             (log/info {:id ::dataset-schema-loading}
                       "Loading Dataset schema from dataset/dataset.json")
             (let [dataset-version (<-transit (slurp (io/resource "dataset/dataset.json")))]
               (when-not dataset-version
                 (throw (ex-info "Failed to load dataset.json - transit returned nil"
                                 {:phase :bootstrap
                                  :hint "Check if transit handlers are initialized"})))
               (log/info {:id ::dataset-json-loaded
                          :data {:name (:name dataset-version)}}
                         "Loaded dataset.json")

               (dataset/save-model! nil)
               (core/mount db dataset-version)
               (core/reload db dataset-version))
             (log/info {:id ::dataset-schema-mounted} "Mounted dataset.json schema")

             (log/info {:id ::dataset-schema-deploying :data {:action :deploying :subject :dataset-schema}}
                       "Deploying Dataset schema to history")
             (core/deploy! db (<-transit (slurp (io/resource "dataset/dataset.json"))))

             (log/info {:id ::model-reloading :data {:action :reloading :subject :model}} "Reloading model")
             (core/reload db)

             (log/info {:id ::model-history-adding :data {:action :recording :subject :deployed-model}} "Adding deployed model to history")
             (add-to-deploy-history! (core/get-model db))

             (log/info {:id ::lifecycle-setup-complete :data {:action :setup-complete}} "Dataset system setup complete")))

  :cleanup (fn []
             ;; Clear in-memory caches so setup! can run fresh
             ;; Note: Database file deletion is handled by :synthigy/database cleanup
             (log/info {:id ::lifecycle-cleanup-starting :data {:action :cleanup}} "Clearing dataset caches")
             (dataset/save-model! nil)
             (query/deploy-schema nil)
             (log/info {:id ::lifecycle-cleanup-complete :data {:action :cleanup-complete}} "Dataset cleanup complete"))

  :start (fn []
           ;; Runtime: Initialize delta channels, apply patches, load model
           (log/info {:id ::lifecycle-starting :data {:action :starting}} "Starting dataset system")
           (dataset/start)
           ;; Plug delta-queue triggers + drainer are owned by
           ;; :synthigy/plug. Both server profiles now start it
           ;; (bare-server included), so writes carry the plug trigger
           ;; tax. To skip it — a trigger-free fast write path — start
           ;; :synthigy/dataset alone (no server profile).
           ;; Audit persistence is owned by the observability plug
           ;; (`:synthigy/observability`, DuckDB or ClickHouse), started
           ;; separately by the operator/test fixture — not auto-bound
           ;; here. Without it, deltas still dispatch live to subscribers;
           ;; only `/history` is unavailable.
           (log/info {:id ::lifecycle-started :data {:action :started}} "Dataset system started"))

  :stop (fn []
          ;; Runtime: Close delta channels, clear state
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

;;; ============================================================================
;;; Protocol Extensions - TypeCodec, SQLDialect, SchemaIntrospector
;;; ============================================================================

;;; TypeCodec Protocol - Encode/decode ERD types to PostgreSQL types

;;; ============================================================================
;;; TypeCodec Protocol - Encode/decode ERD types to SQLite types
;;; ============================================================================

(extend-type SQLite
  proto/TypeCodec

  (encode [_db type value]
    (case type
      ;; Scalars - pass through (SQLite handles these natively)
      ("string" "int" "float" "timeperiod" "currency" "uuid" "avatar")
      value

      "timestamp"
      (when (some? value) (ts/->sortable-text value))

      ;; boolean - SQLite uses INTEGER (0/1)
      "boolean"
      (if (nil? value) nil (if value 1 0))

      ;; json - Store as TEXT (JSON string)
      "json"
      (when value
        (->json value))

      ;; encrypted - NOT a TypeCodec type, see postgres.clj's identical case
      ;; for why (needs the active DEK, sealed at the storage boundary).
      "encrypted"
      (throw (ex-info
              (str "\"encrypted\" cannot be encoded via TypeCodec — it seals "
                   "at the storage boundary (synthigy.dataset.encryption/"
                   "seal-cell), never here.")
              {:code :encrypted-not-a-codec-type}))

      ;; hashed - bcrypt hash string
      "hashed"
      (when value
        (hashers/derive value))

      ;; transit - pass through as string (application handles encoding)
      "transit"
      value

      ;; Default: treat as enum type (store as TEXT)
      (when value
        (name value))))

  (decode [_db type value]
    (case type
      ;; Scalars - pass through
      ("string" "int" "float" "timeperiod" "currency" "uuid" "avatar")
      value

      "timestamp"
      (when (some? value) (ts/->date value))

      ;; boolean - Convert INTEGER to boolean
      "boolean"
      (when value
        (cond
          (boolean? value) value
          (number? value) (not (zero? value))
          :else value))

      ;; json - opaque user data: parse with NO transformation. `:keyfn
      ;; identity` keeps object keys as strings; `:valfn nil` disables
      ;; the date/UUID value coercion that would otherwise mangle string
      ;; values resembling dates or UUIDs.
      ;; Tolerant parsing: if JSON parse fails, return raw string (for string→json migrations)
      "json"
      (when value
        (cond
          (or (map? value) (vector? value)) value
          (string? value) (try
                            (<-json value {:keyfn identity :valfn nil})
                            (catch Exception _
                              ;; Not valid JSON - return as plain string
                              ;; This handles data from string→json type conversion
                              value))
          :else value))

      ;; encrypted - NOT a TypeCodec type; see the encode case above.
      "encrypted"
      (throw (ex-info
              (str "\"encrypted\" cannot be decoded via TypeCodec — it unseals "
                   "at the storage boundary (synthigy.dataset.encryption/"
                   "unseal-cell), never here.")
              {:code :encrypted-not-a-codec-type}))

      ;; hashed - not decoded, only verified
      "hashed"
      value

      ;; transit - pass through as string (application handles decoding)
      "transit"
      value

      ;; Default: treat as enum (keyword)
      (when value
        (cond
          (keyword? value) value
          (string? value) (keyword value)
          :else value)))))

;;; ============================================================================
;;; SQLDialect Protocol - SQLite-specific SQL syntax
;;; ============================================================================

(extend-type SQLite
  proto/SQLDialect

  (like-operator [_db case-sensitive?]
    ;; SQLite LIKE is case-insensitive by default
    ;; GLOB is case-sensitive
    (if case-sensitive?
      "GLOB"
      "LIKE"))

  (limit-offset-clause [_db limit offset]
    ;; SQLite requires LIMIT before OFFSET
    ;; Use LIMIT -1 (no limit) when only offset is specified
    (cond
      (and limit offset) (str "LIMIT " limit " OFFSET " offset)
      limit (str "LIMIT " limit)
      offset (str "LIMIT -1 OFFSET " offset)
      :else ""))

  (placeholder-for-type [_db _field-type]
    ;; SQLite: No type casting - always plain ?
    "?")

  (excluded-ref [_db column]
    ;; SQLite: lowercase 'excluded'
    (format "excluded.%s" column))

  (max-bind-params [_db]
    ;; SQLITE_MAX_VARIABLE_NUMBER — 32766 since SQLite 3.32 (2020).
    32766))

;;; ============================================================================
;;; SchemaManager Protocol - SQLite schema introspection and maintenance
;;; ============================================================================

(extend-type SQLite
  proto/SchemaManager

  ;; === Introspection Operations ===

  (get-tables [db]
    ;; Query sqlite_master for tables (excluding system tables)
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
    ;; Use PRAGMA table_info to get column information
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
    ;; SQLite doesn't have native enum types
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

  ;; === Maintenance Operations ===

  (list-tables-like [_db pattern]
    ;; Query sqlite_master for tables matching LIKE pattern
    (execute!
     ["SELECT name as tablename FROM sqlite_master
       WHERE type = 'table'
       AND name LIKE ?
       ORDER BY name"
      pattern]
     :raw))

  (drop-table! [_db table]
    ;; SQLite: DROP TABLE IF EXISTS (no CASCADE keyword)
    (try
      (execute-one! [(str "DROP TABLE IF EXISTS \"" table "\"")])
      true
      (catch Throwable _
        false)))

  (drop-tables-like! [db pattern]
    ;; Find all matching tables and drop them
    (let [tables (proto/list-tables-like db pattern)]
      (doseq [{:keys [tablename]} tables]
        (proto/drop-table! db tablename))
      (count tables)))

  (truncate-table! [_db table]
    ;; SQLite doesn't have TRUNCATE - use DELETE FROM
    (execute-one! [(str "DELETE FROM \"" table "\"")]))

  (list-types-like [_db pattern]
    ;; SQLite doesn't have custom types - return empty vector
    [])

  (drop-type! [_db type-name]
    ;; SQLite doesn't have custom types - no-op
    false)

  (drop-types-like! [_db pattern]
    ;; SQLite doesn't have custom types - return 0
    0))
