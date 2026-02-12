(ns synthigy.dataset.sqlite
  (:require
    [buddy.hashers :as hashers]
    clojure.data
    ;; NOTE: Using synthigy.json/->json instead of clojure.data.json for consistency
    [clojure.java.io :as io]
    clojure.set
    clojure.string
    [clojure.tools.logging :as log]
    [next.jdbc :as jdbc]
   ; [clojure.pprint :refer [pprint]]
    next.jdbc.date-time
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.encryption :refer [encrypt-data decrypt-data]]
    [synthigy.dataset.enhance :as enhance]
    [synthigy.dataset.id :as id]
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
    [synthigy.json :refer [json->data <-json ->json]]
    [synthigy.transit
     :refer [<-transit ->transit]])
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
      (log/warnf "No table-fn registered for reference type: %s" type-name)
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
        (log/warnf "Type conversion warning for %s.%s (%s → %s): %s"
                   (:name entity)
                   (:name attribute)
                   old-type
                   new-type
                   (:warning validation))
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
      ;; Standard types
      (case t
        "currency" "TEXT"
        ("avatar" "string" "hashed") "TEXT"
        "timestamp" "TEXT"
        ("json" "encrypted" "timeperiod") "TEXT"
        "transit" "TEXT"
        "int" "INTEGER"
        t))
    (catch Throwable e
      (throw (ex-info
               (format "Failed to generate DDL for type '%s'" t)
               {:type ::type-ddl-generation-error
                :phase :ddl-generation
                :attribute-type t}
               e)))))

(defn attribute->ddl
  "Function converts attribute to DDL syntax (SQLite - enums stored as TEXT)"
  [entity {n :name
           t :type}]
  (case t
    "enum"
    ;; SQLite doesn't have ENUM types - just use TEXT
    (str (column-name n) " TEXT")
    ;;
    (clojure.string/join
      " "
      (remove
        empty?
        [(column-name n)
         (type->ddl t)]))))

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
                            (log/infof "Generating constraint %s %s" n id)
                            (->
                              (core/get-attribute entity id)
                              :name
                              column-name))
                          ids)))))
                (:unique cs))
          rows (concat pk as' cs')]
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
                :entity-euuid (:euuid entity)
                :table-name (try (entity->table-name entity) (catch Throwable _ nil))
                :attribute-count (count as)
                :has-constraints (some? cs)}
               e)))))

(defn generate-relation-ddl
  "Returns relation table DDL for given model and target relation"
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
             (str "unique(" to-field ")")])
          (clojure.string/join
            ",\n "
            [(str from-field " INTEGER not null references \"" from-table "\"(_eid) on delete cascade")
             (str to-field " INTEGER not null references \"" to-table "\"(_eid) on delete cascade")
             (str "unique(" from-field "," to-field ")")]))))
    (catch Throwable e
      (throw (ex-info
               (format "Failed to generate CREATE TABLE DDL for relation between '%s' and '%s'"
                       (:name f) (:name t))
               {:type ::relation-ddl-generation-error
                :phase :ddl-generation
                :from-entity (:name f)
                :from-entity-euuid (:euuid f)
                :to-entity (:name t)
                :to-entity-euuid (:euuid t)
                :relation-euuid (:euuid relation)
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

;;
(defn attribute-delta->ddl
  [entity
   {:keys [name type]
    {:keys [values]} :configuration
    :as attribute}]
  ;; Don't look at primary key since that is EYWA problem
  (let [new-table (entity->table-name entity)
        oentity (core/suppress entity)
        old-table (entity->table-name oentity)
        diff (core/diff attribute)]
    (if (core/new-attribute? attribute)
      ;;
      (do
        (log/debugf "Adding attribute %s to table %s" name old-table)
        (case type
          "enum"
          ;; SQLite: Enum is just TEXT (no custom types)
          [(str "alter table \"" old-table "\" add column " (column-name name) " TEXT")]
          ;; Add new scalar column to table
          [(str "alter table \"" old-table "\" add column " (column-name name) " " (type->ddl type))]
          ;; TODO - remove mandatory
          #_[(cond-> (str "alter table \"" old-table "\" add column " (column-name name) " " (type->ddl type))
               (= "mandatory" constraint) (str " not null"))]))
      ;;
      (when (or
              (:name (core/diff entity)) ;; If entity name has changed check if there are some enums
              (not-empty (dissoc diff :pk))) ;; If any other change happend follow steps
        (let [{dn :name
               dt :type
               dconfig :configuration} diff
              column (column-name (or dn name))
              old-enum-name (normalize-name (str old-table \space (or dn name)))
              new-enum-name (normalize-name (str new-table \space name))]
          (when dt (check-type-conversion! entity attribute dt type))
          (cond-> []
            ;; Change attribute name
            (and dn (not= (column-name dn) (column-name name)))
            (conj
              (do
                (log/debugf "Renaming table %s column %s -> %s" old-table column (column-name name))
                (format
                  "alter table \"%s\" rename column %s to %s"
                  old-table column (column-name name))))
            ;; SQLite: Enum is TEXT, no custom types needed
            ;; (no-op - enum type changes don't need DDL)
            ;; If type is one of
            ;; Set all current values to null
            ;; NOTE: This is now caught by validation and should throw an error
            (and dt
                 (#{"avatar"} dt)
                 (#{"json"} type))
            (conj
              (do
                (log/debugf "Setting all avatar values to NULL")
                (format "update \"%s\" set %s = NULL" old-table column)))
            ;; Type has changed
            dt
            (as-> statements
                  (log/debugf "Changing table %s column %s type %s -> %s" old-table column dt type)
              ;; SQLite: Dynamic typing! Most type changes don't need DDL.
              ;; Only foreign key constraints (reference types) need updates.
              (if (core/reference-type? type)
                (let [attribute-name (normalize-name (or dn name))
                      constraint-name (str old-table \_ attribute-name "_fkey")
                      refered-table (reference-table type)]
                  (conj
                    (vec statements)
                    (format "alter table \"%s\" drop constraint %s" old-table constraint-name)
                    (format
                      "alter table \"%s\" add constraint \"%s\" foreign key (%s) references \"%s\"(_eid) on delete set null"
                      old-table constraint-name attribute-name refered-table)))
                ;; For all other type changes, no DDL needed - SQLite uses dynamic typing!
                ;; string→json works because decoder is tolerant of plain strings
                (do
                  (log/debugf "SQLite: No DDL needed for type change %s -> %s (dynamic typing)" dt type)
                  statements)))
            ;; SQLite: Enum type changes don't need special handling (just TEXT)
            (= dt "enum")
            identity
            ;; SQLite: Enum name changes don't need special handling (no type system)
            (and (= type "enum") (not= old-enum-name new-enum-name))
            identity
            ;; If type is enum and enum values have changed, update the data
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
                        column (column-name name)]
                    (log/tracef "Diff config\n%s" dconfig)
                    (log/tracef "Old enums: %s" ov)
                    (log/tracef "New enums: %s" nv)
                    ;; SQLite: Enum is just TEXT, only need UPDATE if values changed
                    (if (empty? ov)
                      statements
                      (conj
                        statements
                        (str
                          "update " old-table " set " column " ="
                          "\n  case " column "\n    "
                          (clojure.string/join
                            "\n    "
                            (reduce-kv
                              (fn [r euuid old-name]
                                (if-let [new-name (get nv euuid)]
                                  (conj r (str "when '" old-name "' then '" new-name "'"))
                                  (conj r (str "when '" old-name "' then '" old-name "'"))))
                              (list (str "else " column))
                              ov))
                          "\n  end")))))))))))

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
  [{:keys [attributes]
    :as entity}]
  (assert (core/diff? entity) "This entity is already synced with DB")
  (let [diff (core/diff entity)
        old-entity (core/suppress entity)
        old-table (entity->table-name old-entity)
        old-constraints (get-in old-entity [:configuration :constraints :unique])
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
    (cond-> (reduce into [] attributes')
      ;; Renaming occured
      (:name diff)
      (into
        ;; SQLite: Just rename the table - constraints, indexes, and triggers move automatically
        ;; No need for RENAME CONSTRAINT (doesn't exist in SQLite) or ALTER SEQUENCE (no sequences)
        [(format "alter table \"%s\" rename to \"%s\"" old-table table)])
      ;; If there are some differences in constraints
      (-> diff :configuration :constraints)
      (into
        ;; concatenate constraints
        (let [ncs (-> entity :configuration :constraints :unique)
              ocs old-constraints
              groups (max (count ocs) (count ncs))]
          ;; by reducing
          (when (pos? groups)
            (reduce
              (fn [statements idx]
                (let [o (try (nth ocs idx) (catch Throwable _ nil))
                      n (try (nth ncs idx) (catch Throwable _ nil))
                      constraint (str "_eucg_" idx)
                      new-constraint (format
                                       "alter table \"%s\" add constraint %s unique(%s)"
                                       table (str table constraint)
                                       (clojure.string/join
                                         ","
                                         (map
                                           (fn [id]
                                             (->
                                               (core/get-attribute entity id)
                                               :name
                                               column-name))
                                           n)))
                      drop-constraint (format
                                        "alter table \"%s\" drop constraint %s"
                                        table
                                        (str table constraint))]
                  (cond->
                    statements
                    ;; Add new constraint
                    (empty? o)
                    (conj new-constraint)
                    ;; Delete old constraint group
                    (empty? n)
                    (conj drop-constraint)
                    ;; When constraint has changed
                    (and
                      (every? not-empty [o n])
                      (not= (set o) (set n)))
                    (conj
                      drop-constraint
                      new-constraint))))
              []
              (range groups))))))))

(defn transform-relation
  [tx {:keys [from to]
       :as relation}]
  (try
    (let [diff (core/diff relation)
          _ (log/tracef "Transforming relation\ndiff=%s\nfrom=%s\nto=%s" diff (pr-str from) (pr-str to))
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
          (log/debugf "Renaming relation table %s->%s\n%s" old-name new-name sql)
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (ex-info
                       (format "Failed to rename relation table from '%s' to '%s' (relation: %s → %s)"
                               old-name new-name (:name from) (:name to))
                       {:type ::relation-rename-error
                        :phase :ddl-execution
                        :operation :rename-table
                        :from-entity (:name from)
                        :from-entity-euuid (:euuid from)
                        :to-entity (:name to)
                        :to-entity-euuid (:euuid to)
                        :relation-euuid (:euuid relation)
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
          (log/debugf "Renaming relation table %s -> %s\n%s" old-name new-name sql)
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (ex-info
                       (format "Failed to rename 'to' column in relation table '%s' from '%s' to '%s' (relation: %s → %s)"
                               new-name o n (:name from) (:name to))
                       {:type ::relation-column-rename-error
                        :phase :ddl-execution
                        :operation :rename-to-column
                        :from-entity (:name from)
                        :from-entity-euuid (:euuid from)
                        :to-entity (:name to)
                        :to-entity-euuid (:euuid to)
                        :relation-euuid (:euuid relation)
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
          (log/debugf "Renaming relation %s -> %s\n%s" old-name new-name sql)
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (ex-info
                       (format "Failed to rename 'from' column in relation table '%s' from '%s' to '%s' (relation: %s → %s)"
                               new-name o n (:name from) (:name to))
                       {:type ::relation-column-rename-error
                        :phase :ddl-execution
                        :operation :rename-from-column
                        :from-entity (:name from)
                        :from-entity-euuid (:euuid from)
                        :to-entity (:name to)
                        :to-entity-euuid (:euuid to)
                        :relation-euuid (:euuid relation)
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
                :from-entity-euuid (:euuid from)
                :to-entity (:name to)
                :to-entity-euuid (:euuid to)
                :relation-euuid (:euuid relation)}
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
  (log/debugf "Transforming database\n%s" configuration)
  (let [{ne :new/entities
         re :reactivated/entities
         nr :new/relations
         nrr :new/recursive-relations
         ce :changed/entities
         cr :changed/relations
         crr :changed/recursive-relations} (analyze-projection projection)]
    (log/tracef
      "Transform projection analysis\nNew\n%s\nReactivated\n%s\nChanged\n%s"
      {:new/entities (map :name ne)
       :new/relations (map (juxt :from-label :to-label) nr)
       :new/recursive (map (juxt :from-label :to-label) nrr)}
      {:reactivated/entities (map :name re)}
      {:changed/entities (map :name ce)
       :changed/relations (map (juxt :from-label :to-label) cr)
       :changed/recursive (map (juxt :from-label :to-label) crr)})
    (jdbc/with-transaction [tx ds]
      (when (not-empty re)
        (log/infof "Reactivating %d entities (tables already exist)" (count re))
        (doseq [{:keys [name]} re]
          (log/debugf "Reactivated entity: %s" name)))
      ;; Generate new entities
      (let [entity-priority {:iam/user -100}
            ne (sort-by
                 (fn [{:keys [euuid]}]
                   (get entity-priority euuid 0))
                 ne)]
        (when (not-empty ne)
          (log/infof
            "Generating new entities... %s"
            (clojure.string/join ", " (map :name ne))))
        (doseq [{n :name
                 :as entity} ne
                :let [table-sql (generate-entity-ddl entity)
                      table (entity->table-name entity)]]
          (try
            (log/debugf "Adding entity %s to DB\n%s" n table-sql)
            (try
              (execute-one! tx [table-sql])
              (catch Throwable e
                (throw (ex-info
                         (format "Failed to create table for entity '%s'" n)
                         {:type ::entity-table-creation-error
                          :phase :ddl-execution
                          :operation :create-table
                          :entity-name n
                          :entity-euuid (:euuid entity)
                          :table-name table
                          :sql table-sql}
                         e))))
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
                        :entity-euuid (:euuid entity)
                        :table-name table}
                       e))))))

      (when (not-empty ne)
        (enhance/transform-audit *db* tx ne))

      ;; Change entities
      (when (not-empty ce) (log/info "Checking changed entities..."))
      (doseq [{n :name
               :as entity} ce
              :let [sql (entity-delta->ddl entity)]]
        (log/debugf "Changing entity %s" n)
        (doseq [statement sql]
          (def statement statement)
          (log/debugf "Executing statement %s\n%s" n statement)
          (try
            (execute-one! tx [statement])
            (catch Throwable e
              (throw (ex-info
                       (format "Failed to execute DDL statement for entity '%s'" n)
                       {:type ::entity-change-error
                        :phase :ddl-execution
                        :operation :alter-entity
                        :entity-name n
                        :entity-euuid (:euuid entity)
                        :table-name (entity->table-name entity)
                        :sql statement}
                       e))))))
      ;; Change relations
      (when (not-empty cr)
        (log/info "Checking changed trans entity relations..."))
      (doseq [r cr] (transform-relation tx r))
      ;; Change recursive relation
      (when (not-empty crr)
        (log/info "Checking changed recursive relations..."))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label
               diff :diff
               euuid :euuid} crr
              :let [table (entity->table-name e)
                    ; _ (log/debugf "RECURSIVE RELATION\n%s" diff)
                    previous-column (when-some [label (not-empty (:to-label diff))]
                                      (column-name label))]]
        (when-not (and (some? tl) (not-empty tl))
          (throw
            (ex-info
              (str "Can't change recursive relation for entity " tname " that has empty label")
              {:entity e
               :relation {:euuid euuid
                          :label (:to-label diff)}
               :type ::core/error-recursive-no-label})))
        (if (empty? previous-column)
          (do
            (log/debugf
              "Previous deploy didn't have to label for recursive relation %s at entity %s"
              euuid tname)
            (when tl
              (when-not (column-exists? tx table tl)
                (let [sql (format
                            "alter table %s add %s INTEGER references \"%s\"(_eid) on delete cascade"
                            table tl table)]
                  (log/debug "Creating recursive relation for entity %s\n%s" tname sql)
                  (try
                    (execute-one! tx [sql])
                    (catch Throwable ex
                      (throw (ex-info
                               (format "Failed to create recursive relation column for entity '%s'" tname)
                               {:type ::recursive-relation-creation-error
                                :phase :ddl-execution
                                :operation :add-recursive-column
                                :entity-name tname
                                :entity-euuid (:euuid e)
                                :relation-euuid euuid
                                :table-name table
                                :column-name tl
                                :sql sql}
                               ex))))))))
          ;; Apply changes
          (when diff
            (let [sql (format
                        "alter table %s rename column %s to %s"
                        table previous-column (column-name tl))]
              (log/debugf "Updating recursive relation for entity %s\n%s" tname sql)
              (try
                (execute-one! tx [sql])
                (catch Throwable ex
                  (throw (ex-info
                           (format "Failed to rename recursive relation column for entity '%s'" tname)
                           {:type ::recursive-relation-rename-error
                            :phase :ddl-execution
                            :operation :rename-recursive-column
                            :entity-name tname
                            :entity-euuid (:euuid e)
                            :relation-euuid euuid
                            :table-name table
                            :old-column-name previous-column
                            :new-column-name (column-name tl)
                            :sql sql}
                           ex))))))))
      ;; Generate new relations
      (when (not-empty nr) (log/info "Generating new relations..."))
      (doseq [{{tname :name
                :as to-entity} :to
               {fname :name
                :as from-entity} :from
               :as relation} nr
              :let [sql (generate-relation-ddl projection relation)
                    [from-idx to-idx] (generate-relation-indexes-ddl projection relation)]]
        (try
          (log/debugf "Connecting entities %s <-> %s\n%s" fname tname sql)
          (try
            (execute-one! tx [sql])
            (catch Throwable e
              (throw (ex-info
                       (format "Failed to create relation table between '%s' and '%s'" fname tname)
                       {:type ::relation-creation-error
                        :phase :ddl-execution
                        :operation :create-relation-table
                        :from-entity fname
                        :from-entity-euuid (:euuid from-entity)
                        :to-entity tname
                        :to-entity-euuid (:euuid to-entity)
                        :relation-euuid (:euuid relation)
                        :table-name (relation->table-name relation)
                        :sql sql}
                       e))))
          (when from-idx
            (log/debugf "Creating from indexes for relation: %s <-> %s\n%s" fname tname from-idx)
            (try
              (execute-one! tx [from-idx])
              (catch Throwable e
                (throw (ex-info
                         (format "Failed to create 'from' index for relation between '%s' and '%s'" fname tname)
                         {:type ::relation-index-creation-error
                          :phase :ddl-execution
                          :operation :create-from-index
                          :from-entity fname
                          :from-entity-euuid (:euuid from-entity)
                          :to-entity tname
                          :to-entity-euuid (:euuid to-entity)
                          :relation-euuid (:euuid relation)
                          :table-name (relation->table-name relation)
                          :sql from-idx}
                         e)))))
          (when to-idx
            (log/debugf "Creating to indexes for relation: %s <-> %s\n%s" fname tname to-idx)
            (try
              (execute-one! tx [to-idx])
              (catch Throwable e
                (throw (ex-info
                         (format "Failed to create 'to' index for relation between '%s' and '%s'" fname tname)
                         {:type ::relation-index-creation-error
                          :phase :ddl-execution
                          :operation :create-to-index
                          :from-entity fname
                          :from-entity-euuid (:euuid from-entity)
                          :to-entity tname
                          :to-entity-euuid (:euuid to-entity)
                          :relation-euuid (:euuid relation)
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
                      :from-entity-euuid (:euuid from-entity)
                      :to-entity tname
                      :to-entity-euuid (:euuid to-entity)
                      :relation-euuid (:euuid relation)}
                     e)))))
      ;; Add new recursive relations
      (when (not-empty nrr)
        (log/info "Adding new recursive relations..."))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label
               rel-euuid :euuid} nrr
              :when (not-empty tl)
              :let [table (entity->table-name e)
                    sql (format
                          "alter table %s add %s INTEGER references \"%s\"(_eid) on delete cascade"
                          table (column-name tl) table)]]
        (log/debug "Creating recursive relation for entity %s\n%s" tname sql)
        (try
          (execute-one! tx [sql])
          (catch Throwable ex
            (throw (ex-info
                     (format "Failed to add new recursive relation column for entity '%s'" tname)
                     {:type ::recursive-relation-creation-error
                      :phase :ddl-execution
                      :operation :add-new-recursive-column
                      :entity-name tname
                      :entity-euuid (:euuid e)
                      :relation-euuid rel-euuid
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

(defn deployed-versions
  "Returns all deployed versions ordered by deployed_on (most recently deployed last).
   This ensures that when a version is redeployed, it becomes the 'latest' and
   its model takes precedence in rebuild-global-model."
  []
  (dataset/search-entity
    :dataset/version
    {:deployed {:_boolean :TRUE}
     :_order_by {:deployed_on :asc}}
    {(id/key) nil
     :model nil
     :deployed_on nil
     :dataset [{:selections {(id/key) nil}}]}))

(defn get-version
  [version-id]
  (dataset/get-entity
    :dataset/version
    {(id/key) version-id}
    {(id/key) nil
     :model nil
     :modified_on nil
     :dataset [{:selections {(id/key) nil}}]}))

(comment
  (def version (get-version #uuid "b54595c9-3759-470f-9657-f4b0bc19e294"))
  (dataset/sync-entity :dataset/version {:euuid (:euuid version)
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
                                :args {:deployed {:_boolean :TRUE}
                                       :_order_by {:deployed_on :desc}
                                       :_limit 1}}]})]
    (mapcat :versions datasets)))

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
    {:euuid nil
     :name nil
     :groups [{:selections {:name nil}}]
     :roles [{:selections {:name nil}}]}))

(defn deployed-version-per-dataset-euuids
  []
  (try
    (set (map id/extract (last-deployed-version-per-dataset)))
    (catch Throwable _ #{})))

(defn gen-activation-filter
  "Returns a filter function for activate-model.
   Bootstrap behavior: if no versions deployed, everything is active.
   Optional: include-version-id to include a version being deployed (not yet visible in DB query)."
  ([] (gen-activation-filter nil))
  ([include-version-id]
   (let [deployed-versions (cond-> (deployed-version-per-dataset-euuids)
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
     (core/activate-model final-model (gen-activation-filter)))))

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
              (not= (:euuid found-entity) (:euuid new-entity)))
        (throw
          (ex-info
            (format "Entity name conflict: '%s' already exists with different UUID" (:name new-entity))
            {:type ::entity-name-conflict
             :new-entity new-entity
             :existing-entity (get entities-by-table table)
             :table-name (entity->table-name new-entity)}))))))

(extend-protocol core/DatasetProtocol
  synthigy.db.SQLite
  (core/deploy!
    [this {:keys [model]
           :as version}]
    (try
      (let [;; Get current global model WITH CLAIMS (or empty if first deployment)
            ;; MUST use last-deployed-model (not fallback) because we need claims
            global (or (try
                         (last-deployed-model)
                         (catch Throwable _
                           (log/warn "Cannot query dataset entities during deploy, starting with empty model")
                           nil))
                       (core/map->ERDModel {:entities {}
                                            :relations {}}))

            ;; 1. Check for entity name conflicts (throws on conflict)
            _ (check-entity-name-conflicts! global model)

            ;; 3. Call mount to transform database with updated global model
            dataset' (core/mount this (assoc version :model model))

            ;; 4. Prepare version metadata for saving (with original model v1, not global)
            version'' (assoc
                        version
                        :model (assoc model :version 1)
                        :deployed_on (java.util.Date.)
                        :deployed true)]
        ;; Mark version as deployed in database
        (sync-entity this :dataset/version version'')
        ;; Reload current projection so that you can sync data for new model
        (core/reload this dataset')

        ;; Rebuild global model from ALL deployed versions to compute correct :active flags
        ;; This ensures entities not in latest versions are marked as inactive
        (let [updated-model (assoc (rebuild-global-model) :version 1)]
          (dataset/save-model! updated-model)
          (add-to-deploy-history! this updated-model))

        (log/info "Preparing model for DB")
        version'')
      (catch Throwable e
        (log/errorf e "Couldn't deploy dataset %s@%s"
                    (:name version)
                    (:name (:dataset version)))
        (throw e))))
  ;;
  (core/recall!
    [this version-ref]
    (let [version-id (id/extract version-ref)]
      (assert version-id "Version ID is required")

      ;; 1. Query the version to get its information
      (let [version (dataset/get-entity
                      :dataset/version
                      {(id/key) version-id}
                      {(id/key) nil
                       :name nil
                       :model nil
                       :deployed nil
                       :dataset [{:selections {(id/key) nil
                                               :name nil}}]})

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

        #_(do
            (def global global)
            (def exclusive-entities exclusive-entities)
            (def exclusive-relations exclusive-relations)
            (def dataset dataset)
            (def all-deployed-versions all-deployed-versions)
            (def only-version? only-version?)
            (def version version)
            (def euuid euuid)
            (def most-recent-version (first all-deployed-versions))
            (def is-most-recent? (= euuid (:euuid most-recent-version)))
            (def updated-model (rebuild-global-model (remove #(= (:euuid %) euuid) (deployed-versions)))))

      ; (throw (Exception. "WTF!"))

        (log/infof "Recalling version %s@%s (only=%s, most-recent=%s)"
                   dataset-name (:name version) only-version? is-most-recent?)

      ;; 8. Unmount ONLY exclusive entities/relations
        (when (or (not-empty exclusive-entities) (not-empty exclusive-relations))
          (log/infof "Pruning %d exclusive entities and %d exclusive relations"
                     (count exclusive-entities) (count exclusive-relations))
          (with-open [con (jdbc/get-connection (:datasource *db*))]
          ;; Drop relation tables
            (doseq [relation exclusive-relations
                    :let [{:keys [from to]
                           :as relation} (core/get-relation global (id/extract relation))
                          table-name (relation->table-name relation)
                          sql (format "drop table if exists \"%s\"" table-name)]]
              (execute-one! con [sql])
              (log/tracef "Removed relation table from %s to %s: %s"
                          (:name from) (:name to) table-name))
          ;; Drop entity tables
            (doseq [entity exclusive-entities
                    :let [entity (core/get-entity global (id/extract entity))]
                    :when (some? entity)]
              (let [table-name (entity->table-name entity)
                    sql (format "drop table if exists \"%s\"" table-name)]
                (log/tracef "Removing entity %s: %s" (:name entity) table-name)
                (execute-one! con [sql])))))

      ;; 9. Drop orphaned attribute columns and rebuild model
      ;; After deleting the version, find attributes that exist ONLY in the recalled version
      ;; and drop their database columns
        (let [;; Rebuild global model from remaining deployed versions
              updated-model (rebuild-global-model (remove #(= (id/extract %) version-id) (deployed-versions)))
              recalled-model (:model version)

            ;; Project to find what recalled version would ADD to updated global
            ;; Attributes marked :added? don't exist in updated-model → orphaned
              projection (core/project updated-model recalled-model)

            ;; Create set of exclusive entity euuids for efficient lookup
              exclusive-entity-euuids (set (map :euuid exclusive-entities))

            ;; Find all orphaned attributes, excluding those from exclusive entities
            ;; (exclusive entities have their entire tables dropped, so no need to drop individual columns)
              orphaned-attrs (for [entity (core/get-entities projection)
                                   :when (not (contains? exclusive-entity-euuids (:euuid entity)))
                                   attr (:attributes entity)
                                   :when (core/new-attribute? attr)]
                               {:entity entity
                                :attribute attr})]

        ;; Drop orphaned columns from database
          (when (not-empty orphaned-attrs)
            (log/infof "Dropping %d orphaned attribute columns from recalled version" (count orphaned-attrs))
            (with-open [con (jdbc/get-connection (:datasource *db*))]
              (doseq [orphan orphaned-attrs]
                (let [ddl-statements (orphaned-attribute->drop-ddl orphan)]
                  (doseq [sql ddl-statements]
                    (execute-one! con [sql])
                    (log/debugf "Dropped orphaned column: %s" sql))))))

        ;; 10. Handle three cases: only version, most recent, or older version
        ;; All cases rebuild from updated-model (from step 9.5)
          (cond
            ;; Case 1: Only deployed version - delete the dataset itself
            only-version?
            (do
              (log/infof "Recalled most recent version, rebuilding from remaining versions")
              (core/mount this {:model updated-model})
              (log/infof "Deleting version record %s@%s" dataset-name (:name version))
              (delete-entity this :dataset/version {(id/key) version-id})
              (log/infof "Last version deleted, removing dataset %s" dataset-name)
              (dataset/delete-entity :dataset/dataset {(id/key) dataset-id}))
            ;; Case 2: Most recent version - SIMPLIFIED (no redeploy!)
            is-most-recent?
            (do
              (log/infof "Recalled most recent version, rebuilding from remaining versions")
              (core/mount this {:model updated-model})
              (log/infof "Deleting version record %s@%s" dataset-name (:name version))
              (delete-entity this :dataset/version {(id/key) version-id}))
            ;; Case 3: Older version - just reload model without this version
            :else
            (log/infof "Recalled older version %s@%s, rebuilding model" dataset-name (:name version)))
          (dataset/delete-entity :dataset/version {(id/key) version-id})
          (let [final-model (rebuild-global-model)]
            (dataset/save-model! final-model)
            (query/deploy-schema (query/model->schema final-model))
            (add-to-deploy-history! this final-model))))))
  ;;
  (core/destroy! [this {:keys [name]
                        :as record}]
    (assert (or name (id/extract record)) "Specify dataset name or id!")
    ;; Get all versions for this dataset - query fresh from DB with deployed filter
    (let [dataset-id (id/extract record)
          {all-versions :versions} (dataset/get-entity
                                     :dataset/dataset
                                     {(id/key) dataset-id}
                                     {:name nil
                                      (id/key) nil
                                      :versions [{:args {:_order_by [{:modified_on :asc}]
                                                         :_where {:deployed {:_boolean :TRUE}}}
                                                  :selections {:name nil
                                                               (id/key) nil
                                                               :model nil}}]})]
      (log/infof "Destroying dataset %s: recalling %d versions in reverse order"
                 (or name dataset-id) (count all-versions))
      ;; Recall each version in reverse chronological order (most recent first)
      ;; This ensures proper cleanup and rollback behavior
      (doseq [version all-versions]
        (core/recall! this {(id/key) (id/extract version)}))))
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
    ([this {:keys [model euuid] :as version}]
     (let [global (or
                    (core/get-model this)
                    (core/map->ERDModel nil))
           ;; Use id/extract to get the version ID in the current provider format (xid or euuid)
           ;; This ensures claims match deployed-version-per-dataset-euuids which also uses id/extract
           version-id (id/extract version)
           model' (core/join-models global (core/add-claims model version-id))
           ;; Pass version-id to filter so it's included even if not yet visible in DB query
           model'' (core/activate-model model' (gen-activation-filter version-id))
           schema (query/model->schema model'')]
       (query/deploy-schema schema)
       (dataset/save-model! model'')
       model'')))
  (core/mount
    [this {model :model
           :as version}]
    (log/debugf "Mounting dataset version %s@%s" (:name version) (get-in version [:dataset :name]))
    (let [global (or
                   (core/get-model this)
                   (core/map->ERDModel nil))
          projection (core/project global model)]
      (transform-database (:datasource *db*) projection nil)
      version))
  (core/unmount
    [this {:keys [model]}]
    ;; USE Global model to reference true DB state
    (let [global (core/get-model this)]
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (doseq [relation (core/get-relations model)
                :let [{:keys [from to]
                       :as relation} (core/get-relation global (:euuid relation))
                      sql (format "drop table if exists \"%s\"" (relation->table-name relation))]]
          (try
            (execute-one! con [sql])
            (log/tracef
              "Removing relation from %s to %s\n%s"
              (:name from)
              (:name to)
              sql)
            (delete-entity this :dataset/relation (select-keys relation [:euuid]))
            (catch Throwable e
              (log/errorf e "Couldn't remove table %s" (relation->table-name relation)))))
        (doseq [entity (core/get-entities model)
                :let [{:keys [attributes]
                       :as entity} (core/get-entity global (:euuid entity))]
                :when (some? entity)]
          (try
            (let [sql (format "drop table if exists \"%s\"" (entity->table-name entity))]
              (log/tracef "Removing entity %s\n%s" (:name entity) sql)
              (execute-one! con [sql])
              (delete-entity this :dataset/entity (select-keys entity [:euuid]))
              (doseq [{:keys [euuid]} attributes]
                (delete-entity this :dataset/entity-attribute euuid)))
            (catch Throwable e
              (log/errorf e "Couldn't remove table %s" (entity->table-name entity)))))))
    (core/reload this))
  (core/get-last-deployed
    ([this]
     ;; ⚠️ MANUAL RECOVERY ONLY - Read from __deploy_history audit table
     ;; This is NOT part of normal bootstrap flow - reload handles that
     (log/warn "get-last-deployed called - this is for manual recovery only")
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
  {:depends-on [:synthigy/database :synthigy.dataset/encryption]
   :setup (fn []
           ;; One-time: Create database file, deploy dataset meta-model
            (log/info "[DATASET] Setting up dataset meta-model...")

            (synthigy.transit/init)
            (log/info "[DATASET] Transit handlers initialized")

            (let [db-config (sqlite/from-env)
                  db (sqlite/connect db-config)]

             ;; Set as default database
              (alter-var-root #'synthigy.db/*db* (constantly db))

              (log/infof "Initializing dataset tables for SQLite database\n%s"
                         (pr-str db-config))

             ;; Create deployment history table
              (create-deploy-history!)
              (log/info "Created __deploy_history")

              (log/info "Loading Dataset schema from dataset/dataset.json")
              (let [dataset-version (<-transit (slurp (io/resource "dataset/dataset.json")))]
                (when-not dataset-version
                  (throw (ex-info "Failed to load dataset.json - transit returned nil"
                                  {:phase :bootstrap
                                   :hint "Check if transit handlers are initialized"})))
                (log/infof "[DATASET] Loaded dataset.json: %s" (:name dataset-version))

                (dataset/save-model! nil)
                (core/mount db dataset-version)
                (core/reload db dataset-version))
              (log/info "Mounted dataset.json schema")

              (log/info "Deploying Dataset schema to history")
              (core/deploy! db (<-transit (slurp (io/resource "dataset/dataset.json"))))

              (log/info "Reloading model")
              (core/reload db)

              (log/info "Adding deployed model to history")
              (add-to-deploy-history! (core/get-model db))

              (log/info "Dataset system setup complete")))

   :cleanup (fn []
             ;; Clear in-memory caches so setup! can run fresh
             ;; Note: Database file deletion is handled by :synthigy/database cleanup
              (log/info "[DATASET] Clearing dataset caches...")
              (dataset/save-model! nil)
              (query/deploy-schema nil)
              (log/info "[DATASET] Dataset cleanup complete"))

   :start (fn []
           ;; Runtime: Initialize delta channels, apply patches, load model
            (log/info "[DATASET] Starting dataset system...")
            (dataset/start)
            (log/info "[DATASET] Dataset system started"))

   :stop (fn []
          ;; Runtime: Close delta channels, clear state
           (log/info "[DATASET] Stopping dataset system...")
           (dataset/stop)
           (log/info "[DATASET] Dataset system stopped"))})

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

;;; Database Healthcheck
;;; Verifies that the deployed dataset schema matches the actual PostgreSQL database state

(defn get-pg-tables
  "Returns a set of all table names in the public schema (excluding system tables)"
  []
  (let [system-tables #{"__deks" "__version_history"}]
    (set (remove system-tables
                 (map :table_name
                      (execute! ["SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                AND table_name NOT LIKE '__deploy%'"]))))))

(defn get-pg-columns
  "Returns a set of column names for a given table"
  [table-name]
  (set (map (comp keyword :column_name)
            (execute! ["SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?"
                       table-name]))))

(defn get-pg-enum-types
  "Returns a set of all enum type names in the database"
  []
  (set (map :typname
            (execute! ["SELECT typname FROM pg_type
            WHERE typtype = 'e'"]))))

(defn healthcheck-database
  "Checks if deployed dataset schema matches actual PostgreSQL database state.

   Returns a structured map with namespaced keywords:
   - :healthy? - true if everything matches
   - :expected/* - expected counts from schema
   - :actual/* - actual counts from database
   - :missing/* - {:count N :items [...]} missing items (in schema but not in DB)
   - :extra/* - {:count N :items [...]} extra items (in DB but not in schema)"
  []
  (let [schema (query/deployed-schema)

        ;; Get all actual tables from PostgreSQL
        pg-tables (get-pg-tables)

        ;; Get all actual enum types from PostgreSQL
        pg-enums (get-pg-enum-types)

        ;; Get all expected entity tables from schema
        expected-entity-tables (set (keep (fn [[_ entity-data]]
                                            (:table entity-data))
                                          schema))

        ;; Get all expected relation tables from schema
        expected-relation-tables (set (mapcat
                                        (fn [[_ entity-data]]
                                          (map :table (vals (:relations entity-data))))
                                        schema))

        ;; Get all expected enum types from schema
        expected-enums (set (mapcat
                              (fn [[_ entity-data]]
                                (keep (fn [field]
                                        (:enum/name field))
                                      (vals (:fields entity-data))))
                              schema))

        expected-tables (clojure.set/union expected-entity-tables expected-relation-tables)

        ;; Find missing tables
        missing-tables (clojure.set/difference expected-tables pg-tables)

        ;; Find extra tables (could be inactive/historical)
        extra-tables (clojure.set/difference pg-tables expected-tables)

        ;; Find missing enum types
        missing-enums (clojure.set/difference expected-enums pg-enums)

        ;; Check columns for each entity table
        missing-columns
        (for [[entity-uuid entity-data] schema
              :let [table-name (:table entity-data)
                    expected-cols (set (concat
                                         [:_eid :euuid :modified_by :modified_on]
                                         (map :key (vals (:fields entity-data)))))
                    actual-cols (when (pg-tables table-name)
                                  (get-pg-columns table-name))
                    missing (when actual-cols
                              (clojure.set/difference expected-cols actual-cols))]
              :when (and missing (not-empty missing))]
          {:entity (:name entity-data)
           :entity-uuid entity-uuid
           :table table-name
           :missing-columns missing})

        ;; Check relation tables
        missing-relations (filter #(not (pg-tables %)) expected-relation-tables)]

    {:healthy? (and (empty? missing-tables)
                    (empty? missing-columns)
                    (empty? missing-relations)
                    (empty? missing-enums))

     ;; Expected counts from schema
     :expected/tables (count expected-tables)
     :expected/entities (count expected-entity-tables)
     :expected/relations (count expected-relation-tables)
     :expected/enums (count expected-enums)

     ;; Actual counts from database
     :actual/tables (count pg-tables)
     :actual/enums (count pg-enums)

     ;; Missing items (expected but not found in DB)
     :missing/tables {:count (count missing-tables)
                      :items missing-tables}
     :missing/columns {:count (count missing-columns)
                       :items missing-columns}
     :missing/relations {:count (count missing-relations)
                         :items missing-relations}
     :missing/enums {:count (count missing-enums)
                     :items missing-enums}

     ;; Extra items (in DB but not expected)
     :extra/tables {:count (count extra-tables)
                    :items extra-tables}}))

(defn healthcheck-report
  "Generates a healthcheck report with human-readable message.
   Returns a map with:
   - :healthy? - boolean
   - :message - human-readable report string
   - :data - full healthcheck data"
  ([] (healthcheck-report (healthcheck-database)))
  ([health]
   (let [lines (atom [])
         println! (fn [& args]
                    (swap! lines conj (apply str args)))

         missing-count (+ (get-in health [:missing/tables :count])
                          (get-in health [:missing/columns :count])
                          (get-in health [:missing/relations :count])
                          (get-in health [:missing/enums :count]))]

     ;; Build report message
     (println! "=== Database Health Check ===")
     (println! "")
     (if (:healthy? health)
       (println! "✅ Database is HEALTHY - schema matches deployed model")
       (println! "❌ Database has ISSUES - schema does not match deployed model"))

     ;; Expected vs Actual Summary
     (println! "")
     (println! "Expected (from schema):")
     (println! (format "  Tables:    %d (%d entities + %d relations)"
                       (:expected/tables health)
                       (:expected/entities health)
                       (:expected/relations health)))
     (println! (format "  Enums:     %d" (:expected/enums health)))

     (println! "")
     (println! "Actual (in database):")
     (println! (format "  Tables:    %d" (:actual/tables health)))
     (println! (format "  Enums:     %d" (:actual/enums health)))

     ;; Missing items
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

     (when (pos? (get-in health [:missing/enums :count]))
       (println! "")
       (println! (format "  Enums (%d):" (get-in health [:missing/enums :count])))
       (doseq [enum-type (get-in health [:missing/enums :items])]
         (println! (format "    - %s" enum-type))))

     ;; Extra items (informational)
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

     ;; Return structured response
     {:healthy? (:healthy? health)
      :message (clojure.string/join "\n" @lines)
      :data health})))

(defn healthcheck-print
  "Convenience function that prints the healthcheck report to stdout"
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
      (fn [{{euuid :euuid} :dataset}]
        (nil? euuid))
      deployed))
  (doseq [version without-dataset]
    (println (dataset/delete-entity :dataset/version {:euuid (:euuid version)})))
  (def versions
    (dataset/get-entity
      :dataset/dataset
      {:euuid #uuid "743a9023-3980-405a-8340-3527d91064d8"}
      {:versions [{:selections
                   {:euuid nil
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

      ;; timestamp - SQLite stores as TEXT (ISO-8601 string)
      "timestamp"
      (when value
        (cond
          (string? value) value  ; Assume already formatted
          (instance? java.time.Instant value) (str value)
          (instance? java.util.Date value) (str (.toInstant ^java.util.Date value))
          (instance? java.time.LocalDateTime value) (str value)
          :else (str value)))

      ;; boolean - SQLite uses INTEGER (0/1)
      "boolean"
      (if (nil? value) nil (if value 1 0))

      ;; json - Store as TEXT (JSON string)
      "json"
      (when value
        (->json value))

      ;; encrypted - Store as TEXT (JSON string with encrypted payload)
      "encrypted"
      (when value
        (->json (encrypt-data value)))

      ;; hashed - bcrypt hash string
      "hashed"
      (when value
        (hashers/derive value))

      ;; transit - transit-encoded string
      "transit"
      (when value
        (->transit value))

      ;; Default: treat as enum type (store as TEXT)
      (when value
        (name value))))

  (decode [_db type value]
    (case type
      ;; Scalars - pass through
      ("string" "int" "float" "timestamp" "timeperiod" "currency" "uuid" "avatar")
      value

      ;; boolean - Convert INTEGER to boolean
      "boolean"
      (when value
        (cond
          (boolean? value) value
          (number? value) (not (zero? value))
          :else value))

      ;; json - Parse JSON string, preserving string keys for JSON semantics
      ;; Tolerant parsing: if JSON parse fails, return raw string (for string→json migrations)
      "json"
      (when value
        (cond
          (or (map? value) (vector? value)) value
          (string? value) (try
                            (<-json value {:keyfn identity})
                            (catch Exception _
                              ;; Not valid JSON - return as plain string
                              ;; This handles data from string→json type conversion
                              value))
          :else value))

      ;; encrypted - Parse and decrypt, preserving string keys
      "encrypted"
      (when value
        (let [encrypted-data (cond
                               (or (map? value) (vector? value)) value
                               (string? value) (<-json value {:keyfn identity})
                               :else value)]
          (decrypt-data encrypted-data)))

      ;; hashed - not decoded, only verified
      "hashed"
      value

      ;; transit - decode from transit string
      "transit"
      (when value
        (<-transit value))

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

  (distinct-clause [_db fields]
    ;; SQLite doesn't support DISTINCT ON - just return DISTINCT
    (when (seq fields)
      "DISTINCT"))

  (order-by-clause [_db field direction nulls-position]
    ;; SQLite doesn't support NULLS FIRST/LAST syntax
    ;; Use CASE WHEN to control null ordering
    (let [field-name (name field)
          dir-str (case direction
                    :asc "ASC"
                    :desc "DESC"
                    "ASC")]
      (if nulls-position
        (case nulls-position
          :first (str "CASE WHEN " field-name " IS NULL THEN 0 ELSE 1 END, " field-name " " dir-str)
          :last (str "CASE WHEN " field-name " IS NULL THEN 1 ELSE 0 END, " field-name " " dir-str))
        (str field-name " " dir-str))))

  (upsert-clause [_db table conflict-columns update-columns]
    ;; SQLite 3.24+ supports ON CONFLICT ... DO UPDATE
    (let [conflict-cols (clojure.string/join ", " (map #(str "\"" % "\"") conflict-columns))
          updates (clojure.string/join ", "
                                       (map #(str "\"" % "\" = excluded.\"" % "\"")
                                            update-columns))]
      (str "ON CONFLICT (" conflict-cols ") DO UPDATE SET " updates)))

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

  (returning-clause [_db columns]
    ;; SQLite 3.35+ supports RETURNING
    (when (seq columns)
      (str "RETURNING " (clojure.string/join ", " (map #(str "\"" (name %) "\"") columns)))))

  (identifier-quote [_db identifier]
    ;; SQLite uses double quotes for identifiers
    (str "\"" identifier "\""))

  ;; === Array and Tree Operations ===

  (array-contains-clause [_db array-col element]
    ;; SQLite: Use string path with INSTR for cycle detection
    (format "INSTR(%s, ',' || %s || ',') > 0" array-col element))

  (array-init-expr [_db element]
    ;; SQLite: Initialize path as comma-delimited string
    (format "',' || %s || ','" element))

  (array-append-expr [_db array-col element]
    ;; SQLite: Append to comma-delimited string path
    (format "%s || %s || ','" array-col element))

  (cycle-literal [_db is-cycle?]
    ;; SQLite: Use 0/1 for boolean
    (if is-cycle? "1" "0"))

  (in-clause [_db column count]
    ;; SQLite: Use IN (?, ?, ...) with expanded placeholders
    {:sql (format "%s IN (%s)" column (clojure.string/join "," (repeat count "?")))
     :params-fn identity})

  (placeholder-for-type [_db _field-type]
    ;; SQLite: No type casting - always plain ?
    "?")

  (excluded-ref [_db column]
    ;; SQLite: lowercase 'excluded'
    (format "excluded.%s" column)))

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
