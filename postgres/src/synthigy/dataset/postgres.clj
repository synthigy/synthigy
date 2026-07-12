(ns synthigy.dataset.postgres
  (:require
   [buddy.hashers :as hashers]
   clojure.data
    ;; JSON via synthigy.json (jsonista)
   [clojure.java.io :as io]
   clojure.set
   [clojure.string :as str]
   [next.jdbc :as jdbc]
    ; [clojure.pprint :refer [pprint]]
   next.jdbc.date-time
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.encryption :refer [encrypt-data decrypt-data]]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   synthigy.dataset.operations
   ;; required for its load-time patch registrations (no direct refs)
   synthigy.dataset.postgres.patch
   [synthigy.dataset.postgres.xid :as xid]
   synthigy.dataset.projection
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
   [synthigy.db
    :refer [*db*
            sync-entity
            delete-entity]]
   [synthigy.db.sql :refer [execute! execute-one!]]
   [synthigy.json :refer [<-json ->json]]
   [synthigy.log :as log]
   ;; loaded for its lifecycle registration — :synthigy/dataset depends on
   ;; :synthigy/log.config so the data plane always carries DB-backed log routing
   [synthigy.log.config]
   [synthigy.transit
    :refer [<-transit ->transit]])
  (:import
   [org.postgresql.util PGobject]
   [synthigy.db Postgres]))

(defn reference-table
  "Returns table name for a reference type, or nil if not registered.
   Logs a warning when reference type is not registered."
  [type-name]
  (if-let [table-fn (core/reference-table-fn type-name)]
    (table-fn)
    (do
      (log/warn {:id ::reference-type-unregistered :data {:type-name type-name}}
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
  "Converts type to DDL syntax"
  [t]
  (try
    (if (core/reference-type? t)
      ;; Dynamic reference type - look up table from *reference-mapping*
      (if-let [table (reference-table t)]
        (str "bigint references \"" table "\"(_eid) on delete set null")
        (throw (ex-info
                (format "Cannot generate DDL for unregistered reference type '%s'" t)
                {:type ::unregistered-reference-type
                 :phase :ddl-generation
                 :reference-type t})))
      ;; Standard types
      (case t
        "currency" "jsonb"
        ("avatar" "string" "hashed") "text"
        "timestamp" "timestamp"
        ("json" "encrypted" "timeperiod") "jsonb"
        "transit" "text"
        "int" "bigint"
        t))
    (catch Throwable e
      (throw (ex-info
              (format "Failed to generate DDL for type '%s'" t)
              {:type ::type-ddl-generation-error
               :phase :ddl-generation
               :attribute-type t}
              e)))))

(defn attribute->ddl
  "Function converts attribute to DDL syntax"
  [entity {n :name
           t :type}]
  (case t
    "enum"
    (let [table (entity->table-name entity)
          enum-name (normalize-name (str table \space n))]
      (str (column-name n) \space enum-name))
    ;;
    (clojure.string/join
     " "
     (remove
      empty?
      [(column-name n)
       (type->ddl t)]))))

(defn normalized-enum-value [value]
  (clojure.string/replace value #"-|\s" "_"))

(defn generate-enum-type-ddl
  "Idempotent CREATE TYPE. Postgres has no `CREATE TYPE IF NOT EXISTS`, so
   guard with a `pg_type` existence check inside a DO block — dataset setup
   can then re-run safely after a partial failure."
  [enum-name values]
  (format
   "do $$ begin if not exists (select 1 from pg_type where typname='%s') then create type \"%s\" as enum%s; end if; end $$;"
   enum-name
   enum-name
   (when (not-empty values)
     (str " ("
          (clojure.string/join
           ", "
           (map (comp #(str \' % \') normalized-enum-value :name) values))
          \)))))

(defn generate-entity-attribute-enum-ddl
  [table {n :name
          t :type
          {values :values} :configuration}]
  (let [enum-name (normalize-name (str table \space n))]
    (when (= t "enum")
      (generate-enum-type-ddl enum-name values))))

(defn generate-entity-enums-ddl
  [{as :attributes
    :as entity}]
  (let [table (entity->table-name entity)]
    (clojure.string/join
     ";"
     (keep (partial generate-entity-attribute-enum-ddl table) as))))

(defn drop-entity-enums-ddl
  [{as :attributes
    :as entity}]
  (let [table (entity->table-name entity)]
    (clojure.string/join
     ";"
     (keep
      (fn [{n :name
            t :type}]
        (when (= t "enum")
          (str "drop type if exists " (normalize-name (str table \space n)))))
      as))))

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
              ;; Fresh installs are xid-only. euuid exists solely as a legacy
              ;; EYWA source format, brought in via import — never freshly
              ;; created — so we only ever emit the xid id column here.
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
          ;; Pre-create the timestamp audit columns at CREATE TABLE time.
          ;; Timestamps are the bare-mode-safe subset that the dataset
          ;; module can guarantee regardless of which AuditEnhancement
          ;; impl is active (timestamp-only default vs principal-aware
          ;; when IAM is loaded). The `_by` columns are added via the
          ;; ALTER TABLE path by the principal-aware enhancer — they're
          ;; not pre-created here because they depend on IAM state.
          ;; Pre-creating timestamps avoids the post-creation read race
          ;; documented previously (readers needing the columns before
          ;; the enhancer's reconcile pass runs).
          audit-cols (cond-> []
                       (core/audit-modified? entity)
                       (conj "modified_on timestamp not null default localtimestamp")
                       (core/audit-created? entity)
                       (conj "created_on timestamp not null default localtimestamp"))
          rows (concat pk as' audit-cols cs')]
      (format
       "create table if not exists \"%s\" (\n  %s\n)"
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
  "Returns relation table DDL for given model and target relation.
   Includes from_xid / to_xid TEXT columns for the relation-audit
   substrate (denormalized at link time; survives entity cascade
   deletes for audit purposes). Populated by the BEFORE INSERT trigger
   installed in transform-relation-audit."
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
        ;; Create table
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
  "Returns relation table DDL for given model and target relation"
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
        ;;
        ;; NEW: Detect reactivated entities (exist but were inactive)
        reactivated-entities
        (filter
         (fn [entity]
           (and (not (core/added? entity))
                (false? (:active (core/suppress entity))) ; Was inactive
                (true? (:active entity)))) ; Now active
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
        (log/debug {:id ::attribute-added :data {:attribute name :table old-table}}
                   "Adding attribute to table")
        (case type
          "enum"
          ;; Create new enum type and add it to table
          [(generate-entity-attribute-enum-ddl new-table attribute)
           (format "alter table \"%s\" add column if not exists %s %s"
                   old-table (column-name name)
                   (normalize-name (str new-table \space name)))]
          ;; Add new scalar column to table
          [(str "alter table \"" old-table "\" add column if not exists " (column-name name) " " (type->ddl type))]
          ;; TODO - remove mandatory
          #_[(cond-> (str "alter table \"" old-table "\" add column if not exists " (column-name name) " " (type->ddl type))
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
               (log/debug {:id ::column-renamed
                           :data {:table old-table :from column :to (column-name name)}}
                          "Renaming table column")
               (format
                "alter table \"%s\" rename column %s to %s"
                old-table column (column-name name))))
            ;; If attribute type has changed to enum
            ;; than create new enum type with defined values
            (and (= type "enum") (some? dt))
            (conj (generate-enum-type-ddl new-enum-name values))
            ;; Type has changed so you better cast to target type
            dt
            (as-> statements
                  (log/debug {:id ::column-type-changed
                              :data {:table old-table :column column :from-type dt :to-type type}}
                             "Changing table column type")
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
                ;; Proceed with acceptable type alter
                (conj
                 statements
                 (cond->
                  (format
                   "alter table \"%s\" alter column %s type %s"
                   old-table column
                   (case type
                     "enum" new-enum-name
                     (type->ddl type)))
                   (= "int" type) (str " using(trim(" column ")::integer)")
                   (= "float" type) (str " using(trim(" column ")::float)")
                   (= "string" type) (str " using(" column "::text)")
                    ; (= "json" type) (str " using(" column "::jsonb)")
                    ;; String to JSON conversion:
                    ;; - JSON objects/arrays: cast directly
                    ;; - Plain strings: wrap in quotes to make valid JSON string
                    ;; - NULL: keep as NULL
                   (= "json" type) (str " using\n case"
                                        "\n  when " column " is null then null"
                                        "\n  when " column " ~ '^\\s*[\\{\\[]' then " column "::jsonb"
                                        "\n  else to_jsonb(" column ")"
                                        "\nend")
                   (= "transit" type) (str " using(" column "::text)")
                   (= "avatar" type) (str " using(" column "::text)")
                   (= "encrypted" type) (str " using(" column "::text)")
                   (= "hashed" type) (str " using(" column "::text)")
                   (= "boolean" type) (str " using(trim(" column ")::boolean)")
                   (= "enum" type) (str " using(" column ")::" old-enum-name)))))
            ;; If attribute was previously enum and now it is not enum
            ;; than delete enum type
            (= dt "enum")
            (conj (format "drop type \"%s\"" old-enum-name))
            ;; If enum name has changed than apply changes
            (and (= type "enum") (not= old-enum-name new-enum-name))
            (conj (format "alter type %s rename to %s" old-enum-name new-enum-name))
            ;; If type is enum and it hasn't changed
            ;; but enum configuration has changed, than create statements to
            ;; compensate changes  
            (and (= type "enum") (nil? dt) (not-empty dconfig))
            (as-> statements
                  (let [[ov nv sv] (clojure.data/diff
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
                        column (column-name name)]

                    ;; (def values values)
                    ;; (def dconfig dconfig)
                    ; alter type my_enum rename to my_enum__;
                    ; -- create the new enum
                    ; create type my_enum as enum ('value1', 'value2', 'value3');

                    ; -- alter all you enum columns
                    ; alter table my_table
                    ; alter column my_column type my_enum using my_column::text::my_enum;

                    ; -- drop the old enum
                    ; drop type my_enum__;
                    (log/trace {:id ::enum-diff
                                :data {:config dconfig :old-enums ov :new-enums nv}}
                               "Diffing enum values")

                    ;; (def ov ov)
                    ;; (def nv nv)
                    ;; (def sv sv)
                    ;; (def column column)
                    ;; (def old-table old-table)
                    ;; (throw (Exception. "HI"))
                    ;; DDL sequence for enum changes:
                    ;; 1. Set removed values to NULL (before type change)
                    ;; 2. Rename old type to type__
                    ;; 3. Create new type with all values
                    ;; 4. ALTER column to new type (converts old values via text cast)
                    ;; 5. UPDATE renamed values (now column is new type, can accept new names)
                    ;; 6. Drop old type
                    (conj
                     statements
                     (when-let [values-to-remove (reduce-kv
                                                  (fn [r k v]
                                                    (if (contains? nv k)
                                                      r
                                                      (conj r v)))
                                                  nil
                                                  ov)]
                       (format
                        "update \"%s\" set %s = null where %s::text in (%s)"
                        old-table column column
                        (str/join
                         ", "
                         (map
                          #(str \' % \')  ; Compare text to text, not text to enum
                          values-to-remove))))
                     (format "alter type %s rename to %s__" new-enum-name new-enum-name)
                     (format "create type %s as enum (%s)" new-enum-name (clojure.string/join "," (map #(str \' % \') (remove empty? (vals (merge nv sv))))))
                      ;; ALTER column to new type - handle renames in the USING clause
                      ;; If there are renames, use CASE to map old names to new names during conversion
                     (if (empty? ov)
                        ;; No renames - simple cast
                       (format "alter table \"%s\" alter column %s type %s using %s::text::%s"
                               old-table column new-enum-name column new-enum-name)
                        ;; Has renames - use CASE in USING clause to map old names to new
                       (str
                        "alter table \"" old-table "\" alter column " column " type " new-enum-name
                        " using (case " column "::text"
                        (clojure.string/join
                         ""
                         (reduce-kv
                          (fn [r id old-name]
                            (let [new-name (get nv id)]
                              (if (and new-name old-name (not= old-name new-name))
                                (conj r (str " when '" old-name "' then '" new-name "'::" new-enum-name))
                                r)))
                          []
                          ov))
                        " else " column "::text::" new-enum-name " end)"))
                     (format "drop type %s__" new-enum-name))))))))))

(defn orphaned-attribute->drop-ddl
  "Generates DROP COLUMN DDL for an orphaned attribute.
   An orphaned attribute exists only in a recalled/destroyed version,
   not in any remaining deployed versions.

   The DROP uses CASCADE because audit-tracked entities have a per-row
   trigger function whose body references every user column by name
   (see synthigy.dataset.postgres.audit/entity-emit-fn-ddl). Without
   CASCADE, Postgres refuses the drop with
   `cannot drop column X of table Y because other objects depend on it`.
   The cascaded trigger function gets reinstalled by
   `core/recall!`'s post-drop reconcile pass against the rebuilt model.

   Returns a vector of DDL statements to execute."
  [{:keys [entity attribute]}]
  (let [table-name (entity->table-name entity)
        column-name (column-name (:name attribute))
        attr-type (:type attribute)]
    (cond-> [(format "ALTER TABLE \"%s\" DROP COLUMN IF EXISTS %s CASCADE"
                     table-name column-name)]
      ;; If enum type, also drop the enum type definition
      (= "enum" attr-type)
      (conj (format "DROP TYPE IF EXISTS \"%s\" CASCADE"
                    (normalize-name (str table-name " " (:name attribute))))))))

;; 1. Change attributes by calling attribute-delta->ddl
;; 2. Rename table if needed
;; 3. Change constraints
(defn entity-delta->ddl
  [{:keys [attributes]
    :as entity}]
  ;; (def attributes attributes)
  (assert (core/diff? entity) "This entity is already synced with DB")
  (let [diff (core/diff entity)
        old-entity (core/suppress entity)
        old-table (entity->table-name old-entity)
        old-constraints (get-in old-entity [:configuration :constraints :unique])
        ;; Index-stable, active-aware unique groups. A composite unique key
        ;; whose attribute was deactivated nils out at its position → the diff
        ;; below emits DROP for that index. (Source of truth = core; same view
        ;; the schema uses.) Positions are preserved so `_eucg_<idx>` names stay
        ;; stable across the old/new comparison.
        new-unique (core/unique-constraints-indexed entity)
        old-unique (core/unique-constraints-indexed old-entity)
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
    ;; Filter out nil and empty string statements from attribute DDL
    (cond-> (->> attributes'
                 (reduce into [])
                 (remove #(or (nil? %) (and (string? %) (str/blank? %))))
                 vec)
      ;; Renaming occured
      (:name diff)
      (into
       (cond->
        [(format "alter table \"%s\" rename to \"%s\"" old-table table)
         (format "alter table \"%s\" rename constraint \"%s_pkey\" to \"%s_pkey\"" table old-table table)
         (format "alter table \"%s\" rename constraint \"%s_%s_key\" to \"%s_%s_key\"" table old-table (id/field) table (id/field))
         (format "alter sequence \"%s__eid_seq\" rename to \"%s__eid_seq\"" old-table table)]
          ;;
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
      ;; Reconcile unique constraints when the constraints config changes. Pure
      ;; attribute-deactivation is handled by the orphan-column DROP ... CASCADE
      ;; (which takes the dependent unique constraint with it); when the config
      ;; itself changes we rebuild from the ACTIVE-aware groups so a combo that
      ;; lost an attribute is dropped (all-or-nothing) and never references a
      ;; dead column.
      (-> diff :configuration :constraints)
      (into
        ;; concatenate constraints
       (let [ncs new-unique
             ocs old-unique
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
                                     ;; IF EXISTS: a deactivated attribute's
                                     ;; column may have already been dropped with
                                     ;; CASCADE, taking this constraint with it.
                                     "alter table \"%s\" drop constraint if exists %s"
                                     table
                                     (str table constraint))]
                (cond->
                 statements
                    ;; Add new constraint (only when there's a real new group —
                    ;; an index where both old and new are empty/nil, e.g. a
                    ;; group dropped in a prior deploy, must emit nothing).
                  (and (empty? o) (seq n))
                  (conj new-constraint)
                    ;; Delete old constraint group
                  (and (seq o) (empty? n))
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
          _ (log/trace {:id ::relation-transforming
                        :data {:diff diff :from (pr-str from) :to (pr-str to)}}
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
        ;; Rename table
        (let [sql (format
                   "alter table \"%s\" rename to \"%s\""
                   old-name new-name)]
          (log/debug {:id ::relation-renamed
                      :data {:from-table old-name :to-table new-name :sql sql}}
                     "Renaming relation table")
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
                       :from-entity-id (id/extract from)
                       :to-entity (:name to)
                       :to-entity-id (id/extract to)
                       :relation-id (id/extract relation)
                       :old-table-name old-name
                       :new-table-name new-name
                       :sql sql}
                      e)))))
        ;; Rename indexes (fidx and tidx) to match new table name
        (let [old-fidx (str old-name "_fidx")
              new-fidx (str new-name "_fidx")
              old-tidx (str old-name "_tidx")
              new-tidx (str new-name "_tidx")]
          ;; Rename from-index
          (let [sql (format "alter index if exists \"%s\" rename to \"%s\"" old-fidx new-fidx)]
            (log/debug {:id ::relation-from-index-renamed
                        :data {:from-index old-fidx :to-index new-fidx :sql sql}}
                       "Renaming relation from-index")
            (try
              (execute-one! tx [sql])
              (catch Throwable e
                (throw (ex-info
                        (format "Failed to rename relation from-index from '%s' to '%s'" old-fidx new-fidx)
                        {:type ::relation-index-rename-error
                         :phase :ddl-execution
                         :operation :rename-from-index
                         :relation-id (id/extract relation)
                         :old-index-name old-fidx
                         :new-index-name new-fidx
                         :sql sql}
                        e)))))
          ;; Rename to-index
          (let [sql (format "alter index if exists \"%s\" rename to \"%s\"" old-tidx new-tidx)]
            (log/debug {:id ::relation-to-index-renamed
                        :data {:from-index old-tidx :to-index new-tidx :sql sql}}
                       "Renaming relation to-index")
            (try
              (execute-one! tx [sql])
              (catch Throwable e
                (throw (ex-info
                        (format "Failed to rename relation to-index from '%s' to '%s'" old-tidx new-tidx)
                        {:type ::relation-index-rename-error
                         :phase :ddl-execution
                         :operation :rename-to-index
                         :relation-id (id/extract relation)
                         :old-index-name old-tidx
                         :new-index-name new-tidx
                         :sql sql}
                        e)))))))
      ;; when to name has changed than change table column
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
              (throw (ex-info
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
                   "alter table \"%s\" rename column %s to %s"
                   new-name o n)]
          (log/debug {:id ::relation-from-column-renamed
                      :data {:table new-name :from-column o :to-column n :sql sql}}
                     "Renaming relation from-column")
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
  [tx table column]
  (let [sql (str
             "select exists ("
             "select 1 from pg_attribute where attrelid = '" table "'::regclass"
             " and attname = '" column "'"
             " and attnum > 0"
             " and not attisdropped"
             ");")]
    (:exists (execute-one! tx [sql]))))

;; 1. Generate new entities by creating tables
;;  - Create new types if needed by enum attributes
;; 2. Add audit attributes if present (modified_by,modified_on)
;; 3. Check if model has changed attributes
;;  - If so try to resolve changes by calling entity-delta->ddl
;; 4. Check if model has changed relations
;;  - If so try to resolve changes by calling transform-relation
;; 5. Generate new relations by connecting entities
(defn transform-database [ds projection configuration]
  ; (throw (Exception. "HHH"))
  (log/debug {:id ::database-transforming :data {:config configuration}}
             "Transforming database")
  (let [{ne :new/entities
         re :reactivated/entities ; NEW
         nr :new/relations
         nrr :new/recursive-relations
         ce :changed/entities
         cr :changed/relations
         crr :changed/recursive-relations} (analyze-projection projection)]
    ; (-> projection analyze-projection :changed/entities first core/diff)
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
      ;; Handle reactivated entities (table exists, just mark active)
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
          (log/info {:id ::entities-generating
                     :data {:entities (mapv :name ne)}}
                    "Generating new entities"))
        (doseq [{n :name
                 :as entity} ne
                :let [table-sql (generate-entity-ddl entity)
                      enum-sql (generate-entity-enums-ddl entity)
                      table (entity->table-name entity)]]
          (try
            (when (not-empty enum-sql)
              (log/debug {:id ::entity-enums-adding
                          :data {:entity n :sql enum-sql}}
                         "Adding entity enums")
              (try
                (execute! tx [enum-sql])
                (catch Throwable e
                  (throw (ex-info
                          (format "Failed to create enum types for entity '%s'" n)
                          {:type ::entity-enum-creation-error
                           :phase :ddl-execution
                           :operation :create-enum-types
                           :entity-name n
                           :entity-id (id/extract entity)
                           :table-name table
                           :sql enum-sql}
                          e)))))
            (log/debug {:id ::entity-added
                        :data {:entity n :sql table-sql}}
                       "Adding entity to DB")
            (try
              (execute-one! tx [table-sql])
              ;; Install the xid immutability trigger — the id is immutable at
              ;; the DB layer (any UPDATE to the xid column is silently reverted).
              (execute! tx [(xid/postgres-xid-trigger table)])
              (log/debug {:id ::id-immutability-trigger-created
                          :data {:table table :id-key (id/key)}}
                         "Created ID immutability trigger")
              (catch Throwable e
                (throw (ex-info
                        (format "Failed to create table for entity '%s'" n)
                        {:type ::entity-table-creation-error
                         :phase :ddl-execution
                         :operation :create-table
                         :entity-name n
                         :entity-id (id/extract entity)
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
                       :entity-id (:id/extract entity)
                       :table-name table}
                      e))))))

      ;; Apply audit columns to new entity tables (via protocol)
      (when (not-empty ne)
        (enhance/transform-audit *db* tx ne))

      ;; Change entities
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
          ;; (def sql sql)
          (try
            (execute-one! tx [statement])
            (catch Throwable e
              (throw (ex-info
                      (format "Failed to execute DDL statement for entity '%s'" n)
                      {:type ::entity-change-error
                       :phase :ddl-execution
                       :operation :alter-entity
                       :entity-name n
                       :entity-id (id/extract entity)
                       :table-name (entity->table-name entity)
                       :sql statement}
                      e))))))
      ;; Change relations
      (when (not-empty cr)
        (log/info {:id ::changed-relations-checking :data {:count (count cr)}}
                  "Checking changed trans entity relations"))
      (doseq [r cr] (transform-relation tx r))
      ;; Change recursive relation
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
                    ; _ (log/debug "RECURSIVE RELATION" diff)
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
                      (throw (ex-info
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
              (log/debug {:id ::recursive-relation-updating
                          :data {:entity tname :sql sql}}
                         "Updating recursive relation for entity")
              (try
                (execute-one! tx [sql])
                (catch Throwable ex
                  (throw (ex-info
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
              (throw (ex-info
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
                (throw (ex-info
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
                (throw (ex-info
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
            (throw (ex-info
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
  "Gets all versions for a dataset by its id or :name, ordered by deployed_on desc"
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

(defn- decode-version-model
  "Decode the :model field from transit string to ERDModel record."
  [version]
  (update version :model (fn [m] (if (string? m) (<-transit m) m))))

(defn- encode-version-model
  "Encode the :model field to transit string for DB storage."
  [version]
  (update version :model (fn [m] (if (string? m) m (->transit m)))))

(defn deployed-versions
  []
  ;; Order by `deployed_on` — it is part of the meta-schema and the correct
  ;; cursor for deployment recency. (`modified_on` is a row-stamp audit
  ;; column the meta-model entities don't carry, so it must not be used.)
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

(defn deployed-version-per-dataset-ids
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
   (let [;; Build global model with ALL claims
         ;; join-models handles active flags via "last deployment wins"
         initial-model (core/map->ERDModel {:entities {}
                                            :relations {}})
         final-model
         (reduce
          (fn [global {:keys [model]
                       :as version}]
            (core/join-models
             global
             (core/add-claims
              (dataset/adapt-model-to-provider model)
              (id/extract version))))
          initial-model
          all-versions)]
     (core/activate-model final-model (gen-activation-filter)))))

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

;;; ============================================================================
;;; Private Helper Functions
;;; ============================================================================

(defn- create-deploy-history!
  "Creates __deploy_history table for tracking model deployments.
   Private helper used during lifecycle setup. Idempotent."
  []
  (execute-one!
   [(format
     "create table if not exists __deploy_history (
           \"deployed_on\" timestamp not null default localtimestamp,
           \"model\" text)")]))

(defn- add-to-deploy-history!
  "Records a model version in __deploy_history.
   Private helper used during lifecycle setup."
  [model]
  (execute-one!
   ["insert into __deploy_history (model) values (?)"
    (->transit model)]))

(extend-protocol core/DatasetProtocol
  synthigy.db.Postgres
  (core/deploy! [this version]
    (let [version (decode-version-model version)
          {:keys [model]} version]
      (try
        (let [;; Get current global model WITH CLAIMS (or empty if first deployment)
            ;; MUST use last-deployed-model (not fallback) because we need claims
              model (dataset/adapt-model-to-provider model)
            ;; Thread the adapted model back into the version so mount/DDL see
            ;; the SAME id form as the active provider. Callers that load via
            ;; `<-resource` already adapt up-front (making this a no-op), but a
            ;; direct cross-format deploy (e.g. an xid-authored model under a
            ;; euuid provider) would otherwise mount the raw model and feed
            ;; xid constraint refs into a euuid-keyed `get-attribute`.
              version (assoc version :model model)
              global (or (dataset/deployed-model)
                         (core/map->ERDModel {:entities {}
                                              :relations {}}))

            ;; 1. Check for entity name conflicts (throws on conflict)
              _ (check-entity-name-conflicts! global model)

            ;; 3. Call mount to transform database with updated global model
              _ (core/mount this version)

            ;; 4. Prepare version metadata for saving (with original model v1, not global)
              version'' (encode-version-model
                         (assoc version
                                :model (assoc model :version 1)
                                :deployed_on (java.util.Date.)
                                :deployed true))]
        ;; Mark version as deployed in database
          (sync-entity this :dataset/version version'')
        ;; Reload current projection so that you can sync data for new model
          (core/reload this version'')

        ;; Rebuild global model from ALL deployed versions to compute correct :active flags
        ;; This ensures entities not in latest versions are marked as inactive
          (let [updated-model (assoc (rebuild-global-model) :version 1)]
            (dataset/save-model! updated-model)
            (add-to-deploy-history! updated-model)
            ;; IAM-audit columns (created_by/modified_by/_on) — reconcile
            ;; against EVERY current entity. Flipping :audit ON for an existing
            ;; entity now adds the columns + triggers instead of silently
            ;; no-op'ing. transform-audit is idempotent on both backends.
            ;;
            ;; Substrate trigger reconcile (entity + relation delta queues) is
            ;; owned by :synthigy/subscriptions.postgres (via add-model-watch!
            ;; on *deployed-model*). When that module is started, save-model!
            ;; above fires the watch synchronously and the substrate is
            ;; installed/refreshed against the new schema. Bare-server config
            ;; doesn't start that module — substrate stays uninstalled — which
            ;; is the recovered fast-path (no audit-substrate trigger tax).
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
  ;;
  (core/recall!
    [this record]
    (assert (id/extract record) "Version id is required")

    ;; 1. Query the version to get its information.
    ;;    `:model` comes back from the DB as a transit string — must be
    ;;    decoded before it's handed to anything model-shaped (project,
    ;;    get-entities, etc.), otherwise dispatch fails with
    ;;    "No implementation of method :get-entities for class String".
    ;;    The sqlite impl does this at the equivalent point; postgres
    ;;    was missing the call entirely. Mirror sqlite/dataset.clj:1248.
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
          ;; 2. Check if version was deployed
          _ (when-not (:deployed version)
              (throw (ex-info (format "Version %s was never deployed, cannot recall" id)
                              {:type :version-not-deployed
                               (id/key) id
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
          is-most-recent? (= id (id/extract most-recent-version))

          ;; 6. Get global model with claims
          global (dataset/deployed-model)
          version-uuids #{id}

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
          (def id id)
          (def version-uuids #{id})
          (def most-recent-version (first all-deployed-versions))
          (def is-most-recent? (= id (id/extract most-recent-version)))
          (def updated-model (rebuild-global-model (remove #(= (id/extract%) id) (deployed-versions)))))

      ; (throw (Exception. "WTF!"))

      (log/info {:id ::version-recalling
                 :data {:dataset dataset-name
                        :version (:name version)
                        :only? only-version?
                        :most-recent? is-most-recent?}}
                "Recalling version")

      ;; 8. Unmount ONLY exclusive entities/relations
      (when (or (not-empty exclusive-entities) (not-empty exclusive-relations))
        (log/info {:id ::exclusive-pruning
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
                       "Removed relation table")
            (dataset/delete-entity :dataset/relation relation))
          ;; Drop entity tables
          (doseq [entity exclusive-entities
                  :let [entity (core/get-entity global (id/extract entity))]
                  :when (some? entity)]
            (let [table-name (entity->table-name entity)
                  sql (format "drop table if exists \"%s\"" table-name)
                  enums-sql (drop-entity-enums-ddl entity)]
              (log/trace {:id ::entity-removing
                          :data {:entity (:name entity) :table table-name}}
                         "Removing entity")
              (execute-one! con [sql])
              ;; Delete enum types for this entity
              (when (not-empty enums-sql)
                (log/trace {:id ::entity-enums-removing
                            :data {:entity (:name entity) :sql enums-sql}}
                           "Removing entity enum types")
                (execute-one! con [enums-sql]))
              (dataset/delete-entity :dataset/relation entity)))))

      ;; 9. Drop orphaned attribute columns and rebuild model
      ;; After deleting the version, find attributes that exist ONLY in the recalled version
      ;; and drop their database columns
      (let [;; Rebuild global model from remaining deployed versions
            updated-model (rebuild-global-model (remove #(= (id/extract %) id) (deployed-versions)))
            recalled-model (:model version)

            ;; Project to find what recalled version would ADD to updated global
            ;; Attributes marked :added? don't exist in updated-model → orphaned
            projection (core/project updated-model recalled-model)

            ;; Create set of exclusive entity euuids for efficient lookup
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
            (delete-entity this :dataset/version {(id/key) id})
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
            (delete-entity this :dataset/version {(id/key) id}))
            ;; Case 3: Older version - just reload model without this version
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
          ;; Reinstall IAM-audit columns against the post-recall schema.
          ;; The orphan-column DROPs above used CASCADE, which removed
          ;; per-row trigger fns that referenced the dropped columns.
          ;; transform-audit is idempotent.
          ;;
          ;; Substrate delta-queue triggers are owned by
          ;; :synthigy/subscriptions.postgres via add-model-watch! on
          ;; *deployed-model*. save-model! above fires the watch
          ;; synchronously when the module is started; bare-server
          ;; skips both.
          (let [all-entities (vec (core/get-entities final-model))]
            (enhance/transform-audit *db* (:datasource *db*) all-entities))))))
  ;;
  (core/destroy! [this {:keys [name]
                        :as record}]
    (assert (or name (id/extract record)) "Specify dataset name or id!")
    ;; Get all versions for this dataset
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
              :as dataset-version} (dataset/<-resource "dataset/dataset.json")]
         (core/reload this dataset-version)
         (query/deploy-schema (query/model->schema model))
         model))
     (let [model (rebuild-global-model)
           ;; Convert claims to :active flags
           ;; Ensure we always have an ERDModel, even if database is empty
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
           ;; Adapt to the active id form (idempotent) before building the
           ;; schema + saving — same rationale as `mount`.
           model (dataset/adapt-model-to-provider model)
           record (assoc record :model model)
           ;; Use id/extract to get the version ID in the current provider format
           ;; This ensures claims match deployed-version-per-dataset-ids which also uses id/extract
           version-id (id/extract record)
           global (or
                   (core/get-model this)
                   (core/map->ERDModel nil))
           model' (core/join-models global (core/add-claims model version-id))
           ;; Pass version-id to filter so it's included even if not yet visible in DB query
           model'' (core/activate-model model' (gen-activation-filter version-id))
           schema (query/model->schema model'')]
       (query/deploy-schema schema)
       (dataset/save-model! model'')
       model'')))
  (core/mount
    [this version]
    (let [{model :model :as version} (decode-version-model version)
          ;; Adapt to the active id form so the projection + DDL never feed a
          ;; foreign-format model (e.g. xid-authored resource under a euuid
          ;; provider) into the id-keyed `get-attribute`. Idempotent: an
          ;; already-active model detects as a no-op transform.
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
            (let [sql (format "drop table if exists \"%s\"" (entity->table-name entity))
                  enums-sql (drop-entity-enums-ddl entity)]
              (log/trace {:id ::unmount-entity-removing
                          :data {:entity (:name entity) :sql sql}}
                         "Removing entity")
              (execute-one! con [sql])
              (delete-entity this :dataset/entity {(id/key) (id/extract entity)})
              (doseq [attribute attributes]
                (delete-entity this :dataset/entity-attribute (id/extract attribute)))
              (when (not-empty enums-sql)
                (log/trace {:id ::unmount-entity-enums-removing
                            :data {:entity (:name entity) :sql enums-sql}}
                           "Removing entity enum types")
                (execute-one! con [enums-sql])))
            (catch Throwable e
              (log/error! {:id ::entity-table-remove-failed
                           :data {:table (entity->table-name entity)}}
                          e))))))
    (core/reload this))
  (core/get-last-deployed
    ([this]
     ;; ⚠️ MANUAL RECOVERY ONLY - Read from __deploy_history audit table
     ;; This is NOT part of normal bootstrap flow - reload handles that
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

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
 :synthigy/dataset
 {:depends-on [:synthigy/log :synthigy/database :synthigy.dataset/encryption :synthigy/log.config]
  :doc "ERD model — schema deploy, /data query engine"
  :setup (fn []
            ;; Note: Database and *db* are already set up by :synthigy/database
            ;; Check for legacy BEFORE detect-and-set-provider! (which stores format)
           (let [legacy? (xid/legacy-deployment?)]

              ;; Detect and set ID provider BEFORE loading model
             (xid/detect-and-set-provider!)

             (if legacy?
                ;; Legacy Postgres: just load existing schema, don't redeploy
               (do
                 (log/info {:id ::legacy-setup-starting}
                           "Legacy Postgres deployment detected, loading existing schema")
                 ;; Forward-reconcile meta-schema columns newer versions expect,
                 ;; BEFORE any version-detection reads them.
                 (xid/reconcile-legacy-meta-columns!)
                 (create-deploy-history!)  ;; Ensure exists for old deployments
                 (core/reload *db*)
                 (log/info {:id ::legacy-setup-complete}
                           "Legacy setup complete"))

                ;; Fresh Postgres: full setup with deploy
               (do
                 (log/info {:id ::fresh-setup-starting}
                           "Fresh Postgres, deploying dataset meta-model")
                 (dataset/save-model! (core/map->ERDModel nil))
                 (log/info {:id ::fresh-setup-database
                            :data {:database (dissoc *db* :password :datasource)}}
                           "Using database")

                  ;; Create deployment history table
                 (create-deploy-history!)
                 (log/info {:id ::deploy-history-created :data {:action :created :subject :deploy-history}}
                           "Created __deploy_history")

                  ;; Load and deploy the Dataset schema (meta-model)
                 (log/info {:id ::dataset-schema-loading :data {:action :loading :subject :dataset-schema}}
                           "Loading Dataset schema from dataset/dataset.json")
                 (let [model (<-transit (slurp (io/resource "dataset/dataset.json")))]
                   (core/mount *db* model)
                   (core/reload *db* model))
                 (log/info {:id ::dataset-schema-mounted :data {:action :mounted :subject :dataset-schema}}
                           "Mounted dataset.json schema")

                 (log/info {:id ::dataset-schema-deploying :data {:action :deploying :subject :dataset-schema}}
                           "Deploying Dataset schema to history")
                 (core/deploy! *db* (<-transit (slurp (io/resource "dataset/dataset.json"))))

                 (log/info {:id ::model-reloading :data {:action :reloading :subject :model}}
                           "Reloading model")
                 (core/reload *db*)

                 (log/info {:id ::deployed-model-recording :data {:action :recording :subject :deployed-model}}
                           "Adding deployed model to history")
                 (add-to-deploy-history! (core/get-model *db*))

                 (log/info {:id ::setup-complete :data {:action :setup-complete :subject :dataset}}
                           "Setup complete")))))

  :cleanup (fn []
              ;; One-time: Clean up dataset tables (database cleanup handled by :synthigy/database)
             (log/info {:id ::cleanup-starting}
                       "Cleaning up dataset tables")
              ;; TODO: Drop dataset-specific tables if needed
             (log/info {:id ::cleanup-complete}
                       "Dataset cleanup complete"))

  :start (fn []
           ;; Runtime: Initialize delta channels, apply patches, load model
           (log/info {:id ::lifecycle-starting :data {:action :starting}}
                     "Starting dataset system")
            ;; Detect and set ID provider BEFORE loading model
           (xid/detect-and-set-provider!)
           ;; First-class legacy awareness: Synthigy is xid-native. A legacy
           ;; EYWA (euuid) layout is loudly flagged and requires an EXPLICIT,
           ;; operator-run one-time import — we never auto-migrate on boot.
           (when (xid/legacy-eywa-layout?)
             (log/warn {:id ::legacy-eywa-detected
                        :data {:action :not-initialized :subject :id-format :format "euuid"}}
                       (str "LEGACY EYWA (euuid) DATABASE DETECTED. Synthigy is xid-native — "
                            "run (synthigy.dataset.postgres.xid/import-eywa->synthigy!) to convert "
                            "this database to the Synthigy xid layout. Until then the system "
                            "runs against the legacy euuid layout.")))
           ;; NB: the per-boot reconcile band-aid is gone from the normal start.
           ;; The importer (`import-eywa->synthigy!`) owns the proactive reconcile
           ;; (step 0); a native/modern boot already has `deployed_on`/`active`,
           ;; and an un-imported old-EYWA boot self-heals via the
           ;; `latest-deployed-version` nil-catch + the model patch that adds them.
           (dataset/start)
           ;; Substrate delta-queue triggers + drainer are owned by
           ;; :synthigy/subscriptions.postgres. Bare-server doesn't start
           ;; that module → no substrate → no audit-substrate trigger tax
           ;; on writes. Full-server / test fixtures must explicitly start
           ;; :synthigy/subscriptions.postgres for SSE + audit to capture.
           ;; Audit persistence is owned by the observability substrate
           ;; (`:synthigy/observability`, DuckDB or ClickHouse), started
           ;; separately by the operator/test fixture — not auto-bound
           ;; here. Without it, deltas still dispatch live to subscribers;
           ;; only `/history` is unavailable.
           (log/info {:id ::lifecycle-started :data {:action :started}}
                     "Dataset system started"))

  :stop (fn []
          ;; Runtime: Close delta channels, clear state
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
                                        [:_eid (id/key) :modified_by :modified_on]
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

;;; ============================================================================
;;; Protocol Extensions - TypeCodec, SQLDialect, SchemaIntrospector
;;; ============================================================================

;;; TypeCodec Protocol - Encode/decode ERD types to PostgreSQL types

(extend-type Postgres
  proto/TypeCodec

  (encode [_db type value]
    (case type
      ;; Scalars - pass through (PostgreSQL native types)
      ("string" "int" "float" "boolean" "timeperiod" "currency" "uuid" "avatar")
      value

      ;; timestamp - ensure proper Java temporal type
      "timestamp"
      (when value
        (cond
          (instance? java.time.Instant value) value
          (instance? java.util.Date value) value
          (instance? java.time.LocalDateTime value) value
          (string? value) (try
                            (java.time.Instant/parse value)
                            (catch Exception _
                              ;; Try parsing as LocalDateTime if ISO instant fails
                              (java.time.LocalDateTime/parse value)))
          :else value))

      ;; json - PGobject jsonb
      "json"
      (when value
        (doto (PGobject.)
          (.setType "jsonb")
          (.setValue (->json value))))

      ;; encrypted - PGobject jsonb with encrypted payload
      "encrypted"
      (when value
        (doto (PGobject.)
          (.setType "jsonb")
          (.setValue (->json (encrypt-data value)))))

      ;; hashed - bcrypt hash
      "hashed"
      (when value
        (hashers/derive value))

      ;; transit - pass through as string (application handles encoding)
      "transit"
      value

      ;; Default: treat as enum type
      ;; Use plain string instead of PGobject to avoid JDBC type OID caching issues
      ;; when enum types are dropped and recreated. PostgreSQL will implicitly cast
      ;; the string to the enum type.
      (when value
        (name value))))

  (decode [_db type value]
    (case type
      ;; Scalars - pass through
      ("string" "int" "float" "boolean" "timestamp" "timeperiod" "currency" "uuid" "avatar")
      value

      ;; json - opaque user data: decode with NO transformation. `:keyfn
      ;; identity` keeps object keys as strings (a key like "Key with
      ;; space" must survive verbatim), and `:valfn nil` disables
      ;; `synthigy-val-fn`, which would otherwise coerce string values
      ;; that happen to look like dates/UUIDs into Date/UUID objects.
      "json"
      (when value
        (cond
          (or (map? value) (vector? value)) value
          (instance? PGobject value) (<-json (.getValue ^PGobject value) {:keyfn identity :valfn nil})
          (string? value) (<-json value {:keyfn identity :valfn nil})
          :else value))

      ;; encrypted - decode and decrypt with keyword keys (decrypt-data expects :data :dek :iv)
      "encrypted"
      (when value
        (let [encrypted-data (cond
                               (or (map? value) (vector? value)) value
                               (instance? PGobject value) (<-json (.getValue ^PGobject value) {:keyfn keyword})
                               (string? value) (<-json value {:keyfn keyword})
                               :else value)]
          (decrypt-data encrypted-data)))

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
          (instance? PGobject value) (keyword (.getValue ^PGobject value))
          (string? value) (keyword value)
          :else value)))))

;;; SQLDialect Protocol - PostgreSQL-specific SQL syntax

(extend-type Postgres
  proto/SQLDialect

  (like-operator [_db case-sensitive?]
    (if case-sensitive? "LIKE" "ILIKE"))

  (limit-offset-clause [_db limit offset]
    ;; PostgreSQL allows OFFSET without LIMIT, but we use LIMIT ALL for clarity
    (cond
      (and limit offset) (str "LIMIT " limit " OFFSET " offset)
      limit (str "LIMIT " limit)
      offset (str "OFFSET " offset)
      :else nil))

  (placeholder-for-type [_db field-type]
    ;; PostgreSQL: Add ::type cast for enum types
    (if (and field-type
             (not (#{"boolean" "string" "int" "float"
                     "timestamp" "timeperiod" "currency"
                     "uuid" "json" "transit" "encrypted"
                     "hashed" "avatar"} field-type)))
      (str "?::" field-type)
      "?"))

  (excluded-ref [_db column]
    ;; PostgreSQL: uppercase 'EXCLUDED'
    (format "EXCLUDED.%s" column))

  (max-bind-params [_db]
    ;; PostgreSQL wire protocol counts Bind parameters in an Int16.
    65535))

;;; SchemaManager Protocol - PostgreSQL schema introspection and maintenance

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

  ;; === Maintenance Operations ===

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

;;; ============================================================================
;;; Default audit-fields impl — timestamp-only AuditEnhancement
;;;
;;; The dataset backend always ships these semantics: entities with
;;; `:audit :actions #{:created :modified}` get `modified_on` /
;;; `created_on` columns plus an UPDATE trigger that keeps `modified_on`
;;; fresh. No user attribution, no FK. Works in any mode.
;;;
;;; `:synthigy/audit` (IAM-coupled lifecycle module) overlays this on
;;; :start with the principal-aware variant (`_by` columns + write-time
;;; fill from `*principal*`), and re-installs these defaults on :stop.
;;; `install-default-audit-enhancement!` is the public entry point that
;;; module calls.
;;; ============================================================================

(def ^:private ^:const update-modified-on-fn
  "CREATE OR REPLACE FUNCTION update_modified_on()
   RETURNS TRIGGER AS $$
   BEGIN
     NEW.modified_on = localtimestamp;
     RETURN NEW;
   END;
   $$ LANGUAGE plpgsql")

(def ^:private ^:const preserve-created-on-fn
  "CREATE OR REPLACE FUNCTION preserve_created_on()
   RETURNS TRIGGER AS $$
   BEGIN
     IF TG_OP = 'UPDATE' THEN
       NEW.created_on = OLD.created_on;
     END IF;
     RETURN NEW;
   END;
   $$ LANGUAGE plpgsql")

(defn- install-modified-on-trigger! [tx entity]
  (let [table        (entity->table-name entity)
        trigger-name (str "update_" (normalize-name (:name entity)) "_modified_on")]
    (execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS modified_on timestamp NOT NULL DEFAULT localtimestamp" table)])
    (execute! tx [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger-name table)])
    (execute! tx [(format "CREATE TRIGGER %s BEFORE UPDATE ON \"%s\" FOR EACH ROW EXECUTE FUNCTION update_modified_on()"
                          trigger-name table)])))

(defn- install-created-on-trigger! [tx entity]
  ;; Trigger name aligns with the principal-aware variant so a mode swap
  ;; cleanly drops + recreates the trigger pointing at the right function.
  (let [table        (entity->table-name entity)
        trigger-name (str "preserve_" (normalize-name (:name entity)) "_created_audit")]
    (execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS created_on timestamp NOT NULL DEFAULT localtimestamp" table)])
    (execute! tx [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger-name table)])
    (execute! tx [(format "CREATE TRIGGER %s BEFORE UPDATE ON \"%s\" FOR EACH ROW EXECUTE FUNCTION preserve_created_on()"
                          trigger-name table)])))

(defn transform-audit-timestamp-only
  "Per-entity DDL for audit timestamps + UPDATE triggers. Idempotent.
   Skips entities without `[:configuration :audit :actions]`."
  [_db tx entities]
  (let [audited (filter (fn [e] (or (core/audit-modified? e) (core/audit-created? e))) entities)]
    (when (seq audited)
      (log/info {:id ::transform-audit-starting
                 :data {:action :starting :subject :audit-fields :mode :timestamp-only
                        :entities (mapv :name audited)}}
                "Installing timestamp audit columns + triggers")
      (execute! tx [update-modified-on-fn])
      (execute! tx [preserve-created-on-fn])
      (doseq [entity audited]
        (when (core/audit-modified? entity) (install-modified-on-trigger! tx entity))
        (when (core/audit-created? entity)  (install-created-on-trigger! tx entity))
        (log/debug {:id ::table-audited
                    :data {:table (entity->table-name entity)
                           :modified (core/audit-modified? entity)
                           :created (core/audit-created? entity)
                           :mode :timestamp-only}}
                   "Audited table")))))

(defn augment-schema-timestamp-only
  [_db entity]
  (let [modified? (core/audit-modified? entity)
        created?  (core/audit-created? entity)]
    (cond-> {:fields {}}
      modified? (assoc-in [:fields :modified_on] {:key :modified_on :type "timestamp"})
      created?  (assoc-in [:fields :created_on]  {:key :created_on  :type "timestamp"}))))

(defn install-default-audit-enhancement!
  "Bind `AuditEnhancement` for Postgres to the timestamp-only impl.
   Called at ns load to establish the default, and called again by
   `synthigy.iam.audit/uninstall-principal-aware!` to revert from the
   IAM overlay."
  []
  (extend-protocol enhance/AuditEnhancement
    synthigy.db.Postgres
    (transform-audit [db tx entities] (transform-audit-timestamp-only db tx entities))
    (augment-schema  [db entity]      (augment-schema-timestamp-only db entity))
    (audit           [_ _ data _]     data)))

(install-default-audit-enhancement!)
