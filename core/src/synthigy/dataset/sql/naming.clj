(ns synthigy.dataset.sql.naming
  "Database-agnostic SQL naming conventions.

  Handles:
  - UUID hashing (base-36 encoding for compact table names)
  - Name normalization (lowercase, replace special chars)
  - Column and table name generation
  - Relation table naming

  Note: 63-character limit is used (PostgreSQL/MySQL standard)"
  (:require
    [clojure.string :as str]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]))

(def alphabet (.toCharArray "0123456789abcdefghijklmnopqrstuvwxyz"))
(def MASK 35)

(defn hash-uuid
  "Encodes a UUID into a compact base-36 string for use in table names"
  [uuid]
  (let [^long lo (.getLeastSignificantBits uuid)
        ^long hi (.getMostSignificantBits uuid)
        uuid-bytes (-> (java.nio.ByteBuffer/allocate 16)
                       (.putLong hi)
                       (.putLong lo)
                       (.array))
        builder (StringBuilder.)]
    (.toString
      (reduce
        (fn [b by]
          (.append b (get alphabet (bit-and by MASK))))
        builder
        uuid-bytes))))

(def npattern #"[\s\_\-\.\$\[\]\{\}\#]+")

(def
  ^{:doc "Name normalization function"}
  normalize-name
  (memoize
    (fn
      [n]
      (when-not (string? n)
        (throw (ex-info (format "normalize-name expects a string, got %s of type %s" (pr-str n) (type n))
                        {:value n
                         :type (type n)})))
      (str/lower-case
        (str/replace n npattern "_")))))

(defn column-name
  "Returns quoted column name for SQL"
  [n]
  (when-not (string? n)
    (throw (ex-info (format "column-name expects a string, got %s of type %s" (pr-str n) (type n))
                    {:value n
                     :type (type n)})))
  (str \" (normalize-name n) \"))

(def table-name
  "Returns normalized table name (63-char limit for PostgreSQL/MySQL compatibility)"
  (memoize
    (fn [n]
      (let [n' (str/lower-case
                 (str/replace n npattern "_"))]
        (cond-> n'
          (> (count n') 63) (subs 0 64))))))

(def
  ^{:doc "Returns DB table name for given entity"}
  entity->table-name
  (comp table-name :name))

(def relation-field
  "Returns foreign key field name for a table"
  (memoize
    (fn [table-name]
      (let [f (str table-name "_id")]
        (cond-> f
          (> (count f) 63) (subs 0 64))))))

(defn entity->relation-field
  "Returns foreign key field name for an entity, handling cloned entities"
  [entity]
  (as-> (entity->table-name entity) field-name
    (if (core/cloned? entity)
      (str field-name "_cloned")
      field-name)
    (relation-field field-name)))

(defn- short-table
  "Abbreviates table name for use in relation table names."
  [name]
  (str/join
    "_"
    (keep
      (comp #(when % (str/lower-case %)) #(re-find #"\w{2,4}" %))
      (str/split name #"[\_\-\s]"))))

(defn relation->table-name-for-id
  "Returns relation table name using specified id-key (:euuid or :xid)."
  [relation id-key]
  (let [{inverted? :dataset.relation/inverted?} (meta relation)
        rel (if inverted? (core/invert-relation relation) relation)
        {:keys [euuid xid from to]} rel
        id-part (case id-key
                  :euuid (hash-uuid euuid)
                  :xid xid)
        rn (str (short-table (:name from)) \_ id-part \_ (short-table (:name to)))]
    (cond-> rn (> (count rn) 63) (subs 0 64))))

(defn relation->table-name
  "Returns relation DB table name using current ID provider."
  [relation]
  (relation->table-name-for-id relation (id/key)))

(defprotocol SQLNameResolution
  "Protocol for resolving SQL names in the context of a deployed model"
  (table [this euuid] "Returns table name based on input euuid")
  (relation [this table label] "Returns relation table name based on label, that can be keyword or UUID")
  (related-table [this table label] "Returns related table name based on label, that can be keyword or UUID")
  (relation-from-field [this table label] "Returns name for relation field that is directed from current entity")
  (relation-to-field [this table label] "Returns name for relation field that is directed towards referenced entity"))
