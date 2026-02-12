(ns synthigy.dataset.id
  "Pluggable ID provider pattern for entity/relation identifiers.

  Provides abstraction over ID generation and field access,
  enabling future migration from UUID to NanoID.

  Usage:
    (require '[synthigy.dataset.id :as id])

    ;; Generate new ID
    (id/generate)  ;; => #uuid \"7c99...\" or \"V1StG...\"

    ;; Get field keyword/string
    (id/key)    ;; => :euuid or :xid
    (id/field)  ;; => \"euuid\" or \"xid\"

    ;; Extract ID from record
    (id/extract entity)  ;; => #uuid \"...\"

    ;; Deterministic UUID<->Base58 conversion
    (id/uuid->nanoid #uuid \"edcab1db-ee6f-4744-bfea-447828893223\")
    ;; => consistent 22-char Base58 string
    (id/nanoid->uuid \"WN5xU8Do5pcdhTkxXvEYwt\")
    ;; => #uuid \"edcab1db-ee6f-4744-bfea-447828893223\"
    "
  (:refer-clojure :exclude [key])
  #?(:clj (:require [clojure.string :as str]
                    [nano-id.core :as nano-id])
     :cljs (:require [clojure.string :as str]))
  #?(:clj (:import [java.util UUID]
                   [java.nio ByteBuffer]
                   [java.math BigInteger])))

;; =============================================================================
;; Protocol
;; =============================================================================

(defprotocol IDProvider
  "Protocol for entity/relation ID generation and access."

  (generate* [this]
    "Generate a new unique ID. Returns UUID or string.")

  (key* [this]
    "Returns the keyword for ID field. E.g., :euuid or :xid")

  (extract* [this record]
    "Extract ID from record. May handle transition logic."))


;; =============================================================================
;; Base58 NanoID Generation
;; =============================================================================
;; Proper nanoid generation using Base58 alphabet (no ambiguous characters).
;; Base58 excludes: 0, O, I, l (and +, /, =, _, - from base64url).

(def base58-alphabet
  "Base58 alphabet: digits 1-9, uppercase A-Z (no I, O), lowercase a-z (no l)."
  "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

#?(:clj
   (def generate-xid
     "Generate a 22-character nanoid using Base58 alphabet.
     This is the canonical XID generator for new records."
     (nano-id/custom base58-alphabet 22)))


;; =============================================================================
;; Deterministic UUID<->Base58 Conversion
;; =============================================================================
;; Bidirectional conversion between UUID and 22-char Base58 string.
;; UUID (128 bits) → BigInteger → base58 digits → 22 chars (zero-padded).
;; Same algorithm as Bitcoin addresses (Base58). Fully lossless roundtrip.
;; 58^22 > 2^128, so every UUID fits in exactly 22 Base58 characters.

#?(:clj (def ^:private ^BigInteger fifty-eight (BigInteger/valueOf 58)))
#?(:clj (def ^:private xid-length 22))

#?(:clj
   (defn uuid->nanoid
     "Deterministic conversion from UUID to 22-char Base58 string.
     Every UUID maps to exactly one nanoid and vice versa.
     Returns nil for nil input. Output uses Base58 alphabet only."
     ^String [^UUID uuid]
     (when uuid
       (let [buf (doto (ByteBuffer/allocate 16)
                   (.putLong (.getMostSignificantBits uuid))
                   (.putLong (.getLeastSignificantBits uuid)))
             n (BigInteger. 1 (.array buf))
             sb (StringBuilder.)]
         (loop [n n]
           (when (pos? (.signum n))
             (let [^"[Ljava.math.BigInteger;" qr (.divideAndRemainder n fifty-eight)]
               (.append sb (.charAt base58-alphabet (.intValue (aget qr 1))))
               (recur (aget qr 0)))))
         (let [s (.toString sb)
               pad (- xid-length (count s))]
           (str (when (pos? pad)
                  (apply str (repeat pad \1)))
                (str/reverse s)))))))

#?(:clj
   (defn nanoid->uuid
     "Deterministic conversion from 22-char Base58 string back to UUID.
     Returns nil for nil input, wrong length, invalid Base58 characters,
     or values exceeding UUID range (>2^128-1)."
     ^UUID [^String nanoid]
     (when (and nanoid (= xid-length (count nanoid)))
       (try
         (let [n (reduce
                   (fn [^BigInteger acc c]
                     (let [idx (.indexOf base58-alphabet (str c))]
                       (if (neg? idx)
                         (throw (IllegalArgumentException. "Invalid base58"))
                         (.add (.multiply acc fifty-eight)
                               (BigInteger/valueOf idx)))))
                   BigInteger/ZERO
                   nanoid)]
           (when (<= (.bitLength n) 128)
             (let [bs (.toByteArray n)
                   padded (byte-array 16)]
               (System/arraycopy bs
                                 (max 0 (- (alength bs) 16))
                                 padded
                                 (max 0 (- 16 (alength bs)))
                                 (min 16 (alength bs)))
               (let [buf (ByteBuffer/wrap padded)]
                 (UUID. (.getLong buf) (.getLong buf))))))
         (catch Exception _ nil)))))


;; =============================================================================
;; UUID Implementation (Current/Legacy)
;; =============================================================================

(defrecord UUIDProvider []
  IDProvider
  (generate* [_]
    #?(:clj (java.util.UUID/randomUUID)
       :cljs (random-uuid)))
  (key* [_] :euuid)
  (extract* [_ record] (:euuid record)))

;; =============================================================================
;; NanoID Implementation (Future)
;; =============================================================================

#?(:clj
   (defrecord NanoIDProvider []
     IDProvider
     (generate* [_] (generate-xid))
     (key* [_] :xid)
     (extract* [_ record]
       ;; Transition: check new key first, fall back to legacy
       (or (:xid record) (:euuid record)))))

#?(:clj
   (defn ->NanoIDProvider
     "Create NanoID provider that generates 22-char Base58 nanoids.
     Base58 alphabet excludes ambiguous characters (0, O, I, l)
     and non-URL-safe characters (+, /, _, -)."
     []
     (NanoIDProvider.)))


;; =============================================================================
;; Global Provider Management
;; =============================================================================

(def ^{:dynamic true
       :doc "The current ID provider.
                 Override with with-provider for testing."}
  *provider*
  (->NanoIDProvider))

(defn set-provider!
  "Set the global ID provider."
  [provider]
  #?(:clj (alter-var-root #'*provider* (constantly provider))
     :cljs (set! *provider* provider)))

(defmacro with-provider
  "Temporarily override ID provider for scope of body."
  [provider & body]
  `(binding [*provider* ~provider]
     ~@body))



;; =============================================================================
;; High-Level API
;; =============================================================================

(defn generate
  "Generate a new ID using current provider."
  []
  (generate* *provider*))

(defn key
  "Get the ID field keyword from current provider.
   E.g., :euuid or :xid"
  []
  (key* *provider*))

(defn field
  "Get the ID column/field name string from current provider.
   E.g., \"euuid\" or \"xid\""
  []
  (name (key* *provider*)))

(defn extract
  "Extract ID from record using current provider.
   Handles transition logic (checks both old and new keys)."
  [record]
  (extract* *provider* record))

;; =============================================================================
;; Entity & Data ID Resolution (Compile-time registration via multimethods)
;; =============================================================================

(defn provider-type
  "Returns :euuid or :xid based on current provider."
  []
  (if (instance? UUIDProvider *provider*) :euuid :xid))


(defmulti -entity-
  "Internal: Resolve entity schema ID. Dispatches on [entity-key provider-type].
   Use `entity` function instead of calling this directly."
  (fn [entity-key provider-type]
    [entity-key provider-type]))

(defmulti -relation-
  "Internal: Resolve relation schema ID. Dispatches on [relation-key provider-type].
   Use `relation` function instead of calling this directly."
  (fn [relation-key provider-type]
    [relation-key provider-type]))

(defmulti -data-
  "Internal: Resolve data ID. Dispatches on [data-key provider-type].
   Use `data` function instead of calling this directly."
  (fn [data-key provider-type]
    [data-key provider-type]))

;; -----------------------------------------------------------------------------
;; Memoized Lookups
;; -----------------------------------------------------------------------------

(def ^:private memo-entity
  "Memoized entity lookup. Cache key: [entity-key provider-type]"
  (memoize (fn [entity-key pt] (-entity- entity-key pt))))

(def ^:private memo-relation
  "Memoized relation lookup. Cache key: [relation-key provider-type]"
  (memoize (fn [relation-key pt] (-relation- relation-key pt))))

(def ^:private memo-data
  "Memoized data lookup. Cache key: [data-key provider-type]"
  (memoize (fn [data-key pt] (-data- data-key pt))))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn entity
  "Resolve entity schema ID based on current provider.

   Returns the appropriate UUID or XID for the entity.
   Pass-through for raw UUIDs/strings.
   Memoized for performance.

   Example:
     (entity :iam/user)  ;; => #uuid \"...\" or \"IAM_USER\""
  ([entity-key] (entity entity-key (provider-type)))
  ([entity-key provider]
   (if (keyword? entity-key)
     (memo-entity entity-key provider)
     entity-key)))

(defn relation
  "Resolve relation schema ID based on current provider.

   Returns the appropriate UUID or XID for the relation.
   Pass-through for raw UUIDs/strings.
   Memoized for performance.

   Example:
     (relation :iam/user->roles)  ;; => #uuid \"...\" or \"USR_ROLES\""
  ([relation-key] (relation relation-key (provider-type)))
  ([relation-key provider]
   (if (keyword? relation-key)
     (memo-relation relation-key provider)
     relation-key)))

(defn data
  "Resolve well-known data ID based on current provider.

   Used for system entities like ROOT, SYNTHIGY, PUBLIC.
   Pass-through for raw UUIDs/strings.
   Memoized for performance.

   Example:
     (data :data/root-role)  ;; => #uuid \"...\" or \"SYS_SUPERUSER\""
  ([data-key] (data data-key (provider-type)))
  ([data-key provider]
   (if (keyword? data-key)
     (memo-data data-key provider)
     data-key)))

;; -----------------------------------------------------------------------------
;; Introspection
;; -----------------------------------------------------------------------------

(defn registered-entities
  "Returns information about all registered entity keys.

  With no args, returns set of entity keys (backward compatible).
  With :full, returns map of entity-key -> {:key :iam/user :euuid UUID :xid String :ns Symbol}

  Example:
    (registered-entities)       ;; => #{:iam/user :iam/app ...}
    (registered-entities :full) ;; => {:iam/user {:key :iam/user :euuid #uuid\"...\" :xid \"IAM_USER\" :ns synthigy.iam}}"
  ([]
   (reduce-kv
     (fn [result [entity-key _key] method]
       (->
         result
         (assoc-in [entity-key _key] (-entity- entity-key _key))
         (assoc-in [entity-key :_ns] (first (clojure.string/split (str method) #"\$")))))
     nil
     (methods -entity-))))

(defn registered-relations
  "Returns information about all registered relation keys.

  Returns map of relation-key -> {:euuid UUID :xid String :_ns namespace}

  Example:
    (registered-relations) ;; => {:iam/user->roles {:euuid #uuid\"...\" :xid \"USR_ROLES\" ...}}"
  ([]
   (reduce-kv
     (fn [result [relation-key _key] method]
       (->
         result
         (assoc-in [relation-key _key] (-relation- relation-key _key))
         (assoc-in [relation-key :_ns] (first (clojure.string/split (str method) #"\$")))))
     nil
     (methods -relation-))))


(defn registered-data
  "Returns information about all registered data keys.

  With no args, returns set of data keys (backward compatible).
  With :full, returns map of data-key -> {:key :data/root :euuid UUID :xid String :ns Symbol}"
  ([]
   (reduce-kv
     (fn [result [data-key _key] method]
       (->
         result
         (assoc-in [data-key _key] (-data- data-key _key))
         (assoc-in [data-key :_ns] (first (clojure.string/split (str method) #"\$")))))
     nil
     (methods -data-))))

(defn entity-id-for-key
  "Get a registered entity's ID in a specific format.

   Args:
     entity-key - Keyword like :iam/user
     id-key     - :euuid or :xid

   Examples:
     (entity-id-for-key :iam/user :euuid) ;; => #uuid \"edcab1db-...\"
     (entity-id-for-key :iam/user :xid)   ;; => \"IAM_USER\""
  [entity-key id-key]
  (-entity- entity-key id-key))

(defn relation-id-for-key
  "Get a registered relation's ID in a specific format.

   Args:
     relation-key - Keyword like :iam/user->roles
     id-key       - :euuid or :xid

   Examples:
     (relation-id-for-key :iam/user->roles :euuid) ;; => #uuid \"...\"
     (relation-id-for-key :iam/user->roles :xid)   ;; => \"USR_ROLES\""
  [relation-key id-key]
  (-relation- relation-key id-key))

(defn data-id-for-key
  "Get a registered data ID in a specific format.

   Args:
     data-key - Keyword like :data/root-role
     id-key   - :euuid or :xid

   Examples:
     (data-id-for-key :data/root-role :euuid) ;; => #uuid \"601ee98d-...\"
     (data-id-for-key :data/root-role :xid)   ;; => \"SYS_SUPERUSER\""
  [data-key id-key]
  (-data- data-key id-key))


;; =============================================================================
;; Registration Macros
;; =============================================================================

#?(:clj
   (defmacro defentity
     "Define a relation with compile-time values for multiple provider types.
   Registers multimethod dispatch entries for each provided key-value pair.
   
   Example:
     (defrelation :iam/user->roles
       :euuid #uuid \"abc123...\"
       :xid \"USR_ROLES\"
       :custom-key \"custom-value\")
   
   Usage:
     (relation :iam/user->roles)  ;; => value based on current provider"
     [relation-key & {:as opts}]
     `(do
        ~@(for [[k v] opts]
            `(defmethod synthigy.dataset.id/-entity- [~relation-key ~k] [~'_ ~'_] ~v))
        ~relation-key)))

#?(:clj
   (defmacro defrelation
     "Define a relation with compile-time values for multiple provider types.
   Registers multimethod dispatch entries for each provided key-value pair.
   
   Example:
     (defrelation :iam/user->roles
       :euuid #uuid \"abc123...\"
       :xid \"USR_ROLES\"
       :custom-key \"custom-value\")
   
   Usage:
     (relation :iam/user->roles)  ;; => value based on current provider"
     [relation-key & {:as opts}]
     `(do
        ~@(for [[k v] opts]
            `(defmethod synthigy.dataset.id/-relation- [~relation-key ~k] [~'_ ~'_] ~v))
        ~relation-key)))

#?(:clj
   (defmacro defdata
     "Define a relation with compile-time values for multiple provider types.
   Registers multimethod dispatch entries for each provided key-value pair.
   
   Example:
     (defrelation :iam/user->roles
       :euuid #uuid \"abc123...\"
       :xid \"USR_ROLES\"
       :custom-key \"custom-value\")
   
   Usage:
     (relation :iam/user->roles)  ;; => value based on current provider"
     [relation-key & {:as opts}]
     `(do
        ~@(for [[k v] opts]
            `(defmethod synthigy.dataset.id/-data- [~relation-key ~k] [~'_ ~'_] ~v))
        ~relation-key)))
