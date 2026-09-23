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

(ns synthigy.dataset.id
  "Pluggable ID provider for entity/relation identifiers — UUIDProvider (:euuid)
   or NanoIDProvider (:xid)."
  (:refer-clojure :exclude [key])
  (:require [clojure.string :as str]
            [nano-id.core :as nano-id]
            #?@(:cljs [[goog.crypt :as gcrypt]]))
  #?(:clj (:import [java.util UUID]
                   [java.nio ByteBuffer]
                   [java.math BigInteger])
     :cljs (:import [goog.crypt Md5])))

(defprotocol IDProvider
  "Entity/relation ID generation and access."

  (generate* [this]
    "Generates a new unique ID.")

  (key* [this]
    "Returns the ID field keyword, e.g. :euuid or :xid.")

  (extract* [this record]
    "Extracts the ID from a record.")

  (derive* [this parent name-str]
    "Deterministically derives {:euuid <UUID> :xid <string>} from a parent id + name — same input, forever the same output."))

(def base58-alphabet
  "Base58 alphabet: digits 1-9, uppercase A-Z (no I, O), lowercase a-z (no l)."
  "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

(def generate-xid
  "The canonical XID generator for new records: 22-char Base58 nanoid."
  (nano-id/custom base58-alphabet 22))

(def xid-length 22)

#?(:clj (def ^:private ^BigInteger fifty-eight (BigInteger/valueOf 58)))

(defn uuid->nanoid
  "Deterministic, lossless UUID -> 22-char Base58 string. Nil in, nil out."
  [uuid]
  (when uuid
    #?(:clj
       (let [^UUID uuid uuid
             buf (doto (ByteBuffer/allocate 16)
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
                (str/reverse s))))
       :cljs
       (let [;; UUID string → hex → BigInt
             hex (str/replace (str uuid) "-" "")
             n (js/BigInt (str "0x" hex))
             base (js/BigInt 58)]
         (loop [n n
                chars []]
           (if (pos? n)
             (let [r (js/Number (mod n base))
                   q (js/BigInt (/ (- n (js/BigInt r)) base))]
               (recur q (conj chars (.charAt base58-alphabet r))))
             (let [s (apply str (reverse chars))
                   pad (- xid-length (count s))]
               (str (when (pos? pad)
                      (apply str (repeat pad \1)))
                    s))))))))

(defn nanoid->uuid
  "Inverse of uuid->nanoid. Nil for nil input, wrong length, or invalid Base58."
  [nanoid]
  (when (and nanoid (= xid-length (count nanoid)))
    #?(:clj
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
         (catch Exception _ nil))
       :cljs
       (try
         (let [base (js/BigInt 58)
               n (reduce
                   (fn [acc c]
                     (let [idx (.indexOf base58-alphabet (str c))]
                       (if (neg? idx)
                         (throw (js/Error. "Invalid base58"))
                         (+ (* acc base) (js/BigInt idx)))))
                   (js/BigInt 0)
                   nanoid)
               hex (.toString n 16)
               ;; Pad to 32 hex chars (128 bits)
               padded (str (apply str (repeat (- 32 (count hex)) "0")) hex)]
           ;; Format as UUID: 8-4-4-4-12
           (str (subs padded 0 8) "-"
                (subs padded 8 12) "-"
                (subs padded 12 16) "-"
                (subs padded 16 20) "-"
                (subs padded 20 32)))
         (catch :default _ nil)))))


;; MD5 (UUID v3) is locked forever — changing it shifts every derived id ever
;; produced
(defn canonical-anchor-string
  "Coerces parent-id (UUID, xid string, or {:euuid :xid} map) to a stable hash
   anchor."
  [parent]
  (cond
    (string? parent) parent
    (uuid? parent)   (uuid->nanoid parent)
    (map? parent)    (or (:xid parent)
                         (some-> (:euuid parent) uuid->nanoid)
                         (throw (ex-info "Map carries no :euuid or :xid"
                                         {:parent parent})))
    :else (throw (ex-info "Cannot derive from non-id value"
                          {:parent parent}))))

(defn canonical-derive-uuid
  "Hashes (anchor-string, name) into a deterministic UUID v3, identical in CLJ
   and CLJS."
  [parent name-str]
  (let [anchor (canonical-anchor-string parent)]
    #?(:clj
       (UUID/nameUUIDFromBytes (.getBytes (str anchor \: name-str) "UTF-8"))
       :cljs
       (let [md (Md5.)
             _ (.update md (gcrypt/stringToUtf8ByteArray (str anchor ":" name-str)))
             bs (.digest md)
             _ (aset bs 6 (bit-or (bit-and (aget bs 6) 0x0f) 0x30))
             _ (aset bs 8 (bit-or (bit-and (aget bs 8) 0x3f) 0x80))
             hex (gcrypt/byteArrayToHex bs)]
         (uuid (str (subs hex 0 8) "-" (subs hex 8 12) "-"
                    (subs hex 12 16) "-" (subs hex 16 20) "-"
                    (subs hex 20 32)))))))

(defrecord UUIDProvider []
  IDProvider
  (generate* [_]
    #?(:clj (java.util.UUID/randomUUID)
       :cljs (random-uuid)))
  (key* [_] :euuid)
  (extract* [_ record] (:euuid record))
  (derive* [_ parent name-str]
    (let [u (canonical-derive-uuid parent name-str)]
      {:euuid u :xid (uuid->nanoid u)})))

(defrecord NanoIDProvider []
  IDProvider
  (generate* [_] (generate-xid))
  (key* [_] :xid)
  (extract* [_ record]
    (or (:xid record) (:euuid record)))
  (derive* [_ parent name-str]
    (let [u (canonical-derive-uuid parent name-str)]
      {:euuid u :xid (uuid->nanoid u)})))

(def ^{:dynamic true
       :doc "The active ID provider. Override with with-provider for testing."}
  *provider*
  (->NanoIDProvider))

(defn set-provider!
  "Sets the global ID provider."
  [provider]
  #?(:clj (alter-var-root #'*provider* (constantly provider))
     :cljs (set! *provider* provider)))

(defmacro with-provider
  "Temporarily overrides the ID provider for the scope of body."
  [provider & body]
  `(binding [*provider* ~provider]
     ~@body))

(defn generate
  "Generates a new ID using the current provider."
  []
  (generate* *provider*))

(defn key
  "Gets the ID field keyword (:euuid or :xid) from the current provider."
  []
  (key* *provider*))

(defn field
  "Gets the ID field name string from the current provider."
  []
  (name (key* *provider*)))

(defn extract
  "Extracts the ID from a record using the current provider."
  [record]
  (extract* *provider* record))

(defn coerce-arg
  "Coerces a query-arg value to the active provider's runtime id type — PostgreSQL refuses
   an implicit uuid = varchar comparison. Tolerant, never throws. CLJS passes through."
  [v]
  (if (and (= :euuid (key)) (string? v))
    #?(:clj  (try (java.util.UUID/fromString v) (catch Exception _ v))
       :cljs v)
    v))

(defn coerce-stored-id
  "Coerces an id persisted as TEXT (audit/relation plug) back to the active
   provider's runtime type. Strict — assumes a real id string; nil for nil."
  [s]
  (when s
    (case (key)
      :xid   s
      :euuid #?(:clj  (java.util.UUID/fromString s)
                :cljs s))))

(defn current-provider
  "Returns the active ID provider record."
  []
  *provider*)

(defn derive-id
  "Deterministically derives {:euuid :xid} from a parent id + name string."
  [parent name-str]
  (derive* *provider* parent name-str))

(defn new-model-node-id
  "Mints a fresh, euuid-first dual-id for a portable schema artifact (entity/attribute/relation).
   Random, not derived — records use generate instead."
  []
  (let [u #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))]
    {:euuid u :xid (uuid->nanoid u)}))

(defn ensure-dual-ids
  "Fills in the missing half of :euuid/:xid on a record; unchanged if both or
   neither present."
  [record]
  (let [euuid (:euuid record)
        xid (:xid record)]
    (cond
      (and euuid (not xid))
      (assoc record :xid (uuid->nanoid euuid))

      (and xid (not euuid))
      (assoc record :euuid (nanoid->uuid xid))

      :else record)))

(defn provider-type
  "Returns :euuid or :xid based on the current provider."
  []
  (if (instance? UUIDProvider *provider*) :euuid :xid))

(defmulti -entity-
  "Resolves entity schema ID, dispatching on [entity-key provider-type]. Use
   entity instead."
  (fn [entity-key provider-type]
    [entity-key provider-type]))

(defmulti -relation-
  "Resolves relation schema ID, dispatching on [relation-key provider-type]. Use
   relation instead."
  (fn [relation-key provider-type]
    [relation-key provider-type]))

(defmulti -data-
  "Resolves data ID, dispatching on [data-key provider-type]. Use data instead."
  (fn [data-key provider-type]
    [data-key provider-type]))

(def memo-entity
  (memoize (fn [entity-key pt] (-entity- entity-key pt))))

(def memo-relation
  (memoize (fn [relation-key pt] (-relation- relation-key pt))))

(def memo-data
  (memoize (fn [data-key pt] (-data- data-key pt))))

(defn entity
  "Resolves an entity schema ID for the current provider; pass-through for raw
   UUIDs/strings."
  ([entity-key] (entity entity-key (provider-type)))
  ([entity-key provider]
   (if (keyword? entity-key)
     (memo-entity entity-key provider)
     entity-key)))

(defn relation
  "Resolves a relation schema ID for the current provider; pass-through for raw
   UUIDs/strings."
  ([relation-key] (relation relation-key (provider-type)))
  ([relation-key provider]
   (if (keyword? relation-key)
     (memo-relation relation-key provider)
     relation-key)))

(defn data
  "Resolves a well-known data ID (ROOT, SYNTHIGY, PUBLIC, ...) for the current
   provider."
  ([data-key] (data data-key (provider-type)))
  ([data-key provider]
   (if (keyword? data-key)
     (memo-data data-key provider)
     data-key)))

(defn registered-entities
  "Returns a map of entity-key -> {:euuid UUID :xid String :_ns namespace}."
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
  "Returns a map of relation-key -> {:euuid UUID :xid String :_ns namespace}."
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
  "Returns a map of data-key -> {:euuid UUID :xid String :_ns namespace}."
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
  "Gets a registered entity's ID in a specific format (:euuid or :xid),
   regardless of active provider."
  [entity-key id-key]
  (-entity- entity-key id-key))

(defn relation-id-for-key
  "Gets a registered relation's ID in a specific format (:euuid or :xid),
   regardless of active provider."
  [relation-key id-key]
  (-relation- relation-key id-key))

(defn data-id-for-key
  "Gets a registered data ID in a specific format (:euuid or :xid), regardless
   of active provider."
  [data-key id-key]
  (-data- data-key id-key))

#?(:clj
   (defmacro defentity
     "Registers an entity's per-provider id values as -entity- multimethod
      dispatch entries."
     [relation-key & {:as opts}]
     `(do
        ~@(for [[k v] opts]
            `(defmethod synthigy.dataset.id/-entity- [~relation-key ~k] [~'_ ~'_] ~v))
        ~relation-key)))

#?(:clj
   (defmacro defrelation
     "Registers a relation's per-provider id values as -relation- multimethod
      dispatch entries."
     [relation-key & {:as opts}]
     `(do
        ~@(for [[k v] opts]
            `(defmethod synthigy.dataset.id/-relation- [~relation-key ~k] [~'_ ~'_] ~v))
        ~relation-key)))

#?(:clj
   (defmacro defdata
     "Registers a data id's per-provider values as -data- multimethod dispatch
      entries."
     [relation-key & {:as opts}]
     `(do
        ~@(for [[k v] opts]
            `(defmethod synthigy.dataset.id/-data- [~relation-key ~k] [~'_ ~'_] ~v))
        ~relation-key)))
