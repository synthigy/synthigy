(ns synthigy.dataset.key
  "Key format transformation for dataset operations.

  Transforms keys between snake_case (internal default), kebab-case,
  and camelCase at the API boundary. Schema-aware: JSON-type field
  values are passed through untouched, relations are recursed."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string]
   [synthigy.dataset.sql.schema :as sql-schema]))

(defn- preserve-prefix
  "Apply key-fn while preserving leading underscore/dash prefixes.
  Leading dashes are normalized to underscores (:-where → :_where)."
  [key-fn k]
  (let [s (name k)
        [_ prefix base] (re-matches #"([_\-]{1,})(.*)" s)]
    (if prefix
      (let [normalized-prefix (clojure.string/replace prefix "-" "_")]
        (keyword (str normalized-prefix (name (key-fn (keyword base))))))
      (key-fn k))))

(def ->snake
  (partial preserve-prefix csk/->snake_case_keyword))

(def ->kebab
  (partial preserve-prefix csk/->kebab-case-keyword))

(def ->camel
  (partial preserve-prefix csk/->camelCaseKeyword))

(def ->pascal
  (partial preserve-prefix csk/->PascalCaseKeyword))

(def format->key-fn
  {"snake"  ->snake
   :snake   ->snake
   "kebab"  ->kebab
   :kebab   ->kebab
   "camel"  ->camel
   :camel   ->camel
   "pascal" ->pascal
   :pascal  ->pascal})

(defn normalize-key
  "Normalize a key to snake_case keyword. Handles camelCase,
  kebab-case, snake_case, and space-separated input."
  [k]
  (->snake k))

(defn normalize-keys
  "Shallow normalize all keys in a map to snake_case keywords."
  [data]
  (reduce-kv
   (fn [r k v]
     (if-not k r
             (assoc r (normalize-key k) v)))
   nil
   data))

(defn normalize-keys-deep
  "Deep normalize all keys in a nested structure to snake_case keywords.
  Recurses into maps and vectors."
  [data]
  (cond
    (map? data)
    (reduce-kv
     (fn [m k v]
       (if-not k m
               (assoc m (normalize-key k) (normalize-keys-deep v))))
     {}
     data)

    (vector? data)
    (mapv normalize-keys-deep data)

    :else data))

(defn- alias->original
  "Build a map from result-key → original schema key for a normalized
  selection. Result-key is the alias when present, else the original key.
  Used so transform-result can look up relation metadata when the data is
  keyed by an alias.

  Aliases supplied as strings are keywordized to match what the JSON
  parser produces (`synthigy.json/pkey-fn` turns every JSON key into a
  keyword)."
  [selection]
  (when (map? selection)
    (reduce-kv
     (fn [m k v]
       (if (and (vector? v) (seq v) (map? (first v))
                (some #(contains? (first v) %) [:selections :args :alias]))
         (reduce (fn [m {:keys [alias]}]
                   (assoc m (if alias (keyword alias) k) k))
                 m v)
         (assoc m k k)))
     {}
     selection)))

(defn- selection-sub-map
  [v]
  (when (and (vector? v) (seq v) (map? (first v)))
    (:selections (first v))))

(defn- sub-selection-for
  "Find the sub-selection in `selection` whose entry produced `result-k`
  (matching by alias or by original key). Aliases are keywordized to
  match parsed JSON keys."
  [selection original-k result-k]
  (let [v (get selection original-k)]
    (if (and (vector? v) (seq v) (map? (first v)))
      (some (fn [entry]
              (let [entry-k (if (:alias entry)
                              (keyword (:alias entry))
                              original-k)]
                (when (= entry-k result-k)
                  (:selections entry))))
            v)
      (selection-sub-map v))))

(defn transform-result
  "Schema-aware key transformation for outbound data.

  Walks the result using deployed schema metadata:
  - Regular scalar fields: transform key with key-fn
  - JSON fields: transform key, leave value untouched
  - Junction-based relations (m2m / some o2m): transform key, recurse into
    target entity
  - Recursions (self-FK tree relations like :father, :mother): transform
    key, recurse into the SAME entity's schema
  - Reference-typed fields (e.g. :assignee type \"user\"): transform key,
    recurse into the referenced entity's schema

  When `selection` is supplied (normalized internal format), result keys may
  be aliases; relation metadata is looked up via the original schema key
  carried in the selection.

  Maps only — nil and non-map values (empty refs, scalars) pass through."
  ([entity-id key-fn data]
   (transform-result entity-id key-fn data nil))
  ([entity-id key-fn data selection]
   (let [{:keys [fields field->attribute relations recursions]}
         (sql-schema/deployed-schema-entity entity-id)
         rk->ok (alias->original selection)]
     (reduce-kv
      (fn [m k v]
        (let [original-k (get rk->ok k k)
              alias? (and selection (not= original-k k))
              new-key (if alias? k (key-fn k))
              rel (get relations original-k)
              attr-id (get field->attribute original-k)
              attr (when attr-id (get fields attr-id))
              ref-entity (:reference/entity attr)
              recursion? (contains? recursions original-k)
              sub-sel (when selection (sub-selection-for selection original-k k))]
          (cond
            ;; Junction-based relation — can be :many (vector) or :one (map).
            ;; :one may also arrive as a bare FK scalar (e.g. audit fields
            ;; like :modified_by → user._eid); pass those through untouched.
            rel
            (let [target-id (:to rel)
                  xf #(transform-result target-id key-fn % sub-sel)]
              (assoc m new-key
                     (cond
                       (= (:type rel) :many) (mapv xf v)
                       (map? v)              (xf v)
                       :else                 v)))

            ;; Self-referential tree FK (recursion) — recurse into same entity
            recursion?
            (assoc m new-key
                   (if (map? v) (transform-result entity-id key-fn v sub-sel) v))

            ;; Field-level entity reference (e.g. :assignee → User)
            ref-entity
            (assoc m new-key
                   (if (map? v) (transform-result ref-entity key-fn v sub-sel) v))

            ;; JSON field — key transforms, value left alone
            (and attr (= "json" (:type attr)))
            (assoc m new-key v)

            :else
            (assoc m new-key v))))
      {}
      data))))
