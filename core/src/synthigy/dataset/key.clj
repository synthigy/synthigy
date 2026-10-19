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

(ns synthigy.dataset.key
  "Key format transformation for dataset operations."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.dataset.sql.schema :as sql-schema]))

(defn preserve-prefix
  "Apply key-fn while preserving leading underscore/dash prefixes."
  [key-fn k]
  (let [s (name k)
        [_ prefix base] (re-matches #"([_\-]{1,})(.*)" s)]
    (if prefix
      (let [normalized-prefix (str/replace prefix "-" "_")]
        (keyword (str normalized-prefix (name (key-fn (keyword base))))))
      (key-fn k))))

(defn label->skins
  "Renders {:pascal :camel :kebab :snake} from a label's words."
  [label]
  (let [ws (remove str/blank? (str/split label naming/npattern))
        cap (fn [w] (if (re-find #"[A-Z]" w) w
                      (str (str/upper-case (subs w 0 1)) (subs w 1))))
        decap (fn [w] (if-let [[_ run r] (re-matches #"([A-Z]+)(.*)" w)]
                        (str (str/lower-case run) r) w))]
    (when (seq ws)
      {:pascal (keyword (apply str (map cap ws)))
       :camel  (keyword (apply str (decap (first ws)) (map cap (rest ws))))
       :kebab  (keyword (str/join "-" (map str/lower-case ws)))
       :snake  (keyword (str/join "_" (map str/lower-case ws)))})))

(defn key->skins
  "label->skins fallback for a key with no label."
  [k]
  (label->skins (str/replace (name k) "_" " ")))

(defn skin-fn
  "Bare key->skins-based renderer for fmt; carries fmt as metadata."
  [fmt]
  (with-meta
    (fn [k] (preserve-prefix (comp fmt key->skins) k))
    {:key-format fmt}))

(def format->key-fn
  (let [snake (skin-fn :snake) kebab (skin-fn :kebab)
        camel (skin-fn :camel) pascal (skin-fn :pascal)]
    {"snake" snake :snake snake
     "kebab" kebab :kebab kebab
     "camel" camel :camel camel
     "pascal" pascal :pascal pascal}))

(defonce ^:private _skin-index (atom {}))

(defn set-skin-index!
  "Replaces the {skin -> canonical-key} inbound lookup."
  [idx]
  (reset! _skin-index (or idx {})))

(defn skin-index
  "Returns the current {skin -> canonical-key} inbound lookup."
  []
  @_skin-index)

(defn squash
  "Separator-squash fallback for a key with no matching skin."
  [k]
  (keyword (str/lower-case (str/replace (name k) naming/npattern "_"))))

(defn normalize-key
  "Resolves an inbound key against the skin index, else squash."
  [k]
  (preserve-prefix
   (fn [k*] (or (get @_skin-index k*) (squash k*)))
   k))

(defn normalize-keys
  "Shallow-resolve all keys in a map — see normalize-key."
  [data]
  (reduce-kv
   (fn [r k v]
     (if-not k r
             (assoc r (normalize-key k) v)))
   nil
   data))

(defn normalize-keys-deep
  "Deep-resolve all keys in a nested structure — see normalize-key."
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

(defn alias->original
  "Map result-key (alias when present, else original key) → original schema key."
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

(defn selection-sub-map
  [v]
  (when (and (vector? v) (seq v) (map? (first v)))
    (:selections (first v))))

(defn sub-selection-for
  "Find the sub-selection in `selection` whose entry produced `result-k`."
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
  "Schema-aware key transformation for outbound data."
  ([entity-id key-fn data]
   (transform-result entity-id key-fn data nil))
  ([entity-id key-fn data selection]
   (let [{:keys [fields field->attribute relations recursions]}
         (sql-schema/deployed-schema-entity entity-id)
         rk->ok (alias->original selection)
         fmt (:key-format (meta key-fn))]
     (reduce-kv
      (fn [m k v]
        (let [original-k (get rk->ok k k)
              alias? (and selection (not= original-k k))
              rel (get relations original-k)
              attr-id (get field->attribute original-k)
              attr (when attr-id (get fields attr-id))
              new-key (cond alias? k
                            (and fmt attr) (or (get-in attr [:skins fmt]) (key-fn k))
                            :else (key-fn k))
              ref-entity (:reference/entity attr)
              recursion? (contains? recursions original-k)
              sub-sel (when selection (sub-selection-for selection original-k k))]
          (cond
            rel
            (let [target-id (:to rel)
                  xf #(transform-result target-id key-fn % sub-sel)]
              (assoc m new-key
                     (cond
                       (and (= (:type rel) :many)
                            (sequential? v)) (mapv xf v)
                       (map? v)              (xf v)
                       :else                 v)))

            recursion?
            (assoc m new-key
                   (if (map? v) (transform-result entity-id key-fn v sub-sel) v))

            ref-entity
            (assoc m new-key
                   (if (map? v) (transform-result ref-entity key-fn v sub-sel) v))

            (and attr (= "json" (:type attr)))
            (assoc m new-key v)

            :else
            (assoc m new-key v))))
      {}
      data))))
