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

(ns synthigy.wire
  "Engine results in a client's format: trimmed to :selections, keys in the requested key format."
  (:require
   [synthigy.dataset :as dataset]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.sql.query :as sql-query]))

(defn key-fn
  "The key renderer for a request's key_format; snake when none is given."
  [key-format]
  (dk/format->key-fn (or key-format "snake")))

(defn strip-eid
  "Recursively remove the internal :_eid from a result value."
  [v]
  (cond
    (map? v)        (reduce-kv (fn [m k val]
                                 (if (= k :_eid) m (assoc m k (strip-eid val))))
                               {} v)
    (sequential? v) (mapv strip-eid v)
    :else           v))

(defn shape
  "One operation's data as a client receives it; `op` carries :entity, :entity-id and :selections."
  [key-fn op data]
  (cond
    (:synthigy.engine/count-only? (meta data))
    data

    (and (:entity op) (coll? data))
    (let [entity-id (or (:entity-id op) (sql-query/resolve-entity (:entity op)))
          sel (when (map? (:selections op))
                (dataset/normalize-selection (:selections op)))
          strip #(dataset/strip-to-selection entity-id sel %)
          key-xf #(dk/transform-result entity-id key-fn % sel)
          xf (cond
               (and sel key-fn) (comp key-xf strip)
               sel strip
               key-fn (comp strip-eid key-xf)
               :else strip-eid)]
      (cond
        (sequential? data) (mapv #(cond-> % (map? %) xf) data)
        (map? data) (xf data)
        :else data))

    (and (nil? (:entity op)) key-fn (sequential? data))
    (mapv #(cond-> % (map? %) (update-keys key-fn)) data)

    :else data))

(defn results
  "Engine/execute results as a client receives them; failed results pass through."
  [key-fn results]
  (mapv (fn [result]
          (let [op (:synthigy.engine/op-shape result)
                result (dissoc result :synthigy.engine/op-shape :synthigy.engine/throwable)]
            (if (:ok result)
              (assoc result :data (shape key-fn op (:data result)))
              result)))
        results))
