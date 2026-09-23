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

(ns synthigy.embedded.selection
  "Selection shorthand normalizer; port of synthigy.client.selection — run
   sdk/clj's drift test after touching either."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.key :as dk]))

;; hand-duplicated from sdk/clj's synthigy.client.key/->snake_case — pinned by contract/synthigy/contract/selection_drift_test.clj
(defn ->snake
  "Syntactic camelCase/kebab -> snake_case guess for shorthand keys."
  [k]
  (let [s (name k)
        [_ prefix base] (re-matches #"([_\-]+)(.*)" s)]
    (keyword
     (str (some-> prefix (str/replace "-" "_"))
          (-> (or base s)
              (str/replace #"([a-z])([A-Z])" "$1_$2")
              (str/replace #"[-\s]+" "_")
              str/lower-case)))))

(defn normalize
  "Normalize a friendly selection shorthand into the engine's selection format."
  [selection]
  (cond
    (or (nil? selection) (true? selection))
    nil

    (and (vector? selection) (every? keyword? selection))
    (normalize (zipmap selection (repeat nil)))

    (and (vector? selection)
         (every? #(or (keyword? %) (map? %)) selection))
    (normalize
     (reduce (fn [m item]
               (if (keyword? item)
                 (assoc m item nil)
                 (merge m item)))
             {}
             selection))

    (map? selection)
    (reduce-kv
     (fn [m k v]
       (let [k (->snake k)]
         (cond
           (and (vector? v) (seq v) (map? (first v))
                (some #(contains? (first v) %) [:selections :args :alias]))
           (assoc m k (mapv (fn [cfg]
                              (cond-> cfg
                                (:selections cfg) (update :selections normalize)
                                (:args cfg) (update :args dk/normalize-keys-deep)))
                            v))

           (or (nil? v) (true? v))
           (assoc m k nil)

           (and (vector? v)
                (every? #(or (keyword? %) (map? %)) v))
           (assoc m k [{:selections (normalize v)}])

           (and (map? v) (not (contains? v :selections)))
           (assoc m k [{:selections (normalize v)}])

           (and (map? v) (contains? v :selections))
           (assoc m k [(cond-> v
                         (:selections v) (update :selections normalize)
                         (:args v) (update :args dk/normalize-keys-deep))])

           :else
           (assoc m k v))))
     {}
     selection)

    :else selection))
