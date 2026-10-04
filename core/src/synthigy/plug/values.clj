;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.plug.values
  "One value shape for trigger payloads, whichever database's JSON builder produced them."
  (:require
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.json :as json]
   [synthigy.timestamp :as timestamp]))

(defn ->bool [v]
  (cond (boolean? v) v
        (number? v)  (not (zero? (long v)))
        (string? v)  (contains? #{"true" "t" "1"} v)
        :else        v))

(defn ->json [v]
  (if (string? v)
    (try (json/read-str v) (catch Exception _ v))
    v))

(defn digits? [^String s from to]
  (loop [i (int from)]
    (or (>= i to)
        (and (Character/isDigit (.charAt s i)) (recur (inc i))))))

(defn zoneless-iso
  "Fast path for `yyyy-MM-dd[T ]HH:mm:ss[.f]` without offset, formatted exactly as Instant/toString; nil otherwise."
  [^String s]
  (let [n (.length s)]
    (when (and (>= n 19) (digits? s 0 4) (= \- (.charAt s 4)) (digits? s 5 7) (= \- (.charAt s 7))
               (digits? s 8 10) (#{\T \space} (.charAt s 10)) (digits? s 11 13) (= \: (.charAt s 13))
               (digits? s 14 16) (= \: (.charAt s 16)) (digits? s 17 19)
               (or (= n 19) (and (= \. (.charAt s 19)) (> n 20) (<= n 29) (digits? s 20 n))))
      (let [frac (if (= n 19) "" (subs s 20))
            end  (loop [e (.length frac)] (if (and (pos? e) (= \0 (.charAt frac (dec e)))) (recur (dec e)) e))
            frac (subs frac 0 end)
            pad  (cond (zero? end) 0 (<= end 3) 3 (<= end 6) 6 :else 9)
            sb   (StringBuilder. 32)]
        (.append sb s 0 10)
        (.append sb \T)
        (.append sb s 11 19)
        (when (pos? pad)
          (.append sb \.)
          (.append sb frac)
          (dotimes [_ (- pad end)] (.append sb \0)))
        (.append sb \Z)
        (.toString sb)))))

(defn ->iso [v]
  (or (when (string? v) (zoneless-iso v))
      (try (str (timestamp/->instant v)) (catch Exception _ v))))

(def json-types #{"json" "timeperiod" "currency" "encrypted"})

(defn fixer [type json-text?]
  (cond
    (= "boolean" type)                     ->bool
    (and json-text? (json-types type))     ->json
    (= "timestamp" type)                   ->iso))

(defonce fixers-by-entity (atom {}))

(dataset/add-model-watch! ::fixers-by-entity (fn [_ _ _ _] (reset! fixers-by-entity {})))

(defn attribute-types
  "Attribute-xid keyword → type for a deployed entity."
  [entity-xid]
  (into {}
        (map (juxt (comp keyword str id/extract) :type))
        (:attributes (dataset/deployed-entity entity-xid))))

(defn fixers
  "[attribute-key f] pairs for the attributes of `entity-xid` whose values need normalizing."
  [entity-xid json-text?]
  (let [k [entity-xid json-text?]]
    (or (get @fixers-by-entity k)
        (let [fs (into [] (keep (fn [[a t]] (when-let [f (fixer t json-text?)] [a f])))
                       (attribute-types entity-xid))]
          (swap! fixers-by-entity assoc k fs)
          fs))))

(defn canonical-values
  "Normalize a before/after attribute map by each attribute's deployed type; `json-text?` when the backend ships JSON columns as text."
  [entity-xid attrs json-text?]
  (if (empty? attrs)
    attrs
    (reduce (fn [m [a f]]
              (let [v (get m a)]
                (if (nil? v) m (assoc m a (f v)))))
            attrs
            (fixers entity-xid json-text?))))
