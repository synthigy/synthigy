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

(ns synthigy.json
  "JSON utilities: Clojure data <-> JSON string, with kebab-case keyword keys
   and date/UUID string coercion."
  (:require
   clojure.instant
   clojure.string
   [jsonista.core :as json]))

(def write-mapper
  (json/object-mapper
   {:encode-key-fn (fn [k]
                     (if (keyword? k)
                       (if-let [n (namespace k)]
                         (str n "/" (name k))
                         (name k))
                       k))}))

(def uuid-pattern #"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[34][0-9a-fA-F]{3}-[89ab][0-9a-fA-F]{3}-[0-9a-fA-F]{12}")

(def date-pattern #"\d{4}-(0[1-9]|1[0-2])-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d")

(defn pkey-fn [data]
  (cond
    ;; blank keys would EOF read-string — pass through verbatim
    (clojure.string/blank? data) data
    (re-find #"[a-zA-Z]" data)
    (if (re-find uuid-pattern data)
      data
      (let [[keyword-or-namespace _keyword]
            (clojure.string/split data #"/")]
        (if _keyword
          (keyword
           keyword-or-namespace
           (clojure.string/replace
            _keyword
            #"[_\s]+" "-"))
          (keyword
           (clojure.string/replace
            keyword-or-namespace
            #"[_\s]+" "-")))))
    :else (read-string data)))

(defn synthigy-val-fn
  "Recursively coerce date-pattern strings to Dates and UUID-pattern strings to
   UUIDs."
  [_ data]
  (letfn [(cast-date [date]
            (try
              (clojure.instant/read-instant-date date)
              (catch Exception _ nil)))]
    (cond
      (and (string? data) (re-find date-pattern data)) (cast-date data)
      (and (string? data) (re-find uuid-pattern data)) (try
                                                         (java.util.UUID/fromString data)
                                                         (catch Throwable _ data))
      (vector? data) (mapv #(synthigy-val-fn nil %) data)
      (map? data) (reduce
                   (fn [r [k v]] (assoc r k (synthigy-val-fn k v)))
                   {}
                   data)
      :else data)))

(def default-read-mapper
  (json/object-mapper {:decode-key-fn pkey-fn}))

(defn <-json
  "Parse JSON string to Clojure data structures."
  ([v] (<-json v {}))
  ([v {:keys [keyfn valfn]
       :or {keyfn pkey-fn
            valfn synthigy-val-fn}}]
   (let [mapper (if (= keyfn pkey-fn)
                  default-read-mapper
                  (json/object-mapper {:decode-key-fn keyfn}))
         result (json/read-value v mapper)]
     (if valfn
       (synthigy-val-fn nil result)
       result))))

(def keyword-mapper
  "Mapper that keywordizes keys but does no value transformation."
  (json/object-mapper {:decode-key-fn keyword}))

(def string-key-mapper
  "Mapper with no key or value transformations (string keys)."
  (json/object-mapper {}))

(defn ->json
  "Convert Clojure data to JSON string."
  [data]
  (json/write-value-as-string data write-mapper))

(def write-str
  "Convert Clojure data to JSON string. Alias for ->json."
  ->json)

(defn read-str
  "Parse JSON string to Clojure data with keyword keys. No value
   transformations."
  [s]
  (json/read-value s keyword-mapper))

(defn read-str-raw
  "Parse JSON string to Clojure data with string keys. No value transformations."
  [s]
  (json/read-value s string-key-mapper))

(defn ->timestamp [date] (when date (java.sql.Timestamp. (.getTime date))))

(defn <-timestamp [text] (when text (java.sql.Timestamp/valueOf text)))

(defn current-time [] (->timestamp (java.util.Date.)))
