(ns synthigy.json
  "JSON/JSONB utilities for database operations - works with PostgreSQL, SQLite, etc."
  (:require
   clojure.instant
   clojure.string
   [jsonista.core :as json])
  (:import
   [org.postgresql.util PGobject]))

(defn jsonb-field? [data]
  (and
   (instance? PGobject data)
   (#{"jsonb" "json"} (.getType data))))

(def write-mapper
  (json/object-mapper
   {:encode-key-fn (fn [k]
                     (if (keyword? k)
                       (if-let [n (namespace k)]
                         (str n "/" (name k))
                         (name k))
                       k))}))

(defn data->json [data]
  (if (jsonb-field? data)
    data
    (doto (PGobject.)
      (.setType "jsonb")
      (.setValue (json/write-value-as-string data write-mapper)))))

(def uuid-pattern #"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[34][0-9a-fA-F]{3}-[89ab][0-9a-fA-F]{3}-[0-9a-fA-F]{12}")

(def date-pattern #"\d{4}-(0[1-9]|1[0-2])-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d")

(defn pkey-fn [data]
  (if (re-find #"[a-zA-Z]" data)
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
    (read-string data)))

(defn synthigy-val-fn
  "Helper function for transforming dates and other objects to Clojure data
   objects"
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

(defn json->data
  ([v] (json->data v {}))
  ([v {:keys [keyfn valfn]
       :or {keyfn pkey-fn
            valfn synthigy-val-fn}}]
   (if (jsonb-field? v)
     (when-let [s (.getValue v)]
       (let [mapper (if (= keyfn pkey-fn)
                      default-read-mapper
                      (json/object-mapper {:decode-key-fn keyfn}))
             result (json/read-value s mapper)]
         (if valfn
           (synthigy-val-fn nil result)
           result)))
     v)))

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

(defn ->json
  "Convert Clojure data to JSON string."
  [data]
  (json/write-value-as-string data write-mapper))

(defn ->timestamp [date] (when date (java.sql.Timestamp. (.getTime date))))

(defn <-timestamp [text] (when text (java.sql.Timestamp/valueOf text)))

(defn current-time [] (->timestamp (java.util.Date.)))
