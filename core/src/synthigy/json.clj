(ns synthigy.json
  "JSON/JSONB utilities for database operations - works with PostgreSQL, SQLite, etc."
  (:require
   clojure.instant
   clojure.string
   [jsonista.core :as json]))

;; Database-agnostic JSON field detection
;; Uses duck-typing to avoid hard dependency on PGobject

(defn- pgobject?
  "Check if value is a PostgreSQL PGobject (without compile-time dependency)."
  [data]
  (when data
    (let [class-name (.getName (class data))]
      (= class-name "org.postgresql.util.PGobject"))))

(defn- pgobject-type
  "Get the type of a PGobject using reflection."
  [data]
  (when (pgobject? data)
    (.invoke (.getMethod (class data) "getType" (into-array Class []))
             data
             (into-array Object []))))

(defn- pgobject-value
  "Get the value of a PGobject using reflection."
  [data]
  (when (pgobject? data)
    (.invoke (.getMethod (class data) "getValue" (into-array Class []))
             data
             (into-array Object []))))

(defn jsonb-field?
  "Check if data is a JSON/JSONB database field."
  [data]
  (and (pgobject? data)
       (#{"jsonb" "json"} (pgobject-type data))))

(def write-mapper
  (json/object-mapper
   {:encode-key-fn (fn [k]
                     (if (keyword? k)
                       (if-let [n (namespace k)]
                         (str n "/" (name k))
                         (name k))
                       k))}))

(defn- create-pgobject
  "Create a PGobject for JSONB storage (Postgres-specific, fails gracefully for other DBs)."
  [json-str]
  (try
    (let [pg-class (Class/forName "org.postgresql.util.PGobject")
          pg-obj (.newInstance pg-class)]
      (.invoke (.getMethod pg-class "setType" (into-array Class [String]))
               pg-obj
               (into-array Object ["jsonb"]))
      (.invoke (.getMethod pg-class "setValue" (into-array Class [String]))
               pg-obj
               (into-array Object [json-str]))
      pg-obj)
    (catch ClassNotFoundException _
      ;; PostgreSQL driver not available - return plain JSON string
      json-str)))

(defn data->json
  "Convert Clojure data to JSON format suitable for database storage.
   For PostgreSQL, returns PGobject with type 'jsonb'.
   For other databases, returns JSON string."
  [data]
  (if (jsonb-field? data)
    data
    (create-pgobject (json/write-value-as-string data write-mapper))))

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
  "Parse JSON from database field to Clojure data.
   Handles both PGobject (PostgreSQL) and plain strings (SQLite, etc.)."
  ([v] (json->data v {}))
  ([v {:keys [keyfn valfn]
       :or {keyfn pkey-fn
            valfn synthigy-val-fn}}]
   (cond
     ;; Handle PGobject (PostgreSQL JSONB)
     (jsonb-field? v)
     (when-let [s (pgobject-value v)]
       (let [mapper (if (= keyfn pkey-fn)
                      default-read-mapper
                      (json/object-mapper {:decode-key-fn keyfn}))
             result (json/read-value s mapper)]
         (if valfn
           (synthigy-val-fn nil result)
           result)))

     ;; Handle plain string (SQLite JSON, etc.)
     (string? v)
     (let [mapper (if (= keyfn pkey-fn)
                    default-read-mapper
                    (json/object-mapper {:decode-key-fn keyfn}))
           result (json/read-value v mapper)]
       (if valfn
         (synthigy-val-fn nil result)
         result))

     ;; Already parsed or nil
     :else v)))

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
