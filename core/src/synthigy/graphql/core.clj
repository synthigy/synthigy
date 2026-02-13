(ns synthigy.graphql.core
  "Core protocols and utilities for GraphQL HTTP handling."
  (:require
    [clojure.string :as str]
    [com.walmartlabs.lacinia.util :as util]))

;;; ============================================================================
;;; Schema Provider Protocol
;;; ============================================================================

(defprotocol SchemaProvider
  "Protocol for obtaining compiled Lacinia schema.
   Allows schema to be passed as value, function, or reference."
  (get-schema [this] "Returns compiled Lacinia schema"))

;; Maps implement IFn (for keyword lookup), so we check explicitly
;; Lacinia's CompiledSchema is a deftype, handled by Object fallback
(extend-protocol SchemaProvider
  clojure.lang.IFn
  (get-schema [f]
    (if (map? f)
      f  ; Direct schema value
      (f)))  ; Provider function

  clojure.lang.IDeref
  (get-schema [r] @r)

  Object
  (get-schema [obj] obj))

;;; ============================================================================
;;; Content-Type Parsing
;;; ============================================================================

(defn parse-content-type
  "Extract media type from Content-Type header, ignoring parameters.

   Examples:
     'application/json' -> :application/json
     'application/json; charset=utf-8' -> :application/json
     'APPLICATION/JSON' -> :application/json
     nil -> nil"
  [header]
  (when header
    (let [media-type (-> header
                         (str/split #";" 2)
                         first
                         str/trim
                         str/lower-case)]
      (when-not (str/blank? media-type)
        (keyword media-type)))))

;;; ============================================================================
;;; Error Handling
;;; ============================================================================

(defn throwable->error
  "Convert Throwable to GraphQL error map using Lacinia's utility."
  [^Throwable t]
  (util/as-error-map t))

(defn error-response
  "Create error response map with status."
  ([message] (error-response 400 message))
  ([status message]
   {::errors [{:message message}]
    ::status status}))

(defn errors-response
  "Create response from error maps with status."
  ([errors] (errors-response 400 errors))
  ([status errors]
   {::errors errors
    ::status status}))

(defn early-error?
  "Check if result is an early error (before execution)."
  [result]
  (contains? result ::errors))

;;; ============================================================================
;;; Cache Key
;;; ============================================================================

(defn cache-key
  "Build cache key from query and optional operation name.
   Returns string when no operation (common case, faster hashing)."
  [query operation-name]
  (if operation-name
    [query operation-name]
    query))
