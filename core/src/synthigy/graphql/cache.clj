(ns synthigy.graphql.cache
  "Parsed query caching with LRU eviction.

   Caches the result of parser/parse-query to avoid re-parsing
   identical queries. This matches lacinia-pedestal's approach."
  (:import
    [java.util LinkedHashMap Collections Map]))

;;; ============================================================================
;;; Protocol
;;; ============================================================================

(defprotocol ParsedQueryCache
  "Protocol for caching parsed GraphQL queries."
  (cache-get [this key] "Get cached parsed query or nil")
  (cache-put [this key parsed-query] "Store parsed query, returns it"))

;; nil acts as no-op cache
(extend-type nil
  ParsedQueryCache
  (cache-get [_ _] nil)
  (cache-put [_ _ parsed-query] parsed-query))

;;; ============================================================================
;;; LRU Cache Implementation
;;; ============================================================================

(defn- make-lru-map
  "Create synchronized LRU LinkedHashMap.

   LinkedHashMap with accessOrder=true:
   - Reorders entries on access (get/put)
   - removeEldestEntry evicts oldest when size > max

   Thread-safe via Collections/synchronizedMap."
  ^Map [max-size]
  (-> (proxy [LinkedHashMap] [16 0.75 true]
        (removeEldestEntry [_]
          (> (.size ^LinkedHashMap this) max-size)))
      (Collections/synchronizedMap)))

(defrecord LRUCache [^Map cache-map]
  ParsedQueryCache
  (cache-get [_ key]
    (.get cache-map key))
  (cache-put [_ key parsed-query]
    (.put cache-map key parsed-query)
    parsed-query))

(defn lru-cache
  "Create thread-safe LRU parsed query cache.

   Args:
     max-size - Maximum number of cached queries

   Uses LinkedHashMap with access-ordering for O(1) LRU eviction."
  [max-size]
  (assert (pos-int? max-size) "max-size must be positive integer")
  (->LRUCache (make-lru-map max-size)))

;;; ============================================================================
;;; Convenience
;;; ============================================================================

(defn no-cache
  "Returns nil, which acts as no-op cache via protocol extension."
  []
  nil)
