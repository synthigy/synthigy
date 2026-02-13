(ns synthigy.graphql.cache-test
  (:require
    [clojure.test :refer [deftest testing is]]
    [synthigy.graphql.cache :as cache]))

;;; ============================================================================
;;; Nil Cache (No-op) Tests
;;; ============================================================================

(deftest nil-cache-test
  (testing "nil cache get returns nil"
    (is (nil? (cache/cache-get nil "any-key")))
    (is (nil? (cache/cache-get nil ["query" "op"]))))

  (testing "nil cache put returns the value unchanged"
    (let [parsed {:parsed true}]
      (is (= parsed (cache/cache-put nil "key" parsed)))))

  (testing "no-cache function returns nil"
    (is (nil? (cache/no-cache)))))

;;; ============================================================================
;;; LRU Cache Basic Operations Tests
;;; ============================================================================

(deftest lru-cache-basic-test
  (testing "cache creation"
    (let [c (cache/lru-cache 10)]
      (is (some? c))
      (is (satisfies? cache/ParsedQueryCache c))))

  (testing "put and get"
    (let [c (cache/lru-cache 10)
          parsed {:type :query :selections []}]
      (cache/cache-put c "query1" parsed)
      (is (= parsed (cache/cache-get c "query1")))))

  (testing "get returns nil for missing key"
    (let [c (cache/lru-cache 10)]
      (is (nil? (cache/cache-get c "nonexistent")))))

  (testing "put returns the stored value"
    (let [c (cache/lru-cache 10)
          parsed {:data true}]
      (is (= parsed (cache/cache-put c "key" parsed)))))

  (testing "overwrite existing key"
    (let [c (cache/lru-cache 10)]
      (cache/cache-put c "key" {:version 1})
      (cache/cache-put c "key" {:version 2})
      (is (= {:version 2} (cache/cache-get c "key"))))))

;;; ============================================================================
;;; LRU Eviction Tests
;;; ============================================================================

(deftest lru-cache-eviction-test
  (testing "evicts oldest when exceeding max size"
    (let [c (cache/lru-cache 3)]
      ;; Fill cache
      (cache/cache-put c "q1" {:n 1})
      (cache/cache-put c "q2" {:n 2})
      (cache/cache-put c "q3" {:n 3})

      ;; All should be present
      (is (= {:n 1} (cache/cache-get c "q1")))
      (is (= {:n 2} (cache/cache-get c "q2")))
      (is (= {:n 3} (cache/cache-get c "q3")))

      ;; Add one more - q1 should be evicted (oldest)
      (cache/cache-put c "q4" {:n 4})

      (is (nil? (cache/cache-get c "q1")) "q1 should be evicted")
      (is (= {:n 2} (cache/cache-get c "q2")))
      (is (= {:n 3} (cache/cache-get c "q3")))
      (is (= {:n 4} (cache/cache-get c "q4")))))

  (testing "access reorders for LRU"
    (let [c (cache/lru-cache 3)]
      ;; Fill cache
      (cache/cache-put c "q1" {:n 1})
      (cache/cache-put c "q2" {:n 2})
      (cache/cache-put c "q3" {:n 3})

      ;; Access q1 - makes it most recently used
      (cache/cache-get c "q1")

      ;; Add q4 - q2 should be evicted (now oldest accessed)
      (cache/cache-put c "q4" {:n 4})

      (is (= {:n 1} (cache/cache-get c "q1")) "q1 should still exist (was accessed)")
      (is (nil? (cache/cache-get c "q2")) "q2 should be evicted")
      (is (= {:n 3} (cache/cache-get c "q3")))
      (is (= {:n 4} (cache/cache-get c "q4"))))))

;;; ============================================================================
;;; Cache Key Types Tests
;;; ============================================================================

(deftest cache-key-types-test
  (testing "string keys"
    (let [c (cache/lru-cache 10)]
      (cache/cache-put c "{ hello }" {:parsed 1})
      (is (= {:parsed 1} (cache/cache-get c "{ hello }")))))

  (testing "vector keys (query + operation name)"
    (let [c (cache/lru-cache 10)]
      (cache/cache-put c ["query Q { x }" "Q"] {:parsed 2})
      (is (= {:parsed 2} (cache/cache-get c ["query Q { x }" "Q"])))))

  (testing "different key types don't collide"
    (let [c (cache/lru-cache 10)
          query "{ hello }"]
      (cache/cache-put c query {:type :no-op})
      (cache/cache-put c [query "Op"] {:type :with-op})

      (is (= {:type :no-op} (cache/cache-get c query)))
      (is (= {:type :with-op} (cache/cache-get c [query "Op"]))))))

;;; ============================================================================
;;; Thread Safety Tests
;;; ============================================================================

(deftest cache-thread-safety-test
  (testing "concurrent access doesn't corrupt cache"
    (let [c (cache/lru-cache 1000)
          threads 10
          ops-per-thread 100
          latch (java.util.concurrent.CountDownLatch. threads)]

      ;; Launch threads doing concurrent puts and gets
      (dotimes [t threads]
        (future
          (try
            (dotimes [i ops-per-thread]
              (let [key (str "thread-" t "-key-" i)]
                (cache/cache-put c key {:thread t :op i})
                (cache/cache-get c key)
                ;; Also read from other threads
                (cache/cache-get c (str "thread-" (mod (inc t) threads) "-key-" i))))
            (finally
              (.countDown latch)))))

      ;; Wait for all threads
      (.await latch 5 java.util.concurrent.TimeUnit/SECONDS)

      ;; Cache should still be functional
      (cache/cache-put c "final" {:done true})
      (is (= {:done true} (cache/cache-get c "final"))))))

;;; ============================================================================
;;; Edge Cases Tests
;;; ============================================================================

(deftest cache-edge-cases-test
  (testing "cache size of 1"
    (let [c (cache/lru-cache 1)]
      (cache/cache-put c "a" {:a 1})
      (is (= {:a 1} (cache/cache-get c "a")))

      (cache/cache-put c "b" {:b 2})
      (is (nil? (cache/cache-get c "a")))
      (is (= {:b 2} (cache/cache-get c "b")))))

  (testing "nil values can be cached"
    (let [c (cache/lru-cache 10)]
      (cache/cache-put c "nil-value" nil)
      ;; Note: cache-get returns nil for both missing and nil values
      ;; This is acceptable - parsed queries are never nil
      (is (nil? (cache/cache-get c "nil-value")))))

  (testing "assertion on invalid max-size"
    (is (thrown? AssertionError (cache/lru-cache 0)))
    (is (thrown? AssertionError (cache/lru-cache -1)))))
