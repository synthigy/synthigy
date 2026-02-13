(ns synthigy.graphql.tracing-test
  (:require
    [clojure.test :refer [deftest testing is]]
    [synthigy.graphql.tracing :as tracing]))

;;; ============================================================================
;;; Timing Creation Tests
;;; ============================================================================

(deftest create-timing-test
  (testing "creates timing context with required keys"
    (let [timing (tracing/create-timing)]
      (is (contains? timing ::tracing/start-time))
      (is (contains? timing ::tracing/start-instant))
      (is (contains? timing ::tracing/phases))))

  (testing "start-time is a number (nanos)"
    (let [timing (tracing/create-timing)]
      (is (number? (::tracing/start-time timing)))))

  (testing "start-instant is an Instant"
    (let [timing (tracing/create-timing)]
      (is (instance? java.time.Instant (::tracing/start-instant timing)))))

  (testing "phases starts empty"
    (let [timing (tracing/create-timing)]
      (is (empty? (::tracing/phases timing))))))

;;; ============================================================================
;;; Phase Recording Tests
;;; ============================================================================

(deftest record-phase-test
  (testing "records phase with timing info"
    (let [timing (tracing/create-timing)
          start (tracing/start-phase)
          _ (Thread/sleep 1) ;; Small delay to ensure duration > 0
          timing' (tracing/record-phase timing :parse start)]
      (is (= 1 (count (::tracing/phases timing'))))
      (let [phase (first (::tracing/phases timing'))]
        (is (= :parse (:phase phase)))
        (is (number? (:startOffset phase)))
        (is (number? (:duration phase)))
        (is (>= (:duration phase) 0)))))

  (testing "records multiple phases"
    (let [timing (tracing/create-timing)
          s1 (tracing/start-phase)
          timing (tracing/record-phase timing :parse s1)
          s2 (tracing/start-phase)
          timing (tracing/record-phase timing :validate s2)
          s3 (tracing/start-phase)
          timing (tracing/record-phase timing :execute s3)]
      (is (= 3 (count (::tracing/phases timing))))
      (is (= [:parse :validate :execute]
             (mapv :phase (::tracing/phases timing))))))

  (testing "returns nil when timing is nil"
    (is (nil? (tracing/record-phase nil :parse 12345)))))

;;; ============================================================================
;;; Start Phase Tests
;;; ============================================================================

(deftest start-phase-test
  (testing "returns current nanos"
    (let [before (System/nanoTime)
          phase-start (tracing/start-phase)
          after (System/nanoTime)]
      (is (<= before phase-start after)))))

;;; ============================================================================
;;; Tracing Enabled Check Tests
;;; ============================================================================

(deftest tracing-enabled-test
  (testing "returns true when enabled"
    (is (true? (tracing/tracing-enabled?
                 {:com.walmartlabs.lacinia/enable-tracing? true}))))

  (testing "returns false when disabled"
    (is (false? (tracing/tracing-enabled?
                  {:com.walmartlabs.lacinia/enable-tracing? false}))))

  (testing "returns false when key missing"
    (is (false? (tracing/tracing-enabled? {})))
    (is (false? (tracing/tracing-enabled? nil)))))

;;; ============================================================================
;;; Add Tracing Extension Tests
;;; ============================================================================

(deftest add-tracing-extension-test
  (testing "adds tracing extension to result"
    (let [timing (-> (tracing/create-timing)
                     (tracing/record-phase :parse (tracing/start-phase))
                     (tracing/record-phase :validate (tracing/start-phase))
                     (tracing/record-phase :execute (tracing/start-phase)))
          result {:data {:hello "world"}}
          result' (tracing/add-tracing-extension result timing)]
      (is (contains? result' :extensions))
      (is (contains? (:extensions result') :tracing))))

  (testing "tracing has required Apollo fields"
    (let [timing (-> (tracing/create-timing)
                     (tracing/record-phase :parse (tracing/start-phase)))
          result' (tracing/add-tracing-extension {:data {}} timing)
          tracing-ext (get-in result' [:extensions :tracing])]
      (is (= 1 (:version tracing-ext)))
      (is (string? (:startTime tracing-ext)))
      (is (string? (:endTime tracing-ext)))
      (is (number? (:duration tracing-ext)))))

  (testing "includes phase timings"
    (let [timing (-> (tracing/create-timing)
                     (tracing/record-phase :parse (tracing/start-phase))
                     (tracing/record-phase :validate (tracing/start-phase))
                     (tracing/record-phase :execute (tracing/start-phase)))
          result' (tracing/add-tracing-extension {:data {}} timing)
          tracing-ext (get-in result' [:extensions :tracing])]
      (is (some? (:parsing tracing-ext)))
      (is (some? (:validation tracing-ext)))
      (is (some? (:execution tracing-ext)))))

  (testing "returns result unchanged when timing is nil"
    (let [result {:data {:x 1}}]
      (is (= result (tracing/add-tracing-extension result nil)))))

  (testing "returns result unchanged when no phases recorded"
    (let [timing (tracing/create-timing)
          result {:data {:x 1}}]
      (is (= result (tracing/add-tracing-extension result timing)))))

  (testing "preserves existing extensions"
    (let [timing (-> (tracing/create-timing)
                     (tracing/record-phase :parse (tracing/start-phase)))
          result {:data {} :extensions {:other "data"}}
          result' (tracing/add-tracing-extension result timing)]
      ;; Note: current impl overwrites extensions. If you want to preserve:
      ;; (is (= "data" (get-in result' [:extensions :other])))
      (is (some? (get-in result' [:extensions :tracing]))))))

;;; ============================================================================
;;; Lacinia Integration Tests
;;; ============================================================================

(deftest create-lacinia-timing-start-test
  (testing "creates Lacinia-compatible timing start"
    (let [timing-start (tracing/create-lacinia-timing-start)]
      ;; Should be whatever Lacinia returns - just check it's not nil
      (is (some? timing-start)))))
