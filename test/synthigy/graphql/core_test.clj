(ns synthigy.graphql.core-test
  (:require
    [clojure.test :refer [deftest testing is are]]
    [synthigy.graphql.core :as core]))

;;; ============================================================================
;;; Content-Type Parsing Tests
;;; ============================================================================

(deftest parse-content-type-test
  (testing "simple content types"
    (is (= :application/json (core/parse-content-type "application/json")))
    (is (= :application/graphql (core/parse-content-type "application/graphql")))
    (is (= :text/plain (core/parse-content-type "text/plain"))))

  (testing "content type with charset parameter"
    (is (= :application/json (core/parse-content-type "application/json; charset=utf-8")))
    (is (= :application/json (core/parse-content-type "application/json;charset=utf-8")))
    (is (= :application/json (core/parse-content-type "application/json ; charset=UTF-8"))))

  (testing "case insensitivity"
    (is (= :application/json (core/parse-content-type "APPLICATION/JSON")))
    (is (= :application/json (core/parse-content-type "Application/Json"))))

  (testing "whitespace handling"
    (is (= :application/json (core/parse-content-type "  application/json  ")))
    (is (= :application/json (core/parse-content-type "application/json ; charset=utf-8"))))

  (testing "nil and empty"
    (is (nil? (core/parse-content-type nil)))
    (is (nil? (core/parse-content-type "")))
    (is (nil? (core/parse-content-type "   ")))))

;;; ============================================================================
;;; Cache Key Tests
;;; ============================================================================

(deftest cache-key-test
  (testing "query without operation name returns string"
    (is (= "{ hello }" (core/cache-key "{ hello }" nil)))
    (is (string? (core/cache-key "query { users }" nil))))

  (testing "query with operation name returns vector"
    (is (= ["{ hello }" "MyOp"] (core/cache-key "{ hello }" "MyOp")))
    (is (vector? (core/cache-key "query Q { x }" "Q"))))

  (testing "different operation names produce different keys"
    (let [query "query Op($id: ID!) { user(id: $id) { name } }"]
      (is (not= (core/cache-key query "Op1")
                (core/cache-key query "Op2"))))))

;;; ============================================================================
;;; Error Response Tests
;;; ============================================================================

(deftest error-response-test
  (testing "default status is 400"
    (let [resp (core/error-response "Something went wrong")]
      (is (= 400 (::core/status resp)))
      (is (= [{:message "Something went wrong"}] (::core/errors resp)))))

  (testing "custom status"
    (let [resp (core/error-response 500 "Internal error")]
      (is (= 500 (::core/status resp)))
      (is (= [{:message "Internal error"}] (::core/errors resp)))))

  (testing "errors-response with multiple errors"
    (let [errors [{:message "Error 1"} {:message "Error 2"}]
          resp (core/errors-response errors)]
      (is (= 400 (::core/status resp)))
      (is (= errors (::core/errors resp)))))

  (testing "errors-response with custom status"
    (let [resp (core/errors-response 422 [{:message "Validation failed"}])]
      (is (= 422 (::core/status resp))))))

;;; ============================================================================
;;; Early Error Detection Tests
;;; ============================================================================

(deftest early-error-test
  (testing "detects early errors"
    (is (true? (core/early-error? (core/error-response "test"))))
    (is (true? (core/early-error? {::core/errors [{:message "x"}]}))))

  (testing "normal results are not early errors"
    (is (false? (core/early-error? {:data {:hello "world"}})))
    (is (false? (core/early-error? {:data nil :errors [{:message "x"}]})))
    (is (false? (core/early-error? {})))))

;;; ============================================================================
;;; Schema Provider Protocol Tests
;;; ============================================================================

(deftest schema-provider-test
  (testing "function provider"
    (let [schema {:compiled true}
          provider (fn [] schema)]
      (is (= schema (core/get-schema provider)))))

  (testing "map provider (direct schema)"
    (let [schema {:compiled true}]
      (is (= schema (core/get-schema schema)))))

  (testing "atom provider"
    (let [schema {:compiled true}
          provider (atom schema)]
      (is (= schema (core/get-schema provider)))))

  (testing "ref provider"
    (let [schema {:compiled true}
          provider (ref schema)]
      (is (= schema (core/get-schema provider)))))

  (testing "var provider"
    (def ^:dynamic *test-schema* {:compiled true})
    (is (= {:compiled true} (core/get-schema #'*test-schema*))))

  (testing "function is called each time (for hot reload)"
    (let [call-count (atom 0)
          provider (fn [] (swap! call-count inc) {:version @call-count})]
      (is (= {:version 1} (core/get-schema provider)))
      (is (= {:version 2} (core/get-schema provider)))
      (is (= 2 @call-count)))))

;;; ============================================================================
;;; Throwable Conversion Tests
;;; ============================================================================

(deftest throwable->error-test
  (testing "converts exception to error map"
    (let [ex (Exception. "Something failed")
          error (core/throwable->error ex)]
      (is (map? error))
      (is (contains? error :message))
      (is (string? (:message error)))))

  (testing "preserves exception message"
    (let [error (core/throwable->error (Exception. "Specific error"))]
      (is (clojure.string/includes? (:message error) "Specific error")))))
