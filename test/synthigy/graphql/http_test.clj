(ns synthigy.graphql.http-test
  "HTTP handler tests using stub Lacinia schema."
  (:require
    [synthigy.json :as json]
    [clojure.test :refer [deftest testing is use-fixtures]]
    [com.walmartlabs.lacinia.schema :as schema]
    [synthigy.graphql.cache :as cache]
    [synthigy.graphql.core :as core]
    [synthigy.graphql.http :as http]))

;;; ============================================================================
;;; Test Schema
;;; ============================================================================

(def test-schema
  "Simple schema for testing - no database required."
  (schema/compile
    {:queries
     {:hello
      {:type 'String
       :resolve (fn [_ _ _] "world")}

      :echo
      {:type 'String
       :args {:message {:type 'String}}
       :resolve (fn [_ {:keys [message]} _] message)}

      :contextValue
      {:type 'String
       :resolve (fn [ctx _ _] (:test-value ctx))}

      :throwError
      {:type 'String
       :resolve (fn [_ _ _] (throw (Exception. "Intentional error")))}}

     :mutations
     {:setMessage
      {:type 'String
       :args {:msg {:type 'String}}
       :resolve (fn [_ {:keys [msg]} _] msg)}}

     :subscriptions
     {:events
      {:type 'String
       :stream (fn [_ _ _] nil)}}}))

;;; ============================================================================
;;; Request Parsing Tests
;;; ============================================================================

(deftest parse-request-test
  (testing "POST with JSON body"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/json"}
                    :body "{\"query\": \"{ hello }\", \"variables\": {\"x\": 1}, \"operationName\": \"Q\"}"})]
      (is (= "{ hello }" (:query result)))
      (is (= {:x 1} (:variables result)))
      (is (= "Q" (:operation-name result)))))

  (testing "POST with JSON body and charset"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/json; charset=utf-8"}
                    :body "{\"query\": \"{ hello }\"}"})]
      (is (= "{ hello }" (:query result)))))

  (testing "POST with invalid JSON"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/json"}
                    :body "not json"})]
      (is (core/early-error? result))
      (is (= 400 (::core/status result)))))

  (testing "POST with application/graphql"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/graphql"}
                    :body "{ hello }"
                    :params {:variables "{\"x\": 1}"}})]
      (is (= "{ hello }" (:query result)))
      (is (= {:x 1} (:variables result)))))

  (testing "GET request"
    (let [result (http/parse-request
                   {:request-method :get
                    :headers {}
                    :params {:query "{ hello }"
                             :variables "{\"x\": 1}"
                             :operationName "Q"}})]
      (is (= "{ hello }" (:query result)))
      (is (= {:x 1} (:variables result)))
      (is (= "Q" (:operation-name result)))))

  (testing "POST with unsupported content type"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "text/plain"}
                    :body "{ hello }"})]
      (is (core/early-error? result))
      (is (= 415 (::core/status result)))))

  (testing "unsupported method"
    (let [result (http/parse-request
                   {:request-method :put
                    :headers {"content-type" "application/json"}
                    :body "{\"query\": \"{ hello }\"}"})]
      (is (core/early-error? result))
      (is (= 405 (::core/status result)))))

  ;; GraphQL over HTTP spec compliance tests
  (testing "POST without Content-Type header returns 400"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {}
                    :body "{\"query\": \"{ hello }\"}"})]
      (is (core/early-error? result))
      (is (= 400 (::core/status result)))))

  (testing "POST with empty body returns 400"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/json"}
                    :body ""})]
      (is (core/early-error? result))
      (is (= 400 (::core/status result)))))

  (testing "query parameter must be string"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/json"}
                    :body "{\"query\": 123}"})]
      (is (core/early-error? result))
      (is (= 400 (::core/status result)))))

  (testing "variables parameter must be object"
    (let [result (http/parse-request
                   {:request-method :post
                    :headers {"content-type" "application/json"}
                    :body "{\"query\": \"{ hello }\", \"variables\": \"not-an-object\"}"})]
      (is (core/early-error? result))
      (is (= 400 (::core/status result))))))

;;; ============================================================================
;;; Query Parsing Tests
;;; ============================================================================

(deftest parse-query-test
  (testing "parses valid query"
    (let [result (http/parse-query test-schema "{ hello }" nil nil nil)]
      (is (contains? result :parsed))
      (is (not (:cached? result)))))

  (testing "returns error for blank query"
    (let [result (http/parse-query test-schema "" nil nil nil)]
      (is (core/early-error? result))))

  (testing "returns error for nil query"
    (let [result (http/parse-query test-schema nil nil nil nil)]
      (is (core/early-error? result))))

  (testing "returns error for invalid syntax"
    (let [result (http/parse-query test-schema "{ invalid syntax {{" nil nil nil)]
      (is (core/early-error? result))))

  (testing "caches parsed query"
    (let [c (cache/lru-cache 10)
          result1 (http/parse-query test-schema "{ hello }" nil c nil)
          result2 (http/parse-query test-schema "{ hello }" nil c nil)]
      (is (not (:cached? result1)))
      (is (:cached? result2))))

  (testing "different operation names use different cache keys"
    (let [c (cache/lru-cache 10)
          query "query Op { hello }"
          _ (http/parse-query test-schema query "Op1" c nil)
          result (http/parse-query test-schema query "Op2" c nil)]
      (is (not (:cached? result))))))

;;; ============================================================================
;;; Operation Check Tests
;;; ============================================================================

(deftest check-operation-test
  (testing "allows queries over GET"
    (let [{:keys [parsed]} (http/parse-query test-schema "{ hello }" nil nil nil)]
      (is (nil? (http/check-operation parsed :get)))))

  (testing "allows queries over POST"
    (let [{:keys [parsed]} (http/parse-query test-schema "{ hello }" nil nil nil)]
      (is (nil? (http/check-operation parsed :post)))))

  (testing "allows mutations over POST"
    (let [{:keys [parsed]} (http/parse-query test-schema "mutation { setMessage(msg: \"x\") }" nil nil nil)]
      (is (nil? (http/check-operation parsed :post)))))

  (testing "rejects mutations over GET (spec: 405)"
    (let [{:keys [parsed]} (http/parse-query test-schema "mutation { setMessage(msg: \"x\") }" nil nil nil)
          result (http/check-operation parsed :get)]
      (is (core/early-error? result))
      (is (= 405 (::core/status result)))))

  (testing "rejects subscriptions over POST"
    (let [{:keys [parsed]} (http/parse-query test-schema "subscription { events }" nil nil nil)
          result (http/check-operation parsed :post)]
      (is (core/early-error? result))
      (is (= 400 (::core/status result)))))

  (testing "rejects subscriptions over GET"
    (let [{:keys [parsed]} (http/parse-query test-schema "subscription { events }" nil nil nil)
          result (http/check-operation parsed :get)]
      (is (core/early-error? result))
      (is (= 400 (::core/status result))))))

;; Legacy test - check-subscription still works
(deftest check-subscription-test
  (testing "allows queries"
    (let [{:keys [parsed]} (http/parse-query test-schema "{ hello }" nil nil nil)]
      (is (nil? (http/check-subscription parsed)))))

  (testing "allows mutations"
    (let [{:keys [parsed]} (http/parse-query test-schema "mutation { setMessage(msg: \"x\") }" nil nil nil)]
      (is (nil? (http/check-subscription parsed)))))

  (testing "rejects subscriptions"
    (let [{:keys [parsed]} (http/parse-query test-schema "subscription { events }" nil nil nil)
          result (http/check-subscription parsed)]
      (is (core/early-error? result))
      (is (= 400 (::core/status result))))))

;;; ============================================================================
;;; Response Formatting Tests
;;; ============================================================================

(deftest format-response-test
  (testing "formats successful response"
    (let [resp (http/format-response {:data {:hello "world"}})]
      (is (= 200 (:status resp)))
      (is (= "application/json" (get-in resp [:headers "Content-Type"])))
      (is (= {:data {:hello "world"}} (json/read-str (:body resp))))))

  (testing "formats error response"
    (let [resp (http/format-response {:errors [{:message "Error"}]})]
      (is (= 400 (:status resp)))
      (let [body (json/read-str (:body resp))]
        (is (= [{:message "Error"}] (:errors body))))))

  (testing "formats early error"
    (let [resp (http/format-response (core/error-response 415 "Bad content type"))]
      (is (= 415 (:status resp)))))

  (testing "extracts status from error extensions"
    (let [resp (http/format-response
                 {:data nil
                  :errors [{:message "Not found"
                            :extensions {:status 404}}]})]
      (is (= 404 (:status resp)))
      ;; Status should be removed from extensions in response
      (let [body (json/read-str (:body resp))]
        (is (nil? (get-in body [:errors 0 :extensions :status]))))))

  (testing "uses max status from multiple errors"
    (let [resp (http/format-response
                 {:data nil
                  :errors [{:message "E1" :extensions {:status 400}}
                           {:message "E2" :extensions {:status 500}}
                           {:message "E3" :extensions {:status 403}}]})]
      (is (= 500 (:status resp)))))

  (testing "preserves extensions in response"
    (let [resp (http/format-response {:data {:x 1} :extensions {:timing 123}})]
      (let [body (json/read-str (:body resp))]
        (is (= 123 (get-in body [:extensions :timing])))))))

;;; ============================================================================
;;; Full Handler Tests
;;; ============================================================================

(deftest handler-test
  (let [handler (http/handler test-schema {})]

    (testing "successful query"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"}
                           :body "{\"query\": \"{ hello }\"}"})]
        (is (= 200 (:status resp)))
        (is (= {:data {:hello "world"}}
               (json/read-str (:body resp))))))

    (testing "query with variables"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"}
                           :body "{\"query\": \"query E($m: String) { echo(message: $m) }\", \"variables\": {\"m\": \"test\"}}"})]
        (is (= 200 (:status resp)))
        (is (= {:data {:echo "test"}}
               (json/read-str (:body resp))))))

    (testing "mutation"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"}
                           :body "{\"query\": \"mutation { setMessage(msg: \\\"hello\\\") }\"}"})]
        (is (= 200 (:status resp)))
        (is (= {:data {:setMessage "hello"}}
               (json/read-str (:body resp))))))

    (testing "subscription rejected"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"}
                           :body "{\"query\": \"subscription { events }\"}"})]
        (is (= 400 (:status resp)))))

    (testing "mutation over GET rejected with 405"
      (let [resp (handler {:request-method :get
                           :headers {}
                           :params {:query "mutation { setMessage(msg: \"x\") }"}})]
        (is (= 405 (:status resp)))))

    (testing "query over GET allowed"
      (let [resp (handler {:request-method :get
                           :headers {}
                           :params {:query "{ hello }"}})]
        (is (= 200 (:status resp)))
        (is (= {:data {:hello "world"}}
               (json/read-str (:body resp))))))))

;;; ============================================================================
;;; Context Tests
;;; ============================================================================

(deftest handler-context-test
  (testing "context-fn provides resolver context"
    (let [handler (http/handler test-schema
                                {:context-fn (fn [_req] {:test-value "from-context"})})
          resp (handler {:request-method :post
                         :headers {"content-type" "application/json"}
                         :body "{\"query\": \"{ contextValue }\"}"})]
      (is (= 200 (:status resp)))
      (is (= {:data {:contextValue "from-context"}}
             (json/read-str (:body resp))))))

  (testing "request is available in context"
    (let [captured-request (atom nil)
          handler (http/handler test-schema
                                {:context-fn (fn [req]
                                               (reset! captured-request req)
                                               {})})
          req {:request-method :post
               :headers {"content-type" "application/json"}
               :body "{\"query\": \"{ hello }\"}"}]
      (handler req)
      (is (= :post (:request-method @captured-request))))))

;;; ============================================================================
;;; Tracing Tests
;;; ============================================================================

(deftest handler-tracing-test
  (let [handler (http/handler test-schema {:tracing-header "x-trace"})]

    (testing "tracing disabled by default"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"}
                           :body "{\"query\": \"{ hello }\"}"})
            body (json/read-str (:body resp))]
        (is (not (contains? body :extensions)))))

    (testing "tracing enabled with header"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"
                                     "x-trace" "true"}
                           :body "{\"query\": \"{ hello }\"}"})
            body (json/read-str (:body resp))]
        (is (contains? (:extensions body) :tracing))
        (is (= 1 (get-in body [:extensions :tracing :version])))))))

;;; ============================================================================
;;; Cache Integration Tests
;;; ============================================================================

(deftest handler-cache-test
  (let [parse-count (atom 0)
        ;; We can't easily count parses, but we can verify caching works
        c (cache/lru-cache 10)
        handler (http/handler test-schema {:cache c})]

    (testing "cache is used"
      ;; First request
      (handler {:request-method :post
                :headers {"content-type" "application/json"}
                :body "{\"query\": \"{ hello }\"}"})

      ;; Cache should have entry
      (is (some? (cache/cache-get c "{ hello }"))))))

;;; ============================================================================
;;; Error Handling Tests
;;; ============================================================================

(deftest handler-error-test
  (let [handler (http/handler test-schema {})]

    (testing "resolver error returns errors in response"
      (let [resp (handler {:request-method :post
                           :headers {"content-type" "application/json"}
                           :body "{\"query\": \"{ throwError }\"}"})
            body (json/read-str (:body resp))]
        ;; Still returns 200 for execution errors per GraphQL spec
        (is (= 200 (:status resp)))
        (is (some? (:errors body)))))))

;;; ============================================================================
;;; Schema Provider Tests
;;; ============================================================================

(deftest handler-schema-provider-test
  (testing "accepts function schema provider"
    (let [handler (http/handler (fn [] test-schema) {})
          resp (handler {:request-method :post
                         :headers {"content-type" "application/json"}
                         :body "{\"query\": \"{ hello }\"}"})]
      (is (= 200 (:status resp)))))

  (testing "accepts atom schema provider"
    (let [handler (http/handler (atom test-schema) {})
          resp (handler {:request-method :post
                         :headers {"content-type" "application/json"}
                         :body "{\"query\": \"{ hello }\"}"})]
      (is (= 200 (:status resp)))))

  (testing "function called on each request (hot reload)"
    (let [call-count (atom 0)
          provider (fn []
                     (swap! call-count inc)
                     test-schema)
          handler (http/handler provider {})]
      (handler {:request-method :post
                :headers {"content-type" "application/json"}
                :body "{\"query\": \"{ hello }\"}"})
      (handler {:request-method :post
                :headers {"content-type" "application/json"}
                :body "{\"query\": \"{ hello }\"}"})
      (is (= 2 @call-count)))))
