(ns synthigy.graphql.adapter-test
  "Tests for Ring, http-kit, and Pedestal adapters."
  (:require
    [clojure.data.json :as json]
    [clojure.test :refer [deftest testing is]]
    [com.walmartlabs.lacinia.schema :as schema]
    [synthigy.graphql.adapter.httpkit :as httpkit]
    [synthigy.graphql.adapter.pedestal :as pedestal]
    [synthigy.graphql.adapter.ring :as ring]
    [synthigy.graphql.cache :as cache]
    [synthigy.graphql.core :as core]))

;;; ============================================================================
;;; Test Schema
;;; ============================================================================

(def test-schema
  (schema/compile
    {:queries
     {:hello {:type 'String :resolve (fn [_ _ _] "world")}
      :contextValue {:type 'String :resolve (fn [ctx _ _] (:test-value ctx))}}
     :mutations
     {:setMessage {:type 'String
                   :args {:msg {:type 'String}}
                   :resolve (fn [_ {:keys [msg]} _] msg)}}}))

(def test-request
  {:request-method :post
   :headers {"content-type" "application/json"}
   :body "{\"query\": \"{ hello }\"}"})

;;; ============================================================================
;;; Ring Adapter Tests
;;; ============================================================================

(deftest ring-graphql-handler-test
  (testing "creates working handler"
    (let [handler (ring/graphql-handler test-schema {})]
      (is (fn? handler))
      (let [resp (handler test-request)]
        (is (= 200 (:status resp)))
        (is (= {:data {:hello "world"}}
               (json/read-str (:body resp) :key-fn keyword)))))))

(deftest ring-wrap-graphql-test
  (testing "handles requests at specified path"
    (let [fallback (fn [_] {:status 404 :body "Not found"})
          handler (ring/wrap-graphql fallback "/graphql" test-schema {})]

      ;; Request to GraphQL path
      (let [resp (handler (assoc test-request :uri "/graphql"))]
        (is (= 200 (:status resp))))

      ;; Request to other path falls through
      (let [resp (handler (assoc test-request :uri "/other"))]
        (is (= 404 (:status resp)))))))

;;; ============================================================================
;;; http-kit Adapter Tests
;;; ============================================================================

(deftest httpkit-graphql-handler-test
  (testing "creates working sync handler"
    (let [handler (httpkit/graphql-handler test-schema {})]
      (is (fn? handler))
      (let [resp (handler test-request)]
        (is (= 200 (:status resp)))))))

(deftest httpkit-wrap-graphql-test
  (testing "handles requests at specified path"
    (let [fallback (fn [_] {:status 404})
          handler (httpkit/wrap-graphql fallback "/graphql" test-schema {})]

      (let [resp (handler (assoc test-request :uri "/graphql"))]
        (is (= 200 (:status resp))))

      (let [resp (handler (assoc test-request :uri "/other"))]
        (is (= 404 (:status resp)))))))

(deftest httpkit-async-handler-test
  (testing "async handler calls respond callback"
    (let [handler (httpkit/async-handler test-schema {})
          result (promise)]
      ;; Call async handler
      (handler test-request
               (fn [resp] (deliver result resp))
               (fn [ex] (deliver result ex)))

      ;; Wait for result
      (let [resp (deref result 5000 :timeout)]
        (is (not= :timeout resp))
        (is (= 200 (:status resp)))
        (is (= {:data {:hello "world"}}
               (json/read-str (:body resp) :key-fn keyword))))))

  (testing "async handler calls raise on error"
    ;; Create handler with invalid schema that will error
    (let [bad-schema (schema/compile
                       {:queries {:fail {:type 'String
                                         :resolve (fn [_ _ _]
                                                    (throw (Exception. "fail")))}}})
          handler (httpkit/async-handler bad-schema {})
          result (promise)]

      (handler {:request-method :post
                :headers {"content-type" "application/json"}
                :body "{\"query\": \"{ fail }\"}"}
               (fn [resp] (deliver result resp))
               (fn [ex] (deliver result [:error ex])))

      ;; Should get response (not raise) for resolver errors
      (let [resp (deref result 5000 :timeout)]
        (is (not= :timeout resp))
        ;; Resolver errors return response, not raise
        (is (map? resp))))))

;;; ============================================================================
;;; Pedestal Adapter Tests
;;; ============================================================================

(deftest pedestal-graphql-interceptor-test
  (testing "creates single interceptor"
    (let [interceptor (pedestal/graphql-interceptor test-schema {})]
      (is (= ::pedestal/graphql (:name interceptor)))
      (is (fn? (:enter interceptor)))))

  (testing "interceptor processes request"
    (let [interceptor (pedestal/graphql-interceptor test-schema {})
          ctx {:request test-request}
          result ((:enter interceptor) ctx)]
      (is (contains? result :response))
      (is (= 200 (get-in result [:response :status]))))))

(deftest pedestal-parse-request-interceptor-test
  (testing "extracts GraphQL parameters"
    (let [ctx {:request test-request}
          result ((:enter pedestal/parse-request-interceptor) ctx)]
      (is (= "{ hello }" (get-in result [:request :graphql/query])))))

  (testing "short-circuits on parse error"
    (let [ctx {:request {:request-method :post
                         :headers {"content-type" "application/json"}
                         :body "invalid"}}
          result ((:enter pedestal/parse-request-interceptor) ctx)]
      (is (contains? result :response))
      (is (= 400 (get-in result [:response :status])))))

  (testing "leave cleans up request keys"
    (let [ctx {:request {:graphql/query "q"
                         :graphql/variables {}
                         :graphql/operation-name "op"}}
          result ((:leave pedestal/parse-request-interceptor) ctx)]
      (is (not (contains? (:request result) :graphql/query)))
      (is (not (contains? (:request result) :graphql/variables)))
      (is (not (contains? (:request result) :graphql/operation-name))))))

(deftest pedestal-context-interceptor-test
  (testing "adds context from context-fn"
    (let [interceptor (pedestal/context-interceptor
                        (fn [_req] {:user "alice" :db :test-db}))
          ctx {:request {}}
          result ((:enter interceptor) ctx)]
      (is (= {:user "alice" :db :test-db}
             (get-in result [:request :graphql/context])))))

  (testing "leave cleans up context"
    (let [interceptor (pedestal/context-interceptor (constantly {}))
          ctx {:request {:graphql/context {:data true}}}
          result ((:leave interceptor) ctx)]
      (is (not (contains? (:request result) :graphql/context))))))

(deftest pedestal-execute-interceptor-test
  (testing "executes query and sets response"
    (let [interceptor (pedestal/execute-interceptor test-schema {})
          ctx {:request {:graphql/query "{ hello }"
                         :graphql/variables nil
                         :graphql/operation-name nil
                         :graphql/context {}
                         :headers {}}}
          result ((:enter interceptor) ctx)]
      (is (contains? result :response))
      (is (= 200 (get-in result [:response :status]))))))

(deftest pedestal-default-interceptors-test
  (testing "returns vector of interceptors"
    (let [interceptors (pedestal/default-interceptors test-schema {})]
      (is (vector? interceptors))
      (is (= 3 (count interceptors)))))

  (testing "interceptor chain processes request"
    (let [interceptors (pedestal/default-interceptors
                         test-schema
                         {:context-fn (fn [_] {:test-value "ctx-value"})
                          :cache (cache/lru-cache 10)})
          ;; Simulate Pedestal interceptor chain execution
          ctx {:request {:request-method :post
                         :headers {"content-type" "application/json"}
                         :body "{\"query\": \"{ contextValue }\"}"}}
          result (reduce (fn [c interceptor]
                           (if-let [enter (:enter interceptor)]
                             (let [c' (enter c)]
                               (if (:response c')
                                 (reduced c')
                                 c'))
                             c))
                         ctx
                         interceptors)]
      (is (= 200 (get-in result [:response :status])))
      (is (= {:data {:contextValue "ctx-value"}}
             (json/read-str (get-in result [:response :body]) :key-fn keyword))))))

(deftest pedestal-from-lacinia-pedestal-test
  (testing "creates compatible interceptors"
    (let [interceptors (pedestal/from-lacinia-pedestal test-schema)]
      (is (= 3 (count interceptors)))))

  (testing "accepts app-context"
    (let [interceptors (pedestal/from-lacinia-pedestal
                         test-schema
                         {:app "context"})]
      (is (= 3 (count interceptors)))))

  (testing "accepts options with cache"
    (let [interceptors (pedestal/from-lacinia-pedestal
                         test-schema
                         nil
                         {:parsed-query-cache (cache/lru-cache 100)})]
      (is (= 3 (count interceptors))))))

;;; ============================================================================
;;; Integration Tests
;;; ============================================================================

(deftest adapter-consistency-test
  (testing "all adapters return same result"
    (let [ring-handler (ring/graphql-handler test-schema {})
          httpkit-handler (httpkit/graphql-handler test-schema {})
          pedestal-interceptor (pedestal/graphql-interceptor test-schema {})

          ring-resp (ring-handler test-request)
          httpkit-resp (httpkit-handler test-request)
          pedestal-resp (get-in ((:enter pedestal-interceptor)
                                  {:request test-request})
                                [:response])]

      ;; All should return same status
      (is (= 200 (:status ring-resp) (:status httpkit-resp) (:status pedestal-resp)))

      ;; All should return same body
      (let [ring-body (json/read-str (:body ring-resp) :key-fn keyword)
            httpkit-body (json/read-str (:body httpkit-resp) :key-fn keyword)
            pedestal-body (json/read-str (:body pedestal-resp) :key-fn keyword)]
        (is (= ring-body httpkit-body pedestal-body))))))

(deftest adapter-mutation-over-get-test
  (testing "all adapters reject mutations over GET with 405"
    (let [mutation-request {:request-method :get
                            :headers {}
                            :params {:query "mutation { setMessage(msg: \"x\") }"}}
          ring-handler (ring/graphql-handler test-schema {})
          httpkit-handler (httpkit/graphql-handler test-schema {})
          pedestal-interceptors (pedestal/default-interceptors test-schema {})

          ring-resp (ring-handler mutation-request)
          httpkit-resp (httpkit-handler mutation-request)
          ;; Run through Pedestal interceptor chain
          pedestal-resp (get-in
                          (reduce (fn [c interceptor]
                                    (if-let [enter (:enter interceptor)]
                                      (let [c' (enter c)]
                                        (if (:response c') (reduced c') c'))
                                      c))
                                  {:request mutation-request}
                                  pedestal-interceptors)
                          [:response])]

      (is (= 405 (:status ring-resp)) "Ring should reject mutation over GET")
      (is (= 405 (:status httpkit-resp)) "http-kit should reject mutation over GET")
      (is (= 405 (:status pedestal-resp)) "Pedestal should reject mutation over GET"))))
