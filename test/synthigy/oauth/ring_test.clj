(ns synthigy.oauth.ring-test
  "Unit tests for Ring utilities in synthigy.oauth.ring"
  (:require
   [clojure.test :refer [deftest testing is]]
   [synthigy.oauth.ring :as ring]))

;; =============================================================================
;; Middleware Composition Tests
;; =============================================================================

(deftest test-compose-middleware
  (testing "Middleware composition left-to-right"
    (let [add-a (fn [handler] (fn [req] (handler (update req :value (fnil conj []) :a))))
          add-b (fn [handler] (fn [req] (handler (update req :value (fnil conj []) :b))))
          add-c (fn [handler] (fn [req] (handler (update req :value (fnil conj []) :c))))
          handler (fn [req] (:value req))
          composed (ring/compose-middleware add-a add-b add-c handler)]
      (is (= [:a :b :c] (composed {})))))

  (testing "Single middleware composition"
    (let [add-one (fn [handler] (fn [req] (handler (update req :count inc))))
          handler (fn [req] (:count req))
          composed (ring/compose-middleware add-one handler)]
      (is (= 1 (composed {:count 0})))))

  (testing "Empty middleware composition returns identity"
    (let [composed (ring/compose-middleware)]
      (is (= {:foo :bar} (composed {:foo :bar}))))))

;; =============================================================================
;; State Passing Tests
;; =============================================================================

(deftest test-pass-state
  (testing "Pass custom state through request map"
    (let [request {:params {:username "alice"}}
          request' (ring/pass-state request ::session "session-123")]
      (is (= "session-123" (::session request')))
      (is (= {:username "alice"} (:params request')))))

  (testing "Pass multiple state keys"
    (let [request {}
          request' (-> request
                       (ring/pass-state ::session "session-123")
                       (ring/pass-state ::user "alice")
                       (ring/pass-state ::flow :authorization-code))]
      (is (= "session-123" (::session request')))
      (is (= "alice" (::user request')))
      (is (= :authorization-code (::flow request'))))))

(deftest test-get-state
  (testing "Get custom state from request map"
    (let [request {::session "session-123" ::user "alice"}]
      (is (= "session-123" (ring/get-state request ::session)))
      (is (= "alice" (ring/get-state request ::user)))))

  (testing "Get state returns nil for missing keys"
    (let [request {}]
      (is (nil? (ring/get-state request ::session)))
      (is (nil? (ring/get-state request ::missing))))))

(deftest test-update-state
  (testing "Update custom state with function"
    (let [request {::counter 0}
          request' (ring/update-state request ::counter inc)]
      (is (= 1 (::counter request')))))

  (testing "Update state with args"
    (let [request {::list []}
          request' (ring/update-state request ::list conj :a :b :c)]
      (is (= [:a :b :c] (::list request')))))

  (testing "Update state creates key if missing"
    (let [request {}
          request' (ring/update-state request ::counter (fnil inc 0))]
      (is (= 1 (::counter request'))))))

;; =============================================================================
;; Parameter Utilities Tests
;; =============================================================================

(deftest test-keywordize-params
  (testing "Keywordize string keys"
    (is (= {:username "alice" :password "secret"}
           (ring/keywordize-params {"username" "alice" "password" "secret"}))))

  (testing "Keywordize nested maps"
    (is (= {:user {:name "alice" :age 30}}
           (ring/keywordize-params {"user" {"name" "alice" "age" 30}}))))

  (testing "Keywordize preserves keyword keys"
    (is (= {:username "alice"}
           (ring/keywordize-params {:username "alice"}))))

  (testing "Keywordize empty map"
    (is (= {} (ring/keywordize-params {})))))

(deftest test-stringify-params
  (testing "Stringify keyword keys"
    (is (= {"username" "alice" "password" "secret"}
           (ring/stringify-params {:username "alice" :password "secret"}))))

  (testing "Stringify nested maps"
    (is (= {"user" {"name" "alice" "age" 30}}
           (ring/stringify-params {:user {:name "alice" :age 30}}))))

  (testing "Stringify preserves string keys"
    (is (= {"username" "alice"}
           (ring/stringify-params {"username" "alice"}))))

  (testing "Stringify empty map"
    (is (= {} (ring/stringify-params {})))))

;; =============================================================================
;; Response Utilities Tests
;; =============================================================================

(deftest test-set-cookie
  (testing "Set cookie with value only"
    (let [response {:status 200 :body "OK"}
          response' (ring/set-cookie response "session" "abc123")]
      (is (= "abc123" (get-in response' [:cookies "session" :value])))))

  (testing "Set cookie with options"
    (let [response {:status 200 :body "OK"}
          response' (ring/set-cookie response "session" "abc123"
                                     {:http-only true
                                      :secure true
                                      :same-site :strict})]
      (is (= "abc123" (get-in response' [:cookies "session" :value])))
      (is (true? (get-in response' [:cookies "session" :http-only])))
      (is (true? (get-in response' [:cookies "session" :secure])))
      (is (= :strict (get-in response' [:cookies "session" :same-site])))))

  (testing "Set multiple cookies"
    (let [response {:status 200 :body "OK"}
          response' (-> response
                        (ring/set-cookie "session" "abc123")
                        (ring/set-cookie "prefs" "theme=dark"))]
      (is (= "abc123" (get-in response' [:cookies "session" :value])))
      (is (= "theme=dark" (get-in response' [:cookies "prefs" :value]))))))

(deftest test-delete-cookie
  (testing "Delete cookie sets max-age to 0"
    (let [response {:status 200 :body "OK"}
          response' (ring/delete-cookie response "session")]
      (is (= "" (get-in response' [:cookies "session" :value])))
      (is (= 0 (get-in response' [:cookies "session" :max-age])))
      (is (= "/" (get-in response' [:cookies "session" :path])))))

  (testing "Delete multiple cookies"
    (let [response {:status 200 :body "OK"}
          response' (-> response
                        (ring/delete-cookie "session")
                        (ring/delete-cookie "prefs"))]
      (is (= 0 (get-in response' [:cookies "session" :max-age])))
      (is (= 0 (get-in response' [:cookies "prefs" :max-age]))))))

(deftest test-merge-cookies
  (testing "Merge multiple cookies into response"
    (let [response {:status 200 :body "OK"}
          cookies {"session" {:value "abc123" :http-only true}
                   "prefs" {:value "theme=dark" :max-age 3600}}
          response' (ring/merge-cookies response cookies)]
      (is (= "abc123" (get-in response' [:cookies "session" :value])))
      (is (= "theme=dark" (get-in response' [:cookies "prefs" :value])))
      (is (true? (get-in response' [:cookies "session" :http-only])))
      (is (= 3600 (get-in response' [:cookies "prefs" :max-age])))))

  (testing "Merge cookies preserves existing cookies"
    (let [response {:status 200 :cookies {"existing" {:value "keep-me"}}}
          cookies {"session" {:value "abc123"}}
          response' (ring/merge-cookies response cookies)]
      (is (= "keep-me" (get-in response' [:cookies "existing" :value])))
      (is (= "abc123" (get-in response' [:cookies "session" :value]))))))

;; =============================================================================
;; Short-Circuit Tests
;; =============================================================================

(deftest test-short-circuit?
  (testing "Detects valid response maps"
    (is (true? (ring/short-circuit? {:status 200 :body "OK"})))
    (is (true? (ring/short-circuit? {:status 302 :headers {"Location" "/"}})))
    (is (true? (ring/short-circuit? {:status 404}))))

  (testing "Returns false for non-response maps"
    (is (false? (ring/short-circuit? {})))
    (is (false? (ring/short-circuit? {:body "OK"})))
    (is (false? (ring/short-circuit? {:headers {}}))))

  (testing "Returns false for non-maps"
    (is (false? (ring/short-circuit? nil)))
    (is (false? (ring/short-circuit? "not a map")))
    (is (false? (ring/short-circuit? 42)))))

;; =============================================================================
;; Integration Tests (Middleware Pattern Examples)
;; =============================================================================

(deftest test-middleware-pattern-with-state
  (testing "Middleware pattern: pass state through request"
    (let [wrap-add-session (fn [handler]
                             (fn [request]
                               (let [session-id "generated-session-123"]
                                 (handler (ring/pass-state request ::session session-id)))))
          my-handler (fn [request]
                       {:status 200
                        :body (str "Session: " (ring/get-state request ::session))})
          app (wrap-add-session my-handler)
          response (app {})]
      (is (= 200 (:status response)))
      (is (= "Session: generated-session-123" (:body response))))))

(deftest test-middleware-pattern-with-cookies
  (testing "Middleware pattern: set cookie in response"
    (let [login-handler (fn [request]
                          (let [session-id "session-abc123"
                                response {:status 302 :headers {"Location" "/"}}]
                            (ring/set-cookie response "idsrv.session" session-id
                                             {:http-only true
                                              :secure true
                                              :same-site :none})))
          response (login-handler {})]
      (is (= 302 (:status response)))
      (is (= "session-abc123" (get-in response [:cookies "idsrv.session" :value])))
      (is (true? (get-in response [:cookies "idsrv.session" :http-only])))
      (is (true? (get-in response [:cookies "idsrv.session" :secure])))
      (is (= :none (get-in response [:cookies "idsrv.session" :same-site]))))))

(deftest test-middleware-pattern-short-circuit
  (testing "Middleware pattern: short-circuit on authentication failure"
    (let [wrap-auth (fn [handler]
                      (fn [request]
                        (if (get-in request [:headers "authorization"])
                          (handler request)
                          {:status 401 :body "Unauthorized"})))
          my-handler (fn [_] {:status 200 :body "OK"})
          app (wrap-auth my-handler)]
      ;; Without auth header - short circuit
      (is (= 401 (:status (app {}))))
      (is (= "Unauthorized" (:body (app {}))))
      ;; With auth header - continue
      (is (= 200 (:status (app {:headers {"authorization" "Bearer token"}}))))
      (is (= "OK" (:body (app {:headers {"authorization" "Bearer token"}})))))))
