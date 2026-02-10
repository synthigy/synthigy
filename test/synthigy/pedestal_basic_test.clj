(ns synthigy.pedestal-basic-test
  "Basic Pedestal server tests without external HTTP client.

  Tests server lifecycle, route configuration, and interceptor setup."
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [io.pedestal.http :as http]
   [io.pedestal.http.route :as route]
   [synthigy.pedestal :as pedestal]
   [synthigy.iam.encryption :as encryption]
   [synthigy.test-helper :as test-helper]))

(use-fixtures :once test-helper/system-fixture)

;; =============================================================================
;; Server Lifecycle Tests
;; =============================================================================

(deftest test-server-lifecycle
  (testing "Server can start and stop"
    ;; Ensure encryption keys exist
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    ;; Start server
    (pedestal/start {:host "localhost"
                     :port 18081
                     :spa-root nil
                     :info {:test true}})

    (is (some? @pedestal/server) "Server should be running")

    ;; Server reference should be a proper Pedestal server
    (is (map? @pedestal/server))
    (is (contains? @pedestal/server ::http/server))

    ;; Stop server
    (pedestal/stop)

    (is (nil? @pedestal/server) "Server should be stopped")))

(deftest test-server-configuration
  (testing "Server is configured correctly"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (pedestal/start {:host "localhost"
                     :port 18082
                     :spa-root nil
                     :info {:version "test"}})

    (let [server @pedestal/server]
      ;; Check server has expected configuration
      (is (= "localhost" (::http/host server)))
      (is (= 18082 (::http/port server)))
      (is (= :jetty (::http/type server)))
      (is (false? (::http/join? server))))

    (pedestal/stop)))

;; =============================================================================
;; Route Configuration Tests
;; =============================================================================

(deftest test-oauth-routes-registered
  (testing "OAuth routes are registered in route table"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (pedestal/start {:host "localhost"
                     :port 18083
                     :spa-root nil})

    (let [server @pedestal/server
          ;; Get the router from interceptors
          interceptors (::http/interceptors server)
          ;; Find the router interceptor
          router-interceptor (some #(when (= ::route/router (:name %)) %) interceptors)]

      (is (some? router-interceptor) "Router interceptor should exist")

      ;; Verify OAuth routes exist by checking route names
      (let [route-table (get-in server [::http/routes])
            route-names (when route-table (set (map :route-name route-table)))]

        (when route-names
          (is (contains? route-names ::pedestal/oauth-authorize) "OAuth authorize route exists")
          (is (contains? route-names ::pedestal/oauth-token) "OAuth token route exists")
          (is (contains? route-names ::pedestal/oidc-userinfo) "OIDC userinfo route exists")
          (is (contains? route-names ::pedestal/oidc-jwks) "OIDC JWKS route exists")
          (is (contains? route-names ::pedestal/oidc-discovery) "OIDC discovery route exists")
          (is (contains? route-names ::pedestal/graphql-api) "GraphQL route exists"))))

    (pedestal/stop)))

;; =============================================================================
;; Interceptor Chain Tests
;; =============================================================================

(deftest test-interceptor-chain
  (testing "Interceptor chain is configured correctly"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (pedestal/start {:host "localhost"
                     :port 18084
                     :spa-root nil})

    (let [server @pedestal/server
          interceptors (::http/interceptors server)]

      (is (vector? interceptors) "Interceptors should be a vector")
      (is (seq interceptors) "Should have at least one interceptor")

      ;; Check for key interceptors
      (let [interceptor-names (set (map :name interceptors))]
        ;; Should have CORS
        (is (some #(re-find #"cors" (str %)) interceptor-names)
            "Should have CORS interceptor")

        ;; Should have router
        (is (contains? interceptor-names ::route/router)
            "Should have router interceptor")))

    (pedestal/stop)))

;; =============================================================================
;; SPA Configuration Tests
;; =============================================================================

(deftest test-spa-interceptor-optional
  (testing "SPA interceptor is optional"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    ;; Start without SPA
    (pedestal/start {:host "localhost"
                     :port 18085
                     :spa-root nil})

    (let [server @pedestal/server
          interceptors (::http/interceptors server)
          interceptor-names (set (map :name interceptors))]

      ;; Should NOT have SPA interceptor
      (is (not (some #(re-find #"spa" (str %)) interceptor-names))
          "Should not have SPA interceptor when spa-root is nil"))

    (pedestal/stop)))

(deftest test-spa-interceptor-included
  (testing "SPA interceptor is included when spa-root is provided"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    ;; Create a temporary SPA root directory
    (let [temp-dir (str (System/getProperty "java.io.tmpdir") "/test-spa-" (System/currentTimeMillis))]
      (.mkdir (java.io.File. temp-dir))

      (pedestal/start {:host "localhost"
                       :port 18086
                       :spa-root temp-dir})

      (let [server @pedestal/server
            interceptors (::http/interceptors server)
            interceptor-names (set (map :name interceptors))]

        ;; Should have SPA interceptor
        (is (some #(re-find #"spa" (str %)) interceptor-names)
            "Should have SPA interceptor when spa-root is provided"))

      (pedestal/stop)

      ;; Cleanup
      (.delete (java.io.File. temp-dir)))))

;; =============================================================================
;; Multiple Start/Stop Cycles
;; =============================================================================

(deftest test-multiple-start-stop-cycles
  (testing "Server can be started and stopped multiple times"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (dotimes [i 3]
      (pedestal/start {:host "localhost"
                       :port (+ 18090 i)
                       :spa-root nil})

      (is (some? @pedestal/server) (str "Server should start on cycle " (inc i)))

      (pedestal/stop)

      (is (nil? @pedestal/server) (str "Server should stop on cycle " (inc i))))))
