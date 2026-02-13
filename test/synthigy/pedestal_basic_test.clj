(ns synthigy.pedestal-basic-test
  "Basic Pedestal server tests without external HTTP client.

  Tests server lifecycle, route configuration, and interceptor setup."
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [io.pedestal.http :as http]
   [io.pedestal.http.route :as route]
   [synthigy.server :as server]
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
    (server/start {:host "localhost"
                   :port 18081
                   :spa-root nil
                   :info {:test true}})

    (is (some? @server/server) "Server should be running")

    ;; Server reference should be a proper Pedestal server
    (is (map? @server/server))
    (is (contains? @server/server ::http/server))

    ;; Stop server
    (server/stop)

    (is (nil? @server/server) "Server should be stopped")))

(deftest test-server-configuration
  (testing "Server is configured correctly"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (server/start {:host "localhost"
                   :port 18082
                   :spa-root nil
                   :info {:version "test"}})

    (let [s @server/server]
      ;; Check server has expected configuration
      (is (= "localhost" (::http/host s)))
      (is (= 18082 (::http/port s)))
      (is (= :jetty (::http/type s)))
      (is (false? (::http/join? s))))

    (server/stop)))

;; =============================================================================
;; Route Configuration Tests
;; =============================================================================

(deftest test-oauth-routes-registered
  (testing "OAuth routes are registered in route table"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (server/start {:host "localhost"
                   :port 18083
                   :spa-root nil})

    (let [s @server/server
          ;; Get the router from interceptors
          interceptors (::http/interceptors s)
          ;; Find the router interceptor
          router-interceptor (some #(when (= ::route/router (:name %)) %) interceptors)]

      (is (some? router-interceptor) "Router interceptor should exist")

      ;; Verify OAuth routes exist by checking route names
      (let [route-table (get-in s [::http/routes])
            route-names (when route-table (set (map :route-name route-table)))]

        (when route-names
          (is (contains? route-names ::server/oauth-authorize) "OAuth authorize route exists")
          (is (contains? route-names ::server/oauth-token) "OAuth token route exists")
          (is (contains? route-names ::server/oidc-userinfo) "OIDC userinfo route exists")
          (is (contains? route-names ::server/oidc-jwks) "OIDC JWKS route exists")
          (is (contains? route-names ::server/oidc-discovery) "OIDC discovery route exists")
          (is (contains? route-names ::server/graphql-api) "GraphQL route exists"))))

    (server/stop)))

;; =============================================================================
;; Interceptor Chain Tests
;; =============================================================================

(deftest test-interceptor-chain
  (testing "Interceptor chain is configured correctly"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    (server/start {:host "localhost"
                   :port 18084
                   :spa-root nil})

    (let [s @server/server
          interceptors (::http/interceptors s)]

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

    (server/stop)))

;; =============================================================================
;; SPA Configuration Tests
;; =============================================================================

(deftest test-spa-interceptor-optional
  (testing "SPA interceptor is optional"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    ;; Start without SPA
    (server/start {:host "localhost"
                   :port 18085
                   :spa-root nil})

    (let [s @server/server
          interceptors (::http/interceptors s)
          interceptor-names (set (map :name interceptors))]

      ;; Should NOT have SPA interceptor
      (is (not (some #(re-find #"spa" (str %)) interceptor-names))
          "Should not have SPA interceptor when spa-root is nil"))

    (server/stop)))

(deftest test-spa-interceptor-included
  (testing "SPA interceptor is included when spa-root is provided"
    (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
      (encryption/rotate-keypair encryption/*encryption-provider*))

    ;; Create a temporary SPA root directory
    (let [temp-dir (str (System/getProperty "java.io.tmpdir") "/test-spa-" (System/currentTimeMillis))]
      (.mkdir (java.io.File. temp-dir))

      (server/start {:host "localhost"
                     :port 18086
                     :spa-root temp-dir})

      (let [s @server/server
            interceptors (::http/interceptors s)
            interceptor-names (set (map :name interceptors))]

        ;; Should have SPA interceptor
        (is (some #(re-find #"spa" (str %)) interceptor-names)
            "Should have SPA interceptor when spa-root is provided"))

      (server/stop)

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
      (server/start {:host "localhost"
                     :port (+ 18090 i)
                     :spa-root nil})

      (is (some? @server/server) (str "Server should start on cycle " (inc i)))

      (server/stop)

      (is (nil? @server/server) (str "Server should stop on cycle " (inc i))))))
