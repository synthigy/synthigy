(ns synthigy.server.spa-test
  "Tests for SPA Ring middleware and Pedestal interceptor.

  Uses real test fixtures in core/testing/server/ directory."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [synthigy.server.spa :as spa]))

;;; ============================================================================
;;; Test Configuration
;;; ============================================================================

(def test-root
  "Absolute path to testing/server/ directory with real test fixtures."
  (str (System/getProperty "user.dir") "/testing/server"))

(defn slurp-body
  "Read response body (File) as string for assertions."
  [response]
  (when-let [body (:body response)]
    (if (instance? java.io.File body)
      (slurp body)
      body)))

;;; ============================================================================
;;; Mock Handlers
;;; ============================================================================

(defn mock-api-handler
  "Mock handler that simulates API routes.

  Returns:
  - {:status 200} for /api/users
  - {:status 404} for /api/missing
  - nil for everything else (no route match)"
  [request]
  (case (:uri request)
    "/api/users" {:status 200 :body {:users []}}
    "/api/missing" {:status 404 :body {:error "Not found"}}
    nil))

(defn always-nil-handler
  "Handler that never matches (always returns nil)."
  [request]
  nil)

;;; ============================================================================
;;; Tests: Routes-First Behavior
;;; ============================================================================

(deftest test-routes-first-api-success
  (testing "API route returns 200 - SPA does nothing"
    (let [handler (spa/wrap-spa mock-api-handler {:root test-root})
          response (handler {:uri "/api/users"})]
      (is (= 200 (:status response)))
      (is (= {:users []} (:body response))))))

(deftest test-routes-first-api-404
  (testing "API route returns 404 - SPA respects it (doesn't serve index.html)"
    (let [handler (spa/wrap-spa mock-api-handler {:root test-root})
          response (handler {:uri "/api/missing"})]
      (is (= 404 (:status response)))
      (is (= {:error "Not found"} (:body response))))))

;;; ============================================================================
;;; Tests: Static File Serving (Real Files)
;;; ============================================================================

(deftest test-static-file-js
  (testing "Static .js file served with correct content"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/assets/app.js"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (some? content))
      (is (str/includes? content "console.log"))
      (is (str/includes? content "Test SPA application loaded")))))

(deftest test-static-file-css
  (testing "Static .css file served with correct content"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/assets/style.css"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (some? content))
      (is (str/includes? content "body"))
      (is (str/includes? content "margin: 0")))))

(deftest test-static-file-ico
  (testing "Static .ico file served"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/favicon.ico"})]
      (is (= 200 (:status response)))
      (is (some? (:body response))))))

(deftest test-static-file-txt
  (testing "Static .txt file served with correct content"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/robots.txt"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (str/includes? content "User-agent"))
      (is (str/includes? content "Disallow: /admin/")))))

(deftest test-static-file-missing
  (testing "Missing static file returns 404"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/missing.js"})]
      (is (= 404 (:status response))))))

;;; ============================================================================
;;; Tests: SPA Fallback (Real index.html)
;;; ============================================================================

(deftest test-spa-root-index
  (testing "Root path serves root index.html with correct content"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      (is (str/includes? content "Root SPA"))
      (is (str/includes? content "root index.html")))))

(deftest test-spa-route-fallback
  (testing "SPA route without extension serves root index.html"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/users/123"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      (is (str/includes? content "Root SPA")))))

(deftest test-spa-nested-route-fallback
  (testing "Nested SPA route serves root index.html"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/app/dashboard/settings"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      (is (str/includes? content "Root SPA")))))

;;; ============================================================================
;;; Tests: Progressive index.html Search (Real Microfrontends)
;;; ============================================================================

(deftest test-progressive-search-admin
  (testing "Admin route serves /admin/index.html (not root)"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/admin/users"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      ;; Verify it's the ADMIN index, not root
      (is (str/includes? content "Admin Microfrontend"))
      (is (not (str/includes? content "Root SPA"))))))

(deftest test-progressive-search-admin-root
  (testing "Admin root path serves /admin/index.html"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/admin"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (str/includes? content "Admin Microfrontend")))))

(deftest test-progressive-search-docs
  (testing "Docs route serves /docs/index.html (not root)"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/docs/api/endpoints"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      ;; Verify it's the DOCS index, not root
      (is (str/includes? content "Documentation"))
      (is (not (str/includes? content "Root SPA"))))))

(deftest test-progressive-search-fallback-to-root
  (testing "Unknown section falls back to root index.html"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/unknown/section"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      ;; Should be root, not admin or docs
      (is (str/includes? content "Root SPA"))
      (is (not (str/includes? content "Admin")))
      (is (not (str/includes? content "Documentation"))))))

;;; ============================================================================
;;; Tests: Edge Cases
;;; ============================================================================

(deftest test-no-root-configured
  (testing "No root configured returns 404"
    (let [handler (spa/wrap-spa always-nil-handler {:root nil})
          response (handler {:uri "/users/123"})]
      (is (= 404 (:status response)))
      (is (some? (:body response))))))

(deftest test-trailing-slash
  (testing "Trailing slash serves index.html from that directory"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})
          response (handler {:uri "/admin/"})
          content (slurp-body response)]
      (is (= 200 (:status response)))
      (is (= "text/html" (get-in response [:headers "Content-Type"])))
      (is (str/includes? content "Admin Microfrontend")))))

(deftest test-mixed-api-and-spa
  (testing "API routes work alongside SPA routes"
    (let [handler (spa/wrap-spa mock-api-handler {:root test-root})]
      ;; API route
      (let [api-resp (handler {:uri "/api/users"})]
        (is (= 200 (:status api-resp)))
        (is (= {:users []} (:body api-resp))))

      ;; SPA route
      (let [spa-resp (handler {:uri "/users/123"})
            content (slurp-body spa-resp)]
        (is (= 200 (:status spa-resp)))
        (is (= "text/html" (get-in spa-resp [:headers "Content-Type"])))
        (is (str/includes? content "Root SPA")))

      ;; Static file
      (let [file-resp (handler {:uri "/assets/app.js"})
            content (slurp-body file-resp)]
        (is (= 200 (:status file-resp)))
        (is (str/includes? content "console.log"))))))

;;; ============================================================================
;;; Tests: File Content Verification
;;; ============================================================================

(deftest test-handler-serves-actual-files
  (testing "Handler actually reads and serves real files from disk"
    (let [handler (spa/wrap-spa always-nil-handler {:root test-root})]

      ;; Verify JS file content
      (let [js-resp (handler {:uri "/assets/app.js"})
            content (slurp-body js-resp)]
        (is (str/includes? content "export { init }"))
        (is (str/includes? content "function init()")))

      ;; Verify CSS file content
      (let [css-resp (handler {:uri "/assets/style.css"})
            content (slurp-body css-resp)]
        (is (str/includes? content "font-family:"))
        (is (str/includes? content "background-color: #f5f5f5")))

      ;; Verify HTML content differences
      (let [root-html (slurp-body (handler {:uri "/"}))
            admin-html (slurp-body (handler {:uri "/admin/dashboard"}))
            docs-html (slurp-body (handler {:uri "/docs/guide"}))]

        ;; Each should have different content
        (is (str/includes? root-html "Root SPA"))
        (is (str/includes? admin-html "Admin Microfrontend"))
        (is (str/includes? docs-html "Documentation"))

        ;; Verify they're different files
        (is (not= root-html admin-html))
        (is (not= root-html docs-html))
        (is (not= admin-html docs-html))))))

;;; ============================================================================
;;; Tests: Pedestal Interceptor
;;; ============================================================================

(deftest test-pedestal-interceptor-leave
  (testing "Pedestal interceptor serves SPA in :leave phase"
    (let [interceptor (spa/make-spa-interceptor {:root test-root})
          context {:request {:uri "/users/123"}
                   :response nil}
          result ((:leave interceptor) context)
          content (slurp-body (:response result))]
      (is (= 200 (get-in result [:response :status])))
      (is (= "text/html" (get-in result [:response :headers "Content-Type"])))
      (is (str/includes? content "Root SPA")))))

(deftest test-pedestal-interceptor-respects-existing-response
  (testing "Pedestal interceptor doesn't override existing response"
    (let [interceptor (spa/make-spa-interceptor {:root test-root})
          context {:request {:uri "/users/123"}
                   :response {:status 200 :body "API response"}}
          result ((:leave interceptor) context)]
      (is (= "API response" (get-in result [:response :body])))
      (is (= 200 (get-in result [:response :status]))))))

(deftest test-pedestal-interceptor-static-files
  (testing "Pedestal interceptor serves static files"
    (let [interceptor (spa/make-spa-interceptor {:root test-root})
          context {:request {:uri "/assets/app.js"}
                   :response nil}
          result ((:leave interceptor) context)
          content (slurp-body (:response result))]
      (is (= 200 (get-in result [:response :status])))
      (is (str/includes? content "console.log")))))

;;; ============================================================================
;;; Tests: Path Handling
;;; ============================================================================

(deftest test-absolute-path-resolution
  (testing "Test root uses absolute path correctly"
    (is (str/includes? test-root "/synthigy/core/testing/server"))
    (is (str/starts-with? test-root "/"))
    ;; Verify the directory actually exists
    (is (.exists (io/file test-root)))
    (is (.isDirectory (io/file test-root)))))

(deftest test-file-existence
  (testing "Test fixture files exist"
    (is (.exists (io/file test-root "index.html")))
    (is (.exists (io/file test-root "admin/index.html")))
    (is (.exists (io/file test-root "docs/index.html")))
    (is (.exists (io/file test-root "assets/app.js")))
    (is (.exists (io/file test-root "assets/style.css")))
    (is (.exists (io/file test-root "favicon.ico")))
    (is (.exists (io/file test-root "robots.txt")))))
