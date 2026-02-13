(ns synthigy.server.spa
  "Ring middleware for Single Page Application (SPA) support.

  Provides filesystem-based static file serving with intelligent index.html
  fallback for client-side routing. Server-agnostic - works with any Ring-based
  server (Pedestal, http-kit, Jetty, Compojure, Reitit, etc.).

  ## Features

  - **Routes-first approach**: API routes run before SPA fallback
  - **Progressive index.html search**: Supports nested SPAs and microfrontends
  - **Static file serving**: Serves CSS, JS, images from filesystem
  - **Client-side routing**: Non-file requests fallback to index.html

  ## Usage

  ### Ring Middleware (Compojure, http-kit, etc.)

  ```clojure
  (require '[synthigy.server.spa :as spa])

  (def app
    (-> api-routes
        (spa/wrap-spa {:root \"/var/www/dist\"})))
  ```

  ### Pedestal Interceptor

  ```clojure
  (require '[synthigy.server.spa :as spa])

  (def service-map
    {::http/interceptors
     [cors params router
      (spa/make-spa-interceptor {:root \"/var/www/dist\"})]})
  ```

  ## Behavior

  **API routes (handler returns response):**
  - GET /api/users → API response (200, 404, etc.)
  - Middleware returns response unchanged

  **Static files (no route match, has extension):**
  - GET /app.js → Serve /dist/app.js
  - GET /missing.js → 404

  **SPA routes (no route match, no extension):**
  - GET /users/123 → Serve /dist/index.html
  - GET /admin/dashboard → Try /dist/admin/index.html then /dist/index.html

  ## Configuration

  Environment variable:
  - SYNTHIGY_SERVE - Default root path for static files

  Options:
  - :root - Filesystem path to serve (required)
  - :index - Index filename (default: \"index.html\")"
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [ring.util.response :as response]))

;;; ============================================================================
;;; File System Utilities
;;; ============================================================================

(defn- file-exists?
  "Check if file exists on filesystem.

  Args:
    path - Filesystem path (String)

  Returns:
    true if file exists and is a regular file, false otherwise"
  [path]
  (when path
    (let [f (io/file path)]
      (and (.exists f) (.isFile f)))))

(defn- normalize-path
  "Remove leading slash from path for filesystem lookup.

  Args:
    path - URI path (e.g., \"/app.js\")

  Returns:
    Normalized path (e.g., \"app.js\")"
  [path]
  (if (.startsWith path "/")
    (subs path 1)
    path))

(defn- has-extension?
  "Check if URI has a file extension.

  Args:
    uri - Request URI

  Returns:
    true if extension found (e.g., .js, .css), false otherwise"
  [uri]
  (boolean (re-find #"\.[^/]+$" uri)))

(defn- serve-static-file
  "Serve a static file from filesystem.

  Args:
    root - Filesystem root path
    uri - Request URI

  Returns:
    Ring response map if file exists, nil otherwise"
  [root uri]
  (when root
    (let [path (normalize-path uri)
          file-path (str root "/" path)]
      (when (file-exists? file-path)
        (log/tracef "[SPA] Serving static file: %s" file-path)
        (response/file-response file-path)))))

;;; ============================================================================
;;; Index.html Progressive Search
;;; ============================================================================

(defn- serve-index-html
  "Serve index.html with progressive directory search.

  Searches for index.html in progressively higher directories:
  - /foo/bar/baz → tries /foo/bar/baz/index.html
  - Not found? → tries /foo/bar/index.html
  - Not found? → tries /foo/index.html
  - Not found? → tries /index.html

  This supports nested SPAs and microfrontends where different sections
  of the app may have their own index.html.

  Args:
    root - Filesystem root path
    uri - Request URI
    index - Index filename (default: \"index.html\")

  Returns:
    Ring response map with index.html, or 404 if not found"
  [root uri index]
  (if-not root
    {:status 404
     :body "Not found - SPA root not configured"}

    (let [sections (remove empty? (str/split uri #"/"))]
      (loop [path-segments sections]
        (let [search-path (if (empty? path-segments)
                            index
                            (str (str/join "/" path-segments) "/" index))
              file-path (str root "/" search-path)]

          (cond
            ;; Found index.html at this level
            (file-exists? file-path)
            (do
              (log/tracef "[SPA] Serving index.html from: %s" file-path)
              (-> (response/file-response file-path)
                  (assoc-in [:headers "Content-Type"] "text/html")))

            ;; Try parent directory
            (seq path-segments)
            (recur (butlast path-segments))

            ;; No index.html found anywhere
            :else
            (do
              (log/warnf "[SPA] No index.html found in %s for URI: %s" root uri)
              {:status 404
               :body "Not found"})))))))

;;; ============================================================================
;;; Ring Middleware
;;; ============================================================================

(defn wrap-spa
  "Ring middleware for SPA support with routes-first approach.

  **Execution order:**
  1. Call inner handler (API routes run first)
  2. If handler returns response → use it (even if 404)
  3. If no response + has extension → serve static file
  4. If no response + no extension → serve index.html (SPA fallback)

  This ensures:
  - API routes have full control (including custom 404s)
  - Static files only served when no route matches
  - SPA routing works for non-file URLs

  Args:
    handler - Inner Ring handler (your API routes)
    opts - Configuration map:
           :root - Filesystem path to serve (required)
           :index - Index filename (default: \"index.html\")

  Returns:
    Wrapped Ring handler

  Example:
    (def app
      (-> api-routes
          (wrap-spa {:root \"/var/www/dist\"})))"
  [handler {:keys [root index] :or {index "index.html"}}]
  (fn [request]
    (let [uri (:uri request)
          ;; CRITICAL: Call handler FIRST (routes run before SPA)
          response (handler request)]

      (if response
        ;; Route handled request (API response or custom 404)
        response

        ;; No route matched - SPA fallback
        (if (has-extension? uri)
          ;; Has extension - try static file
          (or (serve-static-file root uri)
              {:status 404
               :body "Not found"})

          ;; No extension - serve index.html (SPA routing)
          (serve-index-html root uri index))))))

;;; ============================================================================
;;; Pedestal Interceptor
;;; ============================================================================

(defn make-spa-interceptor
  "Create a Pedestal interceptor for SPA support.

  Wraps wrap-spa middleware as a Pedestal interceptor. Should be placed
  as the LAST interceptor in your chain (after router).

  Args:
    opts - Configuration map (same as wrap-spa)

  Returns:
    Pedestal interceptor

  Example:
    (def service-map
      {::http/interceptors
       [cors params router
        (spa/make-spa-interceptor {:root \"/var/www/dist\"})]})

  Note: The interceptor only uses :leave phase since Pedestal's router
        runs in :enter phase. This ensures routes are checked before SPA."
  [{:keys [root index] :or {index "index.html"} :as opts}]
  {:name ::spa
   :leave
   (fn [context]
     (let [{{:keys [uri]} :request} context
           response (:response context)]

       (if response
         ;; Route or previous interceptor set response
         context

         ;; No response - SPA fallback
         (let [spa-response
               (if (has-extension? uri)
                 ;; Has extension - try static file
                 (or (serve-static-file root uri)
                     {:status 404
                      :body "Not found"})

                 ;; No extension - serve index.html
                 (serve-index-html root uri index))]

           (assoc context :response spa-response)))))})
