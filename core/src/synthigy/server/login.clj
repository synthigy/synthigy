(ns synthigy.server.login
  "Filesystem-backed serving of a custom login page directory mounted at /login/*.

  Activated by SYNTHIGY_LOGIN_PAGE_PATH (see synthigy.env). When the env var
  is unset the interceptor returns 404 — the OAuth authorize handler then
  falls through to the built-in /oauth/login.

  Path-traversal safe via Ring's response/file-response :root option. The
  interceptor map is plain Ring/Pedestal-shaped — no server-backend coupling."
  (:require
   [clojure.string :as str]
   [ring.util.response :as response]
   [synthigy.env :as env]))

(def mount-prefix
  "URL prefix at which a custom login page is mounted. Single source of
   truth — the OAuth authorize redirect, the static-route registration, and
   the interceptor all reference this constant."
  "/login")

(def canonical-url
  "The canonical URL clients are redirected to (with trailing slash, so a
   user's relative asset paths in index.html resolve under /login/)."
  (str mount-prefix "/"))

(def ^:private content-types
  {"html"  "text/html"
   "htm"   "text/html"
   "js"    "application/javascript"
   "mjs"   "application/javascript"
   "css"   "text/css"
   "json"  "application/json"
   "png"   "image/png"
   "jpg"   "image/jpeg"
   "jpeg"  "image/jpeg"
   "gif"   "image/gif"
   "svg"   "image/svg+xml"
   "ico"   "image/x-icon"
   "woff"  "font/woff"
   "woff2" "font/woff2"
   "ttf"   "font/ttf"
   "otf"   "font/otf"
   "map"   "application/json"})

(def ^:private not-found
  {:status 404 :body "Not found"})

(defn- file-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (subs path (inc idx))))

(defn- serve-login-file
  "Serve a single file from the configured login-page root with a Content-Type
   header derived from the extension. Ring's :root option enforces that the
   resolved path stays inside root, so this is path-traversal safe.

   Returns a Ring response or nil if the file does not exist."
  [root path]
  (when-let [resp (response/file-response path {:root root})]
    (let [ext (file-extension path)
          ct (get content-types ext "application/octet-stream")]
      (assoc-in resp [:headers "Content-Type"] ct))))

(defn- relative-path
  "Map a request URI under the mount prefix to a path relative to the served
   directory. Bare `/login` and `/login/` map to `index.html`."
  [uri]
  (let [slash-prefix (str mount-prefix "/")]
    (cond
      (or (= uri mount-prefix) (= uri slash-prefix)) "index.html"
      (str/starts-with? uri slash-prefix) (subs uri (count slash-prefix))
      :else (subs uri 1))))

(defn login-page-response
  "Compute a Ring response for a request whose URI is at or under the mount
   prefix. Returns 404 when SYNTHIGY_LOGIN_PAGE_PATH is unset, when the URI
   contains path-traversal segments, when the file is missing, or when the
   resolved path leaves the configured root."
  [uri]
  (let [root env/login-page-path
        rel (relative-path uri)]
    (cond
      (not (seq root))
      not-found

      ;; Refuse traversal segments — Ring's :root catches them too, but we
      ;; reject explicitly so the SPA fallback below doesn't serve index.html
      ;; for `/login/../../etc/passwd`-style requests.
      (str/includes? rel "..")
      not-found

      :else
      (or (serve-login-file root rel)
          ;; SPA-style fallback: extensionless paths resolve to index.html
          (when-not (file-extension rel)
            (serve-login-file root "index.html"))
          not-found))))

(defn- canonicalize-redirect
  "When the request hits the bare prefix without a trailing slash, redirect
   the browser to the canonical URL so relative asset paths in the user's
   index.html resolve correctly."
  [uri query-string]
  (when (= uri mount-prefix)
    {:status 302
     :headers {"Location" (cond-> canonical-url
                            (seq query-string) (str "?" query-string))}}))

(def serve-login-page-interceptor
  "Pedestal interceptor that serves the custom login page mounted at /login/*.
   Redirects bare /login to /login/ to keep relative asset paths working."
  {:name ::serve-login-page
   :enter
   (fn [{{:keys [uri query-string]} :request :as ctx}]
     (assoc ctx :response
            (or (canonicalize-redirect uri query-string)
                (login-page-response uri))))})
