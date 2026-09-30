;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.page.custom
  "Filesystem-backed serving of a custom login page directory mounted at
   /login/*; falls back to /oauth/login when SYNTHIGY_IAM_LOGIN_PAGE_PATH is unset."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.util.codec :as codec]
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

(defn file-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (subs path (inc idx))))

(defn serve-login-file
  "Serve a single file from the configured login-page root, Content-Type derived
   from the extension."
  [root path]
  (when-let [resp (response/file-response path {:root root})]
    (let [ext (file-extension path)
          ct (get content-types ext "application/octet-stream")]
      (assoc-in resp [:headers "Content-Type"] ct))))

(defn relative-path
  "Map a request URI under the mount prefix to a path relative to the served
   directory."
  [uri]
  (let [slash-prefix (str mount-prefix "/")]
    (cond
      (or (= uri mount-prefix) (= uri slash-prefix)) "index.html"
      (str/starts-with? uri slash-prefix) (subs uri (count slash-prefix))
      :else (subs uri 1))))

(defn login-page-response
  "Ring response for a request under the mount prefix; 404 when unconfigured,
   traversal, or missing."
  [uri]
  (let [root env/login-page-path
        rel (relative-path uri)]
    (cond
      (not (seq root))
      not-found

      ;; Ring's :root also catches traversal, but reject explicitly so the SPA
      ;; fallback below can't serve index.html for `/login/../../etc/passwd`.
      (str/includes? rel "..")
      not-found

      :else
      (or (serve-login-file root rel)
          (when-not (file-extension rel)
            (serve-login-file root "index.html"))
          not-found))))

(defn has-page?
  "True when the configured login-page folder contains filename. ponytail:
   filename is always a trusted constant, never user input — no traversal guard
   needed."
  [filename]
  (boolean
   (when (seq env/login-page-path)
     (.isFile (io/file env/login-page-path filename)))))

(defn custom-page-redirect
  "302 to filename under the mount prefix, or nil when the login-page folder has
   no such file."
  [filename params]
  (when (has-page? filename)
    {:status 302
     :headers {"Location" (cond-> (str mount-prefix "/" filename)
                            (seq params) (str "?" (codec/form-encode params)))}}))

(defn canonicalize-redirect
  "Redirect bare /login (no trailing slash) to canonical-url."
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
