;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.page.custom
  "Custom login pages: zip bundles in db/LoginPageStore, served from memory at /login/<name>/*."
  (:require
   [clojure.string :as str]
   [ring.util.codec :as codec]
   [synthigy.db :as db]
   [synthigy.log :as log])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream]
   [java.security MessageDigest]
   [java.util.zip ZipInputStream]))

(def mount-prefix "/login")

(def default-name
  "Bundle used when a client names none, and for device/status/error pages."
  "default")

(def max-zip-bytes (* 8 1024 1024))

(def max-unzipped-bytes (* 50 1024 1024))

(def recheck-ms 10000)

(def content-types
  {"html"  "text/html; charset=utf-8"
   "htm"   "text/html; charset=utf-8"
   "js"    "application/javascript"
   "mjs"   "application/javascript"
   "css"   "text/css"
   "json"  "application/json"
   "map"   "application/json"
   "txt"   "text/plain; charset=utf-8"
   "png"   "image/png"
   "jpg"   "image/jpeg"
   "jpeg"  "image/jpeg"
   "gif"   "image/gif"
   "webp"  "image/webp"
   "svg"   "image/svg+xml"
   "ico"   "image/x-icon"
   "woff"  "font/woff"
   "woff2" "font/woff2"
   "ttf"   "font/ttf"
   "otf"   "font/otf"})

(def csp "connect-src 'self'; frame-ancestors 'none'")

(def not-found {:status 404 :body "Not found"})

(defn valid-name? [s]
  (boolean (and (string? s) (re-matches #"[a-z0-9][a-z0-9-]{0,62}" s))))

(defn page-url [name] (str mount-prefix "/" name "/"))

(defn file-extension [path]
  (when-let [idx (str/last-index-of path ".")]
    (str/lower-case (subs path (inc idx)))))

(defn sha256 [^bytes bs]
  (apply str (map #(format "%02x" %) (.digest (MessageDigest/getInstance "SHA-256") bs))))

(def bundle-errors
  {:bad-name "Name must be lowercase letters, digits and dashes."
   :too-large "Bundle is larger than 8 MB."
   :too-large-unzipped "Bundle unzips to more than 50 MB."
   :not-zip "That file is not a valid zip."
   :empty "Bundle is empty."
   :unsafe-paths "Bundle has unsafe paths."
   :unsupported-types "Bundle has unsupported file types — only html, css, js, json, images and fonts are served."
   :no-index "Bundle has no index.html at its root."
   :no-version "No such bundle version."})

(defn bundle-error [reason data]
  (ex-info (bundle-errors reason) (assoc data :type ::invalid-bundle :reason reason)))

(defn junk? [path]
  (or (str/starts-with? path "__MACOSX/")
      (= ".DS_Store" (last (str/split path #"/")))))

(defn zip-entries
  "{path bytes} of every file in zip, capped at max-unzipped-bytes."
  [^bytes zip]
  (with-open [in (ZipInputStream. (ByteArrayInputStream. zip))]
    (let [buf (byte-array 65536)]
      (loop [files {} total 0]
        (if-let [entry (.getNextEntry in)]
          (if (.isDirectory entry)
            (recur files total)
            (let [out (ByteArrayOutputStream.)
                  total (loop [total total]
                          (let [n (.read in buf)]
                            (cond
                              (neg? n) total
                              (> (+ total n) max-unzipped-bytes)
                              (throw (bundle-error :too-large-unzipped {}))
                              :else (do (.write out buf 0 n) (recur (+ total n))))))]
              (recur (assoc files (.getName entry) (.toByteArray out)) total)))
          files)))))

(defn strip-common-root
  "Unwrap a single top-level folder holding index.html (a zipped directory)."
  [files]
  (let [roots (set (map #(first (str/split % #"/" 2)) (keys files)))
        root (first roots)]
    (if (and (= 1 (count roots))
             (not (contains? files "index.html"))
             (contains? files (str root "/index.html")))
      (update-keys files #(subs % (inc (count root))))
      files)))

(defn read-bundle
  "Validated {path bytes} from zip bytes; throws ::invalid-bundle."
  [^bytes zip]
  (when (> (alength zip) max-zip-bytes)
    (throw (bundle-error :too-large {})))
  (when-not (and (> (alength zip) 3) (= 0x50 (aget zip 0)) (= 0x4b (aget zip 1)))
    (throw (bundle-error :not-zip {})))
  (let [files (try (zip-entries zip)
                   (catch java.util.zip.ZipException _
                     (throw (bundle-error :not-zip {}))))
        files (strip-common-root (into {} (remove (comp junk? key)) files))
        unsafe (filter #(or (str/starts-with? % "/") (str/includes? % "\\")
                            (some #{".."} (str/split % #"/")))
                       (keys files))
        unknown (remove #(contains? content-types (file-extension %)) (keys files))]
    (cond
      (empty? files) (throw (bundle-error :empty {}))
      (seq unsafe) (throw (bundle-error :unsafe-paths {:paths (vec unsafe)}))
      (seq unknown) (throw (bundle-error :unsupported-types {:paths (vec unknown)}))
      (not (contains? files "index.html")) (throw (bundle-error :no-index {}))
      :else files)))

(defonce cache
  (atom {:checked 0 :pages {}}))

(defn load-page [name {:keys [version digest]}]
  {:version version
   :digest digest
   :files (read-bundle (db/login-page-zip db/*db* name version))})

(defn refresh!
  "Sync in-memory bundles with the active rows; a bundle that fails to load is left out."
  []
  (let [active (db/active-login-pages db/*db*)
        loaded (:pages @cache)
        pages (into {}
                    (keep (fn [[name {:keys [digest] :as row}]]
                            (if (= digest (get-in loaded [name :digest]))
                              [name (get loaded name)]
                              (try [name (load-page name row)]
                                   (catch Throwable e
                                     (log/error! {:id ::load-failed
                                                  :msg "Failed to load login page bundle"
                                                  :data {:action :loading :subject :login-page
                                                         :name name :version (:version row)}}
                                                 e)
                                     nil)))))
                    active)]
    (reset! cache {:checked (System/currentTimeMillis) :pages pages})
    pages))

(defn pages []
  (let [{:keys [checked pages]} @cache]
    (if (and db/*db* (> (- (System/currentTimeMillis) checked) recheck-ms))
      (try (refresh!)
           (catch Throwable e
             (log/error! {:id ::refresh-failed
                          :msg "Failed to refresh login page bundles"
                          :data {:action :loading :subject :login-page}}
                         e)
             (swap! cache assoc :checked (System/currentTimeMillis))
             pages))
      pages)))

(defn active? [name] (contains? (pages) name))

(defn upload!
  "Validate and store zip as the next inactive version of name."
  [name ^bytes zip uploaded-by]
  (when-not (valid-name? name)
    (throw (bundle-error :bad-name {:name name})))
  (let [files (read-bundle zip)
        version (db/put-login-page db/*db* name zip (sha256 zip) uploaded-by)]
    (log/info {:id ::uploaded
               :data {:action :uploaded :subject :login-page :name name
                      :version version :files (count files) :user uploaded-by}}
              "Login page bundle uploaded")
    {:name name :version version :files (count files)}))

(defn list-pages [] (db/login-pages db/*db*))

(defn activate!
  "Serve version of name; nil version stops serving name."
  [name version user]
  (when (and version (not (some #(and (= name (:name %)) (= version (:version %))) (list-pages))))
    (throw (bundle-error :no-version {:name name :version version})))
  (db/activate-login-page db/*db* name version)
  (log/info {:id ::activated
             :data {:action (if version :activated :deactivated) :subject :login-page
                    :name name :version version :user user}}
            "Login page bundle activation changed")
  (refresh!))

(defn delete! [name version user]
  (db/delete-login-page db/*db* name version)
  (log/info {:id ::deleted
             :data {:action :deleted :subject :login-page :name name :version version :user user}}
            "Login page bundle deleted")
  (refresh!))

(defn export [name version] (db/login-page-zip db/*db* name version))

(defn file-response [{:keys [digest files]} path request]
  (when-let [bs (get files path)]
    (let [etag (str "\"" digest "\"")]
      (if (= etag (get-in request [:headers "if-none-match"]))
        {:status 304 :headers {"ETag" etag}}
        {:status 200
         :headers {"Content-Type" (get content-types (file-extension path))
                   "Cache-Control" "no-cache"
                   "ETag" etag
                   "Content-Security-Policy" csp
                   "X-Content-Type-Options" "nosniff"}
         :body (ByteArrayInputStream. bs)}))))

(defn serve-path
  "Response for path in a bundle; extensionless misses fall back to index.html."
  [page path request]
  (let [path (if (str/blank? path) "index.html" path)]
    (or (file-response page path request)
        (when-not (file-extension path)
          (file-response page "index.html" request))
        not-found)))

(defn handler
  "Ring handler for /login/<name>/*; bare /login/<name> redirects to the trailing slash."
  [{:keys [uri query-string request-method] :as request}]
  (when (= :get request-method)
    (let [[_ name rest] (re-matches #"/login/([^/]+)(/.*)?" uri)
          page (when (valid-name? name) (get (pages) name))]
      (cond
        (nil? page) not-found

        (nil? rest)
        {:status 302
         :headers {"Location" (cond-> (page-url name)
                                (seq query-string) (str "?" query-string))}}

        :else (serve-path page (codec/url-decode (subs rest 1)) request)))))

(defn custom-page-redirect
  "302 to filename in the default bundle, or nil when it has no such file."
  [filename params]
  (when (contains? (get-in (pages) [default-name :files]) filename)
    {:status 302
     :headers {"Location" (cond-> (str (page-url default-name) filename)
                            (seq params) (str "?" (codec/form-encode params)))}}))
