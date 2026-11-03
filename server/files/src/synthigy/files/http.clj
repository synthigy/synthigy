;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.files.http
  "The /files endpoints: bytes in and out, every check made as the signed-in user."
  (:require
   [clojure.string :as str]
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.dataset.sql.query :as query]
   [synthigy.engine :as engine]
   [synthigy.env :as env]
   [synthigy.files.folders :as folders]
   [synthigy.files.guard :as guard]
   [synthigy.files.links :as links]
   [synthigy.files.migrate :as migrate]
   [synthigy.files.model :as model]
   [synthigy.files.schema :as schema]
   [synthigy.files.search :as search]
   [synthigy.files.storage :as storage]
   [synthigy.files.versions :as versions]
   [synthigy.iam.access :as access]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.authentication :as auth])
  (:import
   [java.net URLConnection URLDecoder URLEncoder]
   [java.nio.charset StandardCharsets]))

(defn respond [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/write-str body)})

(defn fail [status code message & [details]]
  (respond status {:error (cond-> {:code code :message message} details (assoc :details details))}))

(defn unauthorized [request]
  (-> (fail 401 "UNAUTHORIZED" "Sign in to use files")
      (assoc-in [:headers "WWW-Authenticate"] (auth/challenge request))))

(defn fields
  "Wire keys of the File attributes and the folder relation, from the deployed model."
  []
  (let [k #(keyword (model/column :files/file %))
        rel (model/relation :files/file->folder)
        folder-label (if (= (id/entity :files/file) (id/extract (:from rel))) (:to-label rel) (:from-label rel))]
    {:name (k :files.file/name)
     :storage-key (k :files.file/storage-key)
     :hash (k :files.file/hash)
     :size (k :files.file/size)
     :status (k :files.file/status)
     :content-type (k :files.file/content-type)
     :uploaded-by (k :files.file/uploaded-by)
     :uploaded-at (k :files.file/uploaded-at)
     :folder (keyword (naming/normalize-name folder-label))}))

(defn selection [f]
  {(id/key) nil
   (:name f) nil (:hash f) nil (:size f) nil (:status f) nil
   (:content-type f) nil (:uploaded-at f) nil
   (:folder f) [{:selections {(id/key) nil :name nil :path nil}}]})

(defn engine-facts
  "Storage key, uploader {xid name} and folder serving of the files `xids`, read as the engine: the key is hidden from the data API, and the caller may hold no grant on User."
  [f xids]
  (if (empty? xids)
    {}
    (access/with-principal nil
      (let [tree (folders/facts)]
        (into {}
              (map (fn [row] [(id/extract row) {:key (get row (:storage-key f))
                                                :uploader (some-> (get row (:uploaded-by f)) (select-keys [(id/key) :name]))
                                                :served (folders/served tree (some-> (get row (:folder f)) id/extract))}]))
              (dataset/search-entity :files/file {:_where {(id/key) {:_in (vec xids)}}}
                                     {(id/key) nil (:storage-key f) nil
                                      (:uploaded-by f) [{:selections {(id/key) nil :name nil}}]
                                      (:folder f) [{:selections {(id/key) nil}}]}))))))

(defn wire
  "The File row as the endpoints answer it; the storage key never leaves the server."
  ([f row] (wire f row (engine-facts f (some-> row id/extract vector))))
  ([f row facts]
   (when row
     {:xid (id/extract row)
      :name (get row (:name f))
      :size (get row (:size f))
      :hash (get row (:hash f))
      :status (some-> (get row (:status f)) name)
      :content_type (get row (:content-type f))
      :uploaded_at (get row (:uploaded-at f))
      :uploaded_by (get-in facts [(id/extract row) :uploader])
      :folder (some-> (get row (:folder f)) (select-keys [(id/key) :name :path]))
      :public (boolean (get-in facts [(id/extract row) :served :public]))})))

(defn file-row [f xid]
  (dataset/get-entity :files/file {(id/key) xid} (selection f)))

(defn storage-key [f xid]
  (get-in (engine-facts f [xid]) [xid :key]))

(defn max-size [] env/server-max-body)

(defn content-type
  "The type the client declared, else one guessed from the name, else octet-stream."
  [request file-name]
  (let [declared (some-> (get-in request [:headers "content-type"]) (str/split #";") first str/trim str/lower-case)]
    (or (when-not (#{nil "" "application/octet-stream" "application/x-www-form-urlencoded"} declared) declared)
        (some-> file-name URLConnection/guessContentTypeFromName)
        "application/octet-stream")))

(defn strip-etag [s]
  (some-> s str/trim (str/replace #"^W/" "") (str/replace #"^\"|\"$" "")))

(defn spool-body-stream!
  [request]
  (try
    {:spooled (storage/spool! (or (:body request) (java.io.ByteArrayInputStream. (byte-array 0))) (max-size))}
    (catch clojure.lang.ExceptionInfo e
      (if (= :file-too-large (:code (ex-data e)))
        {:refusal (fail 413 "FILE_TOO_LARGE" (str "Files are limited to " (max-size) " bytes"))}
        (throw e)))))

(defn form-encoded? [request]
  (some-> (get-in request [:headers "content-type"]) str/lower-case (str/starts-with? "application/x-www-form-urlencoded")))

(defn spool-body!
  "The request body spooled to disk, or a refusal response."
  [request]
  (if (form-encoded? request)
    {:refusal (fail 415 "UNSUPPORTED_MEDIA_TYPE"
                    "Send the bytes with their own Content-Type, or application/octet-stream; a form-encoded body is parsed as a form, not stored")}
    (spool-body-stream! request)))

(defn store!
  "Put the spooled bytes under a fresh storage key `<file xid>-<version xid>`; answers the key."
  [store owner {:keys [file] :as spooled} ctype]
  (let [k (str owner "-" (id/generate))]
    (storage/put-file! store k file (assoc spooled :content-type ctype))
    k))

(defn write-row!
  "Write File `data` through the guard with the caller's grants; deletes blob `k` when the row is refused."
  [store k data]
  (try
    (guard/as-endpoint #(dataset/sync-entity :files/file data))
    (catch Throwable t
      (try (storage/delete-blob! store k) (catch Throwable _))
      (throw t))))

(defn write-refusal
  "A response for a refused row write, or nil when it is not one the endpoints translate."
  [e]
  (let [{:keys [code]} (ex-data e)]
    (cond
      (schema/unique-violation? e) (fail 409 "FILE_NAME_TAKEN" "A file with this name already exists in the folder")
      (#{"ENTITY_FORBIDDEN" "ROW_FORBIDDEN" "FORBIDDEN" "DELETE_FORBIDDEN"} code) (fail 403 "FORBIDDEN" (ex-message e))
      (= "ATTRIBUTE_FORBIDDEN" code) (fail 403 "ATTRIBUTE_FORBIDDEN" (ex-message e))
      (= "FOLDER_NOT_EMPTY" code) (fail 409 "FOLDER_NOT_EMPTY" (ex-message e))
      (= "INVALID_NAME" code) (fail 400 "INVALID_NAME" (ex-message e))
      :else nil)))

(defn refused-write
  "Write File `data`; [nil row] when written, [refusal] when the engine refused it."
  [store k data]
  (try
    [nil (write-row! store k data)]
    (catch Exception e
      [(or (write-refusal e) (throw e))])))

(defn file-writable?
  "Whether the principal may change file `xid`: the role grants update on File and the file's guards let it in."
  [xid]
  (and (daccess/entity-allows? (id/entity :files/file) #{:update})
       (query/row-writable? (id/entity :files/file) xid)))

(defn create-file!
  "A new file `name` in folder `folder` (nil for none) from the request body."
  [store request principal name folder]
  (let [f (fields)]
    (cond
      (nil? name) (fail 400 "NAME_REQUIRED" "Give the file a name: ?name=report.pdf")
      (model/name-problem name) (fail 400 "INVALID_NAME" (model/name-problem name))
      (not (daccess/entity-allows? (id/entity :files/file) #{:create})) (fail 403 "FORBIDDEN" "You may not create files")
      (and folder (nil? (dataset/get-entity :files/folder {(id/key) folder} {(id/key) nil}))) (fail 404 "FOLDER_NOT_FOUND" "No such folder")
      (and folder (not (folders/writable? folder))) (fail 403 "FORBIDDEN" "You may not write to this folder")
      :else
      (let [{:keys [spooled refusal]} (spool-body! request)]
        (or refusal
            (let [ctype (content-type request name)
                  xid (id/generate)
                  k (store! store xid spooled ctype)
                  data (cond-> {(id/key) xid
                                (:name f) name
                                (:storage-key f) k
                                (:hash f) (:sha256 spooled)
                                (:size f) (:size spooled)
                                (:content-type f) ctype
                                (:status f) :READY
                                (:uploaded-at f) (java.util.Date.)
                                (:uploaded-by f) {(id/key) (id/extract principal)}}
                         folder (assoc (:folder f) {(id/key) folder}))
                  [refused row] (refused-write store k data)]
              (or refused (respond 201 (wire f (file-row f (id/extract row)))))))))))

(defn create!
  "POST /files?name=<name>&folder=<xid|path>: a new file from the request body."
  [store request principal]
  (let [{:strs [name folder]} (:query-params request)]
    (create-file! store request principal (some-> name str/trim not-empty) (some-> folder not-empty folders/ref->xid))))

(defn replace!
  "PUT /files/<xid>: a new version of the file; `If-Match: \"<hash>\"` refuses a lost update."
  [store request xid]
  (let [f (fields)
        row (file-row f xid)
        expected (strip-etag (get-in request [:headers "if-match"]))]
    (cond
      (nil? row) (fail 404 "FILE_NOT_FOUND" "No such file")
      (not (file-writable? xid)) (fail 403 "FORBIDDEN" "You may not change this file")
      (and expected (not= "*" expected) (not= expected (get row (:hash f))))
      (fail 412 "FILE_CHANGED" "The file changed since you read it" {:hash (get row (:hash f))})
      :else
      (let [{:keys [spooled refusal]} (spool-body! request)]
        (or refusal
            (let [ctype (content-type request (get row (:name f)))
                  k (store! store xid spooled ctype)
                  data {(id/key) xid
                        (:storage-key f) k
                        (:hash f) (:sha256 spooled)
                        (:size f) (:size spooled)
                        (:content-type f) ctype
                        (:status f) :READY
                        (:uploaded-at f) (java.util.Date.)}
                  [refused] (refused-write store k data)]
              (or refused (respond 200 (wire f (file-row f xid))))))))))

(defn parse-range
  "[start end] for a single `bytes=` range over `size`, :invalid when unsatisfiable, nil when absent."
  [header size]
  (when-let [[_ a b] (some->> header (re-matches #"bytes=(\d*)-(\d*)"))]
    (let [a (parse-long (str a)) b (parse-long (str b))]
      (cond
        (and (nil? a) b) (if (pos? b) [(max 0 (- size b)) (dec size)] :invalid)
        (and a (< a size)) [a (min (dec size) (or b (dec size)))]
        :else :invalid))))

(def inline-types #{"application/pdf" "text/plain"})

(defn disposition [request file-name ctype]
  (let [inline? (and (= "inline" (get-in request [:query-params "disposition"]))
                     (or (inline-types ctype)
                         (some #(str/starts-with? ctype %) ["image/" "video/" "audio/"])))
        ascii (str/replace (or file-name "file") #"[^\x20-\x7e]|[\"\\]" "_")]
    (str (if inline? "inline" "attachment")
         "; filename=\"" ascii "\""
         "; filename*=UTF-8''" (str/replace (URLEncoder/encode (or file-name "file") StandardCharsets/UTF_8) "+" "%20"))))

(defn cache-control
  "Cache-Control from a folder's Cache seconds: `scope` is public or private, no seconds = check every time."
  [scope seconds]
  (if (nil? seconds)
    (str scope ", no-cache")
    (str scope ", max-age=" (max 0 (long seconds)))))

(defn serve
  "The bytes of blob `key`, with ETag, Range, a download disposition and `cache` as Cache-Control; `open?` lets any origin read them."
  [store request {:keys [key size hash content-type file-name cache open?]}]
  (let [ctype (or content-type "application/octet-stream")
        shared (cond-> {"ETag" (str "\"" hash "\"")
                        "Cache-Control" (or cache "private, no-cache")}
                 open? (assoc "Access-Control-Allow-Origin" "*"))]
    (if (= hash (strip-etag (get-in request [:headers "if-none-match"])))
      {:status 304 :headers shared}
      (let [r (parse-range (get-in request [:headers "range"]) size)
            base (merge shared
                        {"Content-Type" ctype
                         "Accept-Ranges" "bytes"
                         "X-Content-Type-Options" "nosniff"
                         "Content-Security-Policy" "sandbox"
                         "Content-Disposition" (disposition request file-name ctype)})
            head? (= :head (:request-method request))]
        (cond
          (= :invalid r)
          (-> (fail 416 "RANGE_NOT_SATISFIABLE" (str "The range is outside the file's " size " bytes"))
              (assoc-in [:headers "Content-Range"] (str "bytes */" size)))

          r
          (let [[a b] r]
            {:status 206
             :headers (assoc base "Content-Range" (str "bytes " a "-" b "/" size)
                             "Content-Length" (str (inc (- b a))))
             :body (when-not head? (storage/open-blob store key a b))})

          :else
          (if-let [in (if head? ::head (storage/open-blob store key))]
            {:status 200
             :headers (assoc base "Content-Length" (str size))
             :body (when-not head? in)}
            (fail 410 "VERSION_EXPIRED" "These bytes are no longer stored")))))))

(defn serve-row
  "The current bytes of file `row`, given its engine `facts`, or why they cannot be served."
  [store request f row facts cache open?]
  (cond
    (nil? row) (fail 404 "FILE_NOT_FOUND" "No such file")
    (not= "READY" (some-> (get row (:status f)) name))
    (fail 409 "FILE_NOT_READY" "The file is not ready for download" {:status (some-> (get row (:status f)) name)})
    (nil? (:key facts)) (fail 404 "FILE_NOT_FOUND" "The file has no stored bytes")
    :else (serve store request {:key (:key facts) :size (get row (:size f)) :hash (get row (:hash f))
                                :content-type (get row (:content-type f)) :file-name (get row (:name f))
                                :cache cache :open? open?})))

(defn download
  "GET/HEAD /files/<xid> and /files/<xid>/<v>/<name>: the current bytes, as the signed-in user."
  [store request xid]
  (let [f (fields)
        row (file-row f xid)
        facts (when row (get (engine-facts f [xid]) xid))]
    (serve-row store request f row facts (cache-control "private" (get-in facts [:served :cache])) false)))

(defn open-download
  "A download without a token: only a file in a Public folder, or one below it."
  [store request xid]
  (let [f (fields)
        row (access/with-principal nil (file-row f xid))
        facts (when row (get (engine-facts f [xid]) xid))
        {:keys [public cache]} (:served facts)]
    (if public
      (serve-row store request f row facts (cache-control "public" cache) true)
      (unauthorized request))))

(defn remove!
  "DELETE /files/<xid>: the file row; its bytes are kept for the grace period."
  [xid]
  (let [f (fields)]
    (if (nil? (file-row f xid))
      (fail 404 "FILE_NOT_FOUND" "No such file")
      (try
        (guard/as-endpoint #(dataset/delete-entity :files/file {(id/key) xid}))
        {:status 204}
        (catch clojure.lang.ExceptionInfo e
          (or (write-refusal e) (throw e)))))))

(defn versions
  "GET /files/<xid>/versions: the kept versions of a file the caller can read, newest first."
  [xid]
  (let [f (fields)]
    (if (nil? (file-row f xid))
      (fail 404 "FILE_NOT_FOUND" "No such file")
      (let [current (storage-key f xid)]
        (respond 200 {:xid xid
                      :versions (mapv (fn [{:keys [key] :as w}]
                                        (-> (dissoc w :key) (assoc :current (= key current))))
                                      (rseq (versions/kept (versions/file-writes xid))))})))))

(defn kept-version [xid id]
  (some #(when (= (str id) (str (:id %))) %) (versions/kept (versions/file-writes xid))))

(defn download-version
  "GET /files/<xid>/versions/<id>: the bytes of one kept version."
  [store request xid id]
  (let [f (fields)
        row (file-row f xid)
        v (when row (kept-version xid id))]
    (cond
      (nil? row) (fail 404 "FILE_NOT_FOUND" "No such file")
      (nil? v) (fail 410 "VERSION_EXPIRED" "No such version is kept")
      :else (serve store request {:key (:key v) :size (:size v) :hash (:hash v)
                                  :content-type (:content_type v) :file-name (get row (:name f))}))))

(defn restore!
  "POST /files/<xid>/versions/<id>/restore: make a kept version current again, without copying bytes."
  [store xid id principal]
  (let [f (fields)
        row (file-row f xid)
        v (when row (kept-version xid id))]
    (cond
      (nil? row) (fail 404 "FILE_NOT_FOUND" "No such file")
      (not (file-writable? xid)) (fail 403 "FORBIDDEN" "You may not change this file")
      (nil? v) (fail 410 "VERSION_EXPIRED" "No such version is kept")
      (nil? (storage/head-blob store (:key v))) (fail 410 "VERSION_EXPIRED" "These bytes are no longer stored")
      :else
      (do
        (guard/as-endpoint
         #(dataset/sync-entity :files/file {(id/key) xid
                                            (:storage-key f) (:key v)
                                            (:hash f) (:hash v)
                                            (:size f) (:size v)
                                            (:content-type f) (:content_type v)
                                            (:status f) :READY
                                            (:uploaded-at f) (java.util.Date.)
                                            (:uploaded-by f) {(id/key) (id/extract principal)}}))
        (respond 200 (wire f (file-row f xid)))))))

(defn param-long [request k default]
  (or (some-> (get-in request [:query-params k]) str/trim not-empty parse-long) default))

(defn list-files
  "GET /files?folder=&folderless=&name=&q=&order=&limit=&offset=: the files the caller can see, newest first; q's words in order, a space for anything between."
  [request]
  (let [f (fields)
        {:strs [folder folderless name q order]} (:query-params request)
        folder (some-> folder not-empty folders/ref->xid)
        folderless (contains? #{"true" "1"} folderless)
        limit (-> (param-long request "limit" 100) (max 1) (min 1000))
        offset (max 0 (param-long request "offset" 0))
        order-by (case order
                   "name" {(:name f) :asc}
                   "oldest" {(:uploaded-at f) :asc}
                   {(:uploaded-at f) :desc})
        sel (cond-> (selection f)
              folderless (assoc (:folder f) [{:args {:_join :none}}])
              (not-empty folder) (assoc (:folder f) [{:selections {(id/key) nil :name nil :path nil}
                                                      :args {:_where {(id/key) {:_eq folder}} :_join :inner}}]))
        rows (if (seq (search/words q))
               (let [page (search/name-page q {:folder (not-empty folder) :folderless folderless :file-name (not-empty name)
                                               :order order :limit limit :offset offset}
                                            (clojure.core/name (:folder f)))
                     found (when (seq page)
                             (into {} (map (juxt id/extract identity))
                                   (dataset/search-entity :files/file {:_where {(id/key) {:_in page}}} sel)))]
                 (keep found page))
               (dataset/search-entity :files/file
                                      (cond-> {:_order_by order-by :_limit limit :_offset offset}
                                        (not-empty name) (assoc :_where {(:name f) {:_eq name}}))
                                      sel))
        facts (engine-facts f (map id/extract rows))]
    (respond 200 {:files (mapv #(wire f % facts) rows)})))

(defn search-files
  "GET /files/search?q=&folder=&limit=&offset=: folders and files whose paths hold the words of q in order, shortest path first."
  [request]
  (let [{:strs [q folder]} (:query-params request)
        scope (some-> folder not-empty folders/ref->xid)
        scope-path (when scope (:path (folders/engine-folder scope)))]
    (cond
      (empty? (search/words q)) (fail 400 "INVALID_QUERY" "Search needs q: words the path holds, in order")
      (and scope (not (and scope-path (folders/visible? scope)))) (fail 404 "FOLDER_NOT_FOUND" "No such folder")
      :else
      (let [f (fields)
            {:keys [matches truncated]} (search/matches q scope-path
                                                        (-> (param-long request "limit" 100) (max 1) (min 1000))
                                                        (max 0 (param-long request "offset" 0))
                                                        (name (:folder f)))
            by-type (group-by :type matches)
            file-rows (when-let [xids (seq (map :xid (:file by-type)))]
                        (into {} (map (juxt id/extract identity))
                              (dataset/search-entity :files/file {:_where {(id/key) {:_in (vec xids)}}} (selection f))))
            file-facts (engine-facts f (keys file-rows))
            folder-names (when-let [xids (seq (map :xid (:folder by-type)))]
                           (into {} (map (juxt id/extract #(get % (folders/name-field))))
                                 (dataset/search-entity :files/folder {:_where {(id/key) {:_in (vec xids)}}}
                                                        {(id/key) nil (folders/name-field) nil})))
            tree (when (seq folder-names) (folders/facts))]
        (respond 200 {:results (vec (keep (fn [{:keys [type xid path]}]
                                            (case type
                                              :folder (when (contains? folder-names xid)
                                                        (assoc (folders/wire {:xid xid :name (get folder-names xid)} tree) :type "folder"))
                                              :file (when-let [row (get file-rows xid)]
                                                      (assoc (wire f row file-facts) :type "file" :path path))))
                                          matches))
                      :truncated truncated})))))

(defn info [xid]
  (let [f (fields)]
    (if-let [row (file-row f xid)]
      (respond 200 (wire f row))
      (fail 404 "FILE_NOT_FOUND" "No such file"))))

(defn json-body [request]
  (let [raw (or (:raw-body request) (some-> (:body request) slurp))]
    (if (str/blank? raw) {} (json/read-str raw))))

(defn split-path
  "[folder path, name] of a file path such as /site/img/logo.png, or nil."
  [path]
  (when (and (string? path) (str/starts-with? path "/") (not (str/ends-with? path "/")))
    (let [cut (inc (str/last-index-of path "/"))]
      [(subs path 0 cut) (subs path cut)])))

(def bad-path [400 "INVALID_PATH" "A file path is /folder/name, for example /site/img/logo.png"])

(defn file-in
  "Xid of the file called `file-name` in folder `folder-xid` that the principal can see, or nil."
  [folder-xid file-name]
  (let [f (fields)]
    (some-> (dataset/search-entity :files/file {:_where {(:name f) {:_eq file-name}} :_limit 1}
                                   {(id/key) nil
                                    (:folder f) [{:selections {(id/key) nil}
                                                  :args {:_where {(id/key) {:_eq folder-xid}} :_join :inner}}]})
            first
            id/extract)))

(defn resolve-path
  "{:xid} of the file at `path` that the principal can see, or {:problem [status code message details]}."
  [path]
  (if-let [[dir file-name] (split-path path)]
    (if-let [missing (folders/first-missing dir)]
      {:problem [404 "FOLDER_NOT_FOUND" (str "No such folder: " missing) {:path missing}]}
      (if-let [xid (file-in (folders/ref->xid dir) file-name)]
        {:xid xid}
        {:problem [404 "FILE_NOT_FOUND" "No such file"]}))
    {:problem bad-path}))

(defn put-path!
  "PUT /files?path=/folder/name: the body becomes that file, created or replaced; the folder must exist."
  [store request principal path]
  (if-let [[dir file-name] (split-path path)]
    (if-let [missing (folders/first-missing dir)]
      (fail 404 "FOLDER_NOT_FOUND" (str "No such folder: " missing) {:path missing})
      (let [folder (folders/ref->xid dir)]
        (if-let [xid (file-in folder file-name)]
          (replace! store request xid)
          (if (strip-etag (get-in request [:headers "if-match"]))
            (fail 412 "FILE_CHANGED" "The file changed since you read it" {:hash nil})
            (create-file! store request principal file-name folder)))))
    (apply fail bad-path)))

(defn patch-file!
  "PATCH /files/<xid> {name, folder}: rename and/or move a file; folder null takes it out of every folder."
  [request xid]
  (let [f (fields)
        body (json-body request)
        new-name (some-> (:name body) str str/trim not-empty)
        move? (contains? body :folder)
        target (some-> (:folder body) str not-empty folders/ref->xid)]
    (cond
      (nil? (file-row f xid)) (fail 404 "FILE_NOT_FOUND" "No such file")
      (not (file-writable? xid)) (fail 403 "FORBIDDEN" "You may not change this file")
      (and new-name (model/name-problem new-name)) (fail 400 "INVALID_NAME" (model/name-problem new-name))
      (and target (not (folders/visible? target))) (fail 404 "FOLDER_NOT_FOUND" "No such folder")
      (and target (not (folders/writable? target))) (fail 403 "FORBIDDEN" "You may not move files there")
      :else
      (try
        (guard/as-endpoint #(dataset/sync-entity :files/file (cond-> {(id/key) xid}
                                                               new-name (assoc (:name f) new-name)
                                                               move? (assoc (:folder f) (when target {(id/key) target})))))
        (respond 200 (wire f (file-row f xid)))
        (catch Exception e
          (or (write-refusal e) (throw e)))))))

(def path-link #"/files/path(/.*)")

(defn decode-path
  "The file path a /files/path/... link names, or nil when a decoded part would hold a /."
  [raw]
  (let [parts (map #(URLDecoder/decode (str/replace % "+" "%2B") StandardCharsets/UTF_8) (str/split raw #"/" -1))]
    (when-not (some #(str/includes? % "/") parts)
      (str/join "/" parts))))

(defn folder-result [{:keys [problem folder]} status]
  (if-let [[st code message] problem]
    (fail st code message)
    (respond status folder)))

(defn folder-route
  [request principal xid tail]
  (let [m (:request-method request)]
    (cond
      (and xid (= "/access" tail) (= :get m))
      (cond
        (not (folders/visible? xid)) (fail 404 "FOLDER_NOT_FOUND" "No such folder")
        (not (folders/lists-readable?)) (fail 403 "FORBIDDEN" "You may not see who has access to this folder")
        :else (respond 200 (folders/access-lists xid)))

      (and xid (#{"/share" "/unshare"} tail) (= :post m))
      (folder-result (folders/share! xid (json-body request) (= "/share" tail)) 200)

      tail (fail 404 "NOT_FOUND" "No such folders endpoint")

      (and (nil? xid) (= :get m))
      (let [parent (or (some-> (get-in request [:query-params "parent"]) not-empty folders/ref->xid) (folders/root))]
        (respond 200 {:parent parent :folders (folders/children parent)}))

      (and (nil? xid) (= :post m))
      (let [{:keys [name parent]} (json-body request)]
        (folder-result (folders/create! principal (some-> name str) (some-> parent not-empty folders/ref->xid)) 201))

      (and xid (= :get m))
      (if-let [f (folders/folder xid)] (respond 200 f) (fail 404 "FOLDER_NOT_FOUND" "No such folder"))

      (and xid (= :patch m))
      (let [{:keys [name parent] :as body} (json-body request)]
        (folder-result (folders/update! xid {:name (some-> name str)
                                             :parent (if (and (contains? body :parent) (str/blank? parent))
                                                       (folders/root)
                                                       (some-> parent not-empty folders/ref->xid))})
                       200))

      (and xid (= :delete m))
      (try
        (if-let [[st code message] (:problem (folders/delete! xid))]
          (fail st code message)
          {:status 204})
        (catch Exception e
          (or (write-refusal e) (throw e))))

      :else (fail 405 "METHOD_NOT_ALLOWED" "Folders: GET/POST /files/folders, GET/PATCH/DELETE /files/folders/<xid> (or /files/folders?path=/a/b/), PUT /files/folders?path=, GET <xid>/access, POST <xid>/share|unshare"))))

(defn folder-put!
  "PUT /files/folders?path=/a/b/ {public, cache}: the folder and any missing above it, then its settings."
  [request principal]
  (let [path (some-> (get-in request [:query-params "path"]) str/trim not-empty)]
    (if (and path (str/starts-with? path "/"))
      (try
        (folder-result (folders/ensure! principal path (select-keys (json-body request) [:public :cache])) 200)
        (catch Exception e
          (or (write-refusal e) (throw e))))
      (fail 400 "INVALID_PATH" "Name the folder by its path: PUT /files/folders?path=/site/img/"))))

(defn needs-audit? [{:keys [uri request-method]}]
  (or (#{:post :put :delete} request-method)
      (re-find #"^/files/[A-Za-z0-9]{22}/versions" uri)))

(defn route
  [store {:keys [uri request-method] :as request} principal]
  (let [[file? xid tail version restore] (re-matches #"/files(?:/([A-Za-z0-9]{22}))?(/versions|/info)?(?:/(\d+))?(/restore)?/?" uri)
        [_ link-xid] (re-matches links/route uri)
        [_ raw-path] (re-matches path-link uri)
        [folders? folder-xid folder-tail] (re-matches #"/files/folders(?:/([A-Za-z0-9]{22}))?(/access|/share|/unshare)?/?" uri)
        path (some-> (get-in request [:query-params "path"]) not-empty)]
    (cond
      (and (nil? audit/*audit-provider*)
           (not folders?) (needs-audit? request))
      (fail 503 "AUDIT_UNAVAILABLE" "File versions and deletions live in audit history, which is not available right now; downloads still work")

      (and (= "/files/search" (str/replace uri #"/$" "")) (= :get request-method)) (search-files request)
      (and folders? (= :put request-method) (nil? folder-xid) (nil? folder-tail)) (folder-put! request principal)
      folders? (folder-route request principal
                             (or folder-xid (some-> (get-in request [:query-params "path"]) not-empty folders/ref->xid))
                             folder-tail)
      link-xid (if (#{:get :head} request-method)
                 (download store request link-xid)
                 (fail 405 "METHOD_NOT_ALLOWED" "A file link only downloads: GET or HEAD"))
      raw-path (if (#{:get :head} request-method)
                 (let [{target :xid problem :problem} (resolve-path (decode-path raw-path))]
                   (if target (download store request target) (apply fail problem)))
                 (fail 405 "METHOD_NOT_ALLOWED" "A file link only downloads: GET or HEAD"))
      (not file?) (fail 404 "NOT_FOUND" "No such files endpoint")
      (and (nil? xid) path (nil? tail) (= :put request-method)) (put-path! store request principal path)
      (and (nil? xid) path (or (nil? tail) (= "/info" tail)) (nil? version) (nil? restore))
      (let [{target :xid problem :problem} (resolve-path path)
            m request-method]
        (cond
          problem (apply fail problem)
          (and (= "/info" tail) (= :get m)) (info target)
          (= "/info" tail) (fail 405 "METHOD_NOT_ALLOWED" "File info is GET")
          (#{:get :head} m) (download store request target)
          (= :patch m) (patch-file! request target)
          (= :delete m) (remove! target)
          :else (fail 405 "METHOD_NOT_ALLOWED" "A file path takes GET, HEAD, PUT, PATCH and DELETE")))
      (and (nil? xid) (nil? tail) (= :post request-method)) (create! store request principal)
      (and (nil? xid) (nil? tail) (= :get request-method))
      (if (and (not-empty (get-in request [:query-params "folder"])) (#{"true" "1"} (get-in request [:query-params "folderless"])))
        (fail 400 "INVALID_QUERY" "Pass folder or folderless, not both")
        (list-files request))
      (and xid (= "/info" tail) (= :get request-method)) (info xid)
      (and xid (nil? tail) (nil? version) (#{:get :head} request-method)) (download store request xid)
      (and xid (nil? tail) (= :put request-method)) (replace! store request xid)
      (and xid (nil? tail) (= :patch request-method)) (patch-file! request xid)
      (and xid (nil? tail) (= :delete request-method)) (remove! xid)
      (and xid (= "/versions" tail) version restore (= :post request-method)) (restore! store xid version principal)
      (and xid (= "/versions" tail) version (nil? restore) (#{:get :head} request-method)) (download-version store request xid version)
      (and xid (= "/versions" tail) (nil? version) (= :get request-method)) (versions xid)
      (and (nil? xid) (= uri "/files")) (fail 405 "METHOD_NOT_ALLOWED" "List with GET /files, upload with POST /files?name=...")
      :else (fail 404 "NOT_FOUND" "No such files endpoint"))))

(defn open-route
  "A request without a token: GET/HEAD of /files/<xid> or a file link, nothing else; nil when it is neither."
  [store {:keys [uri request-method] :as request}]
  (when (#{:get :head} request-method)
    (let [[_ xid] (re-matches #"/files/([A-Za-z0-9]{22})/?" uri)
          [_ link-xid] (re-matches links/route uri)
          [_ raw-path] (re-matches path-link uri)]
      (cond
        (or xid link-xid) (open-download store request (or xid link-xid))
        raw-path (if-let [target (:xid (access/with-principal nil (resolve-path (decode-path raw-path))))]
                   (open-download store request target)
                   (unauthorized request))))))

(defn blob-write? [{:keys [uri request-method]}]
  (and (#{:post :put} request-method) (not (str/starts-with? uri "/files/folders"))))

(defn handler
  "Ring handler for the /files prefix; without an Authorization header only public files download."
  [store-fn request]
  (try
    (if-let [{:keys [principal]} (auth/authenticate-request request engine/platform-audience)]
      (access/with-principal principal
        (if (blob-write? request)
          (migrate/writing (fail 503 "FILES_MIGRATING" "Files are moving to new storage; uploads resume in a moment, downloads still work")
                           #(route (store-fn) request principal))
          (route (store-fn) request principal)))
      (or (when (nil? (get-in request [:headers "authorization"]))
            (open-route (store-fn) request))
          (unauthorized request)))
    (catch Throwable t
      (log/error! {:id ::request-failed :msg "Files request failed"
                   :data {:action :failed :subject :files :uri (:uri request) :method (:request-method request)}}
                  t)
      (fail 500 "INTERNAL_ERROR" (first (engine/safe-error-message t))))))
