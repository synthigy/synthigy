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
   [clojure.walk :as walk]
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.dataset.sql.query :as query]
   [synthigy.engine :as engine]
   [synthigy.env :as env]
   [synthigy.files.folders :as folders]
   [synthigy.files.guard :as guard]
   [synthigy.files.links :as links]
   [synthigy.files.migrate :as migrate]
   [synthigy.files.model :as model]
   [synthigy.files.page :as page]
   [synthigy.files.schema :as schema]
   [synthigy.files.search :as search]
   [synthigy.files.storage :as storage]
   [synthigy.files.versions :as versions]
   [synthigy.files.zip :as zip]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.authentication :as auth]
   [synthigy.oauth.core :as oauth])
  (:import
   [java.net URLConnection URLDecoder URLEncoder]
   [java.nio.charset StandardCharsets]))

(def ^:dynamic *key-fn* nil)

(defn cased [body]
  (if-let [kf *key-fn*]
    (walk/postwalk #(if (map? %) (update-keys % (fn [k] (if (keyword? k) (dk/preserve-prefix kf k) k))) %) body)
    body))

(defn respond [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/write-str (cased body))})

(defn fail [status code message & [details]]
  (respond status {:error (cond-> {:code code :message message} details (assoc :details details))}))

(defn unauthorized [request]
  (if (page/browser? request)
    (page/unauthorized request)
    (-> (fail 401 "UNAUTHORIZED" "Sign in to use files")
        (assoc-in [:headers "WWW-Authenticate"] (auth/challenge request)))))

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
  "Storage key, uploader {xid name} (the label, never the login) and folder serving of the files `xids`, read as the engine: the key is hidden from the data API, and the caller may hold no grant on User."
  [f xids]
  (if (empty? xids)
    {}
    (access/with-principal nil
      (let [rows (dataset/search-entity :files/file {:_where {(id/key) {:_in (vec xids)}}}
                                        {(id/key) nil (:storage-key f) nil
                                         (:uploaded-by f) [{:selections {(id/key) nil :display_name nil}}]
                                         (:folder f) [{:selections {(id/key) nil}}]})
            folder-of #(some-> (get % (:folder f)) id/extract)
            tree (folders/facts (map folder-of rows))]
        (into {}
              (map (fn [row] [(id/extract row) {:key (get row (:storage-key f))
                                                :uploader (some-> (get row (:uploaded-by f)) (as-> u {(id/key) (get u (id/key)) :name (:display_name u)}))
                                                :served (folders/served tree (folder-of row))}]))
              rows)))))

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
  {:spooled (storage/spool! (or (:body request) (java.io.ByteArrayInputStream. (byte-array 0))))})

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
  [store request principal xid]
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
                        (:uploaded-at f) (java.util.Date.)
                        (:uploaded-by f) {(id/key) (id/extract principal)}}
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
          (if-let [body (cond head? ::head
                              (satisfies? storage/FileBacked store) (storage/blob-file store key)
                              :else (storage/open-blob store key))]
            {:status 200
             :headers (assoc base "Content-Length" (str size))
             :body (when-not head? body)}
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
  "GET/HEAD /files/<xid>: the current bytes, as the signed-in user."
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
        (links/delete-of-file! xid)
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

(defn file-list
  "The wired files the caller can see for /files query params, newest first; q's words in order, a space for anything between."
  [request]
  (let [f (fields)
        {:strs [folder folderless name q order]} (:query-params request)
        folder (some-> folder not-empty folders/ref->xid)
        folderless (contains? #{"true" "1"} folderless)
        limit (-> (param-long request "limit" 100) (max 1) (min 1000))
        offset (max 0 (param-long request "offset" 0))
        order-by (case order
                   "name" (array-map (:name f) :asc (id/key) :asc)
                   "oldest" (array-map (:uploaded-at f) :asc (id/key) :asc)
                   (array-map (:uploaded-at f) :desc (id/key) :asc))
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
    (mapv #(wire f % facts) rows)))

(defn list-files
  "GET /files?folder=&folderless=&name=&q=&order=&limit=&offset=."
  [request]
  (respond 200 {:files (file-list request)}))

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
            tree (when (seq folder-names) (folders/facts (keys folder-names)))]
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

(defn json-body
  "The request's JSON object, {} when blank; throws a 400 problem for anything else."
  [request]
  (let [raw (or (:raw-body request) (some-> (:body request) slurp))
        body (if (str/blank? raw)
               {}
               (try (json/read-str raw) (catch Exception _ ::malformed)))]
    (if (map? body)
      body
      (throw (ex-info "Invalid JSON body" {::problem [400 "INVALID_BODY" "The body must be a JSON object"]})))))

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
          (replace! store request principal xid)
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
        (assoc (respond 200 (wire f (file-row f xid))) ::action (if move? :moved :renamed))
        (catch Exception e
          (or (write-refusal e) (throw e)))))))

(defn base-url [request]
  (str/replace (or (not-empty (str env/iam-root-url)) (oauth/original-uri request)) #"/+$" ""))

(defn link-url [request token] (str (base-url request) "/files/link/" token))

(defn create-link!
  "POST /files/<xid>/links {expires, max_downloads, inline}: a link anyone holding it downloads the file with."
  [request principal xid]
  (let [f (fields)
        row (file-row f xid)
        served (when row (get-in (engine-facts f [xid]) [xid :served]))]
    (cond
      (not (links/deployed?)) (fail 404 "NOT_FOUND" "This File System model has no File Link")
      (nil? row) (fail 404 "FILE_NOT_FOUND" "No such file")
      (nil? (get row (:folder f))) (fail 403 "LINKS_DISABLED" "Files in no folder cannot be shared by link")
      (not (:can-link served)) (fail 403 "LINKS_DISABLED" "Links are not allowed in this folder")
      :else
      (let [{:keys [problem token link]} (links/create! principal xid (json-body request))]
        (if-let [[st code message] problem]
          (fail st code message)
          (respond 201 (assoc link :url (link-url request token))))))))

(defn list-links
  "GET /files/<xid>/links: the file's live links the caller can see."
  [xid]
  (cond
    (not (links/deployed?)) (fail 404 "NOT_FOUND" "This File System model has no File Link")
    (nil? (file-row (fields) xid)) (fail 404 "FILE_NOT_FOUND" "No such file")
    :else (try
            (respond 200 {:file xid :links (links/of-file xid)})
            (catch clojure.lang.ExceptionInfo e
              (or (write-refusal e) (throw e))))))

(defn delete-link!
  "DELETE /files/links/<link xid>: the link stops working everywhere at once."
  [link-xid]
  (if-not (links/deployed?)
    (fail 404 "NOT_FOUND" "This File System model has no File Link")
    (try
      (let [{:keys [problem file]} (links/delete! link-xid)]
        (if-let [[st code message] problem]
          (fail st code message)
          {:status 204 ::file file}))
      (catch clojure.lang.ExceptionInfo e
        (or (write-refusal e) (throw e))))))

(defn link-gone [request]
  (if (page/browser? request)
    (page/link-gone request)
    (assoc-in (fail 404 "LINK_NOT_FOUND" "This link expired, was used up or withdrawn, or never existed")
              [:headers "Cache-Control"] "no-store")))

(defn creator-reads?
  "Whether user `creator`, read fresh from the database, is active and can read file `xid`."
  [creator xid]
  (let [p (when creator (access/with-principal nil (iam.context/get-user-details {(id/key) creator})))]
    (and p (not (false? (:active p)))
         (some? (access/with-principal p (dataset/get-entity :files/file {(id/key) xid} {(id/key) nil}))))))

(defn link-download
  "GET/HEAD /files/link/<token>: the file behind a live link, checked as the link's creator on every request."
  [store request token]
  (let [f (fields)
        lf (when (links/deployed?) (links/fields))
        link (when lf
               (links/by-token token (assoc (links/selection lf) (:file lf)
                                            [{:selections {(id/key) nil (:name f) nil (:hash f) nil (:size f) nil (:status f) nil
                                                           (:content-type f) nil (:storage-key f) nil
                                                           (:folder f) [{:selections {(id/key) nil (folders/path-field) nil}}]}}])))
        file (get link (:file lf))
        folder (get file (:folder f))
        served (delay (folders/served (folders/chain (get folder (folders/path-field))) (id/extract folder)))
        limited (links/limited? lf link)
        request (cond-> request
                  limited (update :headers dissoc "range" "if-none-match")
                  (links/inline? lf link) (assoc-in [:query-params "disposition"] "inline"))
        counts? (and (= :get (:request-method request))
                     (or limited
                         (nil? (get-in request [:headers "range"]))
                         (str/starts-with? (str (get-in request [:headers "range"])) "bytes=0-"))
                     (not= (get file (:hash f)) (strip-etag (get-in request [:headers "if-none-match"]))))]
    (if (or (not (links/live? lf link))
            (nil? folder)
            (links/used-up? lf link)
            (not (:can-link @served))
            (not (creator-reads? (some-> (get link (:created-by lf)) id/extract) (id/extract file)))
            (and counts? (not (links/count-download! lf link))))
      (link-gone request)
      (let [cache (:cache @served)
            response (serve-row store request f file {:key (get file (:storage-key f))}
                                (cond limited "private, no-store"
                                      (nil? cache) "private, no-cache"
                                      :else (cache-control "private" (min cache (links/seconds-left lf link))))
                                true)]
        (cond-> (assoc-in response [:headers "Referrer-Policy"] "no-referrer")
          limited (assoc-in [:headers "Accept-Ranges"] "none")
          true (assoc ::link (id/extract link)))))))

(def link-uri #"/files/link/([^/]+)/?")

(def links-uri #"/files/(?:([A-Za-z0-9]{22})/links|links/([A-Za-z0-9]{22}))/?")

(def path-link #"/files/path(/.*)")

(def file-uri #"/files(?:/([A-Za-z0-9]{22}))?(/versions|/info)?(?:/(\d+))?(/restore)?/?")

(def folder-uri #"/files/folders(?:/([A-Za-z0-9]{22}))?(/access|/share|/unshare)?/?")

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
      (if-let [f (folders/folder xid)]
        (if (#{"true" "1"} (get-in request [:query-params "view"]))
          (respond 200 (merge {:folder f
                               :folders (folders/children xid)}
                              (if (folders/lists-readable?)
                                {:access (folders/access-lists xid)}
                                {:access nil :access_error {:code "FORBIDDEN" :message "You may not see who has access to this folder"}})))
          (respond 200 f))
        (fail 404 "FOLDER_NOT_FOUND" "No such folder"))

      (and xid (= :patch m))
      (let [{:keys [name parent] :as body} (json-body request)]
        (assoc (folder-result (folders/update! xid {:name (some-> name str)
                                                    :parent (if (and (contains? body :parent) (str/blank? parent))
                                                              (folders/root)
                                                              (some-> parent not-empty folders/ref->xid))})
                              200)
               ::action (if (contains? body :parent) :moved :renamed)))

      (and xid (= :delete m))
      (try
        (if-let [[st code message] (:problem (folders/delete! xid))]
          (fail st code message)
          {:status 204})
        (catch Exception e
          (or (write-refusal e) (throw e))))

      :else (fail 405 "METHOD_NOT_ALLOWED" "Folders: GET/POST /files/folders, GET/PATCH/DELETE /files/folders/<xid> (or /files/folders?path=/a/b/), PUT /files/folders?path=, GET <xid>/access, POST <xid>/share|unshare"))))

(defn folder-put!
  "PUT /files/folders?path=/a/b/ {public, cache, can_link}: the folder and any missing above it, then its settings."
  [request principal]
  (let [path (some-> (get-in request [:query-params "path"]) str/trim not-empty)]
    (if (and path (str/starts-with? path "/"))
      (try
        (folder-result (folders/ensure! principal path (select-keys (json-body request) [:public :cache :can_link])) 200)
        (catch Exception e
          (or (write-refusal e) (throw e))))
      (fail 400 "INVALID_PATH" "Name the folder by its path: PUT /files/folders?path=/site/img/"))))

(defn zip-request? [{:keys [uri request-method]}]
  (and (= :post request-method) (= "/files/zip" (str/replace uri #"/$" ""))))

(defn ready-rows
  "READY files the caller can read, by xid or by folder, each with its folder path."
  [f {:keys [xids folders]}]
  (dataset/search-entity :files/file
                         {:_where (cond-> {(:status f) {:_eq :READY}}
                                    xids (assoc (id/key) {:_in (vec xids)}))}
                         {(id/key) nil (:name f) nil (:size f) nil (:content-type f) nil
                          (:folder f) [(cond-> {:selections {(id/key) nil :path nil}}
                                         folders (assoc :args {:_where {(id/key) {:_in (vec folders)}} :_join :inner}))]}))

(defn zip-plan
  "The entries and skips POST /files/zip names, every file read as the caller."
  [f {:keys [files folders]}]
  (let [file-refs (map str files)
        resolved (for [r file-refs]
                   (if (str/starts-with? r "/")
                     (let [{:keys [xid]} (resolve-path r)] [r xid])
                     [r r]))
        rows (when-let [xids (seq (keep second resolved))]
               (into {} (map (juxt id/extract identity)) (ready-rows f {:xids (distinct xids)})))
        loose (for [[r xid] resolved]
                (if-let [row (get rows xid)]
                  {:row row :entry (get row (:name f)) :ref r}
                  {:skip {:ref r :reason "not found, not readable or not ready"}}))
        in-folders (for [r (map str folders)
                         :let [xid (folders/ref->xid r)
                               top (when (and (not (str/starts-with? xid "/")) (folders/visible? xid))
                                     (folders/engine-folder xid))]]
                     (if-not top
                       [{:skip {:ref r :reason "folder not found"}}]
                       (let [tree (folders/subtree (:path top))
                             parent (if (= "/" (:path top)) "/" (subs (:path top) 0 (inc (str/last-index-of (:path top) "/" (- (count (:path top)) 2)))))]
                         (for [row (ready-rows f {:folders (map :xid tree)})]
                           {:row row :ref r
                            :entry (str (subs (get-in row [(:folder f) :path]) (count parent)) (get row (:name f)))}))))
        all (concat (apply concat in-folders) loose)
        picked (vals (into {} (comp (filter :row) (map (juxt #(id/extract (:row %)) identity))) (reverse all)))]
    {:items (zip/unique-names (sort-by :entry picked))
     :skipped (vec (keep :skip all))
     :name (if (and (empty? files) (= 1 (count folders)))
             (let [top (some-> (first folders) str folders/ref->xid folders/engine-folder)
                   leaf (some-> (:path top) (str/split #"/") last not-empty)]
               (str (or leaf "files") ".zip"))
             (str "files-" (java.time.LocalDate/now java.time.ZoneOffset/UTC) ".zip"))}))

(defn zip-files
  "POST /files/zip {files, folders}: one zip of the READY files named and inside the folders, streamed."
  [store request]
  (let [f (fields)
        {:keys [files folders] :as body} (json-body request)
        refs? #(or (nil? %) (and (sequential? %) (every? string? %)))]
    (cond
      (not (and (refs? files) (refs? folders))) (fail 400 "INVALID_BODY" "files and folders are arrays of xids or paths")
      (and (empty? files) (empty? folders)) (fail 400 "INVALID_BODY" "Name at least one file or folder")
      :else
      (let [{:keys [items skipped name]} (zip-plan f body)
            bytes (reduce + 0 (keep #(get (:row %) (:size f)) items))]
        (if (or (> (count items) (zip/max-entries)) (> bytes (zip/max-bytes)))
          (fail 413 "ZIP_TOO_LARGE" (str "A zip holds at most " (zip/max-entries) " files and " (zip/max-bytes) " bytes")
                {:files (count items) :bytes bytes :max_files (zip/max-entries) :max_bytes (zip/max-bytes)})
          (let [facts (engine-facts f (map #(id/extract (:row %)) items))
                items (mapv (fn [{:keys [row] :as item}]
                              (assoc item :key (get-in facts [(id/extract row) :key])
                                     :content-type (get row (:content-type f))))
                            items)]
            {:status 200
             :headers {"Content-Type" "application/zip"
                       "Content-Disposition" (disposition request name "application/zip")
                       "Cache-Control" "private, no-store"}
             :body (zip/stream store items skipped)
             ::entries (count items)}))))))

(defn needs-audit? [{:keys [uri request-method] :as request}]
  (and (not (zip-request? request))
       (not (re-matches links-uri uri))
       (or (#{:post :put :delete} request-method)
           (re-find #"^/files/[A-Za-z0-9]{22}/versions" uri))))

(defn dispatch
  [store {:keys [uri request-method] :as request} principal]
  (let [[file? xid tail version restore] (re-matches file-uri uri)
        [_ raw-path] (re-matches path-link uri)
        [folders? folder-xid folder-tail] (re-matches folder-uri uri)
        path (some-> (get-in request [:query-params "path"]) not-empty)]
    (cond
      (and (nil? audit/*audit-provider*)
           (not folders?) (needs-audit? request))
      (fail 503 "AUDIT_UNAVAILABLE" "File versions and deletions live in audit history, which is not available right now; downloads still work")

      (and (= "/files/search" (str/replace uri #"/$" "")) (= :get request-method)) (search-files request)
      (zip-request? request) (zip-files store request)
      (re-matches link-uri uri)
      (if (#{:get :head} request-method)
        (link-download store request (second (re-matches link-uri uri)))
        (fail 405 "METHOD_NOT_ALLOWED" "A file link only downloads: GET or HEAD"))
      (re-matches links-uri uri)
      (let [[_ file-xid link-xid] (re-matches links-uri uri)]
        (cond
          (and file-xid (= :post request-method)) (create-link! request principal file-xid)
          (and file-xid (= :get request-method)) (list-links file-xid)
          (and link-xid (= :delete request-method)) (delete-link! link-xid)
          :else (fail 405 "METHOD_NOT_ALLOWED" "Links: GET/POST /files/<xid>/links, DELETE /files/links/<link xid>")))
      (and folders? (= :put request-method) (nil? folder-xid) (nil? folder-tail)) (folder-put! request principal)
      folders? (folder-route request principal
                             (or folder-xid (some-> (get-in request [:query-params "path"]) not-empty folders/ref->xid))
                             folder-tail)
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
      (and xid (nil? tail) (= :put request-method)) (replace! store request principal xid)
      (and xid (nil? tail) (= :patch request-method)) (patch-file! request xid)
      (and xid (nil? tail) (= :delete request-method)) (remove! xid)
      (and xid (= "/versions" tail) version restore (= :post request-method)) (restore! store xid version principal)
      (and xid (= "/versions" tail) version (nil? restore) (#{:get :head} request-method)) (download-version store request xid version)
      (and xid (= "/versions" tail) (nil? version) (= :get request-method)) (versions xid)
      (and (nil? xid) (= uri "/files")) (fail 405 "METHOD_NOT_ALLOWED" "List with GET /files, upload with POST /files?name=...")
      :else (fail 404 "NOT_FOUND" "No such files endpoint"))))

(defn route
  "Answer one /files request; a ::problem thrown on the way becomes its error response."
  [store request principal]
  (try
    (binding [*key-fn* (let [f (get-in request [:query-params "key_format"])]
                         (when (and f (not= "snake" f)) (dk/format->key-fn f)))]
      (dispatch store request principal))
    (catch clojure.lang.ExceptionInfo e
      (if-let [problem (::problem (ex-data e))]
        (apply fail problem)
        (throw e)))))

(defn open-route
  "A request without a token: GET/HEAD of /files/<xid> or a path link, nothing else; nil when it is neither."
  [store {:keys [uri request-method] :as request}]
  (when (#{:get :head} request-method)
    (let [[_ xid] (re-matches #"/files/([A-Za-z0-9]{22})/?" uri)
          [_ raw-path] (re-matches path-link uri)
          [_ token] (re-matches link-uri uri)]
      (cond
        token (link-download store request token)
        xid (open-download store request xid)
        raw-path (if-let [target (:xid (access/with-principal nil (resolve-path (decode-path raw-path))))]
                   (open-download store request target)
                   (unauthorized request))))))

(defn browser-opened?
  "Whether the browser itself opened this request: same origin, typed or bookmarked, or a top-level navigation."
  [{:keys [headers]}]
  (let [site (get headers "sec-fetch-site")]
    (or (nil? site)
        (contains? #{"same-origin" "none"} site)
        (= "navigate" (get headers "sec-fetch-mode")))))

(defn cookie-principal
  "The principal behind the sign-in cookie, for a file link the browser opens without a token."
  [{:keys [uri request-method headers] :as request}]
  (when (and (#{:get :head} request-method)
             (nil? (get headers "authorization"))
             (or (re-matches #"/files/[A-Za-z0-9]{22}/?" uri) (re-matches path-link uri))
             (browser-opened? request))
    (when-let [sid (auth/session-cookie request)]
      (access/with-principal nil
        (let [{:keys [active authorized-at resource-owner]} (oauth/get-session sid)
              principal (when (and active authorized-at resource-owner)
                          (iam.context/get-user-context resource-owner))]
          (when (and principal (not (false? (:active principal))))
            principal))))))

(defn blob-write? [{:keys [uri request-method] :as request}]
  (and (#{:post :put} request-method) (not (str/starts-with? uri "/files/folders")) (not (zip-request? request))
       (not (re-matches links-uri uri))))

(defn event-of
  "The change a successful /files answer made, as the Logs tool's Files lens reads it; nil for reads and failures."
  [{:keys [uri request-method query-params]} {:keys [status body] ::keys [action] :as response}]
  (when (and status (<= 200 status 299) (not= :head request-method))
    (let [answer (delay (when (string? body) (try (json/read-str body) (catch Exception _ nil))))
          [file? xid tail version restore] (re-matches file-uri uri)
          [folders? folder-xid folder-tail] (re-matches folder-uri uri)
          link? (re-matches path-link uri)
          path (some-> (get query-params "path") not-empty)
          m request-method
          file-event (fn [action subject]
                       (cond-> {:action action :subject subject
                                :file (or xid (:xid @answer))
                                :folder (or (get-in @answer [:folder :path]) (when (and path (not xid)) (first (split-path path))))}
                         version (assoc :version (parse-long version))
                         (and path (not xid)) (assoc :path path)))]
      (cond
        (= "/files/zip" (str/replace uri #"/$" ""))
        (when (= :post m) {:action :downloaded :subject :zip :files (::entries response)})

        (re-matches link-uri uri)
        (when (= :get m) {:action :downloaded :subject :link :link (::link response)})

        (re-matches links-uri uri)
        (let [[_ file-xid link-xid] (re-matches links-uri uri)]
          (case m
            :post {:action :created :subject :link :file file-xid :link (:xid @answer)}
            :delete {:action :deleted :subject :link :file (::file response) :link link-xid}
            nil))

        folders?
        (when-let [[action subject] (cond
                                      (= "/share" folder-tail) [:shared :access]
                                      (= "/unshare" folder-tail) [:unshared :access]
                                      folder-tail nil
                                      (= :post m) [:created :folder]
                                      (= :put m) [:updated :folder]
                                      (= :patch m) [action :folder]
                                      (= :delete m) [:deleted :folder])]
          {:action action :subject subject :folder (or folder-xid path (:path @answer))})

        (and (= :get m) (or link? (and xid (nil? tail)) (and xid version (nil? restore)) (and (nil? xid) path (nil? tail))))
        (file-event :downloaded (if version :version :file))

        (or link? (not file?) (= "/info" tail) (= :get m)) nil
        restore (file-event :restored :version)
        (= :post m) (file-event :uploaded :file)
        (= :put m) (file-event (if (and (nil? xid) (= 201 status)) :uploaded :replaced) :file)
        (= :patch m) (file-event action :file)
        (= :delete m) (file-event :deleted :file)))))

(defn record!
  "Log the change `response` made; downloads only at debug, since every page view of a public file is one."
  [request response]
  (try
    (when-let [{:keys [action subject] :as event} (event-of request response)]
      (let [user (log/current-principal-xid)
            msg (str "File " (name subject) " " (name action) ": " (or (:file event) (:link event) (:path event) (:folder event) (some-> (:files event) (str " files"))))]
        (if (= :downloaded action)
          (log/debug {:id ::changed :user-xid user :data event} msg)
          (log/info {:id ::changed :user-xid user :data event} msg))))
    (catch Throwable _ nil))
  response)

(defn handler
  "Ring handler for the /files prefix; without an Authorization header only public files download."
  [store-fn request]
  (try
    (if-let [principal (or (:principal (auth/authenticate-request request engine/platform-audience))
                           (cookie-principal request))]
      (access/with-principal principal
        (record! request
                 (if (blob-write? request)
                   (migrate/writing (fail 503 "FILES_MIGRATING" "Files are moving to new storage; uploads resume in a moment, downloads still work")
                                    #(route (store-fn) request principal))
                   (route (store-fn) request principal))))
      (or (when (nil? (get-in request [:headers "authorization"]))
            (some->> (open-route (store-fn) request) (record! request)))
          (unauthorized request)))
    (catch Throwable t
      (log/error! {:id ::request-failed :msg "Files request failed"
                   :data {:action :failed :subject :files :uri (:uri request) :method (:request-method request)}}
                  t)
      (fail 500 "INTERNAL_ERROR" (first (engine/safe-error-message t))))))
