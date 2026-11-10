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

(ns synthigy.files.probe
  "The files.probe verb: the portal setup page's Test for S3 storage."
  (:require
   [clojure.string :as str]
   [synthigy.files.storage :as storage]
   [synthigy.files.storage.s3 :as s3]
   [synthigy.supervisor :as supervisor])
  (:import
   [java.io ByteArrayInputStream InputStream]
   [java.nio.charset StandardCharsets]
   [java.util Arrays]))

(defn round-trip!
  "Write, read back, list and delete one throwaway object; throws at the first step that fails."
  [store]
  (let [key (str "synthigy-probe-" (str/replace (str (random-uuid)) "-" ""))
        body (.getBytes (str "Synthigy storage probe " key) StandardCharsets/UTF_8)
        spooled (storage/spool! (ByteArrayInputStream. body))]
    (storage/put-file! store key (:file spooled) (assoc spooled :content-type "text/plain"))
    (try
      (let [^InputStream in (or (storage/open-blob store key)
                                (throw (ex-info "The test object was written but cannot be read back" {:code :storage-error})))]
        (with-open [in in]
          (when-not (Arrays/equals body (.readAllBytes in))
            (throw (ex-info "The test object came back with different bytes" {:code :storage-error})))))
      (dorun (take 1 (storage/list-blobs store)))
      (finally
        (storage/delete-blob! store key)))))

(defn failure [^clojure.lang.ExceptionInfo e]
  (let [{:keys [status s3-code]} (ex-data e)]
    {:ok false
     :kind (if status "rejected" "invalid")
     :message (str (ex-message e)
                   (cond
                     (= "NoSuchBucket" s3-code) " — create the bucket first, Synthigy never creates it."
                     (#{"SignatureDoesNotMatch" "InvalidAccessKeyId"} s3-code) " — check the access key and the secret key."
                     (= 403 status) " — the keys must be able to put, get, list and delete objects in this bucket."))}))

(defn s3-store
  "The S3 store these settings describe, read exactly as boot reads them."
  [{:keys [bucket region endpoint path_style access_key_id secret_access_key]}]
  (s3/create {:bucket (not-empty bucket)
              :region (not-empty region)
              :endpoint (not-empty endpoint)
              :path-style? (true? path_style)
              :access-key (not-empty access_key_id)
              :secret-key (not-empty secret_access_key)}))

(defn probe
  "Round-trip one object through the S3 store these settings describe."
  [{:keys [bucket endpoint path_style] :as settings}]
  (try
    (round-trip! (s3-store settings))
    {:ok true :kind "ok"
     :message (str "Connected. Wrote, read, listed and deleted a test object in " bucket
                   " — files will be stored there.")}
    (catch clojure.lang.ExceptionInfo e
      (failure e))
    (catch Exception e
      {:ok false :kind "unreachable"
       :message (str "Could not reach " (or (not-empty endpoint) "AWS S3") ": "
                     (or (ex-message e) (.getSimpleName (class e)))
                     (when (and (not-empty endpoint) (not (true? path_style)))
                       " — self-hosted stores usually need path-style addressing."))})))

(supervisor/register-method! "files.probe" probe)
