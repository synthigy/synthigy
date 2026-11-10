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

(ns synthigy.files.storage.s3
  "Blob store on an S3-compatible bucket."
  (:require
   [synthigy.files.s3 :as s3]
   [synthigy.files.storage :as storage])
  (:import
   [java.io File]))

(defrecord S3Storage [cfg]
  storage/Storage
  (put-file! [_ key file meta]
    (try
      (s3/put-object! cfg (storage/check-key key) (.toPath ^File file) meta)
      (finally (.delete ^File file))))
  (open-blob [_ key]
    (s3/get-object cfg (storage/check-key key) nil))
  (open-blob [_ key start end]
    (storage/check-range start end)
    (s3/get-object cfg (storage/check-key key) [start end]))
  (head-blob [_ key]
    (s3/head-object cfg (storage/check-key key)))
  (copy-blob! [_ from to]
    (s3/copy-object! cfg (storage/check-key from) (storage/check-key to)))
  (delete-blob! [_ key]
    (s3/delete-object! cfg (storage/check-key key)))
  (list-blobs [_]
    (s3/list-objects cfg)))

(defn create
  "S3 store from {:bucket :region :endpoint :path-style? :access-key :secret-key}."
  [{:keys [bucket region] :as cfg}]
  (when-not (not-empty bucket)
    (throw (ex-info "SYNTHIGY_FILES_S3_BUCKET is not set" {:code :storage-unavailable})))
  (let [cfg (assoc cfg :region (or (not-empty region) "us-east-1"))]
    (s3/endpoint-uri cfg)
    (->S3Storage (-> cfg
                     (assoc :credentials (s3/credentials-fn cfg))
                     (dissoc :access-key :secret-key)))))
