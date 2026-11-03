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

(ns synthigy.files.versions
  "File versions, read from audit history: every write that stored a new storage key is a version."
  (:require
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset.id :as id]
   [synthigy.files.model]
   [synthigy.iam.access :as access])
  (:import
   [java.time Instant]))

(def keep-count
  "Versions kept per file; older blobs are swept."
  10)

(def deleted-grace-days
  "Days a deleted file's versions are kept."
  30)

(defn attrs []
  {:key (id/data :files.file/storage-key)
   :hash (id/data :files.file/hash)
   :size (id/data :files.file/size)
   :type (id/data :files.file/content-type)})

(defn writes
  "Writes of one record's `events` that stored a storage key, oldest first; size, hash and type carry forward."
  [events]
  (let [{:keys [key hash size type]} (attrs)]
    (->> events
         (group-by :seq)
         vals
         (keep (fn [es]
                 (let [by-attr (into {} (map (juxt :attribute-xid :value)) es)]
                   (when-let [k (get by-attr key)]
                     (let [{:keys [ts actor seq]} (first es)]
                       {:id seq
                        :at ts
                        :by actor
                        :key k
                        :hash (get by-attr hash)
                        :size (some-> (get by-attr size) str parse-long)
                        :content_type (get by-attr type)})))))
         (sort-by :id)
         (reduce (fn [acc w]
                   (let [prev (peek acc)]
                     (conj acc (cond-> w
                                 (nil? (:size w)) (assoc :size (:size prev))
                                 (nil? (:hash w)) (assoc :hash (:hash prev))
                                 (nil? (:content_type w)) (assoc :content_type (:content_type prev))))))
                 []))))

(defn whole-history []
  [(str (.minusSeconds (Instant/now) (* 20 365 24 3600))) (str (Instant/now))])

(defn file-writes
  "Every version write of file `xid`, oldest first, read as the engine."
  [xid]
  (access/with-principal nil
    (writes (audit/events audit/*audit-provider* {:record-xid xid :between (whole-history) :limit 10000}))))

(defn kept
  "The versions of `ws` (oldest first) still kept: the last `keep-count`."
  [ws]
  (vec (take-last keep-count ws)))
