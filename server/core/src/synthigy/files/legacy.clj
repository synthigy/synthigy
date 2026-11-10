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

(ns synthigy.files.legacy
  "Legacy file rows: bytes stored under the row's legacy UUID are copied to a module-owned, hashed blob."
  (:require
   [patcho.patch :as patch]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.files.guard :as guard]
   [synthigy.files.model :as model]
   [synthigy.files.storage :as storage]
   [synthigy.iam.access :as access]
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor])
  (:import
   [java.io InputStream]
   [java.net URLConnection]
   [java.security MessageDigest]
   [java.util HexFormat]))

(defn k [attr] (keyword (model/column :files/file attr)))

(defn without-key
  "File rows that carry no storage key and are not FAILED, read as the engine."
  []
  (access/with-principal nil
    (remove #(= :FAILED (some-> (get % (k :files.file/status)) keyword))
            (dataset/search-entity :files/file {:_where {(k :files.file/storage-key) {:_is_null true}}}
                                   {(id/key) nil (k :files.file/name) nil (k :files.file/content-type) nil
                                    (k :files.file/status) nil}))))

(defn legacy-key
  "The legacy store's key of file `xid`: its legacy UUID, which a migrated xid encodes; nil for any other xid."
  [xid]
  (some-> (try (id/nanoid->uuid xid) (catch Exception _ nil)) str not-empty))

(defn pending
  "Rows still to migrate: no storage key, not FAILED, and an xid that encodes a legacy UUID."
  []
  (filter #(legacy-key (id/extract %)) (without-key)))

(defn measure
  "SHA-256 and size of blob `key`, streamed without spooling; nil when absent."
  [store key]
  (when-let [^InputStream in (storage/open-blob store key)]
    (with-open [in in]
      (let [md (MessageDigest/getInstance "SHA-256")
            buf (byte-array 65536)]
        (loop [total 0]
          (let [n (.read in buf)]
            (if (neg? n)
              {:sha256 (.formatHex (HexFormat/of) (.digest md)) :size total}
              (do (.update md buf 0 n) (recur (+ total n))))))))))

(defn migrate-one!
  "Copy one legacy row's bytes to a module key, verify the copy and stamp the row; :migrated, :failed, :skipped or :error."
  [store row]
  (let [xid (id/extract row)
        old (legacy-key xid)
        file-name (get row (k :files.file/name))
        write! #(guard/as-module (fn [] (dataset/sync-entity :files/file (assoc % (id/key) xid))))]
    (if-not old
      :skipped
      (try
        (if-let [{:keys [sha256 size]} (measure store old)]
          (let [new-key (str xid "-" (id/generate))]
            (storage/copy-blob! store old new-key)
            (let [copied (:size (storage/head-blob store new-key))]
              (when-not (= size copied)
                (throw (ex-info "Legacy file copy does not match its source"
                                {:xid xid :key new-key :size size :copied copied}))))
            (write! (cond-> {(k :files.file/storage-key) new-key
                             (k :files.file/hash) sha256
                             (k :files.file/size) size
                             (k :files.file/status) :READY}
                      (nil? file-name) (assoc (k :files.file/name) new-key)
                      (nil? (get row (k :files.file/content-type)))
                      (assoc (k :files.file/content-type)
                             (or (some-> file-name URLConnection/guessContentTypeFromName) "application/octet-stream"))))
            :migrated)
          (do (write! {(k :files.file/status) :FAILED})
              :failed))
        (catch Throwable t
          (log/error! {:id ::file-failed :msg "Legacy file migration failed"
                       :data {:action :migrating :subject :files-legacy :xid xid}}
                      t)
          :error)))))

(defn migrate!
  "Migrate every pending legacy row; a rerun resumes with the rows not yet migrated."
  [store]
  (let [rows (vec (pending))
        total (count rows)
        skipped (- (count (without-key)) total)]
    (log/info {:id ::starting :data {:action :migrating :subject :files-legacy :files total}}
              (str "Migrating " total " legacy files"))
    (let [results (reduce (fn [acc [i row]]
                            (when (and (pos? i) (zero? (mod i 100)))
                              (supervisor/progress-update! {:detail (str "legacy files " i "/" total) :done i :total total})
                              (log/info {:id ::progress :data {:action :migrating :subject :files-legacy :done i :files total}}
                                        (str "Legacy files: " i "/" total)))
                            (update acc (access/with-principal nil (migrate-one! store row)) (fnil inc 0)))
                          (cond-> {} (pos? skipped) (assoc :skipped skipped))
                          (map-indexed vector rows))]
      (log/info {:id ::done :data (assoc results :action :migrated :subject :files-legacy :files total)}
                (str "Legacy files: " (:migrated results 0) " migrated, " (:failed results 0) " FAILED, "
                     (:skipped results 0) " skipped, " (:error results 0) " errors"))
      results)))

(defn reachable?
  "True when the store holds the bytes of any of the first legacy rows; throws when the store itself cannot be reached."
  [store rows]
  (boolean (some #(some->> (legacy-key (id/extract %)) (storage/head-blob store)) (take 20 rows))))

(def ^:dynamic *store* nil)

(patch/current-version :synthigy.files/legacy "1.0.0")

(patch/installed-version :synthigy.files/legacy (if (seq (pending)) "0" "1.0.0"))

(patch/upgrade :synthigy.files/legacy "1.0.0" (migrate! *store*))

(defn level!
  "Migrate legacy rows before the module serves; boot waits for it, and a store that cannot be reached fails the start."
  [store]
  (let [rows (pending)]
    (cond
      (empty? rows) nil
      (not (reachable? store rows))
      (log/warn {:id ::bytes-not-found :data {:action :skipped :subject :files-legacy :files (count rows)}}
                (str (count rows) " legacy files wait for migration, but the configured store does not hold their bytes; "
                     "point SYNTHIGY_FILES_STORAGE / SYNTHIGY_FILES_S3_* at the legacy bucket and restart"))
      :else
      (binding [*store* store]
        (supervisor/progress-patching! :synthigy.files/legacy "0" "1.0.0")
        (patch/level! :synthigy.files/legacy)))))

(defn cleanup!
  "Delete legacy objects whose row is READY with a matching hash; the admin runs it after checking the migration."
  [store]
  (let [rows (access/with-principal nil
               (dataset/search-entity :files/file {:_where {(k :files.file/status) {:_eq :READY}}}
                                      {(id/key) nil (k :files.file/hash) nil (k :files.file/storage-key) nil}))
        results (reduce (fn [acc row]
                          (let [old (legacy-key (id/extract row))]
                            (cond
                              (or (nil? old) (nil? (get row (k :files.file/storage-key)))) acc
                              (nil? (storage/head-blob store old)) (update acc :absent (fnil inc 0))
                              (= (get row (k :files.file/hash)) (:sha256 (measure store old)))
                              (do (storage/delete-blob! store old) (update acc :deleted (fnil inc 0)))
                              :else (update acc :kept (fnil inc 0)))))
                        {}
                        rows)]
    (log/info {:id ::cleaned :data (assoc results :action :deleted :subject :files-legacy)}
              (str "Legacy objects: " (:deleted results 0) " deleted, " (:kept results 0) " kept (hash differs), "
                   (:absent results 0) " already gone"))
    results))
