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

(ns synthigy.files.sweeper
  "Removes the blobs no live file, no kept version and no recently deleted file needs."
  (:require
   [babashka.fs :as fs]
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.files.model :as model]
   [synthigy.files.storage :as storage]
   [synthigy.files.versions :as versions]
   [synthigy.iam.access :as access]
   [synthigy.log :as log])
  (:import
   [java.time Instant]))

(def min-age-seconds
  "Blobs younger than this are never touched: an upload stores bytes before its row commits."
  3600)

(def interval-ms (* 6 3600 1000))

(def owned-key #"([A-Za-z0-9]{22})-[A-Za-z0-9]{22}")

(def bare-key #"[A-Za-z0-9]{22}")

(defn owner [k] (second (re-matches owned-key k)))

(defn live-keys
  "File xid -> current storage key, read as the engine."
  []
  (let [k (keyword (model/column :files/file :files.file/storage-key))]
    (access/with-principal nil
      (into {} (keep (fn [r] (when-let [v (get r k)] [(id/extract r) v])))
            (dataset/search-entity :files/file nil {(id/key) nil k nil})))))

(defn record-events [xid]
  (access/with-principal nil
    (audit/events audit/*audit-provider* {:record-xid xid :between (versions/whole-history) :limit 10000})))

(defn deleted-at [events]
  (some->> events (filter #(= "delete" (:op %))) (map (comp str :ts)) sort last Instant/parse))

(defn older? [^Instant t ^Instant now secs]
  (.isBefore t (.minusSeconds now secs)))

(defn plan
  "The blobs of `blobs` to delete as of `now`, each with the reason; deletes nothing."
  [blobs now]
  (let [live (live-keys)
        current (set (vals live))
        grace (* versions/deleted-grace-days 24 3600)
        settled (remove #(not (older? (:modified %) now min-age-seconds)) blobs)
        {owned true other false} (group-by #(some? (owner (:key %))) settled)]
    (concat
     (for [[xid bs] (group-by (comp owner :key) owned)
           :let [events (record-events xid)
                 kept (if (contains? live xid)
                        (into #{(get live xid)} (map :key) (versions/kept (versions/writes events)))
                        (let [at (deleted-at events)]
                          (if (and at (not (older? at now grace))) (set (map :key bs)) #{})))]
           b bs
           :when (not (contains? kept (:key b)))]
       (assoc b :reason (cond
                          (contains? live xid) :expired-version
                          (deleted-at events) :deleted-file
                          :else :never-committed)))
     (for [b other
           :when (and (re-matches bare-key (:key b))
                      (not (contains? current (:key b)))
                      (older? (:modified b) now grace))]
       (assoc b :reason :unreferenced)))))

(defn sweep-spool!
  "Delete spool files left by uploads that died more than a day ago."
  [now]
  (when (fs/exists? storage/spool-dir)
    (doseq [f (fs/list-dir storage/spool-dir)
            :when (older? (.toInstant (fs/last-modified-time f)) now (* 24 3600))]
      (fs/delete-if-exists f))))

(defn sweep!
  "Delete what `plan` names; skipped while audit is absent or failing to persist, since versions come from audit."
  [store]
  (if (or (nil? audit/*audit-provider*) (:failing-since @audit/persist-state))
    (do (log/warn {:id ::paused :data {:action :skipped :subject :files-sweeper}}
                  "File sweep skipped: audit history is not available or not being persisted")
        {:skipped true})
    (let [now (Instant/now)
          doomed (vec (plan (storage/list-blobs store) now))]
      (doseq [{:keys [key]} doomed]
        (storage/delete-blob! store key))
      (sweep-spool! now)
      (let [summary {:deleted (count doomed) :reasons (frequencies (map :reason doomed))}]
        (log/info {:id ::swept :data (assoc summary :action :deleted :subject :files-sweeper)}
                  (str "File sweep deleted " (count doomed) " blobs"))
        summary))))

(defonce runner (atom nil))

(defn start!
  "Sweep every `interval-ms` on a daemon thread, first after ten minutes."
  [store-fn]
  (when-not @runner
    (let [t (Thread.
             (fn []
               (try
                 (Thread/sleep (* 10 60 1000))
                 (loop []
                   (try (some-> (store-fn) sweep!)
                        (catch InterruptedException e (throw e))
                        (catch Throwable t
                          (log/error! {:id ::failed :msg "File sweep failed" :data {:action :failed :subject :files-sweeper}} t)))
                   (Thread/sleep interval-ms)
                   (recur))
                 (catch InterruptedException _ nil)))
             "synthigy-files-sweeper")]
      (.setDaemon t true)
      (reset! runner t)
      (.start t))))

(defn stop! []
  (when-let [^Thread t @runner]
    (.interrupt t)
    (reset! runner nil)))
