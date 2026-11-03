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

(ns synthigy.files.migrate
  "Moving every blob to another store while the engine serves: copy, pause blob writes, catch up, switch."
  (:require
   [synthigy.files.probe :as probe]
   [synthigy.files.storage :as storage]
   [synthigy.files.storage.local :as local]
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor])
  (:import
   [java.io InputStream]
   [synthigy.files.storage.local LocalStorage]
   [synthigy.files.storage.s3 S3Storage]))

(defonce state (atom nil))

(defonce writes (atom 0))

(defn paused? []
  (contains? #{:waiting :catching-up :ready} (:phase @state)))

(defn writing
  "Run blob write `f`, or answer `refusal` while a move has blob writes paused."
  [refusal f]
  (swap! writes inc)
  (try
    (if (paused?) refusal (f))
    (finally (swap! writes dec))))

(defn location [store]
  (condp instance? store
    LocalStorage [:local (str (:root store))]
    S3Storage [:s3 (:endpoint (:cfg store)) (:bucket (:cfg store))]
    [:other (class store)]))

(defn copy-one!
  "Copy blob `key` unless the target holds it at the source's size; :copied, :skipped or :gone."
  [source target key]
  (let [size (:size (storage/head-blob source key))]
    (cond
      (nil? size) :gone
      (= size (:size (storage/head-blob target key))) :skipped
      :else
      (if-let [^InputStream in (storage/open-blob source key)]
        (let [spooled (with-open [in in] (storage/spool! in nil))]
          (storage/put-file! target key (:file spooled) spooled)
          (when-not (= size (:size (storage/head-blob target key)))
            (throw (ex-info "The copy does not match its source" {:code :storage-error :key key})))
          :copied)
        :gone))))

(defn check-stop! []
  (when (:stop @state)
    (throw (ex-info "Move aborted" {:code :aborted}))))

(defn copy-all!
  "Copy `keys` from source to target, counting outcomes into the move's state; throws at the first failure."
  [source target keys]
  (let [total (count keys)]
    (doseq [[i key] (map-indexed vector keys)]
      (check-stop!)
      (let [outcome (try
                      (copy-one! source target key)
                      (catch Exception e
                        (throw (ex-info (str "Could not copy " key ": " (or (ex-message e) (.getSimpleName (class e))))
                                        {:code :storage-error :key key} e))))]
        (swap! state update outcome (fnil inc 0))
        (supervisor/progress-update! {:done (inc i) :total total :detail key})))))

(defn listed [store]
  (mapv :key (storage/list-blobs store)))

(defn phase! [phase label]
  (check-stop!)
  (swap! state assoc :phase phase)
  (supervisor/progress-phase! label))

(defn finish! [outcome]
  (supervisor/progress-finish!)
  (swap! state #(when % (-> % (merge outcome) (dissoc :source :target :stop)))))

(defn move!
  "The background half of a move: copy, pause, catch up, then wait at :ready for commit or abort."
  [source target]
  (try
    ;; ponytail: every key held in memory per pass; stream the listing if stores reach tens of millions of blobs
    (let [first-keys (listed source)]
      (phase! :copying "copying files")
      (copy-all! source target first-keys)
      (phase! :waiting "pausing uploads")
      (while (pos? @writes)
        (check-stop!)
        (supervisor/progress-update! {:detail (str @writes " uploads still running")})
        (Thread/sleep 100))
      (phase! :catching-up "copying the last uploads")
      (let [seen (set first-keys)]
        (copy-all! source target (into [] (remove seen) (listed source))))
      (phase! :ready "ready to switch")
      (log/info {:id ::ready :data {:action :migrated :subject :files-storage
                                    :copied (:copied @state 0) :skipped (:skipped @state 0)}}
                "Files copied to the new store; uploads paused until the switch"))
    (catch Throwable t
      (let [aborted (= :aborted (:code (ex-data t)))]
        (when-not aborted
          (log/error! {:id ::failed :msg "Moving files to the new store failed"
                       :data {:action :migrating :subject :files-storage}}
                      t))
        (finish! {:phase (if aborted :aborted :failed) :error (ex-message t)})))))

(defn target-store [{:keys [storage] :as settings}]
  (case storage
    "local" (local/create)
    "s3" (probe/s3-store settings)
    (throw (ex-info "Pick local or s3 as the new store" {:code :storage-unavailable}))))

(defn checked-target
  "The target store and a failed verdict, exactly one of them."
  [{:keys [storage] :as settings}]
  (if (= "s3" storage)
    (let [v (probe/probe settings)]
      (if (:ok v) {:target (probe/s3-store settings)} {:verdict v}))
    (try
      (let [t (target-store settings)]
        (probe/round-trip! t)
        {:target t})
      (catch Exception e
        {:verdict {:ok false :kind "invalid" :message (or (ex-message e) (.getSimpleName (class e)))}}))))

(defn begin!
  "Start moving every blob of `source` to `target` in the background; answers a verdict at once."
  [source target]
  (cond
    (= (location source) (location target))
    {:ok false :kind "invalid" :message "Files are already stored there"}
    (not (supervisor/progress-begin! :files-migrate "listing files"))
    {:ok false :kind "busy" :message "Another long operation is running on this engine"}
    :else
    (do
      (reset! state {:phase :listing :source source :target target :started-at (java.util.Date.)})
      (log/info {:id ::starting :data {:action :migrating :subject :files-storage :to (first (location target))}}
                "Moving files to a new store")
      (future (move! source target))
      {:ok true :kind "ok" :message "Moving files"})))

(defn start!
  "Check the store `settings` describe, then begin moving `source` there."
  [source settings]
  (if (nil? source)
    {:ok false :kind "invalid" :message "The files module is not running"}
    (let [{:keys [target verdict]} (checked-target settings)]
      (or verdict (begin! source target)))))

(defn status []
  (if-let [s @state]
    (-> s (dissoc :source :target :stop) (update :phase name) (assoc :paused (paused?)))
    {:phase nil}))

(defn commit!
  "Hand the copied store to `install!`, then lift the pause; only from :ready."
  [install!]
  (let [{:keys [phase target]} @state]
    (if (not= :ready phase)
      {:ok false :message "No move is ready to switch"}
      (do
        (install! target)
        (finish! {:phase :done})
        (log/info {:id ::switched :data {:action :migrated :subject :files-storage :to (first (location target))}}
                  "Files now stored in the new store")
        {:ok true}))))

(defn abort!
  "Stop a move or leave :ready; the current store stays."
  []
  (let [{:keys [phase]} @state]
    (case phase
      (:listing :copying :waiting :catching-up) (swap! state assoc :stop true)
      :ready (finish! {:phase :aborted})
      nil)
    {:ok true}))
