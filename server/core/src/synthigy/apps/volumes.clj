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

(ns synthigy.apps.volumes
  "An App's volumes: the set the portal keeps for it, the State, Used MB and Last Error it reports, and their removal after the grace period."
  (:require
   [clojure.string :as str]
   [synthigy.apps.model]
   [synthigy.dataset :as dataset]
   [synthigy.iam.access :as access]
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor])
  (:import
   [java.util Date]))

(def default-path "/data")

(def grace-ms (* 7 24 60 60 1000))

(def timeout-ms (* 10 60 1000))

(def system-paths ["/bin" "/boot" "/dev" "/etc" "/lib" "/lib32" "/lib64" "/proc" "/run" "/sbin" "/sys" "/tmp" "/usr"])

(def path-shape #"(/[A-Za-z0-9._-]+)+")

(def selection {:xid nil :size_mb nil :mount_path nil :state nil :last_error nil :used_mb nil :delete_after nil})

(defn mount-path [v]
  (or (some-> (:mount_path v) str/trim not-empty) default-path))

(defn path-error
  "Why `path` cannot be a mount path, or nil."
  [path]
  (cond
    (> (count path) 255) "is longer than 255 characters"
    (not (re-matches path-shape path)) "must be an absolute path of letters, digits, dots, dashes and underscores, with no trailing slash"
    (some #{"." ".."} (str/split path #"/")) "must not contain . or .."
    (some #(or (= path %) (str/starts-with? path (str % "/"))) system-paths) "is a system directory"))

(defn state [v] (some-> (:state v) name))

(defn live [vols] (remove #(= "released" (state %)) vols))

(defn limit-error
  "Why an App with volume limit `limit` cannot have `vols`, or nil."
  [limit vols]
  (let [total (reduce + 0 (keep :size_mb vols))
        limit (or limit 0)
        paths (map mount-path vols)]
    (cond
      (some #(not (pos-int? (:size_mb %))) vols)
      "A volume's Size MB must be a positive number"

      (and (seq vols) (zero? limit))
      "This App may have no volumes: an admin sets Volume Limit MB in its App Configuration"

      (> total limit)
      (str "The App's volumes ask for " total " MB together; it may have " limit " MB")

      (not= (count paths) (count (distinct paths)))
      (str "Two volumes of the App mount at " (some (fn [[p n]] (when (> n 1) p)) (frequencies paths)))

      :else
      (some (fn [p] (some->> (path-error p) (str "Mount path " p " "))) (distinct paths)))))

(defn of-app [client-xid]
  (access/with-principal nil
    (:volumes (dataset/get-entity :iam/app {:xid client-xid} {:volumes [{:selections selection}]}))))

(defn request
  "The volumes part of an app.deploy request."
  [vols]
  (mapv (fn [v] {:id (:xid v) :path (mount-path v) :sizeMB (:size_mb v)}) vols))

(defn assert-ready!
  "Refuse to start an App whose volumes are not all ready."
  [vols]
  (when-let [v (first (remove #(= "ready" (state %)) vols))]
    (throw (ex-info (str "Volume " (mount-path v) " is " (or (state v) "not created yet")
                         (some->> (:last_error v) (str ": ")))
                    {}))))

(defn write! [xid data]
  (access/with-principal nil (dataset/stack-entity :apps/volume (assoc data :xid xid))))

(defn apply-reports!
  "Write the portal's report on each volume onto its row, where it changed."
  [vols reports]
  (let [by-xid (into {} (map (juxt :xid identity)) vols)]
    (doseq [{:keys [id state error usedMB]} reports
            :let [v (by-xid id)]
            :when (and v (#{"pending" "ready" "failed"} state))
            :let [data (cond-> {:state state :last_error (not-empty error)}
                         (number? usedMB) (assoc :used_mb (long usedMB)))
                  current (-> (select-keys v (keys data)) (update :state #(some-> % name)))]
            :when (not= data current)]
      (write! id (update data :state keyword)))))

(defn report!
  "Write an asynchronous report from the portal onto the volumes it names."
  [reports]
  (let [ids (vec (keep :id reports))]
    (when (seq ids)
      (apply-reports! (access/with-principal nil (dataset/search-entity :apps/volume {:xid {:_in ids}} selection))
                      reports))))

(defn sandbox
  "The App's sandbox type, as the portal names it."
  [client-xid]
  (some-> (access/with-principal nil (dataset/get-entity :iam/app {:xid client-xid} {:configuration [{:selections {:sandbox_type nil}}]}))
          :configuration :sandbox_type name))

(defn sync!
  "Tell the portal an App's volumes and write back what it reports; a refusal fails them all."
  [client-xid app]
  (let [vols (live (of-app client-xid))]
    (try
      (let [{reports :volumes} (supervisor/call-portal "app.volumes" {:app app :sandbox (sandbox client-xid) :volumes (mapv #(select-keys % [:id :sizeMB]) (request vols))} timeout-ms)]
        (apply-reports! vols reports))
      (catch Throwable t
        (apply-reports! vols (for [v vols] {:id (:xid v) :state "failed" :error (or (ex-message t) (str (class t)))}))
        (log/warn {:id ::sync-failed :data {:action :modified :subject :app :client client-xid :error (ex-message t)}}
                  "App volumes not applied")))
    vols))

(defn usage!
  "Keep Used MB from the portal's usage samples, written only when it moved."
  [reports]
  (doseq [{:keys [id usedMB]} reports
          :when (and (string? id) (number? usedMB))
          :let [v (access/with-principal nil (dataset/get-entity :apps/volume {:xid id} {:xid nil :used_mb nil}))]
          :when (and v (not= (long usedMB) (:used_mb v)))]
    (write! id {:used_mb (long usedMB)})))

(defn release-orphans!
  "Mark volumes whose App is gone released, to be deleted once the grace period ends."
  []
  (let [until (Date. (+ (System/currentTimeMillis) grace-ms))]
    (doseq [v (access/with-principal nil
                (dataset/search-entity :apps/volume nil {:xid nil :state nil :app [{:selections {:xid nil}}]}))
            :when (and (nil? (:app v)) (not= "released" (state v)))]
      (write! (:xid v) {:state :released :delete_after until})
      (log/info {:id ::released :data {:action :modified :subject :app :volume (:xid v) :delete-after until}}
                "Volume of a deleted App released"))))

(defn delete-data! [xid]
  (supervisor/call-portal "app.volume-delete" {:id xid} timeout-ms))

(defn sweep!
  "Delete the released volumes whose grace period is over, data first."
  []
  (let [now (Date.)]
    (doseq [v (access/with-principal nil
                (dataset/search-entity :apps/volume {:state {:_eq "released"}} {:xid nil :delete_after nil}))
            :when (and (:delete_after v) (.before ^Date (:delete_after v) now))]
      (try
        (delete-data! (:xid v))
        (access/with-principal nil (dataset/delete-entity :apps/volume {:xid (:xid v)}))
        (log/info {:id ::deleted :data {:action :deleted :subject :app :volume (:xid v)}} "Released volume deleted")
        (catch Throwable t
          (write! (:xid v) {:last_error (str "Could not delete the volume: " (or (ex-message t) (str (class t))))}))))))
