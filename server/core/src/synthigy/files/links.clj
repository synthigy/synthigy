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

(ns synthigy.files.links
  "Download links: a random token whose SHA-256 names a File Link row, so every engine on the database honours it and deleting the row ends it."
  (:require
   [next.jdbc :as jdbc]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db]
   [synthigy.files.guard :as guard]
   [synthigy.files.model :as model]
   [synthigy.iam.access :as access]
   [synthigy.log :as log])
  (:import
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest SecureRandom]
   [java.time Instant OffsetDateTime]
   [java.util Base64 Date HexFormat]))

(def default-expires 3600)

(def max-expires (* 7 24 3600))

(def token-pattern #"[A-Za-z0-9_-]{43}")

(defn deployed? [] (some? (model/entity :files/link)))

(defn fields []
  (let [k #(keyword (model/column :files/link %))
        rel (model/relation :files/link->file)
        label (if (= (id/entity :files/link) (id/extract (:from rel))) (:to-label rel) (:from-label rel))]
    {:token-hash (k :files.link/token-hash)
     :created-by (k :files.link/created-by)
     :expires-at (k :files.link/expires-at)
     :max-downloads (k :files.link/max-downloads)
     :downloads (k :files.link/downloads)
     :disposition (k :files.link/disposition)
     :file (keyword (naming/normalize-name label))}))

(defn selection [lf]
  {(id/key) nil
   (:expires-at lf) nil (:max-downloads lf) nil (:downloads lf) nil (:disposition lf) nil
   (:created-by lf) [{:selections {(id/key) nil}}]
   (:file lf) [{:selections {(id/key) nil}}]})

(defn token []
  (let [b (byte-array 32)]
    (.nextBytes (SecureRandom.) b)
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) b)))

(defn token-hash [t]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") (.getBytes ^String t StandardCharsets/UTF_8))))

(defn instant [t]
  (cond
    (instance? Instant t) t
    (instance? Date t) (.toInstant ^Date t)
    (instance? OffsetDateTime t) (.toInstant ^OffsetDateTime t)
    (string? t) (Instant/parse t)))

(defn seconds-left [lf link]
  (some-> (get link (:expires-at lf)) instant (.getEpochSecond) (- (.getEpochSecond (Instant/now)))))

(defn live? [lf link]
  (and link (pos? (or (seconds-left lf link) 0))))

(defn limited? [lf link] (some? (get link (:max-downloads lf))))

(defn inline? [lf link] (= "INLINE" (some-> (get link (:disposition lf)) name)))

(defn wire [lf link]
  {:xid (id/extract link)
   :file (some-> (get link (:file lf)) id/extract)
   :created_by (some-> (get link (:created-by lf)) id/extract)
   :expires_at (some-> (get link (:expires-at lf)) instant str)
   :max_downloads (get link (:max-downloads lf))
   :downloads (or (get link (:downloads lf)) 0)
   :inline (inline? lf link)})

(defn problem [status code message] {:problem [status code message]})

(defn options-problem [{:keys [expires max_downloads inline]}]
  (cond
    (and (some? expires) (not (and (int? expires) (<= 1 expires max-expires))))
    (problem 400 "INVALID_EXPIRES" (str "expires is whole seconds, 1 to " max-expires " (7 days)"))
    (and (some? max_downloads) (not (and (int? max_downloads) (pos? max_downloads))))
    (problem 400 "INVALID_BODY" "max_downloads is a whole number, 1 or more, or null")
    (and (some? inline) (not (boolean? inline)))
    (problem 400 "INVALID_BODY" "inline is true or false")))

(defn create!
  "A new link to file `file-xid` made by `principal`; the caller checked that the file is readable and links are on there."
  [principal file-xid {:keys [expires max_downloads inline] :as options}]
  (or (options-problem options)
      (when-not (daccess/entity-allows? (id/entity :files/link) #{:create})
        (problem 403 "FORBIDDEN" "Your roles may not create links"))
      (let [lf (fields)
            t (token)
            xid (id/generate)]
        (guard/as-module
         #(dataset/sync-entity :files/link {(id/key) xid
                                            (:token-hash lf) (token-hash t)
                                            (:created-by lf) {(id/key) (id/extract principal)}
                                            (:expires-at lf) (Date/from (.plusSeconds (Instant/now) (or expires default-expires)))
                                            (:max-downloads lf) max_downloads
                                            (:downloads lf) 0
                                            (:disposition lf) (if inline :INLINE :ATTACHMENT)
                                            (:file lf) {(id/key) file-xid}}))
        {:token t
         :link (access/with-principal nil
                 (wire lf (dataset/get-entity :files/link {(id/key) xid} (selection lf))))})))

(defn of-file
  "The live links to file `file-xid` the principal can see: File Admin all, others their own."
  [file-xid]
  (let [lf (fields)]
    (->> (dataset/search-entity :files/link
                                {(:expires-at lf) {:_gt (Date.)}
                                 :_order_by {(:expires-at lf) :asc}}
                                (assoc (selection lf) (:file lf)
                                       [{:selections {(id/key) nil}
                                         :args {(id/key) {:_eq file-xid} :_join :inner}}]))
         (mapv #(wire lf %)))))

(defn delete!
  "Delete link `link-xid` when the principal can see it: its creator or File Admin."
  [link-xid]
  (let [lf (fields)
        link (dataset/get-entity :files/link {(id/key) link-xid} {(id/key) nil (:file lf) [{:selections {(id/key) nil}}]})]
    (if link
      (do (guard/as-module #(dataset/delete-entity :files/link {(id/key) link-xid}))
          {:deleted link-xid :file (some-> (get link (:file lf)) id/extract)})
      (problem 404 "LINK_NOT_FOUND" "No such link"))))

(defn by-token
  "The link row `t` names, read as the engine with `sel`, or nil."
  ([t] (when (deployed?) (by-token t (selection (fields)))))
  ([t sel]
   (when (and (string? t) (re-matches token-pattern t) (deployed?))
     (access/with-principal nil
       (first (dataset/search-entity :files/link {(:token-hash (fields)) {:_eq (token-hash t)} :_limit 1} sel))))))

(defn used-up? [lf link]
  (and (limited? lf link) (>= (or (get link (:downloads lf)) 0) (get link (:max-downloads lf)))))

(defn count-download!
  "Count one download of `link`; false when a limited link has none left."
  [lf link]
  (let [table (naming/entity->table-name (model/entity :files/link))
        dl (name (:downloads lf))
        mx (name (:max-downloads lf))]
    (pos? (::jdbc/update-count
           (jdbc/execute-one! (:datasource db/*db*)
                              [(format "UPDATE \"%s\" SET \"%s\" = COALESCE(\"%s\", 0) + 1 WHERE \"%s\" = ? AND (\"%s\" IS NULL OR COALESCE(\"%s\", 0) < \"%s\")"
                                       table dl dl (name (id/key)) mx dl mx)
                               (id/extract link)])))))

(defn delete-rows! [xids]
  (access/with-principal nil
    (doseq [x xids]
      (guard/as-module #(dataset/delete-entity :files/link {(id/key) x})))))

(defn delete-of-file!
  "Delete every link to file `file-xid`."
  [file-xid]
  (when (deployed?)
    (let [lf (fields)]
      (delete-rows! (map id/extract
                         (access/with-principal nil
                           (dataset/search-entity :files/link nil
                                                  {(id/key) nil
                                                   (:file lf) [{:selections {(id/key) nil}
                                                                :args {(id/key) {:_eq file-xid} :_join :inner}}]})))))))

(defn sweep!
  "Delete links that expired, ran out of downloads or lost their file; answers how many."
  []
  (if-not (deployed?)
    0
    (let [lf (fields)
          sel (selection lf)
          found #(access/with-principal nil (dataset/search-entity :files/link % sel))
          doomed (->> (concat (found {(:expires-at lf) {:_lt (Date.)}})
                              (filter #(used-up? lf %) (found {(:max-downloads lf) {:_is_null false}}))
                              (access/with-principal nil
                                (dataset/search-entity :files/link nil (assoc sel (:file lf) [{:args {:_join :none}}]))))
                      (map id/extract)
                      distinct)]
      (delete-rows! doomed)
      (count doomed))))

(def sweep-interval-ms (* 3600 1000))

(defonce runner (atom nil))

(defn start!
  "Delete ended links every hour on a daemon thread of its own, first after one minute."
  []
  (when-not @runner
    (let [t (Thread.
             (fn []
               (try
                 (Thread/sleep 60000)
                 (loop []
                   (try (let [n (sweep!)]
                          (when (pos? n)
                            (log/info {:id ::swept :data {:action :deleted :subject :file-links :count n}}
                                      (str "Deleted " n " ended file links"))))
                        (catch InterruptedException e (throw e))
                        (catch Throwable t
                          (log/error! {:id ::sweep-failed :msg "File link sweep failed" :data {:action :failed :subject :file-links}} t)))
                   (Thread/sleep sweep-interval-ms)
                   (recur))
                 (catch InterruptedException _ nil)))
             "synthigy-file-links-sweeper")]
      (.setDaemon t true)
      (reset! runner t)
      (.start t))))

(defn stop! []
  (when-let [^Thread t @runner]
    (.interrupt t)
    (reset! runner nil)))
