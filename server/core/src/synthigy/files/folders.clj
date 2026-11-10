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

(ns synthigy.files.folders
  "Folder structure: names, paths, the ROOT folder, inherited access lists, empty-only delete."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.dataset.sql.query :as query]
   [synthigy.files.guard :as guard]
   [synthigy.files.model :as model]
   [synthigy.iam.access :as access]))

(defn root [] (id/data :files/root-folder))

(defn name-field [] (keyword (model/column :files/folder :files.folder/name)))

(defn path-field [] (keyword (model/column :files/folder :files.folder/path)))

(defn parent-field []
  (keyword (naming/normalize-name (:to-label (model/relation :files/folder->parent)))))

(defn public-field [] (some-> (model/column :files/folder :files.folder/public) keyword))

(defn cache-field [] (some-> (model/column :files/folder :files.folder/cache) keyword))

(defn can-link-field [] (some-> (model/column :files/folder :files.folder/can-link) keyword))

(defn ensure-root!
  "The ROOT folder, path /, created when missing."
  []
  (access/with-principal nil
    (when-not (dataset/get-entity :files/folder {(id/key) (root)} {(id/key) nil})
      (dataset/sync-entity :files/folder {(id/key) (root) (name-field) "ROOT" (path-field) "/"}))))

(defn fact-selection []
  (let [pf (public-field) cf (cache-field) lf (can-link-field)]
    (cond-> {(id/key) nil (path-field) nil (parent-field) [{:selections {(id/key) nil}}]}
      pf (assoc pf nil)
      cf (assoc cf nil)
      lf (assoc lf nil))))

(defn fact [r]
  (let [pf (public-field) cf (cache-field) lf (can-link-field)]
    {:xid (id/extract r) :path (get r (path-field)) :parent (some-> (get r (parent-field)) id/extract)
     :public (when pf (get r pf)) :cache (when cf (get r cf)) :can-link (when lf (get r lf))}))

(defn engine-where
  "Xid, path, parent xid, Public and Cache of the folders `args` match, read as the engine."
  ([args] (engine-where args (fact-selection)))
  ([args selection]
   (access/with-principal nil
     (mapv fact (dataset/search-entity :files/folder args selection)))))

(defn engine-folder [xid]
  (when xid (first (engine-where {:_where {(id/key) {:_eq xid}}}))))

(defn at-path [path]
  (first (engine-where {:_where {(path-field) {:_eq path}}})))

(defn at-paths
  "Path -> xid of the folders among `paths`."
  [paths]
  (if (empty? paths)
    {}
    (into {} (map (juxt :path :xid)) (engine-where {:_where {(path-field) {:_in (vec (distinct paths))}}}))))

(defn engine-children
  "The folders directly inside `xid`, at most `limit` when given."
  [xid & [limit]]
  (engine-where (cond-> {} limit (assoc :_limit limit))
                (assoc (fact-selection) (parent-field)
                       [{:selections {(id/key) nil} :args {:_where {(id/key) {:_eq xid}} :_join :inner}}])))

(defn ref->xid
  "Folder xid for `ref`: a path when it starts with /, otherwise the xid itself."
  [ref]
  (if (and (string? ref) (str/starts-with? ref "/"))
    (let [path (cond-> ref (not (str/ends-with? ref "/")) (str "/"))]
      (or (:xid (at-path path)) ref))
    ref))

(defn visible? [xid]
  (some? (dataset/get-entity :files/folder {(id/key) xid} {(id/key) nil})))

(defn writable?
  "Whether the principal may change folder `xid`: the role grants update on Folder and the folder's guards let it in."
  [xid]
  (and (daccess/entity-allows? (id/entity :files/folder) #{:update})
       (query/row-writable? (id/entity :files/folder) xid)))

(defn lists-readable?
  "Whether the principal's roles may read the folder access lists, the check a /data read makes."
  []
  (every? (fn [[_ _ k]] (daccess/relation-allows? (id/relation k) #{:read})) model/acl-relations))

(defn served
  "How files in folder `xid` are served: Public, Cache and Can Link each from the folder's own value, else the nearest folder above that has one."
  [facts xid]
  (loop [x xid seen #{} public nil cache nil can-link nil]
    (if-let [f (when-not (seen x) (get facts x))]
      (recur (:parent f) (conj seen x)
             (if (some? public) public (:public f))
             (if (some? cache) cache (:cache f))
             (if (some? can-link) can-link (:can-link f)))
      {:public (true? public) :cache cache :can-link (true? can-link)})))

(defn wire
  "A folder as the endpoints answer it: Public and Cache as they apply, its own values under :settings."
  [{:keys [xid] :as row} facts]
  (let [{:keys [path parent public cache can-link]} (get facts xid)
        applied (served facts xid)]
    {:xid xid
     :name (:name row)
     :path path
     :parent parent
     :writable (writable? xid)
     :public (:public applied)
     :cache (:cache applied)
     :can_link (:can-link applied)
     :settings {:public public :cache cache :can_link can-link}}))

(defn prefixes
  "The folder paths from the top down to `path`: /a/b/ → (/a/ /a/b/)."
  [path]
  (let [parts (remove str/blank? (str/split (str path) #"/"))]
    (map #(str "/" (str/join "/" (take % parts)) "/") (range 1 (inc (count parts))))))

(defn facts
  "The folders `xids` and every folder above them, by xid: what `served` and `wire` walk."
  [xids]
  (let [own (when-let [xs (seq (distinct (remove nil? xids)))]
              (engine-where {:_where {(id/key) {:_in (vec xs)}}}))
        above (when (seq own)
                (engine-where {:_where {(path-field) {:_in (vec (into #{"/"} (mapcat (comp prefixes :path)) own))}}}))]
    (into {} (map (juxt :xid identity)) (concat own above))))

(defn chain
  "The folder at `path` and every folder above it, by xid, in one read: what `served` walks."
  [path]
  (into {} (map (juxt :xid identity)) (engine-where {:_where {(path-field) {:_in (vec (prefixes path))}}})))

(defn folder [xid]
  (when-let [row (dataset/get-entity :files/folder {(id/key) xid} {(id/key) nil (name-field) nil})]
    (wire {:xid xid :name (get row (name-field))} (facts [xid]))))

(defn children
  "The visible folders directly inside `parent-xid`."
  [parent-xid]
  (let [kids (mapv :xid (engine-children parent-xid))]
    (if (empty? kids)
      []
      (->> (dataset/search-entity :files/folder
                                  {:_where {(id/key) {:_in kids}} :_order_by (array-map (name-field) :asc (id/key) :asc)}
                                  {(id/key) nil (name-field) nil})
           (map (let [all (facts kids)] #(wire {:xid (id/extract %) :name (get % (name-field))} all)))
           vec))))

(defn inherited-acl
  "Parent's access lists plus the creator on read, write and delete, as wire relation values."
  [parent-xid creator-xid]
  (let [fields (model/acl-fields)
        lists (access/with-principal nil
                (dataset/get-entity :files/folder {(id/key) parent-xid}
                                    (into {(id/key) nil} (map (fn [k] [k [{:selections {(id/key) nil}}]])) (vals fields))))]
    (reduce-kv (fn [acc [access kind] k]
                 (let [held (map (fn [r] {(id/key) (id/extract r)}) (get lists k))]
                   (assoc acc k (vec (distinct (cond-> held
                                                 (and creator-xid (= :users kind) (#{:read :write :delete} access))
                                                 (conj {(id/key) creator-xid})))))))
               {} fields)))

(defn problem [status code message] {:problem [status code message]})

(defn create!
  "A new folder `folder-name` inside `parent-xid` (ROOT when nil), inheriting its access lists."
  [principal folder-name parent-xid]
  (let [parent-xid (or parent-xid (root))
        parent (engine-folder parent-xid)]
    (cond
      (model/name-problem folder-name) (problem 400 "INVALID_NAME" (model/name-problem folder-name))
      (or (nil? parent) (and (not= parent-xid (root)) (not (visible? parent-xid))))
      (problem 404 "FOLDER_NOT_FOUND" "No such parent folder")
      (not (writable? parent-xid)) (problem 403 "FORBIDDEN" "You may not create folders here")
      :else
      (let [path (str (:path parent) folder-name "/")]
        (if (at-path path)
          (problem 409 "FOLDER_NAME_TAKEN" "A folder with this name already exists here")
          (let [row (guard/as-module
                     #(dataset/sync-entity :files/folder
                                           (merge {(name-field) folder-name
                                                   (path-field) path
                                                   (parent-field) {(id/key) parent-xid}}
                                                  (inherited-acl parent-xid (id/extract principal)))))]
            {:folder (folder (id/extract row))}))))))

(defn first-missing
  "The first folder on `path` that is absent or hidden from the principal, or nil when all are there."
  [path]
  (let [by-path (at-paths (prefixes path))]
    (some (fn [p] (let [x (get by-path p)] (when (or (nil? x) (not (visible? x))) p)))
          (prefixes path))))

(defn settings-problem
  "Why `settings` ({:public :cache :can_link}) cannot be written by the principal, or nil."
  [settings]
  (let [{:keys [public cache can_link]} settings]
    (cond
      (and (contains? settings :public) (some? public) (not (boolean? public)))
      (problem 400 "INVALID_SETTINGS" "public is true, false or null")
      (and (contains? settings :cache) (some? cache) (not (and (int? cache) (>= cache 0))))
      (problem 400 "INVALID_SETTINGS" "cache is whole seconds, 0 or more, or null")
      (and (contains? settings :can_link) (some? can_link) (not (boolean? can_link)))
      (problem 400 "INVALID_SETTINGS" "can_link is true, false or null")
      (and (contains? settings :public) (nil? (public-field)))
      (problem 400 "INVALID_SETTINGS" "This File System model has no Public attribute")
      (and (contains? settings :cache) (nil? (cache-field)))
      (problem 400 "INVALID_SETTINGS" "This File System model has no Cache attribute")
      (and (contains? settings :can_link) (nil? (can-link-field)))
      (problem 400 "INVALID_SETTINGS" "This File System model has no Can Link attribute")
      (some (fn [[k attr]] (and (contains? settings k)
                                (not (daccess/attribute-allows? (id/entity :files/folder) (id/data attr) :write))))
            [[:public :files.folder/public] [:cache :files.folder/cache] [:can_link :files.folder/can-link]])
      (problem 403 "ATTRIBUTE_FORBIDDEN" "Your roles may not change this folder setting"))))

(defn write-settings!
  "Write Public, Cache and Can Link of folder `xid` with the caller's grants, so attribute permissions apply."
  [xid settings]
  (let [cols (cond-> {}
               (contains? settings :public) (assoc (public-field) (:public settings))
               (contains? settings :cache) (assoc (cache-field) (:cache settings))
               (contains? settings :can_link) (assoc (can-link-field) (:can_link settings)))]
    (when (seq cols)
      (guard/as-endpoint #(dataset/sync-entity :files/folder (assoc cols (id/key) xid))))))

(defn ensure!
  "Folder `path` and every missing folder above it, created as the principal; then its `settings` (Public, Cache, Can Link)."
  [principal path settings]
  (let [path (cond-> (str path) (not (str/ends-with? (str path) "/")) (str "/"))]
    (or (settings-problem settings)
        (when (and (= "/" path) (seq settings))
          (problem 403 "FORBIDDEN" "ROOT has no settings"))
        (let [by-path (at-paths (prefixes path))]
          (loop [[p & more] (prefixes path) parent (root)]
            (cond
              (nil? p)
              (cond
                (and (seq settings) (not (writable? parent))) (problem 403 "FORBIDDEN" "You may not change this folder")
                :else (do (write-settings! parent settings)
                          {:folder (folder parent)}))

              (get by-path p)
              (let [x (get by-path p)]
                (if (visible? x) (recur more x) (problem 404 "FOLDER_NOT_FOUND" (str "No such folder: " p))))

              :else
              (let [{refused :problem :as r} (create! principal (last (remove str/blank? (str/split p #"/"))) parent)]
                (if refused r (recur more (get-in r [:folder :xid]))))))))))

(defn subtree
  "Folders at or under `path`, deepest last."
  [path]
  (->> (engine-where {:_where {(path-field) {:_like (str (str/replace path #"[\\%_]" "\\\\$0") "%")}}})
       (filter #(str/starts-with? (:path %) path))
       (sort-by (comp count :path))))

(defn update!
  "Rename and/or move folder `xid`; paths below it follow."
  [xid {new-name :name new-parent :parent}]
  (let [current (engine-folder xid)]
    (cond
      (or (nil? current) (not (visible? xid))) (problem 404 "FOLDER_NOT_FOUND" "No such folder")
      (= xid (root)) (problem 403 "FORBIDDEN" "ROOT cannot be renamed or moved")
      (not (writable? xid)) (problem 403 "FORBIDDEN" "You may not change this folder")
      (and new-name (model/name-problem new-name)) (problem 400 "INVALID_NAME" (model/name-problem new-name))
      :else
      (let [parent-xid (or new-parent (:parent current) (root))
            parent (engine-folder parent-xid)
            folder-name (or new-name (last (str/split (:path current) #"/")))
            path (str (:path parent) folder-name "/")]
        (cond
          (or (nil? parent) (and (not= parent-xid (root)) (not (visible? parent-xid))))
          (problem 404 "FOLDER_NOT_FOUND" "No such parent folder")
          (and new-parent (not (writable? parent-xid))) (problem 403 "FORBIDDEN" "You may not move folders there")
          (str/starts-with? (:path parent) (:path current)) (problem 409 "FOLDER_CYCLE" "A folder cannot move inside itself")
          (and (not= path (:path current)) (at-path path))
          (problem 409 "FOLDER_NAME_TAKEN" "A folder with this name already exists there")
          :else
          (let [old (:path current)
                moved (for [{f :xid p :path} (subtree old) :when (not= f xid)]
                        {(id/key) f (path-field) (str path (subs p (count old)))})]
            (guard/as-module
             #(dataset/sync-entity :files/folder
                                   (vec (cons (cond-> {(id/key) xid (name-field) folder-name (path-field) path}
                                                new-parent (assoc (parent-field) {(id/key) parent-xid}))
                                              moved))))
            {:folder (folder xid)}))))))

(defn empty-folder?
  [xid]
  (access/with-principal nil
    (and (empty? (engine-children xid 1))
         (empty? (dataset/search-entity :files/file {:_limit 1}
                                        {(id/key) nil
                                         (keyword (naming/normalize-name (:to-label (model/relation :files/file->folder))))
                                         [{:selections {(id/key) nil} :args {:_where {(id/key) {:_eq xid}} :_join :inner}}]})))))

(defn delete!
  "Delete folder `xid`: never ROOT, only when empty, with the caller's delete rights."
  [xid]
  (cond
    (not (visible? xid)) (problem 404 "FOLDER_NOT_FOUND" "No such folder")
    (= xid (root)) (problem 403 "FORBIDDEN" "ROOT cannot be deleted")
    (not (empty-folder? xid)) (problem 409 "FOLDER_NOT_EMPTY" "Only an empty folder can be deleted")
    :else (do (guard/as-endpoint #(dataset/delete-entity :files/folder {(id/key) xid}))
              nil)))

(def accesses {"read" :read "write" :write "delete" :delete})

(defn access-lists
  "Who holds read, write and delete on folder `xid`: {access {:users [{xid name}] :groups [...]}}, read as the engine; a user's name is the display name."
  [xid]
  (let [fields (model/acl-fields)
        row (access/with-principal nil
              (dataset/get-entity :files/folder {(id/key) xid}
                                  (into {(id/key) nil}
                                        (map (fn [[[_ kind] k]]
                                               [k [{:selections (if (= :users kind)
                                                                  {(id/key) nil :display_name nil}
                                                                  {(id/key) nil :name nil})}]]))
                                        fields)))]
    (reduce-kv (fn [acc [a kind] k]
                 (assoc-in acc [a kind] (mapv #(hash-map (id/key) (get % (id/key))
                                                         :name (if (= :users kind) (:display_name %) (:name %)))
                                              (get row k))))
               {:read {:users [] :groups []} :write {:users [] :groups []} :delete {:users [] :groups []}}
               fields)))

(defn share!
  "Add (`add?`) or remove `users` and `groups` on folder `xid`'s `access` list; writers of the folder may."
  [xid {:keys [access users groups]} add?]
  (let [a (accesses (some-> access name))
        fields (model/acl-fields)]
    (cond
      (not (visible? xid)) (problem 404 "FOLDER_NOT_FOUND" "No such folder")
      (nil? a) (problem 400 "INVALID_ACCESS" "access is read, write or delete")
      (not (writable? xid)) (problem 403 "FORBIDDEN" "You may not share this folder")
      :else
      (let [current (access-lists xid)
            change (fn [kind given]
                     (let [held (set (map id/extract (get-in current [a kind])))
                           given (set (map str given))]
                       (mapv (fn [x] {(id/key) x}) (if add? (into held given) (remove given held)))))]
        (guard/as-module
         #(dataset/sync-entity :files/folder
                               (cond-> {(id/key) xid}
                                 (seq users) (assoc (get fields [a :users]) (change :users users))
                                 (seq groups) (assoc (get fields [a :groups]) (change :groups groups)))))
        {:folder (access-lists xid)}))))
