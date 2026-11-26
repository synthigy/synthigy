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

(ns synthigy.apps.guard
  "App Package Status and Last Error are written by the engine alone; a person's rotation of an App's secret restarts the running App with one the engine mints; a sensitive App Variable's Value reads back only to its App; a variable name is set once per App."
  (:require
   [clojure.string :as str]
   [synthigy.apps.deploy :as deploy]
   [synthigy.apps.volumes :as volumes]
   [synthigy.apps.model]
   [synthigy.iam.access :as access]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.enhance :as enhance]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.dataset.sql.query :as query]))

(defn engine-columns
  "Column keys of the package attributes only the engine writes, from the deployed model."
  []
  (let [owned #{(id/data :apps.package/status) (id/data :apps.package/last-error)}]
    (into #{}
          (comp (filter #(owned (id/extract %))) (map #(keyword (naming/normalize-name (:name %)))))
          (:attributes (core/get-entity (dataset/deployed-model) (id/entity :apps/package))))))

(defn assert-write!
  "Refuse a principal's write that sets a package's Status or Last Error."
  [analysis]
  (when-not (dataset/engine-write?)
    (when-let [e (core/get-entity (dataset/deployed-model) (id/entity :apps/package))]
      (let [owned (engine-columns)]
        (doseq [[_ row] (get-in analysis [:entity (naming/entity->table-name e)])
                column (keys row)
                :when (owned column)]
          (throw (ex-info (str "`" (name column) "` of an App Package is written by the engine; set Active and read the Status it reports.")
                          {:code "ENGINE_OWNED" :rule "apps_status" :attribute (name column)}))))))
  analysis)

(defmethod enhance/write [Object (id/entity :apps/package :xid)] [_ _ analysis _]
  (assert-write! analysis))

(def hostname-shape #"[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?")

(defn hostname-ends
  "[client tmp id, hostname tmp id] for each link the write makes between an App and a hostname."
  [analysis client-table]
  (for [[{:keys [relation] :as r} pairs] (concat (:relations/one analysis) (:relations/many analysis))
        :when (= relation (id/relation :apps/app->hostnames))
        [from to] pairs]
    (if (= client-table (:from/table r)) [from to] [to from])))

(defn assert-hostname-write!
  "Refuse a principal's hostname that is not one lowercase ASCII name, and giving a hostname to an App the principal may not change."
  [analysis]
  (when-not (dataset/engine-write?)
    (let [model (dataset/deployed-model)
          table (some-> (core/get-entity model (id/entity :apps/hostname)) naming/entity->table-name)
          client-table (some-> (core/get-entity model (id/entity :iam/app)) naming/entity->table-name)]
      (when table
        (doseq [[_ row] (get-in analysis [:entity table])
                :let [n (get row :name ::absent)]
                :when (not= ::absent n)]
          (when-not (and (string? n) (re-matches hostname-shape n))
            (throw (ex-info (str "A hostname is lowercase letters, digits, dots and dashes, nothing else: " (pr-str n))
                            {:code "HOSTNAME_INVALID" :rule "apps_hostname" :name n}))))
        (doseq [[client _] (hostname-ends analysis client-table)
                :let [xid (get-in analysis [:entity client-table client :xid])]
                :when (and xid (not (query/row-writable? (id/entity :iam/app) xid)))]
          (throw (ex-info "A hostname goes only to an App you may change"
                          {:code "ROW_FORBIDDEN" :rule "apps_hostname" :client xid}))))))
  analysis)

(defmethod enhance/write [Object (id/entity :apps/hostname :xid)] [_ _ analysis _]
  (assert-hostname-write! analysis))

(defn note-rotations!
  "Mark the OAuth Clients whose secret a person writes; the value is already hashed here."
  [analysis]
  (when-not (dataset/engine-write?)
    (when-let [e (core/get-entity (dataset/deployed-model) (id/entity :iam/app))]
      (doseq [[_ row] (get-in analysis [:entity (naming/entity->table-name e)])
              :let [secret (:secret row)]
              :when (some? secret)
              :let [xid (or (:xid row)
                            (when-let [cid (:id row)]
                              (:xid (access/with-principal nil (dataset/get-entity :iam/app {:id cid} {:xid nil})))))]
              :when xid]
        (deploy/mark-rotated! xid))))
  analysis)

(defmethod enhance/write [Object (id/entity :iam/app :xid)] [_ _ analysis _]
  (note-rotations! analysis))

(defn sensitive-owners
  "_eid of each sensitive App Variable among `eids`, to the _eid of its App's service user."
  [eids]
  (into {}
        (for [{:keys [_eid sensitive app]} (access/with-principal nil
                                             (dataset/search-entity :apps/variable {:_eid {:_in (vec eids)}}
                                                                    {:_eid nil :sensitive nil
                                                                     :app [{:selections {:service_user [{:selections {:_eid nil}}]}}]}))
              :when sensitive]
          [_eid (get-in app [:service_user :_eid])])))

(defn hide-sensitive
  "Null the Value of sensitive App Variables in `rows`, except for the App's own service user."
  [rows]
  (if-let [asked (seq (keep (fn [[eid row]] (when (contains? row :value) eid)) rows))]
    (let [me (daccess/principal-eid)]
      (reduce-kv (fn [rows eid owner]
                   (if (and (some? me) (= me owner)) rows (assoc-in rows [eid :value] nil)))
                 rows
                 (sensitive-owners asked)))
    rows))

(defmethod enhance/read [Object (id/entity :apps/variable :xid)] [_ _ rows]
  (hide-sensitive rows))

(defmethod enhance/history [Object (id/entity :apps/variable :xid)] [_ _ record-xids]
  (let [sensitive (into {}
                        (map (juxt :xid :sensitive))
                        (access/with-principal nil
                          (dataset/search-entity :apps/variable {:xid {:_in (vec record-xids)}} {:xid nil :sensitive nil})))
        value #{(keyword (id/data :apps.variable/value :xid))}]
    (into {} (for [x record-xids :when (get sensitive x true)] [x value]))))

(defn assert-sensitive-stays!
  "Refuse a principal's write that turns a sensitive App Variable back to plain."
  [analysis]
  (when-not (dataset/engine-write?)
    (when-let [e (core/get-entity (dataset/deployed-model) (id/entity :apps/variable))]
      (doseq [[_ row] (get-in analysis [:entity (naming/entity->table-name e)])
              :when (and (contains? row :sensitive) (not (:sensitive row)))
              :let [k (get row (id/key))]
              :when (and k (:sensitive (access/with-principal nil
                                         (dataset/get-entity :apps/variable {(id/key) k} {:sensitive nil}))))]
        (throw (ex-info "A sensitive App Variable stays sensitive; set a new value or delete it"
                        {:code "SENSITIVE_STAYS" :rule "apps_sensitive" :variable k})))))
  analysis)

(defn variable-ends
  "[variable tmp id, client tmp id] for each link the write makes between a variable and its App."
  [analysis variable-table]
  (for [[{:keys [relation] :as r} pairs] (concat (:relations/one analysis) (:relations/many analysis))
        :when (= relation (id/relation :apps/app->variables))
        [from to] pairs]
    (if (= variable-table (:from/table r)) [from to] [to from])))

(defn assert-variable-names!
  "Refuse a principal's write that leaves an App with two variables of one name."
  [analysis]
  (when-not (dataset/engine-write?)
    (let [model (dataset/deployed-model)
          table (some-> (core/get-entity model (id/entity :apps/variable)) naming/entity->table-name)
          client-table (some-> (core/get-entity model (id/entity :iam/app)) naming/entity->table-name)
          k (id/key)
          rows (get-in analysis [:entity table])]
      (when (seq rows)
        (let [linked (into {} (for [[v c] (variable-ends analysis table)
                                    :let [row (get-in analysis [:entity client-table c])
                                          cx (or (get row k)
                                                 (when-let [cid (:id row)]
                                                   (get (access/with-principal nil (dataset/get-entity :iam/app {:id cid} {k nil})) k)))]
                                    :when cx]
                                [v cx]))
              stored (fn [row]
                       (when-let [x (get row k)]
                         (access/with-principal nil
                           (dataset/get-entity :apps/variable {k x} {:name nil :app [{:selections {k nil}}]}))))
              written (for [[tmp row] rows
                            :let [old (stored row)
                                  app (or (linked tmp) (get-in old [:app k]))
                                  n (if (contains? row :name) (:name row) (:name old))]
                            :when (and app n)]
                        {:app app :name n :id (get row k)})]
          (doseq [[app ws] (group-by :app written)
                  :let [mine (set (keep :id ws))
                        others (remove #(mine (get % k))
                                       (:variables (access/with-principal nil
                                                     (dataset/get-entity :iam/app {k app} {:variables [{:selections {k nil :name nil}}]}))))
                        named (set (map :name ws))
                        twice (keep (fn [[n c]] (when (and (named n) (> c 1)) n)) (frequencies (concat (map :name ws) (keep :name others))))]
                  :when (seq twice)]
            (throw (ex-info (str "Variable " (str/join ", " twice) " is already set on this App; change that one or delete it")
                            {:code "VARIABLE_TWICE" :rule "apps_variable" :client app :names (vec twice)})))))))
  analysis)

(defmethod enhance/write [Object (id/entity :apps/variable :xid)] [_ _ analysis _]
  (-> analysis assert-sensitive-stays! assert-variable-names!))

(def volume-engine-columns #{:state :used_mb :last_error :delete_after})

(defn volume-ends
  "[volume tmp id, client tmp id] for each link the write makes between a volume and its App."
  [analysis volume-table]
  (for [[{:keys [relation] :as r} pairs] (concat (:relations/one analysis) (:relations/many analysis))
        :when (= relation (id/relation :apps/app->volumes))
        [from to] pairs]
    (if (= volume-table (:from/table r)) [from to] [to from])))

(defn assert-volume-write!
  "Refuse a principal's volume write that sets what the engine reports, or leaves an App's volumes over its limit, on a system path or two at one path."
  [analysis]
  (when-not (dataset/engine-write?)
    (let [model (dataset/deployed-model)
          table (some-> (core/get-entity model (id/entity :apps/volume)) naming/entity->table-name)
          client-table (some-> (core/get-entity model (id/entity :iam/app)) naming/entity->table-name)
          rows (get-in analysis [:entity table])]
      (when (seq rows)
        (doseq [[_ row] rows
                column (keys row)
                :when (volume-engine-columns column)]
          (throw (ex-info (str "`" (name column) "` of an App Volume is written by the engine; set Size MB and Mount Path")
                          {:code "ENGINE_OWNED" :rule "apps_volume" :attribute (name column)})))
        (let [linked (into {} (for [[v c] (volume-ends analysis table)
                                    :let [cx (get-in analysis [:entity client-table c :xid])]
                                    :when cx]
                                [v cx]))
              app-of (fn [tmp row]
                       (or (linked tmp)
                           (when-let [x (:xid row)]
                             (get-in (access/with-principal nil (dataset/get-entity :apps/volume {:xid x} {:app [{:selections {:xid nil}}]}))
                                     [:app :xid]))))
              by-app (group-by first (keep (fn [[tmp row]] (when-let [c (app-of tmp row)] [c row])) rows))]
          (doseq [[client written] by-app
                  :let [limit (:volume_limit_mb (:configuration (access/with-principal nil
                                                                  (dataset/get-entity :iam/app {:xid client}
                                                                                      {:configuration [{:selections {:volume_limit_mb nil}}]}))))
                        written (map second written)
                        current (into {} (map (juxt :xid identity)) (volumes/live (volumes/of-app client)))
                        merged (vals (reduce (fn [m row] (update m (or (:xid row) (gensym)) merge (select-keys row [:xid :size_mb :mount_path])))
                                             current written))]]
            (when-let [e (volumes/limit-error limit merged)]
              (throw (ex-info e {:code "VOLUME_REFUSED" :rule "apps_volume" :client client}))))))))
  analysis)

(defmethod enhance/write [Object (id/entity :apps/volume :xid)] [_ _ analysis _]
  (assert-volume-write! analysis))
