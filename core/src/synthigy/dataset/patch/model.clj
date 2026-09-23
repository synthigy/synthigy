;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.dataset.patch.model
  "EUUID<->XID model transformation and DB-modifying patch helpers for
   upgrading/downgrading stored models."
  (:require
    [synthigy.log :as log]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming :as naming]
    [synthigy.db.sql :as sql]
    [synthigy.transit :refer [<-transit ->transit]]))

(defn ->iso
  "Best-effort ISO-8601 string for a deployed_on value across backends. Isolated
   so a coercion hiccup never nulls the surrounding version info."
  [v]
  (try
    (if (instance? java.util.Date v)
      (str (.toInstant ^java.util.Date v))
      (str v))
    (catch Throwable _ (str v))))

(def versions-relation-table
  (memoize
    (fn [id-key]
      (naming/relation->table-name-for-id
        {:euuid (id/relation-id-for-key :dataset/dataset->versions :euuid)
         :xid   (id/relation-id-for-key :dataset/dataset->versions :xid)
         :from  {:name "Dataset"}
         :to    {:name "Dataset Version"}}
        id-key))))

(defn deployed-versions
  "Drift stamp — {dataset-id {:version :deployed-at}} for every dataset's latest
   deployed version. Cheap: reads the meta tables directly, no model rebuild."
  []
  (try
    (let [id-col (clojure.core/name (id/key))
          rows   (sql/execute!
                   [(str "SELECT d." id-col " AS dataset, v.name AS version, v.deployed_on
                          FROM dataset_version v
                          JOIN \"" (versions-relation-table (id/key)) "\" j
                            ON j.dataset_version_id = v._eid
                          JOIN dataset d ON d._eid = j.dataset_id
                          WHERE v.deployed = true")])]
      (reduce-kv
        (fn [r dataset versions]
          (let [latest (last (sort-by (comp ->iso :deployed_on) versions))]
            (assoc r (str dataset)
                   {:version     (:version latest)
                    :deployed-at (some-> (:deployed_on latest) ->iso)})))
        {}
        (group-by :dataset rows)))
    (catch Throwable _ nil)))


(defn transform-model
  "Converts all entities/clones/relations/attributes/enum-values/RLS-conditions in a
   model between id formats, via deterministic id/uuid->nanoid."
  ([model ->to]
   (transform-model model (id/key) ->to))
  ([original-model ->from ->to]
   (if (= ->from ->to)
     original-model
     (letfn [(->new-id
               [data]
               ((case ->to
                  :xid (comp id/uuid->nanoid :euuid)
                  :euuid (comp id/nanoid->uuid :xid))
                data))
             (->new-bare-id
               ;; translates a raw id (no surrounding record); pass-through if
               ;; already target form or nil
               [id]
               (cond
                 (nil? id) id
                 (= ->to :xid) (if (uuid? id) (id/uuid->nanoid id) id)
                 (= ->to :euuid) (if (string? id) (id/nanoid->uuid id) id)))
             (transform-rls-condition
               ;; bare ids, no surrounding record — translate field-by-field;
               ;; also the
               ;; forward-migration seam for legacy
               ;; :relation-euuid/:entity-euuid step keys
               [condition]
               (cond-> condition
                 (contains? condition :attribute)
                 (update :attribute ->new-bare-id)

                 (contains? condition :steps)
                 (update :steps
                         (fn [steps]
                           (mapv (fn [step]
                                   (-> step
                                       (assoc :relation-id
                                              (->new-bare-id (or (:relation-id step)
                                                                 (:relation-euuid step)))
                                              :entity-id
                                              (->new-bare-id (or (:entity-id step)
                                                                 (:entity-euuid step))))
                                       (dissoc :relation-euuid :entity-euuid)))
                                 steps)))))
             (transform-rls-guards
               [entity]
               (if (seq (get-in entity [:configuration :rls :guards]))
                 (update-in entity [:configuration :rls :guards]
                            (fn [guards]
                              (mapv (fn [guard]
                                      (update guard :conditions
                                              #(mapv transform-rls-condition %)))
                                    guards)))
                 entity))
             (get-entity [{:keys [entities clones]} id]
               (if-let [e (get entities id)]
                 e
                 (when-some [{:keys [entity position]} (get clones id)]
                   (when-some [entity (get entities entity)]
                     (assoc entity
                       ->from id
                       :position position
                       :clone true
                       :original (id/extract entity))))))]

       (as-> [nil original-model] data
         (reduce
           (fn [[mapping model] entity]
             (letfn [(find-attribute
                       [id]
                       (some
                         (fn [attribute]
                           (when (= (->from attribute) id)
                             attribute))
                         (:attributes entity)))]
               (let [id (->new-id entity)
                     old-id (->from entity)
                     new-entity (->
                                  entity
                                  (assoc ->to id)
                                  (update-in [:configuration :constraints :unique]
                                             (fn [uniques]
                                               (mapv
                                                 (fn [constraints]
                                                   (mapv (comp ->new-id find-attribute) constraints))
                                                 uniques)))
                                  transform-rls-guards
                                  (update :attributes
                                          (fn [as]
                                            (mapv
                                              (fn [{:keys [type]
                                                    :as attribute}]
                                                (let [new-attribute (assoc attribute ->to (->new-id attribute))]
                                                  (case type
                                                    "enum" (update-in new-attribute [:configuration :values]
                                                                      (fn [values]
                                                                        (mapv
                                                                          (fn [value]
                                                                            (assoc value ->to (->new-id value)))
                                                                          values)))
                                                    new-attribute)))
                                              as))))]
                 (when (nil? old-id)
                   (throw
                     (ex-info "Couldn't get entity original id!"
                              {:entity entity
                               :->from ->from
                               :->to ->to})))
                 [(assoc mapping old-id id)
                  (->
                    model
                    (assoc-in [:entities id] new-entity)
                    (update :entities dissoc old-id))])))
           data
           (core/get-entities (second data)))
         (reduce-kv
           (fn [[mapping model] old-id {original-entity-id :entity
                                        :as old-value}]
             (let [original-entity (get-in original-model [:entities original-entity-id])
                   clone-entity (assoc original-entity ->from old-id)
                   clone-new-id (->new-id clone-entity)
                   new-original-entity-id (->new-id original-entity)]
               [(assoc mapping old-id clone-new-id)
                (assoc-in model [:clones clone-new-id] (assoc old-value :entity new-original-entity-id))]))
           (assoc-in data [1 :clones] nil)
           (get-in data [1 :clones]))
         (reduce-kv
           (fn [[mapping model] old-relation-id {:keys [from to]
                                                 :as old-relation}]
             (let [from-entity (get-entity original-model from)
                   to-entity (get-entity original-model to)
                   new-relation-id (->new-id old-relation)
                   new-from-id (->new-id from-entity)
                   new-to-id (->new-id to-entity)
                   new-relation (->
                                  old-relation
                                  (assoc
                                    ->to new-relation-id
                                    :from new-from-id
                                    :to new-to-id))]
               (when (some nil? [old-relation-id new-from-id new-to-id])
                 (throw
                   (ex-info "Couldn't get relation original id!"
                            {:relation old-relation
                             :->from ->from
                             :->to ->to})))
               [(assoc mapping old-relation-id new-relation-id)
                (->
                  model
                  (update :relations dissoc old-relation-id)
                  (assoc-in [:relations new-relation-id] new-relation))]))
           data
           (:relations original-model))
         (second data))))))

;; WARNING: functions below MODIFY THE DATABASE — call only from patcho patch
;; definitions

(comment
  (def version-id #uuid "d908a70f-a1fb-46bd-ac76-801bebe6ceed")
  (def version-id #uuid "8996515d-3447-4ac1-8f36-2c874967913b")
  (def direction :xid)
  (def version-record
    (sql/execute-one!
      ["SELECT euuid, name, model
                           FROM dataset_version
                           WHERE euuid = ?"
       version-id]))
  (def model
    (when-let [m (:model version-record)]
      (<-transit m))))

(defn transform-stored-model!
  "Transforms a single stored model and saves it back to the database. MODIFIES
   THE DATABASE."
  [version-id direction]
  (log/info {:id ::transforming-model
             :data {:version-id version-id :direction direction}}
            "Transforming model")
  (let [version-record (sql/execute-one!
                         ["SELECT euuid, name, model
                           FROM dataset_version
                           WHERE euuid = ?"
                          version-id])
        model (when (:model version-record)
                (<-transit (:model version-record)))]
    (if-not model
      (do
        (log/warn {:id ::model-not-found
                   :data {:version-id version-id}}
                  "No model found for version")
        {:success false
         :error "No model found"})
      (let [transformed (transform-model model direction)
            transit-data (->transit transformed)]
        (sql/execute!
          ["UPDATE dataset_version SET model = ? WHERE euuid = ?"
           transit-data version-id])
        (log/info {:id ::model-transformed
                   :data {:version-name (:name version-record)
                          :entity-count (count (:entities transformed))
                          :direction direction}}
                  "Transformed model")
        {:success true
         :version-name (:name version-record)
         :entity-count (count (:entities transformed))
         :direction direction}))))

(defn transform-stored-models!
  "Transforms ALL deployed models, oldest-first to preserve rebuild-logic
   consistency. MODIFIES THE DATABASE."
  [direction]
  (log/info {:id ::transforming-all-models
             :data {:direction direction}}
            "Transforming all stored models")
  (let [versions (sql/execute!
                   ["SELECT euuid, name, deployed_on
                     FROM dataset_version
                     WHERE deployed = true
                     ORDER BY deployed_on ASC NULLS LAST"])
        results (atom [])]
    (doseq [{:keys [euuid name]} versions]
      (try
        (let [result (transform-stored-model! euuid direction)]
          (swap! results conj result))
        (catch Exception e
          (log/error! {:id ::transform-model-failed
                       :msg "Failed to transform stored model"
                       :data {:version-name name}}
                      e)
          (swap! results conj
                 {:success false
                  :version-name name
                  :error (.getMessage e)}))))
    (let [successes (filter :success @results)
          failures (remove :success @results)]
      (log/info {:id ::transform-all-complete
                 :data {:total (count @results)
                        :successful (count successes)
                        :failed (count failures)}}
                "Bulk model transformation complete")
      (when (seq failures)
        (log/warn {:id ::transform-failures
                   :data {:failures (mapv :version-name failures)}}
                  "Some models failed to transform"))
      {:total (count @results)
       :success-count (count successes)
       :failures failures
       :results @results})))
