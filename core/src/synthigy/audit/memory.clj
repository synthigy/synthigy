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

(ns synthigy.audit.memory
  "Bounded in-memory audit provider: the newest rows per track, for embedded use, demos and tests."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [synthigy.audit.policy :as policy]
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.timestamp :as timestamp])
  (:import
   [java.time Instant]))

(def default-size 10000)

(defn env-size []
  (or (try (some-> (env :synthigy-audit-ring-size) str/trim Long/parseLong) (catch Throwable _ nil))
      default-size))

(defn ->instant ^Instant [x]
  (when (some? x) (timestamp/->instant x)))

(defn entity-event [[ts tenant record entity attribute value op actor request scope txid sq]]
  {:track :entity :seq sq :ts ts :tenant-xid tenant :record-xid record :entity-xid entity
   :attribute-xid attribute :value (json/read-str value) :op op :actor actor :request request
   :scope scope :txid (policy/str-or-nil txid)})

(defn relation-event [[ts tenant relation from to op actor request scope txid sq]]
  {:track :relation :seq sq :ts ts :tenant-xid tenant :relation-xid relation :from-xid from
   :to-xid to :op op :actor actor :request request :scope scope :txid (policy/str-or-nil txid)})

(defn keep-newest [rows size]
  (let [n (count rows)]
    (if (> n size) (subvec rows (- n size)) rows)))

(defn append! [state track rows size]
  (when (seq rows)
    (swap! state update track #(keep-newest (into % rows) size))))

(defn matches? [track {:keys [record-xid between cursor seq tenant]}]
  (let [[t1 t2] (map ->instant between)
        cursor  (->instant cursor)]
    (fn [{:keys [^Instant ts] :as row}]
      (and (or (nil? record-xid)
               (if (= track :relation)
                 (or (= record-xid (:from-xid row)) (= record-xid (:to-xid row)))
                 (= record-xid (:record-xid row))))
           (or (nil? t1) (not (.isBefore ts t1)))
           (or (nil? t2) (not (.isAfter ts t2)))
           (or (nil? cursor) (.isAfter ts cursor))
           (or (nil? seq) (> (:seq row) seq))
           (or (nil? tenant) (= (str tenant) (:tenant-xid row)))))))

(def newest-first #(compare [(:ts %2) (:seq %2)] [(:ts %1) (:seq %1)]))
(def oldest-first #(compare [(:ts %1) (:seq %1)] [(:ts %2) (:seq %2)]))
(def by-seq #(compare (:seq %1) (:seq %2)))

(defn select-events [state track opts order]
  (cond->> (->> (get @state track) (filter (matches? track opts)) (sort order) (map #(update % :ts str)))
    (:limit opts) (take (:limit opts))
    true          vec))

(defn state-at [state record-xid at tenant include-deleted?]
  (let [at      (->instant at)
        latest  (->> (:entity @state)
                     (filter #(and (= record-xid (:record-xid %))
                                   (not (.isAfter ^Instant (:ts %) at))
                                   (or (nil? tenant) (= (str tenant) (:tenant-xid %)))))
                     (sort oldest-first)
                     (reduce #(assoc %1 (:attribute-xid %2) (:value %2)) {}))
        deleted? (contains? latest "__delete__")
        attrs    (into {} (map (fn [[a v]] [(keyword a) v])) (dissoc latest "__delete__"))]
    (if (and deleted? (not include-deleted?)) nil (not-empty attrs))))

(defn remove-record! [state record-xid tenant]
  (let [n     (atom nil)
        owned #(or (nil? tenant) (= (str tenant) (:tenant-xid %)))
        hit-e #(and (= record-xid (:record-xid %)) (owned %))
        hit-r #(and (or (= record-xid (:from-xid %)) (= record-xid (:to-xid %))) (owned %))]
    (swap! state (fn [{:keys [entity relation] :as s}]
                   (reset! n {:entity (count (filter hit-e entity)) :relation (count (filter hit-r relation))})
                   (assoc s
                          :entity (into [] (remove hit-e) entity)
                          :relation (into [] (remove hit-r) relation))))
    @n))

(defrecord MemoryAudit [state size]
  audit/AuditProvider
  (write-entity-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (append! state :entity (mapv entity-event (policy/entity-rows envelopes ->instant)) size)))

  (write-relation-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (append! state :relation (mapv relation-event (policy/relation-rows envelopes ->instant)) size)))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (state-at state record-xid at tenant (boolean include-deleted?)))

  (events [_ {:keys [track] :or {track :entity} :as opts}]
    (select-events state track opts newest-first))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (let [before (or (state-at state record-xid from-ts tenant true) {})
          after  (or (state-at state record-xid to-ts tenant true) {})
          ks     (into #{} (concat (keys before) (keys after)))]
      {:before  before
       :after   after
       :changed (vec (sort (filter #(not= (get before %) (get after %)) ks)))}))

  (timeline [_ {:keys [group-by] :or {group-by :request} :as opts}]
    (clojure.core/group-by (case group-by :actor :actor :scope :scope :request)
                           (select-events state :entity opts newest-first)))

  (since [_ {:keys [track] sq :seq :or {track :entity} :as opts}]
    (select-events state track opts (if sq by-seq oldest-first)))

  (erase! [_ {:keys [record-xid tenant]}]
    (remove-record! state (str record-xid) tenant)))

(defn create-provider
  ([] (create-provider nil))
  ([{:keys [size]}]
   (->MemoryAudit (atom {:entity [] :relation []}) (long (or size (env-size))))))

(def model-watch-key ::audit-policy-watch)

(defn start []
  (alter-var-root #'audit/*audit-provider* (constantly (create-provider)))
  (policy/recompile-policy! (dataset/deployed-model))
  (dataset/add-model-watch! model-watch-key (fn [_ _ _ new-model] (policy/recompile-policy! new-model)))
  (log/info {:id ::started
             :data {:action :started :subject :audit-provider :backend :memory :size (env-size)}}
            "In-memory audit provider started"))

(defn stop []
  (dataset/remove-model-watch! model-watch-key)
  (policy/recompile-policy! nil)
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (log/info {:id ::stopped :data {:action :stopped :subject :audit-provider :backend :memory}}
            "In-memory audit provider stopped"))
