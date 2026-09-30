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

(ns synthigy.audit.policy
  "Entity/relation opt-in policy and the envelope-to-row flattening every audit provider shares."
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [patcho.lifecycle :as lifecycle]
            [synthigy.dataset.classification :as classification]
            [synthigy.json :as json]))

(defonce audit-policy
  (atom {:audit-all? false :entities #{} :relations #{} :redact {}}))

(defn audit-all-env?
  []
  (when-let [v (env :synthigy-audit-all)]
    (contains? #{"true" "1" "yes"} (str/lower-case (str v)))))

(defn audit-policy-snapshot
  "Return the currently-compiled audit policy."
  []
  @audit-policy)

(defn audited-entity?
  "True iff envelopes for the given entity-xid should be persisted."
  [entity-xid]
  (let [p @audit-policy]
    (boolean (or (:audit-all? p)
                 (contains? (:entities p) entity-xid)))))

(defn audited-relation?
  "True iff envelopes for the given relation-xid should be persisted."
  [relation-xid]
  (let [p @audit-policy]
    (boolean (or (:audit-all? p)
                 (contains? (:relations p) relation-xid)))))

(defn attribute-pairs
  "Changed attributes of an insert/update as [attribute-xid json-value] pairs, diffed raw and redacted after."
  [op before after]
  (let [redact (:redact @audit-policy)
        pair   (fn [[k v]] [(name k) (json/->json (or (get redact (name k)) v))])]
    (case op
      "insert" (mapv pair after)
      "update" (into [] (comp (remove (fn [[k v]] (= v (get before k)))) (map pair)) after)
      [])))

(defn str-or-nil [v] (when (some? v) (str v)))

(defn entity-rows
  "Flatten entity envelopes to [ts tenant record entity attribute value op actor request scope txid seq] rows; `ts-fn` converts the envelope ts."
  [envelopes ts-fn]
  (into []
        (mapcat
         (fn [env]
           (let [data (-> env :delta :data)]
             (when-let [record-xid (str-or-nil (:record-xid data))]
               (let [op   (-> env :delta :type name)
                     base [(ts-fn (:ts data)) (str-or-nil (:tenant data)) record-xid
                           (str-or-nil (:entity-xid data))]
                     tail [op (str-or-nil (:actor data)) (str-or-nil (:request data))
                           (str-or-nil (:scope data)) (:txid data) (:seq env)]]
                 (if (= op "delete")
                   [(-> base (conj "__delete__" "null") (into tail))]
                   (map (fn [[attr value]] (-> base (conj attr value) (into tail)))
                        (attribute-pairs op (:before data) (:after data)))))))))
        envelopes))

(defn relation-rows
  "Flatten relation envelopes to [ts tenant relation from to op actor request scope txid seq] rows."
  [envelopes ts-fn]
  (into []
        (keep (fn [env]
                (let [data     (-> env :delta :data)
                      from-xid (str-or-nil (:from-xid data))
                      to-xid   (str-or-nil (:to-xid data))]
                  (when (and from-xid to-xid)
                    [(ts-fn (:ts data)) (str-or-nil (:tenant data)) (str-or-nil (:element env))
                     from-xid to-xid (-> env :delta :type name) (str-or-nil (:actor data))
                     (str-or-nil (:request data)) (str-or-nil (:scope data)) (:txid data) (:seq env)]))))
        envelopes))

(defn persistence-enabled?
  "Runtime gate for audit persistence: true when `:synthigy/iam` is started."
  []
  (boolean (lifecycle/started? :synthigy/iam)))

;; Must move together with synthigy.dataset.core/audit-persist? — inlined so
;; this ns stays a leaf.
(defn persist?
  [entity]
  (boolean (get-in entity [:configuration :audit/persist])))

(defn coerce-coll
  [coll]
  (cond
    (nil? coll)        nil
    (sequential? coll) coll
    (map? coll)        (vals coll)
    :else              (seq coll)))

(defn recompile-policy!
  "Recompute the opt-in entity/relation xid sets and redaction map from `model`
   and swap the policy atom."
  [model]
  (let [entities  (coerce-coll (some-> model :entities))
        relations (coerce-coll (some-> model :relations))
        audit-entities    (filter persist? entities)
        audit-entity-xids (into #{} (keep :xid) audit-entities)
        audit-relation-xids
        (into #{}
              (comp (filter (fn [rel]
                              (or (contains? audit-entity-xids (:from rel))
                                  (contains? audit-entity-xids (:to rel)))))
                    (keep :xid))
              relations)
        redact (into {}
                     (for [entity entities
                           attr   (:attributes entity)
                           :when  (and (:xid attr) (classification/redact? attr))]
                       [(str (:xid attr)) (classification/redaction-marker attr)]))]
    (reset! audit-policy
            {:audit-all? (boolean (audit-all-env?))
             :entities  audit-entity-xids
             :relations audit-relation-xids
             :redact    redact})))
