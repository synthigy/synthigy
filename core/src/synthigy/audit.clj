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

(ns synthigy.audit
  "AuditProvider protocol and the entity/relation opt-in policy gating what
   gets persisted to `/history`."
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [patcho.lifecycle :as lifecycle]
            [synthigy.dataset.classification :as classification]
            [synthigy.log :as log]))

(defonce ^:dynamic *audit-provider* nil)

(defonce ^:private warned-missing-provider (atom false))

(defn warn-missing-provider!
  "Warn once when audit deltas are drained with no provider bound while
  `:synthigy/observability` is registered but not started."
  []
  (when (and (some? (lifecycle/module-info :synthigy/observability))
             (not (lifecycle/started? :synthigy/observability))
             (compare-and-set! warned-missing-provider false true))
    (log/warn {:id ::no-audit-provider
               :data {:action :writing :subject :iam-audit}}
              (str "Draining audit deltas with no :audit-provider bound while "
                   ":synthigy/observability is registered but not started — "
                   "audit-log writes are being DROPPED. Start :synthigy/observability "
                   "to persist them."))))

;; Bound by the drainer to its open tx; providers must use it (when bound) so
;; queue DELETE + audit INSERTs stay atomic on single-writer backends.
(def ^:dynamic *audit-tx* nil)

(defprotocol AuditProvider
  "Audit plug contract: batch writers called synchronously by the
  drainer, readers serving the `/history` endpoint."

  (write-entity-deltas! [this envelopes]
    "Write a drainer batch of entity-track envelopes durably.")

  (write-relation-deltas! [this envelopes]
    "Write a drainer batch of relation-track envelopes durably.")

  (get-at [this opts]
    "Return a record's state as of timestamp T.")

  (events [this opts]
    "Return events for a record (or any record) over a time range.")

  (diff [this opts]
    "Diff a record's state between two timestamps.")

  (timeline [this opts]
    "Return events grouped by :request, :actor, or :scope.")

  (since [this opts]
    "Return events strictly after a cursor timestamp, oldest-first."))

(defonce ^:private audit-policy
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

(defn redact-attributes
  "Replace values of classified attributes with their redaction marker, keeping
   the keys."
  [attrs]
  (let [redact (:redact @audit-policy)]
    (if (or (empty? redact) (empty? attrs))
      attrs
      (reduce-kv (fn [m k v]
                   (assoc m k (or (get redact (name k)) v)))
                 {}
                 attrs))))

(defn redact-envelope
  "Apply `redact-attributes` to an envelope's `:before`/`:after` — persistence
   path only, never live dispatch."
  [env]
  (let [data (-> env :delta :data)]
    (if (or (contains? data :before) (contains? data :after))
      (update-in env [:delta :data]
                 (fn [d]
                   (cond-> d
                     (contains? d :before) (update :before redact-attributes)
                     (contains? d :after)  (update :after redact-attributes))))
      env)))

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
