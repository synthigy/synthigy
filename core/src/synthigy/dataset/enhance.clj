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

(ns synthigy.dataset.enhance
  "Injects access-control conditions into query schemas before execution."
  (:require
   [synthigy.log :as log]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.rls :as rls]
   [synthigy.db :refer [*db*]]))

(defprotocol AuditEnhancement
  "Pluggable infrastructure-layer audit tracking (not part of the domain model)."

  (transform-audit [db tx entities]
    "Phase 1: adds audit columns via DDL, called after table creation.")

  (augment-schema [db entity]
    "Phase 2: returns {:fields ... :relations ...} to merge into the runtime schema.")

  (audit [db entity-id data tx]
    "Phase 3: populates audit values during the mutation pipeline."))

(defprotocol ModelEnhancement
  "Contributes attribute/relation additions to the runtime model. Composition order
   in dataset.runtime: identity attrs -> audit attrs -> reference relations."

  (enhance-model [this model]
    "Returns a new model with this enhancer's contribution applied."))

;; no-op fallback so anything not extending ModelEnhancement passes through the
;; composer
(extend-protocol ModelEnhancement
  Object
  (enhance-model [_ model] model)

  nil
  (enhance-model [_ model] model))

(defmulti schema
  "Enhances a query schema with access control conditions; dispatches on [(class
   db) entity-id]."
  (fn dispatch
    ([db _schema selection]
     [(class db) (:entity _schema)]))
  :default ::default)

(defmethod schema ::default
  [_ _schema _]
  _schema)

(defmulti args
  "Enhances query args with access control at any schema depth; dispatches on
   [(class db) entity-id]."
  (fn [db {entity-id :entity
           :as schema} [stack data]]
    [(class db) entity-id])
  :default ::default)

(defmethod args ::default
  [_ schema current-stack]
  (rls/enhance-args schema current-stack (access/rls-operation)))

(defn explain-enhancement
  "Diffs added/modified relations between original and enhanced schema, for
   audit trails."
  [original-schema enhanced-schema entity-id]
  (let [original-relations (set (keys (:relations original-schema)))
        enhanced-relations (set (keys (:relations enhanced-schema)))
        added-relations (clojure.set/difference enhanced-relations original-relations)
        modified-relations (filter #(not= (get-in original-schema [:relations % :args])
                                          (get-in enhanced-schema [:relations % :args]))
                                   original-relations)]
    {:entity entity-id
     :user (access/current-principal)
     :roles (access/role-ids)
     :group-eids (access/group-eids)
     :added-relations added-relations
     :modified-relations modified-relations
     :access-conditions (into {}
                              (for [rel (concat added-relations modified-relations)]
                                [rel (get-in enhanced-schema [:relations rel :args])]))}))

(defn log-enhancement
  "Logs enhancement details for monitoring and debugging."
  [schema entity-id enhanced-schema]
  (when (not= schema enhanced-schema)
    (log/debug {:id ::enhancement-applied
                :data {:entity entity-id
                       :user (access/current-principal)
                       :explanation (explain-enhancement schema enhanced-schema entity-id)}}
               "Access enhancement applied")))

(defn ensure-path
  "Builds a nested map/vector structure from a mixed key path (numeric keys =>
   vectors)."
  ([m path]
   (ensure-path m path {}))
  ([m path leaf-value]
   (if (empty? path)
     leaf-value
     (let [[k & ks] path
           next-k (first ks)]
       (cond
         (empty? ks)
         (assoc m k leaf-value)

         (number? k)
         (let [v (if (vector? m) m [])
               v' (if (< k (count v))
                    v
                    (into v (repeat (inc (- k (count v))) nil)))
               existing (get v' k)
               new-val (ensure-path existing ks leaf-value)]
           (assoc v' k new-val))

         (number? next-k)
         (assoc m k (ensure-path (get m k []) ks leaf-value))

         :else
         (assoc m k (ensure-path (get m k {}) ks leaf-value)))))))

(defmulti write
  "Checks whether the current user can perform the mutation; dispatches on
   [(class db) entity-id]."
  (fn [db entity-id data tx]
    [(class db) entity-id])
  :default ::default)

(defmethod write ::default
  [_ _ data _]
  data)

(defn apply-write
  "Applies write enhancement to mutation data."
  ([entity-id data tx] (apply-write *db* entity-id data tx))
  ([db entity-id data tx]
   (write db entity-id data tx)))

(defn apply-audit
  "Applies audit enhancement to mutation data."
  ([entity-id data tx] (apply-audit *db* entity-id data tx))
  ([db entity-id data tx]
   (audit db entity-id data tx)))

(defmulti delete
  "Checks whether the current user can perform the mutation; dispatches on
   [(class db) entity-id]."
  (fn [db entity-id data tx]
    [(class db) entity-id])
  :default ::default)

(defmethod delete ::default
  [_ _ data _]
  data)

(defn apply-delete
  "Applies delete enhancement to mutation data."
  ([entity-id data tx] (apply-delete *db* entity-id data tx))
  ([db entity-id data tx]
   (delete db entity-id data tx)))

(defn apply-schema
  "Entry point for schema enhancement — no-op when there is no current
   principal."
  [_schema selection]
  (if (access/current-principal)
    (let [enhanced (schema *db* _schema selection)]
      (log-enhancement _schema (:entity _schema) enhanced)
      enhanced)
    _schema))


