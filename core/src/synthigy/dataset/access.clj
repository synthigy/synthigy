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

(ns synthigy.dataset.access
  "Access control convenience layer over synthigy.dataset.access.protocol."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.access.protocol :as p]
   [synthigy.dataset.core :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.key :as dk]
   [synthigy.dataset.sql.naming :as naming]))

;;; ============================================================================
;;; Default Implementation (Allow All)
;;; ============================================================================

(defrecord AllowAllAccess []
  p/AccessControl
  (entity-allows?    [_ _ _] true)
  (relation-allows?  [_ _ _] true)
  (relation-allows?  [_ _ _ _] true)
  (attribute-allows? [_ _ _ _] true)
  (scope-allowed?   [_ _] true)
  (roles-allowed?   [_ _] true)
  (superuser?       [_] true)
  (get-principal    [_] nil)
  (principal-eid    [_] nil)
  (role-ids         [_] #{})
  (role-eids        [_] #{})
  (group-eids       [_] #{}))

;;; ============================================================================
;;; Dynamic Context
;;; ============================================================================

(defonce ^:dynamic *access-control* (->AllowAllAccess))

(def ^:dynamic *operation-rules*
  "RBAC rules that satisfy the current operation — any one of them suffices."
  #{:read :owns})

(defn rls-operation
  "RLS operation implied by *operation-rules*."
  []
  (cond
    (some *operation-rules* [:create :update :write]) :write
    (some *operation-rules* [:delete :purge]) :delete
    :else :read))

;;; ============================================================================
;;; Convenience Functions
;;; ============================================================================

(defn entity-allows? [entity-id operations]
  (p/entity-allows? *access-control* entity-id operations))

(defn relation-allows?
  ([relation-id operations]
   (p/relation-allows? *access-control* relation-id operations))
  ([relation-id from-to operations]
   (p/relation-allows? *access-control* relation-id from-to operations)))

(defn attribute-allows? [entity-id attribute-id op]
  (p/attribute-allows? *access-control* entity-id attribute-id op))

(defn scope-allowed? [scope]
  (p/scope-allowed? *access-control* scope))

(defn roles-allowed? [role-ids]
  (p/roles-allowed? *access-control* role-ids))

(defn superuser? []
  (p/superuser? *access-control*))

(defn current-principal []
  (p/get-principal *access-control*))

(defn principal-eid []
  (p/principal-eid *access-control*))

(defn role-ids
  "Set of role id-keys (xids) for the current principal. RBAC."
  []
  (p/role-ids *access-control*))

(defn role-eids
  "Set of role :_eid values for the current principal. RLS."
  []
  (p/role-eids *access-control*))

(defn group-eids
  "Set of group :_eid values for the current principal. RLS."
  []
  (p/group-eids *access-control*))

(defn rls-bypass?
  "True when the principal's grants bypass row-scope for entity-id + operation;
   impls without p/RLSBypass never bypass."
  [entity-id operation]
  (let [ac *access-control*]
    (and (satisfies? p/RLSBypass ac)
         (boolean (p/rls-bypass? ac entity-id operation)))))

;;; ============================================================================
;;; Model Protection
;;; ============================================================================

(defn protect-model
  "Removes entities and relations the current principal cannot access from the
   model."
  [model]
  (when model
    (as-> model m
      (reduce
        (fn [m entity]
          (if (entity-allows? (id/extract entity) #{:read :create :update})
            m
            (dataset/remove-entity m entity)))
        m
        (dataset/get-entities m))
      (reduce
        (fn [m {from :from to :to :as relation}]
          (let [from-id (id/extract from)
                to-id (id/extract to)
                relation-id (id/extract relation)]
            (if (or
                  (relation-allows? relation-id [from-id to-id] #{:read :write})
                  (relation-allows? relation-id [to-id from-id] #{:read :write}))
              m
              (dataset/remove-relation m relation))))
        m
        (dataset/get-relations m)))))

;;; ============================================================================
;;; Client Schema Projection
;;; ============================================================================

;; never word-split here (csk) — must equal naming/normalize-name or /schema keys stop resolving
(defn ->snake-str [^String n]
  (when n (naming/normalize-name (str/trim n))))

(defn ->kebab-str [^String n]
  (some-> n str/trim dk/label->skins :kebab name))

(defn ->camel-str [^String n]
  (some-> n str/trim dk/label->skins :camel name))

(def ^:dynamic *name-fn* ->snake-str)

(def name-fn-for
  {"snake" ->snake-str :snake ->snake-str
   "kebab" ->kebab-str :kebab ->kebab-str
   "camel" ->camel-str :camel ->camel-str})

(defn collapse-cardinality [c]
  (case c
    ("o2m" "m2m") "many"
    ("o2o" "m2o") "one"
    "many"))

(defn attribute->client
  "Projects an active attribute to its client-schema [name facts] pair."
  [{:keys [name type constraint configuration] :as attribute}]
  (when (:active attribute)
    [(*name-fn* name)
     (cond-> {:type (or type "string") :skins (dk/label->skins name)}
       (dataset/mandatory-constraint? constraint) (assoc :nullable false)
       (= "enum" type)   (assoc :enum (mapv :name (:values configuration)))
       (and (= "enum" type)
            (some (complement :active) (:values configuration)))
       (assoc :retired (into [] (comp (remove :active) (map :name)) (:values configuration)))
       (= "hashed" type) (assoc :write-only true))]))

(defn attribute->xid-pair [attribute]
  (when (and (:active attribute) (:xid attribute))
    [(*name-fn* (:name attribute)) (str (:xid attribute))]))

(defn entity-unique-constraints->client [entity]
  (let [id->name (into {} (map (juxt id/extract #(*name-fn* (:name %)))) (:attributes entity))
        id-key-name (name (id/key))
        raw (get-in entity [:configuration :constraints :unique])
        named (reduce
                (fn [acc group]
                  (let [ng (keep id->name group)]
                    (if (seq ng) (conj acc (vec ng)) acc)))
                []
                raw)]
    (if (some #(= % [id-key-name]) named)
      named
      (into [[id-key-name]] named))))

(defn entity->client
  "Projects an ERD entity and its focused relations onto the client-schema
   shape."
  [model entity]
  (let [active-attrs (:attributes entity)
        attrs        (into {} (keep attribute->client) active-attrs)
        attr-xids    (into {} (keep attribute->xid-pair) active-attrs)
        rel-rows  (keep (fn [{:keys [xid to to-label cardinality active]}]
                          (when (and active (seq to-label))
                            (let [label (*name-fn* to-label)]
                              [label
                               {:to (*name-fn* (:name to))
                                :cardinality (collapse-cardinality cardinality)
                                :skins (dk/label->skins to-label)}
                               (when xid (str xid))])))
                        (dataset/focus-entity-relations model entity))
        rels      (into {} (map (fn [[label v _]] [label v])) rel-rows)
        rel-xids  (into {} (keep (fn [[label _ xid]] (when xid [label xid]))) rel-rows)]
    {:name (:name entity)
     :xid  (when-let [x (:xid entity)] (str x))
     :skins (dk/label->skins (:name entity))
     :attributes attrs
     :relations rels
     :constraints {:unique (entity-unique-constraints->client entity)}
     :xids {:attributes attr-xids
            :relations rel-xids}}))

(defn schema
  "Thin client projection of an already-IAM-filtered model, optionally
   restricted to entity-names."
  ([model] (schema model nil))
  ([model entity-names]
   (let [allowed  (dataset/get-entities model)
         wanted   (when (seq entity-names) (set (map #(->snake-str (str %)) entity-names)))
         filtered (if wanted
                    (filter #(contains? wanted (->snake-str (:name %))) allowed)
                    allowed)]
     {:id-key (name (id/key))
      :entities (into {}
                      (map (fn [e]
                             [(*name-fn* (:name e))
                              (entity->client model e)]))
                      filtered)})))
