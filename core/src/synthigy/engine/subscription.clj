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

(ns synthigy.engine.subscription
  "Delta → wire-item translation shared by both subscription transports (WS/SSE
   and in-process). Pure translation: no transport, no auth, no I/O."
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.query :as sql-query]))

(def ^:private relation-separator-re
  #"\s*(?:->|\.|\s-\s)\s*|-")

(defn split-relation-name
  "Split a wire-supplied relation reference into [entity-part label-part], or
   nil."
  [s]
  (let [s (str/trim s)
        parts (str/split s relation-separator-re 2)]
    (when (= 2 (count parts))
      (mapv str/trim parts))))

(def ^:private allowed-entity-fields
  #{"type" "entities" "operations"
    :type :entities :operations})

(def ^:private allowed-relation-fields
  #{"type" "relations" "operations"
    :type :relations :operations})

(defn parse-operations
  "Parse the optional operations field on an entity/relation item into fine-op
   keywords, or nil when absent."
  [item]
  (when-let [ops (or (get item "operations") (get item :operations))]
    (when-not (sequential? ops)
      (throw (ex-info "operations must be an array"
                      {:code "INVALID_OPERATIONS"})))
    (set (mapcat (fn [op]
                   (case op
                     "change" [:insert :update]
                     "delete" [:delete]
                     "insert" [:insert]
                     "update" [:update]
                     "link"   [:link]
                     "unlink" [:unlink]
                     [(keyword op)]))
                 ops))))

;; normalize-entity-item / normalize-relation-item / translate-delta are
;; ^:no-doc PUBLIC
;; (not private) because embedded.subscription shares them — see
;; docs/core/synthigy/engine/subscription.md.

(defn ^:no-doc normalize-entity-item
  "Validate a {:type \"entity\" ...} item; resolve names to entity-xids,
   RBAC-checking read on each."
  [item]
  (let [unknown-keys (set/difference (set (keys item)) allowed-entity-fields)]
    (when (seq unknown-keys)
      (throw (ex-info (str "Unknown fields on entity subscription item: "
                           (pr-str unknown-keys))
                      {:code "INVALID_SUBSCRIPTION"
                       :unknown-fields unknown-keys}))))
  (let [entities-spec (or (get item "entities") (get item :entities))]
    (when (or (nil? entities-spec) (not (sequential? entities-spec)))
      (throw (ex-info "Missing required field `entities` on entity subscription item"
                      {:code "MISSING_ENTITIES"})))
    (let [names (into [] (filter string?) entities-spec)]
      (when (empty? names)
        (throw (ex-info "`entities` must be a non-empty array of entity name strings"
                        {:code "EMPTY_ENTITIES"})))
      (let [name-by-xid
            (reduce (fn [acc n]
                      (let [eid (sql-query/resolve-entity n)]
                        (when-not (daccess/entity-allows? eid #{:read})
                          (throw (ex-info (str "No read access to entity: " n)
                                          {:code "ENTITY_NOT_READABLE"
                                           :entity n})))
                        (if (contains? acc eid)
                          acc                ; first-write-wins on duplicates
                          (assoc acc eid n))))
                    {}
                    names)]
        {:name-by-xid name-by-xid
         :ops         (parse-operations item)}))))

(defn ^:no-doc normalize-relation-item
  "Validate a {:type \"relation\" ...} item; references are entity<sep>label
   (see relation-separator-re), RBAC-checked for read."
  [item]
  (let [unknown-keys (set/difference (set (keys item)) allowed-relation-fields)]
    (when (seq unknown-keys)
      (throw (ex-info (str "Unknown fields on relation subscription item: "
                           (pr-str unknown-keys))
                      {:code "INVALID_SUBSCRIPTION"
                       :unknown-fields unknown-keys}))))
  (let [relations-spec (or (get item "relations") (get item :relations))]
    (when (or (nil? relations-spec) (not (sequential? relations-spec)))
      (throw (ex-info "Missing required field `relations` on relation subscription item"
                      {:code "MISSING_RELATIONS"})))
    (let [names (into [] (filter string?) relations-spec)]
      (when (empty? names)
        (throw (ex-info "`relations` must be a non-empty array of entity/label strings"
                        {:code "EMPTY_RELATIONS"})))
      (let [name-by-xid
            (reduce (fn [acc n]
                      (let [[entity-part label] (split-relation-name n)]
                        (when (or (str/blank? entity-part)
                                  (str/blank? label))
                          (throw (ex-info
                                  (str "Relation must be `entity<sep>label` "
                                       "where <sep> is `.`, `->`, ` - `, or `-`: " n)
                                  {:code "INVALID_RELATION_NAME"
                                   :relation n})))
                        (let [rel-id (sql-query/resolve-relation entity-part label)]
                          (when-not (daccess/relation-allows? rel-id #{:read})
                            (throw (ex-info (str "No read access to relation: " n)
                                            {:code "RELATION_NOT_READABLE"
                                             :relation n})))
                          (if (contains? acc rel-id)
                            acc              ; first-write-wins on duplicates
                            (assoc acc rel-id n)))))
                    {}
                    names)]
        {:name-by-xid name-by-xid
         :ops         (parse-operations item)}))))

(defn denied-attribute-xids
  "Attribute-xid keywords denied to roles on entity; walks every attribute since
   deltas carry the full record, not a client selection."
  [entity roles]
  (when entity
    (into #{}
          (comp (remove #(core/attribute-allows-op? entity % :read roles))
                (map (comp keyword str id/extract)))
          (:attributes entity))))

(defn translate-attributes
  "Rewrite a before/after map from attribute-xid keys to user-facing
   attribute-key keys; denied keys are dropped instead of translated."
  [attrs attr-key-index denied]
  (when attrs
    (reduce-kv (fn [m k v]
                 (if (contains? denied k)
                   m
                   (assoc m (or (get attr-key-index k) k) v)))
               {}
               attrs)))

(defn ^:no-doc translate-delta
  "Translate a plug envelope (already gated by records+operations) into 0-2
   SSE-shaped maps; roles optional, omitting denies nothing."
  ([envelope attr-key-index data-records]
   (translate-delta envelope attr-key-index data-records #{}))
  ([{:keys [delta]} attr-key-index data-records roles]
  (let [{:keys [type data]} delta
        track (some-> type namespace)
        op    (some-> type name)]
    (case track
      "entity"
      (when op
        (let [entity (some-> (:entity-xid data) dataset/deployed-entity)
              denied (denied-attribute-xids entity roles)
              base (-> (select-keys data [:ts :tenant :scope :actor :request :txid])
                       (assoc :type       (str "record/" op)
                              :record-xid (:record-xid data)))
              with-before (cond-> base
                            (contains? data :before)
                            (assoc :before (translate-attributes
                                            (:before data) attr-key-index denied)))
              with-after  (cond-> with-before
                            (contains? data :after)
                            (assoc :after (translate-attributes
                                           (:after data) attr-key-index denied)))]
          [{:event "data" :data with-after}]))

      "relation"
      (when (#{"link" "unlink"} op)
        (let [from-xid (:from-xid data)
              to-xid   (:to-xid data)
              from?    (contains? data-records from-xid)
              to?      (contains? data-records to-xid)
              base     (-> (select-keys data [:ts :tenant :scope :actor :request :txid])
                           (assoc :type (str "relation/" op)))
              event-for (fn [subscribed other]
                          {:event "data"
                           :data  (assoc base :data [subscribed other])})]
          (cond
            (and from? to?) [(event-for from-xid to-xid)
                             (event-for to-xid from-xid)]
            from?           [(event-for from-xid to-xid)]
            to?             [(event-for to-xid from-xid)]
            ;; Neither endpoint subscribed — shouldn't happen (matcher already
            ;; gated by :endpoint-xids)
            :else           [])))

      nil))))
