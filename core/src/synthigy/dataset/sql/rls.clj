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

(ns synthigy.dataset.sql.rls
  "RLS compilation - converts id-based guard config to SQL-ready schema."
  (:require
    [clojure.string :as str]
    [synthigy.log :as log]
    [synthigy.dataset.core :as core]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.naming
     :refer [normalize-name
             relation->table-name
             entity->relation-field
             entity->table-name]]))

(defn compile-ref-condition
  "Compile a :ref condition; nil if the attribute is missing or invalid."
  [entity condition]
  (let [attr-uuid (:attribute condition)
        ;; dangling attr must DROP the rule, never throw — get-attribute throws;
        ;; audit who-cols are synthetic, hence the concat
        attr (some #(when (= attr-uuid (id/extract %)) %)
                   (concat (:attributes entity) (core/audit-ref-attrs entity)))]
    (when (and attr (:active attr))
      (let [attr-type (:type attr)]
        (when (#{"user" "group" "role"} attr-type)
          {:type :ref
           :column (normalize-name (:name attr))
           :match (keyword attr-type)})))))


(defn compile-relation-step
  "Compile a single relation step to table/field names; nil if relation not
   found."
  [model step from-entity-id]
  (let [rel-id (:relation-id step)
        relation (core/get-relation model rel-id)]
    (when (and relation (:active relation))
      (let [{:keys [from to]} relation
            forward? (= (id/extract from) from-entity-id)]
        {:table (relation->table-name relation)
         :from-field (entity->relation-field (if forward? from to))
         :to-field (entity->relation-field (if forward? to from))
         :from-entity (if forward? (id/extract from) (id/extract to))
         :to-entity (if forward? (id/extract to) (id/extract from))}))))


(defn entity-id->match-type
  "Match an entity id against the IAM user/group/role entity ids; returns :user,
   :group, :role, or nil."
  [entity-id]
  (condp = entity-id
    (id/entity :iam/user)       :user
    (id/entity :iam/user-group) :group
    (id/entity :iam/user-role)  :role
    nil))


(defn compile-relation-condition
  "Compile a :relation condition; nil if any relation is not found."
  [model entity condition]
  (let [steps (:steps condition)]
    (loop [remaining-steps steps
           current-entity-id (id/extract entity)
           compiled-hops []
           final-entity-id nil]
      (if (empty? remaining-steps)
        (when-let [match (entity-id->match-type final-entity-id)]
          {:type :relation
           :match match
           :hops compiled-hops})
        (let [step (first remaining-steps)
              compiled-step (compile-relation-step model step current-entity-id)]
          (if compiled-step
            (recur (rest remaining-steps)
                   (:to-entity compiled-step)
                   (conj compiled-hops (dissoc compiled-step :from-entity :to-entity))
                   (:to-entity compiled-step))
            nil))))))


(defn compile-hybrid-condition
  "Compile a :hybrid condition (relation hops + final ref attribute); nil if
   invalid."
  [model entity condition]
  (let [steps (:steps condition)
        attr-uuid (:attribute condition)]
    (loop [remaining-steps steps
           current-entity-id (id/extract entity)
           compiled-hops []
           final-entity-id (id/extract entity)]
      (if (empty? remaining-steps)
        (let [final-entity (core/get-entity model final-entity-id)
              ;; dangling attr drops the rule, never throw — same safe lookup as
              ;; compile-ref-condition
              final-attr (when final-entity
                           (some #(when (= attr-uuid (id/extract %)) %)
                                 (concat (:attributes final-entity)
                                         (core/audit-ref-attrs final-entity))))]
          (when (and final-attr (:active final-attr))
            (let [attr-type (:type final-attr)]
              (when (#{"user" "group" "role"} attr-type)
                {:type :hybrid
                 :match (keyword attr-type)
                 :hops compiled-hops
                 :final-table (entity->table-name final-entity)
                 :final-column (normalize-name (:name final-attr))}))))
        (let [step (first remaining-steps)
              compiled-step (compile-relation-step model step current-entity-id)]
          (if compiled-step
            (recur (rest remaining-steps)
                   (:to-entity compiled-step)
                   (conj compiled-hops (dissoc compiled-step :from-entity :to-entity))
                   (:to-entity compiled-step))
            nil))))))


(def self-guard
  "Injected on the IAM User entity: a principal can always read its own row."
  {:id "__self__"
   :operation #{:read}
   :conditions [{:type :ref :column "_eid" :match :user}]})


(defn compile-rls-condition
  "Compile a single RLS condition based on its type."
  [model entity condition]
  (case (:type condition)
    :ref (compile-ref-condition entity condition)
    :relation (compile-relation-condition model entity condition)
    :hybrid (compile-hybrid-condition model entity condition)
    nil))


(defn compile-rls-guard
  "Compile a guard's conditions; if ANY condition fails to resolve the WHOLE
   guard is dropped — a missing AND-term would silently broaden access."
  [model entity guard]
  (let [conditions (:conditions guard)
        compiled (map #(compile-rls-condition model entity %) conditions)]
    (if (and (seq conditions) (every? some? compiled))
      {:id (:id guard)
       :operation (:operation guard)
       :conditions (vec compiled)}
      ;; drop must be LOUD — a dropped :write/:delete guard fail-closes (1=0)
      ;; and writes silently no-op
      (do
        (log/warn {:id ::guard-dropped
                   :data {:action :dropped
                          :subject :rls-guard
                          :entity (:name entity)
                          :guard-id (:id guard)
                          :operation (:operation guard)
                          :conditions (mapv (fn [c cd]
                                              {:type (:type c)
                                               :attribute (:attribute c)
                                               :compiled? (some? cd)})
                                            conditions compiled)}}
                  (str "RLS guard dropped for entity '" (:name entity)
                       "' — a condition references a missing/inactive attribute or "
                       "relation. Operations " (pr-str (:operation guard))
                       " will FAIL CLOSED (silent no-op) unless another guard covers them."))
        nil))))


(defn unresolved-conditions
  "Conditions of `guard` that cannot compile against `model`, described for an
   operator."
  [model entity guard]
  (into []
        (comp (remove #(compile-rls-condition model entity %))
              (map (fn [c]
                     (cond-> {:type (:type c)}
                       (:attribute c) (assoc :attribute (:attribute c))
                       (:steps c) (assoc :relations (mapv :relation-id (:steps c)))))))
        (:conditions guard)))

(defn broken-guards
  "Guards in `model` that would be dropped at compile time — an empty guard, or
   one whose condition points at an inactive/missing attribute or relation."
  [model]
  (vec
   (for [entity (core/get-entities model)
         :when (and (:active entity) (core/rls-enabled? entity))
         guard (core/get-rls-guards entity)
         :let [unresolved (unresolved-conditions model entity guard)]
         :when (or (empty? (:conditions guard)) (seq unresolved))]
     {:entity (:name entity)
      :entity-id (id/extract entity)
      :guard-id (:id guard)
      :operation (:operation guard)
      :unresolved unresolved})))

(defn declares-guard?
  "Whether `model` is the model that authors this guard."
  [model {:keys [entity-id guard-id]}]
  (boolean
   (when-let [entity (core/get-entity model entity-id)]
     (some #(= guard-id (:id %)) (core/get-rls-guards entity)))))

(defn describe-broken
  [broken]
  (str/join
   "; "
   (map (fn [{:keys [entity guard-id operation unresolved]}]
          (str entity " guard " guard-id " ("
               (str/join "," (map name (sort operation)))
               ") → " (if (seq unresolved)
                        (pr-str unresolved)
                        "no conditions")))
        broken)))

(defn assert-guards-compile!
  "Throws when `candidate` breaks a guard it authors itself; a guard some OTHER
   dataset authors only warns — a stale copy of a foreign entity must not hold
   an unrelated deployment hostage."
  [model candidate]
  (let [broken (broken-guards model)
        {own true foreign false} (group-by #(declares-guard? candidate %) broken)]
    (when (seq foreign)
      (log/warn {:id ::guards-would-drop
                 :data {:action :dropping :subject :rls-guard
                        :guards (mapv #(select-keys % [:entity :guard-id :operation :unresolved])
                                      foreign)}}
                (str "Deployment drops RLS guards authored by another dataset: "
                     (describe-broken foreign)
                     ". Those operations will FAIL CLOSED — refresh this model's copy "
                     "of the entity, or delete the guard.")))
    (when (seq own)
      (throw
       (ex-info
        (str "RLS guards would be dropped by this deployment: "
             (describe-broken own)
             ". Those operations would fail closed — delete the guard or keep what "
             "it references active.")
        {:type ::broken-rls-guards
         :code "RLS_GUARD_BROKEN"
         :guards (vec own)})))))

(defn compile-entity-rls
  "Compile all RLS configuration for an entity; nil when RLS is disabled."
  [model entity]
  (when (core/rls-enabled? entity)
    (let [compiled-guards (->> (core/get-rls-guards entity)
                               (keep #(compile-rls-guard model entity %))
                               vec)
          compiled-guards (cond-> compiled-guards
                            (= (id/extract entity) (id/entity :iam/user))
                            (conj self-guard))]
      ;; never nil while enabled — a missing :rls key reads as "no RLS" downstream
      {:enabled true
       :guards compiled-guards})))
