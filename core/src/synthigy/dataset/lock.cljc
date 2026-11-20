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

(ns synthigy.dataset.lock
  "Row locks: a lease held by a user, kept in the row's `:lock` and `:lock-until` stamp attributes."
  (:require
   [synthigy.dataset.core :as core]))

(def kinds #{:lock :lock-until})

(def ^:dynamic *op*
  "True while a lock op writes: triggers then keep `:modify` stamps and mark the delta unaudited."
  false)

(defn lock-kind [attribute]
  (kinds (core/stamp attribute)))

(defn ttl [attribute]
  (get-in attribute [:configuration :lock/ttl]))


(defn lock-attributes
  "Active lock attributes of `entity` by kind, {:lock attr :lock-until attr}."
  [entity]
  (reduce (fn [acc a]
            (if-let [k (and (core/active? a) (lock-kind a))]
              (update acc k (fnil conj []) a)
              acc))
          {}
          (:attributes entity)))

(defn entity-lock
  "{:holder attr :until attr :ttl seconds} of a lockable `entity`, nil when it has no active holder."
  [entity]
  (let [{[holder] :lock [until] :lock-until} (lock-attributes entity)]
    (when holder
      (cond-> {:holder holder}
        until (assoc :until until :ttl (ttl until))))))

(defn entity-problems
  "Coded problems with `entity`'s lock attributes."
  [entity]
  (let [{holders :lock untils :lock-until} (lock-attributes entity)
        problem (fn [code attrs]
                  {:code code
                   :entity (:name entity)
                   :attributes (mapv :name attrs)})]
    (cond-> []
      (< 1 (count holders)) (conj (problem :lock/many-holders holders))
      (< 1 (count untils)) (conj (problem :lock/many-expiries untils))
      (and (seq untils) (empty? holders)) (conj (problem :lock/expiry-without-holder untils))
      (some #(let [t (ttl %)] (not (and (integer? t) (pos? t)))) untils)
      (conj (problem :lock/bad-ttl (remove #(let [t (ttl %)] (and (integer? t) (pos? t))) untils))))))

(defn schema-lock
  "The runtime schema's `:lock` of `entity`: {:holder field-key :until field-key :ttl seconds}."
  [entity field-key]
  (when-let [{:keys [holder until ttl]} (entity-lock entity)]
    (cond-> {:holder (field-key holder)}
      until (assoc :until (field-key until) :ttl ttl))))

(defn model-problems [model]
  (into [] (mapcat entity-problems) (core/get-entities model)))

(defn problems-error
  "The LOCK_MODEL_INVALID error of lock `problems`: code, rule, message and details."
  [problems]
  {:code "LOCK_MODEL_INVALID"
   :rule "record_lock"
   :message (str "Lock attributes are invalid: "
                 (apply str (interpose "; " (map #(str (:entity %) " " (name (:code %)) " " (:attributes %)) problems))))
   :details (mapv #(update % :code (fn [c] (str (namespace c) "/" (name c)))) problems)})

(defn assert-model!
  "Throws when the folded `model` breaks a lock rule."
  [model]
  (when-let [problems (not-empty (model-problems model))]
    (let [{:keys [message] :as error} (problems-error problems)]
      (throw (ex-info message (-> error (dissoc :message) (assoc :type ::invalid-lock)))))))
