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

(ns synthigy.log.topics
  "Topic classification for log signals — what a signal is ABOUT, an axis
   orthogonal to level (how severe it is). Lenses subscribe to a topic
   (`:system`, `:dataset`, `:auth`, `:traffic`, …) drawn from the closed
   `all-topics` vocabulary instead of guessing from ns+level; `classify`
   derives it from explicit `:topics` plus rules over `:id`/`:data`/`:ns`/`:level`,
   with `:system` as the operator catch-all."
  (:require
   [clojure.string :as str]))

(def parent-topics
  "The closed set of top-level topics. Lenses subscribe at this level."
  #{:system :dataset :auth :traffic :audit :diagnostic})

(def child-topics
  "The closed set of two-level topics (`parent/child`); a child never appears
   without its parent. Locked 2026-07-22 (traffic family only)."
  #{:traffic/request :traffic/op :traffic/sse
    :traffic/subscription :traffic/delta})

(def all-topics
  "The closed vocabulary of topics a signal may be tagged with."
  (into parent-topics child-topics))

;;; ============================================================================
;;; Rule tables — the taxonomy, as data
;;; ============================================================================

(def ^:private id->topics
  "Exact `:id` → topics, authoritative over all other rules; covers the
   per-request/op firehose."
  {:synthigy.server/request-completed #{:traffic/request}
   :synthigy.server.data/op-completed #{:traffic/op}})

(def ^:private traffic-id-prefix->topic
  "Id-NAME prefix → authoritative traffic child topic, regardless of
   namespace or action."
  {"sse-"          :traffic/sse
   "subscription-" :traffic/subscription
   "delta-"        :traffic/delta})

(def ^:private lifecycle-actions
  "`:data :action` verbs that mark a module-lifecycle event → `:system`."
  #{:starting :started :stopping :stopped
    :setup :setup-complete :cleanup :cleanup-complete
    :initialized :ready :not-initialized})

(def ^:private deploy-actions
  "`:data :action` verbs marking a deploy/schema/patch event → `:system` + `:dataset`."
  #{:deploying :deployed :deploy-failed
    :recalling :recalled :recall-failed
    :destroying :destroyed :destroy-failed
    :migrating :migrated :installing :installed
    :upgrading :upgraded :patching :patched})

(def ^:private subject->topics
  "`:data :subject` noun → topics; splits identity FLOWS (`:auth`) from
   security INFRASTRUCTURE (`:system`)."
  {;; system infrastructure
   :http-server       #{:system}
   :admin-server      #{:system}
   :port-file         #{:system}
   :admin             #{:system}
   :db-backend        #{:system}
   :encryption        #{:system}
   :dek               #{:system}
   :keypair           #{:system}
   :oauth-persistence #{:system}
   ;; dataset
   :dataset           #{:dataset}
   :dataset-schema    #{:dataset}
   :dataset-model     #{:dataset}
   :dataset-features  #{:dataset}
   :deployed-model    #{:dataset}
   :deploy-history    #{:dataset}
   :model             #{:dataset}
   :id-format         #{:dataset}
   :id-triggers       #{:dataset}
   :xid               #{:dataset}
   :xid-migration     #{:dataset}
   :meta-table        #{:dataset}
   :meta-tables       #{:dataset}
   :data-tables       #{:dataset}
   ;; auth / identity flows
   :oauth             #{:auth}
   :oauth-clients     #{:auth}
   :oauth-store       #{:auth}
   :client-secret     #{:auth}
   :client-secrets    #{:auth}
   :authorization-code #{:auth}
   :device-code       #{:auth}
   :token             #{:auth}
   :session           #{:auth}
   :role-access       #{:auth}
   :iam-access        #{:auth}
   :iam-schema        #{:auth}
   :iam-model         #{:auth}
   :iam-connector     #{:auth}
   :iam-defaults      #{:auth}
   ;; audit / observability
   :iam-audit         #{:audit}
   :observability     #{:audit :system}
   ;; delivery flow
   :delta             #{:traffic/delta}})

(def ^:private ns-prefix->topics
  "Fallback base topic by `:ns` prefix, ORDERED — first (most specific) match wins."
  [["synthigy.oauth"              #{:auth}]
   ["synthigy.oidc"               #{:auth}]
   ["synthigy.iam.audit"          #{:audit}]
   ["synthigy.iam.encryption"     #{:system}]
   ["synthigy.iam"                #{:auth}]
   ["synthigy.dataset.encryption" #{:system}]
   ["synthigy.dataset"            #{:dataset}]
   ["synthigy.observability"      #{:audit}]
   ["synthigy.plug"          #{:audit}]
   ["synthigy.subscriptions"      #{:system}]
   ["synthigy.database"           #{:system}]
   ["synthigy.admin"              #{:system}]
   ["synthigy.log"                #{:system}]
   ["synthigy.server"             #{:system}]])

;;; ============================================================================
;;; Classification
;;; ============================================================================

(defn id-name [id]
  (when (keyword? id) (name id)))

(defn authoritative-traffic
  "Topic set (possibly a child like `:traffic/sse`) when `id` is authoritative
   traffic; nil otherwise."
  [id]
  (or (id->topics id)
      (when-let [n (id-name id)]
        (some (fn [[prefix child]]
                (when (str/starts-with? n prefix) #{child}))
              traffic-id-prefix->topic))))

(defn add-parents
  "Materialize every child topic's parent into the set (`:traffic/sse` ⇒ also
   `:traffic`), so parent-level subscriptions (`:has :traffic`) always match."
  [topics]
  (into topics (keep #(some-> (namespace %) keyword)) topics))

(defn ns-base-topics [ns-str]
  (let [s (str ns-str)]
    (or (some (fn [[prefix topics]]
                (when (str/starts-with? s prefix) topics))
              ns-prefix->topics)
        #{})))

(defn classify
  "Return the topic set for a Telemere `signal`; pure, with `:system` as
   catch-all."
  [{:keys [id ns level data] :as signal}]
  (let [explicit (set (:topics signal))
        action   (:action data)
        subject  (:subject data)
        diag?    (contains? #{:debug :trace} level)
        degraded? (contains? #{:warn :error :fatal} level)]
    (if-let [traffic-t (authoritative-traffic id)]
      (add-parents
        (cond-> (into explicit traffic-t)
          diag? (conj :diagnostic)))
      (let [t (cond-> explicit
                (contains? lifecycle-actions action) (conj :system)
                (contains? deploy-actions action)    (into #{:system :dataset})
                (= :denied action)                   (conj :auth)
                (contains? subject->topics subject)  (into (subject->topics subject))
                true                                 (into (ns-base-topics ns)))
            t (add-parents t)
            t (cond-> t
                (and degraded? (not (contains? t :traffic))) (conj :system)
                (not (some t [:dataset :auth :traffic])) (conj :system)
                diag?                                   (conj :diagnostic))]
        t))))

(defn has-topic?
  "True when `signal` classifies to `topic`."
  [signal topic]
  (contains? (classify signal) topic))
