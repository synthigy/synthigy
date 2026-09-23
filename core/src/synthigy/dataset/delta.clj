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

(ns synthigy.dataset.delta
  "Delta pipeline: trigger plug is the only source of envelopes; subscribers register
   declarative interest and get matching envelopes via a per-subscriber channel + go-loop.
   See docs/core/synthigy/dataset/delta.md for the interest descriptor shape."
  (:require
    [clojure.core.async :as async]
    [synthigy.log :as log]
    [synthigy.traffic :as traffic]))

;; key → {:interest <map> :handler (fn [env]) :ch <chan> :stop? (atom)}
(defonce registry (atom {}))

;; ready lags shutdown!/init! independently of the defonce registry, which
;; survives reloads
(defonce ready (atom false))

(def default-buffer-size 100)

(declare schedule-reconcile!)

(defn init!
  "Marks the pipe as ready. Called by synthigy.dataset/start."
  []
  (reset! ready true)
  nil)

(defn close-subscriber!
  [{:keys [ch stop?]}]
  (when stop? (reset! stop? true))
  (when ch    (async/close! ch)))

(defn shutdown!
  "Tears the pipe down: closes every subscriber's channel, drops the registry.
   Called by synthigy.dataset/stop."
  []
  (reset! ready false)
  (doseq [sub (vals @registry)]
    (close-subscriber! sub))
  (reset! registry {})
  (schedule-reconcile!)
  nil)

(defn ready?
  "True between init! and shutdown!."
  []
  @ready)

(defn entity-shape-active?
  "True iff the interest narrows on any entity-side dimension."
  [interest]
  (some #(seq (get interest %)) [:entity-xids :record-xids :attribute-xids]))

(defn relation-shape-active?
  "True iff the interest narrows on any relation-side dimension."
  [interest]
  (some #(seq (get interest %))
        [:relation-xids :from-xids :to-xids :endpoint-xids]))

(defn envelope-track
  "Classifies envelope by its :delta :type namespace — :entity, :relation, or
   nil."
  [envelope]
  (case (some-> envelope :delta :type namespace)
    "entity"   :entity
    "relation" :relation
    nil))

(defn envelope-op
  "Unqualified op keyword off :delta :type — matches the :ops set in interest
   descriptors."
  [envelope]
  (some-> envelope :delta :type name keyword))

(defn changed-attribute-xids
  "Attribute xids touched by an entity envelope, per op."
  [op data]
  (case op
    :insert (->> (or (:after data) {}) keys (mapv #(if (keyword? %) (name %) %)))
    :update (->> (or (:after data) {})
                 (keep (fn [[k v]] (when (not= v (get (:before data) k)) k)))
                 (mapv #(if (keyword? %) (name %) %)))
    :delete []
    []))

(defn matches-cross-cutting?
  [{:keys [tenant-xid scope-xid actor-xid]} {:keys [tenant scope actor]}]
  (and (or (nil? tenant-xid) (= tenant-xid tenant))
       (or (nil? scope-xid)  (= scope-xid  scope))
       (or (nil? actor-xid)  (= actor-xid  actor))))

(defn matches-entity?
  "Applies entity-side narrowing (entity-xids/record-xids/attribute-xids)."
  [interest envelope]
  (let [{:keys [entity-xids record-xids attribute-xids ops]} interest
        data (-> envelope :delta :data)
        op   (envelope-op envelope)]
    (and (or (empty? entity-xids)
             (contains? entity-xids (:entity-xid data)))
         (or (empty? record-xids)
             (contains? record-xids (:record-xid data)))
         (or (empty? attribute-xids)
             (let [changed (changed-attribute-xids op data)]
               (boolean (some attribute-xids changed))))
         (or (empty? ops) (contains? ops op)))))

(defn matches-relation?
  [interest envelope]
  (let [{:keys [relation-xids from-xids to-xids endpoint-xids ops]} interest
        data (-> envelope :delta :data)
        op   (envelope-op envelope)]
    (and (or (empty? relation-xids)
             (contains? relation-xids (:element envelope)))
         (or (empty? from-xids)
             (contains? from-xids (:from-xid data)))
         (or (empty? to-xids)
             (contains? to-xids (:to-xid data)))
         (or (empty? endpoint-xids)
             (contains? endpoint-xids (:from-xid data))
             (contains? endpoint-xids (:to-xid data)))
         (or (empty? ops) (contains? ops op)))))

(defn matches?
  "True iff envelope should be delivered to a subscriber holding interest."
  [interest envelope]
  (let [track   (envelope-track envelope)
        e-on?   (entity-shape-active? interest)
        r-on?   (relation-shape-active? interest)
        any-on? (or e-on? r-on?)
        data    (-> envelope :delta :data)]
    (boolean
      (and
        (case track
          :entity   (or (not any-on?) e-on?)
          :relation (or (not any-on?) r-on?)
          false)
        (matches-cross-cutting? interest data)
        (case track
          :entity   (if e-on? (matches-entity?   interest envelope) true)
          :relation (if r-on? (matches-relation? interest envelope) true))))))

(defn run-subscriber-loop!
  "Go-loop pulling envelopes off a subscriber's channel; handler exceptions are
   caught+logged."
  [key handler ch stop?]
  (async/go-loop []
    (when-not @stop?
      (when-some [env (async/<! ch)]
        (try (handler env)
             (catch Throwable e
               (log/error! {:id ::handler-failed
                            :data {:key key}
                            :msg "Delta subscriber handler threw"}
                           e)))
        (recur)))))

(defn subscribe!
  "Registers handler against interest under key. Re-subscribing under the same key REPLACES
   the prior binding, losing anything unconsumed — use retune! to change only the interest."
  ([key interest handler] (subscribe! key interest handler nil))
  ([key interest handler on-drop]
   (let [ch    (async/chan default-buffer-size)
         stop? (atom false)
         prior (get @registry key)]
     (when prior (close-subscriber! prior))
     (swap! registry assoc key {:interest interest
                                :handler  handler
                                :ch       ch
                                :stop?    stop?
                                :on-drop  on-drop})
     (run-subscriber-loop! key handler ch stop?)
     (schedule-reconcile!)
     key)))

(defn retune!
  "Atomically replaces the interest of the subscription under key, leaving channel/handler
   untouched — the lossless way to follow a moving interest. No-op (nil) if key is unknown."
  [key interest]
  (let [[old _] (swap-vals! registry
                            (fn [r]
                              (if (contains? r key)
                                (assoc-in r [key :interest] interest)
                                r)))]
    (when (contains? old key)
      (schedule-reconcile!)
      key)))

(defn unsubscribe!
  "Detaches the subscription under key. No-op if unknown."
  [key]
  (when-let [sub (get @registry key)]
    (close-subscriber! sub)
    (swap! registry dissoc key)
    (schedule-reconcile!))
  nil)

(defn subscriptions
  "Diagnostic — set of currently-registered subscription keys."
  []
  (set (keys @registry)))

(defn dispatch!
  "Non-blocking offer of envelope onto every matching subscriber's channel.
   LOCAL fan-out only — producers go through publish!."
  [envelope]
  (let [matched (reduce
                  (fn [matched {:keys [interest ch on-drop]}]
                    (try
                      (if (matches? interest envelope)
                        ;; fixed buffer: offer! false on full is the live-drop
                        ;; signal, hands to on-drop for durable resync
                        (do (when-not (async/offer! ch envelope)
                              (traffic/count! :deltas-dropped)
                              ;; explicit :system topic: authoritative
                              ;; :traffic/delta ids skip ns rules, so a DROP
                              ;; (delivery health) needs this to be visible in
                              ;; any lens
                              (log/warn {:id ::delta-dropped
                                         :topics #{:system}
                                         :data {:seq (:seq envelope)
                                                :envelope-type (some-> envelope :delta :type)
                                                :resync? (some? on-drop)}}
                                        "Subscriber buffer full — delta dropped")
                              (when on-drop (on-drop envelope)))
                            (inc matched))
                        matched)
                      (catch Throwable e
                        (log/error! {:id ::dispatch-match-failed
                                     :data {:envelope-type (some-> envelope :delta :type)}
                                     :msg "Match threw — interest may be malformed"}
                                    e)
                        matched)))
                  0
                  (vals @registry))]
    (log/debug {:id ::delta-published
                :data {:seq (:seq envelope)
                       :envelope-type (some-> envelope :delta :type)
                       :subscribers matched}}
               "Delta dispatched to live subscribers"))
  nil)

;; cross-node transport seam, same idiom as wake/*wake-source* and
;; audit/*audit-provider*
(defprotocol DeltaProvider
  (publish-deltas! [this envelopes]
    "Delivers a drained batch toward every node's local dispatch!.")
  (reconcile-interest! [this interests]
    "Pushes this node's interest union to the transport as a source filter. Optimization only."))

(defrecord Local []
  DeltaProvider
  (publish-deltas!     [_ envelopes] (run! dispatch! envelopes))
  (reconcile-interest! [_ _] nil))

(def ^:dynamic *delta-provider*
  "The active DeltaProvider — Local for single-node, a broker (NATS/Kafka) for clustered payload delivery."
  (->Local))

(defn publish!
  "Producer seam — the drainer calls this once per drained batch."
  [envelopes]
  (publish-deltas! *delta-provider* envelopes))

(defn interests-union
  "Pure coarse union of interest maps per track: {:entity <xid-set|:all|nil>
   :relation <...>}."
  [interests]
  (let [merge-topic (fn [a b]
                      (cond (nil? a) b
                            (nil? b) a
                            (or (= a :all) (= b :all)) :all
                            :else (into a b)))
        topic (fn [on? xids]
                (when on? (or (not-empty (set xids)) :all)))]
    (reduce (fn [acc interest]
              (let [e-on? (boolean (entity-shape-active? interest))
                    r-on? (boolean (relation-shape-active? interest))
                    firehose? (not (or e-on? r-on?))]
                (-> acc
                    (update :entity merge-topic
                            (topic (or e-on? firehose?)
                                   (when e-on? (:entity-xids interest))))
                    (update :relation merge-topic
                            (topic (or r-on? firehose?)
                                   (when r-on? (:relation-xids interest)))))))
            {:entity nil :relation nil}
            interests)))

(defn node-interest
  "This node's interests-union over the live registry."
  []
  (interests-union (map :interest (vals @registry))))

(defonce reconcile-pending (atom false))

(defn schedule-reconcile!
  "Debounced (100ms) push of node-interest to the provider; coalesces
   subscribe!/unsubscribe! bursts."
  []
  (when (compare-and-set! reconcile-pending false true)
    (async/go
      (async/<! (async/timeout 100))
      ;; clear before computing: a registry change landing mid-cycle schedules a
      ;; fresh cycle instead of being missed
      (reset! reconcile-pending false)
      (try
        (reconcile-interest! *delta-provider* (node-interest))
        (catch Throwable e
          (log/warn {:id ::reconcile-failed
                     :data {:action :recovering :subject :delta}}
                    (.getMessage e)))))))

