;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.subscription
  "Server-agnostic /data subscriptions: full-set-replace sessions streamed over SSE."
  (:require
   [clojure.core.async :as async]
   [clojure.set :as set]
   [clojure.string :as str]
   [environ.core :refer [env]]
   [synthigy.log :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.iam.access :as access]
   [synthigy.json :as json]
   [synthigy.engine.subscription :as trans]
   [synthigy.oauth.authentication :as auth]
   [synthigy.traffic :as traffic]
   [synthigy.engine :as engine]
   [synthigy.server.data :as data]
   [synthigy.server.subscriptions.replay :as replay]
   [patcho.lifecycle :as lifecycle])
  (:import
   [java.time Instant]))

(declare ensure-delta-loop! ensure-model-loop! cleanup-identity!
         ensure-model-chan fan-out!)

;;; ============================================================================
;;; Configuration
;;; ============================================================================

(def supported-types
  "Subscription types currently implemented."
  #{"data" "entity" "relation" "deployed-model" "runtime-model"})

;;; ============================================================================
;;; State
;;; ============================================================================

(defonce ^:private subscriptions-state (atom {}))

(defonce ^:private event-streams (atom {}))

(defonce ^:private loops (atom {}))

(defn claim-loop!
  "Atomically claim the loop slot for [identity-key kind]; true iff this caller won it."
  [identity-key kind]
  (let [[old _] (swap-vals! loops update identity-key (fnil conj #{}) kind)]
    (not (contains? (get old identity-key) kind))))

(defn end-loop! [identity-key kind]
  (swap! loops (fn [m]
                 (let [s' (disj (get m identity-key #{}) kind)]
                   (if (seq s')
                     (assoc m identity-key s')
                     (dissoc m identity-key))))))

;;; ============================================================================
;;; Identity
;;; ============================================================================

(defn request-identity
  "[sub client_id] identity key from the request + auth context; with IAM off,
   falls back to the x-synthigy-client header/param."
  [request iam]
  (if iam
    (let [{:keys [sub client_id]} (:claims iam)]
      [(or sub (some-> (:principal iam) id/extract)) client_id])
    ["anonymous" (or (get-in request [:headers "x-synthigy-client"])
                     (get-in request [:query-params "client"]))]))

;;; ============================================================================
;;; Subscription Management
;;; ============================================================================

(defn data-sub-key
  [identity-key]
  [::data identity-key])

(def ^:private max-data-resyncs 5)

(defn ensure-batch-chan
  "Per-identity FIXED-buffer channel for incoming envelopes; also lazily
   allocates the resync atoms (:lagged/:last-seq/:resyncs)."
  [identity-key]
  (-> (swap! subscriptions-state update-in [identity-key :data]
             (fn [d]
               (cond-> (or d {})
                 (nil? (:batch-chan d)) (assoc :batch-chan (async/chan 100))
                 (nil? (:lagged d))     (assoc :lagged   (atom false))
                 (nil? (:last-seq d))   (assoc :last-seq (atom 0))
                 (nil? (:resyncs d))    (assoc :resyncs  (atom 0)))))
      (get-in [identity-key :data :batch-chan])))

(defn flag-data-lagged! [identity-key]
  (some-> (get-in @subscriptions-state [identity-key :data :lagged]) (reset! true)))

(defn collect-data-interest
  "Compute the plug interest descriptor from the identity's current :data state."
  [identity-key]
  (let [records (get-in @subscriptions-state [identity-key :data :records])
        ops     (get-in @subscriptions-state [identity-key :data :operations])
        ops-set (when (seq ops)
                  (set (mapcat (fn [op]
                                 (case op
                                   "change" [:insert :update]
                                   "delete" [:delete]
                                   "insert" [:insert]
                                   "update" [:update]
                                   "link"   [:link]
                                   "unlink" [:unlink]
                                   [(keyword op)]))
                               ops)))]
    (cond-> {}
      (seq records) (assoc :record-xids   records
                           :endpoint-xids records)
      ops-set       (assoc :ops ops-set))))

(defn recompute-data-subscription!
  "Re-register the identity's delta subscription with its current interest, or unsubscribe when empty."
  [identity-key]
  (let [interest (collect-data-interest identity-key)
        sub-key  (data-sub-key identity-key)]
    (if (seq interest)
      (let [ch (ensure-batch-chan identity-key)]
        (delta/subscribe!
         sub-key interest
         (fn [envelope]
           (when-not (async/offer! ch envelope)
             (flag-data-lagged! identity-key)))
         (fn [_env] (flag-data-lagged! identity-key))))
      (delta/unsubscribe! sub-key))))

(defn data-resync-envelopes
  "Replay missed record events from the store when the identity's data stream was flagged lagged."
  [identity-key]
  (let [{:keys [lagged last-seq resyncs]} (get-in @subscriptions-state [identity-key :data])]
    (when (and lagged (compare-and-set! lagged true false))
      (if (< @resyncs max-data-resyncs)
        (let [interest (collect-data-interest identity-key)
              envs     (filterv #(delta/matches? interest %)
                                (replay/replay-envelopes @last-seq 10000))]
          (swap! resyncs inc)
          (log/info {:id ::data-resync
                     :data {:action :recovering :subject :delta
                            :from @last-seq :count (count envs)}}
                    "Healed dropped /data/events records via store replay")
          envs)
        (do (log/warn {:id ::data-resync-budget-exhausted
                       :data {:action :recovering :subject :delta}}
                      "Data resync budget exhausted; client should reconnect")
            nil)))))

(defn normalize-data-item
  "Validate a data subscription item; legacy entity-firehose fields are hard-rejected."
  [item]
  (let [legacy-keys (set/intersection (set (keys item))
                                      #{"entity" "entities" "relations" "firehose"
                                        :entity :entities :relations :firehose})]
    (when (seq legacy-keys)
      (throw (ex-info (str "`data` subscription items take `records` only. "
                           "Found " (pr-str legacy-keys) " — did you mean "
                           "`{type: \"entity\", entities: [...]}` or "
                           "`{type: \"relation\", relations: [...]}` ?")
                      {:code "INVALID_SUBSCRIPTION"
                       :unknown-fields legacy-keys}))))
  (let [records-spec (or (get item "records") (get item :records))
        operations   (when-let [ops (or (get item "operations") (get item :operations))]
                       (when-not (sequential? ops)
                         (throw (ex-info "operations must be an array"
                                         {:code "INVALID_OPERATIONS"})))
                       (set ops))]
    (when (or (nil? records-spec) (not (sequential? records-spec)))
      (throw (ex-info "Missing required field `records` on data subscription item"
                      {:code "MISSING_RECORDS"})))
    (let [records (into #{} (filter string?) records-spec)]
      (when (empty? records)
        (throw (ex-info "`records` must be a non-empty array of xid strings"
                        {:code "EMPTY_RECORDS"})))
      {:records    records
       :operations operations})))

(defn coalesce-loop!
  "Per-identity go-loop draining batch-chan, batching for 100ms, translating +
   coalescing by coalesce-key-fn, then fanning out."
  [identity-key loop-kind batch-chan translate-fn coalesce-key-fn]
  (when (claim-loop! identity-key loop-kind)
    (async/go-loop []
      (if-let [first-env (async/<! batch-chan)]
        (let [batch (loop [batch [first-env]]
                      (let [[val port] (async/alts! [batch-chan (async/timeout 100)])]
                        (if (and val (= port batch-chan))
                          (recur (conj batch val))
                          batch)))
              translated (into [] (keep translate-fn) batch)
              coalesced  (->> translated
                              (group-by coalesce-key-fn)
                              (vals)
                              (mapv last))]
          (when (seq coalesced)
            (fan-out! identity-key coalesced))
          (recur))
        (end-loop! identity-key loop-kind)))))

(defn ensure-track-batch-chan!
  "Per-identity sliding channel for track-key (:entity or :relation)."
  [identity-key track-key]
  (-> (swap! subscriptions-state update-in [identity-key track-key :batch-chan]
             #(or % (async/chan (async/sliding-buffer 100))))
      (get-in [identity-key track-key :batch-chan])))

(defn entity-sub-key   [identity-key] [::entity   identity-key])
(defn relation-sub-key [identity-key] [::relation identity-key])

(defn recompute-entity-subscription!
  [identity-key]
  (let [names   (get-in @subscriptions-state [identity-key :entity :name-by-xid])
        xids    (set (keys names))
        ops     (get-in @subscriptions-state [identity-key :entity :ops])
        sub-key (entity-sub-key identity-key)]
    (if (seq xids)
      (let [ch (ensure-track-batch-chan! identity-key :entity)]
        (delta/subscribe!
         sub-key (cond-> {:entity-xids xids} (seq ops) (assoc :ops ops))
         (fn [envelope] (async/offer! ch envelope))))
      (delta/unsubscribe! sub-key))))

(defn recompute-relation-subscription!
  [identity-key]
  (let [names   (get-in @subscriptions-state [identity-key :relation :name-by-xid])
        xids    (set (keys names))
        ops     (get-in @subscriptions-state [identity-key :relation :ops])
        sub-key (relation-sub-key identity-key)]
    (if (seq xids)
      (let [ch (ensure-track-batch-chan! identity-key :relation)]
        (delta/subscribe!
         sub-key (cond-> {:relation-xids xids} (seq ops) (assoc :ops ops))
         (fn [envelope] (async/offer! ch envelope))))
      (delta/unsubscribe! sub-key))))

(defn translate-entity-delta
  "Stripped SSE shape for entity pokes: {type entity ts} only; entity echoes the
   client's verbatim original string via name-by-xid."
  [{:keys [delta]} name-by-xid]
  (when-let [client-name (get name-by-xid (-> delta :data :entity-xid))]
    {:event "data"
     :data  {:type   "entity/touched"
             :entity client-name
             :ts     (-> delta :data :ts)}}))

(defn translate-relation-delta
  "Stripped SSE shape for relation pokes: {type relation ts} only."
  [{:keys [element delta]} name-by-xid]
  (when-let [client-name (get name-by-xid element)]
    {:event "data"
     :data  {:type     "relation/touched"
             :relation client-name
             :ts       (-> delta :data :ts)}}))

(defn ensure-entity-delta-loop!
  [identity-key]
  (let [batch-chan (get-in @subscriptions-state [identity-key :entity :batch-chan])
        translate  (fn [env]
                     (translate-entity-delta
                      env
                      (get-in @subscriptions-state [identity-key :entity :name-by-xid])))]
    (coalesce-loop! identity-key :entity batch-chan
                    translate (comp :entity :data))))

(defn ensure-relation-delta-loop!
  [identity-key]
  (let [batch-chan (get-in @subscriptions-state [identity-key :relation :batch-chan])
        translate  (fn [env]
                     (translate-relation-delta
                      env
                      (get-in @subscriptions-state [identity-key :relation :name-by-xid])))]
    (coalesce-loop! identity-key :relation batch-chan
                    translate (comp :relation :data))))

(defn apply-data-track
  [state identity-key validated records ops]
  (if (seq validated)
    (-> state
        (assoc-in [identity-key :data :records]    records)
        (assoc-in [identity-key :data :operations] ops))
    (cond-> state
      (get-in state [identity-key :data])
      (update-in [identity-key :data] dissoc :records :operations))))

(defn apply-entity-track
  [state identity-key validated name-by-xid ops]
  (if (seq validated)
    (-> state
        (assoc-in [identity-key :entity :name-by-xid] name-by-xid)
        (assoc-in [identity-key :entity :ops]         ops))
    (cond-> state
      (get-in state [identity-key :entity])
      (update-in [identity-key :entity] dissoc :name-by-xid :ops))))

(defn apply-relation-track
  [state identity-key validated name-by-xid ops]
  (if (seq validated)
    (-> state
        (assoc-in [identity-key :relation :name-by-xid] name-by-xid)
        (assoc-in [identity-key :relation :ops]         ops))
    (cond-> state
      (get-in state [identity-key :relation])
      (update-in [identity-key :relation] dissoc :name-by-xid :ops))))

(defn replace-model-state!
  "Apply the model-subscription side of a set call; anything in the prior state
   but not in desired-types is torn down."
  [identity-key desired-types principal]
  (if (seq desired-types)
    (do (ensure-model-chan identity-key)
        (swap! subscriptions-state assoc-in [identity-key :model :types]
               desired-types)
        (ensure-model-loop! identity-key principal))
    (do (when-let [ch (get-in @subscriptions-state [identity-key :model :chan])]
          (async/unsub dataset/publisher :model/deployed ch)
          (async/close! ch))
        (swap! subscriptions-state update identity-key dissoc :model))))

(defn set-subscriptions!
  "Replace the full subscription state for identity-key with the request body's
   subscriptions array; items of the same type union, absent items drop, empty
   array clears."
  [identity-key body principal]
  (access/with-principal principal
    (let [items (or (get body "subscriptions") (get body :subscriptions) [])
          item-type (fn [item] (or (get item "type") (get item :type) "data"))]
      (doseq [item items]
        (let [t (item-type item)]
          (when-not (supported-types t)
            (throw (ex-info (str "Unsupported subscription type: " t)
                            {:code "UNSUPPORTED_TYPE" :type t
                             :supported (vec supported-types)})))))
      (let [data-items     (filter #(= "data"     (item-type %)) items)
            entity-items   (filter #(= "entity"   (item-type %)) items)
            relation-items (filter #(= "relation" (item-type %)) items)
            model-types    (into #{}
                                 (keep (fn [item]
                                         (let [t (item-type item)]
                                           (when (#{"deployed-model" "runtime-model"} t) t))))
                                 items)
            _              (when (seq model-types)
                             (engine/assert-scope! "dataset:subscription"))
            validated-data     (mapv normalize-data-item     data-items)
            validated-entity   (mapv trans/normalize-entity-item   entity-items)
            validated-relation (mapv trans/normalize-relation-item relation-items)
            first-wins (fn [a _b] a)
            all-records (reduce set/union #{} (map :records validated-data))
            all-operations (if (and (seq validated-data)
                                    (every? :operations validated-data))
                             (reduce set/union #{} (map :operations validated-data))
                             nil)
            all-entity-names   (reduce (partial merge-with first-wins) {}
                                       (map :name-by-xid validated-entity))
            all-entity-ops     (when (and (seq validated-entity) (every? :ops validated-entity))
                                 (reduce set/union #{} (map :ops validated-entity)))
            all-relation-names (reduce (partial merge-with first-wins) {}
                                       (map :name-by-xid validated-relation))
            all-relation-ops   (when (and (seq validated-relation) (every? :ops validated-relation))
                                 (reduce set/union #{} (map :ops validated-relation)))]
        (swap! subscriptions-state
               (fn [s]
                 (-> s
                     (apply-data-track     identity-key validated-data     all-records         all-operations)
                     (apply-entity-track   identity-key validated-entity   all-entity-names    all-entity-ops)
                     (apply-relation-track identity-key validated-relation all-relation-names  all-relation-ops))))
        (replace-model-state! identity-key model-types principal)
        (recompute-data-subscription!     identity-key)
        (recompute-entity-subscription!   identity-key)
        (recompute-relation-subscription! identity-key)
        (traffic/count! :subscriptions)
        (let [[user-xid client-id] identity-key]
          (log/debug {:id ::subscription-set
                     :user-xid user-xid
                     :data {:action :set
                            :subject :subscription
                            :client client-id
                            :record-count   (count all-records)
                            :entity-count   (count all-entity-names)
                            :relation-count (count all-relation-names)
                            :operations (when all-operations (vec all-operations))
                            :model-tracks (when (seq model-types) (vec model-types))}}
                    "Subscription state replaced"))
        (cond
          (and (empty? validated-data)
               (empty? validated-entity)
               (empty? validated-relation)
               (empty? model-types))
          (cleanup-identity! identity-key)

          :else
          (do (when (seq validated-data)     (ensure-delta-loop!          identity-key principal))
              (when (seq validated-entity)   (ensure-entity-delta-loop!   identity-key))
              (when (seq validated-relation) (ensure-relation-delta-loop! identity-key))))
        {:ok true}))))

(defn model-fingerprint
  "Set of visible entity+relation IDs for a principal's access context."
  [model principal]
  (access/with-principal principal
    (let [entity-ids (into #{}
                           (comp (map id/extract)
                                 (filter #(daccess/entity-allows? % #{:read})))
                           (core/get-entities model))
          relation-ids (into #{}
                             (keep (fn [rel]
                                     (let [rid (id/extract rel)
                                           fid (id/extract (:from rel))
                                           tid (id/extract (:to rel))]
                                       (when (or (daccess/relation-allows? rid [fid tid] #{:read})
                                                 (daccess/relation-allows? rid [tid fid] #{:read}))
                                         rid))))
                             (core/get-relations model))]
      (into entity-ids relation-ids))))

(defn ensure-model-chan
  "Get or create the model event channel for an identity; swap-vals! so only the
   installer thread runs async/sub."
  [identity-key]
  (let [[old new] (swap-vals! subscriptions-state update-in
                              [identity-key :model :chan]
                              #(or % (async/chan (async/sliding-buffer 10))))
        ch        (get-in new [identity-key :model :chan])]
    (when-not (get-in old [identity-key :model :chan])
      (async/sub dataset/publisher :model/deployed ch))
    ch))

(defn subscription-status
  "Current subscriptions for identity, grouped by type — an exact echo of what
   the client submitted, name lists sorted for stability."
  [identity-key]
  (let [sub            (get @subscriptions-state identity-key)
        data-state     (:data sub)
        entity-state   (:entity sub)
        relation-state (:relation sub)
        data-sub       (when (seq (:records data-state))
                         (cond-> {:type "data"
                                  :records (vec (:records data-state))}
                           (seq (:operations data-state))
                           (assoc :operations (vec (:operations data-state)))))
        entity-sub     (when (seq (:name-by-xid entity-state))
                         (cond-> {:type "entity"
                                  :entities (vec (sort (vals (:name-by-xid entity-state))))}
                           (seq (:ops entity-state))
                           (assoc :operations (mapv name (:ops entity-state)))))
        relation-sub   (when (seq (:name-by-xid relation-state))
                         (cond-> {:type "relation"
                                  :relations (vec (sort (vals (:name-by-xid relation-state))))}
                           (seq (:ops relation-state))
                           (assoc :operations (mapv name (:ops relation-state)))))
        model-subs     (for [t (get-in sub [:model :types])] {:type t})]
    {:subscriptions (vec (cond->> model-subs
                           data-sub     (cons data-sub)
                           entity-sub   (cons entity-sub)
                           relation-sub (cons relation-sub)))}))

;;; ============================================================================
;;; Delta → Event Translation
;;; ============================================================================

(declare destroy-event-stream)

(def ^:private stream-max-lifetime-ms
  "Hard lifetime for one SSE stream; `SYNTHIGY_SERVER_SSE_MAX_LIFETIME_MS` overrides, 0 disables."
  (let [default 900000
        raw     (env :synthigy-server-sse-max-lifetime-ms)]
    (if (nil? raw)
      default
      ;; never let a malformed value reach `pos?` — it throws at ns load
      (or (parse-long (str/trim (str raw)))
          (do (log/warn {:id ::bad-sse-lifetime
                         :data {:action :ignored :subject :sse-connection
                                :value (str raw) :fallback default}}
                        "SYNTHIGY_SERVER_SSE_MAX_LIFETIME_MS is not a number; using default")
              default)))))

(defn create-event-stream
  "Create a new SSE output channel for an identity."
  [identity-key]
  (let [ch (async/chan (async/sliding-buffer 50))
        [user-xid client-id] identity-key
        stream-count (inc (count (get @event-streams identity-key)))]
    (swap! event-streams update identity-key (fnil conj #{}) ch)
    (when (pos? stream-max-lifetime-ms)
      (async/go
        (async/<! (async/timeout stream-max-lifetime-ms))
        (when (contains? (get @event-streams identity-key) ch)
          (log/debug {:id ::sse-expired
                      :user-xid user-xid
                      :data {:action :expired :subject :sse-connection
                             :client client-id}}
                     "SSE stream hit max lifetime; closing so the client reconnects")
          (destroy-event-stream identity-key ch))))
    (traffic/count! :sse)
    (log/debug {:id ::sse-opened
               :user-xid user-xid
               :data {:action :opened
                      :subject :sse-connection
                      :client client-id
                      :stream-count stream-count}}
              "SSE connection opened")
    ch))

(defn destroy-event-stream
  "Remove an SSE output channel; when the last stream for an identity closes,
   immediately tear the whole session down."
  [identity-key stream-chan]
  (async/close! stream-chan)
  (swap! event-streams update identity-key disj stream-chan)
  (let [[user-xid client-id] identity-key
        remaining (count (get @event-streams identity-key))
        session-final? (zero? remaining)]
    (log/debug {:id ::sse-closed
               :user-xid user-xid
               :data {:action :closed
                      :subject :sse-connection
                      :client client-id
                      :stream-count remaining
                      :session-final? session-final?}}
              "SSE connection closed")
    (when session-final?
      (swap! event-streams dissoc identity-key)
      (cleanup-identity! identity-key))))

(defn fan-out!
  "Send events to all active SSE streams for an identity via offer! (never
   blocks); detects and removes closed channels."
  [identity-key events]
  (when-let [streams (seq (get @event-streams identity-key))]
    (let [[user-xid client-id] identity-key]
      (log/debug {:id ::sse-pushed
                  :user-xid user-xid
                  :data {:action :pushed
                         :subject :sse-payload
                         :client client-id
                         :stream-count (count streams)
                         :event-count (count events)}}
                 "SSE payload pushed"))
    (doseq [stream streams]
      (when (some #(nil? (async/offer! stream %)) events)
        (destroy-event-stream identity-key stream)))))

(defn ensure-delta-loop!
  "Per-identity go-loop translating batched delta envelopes into SSE events under the principal's roles."
  [identity-key principal]
  (when (claim-loop! identity-key :data)
    (let [batch-chan (get-in @subscriptions-state [identity-key :data :batch-chan])
          roles      (access/project-role-xids principal)]
      (async/go-loop []
        (if-let [first-env (async/<! batch-chan)]
          (let [batch (loop [batch [first-env]]
                        (let [[val port] (async/alts! [batch-chan (async/timeout 100)])]
                          (if (and val (= port batch-chan))
                            (recur (conj batch val))
                            batch)))
                replay       (data-resync-envelopes identity-key)
                all          (if (seq replay)
                               (->> (concat replay batch)
                                    (reduce (fn [m e] (assoc m (:seq e) e)) (sorted-map))
                                    vals vec)
                               batch)
                attr-key-idx (sql-query/attribute-key-index)
                records      (get-in @subscriptions-state [identity-key :data :records])
                events       (into []
                                   (mapcat #(trans/translate-delta % attr-key-idx records roles))
                                   all)
                last-atom    (get-in @subscriptions-state [identity-key :data :last-seq])
                mx           (reduce max 0 (keep :seq all))]
            (when (and last-atom (pos? mx))
              (swap! last-atom max mx))
            (when (seq events)
              (fan-out! identity-key events))
            (recur))
          (end-loop! identity-key :data))))))

(defn ensure-model-loop!
  "Model event go-loop for an identity; closes over the principal for per-user
   model filtering."
  [identity-key principal]
  (when (claim-loop! identity-key :model)
    (let [model-chan (get-in @subscriptions-state [identity-key :model :chan])
          superuser? (access/with-principal principal
                       (daccess/superuser?))
          fingerprint (atom (when-not superuser?
                              (model-fingerprint (dataset/deployed-model) principal)))]
      (async/go-loop []
        (if-let [{:keys [model]} (async/<! model-chan)]
          (let [notify? (if superuser?
                          true
                          (let [new-fp (model-fingerprint model principal)
                                old-fp @fingerprint]
                            (when (not= new-fp old-fp)
                              (reset! fingerprint new-fp)
                              true)))]
            (when notify?
              (let [types (get-in @subscriptions-state [identity-key :model :types])
                    ts    (str (Instant/now))]
                (when (seq types)
                  (fan-out! identity-key
                            (mapv (fn [t] {:event t
                                           :data {:action "deploy"
                                                  :timestamp ts}})
                                  types)))))
            (recur))
          (end-loop! identity-key :model))))))

(comment
  (require '[synthigy.transit :refer [->transit]])
  (fan-out!
   ["admin" "SYNTHIGYCOMPONENTSPUBLICCLIENTFOROAUTHFLOWPOPUPWIN"]
   [{:event "deployed-model"
     :data {:action "deploy"
            :timestamp (str (Instant/now))
            :model (->transit (dataset/deployed-model))}}]))

(defn cleanup-identity!
  "Tear down every resource owned by identity-key — delta subscriptions, batch +
   model channels, open SSE streams, loop state."
  [identity-key]
  (delta/unsubscribe! (data-sub-key     identity-key))
  (delta/unsubscribe! (entity-sub-key   identity-key))
  (delta/unsubscribe! (relation-sub-key identity-key))
  (doseq [track-key [:data :entity :relation]]
    (when-let [batch-chan (get-in @subscriptions-state [identity-key track-key :batch-chan])]
      (async/close! batch-chan)))
  (when-let [model-chan (get-in @subscriptions-state [identity-key :model :chan])]
    (async/unsub dataset/publisher :model/deployed model-chan)
    (async/close! model-chan))
  (doseq [stream (get @event-streams identity-key)]
    (async/close! stream))
  (swap! subscriptions-state dissoc identity-key)
  (swap! event-streams       dissoc identity-key)
  (swap! loops               dissoc identity-key))

(defn cleanup-all! []
  (doseq [identity-key (keys @subscriptions-state)]
    (cleanup-identity! identity-key)))

(defn set-handler
  "Ring handler for POST /data/subscription/set — replaces the full subscription
   state for the request's identity."
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (-> (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
          (assoc-in [:headers "WWW-Authenticate"] (auth/challenge request)))
      (let [body (data/parse-request-body request)]
        (if-not body
          (data/json-response 400 {:error {:message "Invalid or missing JSON body"
                                           :code "INVALID_BODY"}})
          (try
            (access/with-principal (:principal iam)
              (data/json-response 200 (set-subscriptions! (request-identity request iam) body (:principal iam))))
            (catch clojure.lang.ExceptionInfo e
              (let [code (:code (ex-data e))]
                (data/json-response (case code
                                      "FORBIDDEN" 403
                                      400)
                                    {:error {:message (ex-message e) :code code}})))))))))

(defn status-handler
  "Ring handler for GET /data/subscription/status"
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (-> (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
          (assoc-in [:headers "WWW-Authenticate"] (auth/challenge request)))
      (data/json-response 200 (subscription-status (request-identity request iam))))))

(defn format-sse [{:keys [event data]}]
  (str "event: " event "\n"
       "data: " (json/write-str data) "\n\n"))

(def keepalive-msg ": keepalive\n\n")

(lifecycle/register-module!
 :synthigy/subscriptions
 ;; never drop :synthigy/plug — it runs the delta drainer; without it subscribers silently get nothing
 {:depends-on [:synthigy/dataset :synthigy/plug]
  :doc "Live /data subscriptions — record/stats deltas over SSE"
  :start (fn []
           (log/info {:id ::lifecycle-start
                      :data {:action :starting :subject :subscriptions}}
                     "Starting subscription system"))
  :stop  (fn []
           (log/info {:id ::lifecycle-stop
                      :data {:action :stopping :subject :subscriptions}}
                     "Stopping subscription system")
           (cleanup-all!)
           (log/info {:id ::lifecycle-stopped
                      :data {:action :stopped :subject :subscriptions}}
                     "All subscriptions cleaned up"))})
