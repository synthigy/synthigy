(ns synthigy.server.subscription
  "Server-agnostic data subscription management.

   Provides REST endpoints for subscribing to data change notifications
   and an SSE event stream for receiving them.

   ## Endpoints

   POST /data/subscription/set     — subscribe to entity/relation changes
   GET  /data/subscription/status  — list active subscriptions
   POST /data/subscription/remove  — unsubscribe
   GET  /data/events               — SSE stream of filtered notifications

   ## Event Model

   Entity events carry IDs:
     event: change
     data: {\"entity\": \"user\", \"ids\": [\"abc\"], \"timestamp\": \"...\"}

   Relation events are blunt (no IDs):
     event: change
     data: {\"entity\": \"user\", \"relation\": \"roles\", \"timestamp\": \"...\"}"
  (:require
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.iam.access :as access]
   [synthigy.json :as json]
   [synthigy.server.auth :as auth]
   [synthigy.server.data :as data]
   [patcho.lifecycle :as lifecycle])
  (:import
   [java.time Instant]))

(declare ensure-delta-loop! cleanup-identity!)

;;; ============================================================================
;;; Configuration
;;; ============================================================================

(def ^:private subscription-ttl-ms
  "Subscriptions without active SSE connections are cleaned up after this duration.
   Default: 30 minutes."
  (* 30 60 1000))

(def ^:private cleanup-interval-ms
  "How often the cleanup loop runs. Default: 5 minutes."
  (* 5 60 1000))

;;; ============================================================================
;;; State
;;; ============================================================================

(defonce ^:private subscriptions-state (atom {}))
(defonce ^:private cleanup-chan (atom nil))

;;; ============================================================================
;;; Identity
;;; ============================================================================

(defn request-identity
  "Extract subscription identity key from authenticated request context.
   Returns [sub client_id] tuple."
  [iam]
  (let [{:keys [sub client_id]} (:claims iam)]
    [(or sub (:user iam)) client_id]))

;;; ============================================================================
;;; Subscription Management
;;; ============================================================================

(defn- ensure-delta-chan
  "Get or create the delta channel for an identity."
  [identity-key]
  (if-let [existing (get-in @subscriptions-state [identity-key :delta-chan])]
    existing
    (let [ch (async/chan (async/sliding-buffer 100))]
      (swap! subscriptions-state assoc-in [identity-key :delta-chan] ch)
      ch)))

(defn- sub-element!
  "Subscribe an element ID to the identity's delta channel."
  [identity-key element-id]
  (let [delta-chan (ensure-delta-chan identity-key)]
    (async/sub core/*delta-publisher* element-id delta-chan)
    (swap! subscriptions-state update-in [identity-key :subscribed-elements]
           (fnil conj #{}) element-id)))

(defn- unsub-element!
  "Unsubscribe an element ID from the identity's delta channel."
  [identity-key element-id]
  (when-let [delta-chan (get-in @subscriptions-state [identity-key :delta-chan])]
    (async/unsub core/*delta-publisher* element-id delta-chan)
    (swap! subscriptions-state update-in [identity-key :subscribed-elements]
           disj element-id)))

(defn- all-entity-relations
  "Get all [label relation-id] pairs for an entity from the relation index."
  [entity-name]
  (for [[rel-id {:keys [entity label]}] (sql-query/relation-index)
        :when (= entity entity-name)]
    [label rel-id]))

(defn subscribe!
  "Process a subscription set request.
   Replaces existing subscription for the entity.

   body: {\"entity\" \"user\"}
         {\"entity\" \"user\" \"relations\" [\"roles\"]}
         {\"entity\" \"user\" \"relations\" \"*\"}"
  [identity-key body]
  (let [entity-name (or (get body "entity") (get body :entity))
        relations-spec (or (get body "relations") (get body :relations))
        operations (when-let [ops (or (get body "operations") (get body :operations))]
                     (set ops))]
    (when-not entity-name
      (throw (ex-info "Missing entity name" {:code "MISSING_ENTITY"})))
    ;; Resolve entity
    (let [entity-id (sql-query/resolve-entity entity-name)]
      ;; RBAC check
      (when-not (or (access/superuser?)
                    (access/entity-allows? entity-id #{:read}))
        (throw (ex-info (str "No read permission on entity: " entity-name)
                        {:code "FORBIDDEN" :entity entity-name})))
      ;; Unsub old entity subscription if exists
      (when-let [old-entity (get-in @subscriptions-state [identity-key :entities entity-name])]
        (unsub-element! identity-key (:entity-id old-entity)))
      ;; Unsub old relation subscriptions for this entity
      (doseq [[_ {:keys [relation-id]}]
              (get-in @subscriptions-state [identity-key :relations entity-name])]
        (unsub-element! identity-key relation-id))
      ;; Subscribe entity
      (sub-element! identity-key entity-id)
      (swap! subscriptions-state assoc-in [identity-key :entities entity-name]
             {:entity-id entity-id :operations operations})
      ;; Subscribe relations if requested
      (let [relation-subs
            (cond
              (= "*" relations-spec)
              (into {}
                    (for [[label rel-id] (all-entity-relations entity-name)]
                      (do (sub-element! identity-key rel-id)
                          [label {:relation-id rel-id}])))

              (sequential? relations-spec)
              (into {}
                    (for [label relations-spec]
                      (let [rel-id (sql-query/resolve-relation entity-name label)]
                        (sub-element! identity-key rel-id)
                        [label {:relation-id rel-id}])))

              :else nil)]
        (if relation-subs
          (swap! subscriptions-state assoc-in [identity-key :relations entity-name]
                 relation-subs)
          (swap! subscriptions-state update-in [identity-key :relations]
                 dissoc entity-name)))
      ;; Start delta processing loop if not already running
      (ensure-delta-loop! identity-key)
      {:ok true})))

(defn unsubscribe!
  "Process a subscription remove request.

   body: {\"entity\" \"user\"}
         {\"entity\" \"user\" \"relations\" [\"roles\"]}"
  [identity-key body]
  (let [entity-name (or (get body "entity") (get body :entity))
        relations-spec (or (get body "relations") (get body :relations))]
    (when-not entity-name
      (throw (ex-info "Missing entity name" {:code "MISSING_ENTITY"})))
    (if relations-spec
      ;; Remove specific relations only
      (let [labels (if (= "*" relations-spec)
                     (keys (get-in @subscriptions-state [identity-key :relations entity-name]))
                     relations-spec)]
        (doseq [label labels]
          (when-let [rel-id (get-in @subscriptions-state
                                      [identity-key :relations entity-name label :relation-id])]
            (unsub-element! identity-key rel-id))
          (swap! subscriptions-state update-in [identity-key :relations entity-name]
                 dissoc label))
        ;; Clean up empty relation map
        (when (empty? (get-in @subscriptions-state [identity-key :relations entity-name]))
          (swap! subscriptions-state update-in [identity-key :relations] dissoc entity-name)))
      ;; Remove entity + all its relations
      (do
        (when-let [{:keys [entity-id]} (get-in @subscriptions-state [identity-key :entities entity-name])]
          (unsub-element! identity-key entity-id))
        (doseq [[_ {:keys [relation-id]}]
                (get-in @subscriptions-state [identity-key :relations entity-name])]
          (unsub-element! identity-key relation-id))
        (swap! subscriptions-state update-in [identity-key :entities] dissoc entity-name)
        (swap! subscriptions-state update-in [identity-key :relations] dissoc entity-name)))
    ;; If no subscriptions remain, clean up
    (when (and (empty? (get-in @subscriptions-state [identity-key :entities]))
               (empty? (get-in @subscriptions-state [identity-key :relations])))
      (cleanup-identity! identity-key))
    {:ok true}))

(defn subscription-status
  "Returns current subscriptions for identity."
  [identity-key]
  (let [sub (get @subscriptions-state identity-key)]
    {:subscriptions
     (vec
      (for [[entity-name {:keys [operations]}] (:entities sub)]
        (cond-> {:entity entity-name}
          operations (assoc :operations (vec operations))
          (get-in sub [:relations entity-name])
          (assoc :relations (vec (keys (get-in sub [:relations entity-name])))))))}))

;;; ============================================================================
;;; Delta → Event Translation
;;; ============================================================================

(defn- extract-ids
  "Extract entity IDs from delta data based on delta type.
   Uses id/extract which returns the current ID format (xid or euuid)."
  [{:keys [type data]}]
  (case type
    :change (vec (keep id/extract data))
    :delete (when-let [id (id/extract data)] [id])
    :purge  (vec (keep id/extract data))
    nil))

(defn- translate-delta
  "Translate a raw delta event into an SSE-friendly map.
   Returns nil if the delta should be filtered out."
  [{:keys [element delta]} entity-index relation-index subscriptions]
  (let [{:keys [type]} delta
        rel-info (get relation-index element)
        entity-info (when-not rel-info
                      ;; Look up entity name from index (reverse of entity-index)
                      (some (fn [[name uuid]]
                              (when (= uuid element) name))
                            entity-index))]
    (cond
      ;; Entity event
      entity-info
      (let [event-type (case type
                         (:change) "change"
                         (:delete :purge) "delete"
                         nil)
            ids (extract-ids delta)]
        (when event-type
          ;; Check operations filter
          (let [ops (get-in subscriptions [:entities entity-info :operations])]
            (when (or (nil? ops) (contains? ops event-type))
              {:event event-type
               :data (cond-> {:entity entity-info
                              :timestamp (str (Instant/now))}
                       (seq ids) (assoc :ids ids))}))))

      ;; Relation event
      rel-info
      (let [{:keys [entity label]} rel-info]
        (when (get-in subscriptions [:relations entity label])
          {:event "change"
           :data {:entity entity
                  :relation label
                  :timestamp (str (Instant/now))}}))

      :else nil)))

(defn- merge-events
  "Merge a batch of translated events. Combines IDs for same entity+event."
  [events]
  (let [grouped (group-by (fn [{:keys [event data]}]
                            [event (:entity data) (:relation data)])
                          events)]
    (for [[_ group] grouped]
      (let [first-event (first group)
            all-ids (into [] (comp (mapcat #(get-in % [:data :ids])) (distinct))
                         group)]
        (if (seq all-ids)
          (assoc-in first-event [:data :ids] all-ids)
          first-event)))))

;;; ============================================================================
;;; Event Streams (SSE Output)
;;; ============================================================================

(defonce ^:private event-streams (atom {}))
;; {identity-key #{<channels>}}

(defn create-event-stream
  "Create a new SSE output channel for an identity.
   Returns the channel."
  [identity-key]
  (let [ch (async/chan (async/sliding-buffer 50))]
    (swap! event-streams update identity-key (fnil conj #{}) ch)
    (swap! subscriptions-state update identity-key dissoc :streams-empty-since)
    ch))

(defn destroy-event-stream
  "Remove an SSE output channel. Marks streams-empty-since when last stream removed."
  [identity-key stream-chan]
  (async/close! stream-chan)
  (swap! event-streams update identity-key disj stream-chan)
  (when (empty? (get @event-streams identity-key))
    (swap! event-streams dissoc identity-key)
    ;; Mark when this identity lost its last SSE connection
    (swap! subscriptions-state assoc-in [identity-key :streams-empty-since]
           (System/currentTimeMillis))))

(defn- fan-out!
  "Send events to all active SSE streams for an identity.
   Detects and removes closed channels.
   Uses offer! which never blocks — sliding buffers drop old values."
  [identity-key events]
  (when-let [streams (seq (get @event-streams identity-key))]
    (doseq [stream streams]
      (when-not (reduce
                  (fn [_ event]
                    (if (some? (async/offer! stream event))
                      true
                      (reduced false)))
                  true
                  events)
        ;; offer! returned nil → channel is closed
        (destroy-event-stream identity-key stream)))))

;;; ============================================================================
;;; Delta Processing Loop
;;; ============================================================================

(defonce ^:private delta-loops (atom #{}))

(defn- ensure-delta-loop!
  "Start the delta processing go-loop for an identity if not running."
  [identity-key]
  (when-not (contains? @delta-loops identity-key)
    (swap! delta-loops conj identity-key)
    (let [delta-chan (get-in @subscriptions-state [identity-key :delta-chan])]
      (async/go-loop []
        (if-let [first-delta (async/<! delta-chan)]
          (let [;; Accumulate batch over 100ms window
                batch (loop [batch [first-delta]]
                        (let [[val port] (async/alts! [delta-chan (async/timeout 100)])]
                          (if (and val (= port delta-chan))
                            (recur (conj batch val))
                            batch)))
                ;; Translate and merge
                entity-idx (sql-query/entity-index)
                relation-idx (sql-query/relation-index)
                subs (get @subscriptions-state identity-key)
                events (->> batch
                            (keep #(translate-delta % entity-idx relation-idx subs))
                            (merge-events))]
            (when (seq events)
              (fan-out! identity-key events))
            (recur))
          ;; Channel closed
          (swap! delta-loops disj identity-key))))))

;;; ============================================================================
;;; Cleanup
;;; ============================================================================

(defn- cleanup-identity!
  "Clean up all resources for an identity."
  [identity-key]
  ;; Close delta channel
  (when-let [delta-chan (get-in @subscriptions-state [identity-key :delta-chan])]
    ;; Unsub all elements first
    (doseq [element (get-in @subscriptions-state [identity-key :subscribed-elements])]
      (async/unsub core/*delta-publisher* element delta-chan))
    (async/close! delta-chan))
  ;; Close all SSE streams
  (doseq [stream (get @event-streams identity-key)]
    (async/close! stream))
  ;; Remove from atoms
  (swap! subscriptions-state dissoc identity-key)
  (swap! event-streams dissoc identity-key)
  (swap! delta-loops disj identity-key))

(defn cleanup-all!
  "Clean up all subscriptions and streams."
  []
  (doseq [identity-key (keys @subscriptions-state)]
    (cleanup-identity! identity-key)))

(defn- cleanup-stale!
  "Remove subscriptions that have no active SSE streams and have been
   disconnected longer than the TTL."
  []
  (let [now (System/currentTimeMillis)
        stale (for [[identity-key sub] @subscriptions-state
                    :let [empty-since (:streams-empty-since sub)]
                    :when (and empty-since
                               (> (- now empty-since) subscription-ttl-ms))]
                identity-key)]
    (when (seq stale)
      (log/infof "[SUBSCRIPTIONS] Cleaning up %d stale subscriptions" (count stale))
      (doseq [identity-key stale]
        (cleanup-identity! identity-key)))))

(defn- start-cleanup-loop!
  "Start periodic cleanup of stale subscriptions."
  []
  (let [ch (async/chan)]
    (reset! cleanup-chan ch)
    (async/go-loop []
      (let [[_ port] (async/alts! [ch (async/timeout cleanup-interval-ms)])]
        (when-not (= port ch)
          (try
            (cleanup-stale!)
            (catch Exception e
              (log/error e "[SUBSCRIPTIONS] Error during cleanup")))
          (recur))))
    (log/infof "[SUBSCRIPTIONS] Cleanup loop started (interval: %ds, TTL: %ds)"
               (quot cleanup-interval-ms 1000)
               (quot subscription-ttl-ms 1000))))

(defn- stop-cleanup-loop!
  "Stop the periodic cleanup loop."
  []
  (when-let [ch @cleanup-chan]
    (async/close! ch)
    (reset! cleanup-chan nil)))

;;; ============================================================================
;;; Ring Handlers
;;; ============================================================================


(defn set-handler
  "Ring handler for POST /data/subscription/set"
  [request]
  (let [iam (auth/authenticate-request request)]
    (if-not iam
      (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (let [body (data/parse-request-body request)]
        (if-not body
          (data/json-response 400 {:error {:message "Invalid or missing JSON body"
                                      :code "INVALID_BODY"}})
          (try
            (binding [access/*user* (:user iam)
                      access/*roles* (:roles iam)
                      access/*groups* (:groups iam)]
              (data/json-response 200 (subscribe! (request-identity iam) body)))
            (catch clojure.lang.ExceptionInfo e
              (let [code (:code (ex-data e))]
                (data/json-response (if (= code "FORBIDDEN") 403 400)
                               {:error {:message (ex-message e) :code code}})))))))))

(defn status-handler
  "Ring handler for GET /data/subscription/status"
  [request]
  (let [iam (auth/authenticate-request request)]
    (if-not iam
      (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (data/json-response 200 (subscription-status (request-identity iam))))))

(defn remove-handler
  "Ring handler for POST /data/subscription/remove"
  [request]
  (let [iam (auth/authenticate-request request)]
    (if-not iam
      (data/json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (let [body (data/parse-request-body request)]
        (if-not body
          (data/json-response 400 {:error {:message "Invalid or missing JSON body"
                                      :code "INVALID_BODY"}})
          (try
            (data/json-response 200 (unsubscribe! (request-identity iam) body))
            (catch clojure.lang.ExceptionInfo e
              (data/json-response 400 {:error {:message (ex-message e)
                                          :code (:code (ex-data e))}}))))))))

;;; ============================================================================
;;; SSE Format
;;; ============================================================================

(defn format-sse
  "Format an event map as SSE text."
  [{:keys [event data]}]
  (str "event: " event "\n"
       "data: " (json/write-str data) "\n\n"))

(def keepalive-msg ": keepalive\n\n")

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/subscriptions
  {:depends-on [:synthigy/dataset :synthigy/iam]
   :start (fn []
            (log/info "[SUBSCRIPTIONS] Starting subscription system")
            (start-cleanup-loop!))
   :stop (fn []
           (log/info "[SUBSCRIPTIONS] Stopping subscription system")
           (stop-cleanup-loop!)
           (cleanup-all!)
           (log/info "[SUBSCRIPTIONS] All subscriptions cleaned up"))})
