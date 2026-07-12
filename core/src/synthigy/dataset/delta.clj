(ns synthigy.dataset.delta
  "Single delta pipeline — the trigger substrate is the only source of
   envelopes; every change becomes one envelope; subscribers register
   declarative interest and receive matching envelopes through a per-
   subscriber sliding-buffer channel + handler go-loop.

   Producer:
     (dispatch! envelope)            ; drainer calls this post-commit

   Consumers:
     (subscribe!   key interest handler)   ; key is `=`-comparable; replaces prior
     (unsubscribe! key)                    ; no-op if unknown
     (subscriptions)                       ; diagnostic — set of keys

   Lifecycle:
     (init!)                         ; called by :synthigy/dataset start
     (shutdown!)                     ; tears down registry + channels
     (ready?)                        ; true between init! and shutdown!

   Interest descriptor — a map of sets and scalars; missing/empty key is
   a wildcard for that dimension. A descriptor opts into the entity track
   by setting any of `:entity-xids` / `:record-xids` / `:attribute-xids`,
   and into the relation track by setting any of `:relation-xids` /
   `:from-xids` / `:to-xids`. Both tracks can be active in one descriptor.

     {;; Entity-track narrowing — empty/absent ⇒ no narrowing
      :entity-xids    #{X Y}    ; match envelope :data :entity-xid
      :record-xids    #{...}    ; match envelope :data :record-xid
      :attribute-xids #{...}    ; any attribute in this set was touched
                                ; (optional — most consumers don't care)

      ;; Relation-track narrowing
      :relation-xids  #{A B}    ; match envelope :element
      :from-xids      #{...}
      :to-xids        #{...}
      :endpoint-xids  #{...}    ; OR-match on from-xid or to-xid — used
                                ; by the wire's record-scoped subscriptions
                                ; (subscribers don't care which side of the
                                ; relation the record sits on)

      ;; Op narrowing — applies to whichever track the envelope is on
      :ops            #{:insert :update :delete :link :unlink}

      ;; Cross-cutting — apply to either track
      :tenant-xid X
      :scope-xid  X
      :actor-xid  X}

   An empty descriptor `{}` is the firehose — matches every envelope.

   Examples:
     {:entity-xids #{user-xid}}                  ; any change to a User record
     {:entity-xids #{user-xid}
      :ops #{:delete}}                           ; only User deletions
     {:relation-xids #{user-role-xid}}           ; any link/unlink on User-Role
     {:entity-xids #{user-xid role-xid}
      :relation-xids #{user-role-xid}}           ; User OR Role OR User-Role
     {:entity-xids #{user-xid}
      :attribute-xids #{email-attr-xid}}         ; only when User.email changes
     {:tenant-xid t}                             ; firehose, scoped to one tenant"
  (:require
    [clojure.core.async :as async]
    [synthigy.log :as log]))

;;; ===========================================================================
;;; Registry — per-subscriber state
;;; ===========================================================================

;; key → {:interest <map> :handler (fn [env]) :ch <chan> :stop? (atom)}
(defonce ^:private registry (atom {}))

;; Tracks whether `init!` has been called and `shutdown!` hasn't undone it.
;; The registry survives across init/shutdown cycles (defonce); this flag
;; just tells `ready?` what to report.
(defonce ^:private ready (atom false))

(def ^:private default-buffer-size 100)

;;; ===========================================================================
;;; Lifecycle
;;; ===========================================================================

(defn init!
  "Mark the pipe as ready. Called by `synthigy.dataset/start`. Registry
   state survives REPL reloads (defonce), so consumers that subscribed
   before init don't lose their slot."
  []
  (reset! ready true)
  nil)

(defn- close-subscriber!
  [{:keys [ch stop?]}]
  (when stop? (reset! stop? true))
  (when ch    (async/close! ch)))

(defn shutdown!
  "Tear the pipe down — close every subscriber's channel, signal its
   go-loop to exit, drop the registry. Called by `synthigy.dataset/stop`."
  []
  (reset! ready false)
  (doseq [sub (vals @registry)]
    (close-subscriber! sub))
  (reset! registry {})
  nil)

(defn ready?
  "True when the delta pipe is initialized — between dataset start and stop."
  []
  @ready)

;;; ===========================================================================
;;; Interest matching
;;; ===========================================================================

(defn- entity-shape-active?
  "True iff the interest narrows on any entity-side dimension. When false,
   entity envelopes only match a fully-empty interest (firehose)."
  [interest]
  (some #(seq (get interest %)) [:entity-xids :record-xids :attribute-xids]))

(defn- relation-shape-active?
  "True iff the interest narrows on any relation-side dimension."
  [interest]
  (some #(seq (get interest %))
        [:relation-xids :from-xids :to-xids :endpoint-xids]))

(defn- envelope-track
  "Classify envelope by its :delta :type namespace. Returns :entity or
   :relation; nil for unknown shapes."
  [envelope]
  (case (some-> envelope :delta :type namespace)
    "entity"   :entity
    "relation" :relation
    nil))

(defn- envelope-op
  "Unqualified op keyword pulled off the :delta :type — e.g. :insert,
   :update, :delete, :link, :unlink. Matches the `:ops` set in interest
   descriptors."
  [envelope]
  (some-> envelope :delta :type name keyword))

(defn- changed-attribute-xids
  "Attribute xids touched by an entity envelope. For :insert all keys of
   :after; for :update keys of :after whose value differs from :before;
   for :delete the empty set (record-level event)."
  [op data]
  (case op
    :insert (->> (or (:after data) {}) keys (mapv #(if (keyword? %) (name %) %)))
    :update (->> (or (:after data) {})
                 (keep (fn [[k v]] (when (not= v (get (:before data) k)) k)))
                 (mapv #(if (keyword? %) (name %) %)))
    :delete []
    []))

(defn- matches-cross-cutting?
  [{:keys [tenant-xid scope-xid actor-xid]} {:keys [tenant scope actor]}]
  (and (or (nil? tenant-xid) (= tenant-xid tenant))
       (or (nil? scope-xid)  (= scope-xid  scope))
       (or (nil? actor-xid)  (= actor-xid  actor))))

(defn- matches-entity?
  "Apply entity-side narrowing — entity-xids / record-xids / attribute-xids.
   Empty/absent set on a dimension = no narrowing on that dimension. All
   xids are nanoid strings."
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

(defn- matches-relation?
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
  "True iff `envelope` should be delivered to a subscriber holding
   `interest`. Always returns a boolean.

   Shape gating — a subscription opts into a track by setting any
   narrowing field on that side; without those fields, envelopes of
   that track are skipped. An interest with neither side active is
   the firehose (every envelope passes shape gating).

   Cross-cutting filters (`:tenant-xid` / `:scope-xid` / `:actor-xid`)
   layer on top of whatever shape gating applies."
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

;;; ===========================================================================
;;; Subscribe / unsubscribe / dispatch
;;; ===========================================================================

(defn- run-subscriber-loop!
  "Spin a go-loop that pulls envelopes off the subscriber's channel and
   invokes its handler. One slow handler can't backpressure others — the
   channel is a fixed buffer and `dispatch!` uses non-blocking `offer!`,
   so a full buffer drops (and signals `on-drop`) rather than stalling the
   dispatcher. Handler exceptions are caught + logged so one bad subscriber
   doesn't poison its sibling."
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
  "Register `handler` against `interest` under `key`. Returns `key`.
   Re-subscribing under the same key replaces the prior binding (its
   channel is closed, its go-loop exits). Safe to call before `init!`.

   Optional `on-drop` (a 1-arg fn of the rejected envelope) is invoked when
   the subscriber's buffer is full — the channel is a FIXED buffer, so
   `offer!` returns false on overflow (dropping the newest) instead of
   silently evicting the oldest. That false is the only signal a live drop
   happened; consumers use it to trigger a durable resync."
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
     key)))

(defn unsubscribe!
  "Detach the subscription under `key`. Closes its channel + signals the
   go-loop. No-op if unknown."
  [key]
  (when-let [sub (get @registry key)]
    (close-subscriber! sub)
    (swap! registry dissoc key))
  nil)

(defn subscriptions
  "Diagnostic — set of currently-registered subscription keys."
  []
  (set (keys @registry)))

(defn dispatch!
  "Walk the registry; for every subscription whose interest matches
   `envelope`, non-blocking offer onto its channel. The drainer calls
   this once per drained envelope. Safe before `init!` (registry empty
   ⇒ no-op)."
  [envelope]
  (doseq [{:keys [interest ch on-drop]} (vals @registry)]
    (try
      (when (matches? interest envelope)
        ;; Fixed buffer: offer! returns false when full. That's the live-drop
        ;; signal — hand the rejected envelope to on-drop so the subscriber can
        ;; resync from the durable store. No on-drop ⇒ best-effort, drop it.
        (when (and (not (async/offer! ch envelope)) on-drop)
          (on-drop envelope)))
      (catch Throwable e
        (log/error! {:id ::dispatch-match-failed
                     :data {:envelope-type (some-> envelope :delta :type)}
                     :msg "Match threw — interest may be malformed"}
                    e))))
  nil)

