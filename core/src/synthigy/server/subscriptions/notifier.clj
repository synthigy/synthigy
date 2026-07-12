(ns synthigy.server.subscriptions.notifier
  "Best-effort live notification fan-out to connected SSE subscribers.
   Backend-agnostic — the HTTP layer (httpkit / pedestal / ring-jetty /
   undertow) supplies a per-connection `emit` callback that ships pre-
   formatted SSE strings to its wire.

   This namespace is now a thin wrapper over `synthigy.dataset.delta`:
   `register!` translates a declared per-connection interest into one or
   more `delta/subscribe!` registrations (one for the entity track, one
   per relation-xid the connection cares about) — each with the same
   emit handler. `unregister!` walks the set of delta keys it created
   and tears them all down.

   See `project_subscriptions_substrate.md` — best-effort, no replay
   protocol, thin xid-keyed wire (no scalar values cross the boundary)."
  (:require
    [clojure.string :as str]
    [synthigy.audit :as audit]
    [synthigy.dataset.delta :as delta]
    [synthigy.json :as json]
    [synthigy.log :as log]))

;; ============================================================================
;; Request parsing — declared interest + replay cursor. Backend-agnostic
;; (pure functions of a Ring request map), shared by every /subscribe
;; handler (httpkit / ring-jetty / undertow) so the query-param contract
;; can't drift across backends.
;; ============================================================================

(defn parse-csv [s]
  (when (seq s) (vec (remove str/blank? (str/split s #",")))))

(defn parse-declared-interest
  "Declared interest is passed as query params on the /subscribe request:
   entity_xid=ex_user&record_xids=rcx_a,rcx_b&attribute_xids=...&relation_xids=..."
  [request]
  (let [qp (or (:query-params request) {})
        eid (get qp "entity_xid")]
    {:entity-xid     (cond
                       (or (nil? eid) (= eid "") (= eid ":any") (= eid "any")) :any
                       :else eid)
     :record-xids    (parse-csv (get qp "record_xids"))
     :attribute-xids (parse-csv (get qp "attribute_xids"))
     :relation-xids  (parse-csv (get qp "relation_xids"))}))

(defn parse-replay-cursor
  "Resolve the backfill start `:seq` from the request, or nil for live-only.
   `?from=<seq>` is the explicit \"give me everything after N\" (0 = full
   history); the browser's `Last-Event-ID` header carries the same number
   automatically on reconnect. Absent / non-numeric → nil (no backfill)."
  [request]
  (some-> (or (not-empty (get-in request [:query-params "from"]))
              (not-empty (get-in request [:headers "last-event-id"])))
          (as-> s (try (Long/parseLong (.trim ^String s)) (catch Exception _ nil)))))

;; replay-and-emit!/maybe-resync! are defined lower (they need the wire +
;; reconstruction helpers); the handlers below reference them at call time.
(declare replay-and-emit! maybe-resync!)

;; A live drop flags the connection; the next delivered envelope on either
;; track heals it in place. Bounded so a persistently-slow client can't loop
;; forever — past the budget we degrade to best-effort (it must reconnect).
(def ^:private max-resyncs 5)

(defn- new-conn-state []
  {:lagged  (atom false)   ; set by on-drop (producer thread)
   :last-seq (atom 0)      ; highest seq emitted to the wire
   :resyncs (atom 0)})

;; ============================================================================
;; Envelope → wire event
;; ============================================================================

(defn- changed-attribute-xids
  [op {:keys [before after]}]
  (case op
    "insert" (->> (or after {}) keys (mapv #(if (keyword? %) (name %) %)))
    "update" (->> (or after {})
                  (keep (fn [[k v]] (when (not= v (get before k)) k)))
                  (mapv #(if (keyword? %) (name %) %)))
    "delete" []
    []))

(defn- entity-envelope->wire
  [{:keys [delta seq]}]
  (let [op (-> delta :type name)
        d  (:data delta)]
    {:stream "entity"
     :seq seq
     :ts (:ts d)
     :op op
     :tenant_xid (some-> d :tenant str)
     :entity_xid (some-> d :entity-xid str)
     :record_xid (some-> d :record-xid str)
     :attributes_changed (changed-attribute-xids op d)
     :scope_xid (some-> d :scope str)}))

(defn- relation-envelope->wire
  [{:keys [delta element seq]}]
  (let [op (-> delta :type name)
        d  (:data delta)]
    {:stream "relation"
     :seq seq
     :ts (:ts d)
     :op op
     :tenant_xid (some-> d :tenant str)
     :relation_xid (some-> element str)
     :from_xid (some-> d :from-xid str)
     :to_xid (some-> d :to-xid str)
     :scope_xid (some-> d :scope str)}))

(defn format-sse-event
  "Format a thin wire event as an SSE event. Emits the delta `:seq` as the
   SSE `id:` line so browsers resend it as `Last-Event-ID` on reconnect (the
   replay cursor), with the same value also inside the JSON for non-browser
   clients."
  [wire]
  (str (when-let [s (:seq wire)] (str "id: " s "\n"))
       "data: " (json/->json wire) "\n\n"))

;; ============================================================================
;; Connection registry — tracks the delta-subscription keys owned by each
;; conn-id so `unregister!` can tear them down in bulk.
;; ============================================================================

;; { conn-id → #{delta-key, ...} }
(defonce ^:private connection-keys (atom {}))

(defn connection-count [] (count @connection-keys))

(defn- entity-interest
  "Project the entity-shape fields out of a declared connection interest.
   Returns nil when none of the entity-narrowing fields are present (i.e.
   this connection isn't interested in entity events). Coerces the
   legacy `:entity-xid` singular into the new `:entity-xids` set."
  [{:keys [entity-xid entity-xids record-xids attribute-xids]}]
  (let [xids (cond-> (set (or entity-xids #{}))
               entity-xid (conj entity-xid))]
    (when (or (seq xids) (seq record-xids) (seq attribute-xids))
      (cond-> {}
        (seq xids)           (assoc :entity-xids xids)
        (seq record-xids)    (assoc :record-xids record-xids)
        (seq attribute-xids) (assoc :attribute-xids attribute-xids)))))

(defn- entity-handler
  "Handler invoked with the raw entity envelope; heals any pending live drop
   (maybe-resync!), then shapes the envelope to SSE wire + emits and advances
   the connection's emitted-seq cursor. Wrapped in try/catch so a write to a
   dead socket doesn't poison the delta dispatcher. `interest` is the FULL
   declared interest (both tracks) used for resync filtering."
  [conn-id interest emit conn]
  (fn [envelope]
    (maybe-resync! conn interest emit)
    (try
      (emit (format-sse-event (entity-envelope->wire envelope)))
      (swap! (:last-seq conn) max (or (:seq envelope) 0))
      (catch Throwable e
        (log/error! {:id ::emit-failed
                     :data {:conn-id conn-id :stream "entity"}
                     :msg "SSE emit threw"}
                    e)))))

(defn- relation-handler
  [conn-id interest emit conn]
  (fn [envelope]
    (maybe-resync! conn interest emit)
    (try
      (emit (format-sse-event (relation-envelope->wire envelope)))
      (swap! (:last-seq conn) max (or (:seq envelope) 0))
      (catch Throwable e
        (log/error! {:id ::emit-failed
                     :data {:conn-id conn-id :stream "relation"}
                     :msg "SSE emit threw"}
                    e)))))

(defn register!
  "Register an SSE connection. `interest` is the declared-interest map
   (same shape as before this migration):

     {:entity-xid     X              ; or :any; nil means no entity interest
      :record-xids    #{...}
      :attribute-xids #{...}
      :relation-xids  #{...}}        ; set; each becomes one delta subscription

   `emit` is a side-effecting fn taking the SSE-formatted string to
   write to the wire. Returns `conn-id`. The notifier translates this
   into 0-or-more `delta/subscribe!` registrations and remembers the
   keys for later teardown via `unregister!`."
  [conn-id interest emit]
  (let [conn         (new-conn-state)
        ;; A drop on EITHER track flags the connection; the next delivered
        ;; envelope heals it via a store replay covering both tracks.
        on-drop      (fn [_env] (reset! (:lagged conn) true))
        ent-interest (entity-interest interest)
        rel-xids     (when (seq (:relation-xids interest))
                       (set (:relation-xids interest)))
        keys-created (cond-> #{}
                       ent-interest
                       (conj (delta/subscribe! [::entity conn-id]
                                               ent-interest
                                               (entity-handler conn-id interest emit conn)
                                               on-drop))

                       (seq rel-xids)
                       (conj (delta/subscribe! [::relation conn-id]
                                               {:relation-xids rel-xids}
                                               (relation-handler conn-id interest emit conn)
                                               on-drop)))]
    (swap! connection-keys assoc conn-id keys-created)
    conn-id))

(defn unregister!
  "Tear down every delta subscription owned by `conn-id`. No-op if
   unknown."
  [conn-id]
  (doseq [k (get @connection-keys conn-id)]
    (delta/unsubscribe! k))
  (swap! connection-keys dissoc conn-id)
  nil)

;; ============================================================================
;; Replay / backfill — reconstruct delta envelopes from the audit store so a
;; reconnecting subscriber recovers the EXACT events it missed (everything
;; after its last SSE `id:`/`:seq`). Reconstructs ENVELOPES (not wire), so the
;; SAME `delta/matches?` and the SAME `entity-interest` translation the live
;; path uses apply unchanged — backfill visibility is identical to live by
;; construction, with no parallel matcher and no extra leak surface.
;; ============================================================================

(defn- rows->entity-envelopes
  "Audit rows are attribute-grain (one per changed field); regroup by `:seq`
   (one delta = one seq) into entity envelopes shaped exactly like live ones.
   `:after` is rebuilt from the row attribute/value pairs so the
   `:attribute-xids` narrowing in `delta/matches?` works."
  [rows]
  (->> rows
       (group-by :seq)
       (map (fn [[sq grp]]
              (let [r     (first grp)
                    after (into {} (for [{a :attribute-xid v :value} grp
                                         :when (not= "__delete__" a)]
                                     [a v]))]
                {:seq   sq
                 :delta {:type (keyword "entity" (:op r))
                         :data {:ts         (:ts r)
                                :record-xid (:record-xid r)
                                :entity-xid (:entity-xid r)
                                :tenant     (:tenant-xid r)
                                :scope      (:scope r)
                                :actor      (:actor r)
                                :request    (:request r)
                                :txid       (:txid r)
                                :before     {}
                                :after      after}}})))))

(defn- rows->relation-envelopes
  "Relation audit rows are already delta-grain (one row per link/unlink)."
  [rows]
  (map (fn [r]
         {:seq     (:seq r)
          :element (:relation-xid r)
          :delta   {:type (keyword "relation" (:op r))
                    :data {:ts       (:ts r)
                           :from-xid (:from-xid r)
                           :to-xid   (:to-xid r)
                           :tenant   (:tenant-xid r)
                           :scope    (:scope r)
                           :actor    (:actor r)
                           :request  (:request r)
                           :txid     (:txid r)}}})
       rows))

(defn replay-envelopes
  "Reconstruct delta envelopes with `:seq` > `cursor-seq`, oldest-first, from
   the audit provider (both tracks, merged by seq). Empty when no provider is
   bound. `limit` bounds each track's scan. Public so other SSE delivery paths
   (e.g. `server.subscription`'s record-scoped /data/events) can replay the
   same reconstructed envelopes through their own translate/fan-out."
  [cursor-seq limit]
  (if-let [provider audit/*audit-provider*]
    (let [base (cond-> {:seq cursor-seq} limit (assoc :limit limit))
          ent  (rows->entity-envelopes   (audit/since provider (assoc base :track :entity)))
          rel  (rows->relation-envelopes (audit/since provider (assoc base :track :relation)))]
      (sort-by :seq (concat ent rel)))
    []))

(defn replay-and-emit!
  "Backfill a reconnecting connection: replay every envelope after `cursor-seq`
   that matches this connection's declared `interest`, formatted + emitted in
   seq order — call BEFORE `register!`. Filtering reuses the live
   `delta/matches?` with the entity/relation interests derived exactly as
   `register!` does, so a client sees on reconnect precisely what it would have
   seen live (a few events may duplicate across the live seam; clients dedup by
   `:seq`). Returns the highest `:seq` emitted, or `cursor-seq` if none."
  [interest cursor-seq emit {:keys [limit] :or {limit 10000}}]
  (let [ent-interest (entity-interest interest)
        rel-xids     (when (seq (:relation-xids interest)) (set (:relation-xids interest)))
        rel-interest (when rel-xids {:relation-xids rel-xids})]
    (reduce
      (fn [hi env]
        (let [entity? (= "entity" (some-> env :delta :type namespace))
              keep?   (if entity?
                        (and ent-interest (delta/matches? ent-interest env))
                        (and rel-interest (delta/matches? rel-interest env)))]
          (if keep?
            (do
              (try
                (emit (format-sse-event (if entity?
                                          (entity-envelope->wire env)
                                          (relation-envelope->wire env))))
                (catch Throwable e
                  (log/error! {:id ::replay-emit-failed
                               :data {:stream (if entity? "entity" "relation")
                                      :seq (:seq env)}
                               :msg "SSE replay emit threw"}
                              e)))
              ;; high-water tracks what we actually EMITTED — that's the seq
              ;; the client's last SSE `id:` (its resume cursor) will carry.
              (max hi (or (:seq env) hi)))
            hi)))
      cursor-seq
      (replay-envelopes cursor-seq limit))))

(defn- maybe-resync!
  "Heal a connection flagged `:lagged` by a live drop: replay every matching
   event after the last-emitted seq from the durable store, in place, so the
   client catches up WITHOUT a reconnect. `compare-and-set!` so only one
   track's go-loop runs the resync; it covers both tracks via
   `replay-and-emit!`. Bounded by `max-resyncs` — past the budget we clear the
   flag and degrade to best-effort (a persistently-slow client must reconnect
   to fully catch up). Runs on the delivering go-loop thread, so its emits are
   serialized with normal ones (no interleaving on the wire). Clients dedup the
   replay/live overlap by `:seq`."
  [{:keys [lagged last-seq resyncs]} interest emit]
  (when (compare-and-set! lagged true false)
    (if (< @resyncs max-resyncs)
      (let [from @last-seq
            hi   (replay-and-emit! interest from emit {})]
        (swap! resyncs inc)
        (swap! last-seq max hi)
        (log/debug {:id ::resync
                    :data {:action :recovering :subject :delta
                           :from from :resync-count @resyncs}}
                   "Healed live delta drop via durable store replay"))
      (log/warn {:id ::resync-budget-exhausted
                 :data {:action :recovering :subject :delta}}
                "Resync budget exhausted; degrading to best-effort (client should reconnect)"))))
