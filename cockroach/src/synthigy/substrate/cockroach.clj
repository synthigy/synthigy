(ns synthigy.substrate.cockroach
  "CockroachDB-side delta substrate — DDL, trigger generation, drainer thread.

   Owned by `:synthigy/subscriptions`. Mirrors
   `synthigy.substrate.sqlite` in shape (no LISTEN/NOTIFY, polling drainer)
   but rides PG-wire DDL primitives: jsonb_build_object, plpgsql functions,
   session GUCs.

   CRDB-specific deltas vs SQLite:
     - jsonb_build_object instead of json_object
     - One plpgsql function per (entity, op) and per (relation, op),
       called by a row-level trigger (CRDB triggers can't be inline-BEGIN
       like SQLite — they must call a function — and CRDB doesn't support
       statement-level triggers + transition tables like Postgres does)
     - Context propagation via PG-style session GUCs
       (`SET LOCAL synthigy.actor_xid = '…'`) read by trigger fns via
       `NULLIF(current_setting('synthigy.actor_xid', true), '')`. No
       `_ctx` table — CRDB supports custom GUC namespaces unchanged.
     - `(OLD).col IS DISTINCT FROM (NEW).col` in WHEN clauses (CRDB
       plpgsql can't reference bare `NEW.field` — issue 114687)
     - Trigger discovery via `information_schema.triggers` instead of
       `sqlite_master`. Function discovery via `pg_proc`.
     - `UNLOGGED TABLE` + `BIGSERIAL` queue (BIGSERIAL on CRDB returns
       `unique_rowid()` — monotonic-ish, large gaps, ORDER BY still works).

   CRDB-specific deltas vs Postgres:
     - No statement-level triggers + transition tables (issue 126362) —
       per-row inline triggers (one fn + trigger per (table, op))
     - No `pg_notify` / LISTEN — polling drainer with in-process
       `*drain-wakeup*` for app-path wake
     - No `txid_current()` — synthesized UUIDv7 in set-context!"
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [synthigy.audit :as audit]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.delta :as delta]
    [synthigy.dataset.id :as id]
    [synthigy.json :refer [<-json]]
    [synthigy.log :as log]
    [synthigy.substrate.wake :as wake]))

;; ============================================================================
;; Wake source — delegate to shared `synthigy.substrate.wake/*wake-source*`.
;;
;; The CRDB default (LocalChannel) is installed by
;; `:synthigy/subscriptions` :start. Operators override by
;; alter-var-root'ing `wake/*wake-source*` before starting subscriptions
;; (e.g. to a Composite of LocalChannel + NATS for multi-node fanout).
;; ============================================================================

(defn wake-drainer!
  "Backend-named alias for `synthigy.substrate.wake/signal-drainer!`.
   Kept so query.cockroach call sites don't have to know they're
   delegating. App-path writes call this post-commit."
  []
  (wake/signal-drainer!))

;; ============================================================================
;; Queue DDL
;; ============================================================================

(def ^:private delta-queue-ddl
  "CREATE UNLOGGED TABLE IF NOT EXISTS __relation_delta_queue (
     id      BIGSERIAL PRIMARY KEY,
     ts      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
     kind    TEXT NOT NULL,
     payload JSONB NOT NULL
   )")

(def ^:private entity-queue-ddl
  "CREATE UNLOGGED TABLE IF NOT EXISTS __entity_delta_queue (
     id      BIGSERIAL PRIMARY KEY,
     ts      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
     payload JSONB NOT NULL
   )")

;; ============================================================================
;; Helpers — UUIDv7 + trigger/function discovery
;; ============================================================================

(defn- uuid-v7
  "Generate a UUIDv7. CRDB has no txid_current(); we synthesize a
   chronologically-sortable id so envelopes can be grouped/ordered without
   a separate `ts` column. Same impl as the SQLite substrate."
  []
  (let [ts (System/currentTimeMillis)
        rnd (java.security.SecureRandom.)
        rand-12 (bit-and (.nextLong rnd) 0xFFF)
        rand-62 (bit-and (.nextLong rnd) 0x3FFFFFFFFFFFFFFF)
        msb (bit-or (bit-shift-left ts 16) 0x7000 rand-12)
        lsb (bit-or (unchecked-long 0x8000000000000000) rand-62)]
    (java.util.UUID. msb lsb)))

(def ^:private trigger-name-patterns
  ["trg\\_%\\_entity\\_audit\\_%"
   "trg\\_%\\_audit\\_insert"
   "trg\\_%\\_audit\\_delete"])

(defn- strip-quotes
  "CRDB v25 SHOW TRIGGERS returns trigger_name with literal surrounding
   double-quotes baked into the string (e.g. `\"trg_…_insert\"` — 41 chars,
   not 39). Strip them for matching and DROP."
  [^String s]
  (when s
    (cond-> s
      (and (.startsWith s "\"") (.endsWith s "\"") (>= (count s) 2))
      (subs 1 (dec (count s))))))

(defn- substrate-trigger?
  "Match our naming convention. Used to filter SHOW TRIGGERS output
   (after stripping the embedded quotes CRDB returns)."
  [trigger-name]
  (let [n (strip-quotes (str trigger-name))]
    (boolean
      (or (re-find #"^trg_.+_entity_audit_(insert|update|delete)$" n)
          (re-find #"^trg_.+_audit_(insert|delete)$"               n)))))

(defn- drop-all-substrate-triggers-for-table!
  "Pattern-drop every substrate trigger attached to `table`. Closes the
   orphan-on-rename gap (a relation- or entity-xid change leaves the old
   trigger firing alongside the new one). Idempotent.

   CRDB stores triggers but does NOT populate `information_schema.triggers`
   or `pg_trigger` in v25.2 — discovery is only via
   `SHOW TRIGGERS FROM <table>`. We filter by our naming convention."
  [tx table]
  (let [rows (try
               (jdbc/execute! tx [(format "SHOW TRIGGERS FROM \"%s\"" table)])
               (catch Throwable e
                 (log/debug {:id ::show-triggers-failed :data {:table table}}
                            (.getMessage e))
                 []))
        all-names (->> rows
                       (map (fn [r] (or (:trigger_name r) (get r (keyword "" "trigger_name")))))
                       (remove nil?)
                       (map strip-quotes)
                       distinct)
        stale (filterv substrate-trigger? all-names)]
    (log/debug {:id ::substrate-trigger-discovery
                :data {:table table :seen all-names :matched stale}}
               "Discovered triggers")
    (doseq [trg stale]
      (try
        (jdbc/execute! tx [(format "DROP TRIGGER IF EXISTS \"%s\" ON \"%s\""
                                   trg table)])
        (log/debug {:id ::trigger-dropped :data {:table table :trigger trg}}
                   "Dropped substrate trigger")
        (catch Throwable e
          (log/error! {:id ::trigger-drop-failed
                       :data {:table table :trigger trg}} e))))))

(defn- drop-substrate-function!
  "Drop a substrate plpgsql function by name. Idempotent. Used during
   reinstall so a renamed entity doesn't leave the old function pinned by
   the new trigger."
  [tx fn-name]
    ;; CRDB v24.3 doesn't support DROP FUNCTION … CASCADE — we drop triggers
  ;; first in the install flow, so the function has no dependents and a
  ;; bare DROP is enough.
  (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS \"%s\"()" fn-name)]))

;; ============================================================================
;; Context — read via current_setting in trigger bodies
;; ============================================================================

;; Convenience SQL fragment for every payload's context fields. Trigger
;; functions can't `current_setting()` an unset GUC without a default, so
;; we wrap each in `NULLIF(current_setting('synthigy.x', true), '')`.
(def ^:private context-payload-pairs
  (str "'tenant_xid', NULLIF(current_setting('synthigy.tenant_xid', true), ''),"
       "'actor_xid',  NULLIF(current_setting('synthigy.actor_xid',  true), ''),"
       "'request_id', NULLIF(current_setting('synthigy.request_id', true), ''),"
       "'scope_xid',  NULLIF(current_setting('synthigy.scope_xid',  true), ''),"
       "'txid',       NULLIF(current_setting('synthigy.txid',       true), '')"))

;; ============================================================================
;; Relation substrate — function + trigger per (relation, op)
;; ============================================================================

(defn- relation-trigger-name
  [rel-xid op]
  (str "trg_" rel-xid "_audit_" (name op)))

(defn- relation-fn-name
  [rel-xid op]
  (str "fn_" rel-xid "_audit_" (name op)))

(defn- insert-trigger-fn-ddl
  "plpgsql function body for relation INSERT. Inlines the relation-xid +
   from/to column names so a single fn handles all rows for that link
   table. CRDB requires (NEW).field for reads."
  [{rel-id :relation table :table from :from/field to :to/field}]
  (let [fn-name (relation-fn-name (str rel-id) :insert)]
    (format "CREATE OR REPLACE FUNCTION \"%s\"() RETURNS TRIGGER AS $$
             BEGIN
               INSERT INTO __relation_delta_queue (kind, payload)
               VALUES ('relation-mutation', jsonb_build_object(
                 'v', 1,
                 'relation_xid', '%s',
                 'op', 'link',
                 'from_eid', (NEW).\"%s\",
                 'to_eid',   (NEW).\"%s\",
                 'from_xid', (NEW).from_xid,
                 'to_xid',   (NEW).to_xid,
                 %s
               ));
               RETURN NULL;
             END $$ LANGUAGE plpgsql"
            fn-name (str rel-id) (name from) (name to) context-payload-pairs)))

(defn- delete-trigger-fn-ddl
  "plpgsql function body for relation DELETE — mirror of insert, reads OLD."
  [{rel-id :relation table :table from :from/field to :to/field}]
  (let [fn-name (relation-fn-name (str rel-id) :delete)]
    (format "CREATE OR REPLACE FUNCTION \"%s\"() RETURNS TRIGGER AS $$
             BEGIN
               INSERT INTO __relation_delta_queue (kind, payload)
               VALUES ('relation-mutation', jsonb_build_object(
                 'v', 1,
                 'relation_xid', '%s',
                 'op', 'unlink',
                 'from_eid', (OLD).\"%s\",
                 'to_eid',   (OLD).\"%s\",
                 'from_xid', (OLD).from_xid,
                 'to_xid',   (OLD).to_xid,
                 %s
               ));
               RETURN NULL;
             END $$ LANGUAGE plpgsql"
            fn-name (str rel-id) (name from) (name to) context-payload-pairs)))

(defn- relation-insert-trigger-ddl [{rel-id :relation table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (relation-trigger-name (str rel-id) :insert)
          table
          (relation-fn-name (str rel-id) :insert)))

(defn- relation-delete-trigger-ddl [{rel-id :relation table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER DELETE ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (relation-trigger-name (str rel-id) :delete)
          table
          (relation-fn-name (str rel-id) :delete)))

(defn- column-exists?
  "Check whether a column exists on a CRDB table via information_schema."
  [tx table column]
  (boolean
   (seq (jdbc/execute! tx
          ["SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
              AND column_name = ?"
           table column]))))

(defn- ensure-xid-columns!
  "Add from_xid / to_xid varchar(22) columns to a relation link table
   if missing. CRDB supports `ADD COLUMN IF NOT EXISTS` directly so we
   don't need the SQLite PRAGMA dance, but using it keeps the diff small."
  [tx table]
  (jdbc/execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS from_xid varchar(22)" table)])
  (jdbc/execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS to_xid   varchar(22)" table)]))

(defn- backfill-xid-columns!
  "Backfill from_xid / to_xid from the entity tables. Only updates rows
   where the column is still NULL — safe to re-run."
  [tx {table :table from-table :from/table to-table :to/table
       from-field :from/field to-field :to/field}]
  (jdbc/execute!
   tx
   [(format (str "UPDATE \"%s\""
                 " SET from_xid = (SELECT xid FROM \"%s\" WHERE _eid = \"%s\".\"%s\")"
                 " WHERE from_xid IS NULL")
            table from-table table (name from-field))])
  (jdbc/execute!
   tx
   [(format (str "UPDATE \"%s\""
                 " SET to_xid = (SELECT xid FROM \"%s\" WHERE _eid = \"%s\".\"%s\")"
                 " WHERE to_xid IS NULL")
            table to-table table (name to-field))]))

(defn- install-relation-triggers!
  "Install INSERT + DELETE substrate function-trigger pairs on one link
   table. Idempotent — pattern-drops every substrate trigger first."
  [tx {table :table rel-id :relation :as rel}]
  (ensure-xid-columns! tx table)
  (drop-all-substrate-triggers-for-table! tx table)
  (drop-substrate-function! tx (relation-fn-name (str rel-id) :insert))
  (drop-substrate-function! tx (relation-fn-name (str rel-id) :delete))
  (jdbc/execute! tx [(insert-trigger-fn-ddl rel)])
  (jdbc/execute! tx [(delete-trigger-fn-ddl rel)])
  (jdbc/execute! tx [(relation-insert-trigger-ddl rel)])
  (jdbc/execute! tx [(relation-delete-trigger-ddl rel)]))

;; ============================================================================
;; Entity substrate — function + trigger per (entity, op)
;; ============================================================================

(defn- entity-trigger-name
  [entity-xid op]
  (str "trg_" entity-xid "_entity_audit_" (name op)))

(defn- entity-fn-name
  [entity-xid op]
  (str "fn_" entity-xid "_entity_audit_" (name op)))

(defn- attribute-distinct-or
  "Build the WHEN-clause body for the entity UPDATE trigger: an OR'd
   chain of `(OLD).col IS DISTINCT FROM (NEW).col` predicates across every
   user-defined field. CRDB plpgsql requires (OLD).field / (NEW).field."
  [field-map]
  (->> field-map
       (map (fn [[col _attr-xid]]
              (format "(OLD).\"%s\" IS DISTINCT FROM (NEW).\"%s\""
                      (name col) (name col))))
       (str/join " OR ")))

(defn- entity-user-fields
  "Walk an entity schema entry's :field->attribute and return
   {column-name attribute-xid ...} for user-defined fields only.
   System fields are keyword-xid'd and skipped."
  [entity]
  (->> (:field->attribute entity)
       (filter (fn [[_col attr-xid]] (string? attr-xid)))
       (into (sorted-map))))

(defn- attribute-json-pairs
  "Build the jsonb_build_object pairs emitting `'<attr-xid>', <ref>.\"<col>\"`
   for every user-defined field. `ref` is `(NEW)` or `(OLD)` (parenthesised
   per CRDB plpgsql rules)."
  [field-map ref]
  (->> field-map
       (map (fn [[col attr-xid]]
              (format "'%s', %s.\"%s\"" attr-xid ref (name col))))
       (str/join ", ")))

(defn- entity-insert-fn-ddl
  [entity-xid {table :table :as entity}]
  (let [fn-name (entity-fn-name entity-xid :insert)
        pairs (attribute-json-pairs (entity-user-fields entity) "(NEW)")]
    (format "CREATE OR REPLACE FUNCTION \"%s\"() RETURNS TRIGGER AS $$
             BEGIN
               INSERT INTO __entity_delta_queue (payload) VALUES (
                 jsonb_build_object(
                   'v', 1,
                   'op', 'insert',
                   'record_xid', (NEW).xid,
                   'entity_xid', '%s',
                   %s,
                   'after', jsonb_build_object(%s)
                 )
               );
               RETURN NULL;
             END $$ LANGUAGE plpgsql"
            fn-name entity-xid context-payload-pairs pairs)))

(defn- entity-update-fn-ddl
  [entity-xid {table :table :as entity}]
  (let [fn-name (entity-fn-name entity-xid :update)
        fields (entity-user-fields entity)
        before (attribute-json-pairs fields "(OLD)")
        after  (attribute-json-pairs fields "(NEW)")]
    (format "CREATE OR REPLACE FUNCTION \"%s\"() RETURNS TRIGGER AS $$
             BEGIN
               INSERT INTO __entity_delta_queue (payload) VALUES (
                 jsonb_build_object(
                   'v', 1,
                   'op', 'update',
                   'record_xid', (NEW).xid,
                   'entity_xid', '%s',
                   %s,
                   'before', jsonb_build_object(%s),
                   'after',  jsonb_build_object(%s)
                 )
               );
               RETURN NULL;
             END $$ LANGUAGE plpgsql"
            fn-name entity-xid context-payload-pairs before after)))

(defn- entity-delete-fn-ddl
  [entity-xid {table :table :as entity}]
  (let [fn-name (entity-fn-name entity-xid :delete)
        pairs (attribute-json-pairs (entity-user-fields entity) "(OLD)")]
    (format "CREATE OR REPLACE FUNCTION \"%s\"() RETURNS TRIGGER AS $$
             BEGIN
               INSERT INTO __entity_delta_queue (payload) VALUES (
                 jsonb_build_object(
                   'v', 1,
                   'op', 'delete',
                   'record_xid', (OLD).xid,
                   'entity_xid', '%s',
                   %s,
                   'before', jsonb_build_object(%s)
                 )
               );
               RETURN NULL;
             END $$ LANGUAGE plpgsql"
            fn-name entity-xid context-payload-pairs pairs)))

(defn- entity-insert-trigger-ddl [entity-xid {table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (entity-trigger-name entity-xid :insert)
          table
          (entity-fn-name entity-xid :insert)))

(defn- entity-update-trigger-ddl [entity-xid {table :table :as entity}]
  (let [when-clause (attribute-distinct-or (entity-user-fields entity))]
    (format "CREATE TRIGGER \"%s\" AFTER UPDATE ON \"%s\"
             FOR EACH ROW WHEN (%s)
             EXECUTE FUNCTION \"%s\"()"
            (entity-trigger-name entity-xid :update)
            table
            when-clause
            (entity-fn-name entity-xid :update))))

(defn- entity-delete-trigger-ddl [entity-xid {table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER DELETE ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (entity-trigger-name entity-xid :delete)
          table
          (entity-fn-name entity-xid :delete)))

(defn- install-entity-triggers!
  "Install AFTER INSERT/UPDATE/DELETE function + trigger pairs on one
   entity table. Idempotent — pattern-drops every substrate trigger and
   function on the table first. Skips entities with no user-defined fields
   (UPDATE WHEN clause would be empty)."
  [tx entity-xid {table :table :as entity}]
  (when (seq (entity-user-fields entity))
    (drop-all-substrate-triggers-for-table! tx table)
    (drop-substrate-function! tx (entity-fn-name entity-xid :insert))
    (drop-substrate-function! tx (entity-fn-name entity-xid :update))
    (drop-substrate-function! tx (entity-fn-name entity-xid :delete))
    (jdbc/execute! tx [(entity-insert-fn-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-update-fn-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-delete-fn-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-insert-trigger-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-update-trigger-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-delete-trigger-ddl entity-xid entity)])))

;; ============================================================================
;; Drainer — polling loop. Mirrors sqlite's shape; CRDB has no
;; LISTEN/NOTIFY so we park on *drain-wakeup* with a safety-poll fallback.
;; ============================================================================

(def ^:private drain-batch-size 500)
(def ^:private safety-poll-ms (long 10000))

(defonce ^:private drainer-state (atom nil))

(defn- row->envelope
  "Translate a __relation_delta_queue row into the delta envelope shape.
   `ts` is passed through as the raw java.sql.Timestamp / Date that JDBC
   returns — downstream consumers (audit writer's `ts->pg`, SDK) accept
   it. Stringifying with `(str ts)` produces a space-separated form
   CRDB's clock_timestamp() emits that `java.time.Instant/parse` won't
   accept, so we leave the type alone here."
  [{ts :__relation_delta_queue/ts
    payload :__relation_delta_queue/payload}]
  (let [data (if (map? payload) payload (<-json (str payload)))]
    {:element (id/coerce-stored-id (:relation-xid data))
     :delta {:type (keyword "relation" (name (:op data)))
             :data {:ts       ts
                    :from-eid (:from-eid data)
                    :to-eid   (:to-eid data)
                    :from-xid (id/coerce-stored-id (:from-xid data))
                    :to-xid   (id/coerce-stored-id (:to-xid data))
                    :tenant   (id/coerce-stored-id (:tenant-xid data))
                    :actor    (id/coerce-stored-id (:actor-xid data))
                    :request  (:request-id data)
                    :scope    (id/coerce-stored-id (:scope-xid data))
                    :txid     (:txid data)}}}))

(defn- entity-row->envelope
  [{ts :__entity_delta_queue/ts
    payload :__entity_delta_queue/payload}]
  (let [data (if (map? payload) payload (<-json (str payload)))]
    {:element (id/coerce-stored-id (:record-xid data))
     :delta {:type (keyword "entity" (name (:op data)))
             :data {:ts         ts
                    :record-xid (id/coerce-stored-id (:record-xid data))
                    :entity-xid (id/coerce-stored-id (:entity-xid data))
                    :tenant     (id/coerce-stored-id (:tenant-xid data))
                    :actor      (id/coerce-stored-id (:actor-xid data))
                    :request    (:request-id data)
                    :scope      (id/coerce-stored-id (:scope-xid data))
                    :txid       (:txid data)
                    :before     (:before data)
                    :after      (:after data)}}}))

(defn- drain-batch!
  "Claim up to N rows in id order, publish each, optionally persist via
   the audit provider, then DELETE on success. Same shape as sqlite."
  [tx]
  (let [rows (jdbc/execute! tx
                            [(str "SELECT id, ts, payload FROM __relation_delta_queue "
                                  "ORDER BY id LIMIT " drain-batch-size)])]
    (when (seq rows)
      (let [envelopes (mapv row->envelope rows)]
        (doseq [envelope envelopes]
          (delta/dispatch! envelope))
        (if-let [provider audit/*audit-provider*]
          (binding [audit/*audit-tx* tx]
            (audit/write-relation-deltas! provider envelopes))
          (audit/warn-missing-provider!)))
      (jdbc/execute! tx
                     [(str "DELETE FROM __relation_delta_queue WHERE id IN ("
                           (str/join "," (map :__relation_delta_queue/id rows))
                           ")")]))
    (count rows)))

(defn- drain-entity-batch!
  [tx]
  (let [rows (jdbc/execute! tx
                            [(str "SELECT id, ts, payload FROM __entity_delta_queue "
                                  "ORDER BY id LIMIT " drain-batch-size)])]
    (when (seq rows)
      (let [envelopes (mapv entity-row->envelope rows)]
        (doseq [envelope envelopes]
          (delta/dispatch! envelope))
        (if-let [provider audit/*audit-provider*]
          (binding [audit/*audit-tx* tx]
            (audit/write-entity-deltas! provider envelopes))
          (audit/warn-missing-provider!)))
      (jdbc/execute! tx
                     [(str "DELETE FROM __entity_delta_queue WHERE id IN ("
                           (str/join "," (map :__entity_delta_queue/id rows))
                           ")")]))
    (count rows)))

(defn- drainer-tick!
  "One iteration. Drain both queues to empty: relation then entity."
  [db]
  (loop []
    (let [n-rel (try
                  (jdbc/with-transaction [tx (:datasource db)]
                    (drain-batch! tx))
                  (catch Throwable e
                    (log/error! {:id ::drain-batch-failed} e)
                    0))
          n-ent (try
                  (jdbc/with-transaction [tx (:datasource db)]
                    (drain-entity-batch! tx))
                  (catch Throwable e
                    (log/error! {:id ::drain-entity-batch-failed} e)
                    0))]
      (when (or (= n-rel drain-batch-size)
                (= n-ent drain-batch-size))
        (recur)))))

(defn- drainer-loop!
  "Worker body. Parks on the registered WakeSource with a safety-poll-ms
   fallback. Whichever wake source is plugged in (LocalChannel, NATS,
   changefeed, Composite, …), the loop shape is unchanged."
  [db wake-source stop?]
  (try
    (log/info {:id ::drainer-wakeup-listening
               :data {:action :started :subject :relation-drainer
                      :safety-poll-ms safety-poll-ms
                      :wake-source (.getName (class wake-source))}}
              "Substrate drainer listening on WakeSource (CRDB)")
    (while (not @stop?)
      (try
        (case (wake/wait! wake-source safety-poll-ms)
          :wakeup (when-not @stop? (drainer-tick! db))
          :poll   (when-not @stop? (drainer-tick! db))
          :stop   (reset! stop? true))
        (catch InterruptedException _
          (reset! stop? true))
        (catch Throwable e
          (log/error! {:id ::drain-loop-error} e)
          (try (Thread/sleep 1000)
               (catch InterruptedException _ (reset! stop? true))))))
    (catch Throwable e
      (log/error! {:id ::drainer-fatal} e))
    (finally
      (log/info {:id ::drainer-loop-exited
                 :data {:action :stopped :subject :relation-drainer}}
                "Substrate drainer loop exited"))))

;; ============================================================================
;; Public API — substrate reconcile + drainer + context
;; ============================================================================

(defn reconcile-relations!
  "Deploy relation-substrate infrastructure: queue table + per-relation
   function/trigger pairs across the deployed schema. Idempotent."
  [_db tx schema-relations]
  (jdbc/execute! tx [delta-queue-ddl])
  (let [rel-seq (if (map? schema-relations)
                  (vals schema-relations)
                  schema-relations)
        unique-by-table (reduce
                         (fn [acc rel]
                           (if-let [t (:table rel)]
                             (if-let [prior (get acc t)]
                               (if (and (nil? (:relation prior))
                                        (some? (:relation rel)))
                                 (assoc acc t rel)
                                 acc)
                               (assoc acc t rel))
                             acc))
                         {}
                         rel-seq)
        unique-by-table (into {}
                              (filter (fn [[_t r]] (some? (:relation r))))
                              unique-by-table)]
    (doseq [rel (vals unique-by-table)]
      (install-relation-triggers! tx rel))
    (doseq [rel (vals unique-by-table)]
      (try
        (backfill-xid-columns! tx rel)
        (catch Throwable e
          (log/error! {:id ::backfill-failed
                       :data {:table (:table rel)}} e)))))
  nil)

(defn reconcile-entities!
  "Deploy entity-substrate infrastructure: queue table + per-entity
   function/trigger triples across every entity in the deployed schema.
   Idempotent."
  [_db tx schema-entities]
  (jdbc/execute! tx [entity-queue-ddl])
  (let [pairs (cond
                (map? schema-entities)
                (seq schema-entities)

                (sequential? schema-entities)
                (map (fn [[id ent]] [id ent]) schema-entities)

                :else
                nil)]
    (doseq [[entity-id entity] pairs]
      (when (:table entity)
        (try
          (install-entity-triggers! tx (str entity-id) entity)
          (catch Throwable e
            (log/error! {:id ::entity-trigger-install-failed
                         :data {:table (:table entity)
                                :entity-id (str entity-id)}} e))))))
  nil)

(defn set-context!
  "Write per-tx request/principal context as session GUCs so substrate
   trigger functions can read them via `current_setting`. CRDB-side:
   `SET LOCAL synthigy.x = '…'` is scoped to the current transaction and
   visible to triggers firing within it. No `_ctx` table needed.

   ISOLATION: SET LOCAL is per-transaction in CRDB (and PG), so each
   connection's principal stays distinct under concurrent writers."
  [_db tx]
  (let [principal (access/current-principal)
        actor   (some-> principal :xid str)
        request (some-> principal :request-id str)
        scope   (some-> principal :scope-xid str)
        tenant  (str (id/data :dataset/id))
        txid    (str (uuid-v7))]
    ;; CRDB doesn't accept parameterised SET LOCAL — value must be a
    ;; literal. The strings come from app-supplied principal data; they
    ;; never carry untrusted user input directly, but we still escape
    ;; single quotes defensively.
    (letfn [(set-guc! [k v]
              (when v
                (jdbc/execute! tx
                  [(format "SET LOCAL synthigy.%s = '%s'"
                           k (str/replace v "'" "''"))])))]
      (set-guc! "actor_xid"  actor)
      (set-guc! "request_id" request)
      (set-guc! "scope_xid"  scope)
      (set-guc! "tenant_xid" tenant)
      (set-guc! "txid"       txid)))
  nil)

(defn start-drainer!
  "Start the substrate drainer thread. Initializes the active
   WakeSource and parks the loop on it. Idempotent."
  [db]
  (if @drainer-state
    (log/info {:id ::drainer-already-running
               :data {:action :starting :subject :relation-drainer}}
              "Substrate drainer already running; ignoring start")
    (let [ws wake/*wake-source*
          _  (wake/start-source! ws)
          stop? (atom false)
          t (Thread. ^Runnable #(drainer-loop! db ws stop?)
                     "synthigy-relation-drainer-cockroach")]
      (.setDaemon t true)
      (.start t)
      (reset! drainer-state {:thread t :stop? stop? :wake-source ws})
      nil)))

(defn stop-drainer!
  "Cleanly stop the substrate drainer and tear down its wake source.
   Idempotent."
  [_db]
  (when-let [{:keys [thread stop? wake-source]} @drainer-state]
    (reset! stop? true)
    (when wake-source (wake/stop-source! wake-source))
    (.interrupt thread)
    (try (.join thread 5000) (catch InterruptedException _))
    (reset! drainer-state nil)
    nil))

;; ============================================================================
;; Unified entry points
;; ============================================================================

(defn trigger-fingerprint
  "Compact value capturing the trigger-relevant shape of `model` — table
   names, user-defined-field maps, relation link-table identities. Two
   models with `=` fingerprints produce byte-identical substrate trigger
   DDL, so the watch can short-circuit reconcile when fingerprint is
   unchanged (e.g. position-coordinate-only save-model! calls)."
  [model]
  (when model
    (let [schema ((requiring-resolve 'synthigy.dataset.sql.query/model->schema) model)]
      {:entities
       (into {}
         (map (fn [[eid ent]]
                [eid {:table (:table ent)
                      :fields (entity-user-fields ent)}]))
         schema)
       :relations
       (->> (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
            (keep (fn [r]
                    (when-let [rid (:relation r)]
                      [rid (select-keys r [:relation :table
                                           :from/field :to/field
                                           :from/table :to/table])])))
            (into {}))})))

(defn reconcile-substrate!
  "Idempotent full resync of the CRDB delta substrate against `model`.
   Backwards-compatible (always reconciles); the watch caller in
   `subscriptions.cockroach` uses `trigger-fingerprint` to skip
   no-op invocations before reaching this entry point."
  [db model]
  (when model
    (let [schema ((requiring-resolve 'synthigy.dataset.sql.query/model->schema) model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn reconcile-substrate-diff!
  "Incremental reconcile: only install/refresh triggers for entities
   and relations whose `trigger-fingerprint` value DIFFERS between
   `old-fp` and `new-model`. Entities/relations that disappeared have
   their substrate triggers + functions dropped.

   For a deployment-test workload that adds one entity to an existing
   N-entity dataset, this collapses an O(N) trigger rebuild to O(1) —
   the dominant cost of CRDB's declarative schema changer is paid only
   once per real change instead of once per entity per save-model!."
  [db old-fp new-model]
  (when new-model
    (let [schema ((requiring-resolve 'synthigy.dataset.sql.query/model->schema) new-model)
          all-relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
          new-fp (trigger-fingerprint new-model)
          old-ents (:entities old-fp)
          new-ents (:entities new-fp)
          old-rels (:relations old-fp)
          new-rels (:relations new-fp)
          ;; Entities to install: new + changed
          ent-changed (->> new-ents
                           (keep (fn [[xid fp]]
                                   (when (not= fp (get old-ents xid)) xid)))
                           set)
          ;; Relations to install: new + changed
          rel-changed (->> new-rels
                           (keep (fn [[rid fp]]
                                   (when (not= fp (get old-rels rid)) rid)))
                           set)
          ;; Removed: present in old, absent in new
          ent-removed (->> (keys old-ents)
                           (remove (set (keys new-ents))))
          rel-removed (->> (keys old-rels)
                           (remove (set (keys new-rels))))
          ;; Filter schema entries to only the changed/added subsets
          schema-changed (into {}
                           (filter (fn [[eid _]] (ent-changed (str eid))))
                           schema)
          rels-changed (filter (fn [r]
                                 (when-let [rid (:relation r)]
                                   (rel-changed rid)))
                               all-relations)]
      (when (or (seq schema-changed) (seq rels-changed)
                (seq ent-removed) (seq rel-removed))
        (log/info {:id ::reconcile-diff
                   :data {:entities-changed (count schema-changed)
                          :relations-changed (count rels-changed)
                          :entities-removed (count ent-removed)
                          :relations-removed (count rel-removed)}}
                  "Incremental substrate reconcile"))
      ;; Drop removed-entity triggers (table is gone or no longer in model;
      ;; if the table itself was dropped this is a no-op). Pass the
      ;; datasource directly — drop-* helpers expect a Sourceable.
      (let [ds (:datasource db)]
        (doseq [old-xid ent-removed
                :let [old-table (get-in old-ents [old-xid :table])]
                :when old-table]
          (try
            (drop-all-substrate-triggers-for-table! ds old-table)
            (doseq [op [:insert :update :delete]]
              (drop-substrate-function! ds (entity-fn-name old-xid op)))
            (catch Throwable e
              (log/warn {:id ::drop-removed-entity-failed
                         :data {:entity-xid old-xid :table old-table}}
                        (.getMessage e)))))
        (doseq [old-rid rel-removed
                :let [old-table (get-in old-rels [old-rid :table])]
                :when old-table]
          (try
            (drop-all-substrate-triggers-for-table! ds old-table)
            (doseq [op [:insert :delete]]
              (drop-substrate-function! ds (relation-fn-name old-rid op)))
            (catch Throwable e
              (log/warn {:id ::drop-removed-relation-failed
                         :data {:relation-xid old-rid :table old-table}}
                        (.getMessage e))))))
      ;; Install/refresh only the changed entities + relations
      (when (seq rels-changed)
        (reconcile-relations! db (:datasource db) rels-changed))
      (when (seq schema-changed)
        (reconcile-entities! db (:datasource db) schema-changed))
      new-fp)))

(defn strip-all!
  "Drop every substrate trigger + function. Used on subscriptions stop and
   as the bare-server upgrader helper. Leaves the queue tables in place —
   they're cheap; re-:start reconciles triggers back.

   CRDB has no cross-table trigger enumeration view, so we walk
   `pg_tables` (public schema) and run SHOW TRIGGERS FROM each."
  [_db tx]
  (let [tables (->> (jdbc/execute! tx
                     [(str "SELECT tablename FROM pg_tables "
                           "WHERE schemaname = 'public'")])
                    (map :pg_tables/tablename)
                    (remove nil?))]
    (doseq [t tables]
      (drop-all-substrate-triggers-for-table! tx t)))
  (let [fns (->> (jdbc/execute! tx
                   ["SELECT proname FROM pg_proc
                     WHERE proname LIKE 'fn\\_%\\_audit\\_%' ESCAPE '\\'
                        OR proname LIKE 'fn\\_%\\_entity\\_audit\\_%' ESCAPE '\\'"])
                 (mapv :pg_proc/proname)
                 (remove nil?)
                 distinct)]
    (doseq [fn-name fns]
      (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS \"%s\"()" fn-name)])))
  nil)
