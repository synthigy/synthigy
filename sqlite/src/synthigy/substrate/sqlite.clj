(ns synthigy.substrate.sqlite
  "SQLite-side delta substrate — DDL, trigger generation, drainer thread.

   Owned by `:synthigy/subscriptions.sqlite` (OSS). Mirrors
   `synthigy.substrate.postgres`. Consumers: SSE subscriptions (OSS) and
   the observability substrate (`:synthigy/observability` — DuckDB OSS,
   ClickHouse Pro).

   SQLite-specific deltas vs PG:
     - No NOTIFY → drainer parks on `*drain-wakeup*` with a safety poll
     - No `current_setting` → per-process `_ctx` regular table
     - No SKIP LOCKED → single-writer model is sufficient
     - SQLite triggers can't share a parameterized function, so we install
       separate INSERT/UPDATE/DELETE triggers per entity table and
       INSERT/DELETE triggers per relation link table, each inlining the
       `json_object` payload."
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [synthigy.audit :as audit]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.delta :as delta]
    [synthigy.dataset.id :as id]
    [synthigy.json :refer [<-json]]
    [synthigy.log :as log]
    [synthigy.substrate.wake :as wake]))

;; ============================================================================
;; In-process drainer wakeup channel — SQLite-specific
;;
;; SQLite has no LISTEN/NOTIFY, so app-path writes nudge the drainer
;; through this channel. `dropping-buffer 1` coalesces concurrent wakeups:
;; the drainer drains-to-empty per wake, so one wake covers N puts.
;;
;; Lives on the substrate ns (not in the query ns) because moving it
;; eliminates the substrate.sqlite ↔ dataset.sqlite.query cyclic require
;; introduced when the query ns started calling substrate/set-context!
;; on every mutation.
;; ============================================================================

(defn wake-drainer!
  "Backend-named alias for `synthigy.substrate.wake/signal-drainer!`.
   Kept so the SQLite query callsites don't have to know they're
   delegating. App-path writes call this post-commit."
  []
  (wake/signal-drainer!))

;; ============================================================================
;; Queue + context table DDL
;; ============================================================================

(def ^:private delta-queue-ddl
  "CREATE TABLE IF NOT EXISTS __relation_delta_queue (
     id      INTEGER PRIMARY KEY AUTOINCREMENT,
     ts      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
     kind    TEXT    NOT NULL,
     payload TEXT    NOT NULL
   )")

(def ^:private audit-ctx-ddl
  "CREATE TABLE IF NOT EXISTS _ctx (
     k TEXT PRIMARY KEY,
     v TEXT
   )")

(def ^:private entity-queue-ddl
  "CREATE TABLE IF NOT EXISTS __entity_delta_queue (
     id      INTEGER PRIMARY KEY AUTOINCREMENT,
     ts      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
     payload TEXT    NOT NULL
   )")

;; ============================================================================
;; Helpers — UUIDv7 + shared trigger plumbing
;; ============================================================================

(defn- uuid-v7
  "Generate a UUIDv7 (RFC 9562): 48-bit unix-ms timestamp prefix, 4-bit
   version=7, 12 random bits, 2-bit variant=10, 62 random bits. Lexically
   and binary-sortable by time — txid grouping AND chronological ordering
   off the same id without leaning on a separate `ts` column."
  []
  (let [ts  (System/currentTimeMillis)
        rnd (java.security.SecureRandom.)
        rand-12 (bit-and (.nextLong rnd) 0xFFF)
        rand-62 (bit-and (.nextLong rnd) 0x3FFFFFFFFFFFFFFF)
        msb (bit-or (bit-shift-left ts 16)
                    0x7000
                    rand-12)
        lsb (bit-or (unchecked-long 0x8000000000000000)
                    rand-62)]
    (java.util.UUID. msb lsb)))

(defn- drop-all-substrate-triggers-for-table!
  "Pattern-drop every substrate trigger attached to `table`, regardless of
   name shape. Closes the orphan-on-rename gap: switching to xid-keyed
   names (or renaming an entity xid) would otherwise leave the old
   table-named triggers firing alongside the new ones, double-emitting
   events. Idempotent — runs on every deploy. Used by both the relation
   and entity install paths."
  [tx table]
  (let [stale (->> (jdbc/execute! tx
                     ["SELECT name FROM sqlite_master
                       WHERE type = 'trigger'
                         AND tbl_name = ?
                         AND (name LIKE 'trg_%_entity_audit_%'
                           OR name LIKE 'trg_%_audit_insert'
                           OR name LIKE 'trg_%_audit_delete')"
                      table])
                   (map :sqlite_master/name)
                   (remove nil?))]
    (doseq [trg stale]
      (jdbc/execute! tx [(format "DROP TRIGGER IF EXISTS \"%s\"" trg)]))))

;; ============================================================================
;; Relation substrate — trigger generation
;; ============================================================================

(defn- relation-trigger-name
  "Deterministic trigger name per (relation-xid, op). Xid-keyed for the
   same reason as entity triggers — link-table renames don't leave
   orphan triggers in `sqlite_master`. SQLite has no per-function dispatch
   so we install one trigger per (relation, INSERT|DELETE)."
  [rel-xid op]
  (str "trg_" rel-xid "_audit_" (name op)))

(defn- insert-trigger-ddl
  "INSERT trigger inlines the json_object payload, reads from_xid/to_xid
   straight off NEW (populated by app-path link-relations subselects),
   reads context from _ctx (NULL when row absent — gracefully handles raw
   mutations outside the app path that didn't go through set-context!)."
  [{rel-id :relation table :table from :from/field to :to/field} audit?]
  (format "CREATE TRIGGER \"%s\"
           AFTER INSERT ON \"%s\"
           FOR EACH ROW
           BEGIN
             INSERT INTO __relation_delta_queue (kind, payload) VALUES (
               'relation-mutation',
               json_object(
                 'v',            1,
                 'relation_xid', '%s',
                 'audit',        %s,
                 'op',           'link',
                 'from_eid',     NEW.\"%s\",
                 'to_eid',       NEW.\"%s\",
                 'from_xid',     NEW.from_xid,
                 'to_xid',       NEW.to_xid,
                 'tenant_xid',   (SELECT v FROM _ctx WHERE k='tenant_xid'),
                 'actor_xid',    (SELECT v FROM _ctx WHERE k='actor_xid'),
                 'request_id',   (SELECT v FROM _ctx WHERE k='request_id'),
                 'scope_xid',    (SELECT v FROM _ctx WHERE k='scope_xid'),
                 'txid',         (SELECT v FROM _ctx WHERE k='txid')
               )
             );
           END"
          (relation-trigger-name (str rel-id) :insert)
          table (str rel-id) (if audit? "json('true')" "json('false')") (name from) (name to)))

(defn- delete-trigger-ddl
  "DELETE trigger — mirror of insert-trigger-ddl, reads OLD.* and op='unlink'.
   from_xid/to_xid come straight off OLD (populated at link time) — so
   cascade deletes from entity removal still emit resolvable xids even
   after the entity row is gone."
  [{rel-id :relation table :table from :from/field to :to/field} audit?]
  (format "CREATE TRIGGER \"%s\"
           AFTER DELETE ON \"%s\"
           FOR EACH ROW
           BEGIN
             INSERT INTO __relation_delta_queue (kind, payload) VALUES (
               'relation-mutation',
               json_object(
                 'v',            1,
                 'relation_xid', '%s',
                 'audit',        %s,
                 'op',           'unlink',
                 'from_eid',     OLD.\"%s\",
                 'to_eid',       OLD.\"%s\",
                 'from_xid',     OLD.from_xid,
                 'to_xid',       OLD.to_xid,
                 'tenant_xid',   (SELECT v FROM _ctx WHERE k='tenant_xid'),
                 'actor_xid',    (SELECT v FROM _ctx WHERE k='actor_xid'),
                 'request_id',   (SELECT v FROM _ctx WHERE k='request_id'),
                 'scope_xid',    (SELECT v FROM _ctx WHERE k='scope_xid'),
                 'txid',         (SELECT v FROM _ctx WHERE k='txid')
               )
             );
           END"
          (relation-trigger-name (str rel-id) :delete)
          table (str rel-id) (if audit? "json('true')" "json('false')") (name from) (name to)))

(defn- column-exists?
  "Check whether a column exists on a SQLite table via PRAGMA table_info."
  [tx table column]
  (some
   #(= column (:name %))
   (jdbc/execute! tx [(format "PRAGMA table_info(\"%s\")" table)]
                  {:builder-fn rs/as-unqualified-maps})))

(defn- ensure-xid-columns!
  "Add from_xid / to_xid TEXT columns to a relation link table if missing.
   SQLite lacks `ADD COLUMN IF NOT EXISTS` so we PRAGMA-check first."
  [tx table]
  (when-not (column-exists? tx table "from_xid")
    (jdbc/execute! tx
                   [(format "ALTER TABLE \"%s\" ADD COLUMN from_xid TEXT" table)]))
  (when-not (column-exists? tx table "to_xid")
    (jdbc/execute! tx
                   [(format "ALTER TABLE \"%s\" ADD COLUMN to_xid TEXT" table)])))

(defn- backfill-xid-columns!
  "Backfill from_xid / to_xid from the entity tables. Only updates rows
   where the column is still NULL — safe to re-run."
  [tx {table      :table
       from-table :from/table
       to-table   :to/table
       from-field :from/field
       to-field   :to/field}]
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
  "Attach INSERT + DELETE substrate triggers to one link table.
   Idempotent — pattern-drops every audit-track trigger on the table
   first, then reinstalls the xid-keyed pair so a switch from table-name
   to xid-name (or any subsequent relation-xid change) leaves no orphans.

   Schema additions for from_xid/to_xid (ALTER TABLE) run first so the
   trigger DDLs can reference the columns.

   Will throw if the underlying table doesn't exist — that's a model/DB
   drift bug to fix at the source, not to swallow here."
  [tx {table :table :as rel} audit?]
  (ensure-xid-columns! tx table)
  (drop-all-substrate-triggers-for-table! tx table)
  (jdbc/execute! tx [(insert-trigger-ddl rel audit?)])
  (jdbc/execute! tx [(delete-trigger-ddl rel audit?)]))

;; ============================================================================
;; Entity substrate — trigger generation
;; ============================================================================

(defn- entity-trigger-name
  "Deterministic trigger name per (entity-xid, op). Xid-keyed so that
   table renames don't leave orphan triggers in `sqlite_master`. Distinct
   prefix from relation triggers so we can grep / drop them independently."
  [entity-xid op]
  (str "trg_" entity-xid "_entity_audit_" (name op)))

(defn- attribute-when-distinct
  "Build the WHEN-clause body for the entity UPDATE trigger — an OR'd
   chain of `OLD.<col> IS NOT NEW.<col>` predicates across every user-defined
   field. SQLite's `IS NOT` is the IS-DISTINCT-FROM operator (NULL-safe).
   Gates the queue insert at trigger time: phantom UPDATE rewrites with no
   real attribute change produce no envelope and no provider write."
  [field-map]
  (->> field-map
       (map (fn [[col _attr-xid]]
              (format "OLD.\"%s\" IS NOT NEW.\"%s\"" (name col) (name col))))
       (str/join " OR ")))

(defn- entity-user-fields
  "Walk an entity schema entry's :field->attribute and return
   {column-name attribute-xid ...} for user-defined fields only.
   System fields (`:created_on`, `:modified_on`, `:created_by`, `:modified_by`)
   are marked with keyword xids and skipped — they're auto-managed by the
   dataset layer and don't carry semantic meaning at the substrate layer."
  [entity]
  (->> (:field->attribute entity)
       (filter (fn [[_col attr-xid]] (string? attr-xid)))
       (into (sorted-map))))

(defn- attribute-json-pairs
  "Build the json_object key/value comma-separated string emitting
   `'<attr-xid>', <ref>.\"<col>\"` for every user-defined field.
   Used by INSERT/UPDATE/DELETE trigger DDLs to assemble before/after maps."
  [field-map ref]
  (->> field-map
       (map (fn [[col attr-xid]]
              (format "'%s', %s.\"%s\"" attr-xid ref (name col))))
       (str/join ", ")))

(defn- entity-insert-trigger-ddl
  [entity-xid {table :table :as entity} audit?]
  (let [pairs (attribute-json-pairs (entity-user-fields entity) "NEW")
        al    (if audit? "json('true')" "json('false')")]
    (format "CREATE TRIGGER \"%s\"
             AFTER INSERT ON \"%s\"
             FOR EACH ROW
             BEGIN
               INSERT INTO __entity_delta_queue (payload) VALUES (
                 json_object(
                   'v',          1,
                   'op',         'insert',
                   'record_xid', NEW.xid,
                   'entity_xid', '%s',
                   'audit',      %s,
                   'tenant_xid', (SELECT v FROM _ctx WHERE k='tenant_xid'),
                   'actor_xid',  (SELECT v FROM _ctx WHERE k='actor_xid'),
                   'request_id', (SELECT v FROM _ctx WHERE k='request_id'),
                   'scope_xid',  (SELECT v FROM _ctx WHERE k='scope_xid'),
                   'txid',       (SELECT v FROM _ctx WHERE k='txid'),
                   'after',      json_object(%s)
                 )
               );
             END"
            (entity-trigger-name entity-xid :insert)
            table
            entity-xid
            al
            pairs)))

(defn- entity-update-trigger-ddl
  [entity-xid {table :table :as entity} audit?]
  (let [fields (entity-user-fields entity)
        before (attribute-json-pairs fields "OLD")
        after  (attribute-json-pairs fields "NEW")
        when-clause (attribute-when-distinct fields)
        al     (if audit? "json('true')" "json('false')")]
    (format "CREATE TRIGGER \"%s\"
             AFTER UPDATE ON \"%s\"
             FOR EACH ROW
             WHEN (%s)
             BEGIN
               INSERT INTO __entity_delta_queue (payload) VALUES (
                 json_object(
                   'v',          1,
                   'op',         'update',
                   'record_xid', NEW.xid,
                   'entity_xid', '%s',
                   'audit',      %s,
                   'tenant_xid', (SELECT v FROM _ctx WHERE k='tenant_xid'),
                   'actor_xid',  (SELECT v FROM _ctx WHERE k='actor_xid'),
                   'request_id', (SELECT v FROM _ctx WHERE k='request_id'),
                   'scope_xid',  (SELECT v FROM _ctx WHERE k='scope_xid'),
                   'txid',       (SELECT v FROM _ctx WHERE k='txid'),
                   'before',     json_object(%s),
                   'after',      json_object(%s)
                 )
               );
             END"
            (entity-trigger-name entity-xid :update)
            table
            when-clause
            entity-xid
            al
            before
            after)))

(defn- entity-delete-trigger-ddl
  [entity-xid {table :table :as entity} audit?]
  (let [pairs (attribute-json-pairs (entity-user-fields entity) "OLD")
        al    (if audit? "json('true')" "json('false')")]
    (format "CREATE TRIGGER \"%s\"
             AFTER DELETE ON \"%s\"
             FOR EACH ROW
             BEGIN
               INSERT INTO __entity_delta_queue (payload) VALUES (
                 json_object(
                   'v',          1,
                   'op',         'delete',
                   'record_xid', OLD.xid,
                   'entity_xid', '%s',
                   'audit',      %s,
                   'tenant_xid', (SELECT v FROM _ctx WHERE k='tenant_xid'),
                   'actor_xid',  (SELECT v FROM _ctx WHERE k='actor_xid'),
                   'request_id', (SELECT v FROM _ctx WHERE k='request_id'),
                   'scope_xid',  (SELECT v FROM _ctx WHERE k='scope_xid'),
                   'txid',       (SELECT v FROM _ctx WHERE k='txid'),
                   'before',     json_object(%s)
                 )
               );
             END"
            (entity-trigger-name entity-xid :delete)
            table
            entity-xid
            al
            pairs)))

(defn- install-entity-triggers!
  "Attach AFTER INSERT/UPDATE/DELETE triggers to one entity table.
   Idempotent — pattern-drops every substrate trigger on the table then
   reinstalls the xid-keyed set so trigger bodies pick up
   column → attribute_xid changes after a rename and a switch from
   table-name to xid-name leaves no orphans. Throws if the table
   doesn't exist (signals model/DB drift; we don't swallow)."
  [tx entity-xid {table :table :as entity} audit?]
  (when (seq (entity-user-fields entity))
    (drop-all-substrate-triggers-for-table! tx table)
    (jdbc/execute! tx [(entity-insert-trigger-ddl entity-xid entity audit?)])
    (jdbc/execute! tx [(entity-update-trigger-ddl entity-xid entity audit?)])
    (jdbc/execute! tx [(entity-delete-trigger-ddl entity-xid entity audit?)])))

;; ============================================================================
;; Drainer — polling loop (no LISTEN/NOTIFY available in SQLite).
;; Single-process scope per the SQLite parity plan; SKIP LOCKED isn't needed
;; because SQLite is single-writer at the DB level.
;; ============================================================================

;; SQLite drain batch size — tuned 2026-06-02.
;;
;; SQLite has no SKIP LOCKED and writer + drainer share the global write
;; lock. Two pressures pull opposite ways:
;;   - bigger batch → fewer write-lock acquisitions → less queue backup
;;   - bigger batch → drainer holds the lock longer per tick → writer
;;     blocks more per tick
;;
;; Measured on the MovieLens 100k-rating burst (in-process, write batch=1000,
;; 3-run medians):
;;   batch=500  → wall 25.12s  / queue residual ~83k (drainer never caught up)
;;   batch=2000 → wall 27.30s  / queue residual ~2.4k (caught up except
;;                                 for the last few drainer ticks' worth)
;;   batch=5000 → wall 27.19s  / queue residual ~2.4k (same)
;;
;; 500 was inherited from the PG drainer (where SKIP LOCKED makes per-tick
;; cost low and many small ticks are fine). On SQLite it's wrong: the
;; drainer literally cannot keep up with a 100k-row burst. 2000 trades
;; ~2s of writer wall for a working drainer — the substrate's whole
;; point is real-time live subs, which a chronically-backed-up queue
;; defeats. 5000 is within noise of 2000 but holds the lock noticeably
;; longer per tick; 2000 keeps the writer's blocking windows shorter.
(def ^:private drain-batch-size 2000)
;; Safety-net poll for raw-SQL mutations that bypass the app-path
;; wakeup. App-path mutations wake the drainer immediately via
;; *drain-wakeup*, so this timeout effectively only fires when there's
;; nothing happening — no per-tick CPU cost beyond the alt!! park.
(def ^:private safety-poll-ms (long 10000))

(defonce ^:private drainer-state (atom nil))

(defn- row->envelope
  "Translate a __relation_delta_queue row into the delta envelope
   shape. Same shape as the PG drainer so downstream subscribers are
   uniform. Xid-shaped fields coerced via id/coerce-stored-id to the
   current id-provider's runtime type.

   The drainer stamps a monotonic `:seq` onto the returned envelope
   (see `drain-batch!`) — the durable replay cursor."
  [{ts      :__relation_delta_queue/ts
    payload :__relation_delta_queue/payload}]
  (let [data (<-json payload)]
    {:element (id/coerce-stored-id (:relation-xid data))
     :audit-persist? (boolean (:audit data))
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
  "Translate a __entity_delta_queue row into the delta envelope.
   Keyed by record_xid so subscribers can sub per-record. Shape mirrors
   the relation envelope: `:element` + `:delta {:type :entity/insert|update|delete
   :data {...}}`. Before/after attribute maps are passed through as-is —
   the JSON keys are attribute_xid strings; the values are whatever the
   trigger's `json_object(NEW.col)` produced."
  [{ts      :__entity_delta_queue/ts
    payload :__entity_delta_queue/payload}]
  (let [data (<-json payload)]
    {:element (id/coerce-stored-id (:record-xid data))
     :audit-persist? (boolean (:audit data))
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

(defn- alloc-seqs!
  "Allocate `n` monotonic delta-seq values from the single-row __delta_seq
   counter, ascending. Race-free + gap-free: SQLite serializes writers, the
   drainer is the sole writer, and this read-then-bump runs inside the drain
   txn (rolls back with the batch on failure)."
  [tx n]
  (let [v (long (:__delta_seq/value
                 (jdbc/execute-one! tx ["SELECT value FROM __delta_seq WHERE id = 0"])))]
    (jdbc/execute! tx ["UPDATE __delta_seq SET value = value + ? WHERE id = 0" n])
    (mapv #(+ v 1 %) (range n))))

(defn- drain-batch!
  "Claim up to N rows in id order, stamp each with a monotonic `:seq`, publish
   each, optionally persist via the audit provider, then DELETE on success.
   Throws on put! timeout (or provider failure) so the whole batch rolls back
   and the rows (and their seqs) are reclaimed on the next tick.

   Wire to durable storage is opt-in: if `audit/*audit-provider*` is
   bound, the drainer calls `(audit/write-relation-deltas! ...)`
   synchronously before deleting. With no provider bound, the drainer
   publishes live and deletes immediately (live-only mode)."
  [tx]
  (let [rows (jdbc/execute! tx
                            [(str "SELECT id, ts, payload FROM __relation_delta_queue "
                                  "ORDER BY id LIMIT " drain-batch-size)])]
    (when (seq rows)
      (let [envelopes (mapv (fn [row sq] (assoc (row->envelope row) :seq sq))
                            rows (alloc-seqs! tx (count rows)))]
        (doseq [envelope envelopes]
          (delta/dispatch! envelope))
        ;; Persist only envelopes whose audit flag was frozen true at enqueue;
        ;; live dispatch above still gets every delta.
        (if-let [provider audit/*audit-provider*]
          (binding [audit/*audit-tx* tx]
            (audit/write-relation-deltas! provider (filterv :audit-persist? envelopes)))
          (audit/warn-missing-provider!)))
      (jdbc/execute! tx
                     [(str "DELETE FROM __relation_delta_queue WHERE id IN ("
                           (str/join "," (map :__relation_delta_queue/id rows))
                           ")")]))
    (count rows)))

(defn- drain-entity-batch!
  "Mirror of drain-batch! for the entity queue. Same SELECT/PUT/(audit)/DELETE
   pattern; different table, different envelope translator. Provider call
   is opt-in via audit/*audit-provider*."
  [tx]
  (let [rows (jdbc/execute! tx
                            [(str "SELECT id, ts, payload FROM __entity_delta_queue "
                                  "ORDER BY id LIMIT " drain-batch-size)])]
    (when (seq rows)
      (let [envelopes (mapv (fn [row sq] (assoc (entity-row->envelope row) :seq sq))
                            rows (alloc-seqs! tx (count rows)))]
        (doseq [envelope envelopes]
          (delta/dispatch! envelope))
        ;; Persist only envelopes whose audit flag was frozen true at enqueue;
        ;; live dispatch above still gets every delta.
        (if-let [provider audit/*audit-provider*]
          (binding [audit/*audit-tx* tx]
            (audit/write-entity-deltas! provider (filterv :audit-persist? envelopes)))
          (audit/warn-missing-provider!)))
      (jdbc/execute! tx
                     [(str "DELETE FROM __entity_delta_queue WHERE id IN ("
                           (str/join "," (map :__entity_delta_queue/id rows))
                           ")")]))
    (count rows)))

(defn- drainer-tick!
  "One iteration. Drain both queues to empty: relation then entity. Loop
   continues whenever either queue produced a full batch (catch-up after
   a bulk write)."
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
  "Worker body. Parks on the registered WakeSource (default
   LocalChannel) with a safety-poll-ms fallback for raw-SQL mutations
   that bypass the wake signal.

   Cost when idle: one park per safety-poll-ms. Cost on wakeup: one
   indexed SELECT, drains all pending rows, then re-parks."
  [db wake-source stop?]
  (try
    (log/info {:id ::drainer-wakeup-listening
               :data {:action :started :subject :relation-drainer
                      :safety-poll-ms safety-poll-ms
                      :wake-source (.getName (class wake-source))}}
              "Substrate drainer listening on WakeSource")
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
  "Deploy relation-substrate infrastructure: idempotent prelude
   (queue + _ctx tables) followed by per-relation triggers across the
   deployed schema.

   Args:
     _db               - SQLite backend instance (unused; kept for symmetry)
     tx                - JDBC-executable target
     schema-relations  - Relations map or seq from (query/model->schema model),
                         each value carrying :relation, :table, :from/field,
                         :to/field pre-resolved.

   Idempotent. Side effects only. Returns nil."
  [_db tx schema-relations]
  ;; _ctx is a regular (non-TEMP) table because SQLite triggers can't
  ;; tolerate a missing table reference even via subquery — see
  ;; set-context! comment for the isolation trade-off.
  (jdbc/execute! tx [delta-queue-ddl])
  (jdbc/execute! tx [audit-ctx-ddl])
  ;; Walk schema relations; dedup by :table (same m2m link appears in
  ;; both endpoints' :relations). All cardinalities are link tables in
  ;; Synthigy → no cardinality filter. When the same :table appears with
  ;; and without :relation set, prefer the record that has it — the
  ;; xid-keyed trigger name depends on :relation, so picking the
  ;; relation-less record would emit `trg__audit_insert` (empty xid).
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
      (install-relation-triggers! tx rel (audit/audited-relation? (str (:relation rel)))))
    (doseq [rel (vals unique-by-table)]
      (try
        (backfill-xid-columns! tx rel)
        (catch Throwable e
          (log/error! {:id ::backfill-failed
                       :data {:table (:table rel)}} e)))))
  nil)

(defn reconcile-entities!
  "Deploy entity-substrate infrastructure: idempotent prelude (entity
   queue) followed by per-entity triggers across every entity in the
   deployed schema.

   Args:
     _db               - SQLite backend instance (unused; kept for symmetry)
     tx                - JDBC-executable target
     schema-entities   - Entities map from (query/model->schema model),
                         each value carrying :table, :fields,
                         :field->attribute pre-resolved.

   Idempotent. Side effects only. Returns nil."
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
          (install-entity-triggers! tx (str entity-id) entity
                                    (audit/audited-entity? (str entity-id)))
          (catch Throwable e
            (log/error! {:id ::entity-trigger-install-failed
                         :data {:table (:table entity)
                                :entity-id (str entity-id)}} e))))))
  nil)

(defn set-context!
  "Write per-tx request/principal context so substrate triggers can record
   who drove the mutation.

   ISOLATION CAVEAT (single-process scope is enough): _ctx is a regular
   table; if connection A writes actor='alice' and connection B does a
   RAW mutation (bypassing set-context!), B's trigger reads 'alice' —
   misattribution. Single-writer SQLite serializes concurrent writers so
   the trigger in a tx that DID call set-context! always sees its own
   write first. Acceptable for the dev / small-on-prem scope this path
   targets."
  [_db tx]
  ;; First-deploy ordering: on a fresh DB the dataset setup hook calls
  ;; set-entity (→ set-context!) before reconcile-* has run. Create _ctx
  ;; defensively so cold-start works.
  (jdbc/execute! tx [audit-ctx-ddl])
  (let [principal (access/current-principal)]
    (jdbc/execute! tx
                   ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('actor_xid', ?)"
                    (some-> principal :xid str)])
    (jdbc/execute! tx
                   ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('request_id', ?)"
                    (some-> principal :request-id str)])
    (jdbc/execute! tx
                   ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('scope_xid', ?)"
                    (some-> principal :scope-xid str)]))
  ;; Install-level tenant identifier — every envelope is stamped with
  ;; the install's primary dataset xid. Becomes per-request when the
  ;; principal projection grows :tenant-xid.
  (jdbc/execute! tx
                 ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('tenant_xid', ?)"
                  (str (id/data :dataset/id))])
  ;; Per-transaction opaque id — SQLite has no `txid_current()` equivalent
  ;; so we synthesize. UUIDv7 (RFC 9562): time-prefix means tx ids are
  ;; naturally chronological — `ORDER BY txid` matches `ORDER BY ts`
  ;; without an extra column.
  (jdbc/execute! tx
                 ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('txid', ?)"
                  (str (uuid-v7))])
  nil)

(defn start-drainer!
  "Start the substrate drainer thread. Initializes the active
   WakeSource and parks the loop on it. Idempotent."
  [db]
  (if @drainer-state
    (log/info {:id ::drainer-already-running
               :data {:action :starting :subject :relation-drainer}}
              "Substrate drainer already running; ignoring start")
    (let [;; Single-row monotonic delta cursor. SQLite serializes ALL writers
          ;; (one write txn at a time) and the drainer is the sole writer of
          ;; this row, so a read-then-bump in the drain txn is gap-free with no
          ;; lock needed — the PG advisory-lock election is unnecessary here.
          _  (jdbc/execute! (:datasource db)
               ["CREATE TABLE IF NOT EXISTS __delta_seq (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"])
          _  (jdbc/execute! (:datasource db)
               ["INSERT OR IGNORE INTO __delta_seq (id, value) VALUES (0, 0)"])
          ws wake/*wake-source*
          _  (wake/start-source! ws)
          stop? (atom false)
          t (Thread. ^Runnable #(drainer-loop! db ws stop?)
                     "synthigy-relation-drainer-sqlite")]
      (.setDaemon t true)
      (.start t)
      (reset! drainer-state {:thread t :stop? stop? :wake-source ws})
      nil)))

(defn stop-drainer!
  "Cleanly stop the substrate drainer and tear down its wake source.
   Idempotent — calling when not running is a no-op."
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

(defn reconcile-substrate!
  "Idempotent full resync of the SQLite delta substrate against `model`.

   Walks the deployed schema, then reconciles entity-side and relation-side
   triggers in a single pass. Called by `:synthigy/subscriptions.sqlite`
   on lifecycle :start and on every `save-model!` via
   `dataset/add-model-watch!`. No-op when `model` is nil.

   Idempotent: each `install-*-triggers!` pattern-drops then re-creates.
   Partial-failure recovery is 'next call reconciles again.'"
  [db model]
  (when model
    ;; Freshen the audit policy from THIS model before baking trigger flags so
    ;; the frozen-at-enqueue audit decision is model-derived and independent of
    ;; watch-firing order. Mirrors the Postgres substrate.
    (audit/recompile-policy! model)
    (let [schema ((requiring-resolve 'synthigy.dataset.sql.query/model->schema) model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn strip-all!
  "Drop every substrate trigger across `sqlite_master`. Used on
   `:synthigy/subscriptions.sqlite` :stop (clean removal) and as the
   bare-server upgrader helper.

   Leaves `__relation_delta_queue`, `__entity_delta_queue`, and `_ctx`
   in place — cheap tables; re-:start reconciles triggers back.

   Idempotent. Safe to call multiple times."
  [_db tx]
  (let [stale (->> (jdbc/execute! tx
                     [(str "SELECT name FROM sqlite_master "
                           "WHERE type = 'trigger' "
                           "AND (name LIKE 'trg_%_entity_audit_%' "
                           "  OR name LIKE 'trg_%_audit_insert' "
                           "  OR name LIKE 'trg_%_audit_delete')")])
                   (mapv :sqlite_master/name)
                   (remove nil?))]
    (doseq [trg stale]
      (jdbc/execute! tx [(format "DROP TRIGGER IF EXISTS \"%s\"" trg)])))
  nil)
