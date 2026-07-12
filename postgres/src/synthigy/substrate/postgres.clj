(ns synthigy.substrate.postgres
  "PG-side delta substrate — DDL, trigger generation, drainer thread.

   Owned by `:synthigy/subscriptions.postgres` (OSS). The substrate captures
   every entity/relation mutation into queue tables, fans them out via a
   drainer thread, and provides `set-context!` to stamp request provenance
   on tx-local GUCs.

   Consumers:
     - SSE subscriptions (OSS) — live notifications over `/data/events`
     - observability substrate (`:synthigy/observability` — DuckDB OSS,
       ClickHouse Pro) — persists envelopes for `/history` queries

   PG-specific deltas vs SQLite:
     - LISTEN/NOTIFY → drainer parks on `getNotifications`
     - GUC (`current_setting` / `set_config`) → no `_ctx` table
     - SKIP LOCKED → multi-drainer-safe
     - `plpgsql` trigger functions → one function per entity table, shared
       across INSERT/UPDATE/DELETE via TG_OP switch
     - `txid_current()` is native → no UUIDv7 synthesis needed"
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [synthigy.audit :as audit]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.delta :as delta]
    [synthigy.dataset.id :as id]
    [synthigy.dataset.sql.query :as sql-query]
    [synthigy.json :refer [<-json]]
    [synthigy.log :as log]
    [synthigy.substrate.wake :as wake]
    [synthigy.substrate.wake.postgres :as wake-pg]))

;; ============================================================================
;; Wake source — delegate to shared `synthigy.substrate.wake/*wake-source*`.
;;
;; The PG default (PostgresNotify on `synthigy_delta_ready`) is installed
;; by `:synthigy/subscriptions.postgres` :start. Operators wanting
;; cross-cluster fanout can alter-var-root `wake/*wake-source*` to a
;; Composite of PostgresNotify + NATS / Kafka / changefeed before :start.
;; ============================================================================

(defn wake-drainer!
  "Backend-named alias for `synthigy.substrate.wake/signal-drainer!`.
   For the default PostgresNotify source, `signal!` is a no-op (the
   trigger fires pg_notify), so this is essentially free."
  []
  (wake/signal-drainer!))

;; ============================================================================
;; Queue + trigger function DDL
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

;; STATEMENT-level audit trigger function — fires once per INSERT/DELETE on
;; a link table and bulk-enqueues one queue row per affected link row from
;; the new_rows/old_rows transition tables (set-based, not per-row). Generic
;; across link tables: the from/to column names arrive via TG_ARGV and the
;; INSERT…SELECT is built with format(). The format template is dollar-quoted
;; ($fmt$…$fmt$) so the SQL's own single quotes need no escaping. Does NOT
;; pg_notify — that's the separate statement-level notify trigger's job.
(def ^:private relation-emit-fn-ddl
  "Shared relation-delta emit function. TG_ARGV[0]=relation-xid, [1]/[2]=from/to
   columns, [3]='true'/'false' audit flag. The `'audit'` flag is frozen into the
   payload at enqueue time (decided by the model at trigger-install) so the
   drainer never re-evaluates the mutable policy at drain time — same
   freeze-at-write contract as the entity emit function."
  "CREATE OR REPLACE FUNCTION synthigy_emit_relation_delta()
   RETURNS TRIGGER AS $$
   BEGIN
     IF (TG_OP = 'INSERT') THEN
       EXECUTE format(
         $fmt$INSERT INTO __relation_delta_queue (kind, payload)
              SELECT 'relation-mutation', jsonb_build_object(
                'v',            1,
                'relation_xid', $1,
                'audit',        $2,
                'op',           'link',
                'from_eid',     n.%I,
                'to_eid',       n.%I,
                'from_xid',     n.from_xid,
                'to_xid',       n.to_xid,
                'tenant_xid',   NULLIF(current_setting('synthigy.tenant_xid', true), ''),
                'actor_xid',    NULLIF(current_setting('synthigy.actor_xid',  true), ''),
                'request_id',   NULLIF(current_setting('synthigy.request_id', true), ''),
                'scope_xid',    NULLIF(current_setting('synthigy.scope_xid',  true), ''),
                'txid',         txid_current())
              FROM new_rows n$fmt$,
         TG_ARGV[1], TG_ARGV[2])
       USING TG_ARGV[0], (TG_ARGV[3] = 'true');
     ELSE
       EXECUTE format(
         $fmt$INSERT INTO __relation_delta_queue (kind, payload)
              SELECT 'relation-mutation', jsonb_build_object(
                'v',            1,
                'relation_xid', $1,
                'audit',        $2,
                'op',           'unlink',
                'from_eid',     o.%I,
                'to_eid',       o.%I,
                'from_xid',     o.from_xid,
                'to_xid',       o.to_xid,
                'tenant_xid',   NULLIF(current_setting('synthigy.tenant_xid', true), ''),
                'actor_xid',    NULLIF(current_setting('synthigy.actor_xid',  true), ''),
                'request_id',   NULLIF(current_setting('synthigy.request_id', true), ''),
                'scope_xid',    NULLIF(current_setting('synthigy.scope_xid',  true), ''),
                'txid',         txid_current())
              FROM old_rows o$fmt$,
         TG_ARGV[1], TG_ARGV[2])
       USING TG_ARGV[0], (TG_ARGV[3] = 'true');
     END IF;
     RETURN NULL;
   END $$ LANGUAGE plpgsql")

;; Statement-level notify trigger function — fires ONCE per mutation
;; statement (regardless of row count). Single pg_notify per statement
;; means a 100k-row slice produces 1 wakeup instead of 100k.
(def ^:private notify-fn-ddl
  "CREATE OR REPLACE FUNCTION synthigy_notify_delta_ready()
   RETURNS TRIGGER AS $$
   BEGIN
     PERFORM pg_notify('synthigy_delta_ready', '');
     RETURN NULL;
   END $$ LANGUAGE plpgsql")

;; BEFORE INSERT populate trigger — fills NEW.from_xid / NEW.to_xid when the
;; caller didn't supply them (raw SQL inserts that bypass link-relations).
;;
;; Fast path: link-relations always populates both xids in xid mode (see
;; project-saved-entities / link-relations 2026-05-20). For that hot path
;; the trigger should be ~free. The dynamic-SQL field extraction + lookup
;; is only needed when a row arrives missing one or both xids (raw
;; back-channel writes). We early-return when both are present to skip
;; ~100k EXECUTEs on a 100k-rating import.
(def ^:private relation-xid-populate-fn-ddl
  "CREATE OR REPLACE FUNCTION synthigy_relation_xid_populate()
   RETURNS TRIGGER AS $$
   DECLARE
     v_from BIGINT;
     v_to   BIGINT;
   BEGIN
     IF NEW.from_xid IS NOT NULL AND NEW.to_xid IS NOT NULL THEN
       RETURN NEW;
     END IF;
     EXECUTE format('SELECT ($1).%I, ($1).%I', TG_ARGV[2], TG_ARGV[3])
       INTO v_from, v_to USING NEW;
     IF NEW.from_xid IS NULL THEN
       EXECUTE format('SELECT xid FROM %I WHERE _eid = $1', TG_ARGV[0])
         INTO NEW.from_xid USING v_from;
     END IF;
     IF NEW.to_xid IS NULL THEN
       EXECUTE format('SELECT xid FROM %I WHERE _eid = $1', TG_ARGV[1])
         INTO NEW.to_xid USING v_to;
     END IF;
     RETURN NEW;
   END $$ LANGUAGE plpgsql")

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- migrate-legacy-queue!
  "Idempotent reconciler from the 1.2.0/1.3.0 schema (`__delta_queue` with
   column `seq`) to the 1.4.0+ schema (`__relation_delta_queue` with column
   `id`). Safe to call on every deploy."
  [tx]
  (let [has-old? (seq (jdbc/execute! tx
                        ["SELECT 1 FROM information_schema.tables
                          WHERE table_schema = 'public'
                            AND table_name   = '__delta_queue'"]))
        has-new? (seq (jdbc/execute! tx
                        ["SELECT 1 FROM information_schema.tables
                          WHERE table_schema = 'public'
                            AND table_name   = '__relation_delta_queue'"]))]
    (cond
      (and has-old? has-new?)
      (do
        (jdbc/execute! tx
          ["INSERT INTO __relation_delta_queue (ts, kind, payload)
            SELECT ts, kind, payload FROM __delta_queue"])
        (jdbc/execute! tx ["DROP TABLE __delta_queue"]))

      has-old?
      (jdbc/execute! tx
        ["ALTER TABLE __delta_queue RENAME TO __relation_delta_queue"])

      :else nil))
  (when (seq (jdbc/execute! tx
               ["SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name   = '__relation_delta_queue'
                   AND column_name  = 'seq'"]))
    (jdbc/execute! tx
      ["ALTER TABLE __relation_delta_queue RENAME COLUMN seq TO id"])))

(defn- ensure-xid-columns!
  "Idempotently add from_xid / to_xid columns to a relation link table."
  [tx table]
  (jdbc/execute! tx
    [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS from_xid TEXT" table)])
  (jdbc/execute! tx
    [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS to_xid TEXT" table)]))

(defn- ensure-entity-xid-columns!
  "Per the `xid_structural` invariant every entity row should carry an `xid`.
   Walks the parent tables of every relation in the schema and adds the
   column where missing."
  [_tx relations]
  (let [parent-tables (->> relations
                           (mapcat (juxt :from/table :to/table))
                           (remove nil?)
                           set)]
    (doseq [t parent-tables]
      (try
        (jdbc/execute! _tx
          [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS xid VARCHAR(64)" t)])
        (catch Throwable e
          (log/warn {:id ::entity-xid-column-add-failed
                     :data {:table t}}
                    (.getMessage e)))))))

(defn- relation-missing?
  "True when `e` is Postgres' 'relation \"x\" does not exist' (SQLSTATE 42P01).
   Means the deployed model references a table that isn't physically present —
   expected drift (deploy hasn't created it yet, or it was dropped externally),
   not an actionable substrate error. Reconcile skips & warns rather than errors."
  [^Throwable e]
  (or (and (instance? java.sql.SQLException e)
           (= "42P01" (.getSQLState ^java.sql.SQLException e)))
      (boolean (some-> (.getMessage e) (str/includes? "does not exist")))))

(defn- backfill-xid-columns!
  "Backfill from_xid / to_xid from the entity tables. Only updates rows
   where the column is still NULL. No-op in :euuid mode."
  [tx {table      :table
       from-table :from/table
       to-table   :to/table
       from-field :from/field
       to-field   :to/field}]
  (when (= :xid (id/key))
    (jdbc/execute! tx
      [(format (str "UPDATE \"%s\" rel"
                    " SET from_xid = src.xid"
                    " FROM \"%s\" src"
                    " WHERE src._eid = rel.\"%s\" AND rel.from_xid IS NULL")
               table from-table (name from-field))])
    (jdbc/execute! tx
      [(format (str "UPDATE \"%s\" rel"
                    " SET to_xid = src.xid"
                    " FROM \"%s\" src"
                    " WHERE src._eid = rel.\"%s\" AND rel.to_xid IS NULL")
               table to-table (name to-field))])))

(defn- install-relation-trigger!
  "Attach the relation-substrate triggers to one link table. Idempotent.
   `audit?` is frozen into the emit trigger's TG_ARGV so the drainer's
   persistence decision is made at enqueue time, not drain time."
  [tx {rel-id      :relation
       table       :table
       from        :from/field
       to          :to/field
       from-table  :from/table
       to-table    :to/table}
   audit?]
  (ensure-xid-columns! tx table)
  (jdbc/execute! tx
    [(format "DROP TRIGGER IF EXISTS trg_relation_xid_populate ON \"%s\"" table)])
  (when (= :xid (id/key))
    (jdbc/execute! tx
      [(format "CREATE TRIGGER trg_relation_xid_populate
                BEFORE INSERT ON \"%s\"
                FOR EACH ROW
                EXECUTE FUNCTION synthigy_relation_xid_populate('%s', '%s', '%s', '%s')"
               table from-table to-table (name from) (name to))]))
  (doseq [trg ["trg_relation_audit" "trg_relation_audit_ins" "trg_relation_audit_del"]]
    (jdbc/execute! tx [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trg table)]))
  ;; STATEMENT-level — one trigger per op so each carries exactly the
  ;; transition table its event provides; the shared function bulk-enqueues.
  (jdbc/execute! tx
    [(format "CREATE TRIGGER trg_relation_audit_ins
              AFTER INSERT ON \"%s\"
              REFERENCING NEW TABLE AS new_rows
              FOR EACH STATEMENT
              EXECUTE FUNCTION synthigy_emit_relation_delta('%s', '%s', '%s', '%s')"
             table (str rel-id) (name from) (name to) (if audit? "true" "false"))])
  (jdbc/execute! tx
    [(format "CREATE TRIGGER trg_relation_audit_del
              AFTER DELETE ON \"%s\"
              REFERENCING OLD TABLE AS old_rows
              FOR EACH STATEMENT
              EXECUTE FUNCTION synthigy_emit_relation_delta('%s', '%s', '%s', '%s')"
             table (str rel-id) (name from) (name to) (if audit? "true" "false"))])
  (jdbc/execute! tx
    [(format "DROP TRIGGER IF EXISTS trg_relation_notify ON \"%s\"" table)])
  (jdbc/execute! tx
    [(format "CREATE TRIGGER trg_relation_notify
              AFTER INSERT OR DELETE ON \"%s\"
              FOR EACH STATEMENT
              EXECUTE FUNCTION synthigy_notify_delta_ready()"
             table)]))

;; ============================================================================
;; Entity substrate — xid-keyed function + trigger names
;; ============================================================================

(defn- entity-emit-fn-name
  "Xid-keyed emit-function name. Stable across table renames; prevents
   orphan accumulation in pg_proc."
  [entity-xid]
  (str "synthigy_emit_" entity-xid "_delta"))

(defn- entity-trigger-name
  "Xid-keyed trigger names per (entity-xid, kind). Same stability story
   as the function name."
  [entity-xid kind]
  (str "trg_" entity-xid "_entity_" (name kind)))

(defn- entity-user-fields
  "Walk an entity schema entry's :field->attribute and return
   {column-name attribute-xid ...} for user-defined fields only."
  [entity]
  (->> (:field->attribute entity)
       (filter (fn [[_col attr-xid]] (string? attr-xid)))
       (into (sorted-map))))

(defn- jsonb-build-pairs
  "Emit a sequence of `'<attr-xid>', <ref>.<col>` arg pairs suitable for
   jsonb_build_object(...)."
  [field-map ref]
  (->> field-map
       (mapv (fn [[col attr-xid]]
               ;; Quote the column identifier — entity attributes can map to
               ;; columns whose names are SQL reserved words (e.g. "from" on
               ;; the meta "Dataset Relation" entity); unquoted `NEW.from`
               ;; fails to parse in the plpgsql trigger body.
               (format "'%s', %s.\"%s\"" attr-xid ref (name col))))
       (str/join ", ")))

(defn- attribute-when-distinct
  "OR'd chain of `<old>.col IS DISTINCT FROM <new>.col` predicates — the
   phantom-UPDATE filter. A statement-trigger UPDATE that rewrites every
   attribute unchanged then produces no queue row."
  [field-map old-ref new-ref]
  (->> field-map
       (map (fn [[col _attr-xid]]
              ;; Quote — see jsonb-build-pairs (reserved-word column names).
              (format "%s.\"%s\" IS DISTINCT FROM %s.\"%s\""
                      old-ref (name col) new-ref (name col))))
       (str/join " OR ")))

(defn- entity-emit-fn-ddl
  "Per-entity-table STATEMENT-level trigger function. Fires once per
   INSERT/UPDATE/DELETE statement and bulk-enqueues one delta row per
   affected row via the `new_rows`/`old_rows` transition tables — set-based,
   replacing the old FOR EACH ROW trigger (which did one INSERT per row).
   The column → attribute_xid mapping is baked at deploy; the function name
   is xid-keyed so renames don't accumulate orphans in pg_proc.

   `audit?` stamps a frozen `'audit'` flag into every payload: the
   audit-persistence decision is made HERE, at enqueue time, from the model
   being deployed — not re-evaluated at drain time against the mutable policy
   atom, which could change between a write committing and the async drainer
   waking (silently dropping already-committed records). The drainer trusts
   this flag; live dispatch ignores it. Re-baked on every reconcile, so a
   model audit-config change reinstalls the trigger with the new decision."
  [entity-xid entity audit?]
  (let [fields  (entity-user-fields entity)
        n-pairs (jsonb-build-pairs fields "n")
        o-pairs (jsonb-build-pairs fields "o")
        changed (attribute-when-distinct fields "o" "n")
        al      (if audit? "true" "false")]
    (format
      "CREATE OR REPLACE FUNCTION %s()
       RETURNS TRIGGER AS $$
       BEGIN
         IF (TG_OP = 'INSERT') THEN
           INSERT INTO __entity_delta_queue (payload)
           SELECT jsonb_build_object(
             'v',          1,
             'op',         'insert',
             'record_xid', n.xid,
             'entity_xid', '%s',
             'audit',      %s,
             'tenant_xid', NULLIF(current_setting('synthigy.tenant_xid', true), ''),
             'actor_xid',  NULLIF(current_setting('synthigy.actor_xid',  true), ''),
             'request_id', NULLIF(current_setting('synthigy.request_id', true), ''),
             'scope_xid',  NULLIF(current_setting('synthigy.scope_xid',  true), ''),
             'txid',       txid_current(),
             'after',      jsonb_build_object(%s))
           FROM new_rows n;
         ELSIF (TG_OP = 'UPDATE') THEN
           INSERT INTO __entity_delta_queue (payload)
           SELECT jsonb_build_object(
             'v',          1,
             'op',         'update',
             'record_xid', n.xid,
             'entity_xid', '%s',
             'audit',      %s,
             'tenant_xid', NULLIF(current_setting('synthigy.tenant_xid', true), ''),
             'actor_xid',  NULLIF(current_setting('synthigy.actor_xid',  true), ''),
             'request_id', NULLIF(current_setting('synthigy.request_id', true), ''),
             'scope_xid',  NULLIF(current_setting('synthigy.scope_xid',  true), ''),
             'txid',       txid_current(),
             'before',     jsonb_build_object(%s),
             'after',      jsonb_build_object(%s))
           FROM new_rows n JOIN old_rows o ON n._eid = o._eid
           WHERE %s;
         ELSE
           INSERT INTO __entity_delta_queue (payload)
           SELECT jsonb_build_object(
             'v',          1,
             'op',         'delete',
             'record_xid', o.xid,
             'entity_xid', '%s',
             'audit',      %s,
             'tenant_xid', NULLIF(current_setting('synthigy.tenant_xid', true), ''),
             'actor_xid',  NULLIF(current_setting('synthigy.actor_xid',  true), ''),
             'request_id', NULLIF(current_setting('synthigy.request_id', true), ''),
             'scope_xid',  NULLIF(current_setting('synthigy.scope_xid',  true), ''),
             'txid',       txid_current(),
             'before',     jsonb_build_object(%s))
           FROM old_rows o;
         END IF;
         RETURN NULL;
       END $$ LANGUAGE plpgsql"
      (entity-emit-fn-name entity-xid)
      entity-xid al n-pairs
      entity-xid al o-pairs n-pairs changed
      entity-xid al o-pairs)))

(defn- drop-stale-substrate-triggers-on-table!
  "Drop every substrate-track trigger attached to `table`, regardless of
   name shape. Pattern-matches the legacy table-name-keyed form
   (`trg_<table>_…`) AND the xid-keyed form (`trg_<entity_xid>_entity_…`).
   Idempotent — runs on every deploy and is what makes the
   table-name → xid-name rename side-effect-free."
  [tx table]
  (let [stale (jdbc/execute! tx
                ["SELECT tgname AS trigger_name
                  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE NOT t.tgisinternal
                    AND n.nspname = 'public'
                    AND c.relname = ?
                    AND (tgname LIKE 'trg\\_%\\_entity\\_audit%' ESCAPE '\\'
                      OR tgname LIKE 'trg\\_%\\_entity\\_notify' ESCAPE '\\')"
                 table])]
    (doseq [{trigger :pg_trigger/trigger_name} stale]
      (jdbc/execute! tx
        [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger table)]))))

(defn- install-entity-triggers!
  "(Re-)create the per-table trigger function and attach AFTER row-level
   + STATEMENT-level NOTIFY triggers. Idempotent — pattern-drops every
   substrate trigger on the table first so renames + table-name → xid-name
   migrations leave no orphans. Skips entities with no user-defined
   attributes."
  [tx entity-xid {table :table :as entity} audit?]
  (when (seq (entity-user-fields entity))
    (let [fn-name         (entity-emit-fn-name entity-xid)
          audit-trg-name  (entity-trigger-name entity-xid :audit)
          notify-trg-name (entity-trigger-name entity-xid :notify)]
      (drop-stale-substrate-triggers-on-table! tx table)
      (jdbc/execute! tx [(entity-emit-fn-ddl entity-xid entity audit?)])
      ;; STATEMENT-level audit triggers — one per op so each carries exactly
      ;; the transition tables its event provides; the shared function
      ;; branches on TG_OP and bulk-enqueues from new_rows / old_rows.
      (jdbc/execute! tx
        [(format "CREATE TRIGGER %s_ins AFTER INSERT ON \"%s\"
                  REFERENCING NEW TABLE AS new_rows
                  FOR EACH STATEMENT EXECUTE FUNCTION %s()"
                 audit-trg-name table fn-name)])
      (jdbc/execute! tx
        [(format "CREATE TRIGGER %s_upd AFTER UPDATE ON \"%s\"
                  REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
                  FOR EACH STATEMENT EXECUTE FUNCTION %s()"
                 audit-trg-name table fn-name)])
      (jdbc/execute! tx
        [(format "CREATE TRIGGER %s_del AFTER DELETE ON \"%s\"
                  REFERENCING OLD TABLE AS old_rows
                  FOR EACH STATEMENT EXECUTE FUNCTION %s()"
                 audit-trg-name table fn-name)])
      (jdbc/execute! tx
        [(format "CREATE TRIGGER %s
                  AFTER INSERT OR UPDATE OR DELETE ON \"%s\"
                  FOR EACH STATEMENT
                  EXECUTE FUNCTION synthigy_notify_delta_ready()"
                 notify-trg-name table)]))))

;; ============================================================================
;; Drainer — LISTEN/NOTIFY loop, multi-instance via SKIP LOCKED
;; ============================================================================

(def ^:private drain-batch-size 2000)  ; ponytail: claim window; 500→2000 cuts lock cycles ~4x under burst
(def ^:private listen-poll-ms 10000)

;; Single-sequencer election. Only the transaction holding this advisory lock
;; drains a batch, so `nextval('delta_seq')` is assigned by one drainer at a
;; time — gap-free in drain order, immune to the MVCC commit-order-vs-serial
;; drift that bit a trigger-assigned id. The lock is transaction-scoped
;; (`pg_try_advisory_xact_lock`): it auto-releases at commit, and a crashed
;; leader's lock vanishes instantly, so a standby's next tick takes over with
;; no heartbeat/lease machinery. Key is an arbitrary fixed bigint ("DELTA").
(def ^:private delta-drainer-lock-key 0x44454C5441)

(defonce ^:private drainer-state (atom nil))

(defn- row->envelope
  [{ts      :__relation_delta_queue/ts
    payload :__relation_delta_queue/payload}]
  (let [data (<-json (.getValue payload))]
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
   Carries :txid so downstream substrate writes (and /history grouping
   queries) can use it as a per-tx grouping key."
  [{ts      :__entity_delta_queue/ts
    payload :__entity_delta_queue/payload}]
  (let [data (<-json (.getValue payload))]
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

(defn- acquire-drainer-lock!
  "Try to take the transaction-scoped drainer advisory lock. Returns true
   iff this transaction is now the sole sequencer. Non-blocking — a standby
   that loses the race gets false and simply skips this batch. Released
   automatically when `tx` commits or rolls back."
  [tx]
  (boolean (:locked (jdbc/execute-one! tx
                      ["SELECT pg_try_advisory_xact_lock(?) AS locked"
                       delta-drainer-lock-key]))))

(defn- next-seqs!
  "Allocate `n` `delta_seq` values in a single round-trip, ascending. Called
   only while holding the drainer lock, so the values are monotonic in drain
   order across the whole deployment."
  [tx n]
  (mapv :seq
        (jdbc/execute! tx
          ["SELECT nextval('delta_seq') AS seq FROM generate_series(1, ?)" n])))

(defn- drain-batch!
  "Claim up to N rows from __relation_delta_queue under SELECT FOR UPDATE
   SKIP LOCKED, stamp each with a monotonic `:seq`, persist them via the
   audit provider, and DELETE the claimed rows — all in the caller's
   transaction. Returns the envelopes so the caller can publish them to
   live subscribers AFTER commit. Returns nil when another drainer holds
   the sequencer lock (this instance stands by).

   Durability (audit write + queue delete) is atomic and must never
   depend on, or roll back for, a slow/stuck live subscriber — hence the
   publish is deliberately NOT done here."
  [tx]
  (when (acquire-drainer-lock! tx)
    (let [rows (jdbc/execute! tx
                 [(str "SELECT id, ts, payload FROM __relation_delta_queue "
                       "ORDER BY id LIMIT " drain-batch-size " "
                       "FOR UPDATE SKIP LOCKED")])]
      (when (seq rows)
        (let [envelopes (mapv (fn [row sq] (assoc (row->envelope row) :seq sq))
                              rows (next-seqs! tx (count rows)))]
          ;; Persist only envelopes whose audit flag was frozen true at enqueue;
          ;; live dispatch (returned `envelopes`) still gets every delta.
          (if-let [provider audit/*audit-provider*]
            (binding [audit/*audit-tx* tx]
              (audit/write-relation-deltas! provider (filterv :audit-persist? envelopes)))
            (audit/warn-missing-provider!))
          (jdbc/execute! tx
            [(str "DELETE FROM __relation_delta_queue WHERE id IN ("
                  (str/join "," (map :__relation_delta_queue/id rows))
                  ")")])
          envelopes)))))

(defn- drain-entity-batch!
  "Entity-queue counterpart of `drain-batch!` — sequencer-locked, stamp
   `:seq`, persist + delete in `tx`, return the envelopes for post-commit
   publishing. Returns nil when another drainer holds the lock."
  [tx]
  (when (acquire-drainer-lock! tx)
    (let [rows (jdbc/execute! tx
                 [(str "SELECT id, ts, payload FROM __entity_delta_queue "
                       "ORDER BY id LIMIT " drain-batch-size " "
                       "FOR UPDATE SKIP LOCKED")])]
      (when (seq rows)
        (let [envelopes (mapv (fn [row sq] (assoc (entity-row->envelope row) :seq sq))
                              rows (next-seqs! tx (count rows)))]
          ;; Persist only envelopes whose audit flag was frozen true at enqueue;
          ;; live dispatch (returned `envelopes`) still gets every delta.
          (if-let [provider audit/*audit-provider*]
            (binding [audit/*audit-tx* tx]
              (audit/write-entity-deltas! provider (filterv :audit-persist? envelopes)))
            (audit/warn-missing-provider!))
          (jdbc/execute! tx
            [(str "DELETE FROM __entity_delta_queue WHERE id IN ("
                  (str/join "," (map :__entity_delta_queue/id rows))
                  ")")])
          envelopes)))))

(defn- publish-envelopes!
  "Fan a drained, already-persisted batch out to live subscribers —
   best-effort, AFTER the drain transaction has committed. A slow or
   stuck subscriber can no longer roll back audit persistence; a
   dropped live notification is recovered on the subscriber's next
   reconnect/backfill via /history."
  [envelopes]
  (doseq [envelope envelopes]
    (try
      (delta/dispatch! envelope)
      (catch Throwable e
        (log/error! {:id ::publish-failed
                     :msg "Failed to publish delta envelope"
                     :data {:action :publishing :subject :delta}}
                    e)))))

(defn- drainer-datasource
  "Pick the drainer's connection source. Prefer the dedicated drainer
   pool (assoc'd onto the Postgres record by `synthigy.db.postgres/start`)
   so the drainer never competes with the writer pool. Falls back to the
   shared writer pool when no drainer pool is configured — keeps test
   fixtures + alt backends working unchanged."
  [db]
  (or (:drainer-datasource db) (:datasource db)))

(defn- drainer-tick!
  [db]
  (loop []
    (let [ds       (drainer-datasource db)
          rel-envs (try
                     (jdbc/with-transaction [tx ds]
                       (drain-batch! tx))
                     (catch Throwable e
                       (log/error! {:id ::drain-batch-failed} e)
                       nil))
          _        (publish-envelopes! rel-envs)
          ent-envs (try
                     (jdbc/with-transaction [tx ds]
                       (drain-entity-batch! tx))
                     (catch Throwable e
                       (log/error! {:id ::drain-entity-batch-failed} e)
                       nil))
          _        (publish-envelopes! ent-envs)]
      (when (or (= (count rel-envs) drain-batch-size)
                (= (count ent-envs) drain-batch-size))
        (recur)))))

(defn- drainer-loop!
  "Worker body. Parks on the registered WakeSource (default
   PostgresNotify, with `signal!` driven by the trigger's pg_notify).
   `wait!` returns :wakeup or :poll; in either case we drain. The
   PostgresNotify implementation translates `timeout-ms` into a
   blocking `getNotifications(timeout-ms)`, preserving the prior
   listen-poll latency."
  [db wake-source stop?]
  (try
    (log/info {:id ::drainer-listening
               :data {:action :started :subject :relation-drainer
                      :wake-source (.getName (class wake-source))}}
              "Relation drainer listening on WakeSource")
    (while (not @stop?)
      (try
        (case (wake/wait! wake-source listen-poll-ms)
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
                "Relation drainer loop exited"))))

(defn- drop-stale-relation-triggers!
  "Drop every relation substrate/notify/populate trigger across the whole
   public schema before re-installing on the current schema's relations."
  [tx]
  (let [staleish
        (jdbc/execute! tx
          ["SELECT tgname AS trigger_name, c.relname AS table_name
            FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
            WHERE NOT t.tgisinternal
              AND tgname IN ('trg_relation_audit',
                             'trg_relation_audit_ins',
                             'trg_relation_audit_del',
                             'trg_relation_notify',
                             'trg_relation_xid_populate')"])]
    (doseq [{table :pg_class/table_name
             trigger :pg_trigger/trigger_name} staleish]
      (jdbc/execute! tx
        [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger table)]))))

(defn- drop-stale-entity-fns!
  "Drop every legacy `synthigy_emit_<table>_delta()` function from pg_proc
   so a re-install picks up the xid-keyed naming cleanly. Idempotent —
   functions still in use are recreated as part of the deploy."
  [tx]
  (let [staleish
        (jdbc/execute! tx
          ["SELECT proname FROM pg_proc
            JOIN pg_namespace n ON n.oid = pronamespace
            WHERE n.nspname = 'public'
              AND proname LIKE 'synthigy\\_emit\\_%\\_delta' ESCAPE '\\'
              AND proname <> 'synthigy_emit_relation_delta'"])]
    (doseq [{p :pg_proc/proname} staleish]
      (try
        (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS %s() CASCADE" p)])
        (catch Throwable e
          (log/warn {:id ::stale-fn-drop-failed
                     :data {:proname p}}
                    (.getMessage e)))))))

;; ============================================================================
;; Public API — substrate reconcile + drainer + context
;; ============================================================================

(defn reconcile-relations!
  "Deploy relation-substrate infrastructure: idempotent prelude (queue table +
   plpgsql trigger functions) followed by per-relation triggers on every
   relation in the deployed schema.

   Args:
     _db               - Postgres backend instance (unused; kept for symmetry)
     tx                - JDBC-executable target (connection / datasource)
     schema-relations  - Relations map or seq from (query/model->schema model),
                         each value carrying :relation, :relation/table,
                         :from/field, :to/field pre-resolved.

   Idempotent. Side effects only. Returns nil."
  [_db tx schema-relations]
  (migrate-legacy-queue! tx)
  (drop-stale-relation-triggers! tx)
  (jdbc/execute! tx [delta-queue-ddl])
  (jdbc/execute! tx [relation-emit-fn-ddl])
  (jdbc/execute! tx [notify-fn-ddl])
  (jdbc/execute! tx [relation-xid-populate-fn-ddl])
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
    (ensure-entity-xid-columns! tx (vals unique-by-table))
    (doseq [rel (vals unique-by-table)]
      (install-relation-trigger! tx rel (audit/audited-relation? (str (:relation rel)))))
    (doseq [rel (vals unique-by-table)]
      (try
        (backfill-xid-columns! tx rel)
        (catch Throwable e
          (if (relation-missing? e)
            (log/warn {:id ::table-missing-skipped
                       :data {:action :skipped :subject :substrate
                              :phase :backfill :table (:table rel)}}
                      "Table for relation missing; skipping backfill (model/schema drift)")
            (log/error! {:id ::backfill-failed
                         :data {:table (:table rel)}} e))))))
  nil)

(defn reconcile-entities!
  "Deploy entity-substrate infrastructure: idempotent prelude (entity queue
   table) followed by per-entity triggers across every entity in the
   deployed schema.

   Args:
     _db               - Postgres backend instance (unused; kept for symmetry)
     tx                - JDBC-executable target (connection / datasource)
     schema-entities   - Entities map from (query/model->schema model),
                         each value carrying :table, :fields,
                         :field->attribute pre-resolved.

   Idempotent. Side effects only. Returns nil. Skipped in :euuid mode
   because entity tables don't carry an `xid` column then."
  [_db tx schema-entities]
  (if (not= :xid (id/key))
    (log/info {:id ::entity-substrate-skipped-euuid
               :data {:action :skipped :subject :entity-substrate
                      :reason "id-key is :euuid; xid columns absent on entity tables"}}
              "Entity substrate triggers skipped (euuid mode)")
    (do
      (drop-stale-entity-fns! tx)
      (jdbc/execute! tx [entity-queue-ddl])
      (let [pairs (cond
                    (map? schema-entities) (seq schema-entities)
                    (sequential? schema-entities)
                    (map (fn [[id ent]] [id ent]) schema-entities)
                    :else nil)]
        (doseq [[entity-id entity] pairs]
          (when (:table entity)
            (try
              (install-entity-triggers! tx (str entity-id) entity
                                        (audit/audited-entity? (str entity-id)))
              (catch Throwable e
                (if (relation-missing? e)
                  (log/warn {:id ::table-missing-skipped
                             :data {:action :skipped :subject :substrate
                                    :phase :entity-trigger
                                    :table (:table entity)
                                    :entity-id (str entity-id)}}
                            "Entity table missing; skipping trigger install (model/schema drift)")
                  (log/error! {:id ::entity-trigger-install-failed
                               :data {:table (:table entity)
                                      :entity-id (str entity-id)}} e)))))))))
  nil)

(defn set-context!
  "Write tx-local request/principal GUCs so substrate triggers can record
   who drove the mutation.

   Called at the top of every mutation tx-opener (set-entity,
   slice-entity, purge-entity 2-arity wrappers). GUCs are tx-local and
   auto-clear at commit/rollback."
  [_db tx]
  (let [principal (access/current-principal)]
    (jdbc/execute! tx
      ["SELECT set_config('synthigy.actor_xid', ?, true)"
       (some-> principal :xid str)])
    (jdbc/execute! tx
      ["SELECT set_config('synthigy.request_id', ?, true)"
       (some-> principal :request-id str)])
    (jdbc/execute! tx
      ["SELECT set_config('synthigy.scope_xid', ?, true)"
       (some-> principal :scope-xid str)]))
  (jdbc/execute! tx
    ["SELECT set_config('synthigy.tenant_xid', ?, true)"
     (str (id/data :dataset/id))])
  nil)

(defn start-drainer!
  "Start the substrate drainer: a long-lived thread that pulls rows off
   __relation_delta_queue + __entity_delta_queue and fans them out via
   `delta/dispatch!` for downstream subscribers.

   Initializes the active WakeSource — defaults to a fresh PostgresNotify
   on `synthigy_delta_ready` against the active db. Override
   `*wake-source*` before calling to plug in a different transport
   (Composite of PG-notify + NATS, changefeed, etc.). Idempotent."
  [db]
  (if @drainer-state
    (log/info {:id ::drainer-already-running
               :data {:action :starting :subject :relation-drainer}}
              "Substrate drainer already running; ignoring start")
    (let [;; The canonical delta cursor. CACHE 1 keeps it near-gap-free
          ;; (gaps only if a leader dies mid-batch); assigned post-commit by
          ;; the single lock-holding drainer, so monotonic in drain order.
          _  (jdbc/execute! (drainer-datasource db)
               ["CREATE SEQUENCE IF NOT EXISTS delta_seq AS bigint CACHE 1"])
          ws wake/*wake-source*
          _  (wake/start-source! ws)
          stop? (atom false)
          t (Thread. ^Runnable #(drainer-loop! db ws stop?)
                     "synthigy-relation-drainer")]
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
  "Idempotent full resync of the PG delta substrate against `model`.

   Walks the deployed schema, then reconciles entity-side and relation-side
   triggers + emit functions in a single pass. Called by
   `:synthigy/subscriptions.postgres` on lifecycle :start and on every
   `save-model!` via `dataset/add-model-watch!`. No-op when `model` is nil.

   Cost: ~30 ms for a small model (Movies), scales linearly with
   entity + relation count (~0.5–1 ms per DDL op). Wraps both reconcile
   passes in one Postgres connection.

   Idempotent: `CREATE OR REPLACE FUNCTION` + `DROP TRIGGER IF EXISTS …;
   CREATE TRIGGER …`. Partial-failure recovery is 'next call reconciles
   again.'"
  [db model]
  (when model
    ;; Freshen the audit policy from THIS model before baking trigger flags, so
    ;; the per-entity `audit?` decision install reads is derived from the model
    ;; being deployed — not whatever the atom happens to hold. Idempotent;
    ;; observability's model-watch recompiles too. Makes the frozen-at-enqueue
    ;; audit decision order-independent of watch firing.
    (audit/recompile-policy! model)
    (let [schema (sql-query/model->schema model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn strip-all!
  "Drop every substrate trigger + emit function across the public schema.
   Used on `:synthigy/subscriptions.postgres` :stop (clean removal — see
   plan decision 1) and as the bare-server upgrader helper.

   Leaves the queue tables in place — they're empty UNLOGGED tables, cheap
   to keep, and not user-visible. Re-:start reconciles triggers back; they
   re-enqueue against the existing tables.

   Idempotent. Safe to call multiple times."
  [_db tx]
  ;; Triggers first (CASCADE on function drop would also do it but explicit
  ;; is friendlier on shared schemas).
  (let [substrate-trgs
        (jdbc/execute! tx
          ["SELECT tgname AS trigger_name, c.relname AS table_name
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal
              AND n.nspname = 'public'
              AND (tgname LIKE 'trg\\_%\\_entity\\_audit%' ESCAPE '\\'
                OR tgname LIKE 'trg\\_%\\_entity\\_notify' ESCAPE '\\'
                OR tgname IN ('trg_relation_audit',
                              'trg_relation_audit_ins',
                              'trg_relation_audit_del',
                              'trg_relation_notify',
                              'trg_relation_xid_populate'))"])]
    (doseq [{trigger :pg_trigger/trigger_name
             table   :pg_class/table_name} substrate-trgs]
      (jdbc/execute! tx
        [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger table)])))
  ;; Per-entity emit functions (xid-keyed) and the shared helpers.
  (let [substrate-fns
        (jdbc/execute! tx
          ["SELECT proname FROM pg_proc
            JOIN pg_namespace n ON n.oid = pronamespace
            WHERE n.nspname = 'public'
              AND (proname LIKE 'synthigy\\_emit\\_%\\_delta' ESCAPE '\\'
                OR proname IN ('synthigy_emit_relation_delta',
                               'synthigy_notify_delta_ready',
                               'synthigy_relation_xid_populate'))"])]
    (doseq [{p :pg_proc/proname} substrate-fns]
      (try
        (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS %s() CASCADE" p)])
        (catch Throwable e
          (log/warn {:id ::strip-fn-drop-failed
                     :data {:proname p}}
                    (.getMessage e))))))
  nil)
