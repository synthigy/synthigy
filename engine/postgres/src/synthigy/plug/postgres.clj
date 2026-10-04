;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.plug.postgres
  "PG-side delta plug — DDL, trigger generation, drainer thread.

   Owned by `:synthigy/subscriptions.postgres` (OSS). The plug captures
   every entity/relation mutation into queue tables, fans them out via a
   drainer thread, and provides `set-context!` to stamp request provenance
   on tx-local GUCs.

   Consumers:
     - SSE subscriptions (OSS) — live notifications over `/data/events`
     - observability plug (`:synthigy/observability` — DuckDB OSS,
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
   [next.jdbc.result-set :as rs]
   [synthigy.audit.protocol :as audit]
   [synthigy.audit.policy :as policy]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.json :refer [read-str]]
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor]
   [synthigy.plug.values :as values]
   [synthigy.plug.wake :as wake]
       ;; Include to extend wake source
   synthigy.plug.wake.postgres))

;; ============================================================================
;; Wake source — delegate to shared `synthigy.plug.wake/*wake-source*`.
;;
;; The PG default (PostgresNotify on `synthigy_delta_ready`) is installed
;; by `:synthigy/subscriptions.postgres` :start. Operators wanting
;; cross-cluster fanout can alter-var-root `wake/*wake-source*` to a
;; Composite of PostgresNotify + NATS / Kafka / changefeed before :start.
;; ============================================================================

(defn wake-drainer!
  "Backend-named alias for `synthigy.plug.wake/signal-drainer!`.
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
   not an actionable plug error. Reconcile skips & warns rather than errors."
  [^Throwable e]
  (or (and (instance? java.sql.SQLException e)
           (= "42P01" (.getSQLState ^java.sql.SQLException e)))
      (boolean (some-> (.getMessage e) (str/includes? "does not exist")))))

(def relation-backfill-batch-size 50000)

(defn- backfill-xid-columns!
  "Backfill from_xid / to_xid from the entity tables. Only updates rows
   where a column is still NULL. No-op in :euuid mode."
  [tx {table      :table
       from-table :from/table
       to-table   :to/table
       from-field :from/field
       to-field   :to/field}]
  (when (= :xid (id/key))
    (let [from-col (name from-field)
          to-col   (name to-field)
          pending  "(rel.from_xid IS NULL OR rel.to_xid IS NULL)"
          {:keys [flo fhi tlo thi]}
          (jdbc/execute-one!
           tx [(format (str "SELECT min(rel.\"%s\") AS flo, max(rel.\"%s\") AS fhi,"
                            " min(rel.\"%s\") AS tlo, max(rel.\"%s\") AS thi"
                            " FROM \"%s\" rel WHERE %s")
                       from-col from-col to-col to-col table pending)])]
      (when (and flo tlo)
        ;; stride the wider-ranging FK — the narrow one can span every row
        (let [[win lo hi] (if (>= (- fhi flo) (- thi tlo))
                            [from-col flo fhi]
                            [to-col tlo thi])
              update-sql (format (str "UPDATE \"%s\" rel SET from_xid = f.xid, to_xid = t.xid"
                                      " FROM \"%s\" f, \"%s\" t"
                                      " WHERE f._eid = rel.\"%s\" AND t._eid = rel.\"%s\""
                                      " AND %s AND rel.\"%s\" >= ? AND rel.\"%s\" < ?")
                                 table from-table to-table from-col to-col pending win win)
              span (inc (- hi lo))]
          (loop [start lo]
            (when (<= start hi)
              (let [end (+ start relation-backfill-batch-size)]
                (jdbc/execute! tx [update-sql start end])
                (supervisor/progress-update! {:done (min (- end lo) span) :total span})
                (recur end)))))))))

(defn- install-relation-trigger!
  "Attach the relation-plug triggers to one link table. Idempotent.
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
;; Entity plug — xid-keyed function + trigger names
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

(defn- drop-stale-plug-triggers-on-table!
  "Drop every plug-track trigger attached to `table`, regardless of
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
   plug trigger on the table first so renames + table-name → xid-name
   migrations leave no orphans. Skips entities with no user-defined
   attributes."
  [tx entity-xid {table :table :as entity} audit?]
  (when (seq (entity-user-fields entity))
    (let [fn-name         (entity-emit-fn-name entity-xid)
          audit-trg-name  (entity-trigger-name entity-xid :audit)
          notify-trg-name (entity-trigger-name entity-xid :notify)]
      (drop-stale-plug-triggers-on-table! tx table)
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

(defn relation-envelope [ts payload]
  (let [data (read-str (.getValue ^org.postgresql.util.PGobject payload))]
    {:element (id/coerce-stored-id (:relation_xid data))
     :audit-persist? (boolean (:audit data))
     :delta {:type (keyword "relation" (name (:op data)))
             :data {:ts       ts
                    :from-eid (:from_eid data)
                    :to-eid   (:to_eid data)
                    :from-xid (id/coerce-stored-id (:from_xid data))
                    :to-xid   (id/coerce-stored-id (:to_xid data))
                    :tenant   (id/coerce-stored-id (:tenant_xid data))
                    :actor    (id/coerce-stored-id (:actor_xid data))
                    :request  (:request_id data)
                    :scope    (id/coerce-stored-id (:scope_xid data))
                    :txid     (:txid data)}}}))

(defn entity-envelope [ts payload]
  (let [data       (read-str (.getValue ^org.postgresql.util.PGobject payload))
        entity-xid (id/coerce-stored-id (:entity_xid data))]
    {:element (id/coerce-stored-id (:record_xid data))
     :audit-persist? (boolean (:audit data))
     :delta {:type (keyword "entity" (name (:op data)))
             :data {:ts         ts
                    :record-xid (id/coerce-stored-id (:record_xid data))
                    :entity-xid entity-xid
                    :tenant     (id/coerce-stored-id (:tenant_xid data))
                    :actor      (id/coerce-stored-id (:actor_xid data))
                    :request    (:request_id data)
                    :scope      (id/coerce-stored-id (:scope_xid data))
                    :txid       (:txid data)
                    :before     (values/canonical-values entity-xid (:before data) false)
                    :after      (values/canonical-values entity-xid (:after data) false)}}}))

(def queues
  {:relation {:queue      "__relation_delta_queue"
              :pending    "__relation_delta_pending"
              :->envelope relation-envelope
              :write!     audit/write-relation-deltas!}
   :entity   {:queue      "__entity_delta_queue"
              :pending    "__entity_delta_pending"
              :->envelope entity-envelope
              :write!     audit/write-entity-deltas!}})

(def pending-ddl
  ["CREATE UNLOGGED TABLE IF NOT EXISTS __relation_delta_pending (
     seq     BIGINT PRIMARY KEY,
     ts      TIMESTAMPTZ NOT NULL,
     payload JSONB NOT NULL)"
   "CREATE UNLOGGED TABLE IF NOT EXISTS __entity_delta_pending (
     seq     BIGINT PRIMARY KEY,
     ts      TIMESTAMPTZ NOT NULL,
     payload JSONB NOT NULL)"])

(defn- acquire-drainer-lock!
  "Take the transaction-scoped sequencer lock; false when another drainer holds it."
  [tx]
  (boolean (:locked (jdbc/execute-one! tx
                                       ["SELECT pg_try_advisory_xact_lock(?) AS locked"
                                        delta-drainer-lock-key]))))

(defn- next-seqs!
  "Allocate `n` ascending `delta_seq` values while holding the sequencer lock."
  [tx n]
  (mapv :seq
        (jdbc/execute! tx
                       ["SELECT nextval('delta_seq') AS seq FROM generate_series(1, ?)" n])))

(defn bigint-array [^java.sql.Connection tx xs]
  (.createArrayOf tx "bigint" (object-array xs)))

(defn claim-batch!
  "Sequence one batch: stamp seq, move audited rows to the pending table, delete the claimed rows; returns envelopes to publish, nil when another drainer holds the lock."
  [tx {:keys [queue pending ->envelope]}]
  (when (acquire-drainer-lock! tx)
    (let [rows (jdbc/execute! tx [(str "SELECT id, ts, payload FROM " queue
                                       " ORDER BY id LIMIT " drain-batch-size
                                       " FOR UPDATE SKIP LOCKED")]
                              {:builder-fn rs/as-unqualified-maps})]
      (when (seq rows)
        (let [seqs      (next-seqs! tx (count rows))
              envelopes (mapv (fn [{:keys [ts payload]} sq] (assoc (->envelope ts payload) :seq sq))
                              rows seqs)
              audited   (keep (fn [[row env]] (when (:audit-persist? env) [(:id row) (:seq env)]))
                              (map vector rows envelopes))]
          (when (seq audited)
            (jdbc/execute! tx [(str "INSERT INTO " pending " (seq, ts, payload)"
                                    " SELECT u.seq, q.ts, q.payload FROM " queue " q"
                                    " JOIN unnest(?::bigint[], ?::bigint[]) AS u(id, seq) ON q.id = u.id")
                               (bigint-array tx (map first audited))
                               (bigint-array tx (map second audited))]))
          (jdbc/execute! tx [(str "DELETE FROM " queue " WHERE id = ANY(?)")
                             (bigint-array tx (map :id rows))])
          envelopes)))))

(defn persist-batch!
  "Write one pending batch to the audit provider and delete it, in `tx`; returns the row count."
  [tx provider {:keys [pending ->envelope write!]}]
  (let [rows (jdbc/execute! tx [(str "SELECT seq, ts, payload FROM " pending
                                     " ORDER BY seq LIMIT " drain-batch-size
                                     " FOR UPDATE SKIP LOCKED")]
                            {:builder-fn rs/as-unqualified-maps})]
    (when (seq rows)
      (binding [audit/*audit-tx* tx]
        (write! provider (mapv (fn [{:keys [seq ts payload]}] (assoc (->envelope ts payload) :seq seq)) rows)))
      (jdbc/execute! tx [(str "DELETE FROM " pending " WHERE seq = ANY(?)")
                         (bigint-array tx (map :seq rows))]))
    (count rows)))

(defn pending-envelopes
  "Audited envelopes sequenced but not yet persisted, `seq` > cursor, oldest first."
  [db cursor-seq limit track]
  (let [{:keys [pending ->envelope]} (queues track)]
    (mapv (fn [{:keys [seq ts payload]}] (assoc (->envelope ts payload) :seq seq))
          (jdbc/execute! (:datasource db)
                         [(str "SELECT seq, ts, payload FROM " pending
                               " WHERE seq > ? ORDER BY seq LIMIT " (long limit))
                          (long (or cursor-seq 0))]
                         {:builder-fn rs/as-unqualified-maps}))))

(defn pending-stats
  "Audited deltas waiting for the provider: {:held n :oldest inst-or-nil}."
  [db]
  (let [rows (for [{:keys [pending]} (vals queues)]
               (jdbc/execute-one! (:datasource db) [(str "SELECT count(*) AS n, min(ts) AS oldest FROM " pending)]
                                  {:builder-fn rs/as-unqualified-maps}))]
    {:held   (reduce + (map :n rows))
     :oldest (some->> (keep :oldest rows) seq (apply min-key #(.getTime ^java.util.Date %)) .toInstant)}))

(defn- publish-envelopes!
  "Fan a sequenced batch out to live subscribers after its transaction committed."
  [envelopes]
  (when (seq envelopes)
    (try
      (delta/publish! envelopes)
      (catch Throwable e
        (log/error! {:id ::publish-failed
                     :msg "Failed to publish delta batch"
                     :data {:action :publishing :subject :delta}}
                    e)))))

(defn- drainer-datasource
  "The dedicated drainer pool when configured, else the shared writer pool."
  [db]
  (or (:drainer-datasource db) (:datasource db)))

(defn- drainer-tick!
  "Sequence and publish until both queues are empty; returns true when audited rows went to pending."
  [db]
  (loop [audited? false]
    (let [ds       (drainer-datasource db)
          claim    (fn [q] (jdbc/with-transaction [tx ds] (claim-batch! tx (queues q))))
          rel-envs (claim :relation)
          _        (publish-envelopes! rel-envs)
          ent-envs (claim :entity)
          _        (publish-envelopes! ent-envs)
          audited? (or audited? (some :audit-persist? rel-envs) (some :audit-persist? ent-envs))]
      (if (or (= (count rel-envs) drain-batch-size)
              (= (count ent-envs) drain-batch-size))
        (recur audited?)
        audited?))))

(defonce ^:private last-pending-warning (atom 0))

(defn warn-pending-without-provider! [db]
  (let [now (System/currentTimeMillis)]
    (when (> (- now @last-pending-warning) 60000)
      (reset! last-pending-warning now)
      (let [n (reduce + (for [{:keys [pending]} (vals queues)]
                          (:n (jdbc/execute-one! (:datasource db) [(str "SELECT count(*) AS n FROM " pending)]
                                                 {:builder-fn rs/as-unqualified-maps}))))]
        (when (pos? n)
          (log/warn {:id ::audit-held-no-provider
                     :topics #{:system}
                     :data {:action :writing :subject :audit :pending n}}
                    (str n " audited deltas held in pending — no audit provider bound; "
                         "they are persisted once one starts")))))))

(defn persister-tick!
  "Persist pending audited deltas until empty; with no provider bound they stay pending."
  [db]
  (if-let [provider audit/*audit-provider*]
    (loop []
      (let [ds    (drainer-datasource db)
            n-rel (jdbc/with-transaction [tx ds] (persist-batch! tx provider (:relation queues)))
            n-ent (jdbc/with-transaction [tx ds] (persist-batch! tx provider (:entity queues)))]
        (audit/persist-ok!)
        (when (or (= n-rel drain-batch-size) (= n-ent drain-batch-size))
          (recur))))
    (warn-pending-without-provider! db)))

(defn persister-loop! [db ^java.util.concurrent.BlockingQueue signal stop?]
  (loop [backoff-ms 0]
    (when-not @stop?
      (let [next-backoff
            (try
              (when (pos? backoff-ms) (Thread/sleep (long backoff-ms)))
              (.poll signal 1000 java.util.concurrent.TimeUnit/MILLISECONDS)
              (when-not @stop? (persister-tick! db))
              0
              (catch InterruptedException _ (reset! stop? true) 0)
              (catch Throwable e
                (audit/persist-failed! e)
                (log/error! {:id ::persist-failed
                             :msg "Audit persistence failed; batch stays pending"
                             :data {:action :writing :subject :audit :retry-ms (max 1000 (* 2 backoff-ms))}}
                            e)
                (min 30000 (max 1000 (* 2 backoff-ms)))))]
        (recur next-backoff)))))

(defn- drainer-loop!
  "Worker body. Parks on the registered WakeSource (default
   PostgresNotify, with `signal!` driven by the trigger's pg_notify).
   `wait!` returns :wakeup or :poll; in either case we drain. The
   PostgresNotify implementation translates `timeout-ms` into a
   blocking `getNotifications(timeout-ms)`, preserving the prior
   listen-poll latency."
  [db wake-source ^java.util.concurrent.BlockingQueue signal stop?]
  (try
    (log/info {:id ::drainer-listening
               :data {:action :started :subject :relation-drainer
                      :wake-source (.getName (class wake-source))}}
              "Relation drainer listening on WakeSource")
    (loop [backoff-ms 0]
      (when-not @stop?
        (recur
         (try
           (case (wake/wait! wake-source listen-poll-ms)
             :wakeup (when-not @stop? (when (drainer-tick! db) (.offer signal :persist)))
             :poll   (when-not @stop? (drainer-tick! db) (.offer signal :persist))
             :stop   (reset! stop? true))
           0
           (catch InterruptedException _
             (reset! stop? true)
             0)
           (catch Throwable e
             (let [wait-ms (min 30000 (max 1000 (* 2 backoff-ms)))]
               (log/error! {:id ::drain-loop-error
                            :data {:action :draining :subject :delta :retry-ms wait-ms}}
                           e)
               (try (Thread/sleep (long wait-ms))
                    (catch InterruptedException _ (reset! stop? true)))
               wait-ms))))))
    (catch Throwable e
      (log/error! {:id ::drainer-fatal} e))
    (finally
      (log/info {:id ::drainer-loop-exited
                 :data {:action :stopped :subject :relation-drainer}}
                "Relation drainer loop exited"))))

(defn- drop-stale-relation-triggers!
  "Drop every relation plug/notify/populate trigger across the whole
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
;; Public API — plug reconcile + drainer + context
;; ============================================================================

(defn reconcile-relations!
  "Deploy relation-plug infrastructure: idempotent prelude (queue table +
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
      (install-relation-trigger! tx rel (policy/audited-relation? (str (:relation rel)))))
    (doseq [[i rel] (map-indexed vector (vals unique-by-table))]
      (supervisor/progress-update! {:detail (format "relation xids %d/%d · %s"
                                                    (inc i) (count unique-by-table) (:table rel))
                                    :done nil :total nil})
      (try
        (backfill-xid-columns! tx rel)
        (catch Throwable e
          (if (relation-missing? e)
            (log/warn {:id ::table-missing-skipped
                       :data {:action :skipped :subject :plug
                              :phase :backfill :table (:table rel)}}
                      "Table for relation missing; skipping backfill (model/schema drift)")
            (log/error! {:id ::backfill-failed
                         :data {:table (:table rel)}} e))))))
  nil)

(defn reconcile-entities!
  "Deploy entity-plug infrastructure: idempotent prelude (entity queue
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
    (log/info {:id ::entity-plug-skipped-legacy-id
               :data {:action :skipped :subject :entity-plug
                      :reason "id-key is :euuid; xid columns absent on entity tables"}}
              "Entity plug triggers skipped (euuid mode)")
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
                                        (policy/audited-entity? (str entity-id)))
              (catch Throwable e
                (if (relation-missing? e)
                  (log/warn {:id ::table-missing-skipped
                             :data {:action :skipped :subject :plug
                                    :phase :entity-trigger
                                    :table (:table entity)
                                    :entity-id (str entity-id)}}
                            "Entity table missing; skipping trigger install (model/schema drift)")
                  (log/error! {:id ::entity-trigger-install-failed
                               :data {:table (:table entity)
                                      :entity-id (str entity-id)}} e)))))))))
  nil)

(defn set-context!
  "Write tx-local request/principal GUCs so plug triggers can record
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
  "Start the sequencer (claim, seq, publish) and persister (pending → audit provider) threads; idempotent."
  [db]
  (if @drainer-state
    (log/info {:id ::drainer-already-running
               :data {:action :starting :subject :relation-drainer}}
              "Plug drainer already running; ignoring start")
    (let [ds     (drainer-datasource db)
          _      (jdbc/execute! ds ["CREATE SEQUENCE IF NOT EXISTS delta_seq AS bigint CACHE 1"])
          _      (doseq [ddl pending-ddl] (jdbc/execute! ds [ddl]))
          ws     wake/*wake-source*
          _      (wake/start-source! ws)
          stop?  (atom false)
          signal (java.util.concurrent.LinkedBlockingQueue. 1)
          t      (Thread. ^Runnable #(drainer-loop! db ws signal stop?) "synthigy-relation-drainer")
          p      (Thread. ^Runnable #(persister-loop! db signal stop?) "synthigy-audit-persister")]
      (doseq [^Thread th [t p]] (.setDaemon th true) (.start th))
      (alter-var-root #'audit/*pending-envelopes*
                      (constantly (fn [cursor-seq limit track] (pending-envelopes db cursor-seq limit track))))
      (alter-var-root #'audit/*pending-stats* (constantly #(pending-stats db)))
      (.offer signal :persist)
      (reset! drainer-state {:threads [t p] :stop? stop? :wake-source ws})
      nil)))

(defn stop-drainer!
  "Stop both drainer threads and the wake source; idempotent."
  [_db]
  (when-let [{:keys [threads stop? wake-source]} @drainer-state]
    (reset! stop? true)
    (when wake-source (wake/stop-source! wake-source))
    (doseq [^Thread th threads]
      (.interrupt th)
      (try (.join th 5000) (catch InterruptedException _)))
    (alter-var-root #'audit/*pending-envelopes* (constantly nil))
    (alter-var-root #'audit/*pending-stats* (constantly nil))
    (reset! drainer-state nil)
    nil))

;; ============================================================================
;; Unified entry points
;; ============================================================================

(defn reconcile-plug!
  "Idempotent full resync of the PG delta plug against `model`.

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
    (policy/recompile-policy! model)
    (let [schema (sql-query/model->schema model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn strip-all!
  "Drop every plug trigger + emit function across the public schema.
   Used on `:synthigy/subscriptions.postgres` :stop (clean removal — see
   plan decision 1) and as the bare-server upgrader helper.

   Leaves the queue tables in place — they're empty UNLOGGED tables, cheap
   to keep, and not user-visible. Re-:start reconciles triggers back; they
   re-enqueue against the existing tables.

   Idempotent. Safe to call multiple times."
  [_db tx]
  ;; Triggers first (CASCADE on function drop would also do it but explicit
  ;; is friendlier on shared schemas).
  (let [plug-trgs
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
             table   :pg_class/table_name} plug-trgs]
      (jdbc/execute! tx
                     [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger table)])))
  ;; Per-entity emit functions (xid-keyed) and the shared helpers.
  (let [plug-fns
        (jdbc/execute! tx
                       ["SELECT proname FROM pg_proc
            JOIN pg_namespace n ON n.oid = pronamespace
            WHERE n.nspname = 'public'
              AND (proname LIKE 'synthigy\\_emit\\_%\\_delta' ESCAPE '\\'
                OR proname IN ('synthigy_emit_relation_delta',
                               'synthigy_notify_delta_ready',
                               'synthigy_relation_xid_populate'))"])]
    (doseq [{p :pg_proc/proname} plug-fns]
      (try
        (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS %s() CASCADE" p)])
        (catch Throwable e
          (log/warn {:id ::strip-fn-drop-failed
                     :data {:proname p}}
                    (.getMessage e))))))
  nil)
