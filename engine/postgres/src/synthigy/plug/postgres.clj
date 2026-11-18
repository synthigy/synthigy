;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.plug.postgres
  "PG-side delta plug — queue and trigger DDL, drainer threads, provenance GUCs."
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
   ;; loaded to extend the wake source
   synthigy.plug.wake.postgres))

(defn wake-drainer!
  []
  (wake/signal-drainer!))

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

(def ^:private relation-emit-fn-ddl
  "Shared relation emit fn; TG_ARGV = relation xid, from col, to col, frozen audit flag."
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

(def ^:private notify-fn-ddl
  "CREATE OR REPLACE FUNCTION synthigy_notify_delta_ready()
   RETURNS TRIGGER AS $$
   BEGIN
     PERFORM pg_notify('synthigy_delta_ready', '');
     RETURN NULL;
   END $$ LANGUAGE plpgsql")

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

(defn migrate-legacy-queue!
  "Idempotently moves `__delta_queue` (column seq) to `__relation_delta_queue` (column id)."
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

(defn xid-columns
  "Every [table column] in the public schema among from_xid, to_xid and xid."
  [tx]
  (into #{}
        (map (juxt :tbl :col))
        (jdbc/execute! tx
                       ["SELECT c.relname AS tbl, a.attname AS col
                           FROM pg_attribute a
                           JOIN pg_class c ON c.oid = a.attrelid
                           JOIN pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = 'public' AND a.attnum > 0 AND NOT a.attisdropped
                            AND a.attname IN ('from_xid', 'to_xid', 'xid')"]
                       {:builder-fn rs/as-unqualified-lower-maps})))

(defn ensure-xid-columns!
  [tx table existing]
  (doseq [col ["from_xid" "to_xid"]
          :when (not (contains? existing [table col]))]
    (jdbc/execute! tx
                   [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS %s TEXT" table col)])))

(defn ensure-entity-xid-columns!
  "Adds a missing `xid` column to every relation parent table."
  [tx relations existing]
  (let [parent-tables (->> relations
                           (mapcat (juxt :from/table :to/table))
                           (remove nil?)
                           set)]
    (doseq [t parent-tables
            :when (not (contains? existing [t "xid"]))]
      (try
        (jdbc/execute! tx
                       [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS xid VARCHAR(64)" t)])
        (catch Throwable e
          (log/warn {:id ::entity-xid-column-add-failed
                     :data {:table t}}
                    (.getMessage e)))))))

(defn relation-missing?
  "True for SQLSTATE 42P01 (relation does not exist) — model/schema drift, not a plug error."
  [^Throwable e]
  (or (and (instance? java.sql.SQLException e)
           (= "42P01" (.getSQLState ^java.sql.SQLException e)))
      (boolean (some-> (.getMessage e) (str/includes? "does not exist")))))

(def relation-backfill-batch-size 50000)

(defn backfill-xid-columns!
  "Fills NULL from_xid/to_xid from the entity tables in strides; no-op in :euuid mode."
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

(defn function-body
  [ddl]
  (subs ddl (+ 2 (str/index-of ddl "$$")) (str/last-index-of ddl "$$")))

(defn installed-functions
  "{name body} of the public functions matching the `where` SQL predicate."
  [tx where]
  (into {}
        (map (juxt :proname :prosrc))
        (jdbc/execute! tx
                       [(str "SELECT p.proname, p.prosrc FROM pg_proc p"
                             " JOIN pg_namespace n ON n.oid = p.pronamespace"
                             " WHERE n.nspname = 'public' AND " where)]
                       {:builder-fn rs/as-unqualified-lower-maps})))

(defn sync-functions!
  "Creates or replaces each `{name ddl}` whose installed body differs."
  [tx installed desired]
  (doseq [[fn-name ddl] desired
          :when (not= (get installed fn-name) (function-body ddl))]
    (jdbc/execute! tx [ddl])))

(defn trigger-args
  [^bytes tgargs]
  (if (pos? (alength tgargs))
    (vec (butlast (.split (String. tgargs "UTF-8") "\u0000" -1)))
    []))

(defn installed-triggers
  "{[table name] spec} of the public non-internal triggers matching the `where` SQL predicate."
  [tx where]
  (into {}
        (map (fn [{:keys [tbl name fn type old new args]}]
               [[tbl name]
                {:fn     fn
                 :args   (trigger-args args)
                 :timing (if (bit-test type 1) "BEFORE" "AFTER")
                 :level  (if (bit-test type 0) "ROW" "STATEMENT")
                 :events (cond-> #{}
                           (bit-test type 2) (conj "INSERT")
                           (bit-test type 3) (conj "DELETE")
                           (bit-test type 4) (conj "UPDATE"))
                 :other  (bit-and type (bit-not 31))
                 :old    old
                 :new    new}]))
        (jdbc/execute! tx
                       [(str "SELECT c.relname AS tbl, t.tgname AS name, p.proname AS fn,"
                             " t.tgtype::int AS type, t.tgoldtable AS old, t.tgnewtable AS new,"
                             " t.tgargs AS args"
                             " FROM pg_trigger t"
                             " JOIN pg_class c ON c.oid = t.tgrelid"
                             " JOIN pg_namespace n ON n.oid = c.relnamespace"
                             " JOIN pg_proc p ON p.oid = t.tgfoid"
                             " WHERE NOT t.tgisinternal AND n.nspname = 'public' AND " where)]
                       {:builder-fn rs/as-unqualified-lower-maps})))

(defn trigger-spec
  [fn-name args timing level events old new]
  {:fn fn-name :args (mapv str args) :timing timing :level level :events (set events)
   :other 0 :old old :new new})

(defn create-trigger-ddl
  [[table trigger] {fn-name :fn :keys [args timing level events old new]}]
  (str "CREATE TRIGGER " trigger " " timing " "
       (str/join " OR " (filter events ["INSERT" "UPDATE" "DELETE"]))
       " ON \"" table "\""
       (when (or old new)
         (str " REFERENCING"
              (when old (str " OLD TABLE AS " old))
              (when new (str " NEW TABLE AS " new))))
       " FOR EACH " level
       " EXECUTE FUNCTION " fn-name "("
       (str/join ", " (map #(str "'" (str/replace % "'" "''") "'") args))
       ")"))

(defn sync-table-triggers!
  "Brings one table's plug triggers to `desired` in one transaction; an unchanged trigger is never touched."
  [tx table installed desired]
  (let [mine?   (fn [[[t _] _]] (= t table))
        stale   (for [[k spec] (filter mine? installed)
                      :when (not= spec (get desired k))]
                  k)
        missing (for [[k spec] (filter mine? desired)
                      :when (not= spec (get installed k))]
                  [k spec])]
    (when (or (seq stale) (seq missing))
      (jdbc/with-transaction [t tx]
        (doseq [[_ trigger] stale]
          (jdbc/execute! t [(format "DROP TRIGGER IF EXISTS %s ON \"%s\"" trigger table)]))
        (doseq [[k spec] missing]
          (jdbc/execute! t [(create-trigger-ddl k spec)]))))))

(defn relation-trigger-specs
  "{[table name] spec} of the relation-plug triggers of one link table; `audit?` is frozen into TG_ARGV."
  [{rel-id     :relation
    table      :table
    from       :from/field
    to         :to/field
    from-table :from/table
    to-table   :to/table}
   audit?]
  (let [emit-args [(str rel-id) (name from) (name to) (if audit? "true" "false")]]
    (cond-> {[table "trg_relation_audit_ins"]
             (trigger-spec "synthigy_emit_relation_delta" emit-args
                           "AFTER" "STATEMENT" ["INSERT"] nil "new_rows")
             [table "trg_relation_audit_del"]
             (trigger-spec "synthigy_emit_relation_delta" emit-args
                           "AFTER" "STATEMENT" ["DELETE"] "old_rows" nil)
             [table "trg_relation_notify"]
             (trigger-spec "synthigy_notify_delta_ready" []
                           "AFTER" "STATEMENT" ["INSERT" "DELETE"] nil nil)}
      (= :xid (id/key))
      (assoc [table "trg_relation_xid_populate"]
             (trigger-spec "synthigy_relation_xid_populate"
                           [from-table to-table (name from) (name to)]
                           "BEFORE" "ROW" ["INSERT"] nil nil)))))

(defn entity-emit-fn-name
  [entity-xid]
  (str/lower-case (str "synthigy_emit_" entity-xid "_delta")))

(defn entity-trigger-name
  [entity-xid kind]
  (str/lower-case (str "trg_" entity-xid "_entity_" (name kind))))

(defn entity-user-fields
  "Sorted {column attribute-xid} of the user-defined fields of a schema entity."
  [entity]
  (->> (:field->attribute entity)
       (filter (fn [[_col attr-xid]] (string? attr-xid)))
       (into (sorted-map))))

(defn jsonb-build-pairs
  [field-map ref]
  (->> field-map
       (mapv (fn [[col attr-xid]]
               ;; never unquote the column — reserved-word columns ("from") fail to parse in plpgsql
               (format "'%s', %s.\"%s\"" attr-xid ref (name col))))
       (str/join ", ")))

(defn attribute-when-distinct
  "Phantom-UPDATE filter: OR'd `IS DISTINCT FROM` over every user column."
  [field-map old-ref new-ref]
  (->> field-map
       (map (fn [[col _attr-xid]]
              ;; never unquote — see jsonb-build-pairs
              (format "%s.\"%s\" IS DISTINCT FROM %s.\"%s\""
                      old-ref (name col) new-ref (name col))))
       (str/join " OR ")))

(defn entity-emit-fn-ddl
  "Per-entity STATEMENT-level emit fn DDL; `audit?` is frozen into every payload at enqueue time."
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

(defn entity-trigger-specs
  "{[table name] spec} of one entity's per-op and notify triggers."
  [entity-xid {table :table}]
  (let [emit  (entity-emit-fn-name entity-xid)
        audit (entity-trigger-name entity-xid :audit)]
    {[table (str audit "_ins")]
     (trigger-spec emit [] "AFTER" "STATEMENT" ["INSERT"] nil "new_rows")
     [table (str audit "_upd")]
     (trigger-spec emit [] "AFTER" "STATEMENT" ["UPDATE"] "old_rows" "new_rows")
     [table (str audit "_del")]
     (trigger-spec emit [] "AFTER" "STATEMENT" ["DELETE"] "old_rows" nil)
     [table (entity-trigger-name entity-xid :notify)]
     (trigger-spec "synthigy_notify_delta_ready" []
                   "AFTER" "STATEMENT" ["INSERT" "UPDATE" "DELETE"] nil nil)}))

(def ^:private drain-batch-size 2000)  ; ponytail: claim window; 500→2000 cuts lock cycles ~4x under burst
(def ^:private listen-poll-ms 10000)

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

(defn acquire-drainer-lock!
  "Take the transaction-scoped sequencer lock; false when another drainer holds it."
  [tx]
  (boolean (:locked (jdbc/execute-one! tx
                                       ["SELECT pg_try_advisory_xact_lock(?) AS locked"
                                        delta-drainer-lock-key]))))

(defn next-seqs!
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

(defn publish-envelopes!
  [envelopes]
  (when (seq envelopes)
    (try
      (delta/publish! envelopes)
      (catch Throwable e
        (log/error! {:id ::publish-failed
                     :msg "Failed to publish delta batch"
                     :data {:action :publishing :subject :delta}}
                    e)))))

(defn drainer-datasource
  [db]
  (or (:drainer-datasource db) (:datasource db)))

(defn drainer-tick!
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

(defn drainer-loop!
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

(def relation-triggers-where
  "t.tgname IN ('trg_relation_audit', 'trg_relation_audit_ins', 'trg_relation_audit_del',
                'trg_relation_notify', 'trg_relation_xid_populate')")

(def entity-triggers-where
  "(t.tgname LIKE 'trg\\_%\\_entity\\_audit%' ESCAPE '\\'
    OR t.tgname LIKE 'trg\\_%\\_entity\\_notify' ESCAPE '\\')")

(def relation-functions-where
  "p.proname IN ('synthigy_emit_relation_delta', 'synthigy_notify_delta_ready',
                 'synthigy_relation_xid_populate')")

(def entity-functions-where
  "p.proname LIKE 'synthigy\\_emit\\_%\\_delta' ESCAPE '\\'
   AND p.proname <> 'synthigy_emit_relation_delta'")

(defn reconcile-relations!
  "Brings the relation queue, emit fns and per-relation triggers to the model, then backfills xids."
  [_db tx schema-relations]
  (migrate-legacy-queue! tx)
  (jdbc/execute! tx [delta-queue-ddl])
  (sync-functions! tx
                   (installed-functions tx relation-functions-where)
                   {"synthigy_emit_relation_delta"   relation-emit-fn-ddl
                    "synthigy_notify_delta_ready"    notify-fn-ddl
                    "synthigy_relation_xid_populate" relation-xid-populate-fn-ddl})
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
                              unique-by-table)
        columns   (xid-columns tx)
        installed (installed-triggers tx relation-triggers-where)
        desired   (into {}
                        (map (fn [rel]
                               (relation-trigger-specs rel (policy/audited-relation? (str (:relation rel))))))
                        (vals unique-by-table))]
    (ensure-entity-xid-columns! tx (vals unique-by-table) columns)
    (doseq [table (keys unique-by-table)]
      (ensure-xid-columns! tx table columns))
    (doseq [table (distinct (map first (concat (keys desired) (keys installed))))]
      (sync-table-triggers! tx table installed desired))
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
  "Brings the entity queue, per-entity emit fns and triggers to the model; skipped in :euuid mode."
  [_db tx schema-entities]
  (if (not= :xid (id/key))
    (log/info {:id ::entity-plug-skipped-legacy-id
               :data {:action :skipped :subject :entity-plug
                      :reason "id-key is :euuid; xid columns absent on entity tables"}}
              "Entity plug triggers skipped (euuid mode)")
    (let [_         (jdbc/execute! tx [entity-queue-ddl])
          entities  (for [[entity-id entity] (cond
                                               (map? schema-entities) (seq schema-entities)
                                               (sequential? schema-entities) schema-entities
                                               :else nil)
                          :when (and (:table entity) (seq (entity-user-fields entity)))]
                      [(str entity-id) entity])
          functions (into {}
                          (map (fn [[xid entity]]
                                 [(entity-emit-fn-name xid)
                                  (entity-emit-fn-ddl xid entity (policy/audited-entity? xid))]))
                          entities)
          desired   (into {} (map (fn [[xid entity]] (entity-trigger-specs xid entity))) entities)
          installed-fns (installed-functions tx entity-functions-where)
          installed (installed-triggers tx entity-triggers-where)
          table->id (into {} (map (fn [[xid entity]] [(:table entity) xid])) entities)]
      (doseq [[fn-name ddl] functions]
        (try
          (sync-functions! tx installed-fns {fn-name ddl})
          (catch Throwable e
            (when-not (relation-missing? e)
              (log/error! {:id ::entity-trigger-install-failed
                           :data {:function fn-name}} e)))))
      (doseq [table (distinct (map first (concat (keys desired) (keys installed))))]
        (try
          (sync-table-triggers! tx table installed desired)
          (catch Throwable e
            (if (relation-missing? e)
              (log/warn {:id ::table-missing-skipped
                         :data {:action :skipped :subject :plug
                                :phase :entity-trigger
                                :table table
                                :entity-id (table->id table)}}
                        "Entity table missing; skipping trigger install (model/schema drift)")
              (log/error! {:id ::entity-trigger-install-failed
                           :data {:table table
                                  :entity-id (table->id table)}} e)))))
      (doseq [fn-name (keys installed-fns)
              :when (not (contains? functions fn-name))]
        (try
          (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS %s() CASCADE" fn-name)])
          (catch Throwable e
            (log/warn {:id ::stale-fn-drop-failed
                       :data {:proname fn-name}}
                      (.getMessage e)))))))
  nil)

(defn set-context!
  "Write the tx-local provenance GUCs the plug triggers stamp on every delta."
  [_db tx]
  (let [principal (access/current-principal)]
    (jdbc/execute! tx
                   [(str "SELECT set_config('synthigy.actor_xid', ?, true),"
                         " set_config('synthigy.request_id', ?, true),"
                         " set_config('synthigy.scope_xid', ?, true)")
                    (some-> principal :xid str)
                    (log/request-id)
                    (some-> principal :scope-xid str)]))
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

(defn reconcile-plug!
  "Idempotent full resync of the PG delta plug against `model`; no-op when nil."
  [db model]
  (when model
    ;; must run BEFORE baking trigger audit flags — they come from this model, not the atom
    (policy/recompile-policy! model)
    (let [schema (sql-query/model->schema model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn strip-all!
  "Idempotently drops every plug trigger and emit fn in the public schema; queue tables stay."
  [_db tx]
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
