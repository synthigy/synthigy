;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.plug.sqlite
  "SQLite delta plug: queue and `_ctx` DDL, per-table triggers, drainer thread."
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]
    [synthigy.audit.protocol :as audit]
   [synthigy.audit.policy :as policy]
    [synthigy.dataset.access :as access]
    [synthigy.db.sqlite :as sqlite-db]
    [synthigy.dataset.delta :as delta]
    [synthigy.dataset.id :as id]
    [synthigy.json :refer [read-str]]
    [synthigy.log :as log]
    [synthigy.plug.values :as values]
    [synthigy.plug.wake :as wake]
    [synthigy.dataset.sql.query :as sql.query]))

(defn wake-drainer!
  "Wakes the drainer; app-path writes call it post-commit."
  []
  (wake/signal-drainer!))

(def ^:private delta-queue-ddl
  "CREATE TABLE IF NOT EXISTS __relation_delta_queue (
     id      INTEGER PRIMARY KEY AUTOINCREMENT,
     ts      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
     kind    TEXT    NOT NULL,
     payload TEXT    NOT NULL,
     seq     INTEGER
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
     payload TEXT    NOT NULL,
     seq     INTEGER
   )")

(defn uuid-v7
  "Time-ordered UUIDv7 (RFC 9562)."
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

(defn drop-all-plug-triggers-for-table!
  "Drops every plug trigger on `table`, whatever its name shape."
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

(defn relation-trigger-name
  [rel-xid op]
  (str "trg_" rel-xid "_audit_" (name op)))

(defn insert-trigger-ddl
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

(defn delete-trigger-ddl
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

(defn column-exists?
  [tx table column]
  (some
   #(= column (:name %))
   (jdbc/execute! tx [(format "PRAGMA table_info(\"%s\")" table)]
                  {:builder-fn rs/as-unqualified-maps})))

(defn ensure-seq-column!
  "Add the drainer's claim marker to a queue table created before it existed."
  [tx table]
  (when-not (column-exists? tx table "seq")
    (jdbc/execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN seq INTEGER" table)])))

(defn ensure-xid-columns!
  [tx table]
  (when-not (column-exists? tx table "from_xid")
    (jdbc/execute! tx
                   [(format "ALTER TABLE \"%s\" ADD COLUMN from_xid TEXT" table)]))
  (when-not (column-exists? tx table "to_xid")
    (jdbc/execute! tx
                   [(format "ALTER TABLE \"%s\" ADD COLUMN to_xid TEXT" table)])))

(defn backfill-xid-columns!
  "Fills NULL from_xid/to_xid from the entity tables; safe to re-run."
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

(defn install-relation-triggers!
  "Reinstalls the INSERT/DELETE plug triggers on one link table; throws when the table is missing."
  [tx {table :table :as rel} audit?]
  (ensure-xid-columns! tx table)
  (drop-all-plug-triggers-for-table! tx table)
  (jdbc/execute! tx [(insert-trigger-ddl rel audit?)])
  (jdbc/execute! tx [(delete-trigger-ddl rel audit?)]))

(defn entity-trigger-name
  [entity-xid op]
  (str "trg_" entity-xid "_entity_audit_" (name op)))

(defn attribute-when-distinct
  [field-map]
  (->> field-map
       (map (fn [[col _attr-xid]]
              (format "OLD.\"%s\" IS NOT NEW.\"%s\"" (name col) (name col))))
       (str/join " OR ")))

(defn entity-user-fields
  "Sorted {column attribute-xid} of user fields; system fields carry keyword xids and are skipped."
  [entity]
  (->> (:field->attribute entity)
       (filter (fn [[_col attr-xid]] (string? attr-xid)))
       (into (sorted-map))))

(defn attribute-json-pairs
  [field-map ref]
  (->> field-map
       (map (fn [[col attr-xid]]
              (format "'%s', %s.\"%s\"" attr-xid ref (name col))))
       (str/join ", ")))

(defn entity-insert-trigger-ddl
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

(defn entity-update-trigger-ddl
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

(defn entity-delete-trigger-ddl
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

(defn install-entity-triggers!
  "Reinstalls the INSERT/UPDATE/DELETE plug triggers on one entity table."
  [tx entity-xid {table :table :as entity} audit?]
  (when (seq (entity-user-fields entity))
    (drop-all-plug-triggers-for-table! tx table)
    (jdbc/execute! tx [(entity-insert-trigger-ddl entity-xid entity audit?)])
    (jdbc/execute! tx [(entity-update-trigger-ddl entity-xid entity audit?)])
    (jdbc/execute! tx [(entity-delete-trigger-ddl entity-xid entity audit?)])))

;; 2000, not PG's 500 — SQLite has no SKIP LOCKED.
(def ^:private drain-batch-size 2000)
(def ^:private safety-poll-ms (long 10000))
;; pages handed back per idle poll — 8 MB at the 4 KB page size, never during a burst
(def ^:private idle-vacuum-pages 2000)

(defonce ^:private drainer-state (atom nil))

(defn relation-envelope [ts payload]
  (let [data (read-str payload)]
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
  (let [data       (read-str payload)
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
                    :before     (values/canonical-values entity-xid (:before data) true)
                    :after      (values/canonical-values entity-xid (:after data) true)}}}))

(defn alloc-seqs!
  "Allocate `n` ascending delta-seq values from the single-row __delta_seq counter inside the claim tx."
  [tx n]
  (let [v (long (:__delta_seq/value
                 (jdbc/execute-one! tx ["SELECT value FROM __delta_seq WHERE id = 0"])))]
    (jdbc/execute! tx ["UPDATE __delta_seq SET value = value + ? WHERE id = 0" n])
    (mapv #(+ v 1 %) (range n))))

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
  ["CREATE TABLE IF NOT EXISTS __relation_delta_pending (
     seq     INTEGER PRIMARY KEY,
     ts      TEXT NOT NULL,
     payload TEXT NOT NULL)"
   "CREATE TABLE IF NOT EXISTS __entity_delta_pending (
     seq     INTEGER PRIMARY KEY,
     ts      TEXT NOT NULL,
     payload TEXT NOT NULL)"])

(defn claim-batch!
  "Sequence the head of a queue: stamp seq (rows stamped by an older drainer keep theirs), move audited rows to pending, delete; returns envelopes to publish."
  [tx {:keys [queue pending ->envelope]}]
  (let [rows (jdbc/execute! tx [(str "SELECT id, ts, payload, seq FROM " queue
                                     " ORDER BY id LIMIT " drain-batch-size)]
                            {:builder-fn rs/as-unqualified-maps})]
    (when (seq rows)
      (let [fresh     (remove :seq rows)
            id->seq   (zipmap (map :id fresh) (when (seq fresh) (alloc-seqs! tx (count fresh))))
            envelopes (mapv (fn [{:keys [id ts payload seq]}]
                              (assoc (->envelope ts payload) :seq (or seq (id->seq id))))
                            rows)
            audited   (keep (fn [[row env]] (when (:audit-persist? env) [(:seq env) (:ts row) (:payload row)]))
                            (map vector rows envelopes))]
        (when (seq audited)
          (jdbc/execute-batch! tx (str "INSERT OR IGNORE INTO " pending " (seq, ts, payload) VALUES (?, ?, ?)")
                               (vec audited) {}))
        (jdbc/execute! tx [(str "DELETE FROM " queue " WHERE id IN (" (str/join "," (map :id rows)) ")")])
        envelopes))))

(defn persist-batch!
  "Write one pending batch to the provider, then delete it; the provider's seq-range delete keeps a retry idempotent."
  [db provider {:keys [pending ->envelope write!]}]
  (let [ds   (:datasource db)
        rows (jdbc/execute! ds [(str "SELECT seq, ts, payload FROM " pending
                                     " ORDER BY seq LIMIT " drain-batch-size)]
                            {:builder-fn rs/as-unqualified-maps})]
    (when (seq rows)
      (write! provider (mapv (fn [{:keys [seq ts payload]}] (assoc (->envelope ts payload) :seq seq)) rows))
      (jdbc/with-transaction [tx ds]
        (jdbc/execute! tx [(str "DELETE FROM " pending " WHERE seq IN (" (str/join "," (map :seq rows)) ")")])))
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
     :oldest (some->> (keep :oldest rows) seq (map #(java.time.Instant/parse %)) (apply min-key #(.toEpochMilli ^java.time.Instant %)))}))

(defn publish!
  [envelopes]
  (try
    (delta/publish! envelopes)
    (catch Throwable e
      (log/error! {:id ::publish-failed
                   :msg "Failed to publish delta batch"
                   :data {:action :publishing :subject :delta}}
                  e))))

(defn drainer-tick!
  "Sequence and publish until both queues are empty; true when audited rows went to pending."
  [db]
  (loop [audited? false]
    (let [ds       (:datasource db)
          claim    (fn [q] (jdbc/with-transaction [tx ds] (claim-batch! tx (queues q))))
          rel-envs (claim :relation)
          _        (publish! rel-envs)
          ent-envs (claim :entity)
          _        (publish! ent-envs)
          audited? (or audited? (some :audit-persist? rel-envs) (some :audit-persist? ent-envs))]
      (if (or (= (count rel-envs) drain-batch-size) (= (count ent-envs) drain-batch-size))
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
      (let [n-rel (persist-batch! db provider (:relation queues))
            n-ent (persist-batch! db provider (:entity queues))]
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
  "Parks on the wake source (safety poll as fallback) and drains on every wake."
  [db wake-source ^java.util.concurrent.BlockingQueue signal stop?]
  (try
    (log/info {:id ::drainer-wakeup-listening
               :data {:action :started :subject :relation-drainer
                      :safety-poll-ms safety-poll-ms
                      :wake-source (.getName (class wake-source))}}
              "Plug drainer listening on WakeSource")
    (loop [backoff-ms 0]
      (when-not @stop?
        (recur
         (try
           (case (wake/wait! wake-source safety-poll-ms)
             :wakeup (when-not @stop? (when (drainer-tick! db) (.offer signal :persist)))
             :poll   (when-not @stop?
                       (drainer-tick! db)
                       (.offer signal :persist)
                       (sqlite-db/incremental-vacuum! db idle-vacuum-pages))
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
                "Plug drainer loop exited"))))

(defn reconcile-relations!
  "Installs the queue and `_ctx` tables, then per-relation triggers across the deployed schema; idempotent."
  [_db tx schema-relations]
  (jdbc/execute! tx [delta-queue-ddl])
  (ensure-seq-column! tx "__relation_delta_queue")
  (jdbc/execute! tx [audit-ctx-ddl])
  (jdbc/execute! tx ["DELETE FROM _ctx WHERE k = 'tenant_xid'"])
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
      (install-relation-triggers! tx rel (policy/audited-relation? (str (:relation rel)))))
    (doseq [rel (vals unique-by-table)]
      (try
        (backfill-xid-columns! tx rel)
        (catch Throwable e
          (log/error! {:id ::backfill-failed
                       :data {:table (:table rel)}} e)))))
  nil)

(defn reconcile-entities!
  "Installs the entity queue, then per-entity triggers across the deployed schema; idempotent."
  [_db tx schema-entities]
  (jdbc/execute! tx [entity-queue-ddl])
  (ensure-seq-column! tx "__entity_delta_queue")
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
                                    (policy/audited-entity? (str entity-id)))
          (catch Throwable e
            (log/error! {:id ::entity-trigger-install-failed
                         :data {:table (:table entity)
                                :entity-id (str entity-id)}} e))))))
  nil)

(defn set-context!
  "Writes the tx's principal/request context into `_ctx` for the plug triggers."
  [_db tx]
  (jdbc/execute! tx [audit-ctx-ddl])
  (let [principal (access/current-principal)]
    (jdbc/execute! tx
                   ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('actor_xid', ?)"
                    (some-> principal :xid str)])
    (jdbc/execute! tx
                   ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('request_id', ?)"
                    (log/request-id)])
    (jdbc/execute! tx
                   ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('scope_xid', ?)"
                    (some-> principal :scope-xid str)]))
  (jdbc/execute! tx
                 ["INSERT OR REPLACE INTO _ctx (k, v) VALUES ('txid', ?)"
                  (str (uuid-v7))])
  nil)

(defn start-drainer!
  "Start the sequencer (claim, seq, publish) and persister (pending → audit provider) threads; idempotent."
  [db]
  (if @drainer-state
    (log/info {:id ::drainer-already-running
               :data {:action :starting :subject :relation-drainer}}
              "Plug drainer already running; ignoring start")
    (let [ds     (:datasource db)
          _      (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS __delta_seq (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"])
          _      (jdbc/execute! ds ["INSERT OR IGNORE INTO __delta_seq (id, value) VALUES (0, 0)"])
          _      (doseq [ddl pending-ddl] (jdbc/execute! ds [ddl]))
          ws     wake/*wake-source*
          _      (wake/start-source! ws)
          stop?  (atom false)
          signal (java.util.concurrent.LinkedBlockingQueue. 1)
          t      (Thread. ^Runnable #(drainer-loop! db ws signal stop?) "synthigy-relation-drainer-sqlite")
          p      (Thread. ^Runnable #(persister-loop! db signal stop?) "synthigy-audit-persister-sqlite")]
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
  "Idempotent full resync of the SQLite delta plug against `model`."
  [db model]
  (when model
    (policy/recompile-policy! model)
    (let [schema (sql.query/model->schema model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn strip-all!
  "Drops every plug trigger; the queue and `_ctx` tables stay."
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
