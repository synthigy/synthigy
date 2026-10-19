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

(ns synthigy.plug.cockroach
  "CockroachDB delta plug: queue DDL, row-level trigger functions, polling drainer, tx context."
  (:require
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [synthigy.audit.protocol :as audit]
    [synthigy.dataset.access :as access]
    [synthigy.dataset.delta :as delta]
    [synthigy.dataset.id :as id]
    [synthigy.json :refer [read-str]]
    [synthigy.log :as log]
    [synthigy.plug.wake :as wake]
    [synthigy.dataset.sql.query :as sql.query]))

(defn wake-drainer!
  "Wakes the drainer; app-path writes call this post-commit (CRDB has no NOTIFY)."
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

(defn uuid-v7
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

(defn strip-quotes
  "Removes the literal double quotes CRDB's SHOW TRIGGERS wraps trigger names in."
  [^String s]
  (when s
    (cond-> s
      (and (.startsWith s "\"") (.endsWith s "\"") (>= (count s) 2))
      (subs 1 (dec (count s))))))

(defn plug-trigger?
  [trigger-name]
  (let [n (strip-quotes (str trigger-name))]
    (boolean
      (or (re-find #"^trg_.+_entity_audit_(insert|update|delete)$" n)
          (re-find #"^trg_.+_audit_(insert|delete)$"               n)))))

(defn drop-all-plug-triggers-for-table!
  "Drops every plug trigger on `table`, found via SHOW TRIGGERS (the only CRDB discovery path)."
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
        stale (filterv plug-trigger? all-names)]
    (log/debug {:id ::plug-trigger-discovery
                :data {:table table :seen all-names :matched stale}}
               "Discovered triggers")
    (doseq [trg stale]
      (try
        (jdbc/execute! tx [(format "DROP TRIGGER IF EXISTS \"%s\" ON \"%s\""
                                   trg table)])
        (log/debug {:id ::trigger-dropped :data {:table table :trigger trg}}
                   "Dropped plug trigger")
        (catch Throwable e
          (log/error! {:id ::trigger-drop-failed
                       :data {:table table :trigger trg}} e))))))

(defn drop-plug-function!
  [tx fn-name]
  ;; no CASCADE on CRDB — drop the function's triggers first
  (jdbc/execute! tx [(format "DROP FUNCTION IF EXISTS \"%s\"()" fn-name)]))

(def ^:private context-payload-pairs
  (str "'tenant_xid', NULLIF(current_setting('synthigy.tenant_xid', true), ''),"
       "'actor_xid',  NULLIF(current_setting('synthigy.actor_xid',  true), ''),"
       "'request_id', NULLIF(current_setting('synthigy.request_id', true), ''),"
       "'scope_xid',  NULLIF(current_setting('synthigy.scope_xid',  true), ''),"
       "'txid',       NULLIF(current_setting('synthigy.txid',       true), '')"))

(defn relation-trigger-name
  [rel-xid op]
  (str "trg_" rel-xid "_audit_" (name op)))

(defn relation-fn-name
  [rel-xid op]
  (str "fn_" rel-xid "_audit_" (name op)))

(defn insert-trigger-fn-ddl
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

(defn delete-trigger-fn-ddl
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

(defn relation-insert-trigger-ddl [{rel-id :relation table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (relation-trigger-name (str rel-id) :insert)
          table
          (relation-fn-name (str rel-id) :insert)))

(defn relation-delete-trigger-ddl [{rel-id :relation table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER DELETE ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (relation-trigger-name (str rel-id) :delete)
          table
          (relation-fn-name (str rel-id) :delete)))

(defn column-exists?
  [tx table column]
  (boolean
   (seq (jdbc/execute! tx
          ["SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
              AND column_name = ?"
           table column]))))

(defn ensure-xid-columns!
  [tx table]
  (jdbc/execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS from_xid varchar(22)" table)])
  (jdbc/execute! tx [(format "ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS to_xid   varchar(22)" table)]))

(defn backfill-xid-columns!
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

(defn install-relation-triggers!
  [tx {table :table rel-id :relation :as rel}]
  (ensure-xid-columns! tx table)
  (drop-all-plug-triggers-for-table! tx table)
  (drop-plug-function! tx (relation-fn-name (str rel-id) :insert))
  (drop-plug-function! tx (relation-fn-name (str rel-id) :delete))
  (jdbc/execute! tx [(insert-trigger-fn-ddl rel)])
  (jdbc/execute! tx [(delete-trigger-fn-ddl rel)])
  (jdbc/execute! tx [(relation-insert-trigger-ddl rel)])
  (jdbc/execute! tx [(relation-delete-trigger-ddl rel)]))

(defn entity-trigger-name
  [entity-xid op]
  (str "trg_" entity-xid "_entity_audit_" (name op)))

(defn entity-fn-name
  [entity-xid op]
  (str "fn_" entity-xid "_entity_audit_" (name op)))

(defn attribute-distinct-or
  [field-map]
  (->> field-map
       (map (fn [[col _attr-xid]]
              (format "(OLD).\"%s\" IS DISTINCT FROM (NEW).\"%s\""
                      (name col) (name col))))
       (str/join " OR ")))

(defn entity-user-fields
  "{column attribute-xid} for user-defined fields; system fields carry keyword xids and are skipped."
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

(defn entity-insert-fn-ddl
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

(defn entity-update-fn-ddl
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

(defn entity-delete-fn-ddl
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

(defn entity-insert-trigger-ddl [entity-xid {table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (entity-trigger-name entity-xid :insert)
          table
          (entity-fn-name entity-xid :insert)))

(defn entity-update-trigger-ddl [entity-xid {table :table :as entity}]
  (let [when-clause (attribute-distinct-or (entity-user-fields entity))]
    (format "CREATE TRIGGER \"%s\" AFTER UPDATE ON \"%s\"
             FOR EACH ROW WHEN (%s)
             EXECUTE FUNCTION \"%s\"()"
            (entity-trigger-name entity-xid :update)
            table
            when-clause
            (entity-fn-name entity-xid :update))))

(defn entity-delete-trigger-ddl [entity-xid {table :table}]
  (format "CREATE TRIGGER \"%s\" AFTER DELETE ON \"%s\"
           FOR EACH ROW EXECUTE FUNCTION \"%s\"()"
          (entity-trigger-name entity-xid :delete)
          table
          (entity-fn-name entity-xid :delete)))

(defn install-entity-triggers!
  [tx entity-xid {table :table :as entity}]
  (when (seq (entity-user-fields entity))
    (drop-all-plug-triggers-for-table! tx table)
    (drop-plug-function! tx (entity-fn-name entity-xid :insert))
    (drop-plug-function! tx (entity-fn-name entity-xid :update))
    (drop-plug-function! tx (entity-fn-name entity-xid :delete))
    (jdbc/execute! tx [(entity-insert-fn-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-update-fn-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-delete-fn-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-insert-trigger-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-update-trigger-ddl entity-xid entity)])
    (jdbc/execute! tx [(entity-delete-trigger-ddl entity-xid entity)])))

(def ^:private drain-batch-size 500)
(def ^:private safety-poll-ms (long 10000))

(defonce ^:private drainer-state (atom nil))

(defn row->envelope
  ;; never (str ts) — CRDB's space-separated form fails Instant/parse downstream
  [{ts :__relation_delta_queue/ts
    payload :__relation_delta_queue/payload}]
  (let [data (if (map? payload) payload (read-str (str payload)))]
    {:element (id/coerce-stored-id (:relation_xid data))
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

(defn entity-row->envelope
  [{ts :__entity_delta_queue/ts
    payload :__entity_delta_queue/payload}]
  (let [data (if (map? payload) payload (read-str (str payload)))]
    {:element (id/coerce-stored-id (:record_xid data))
     :delta {:type (keyword "entity" (name (:op data)))
             :data {:ts         ts
                    :record-xid (id/coerce-stored-id (:record_xid data))
                    :entity-xid (id/coerce-stored-id (:entity_xid data))
                    :tenant     (id/coerce-stored-id (:tenant_xid data))
                    :actor      (id/coerce-stored-id (:actor_xid data))
                    :request    (:request_id data)
                    :scope      (id/coerce-stored-id (:scope_xid data))
                    :txid       (:txid data)
                    :before     (:before data)
                    :after      (:after data)}}}))

(defn drain-batch!
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

(defn drain-entity-batch!
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

(defn drainer-tick!
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

(defn drainer-loop!
  [db wake-source stop?]
  (try
    (log/info {:id ::drainer-wakeup-listening
               :data {:action :started :subject :relation-drainer
                      :safety-poll-ms safety-poll-ms
                      :wake-source (.getName (class wake-source))}}
              "Plug drainer listening on WakeSource (CRDB)")
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
                "Plug drainer loop exited"))))

(defn reconcile-relations!
  "Idempotently installs the relation queue and per-relation trigger pairs, then backfills xids."
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
  "Idempotently installs the entity queue and per-entity trigger triples."
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
  "Writes the tx's actor/request/scope/txid as SET LOCAL GUCs for the plug triggers."
  [_db tx]
  (let [principal (access/current-principal)
        actor   (some-> principal :xid str)
        request (log/request-id)
        scope   (some-> principal :scope-xid str)
        txid    (str (uuid-v7))]
    ;; SET LOCAL takes no parameters on CRDB — never drop the quote escaping
    (letfn [(set-guc! [k v]
              (when v
                (jdbc/execute! tx
                  [(format "SET LOCAL synthigy.%s = '%s'"
                           k (str/replace v "'" "''"))])))]
      (set-guc! "actor_xid"  actor)
      (set-guc! "request_id" request)
      (set-guc! "scope_xid"  scope)
      (set-guc! "txid"       txid)))
  nil)

(defn start-drainer!
  "Starts the drainer thread parked on the active WakeSource; idempotent."
  [db]
  (if @drainer-state
    (log/info {:id ::drainer-already-running
               :data {:action :starting :subject :relation-drainer}}
              "Plug drainer already running; ignoring start")
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
  "Stops the drainer and its wake source; idempotent."
  [_db]
  (when-let [{:keys [thread stop? wake-source]} @drainer-state]
    (reset! stop? true)
    (when wake-source (wake/stop-source! wake-source))
    (.interrupt thread)
    (try (.join thread 5000) (catch InterruptedException _))
    (reset! drainer-state nil)
    nil))

(defn trigger-fingerprint
  "Trigger-relevant shape of `model`; equal fingerprints mean byte-identical plug trigger DDL."
  [model]
  (when model
    (let [schema (sql.query/model->schema model)]
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

(defn reconcile-plug!
  "Idempotent full resync of the CRDB delta plug against `model`."
  [db model]
  (when model
    (let [schema (sql.query/model->schema model)
          relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)]
      (reconcile-relations! db (:datasource db) relations)
      (reconcile-entities!  db (:datasource db) schema)))
  nil)

(defn reconcile-plug-diff!
  "Reinstalls triggers only where the fingerprint changed, drops removed ones; returns the new fingerprint."
  [db old-fp new-model]
  (when new-model
    (let [schema (sql.query/model->schema new-model)
          all-relations (mapcat (fn [[_eid ent]] (vals (:relations ent))) schema)
          new-fp (trigger-fingerprint new-model)
          old-ents (:entities old-fp)
          new-ents (:entities new-fp)
          old-rels (:relations old-fp)
          new-rels (:relations new-fp)
          ent-changed (->> new-ents
                           (keep (fn [[xid fp]]
                                   (when (not= fp (get old-ents xid)) xid)))
                           set)
          rel-changed (->> new-rels
                           (keep (fn [[rid fp]]
                                   (when (not= fp (get old-rels rid)) rid)))
                           set)
          ent-removed (->> (keys old-ents)
                           (remove (set (keys new-ents))))
          rel-removed (->> (keys old-rels)
                           (remove (set (keys new-rels))))
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
                  "Incremental plug reconcile"))
      (let [ds (:datasource db)]
        (doseq [old-xid ent-removed
                :let [old-table (get-in old-ents [old-xid :table])]
                :when old-table]
          (try
            (drop-all-plug-triggers-for-table! ds old-table)
            (doseq [op [:insert :update :delete]]
              (drop-plug-function! ds (entity-fn-name old-xid op)))
            (catch Throwable e
              (log/warn {:id ::drop-removed-entity-failed
                         :data {:entity-xid old-xid :table old-table}}
                        (.getMessage e)))))
        (doseq [old-rid rel-removed
                :let [old-table (get-in old-rels [old-rid :table])]
                :when old-table]
          (try
            (drop-all-plug-triggers-for-table! ds old-table)
            (doseq [op [:insert :delete]]
              (drop-plug-function! ds (relation-fn-name old-rid op)))
            (catch Throwable e
              (log/warn {:id ::drop-removed-relation-failed
                         :data {:relation-xid old-rid :table old-table}}
                        (.getMessage e))))))
      (when (seq rels-changed)
        (reconcile-relations! db (:datasource db) rels-changed))
      (when (seq schema-changed)
        (reconcile-entities! db (:datasource db) schema-changed))
      new-fp)))

(defn strip-all!
  "Drops every plug trigger and function, keeping the queue tables."
  [_db tx]
  (let [tables (->> (jdbc/execute! tx
                     [(str "SELECT tablename FROM pg_tables "
                           "WHERE schemaname = 'public'")])
                    (map :pg_tables/tablename)
                    (remove nil?))]
    (doseq [t tables]
      (drop-all-plug-triggers-for-table! tx t)))
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
