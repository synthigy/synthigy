;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.audit.postgres
  "Audit provider storing `__audit_entity` / `__audit_relation` in the data database, written inside the drainer's transaction."
  (:require
   [clojure.string :as str]
   [patcho.patch :as patch]
   [synthigy.audit.protocol :as audit]
   [synthigy.audit.policy :as policy]
   [synthigy.dataset :as dataset]
   [synthigy.db :as db]
   [synthigy.json :as json]
   [synthigy.log :as log])
  (:import
   [java.io StringReader]
   [java.sql Connection ResultSet]
   [java.time Instant OffsetDateTime ZoneOffset]
   [javax.sql DataSource]
   [org.postgresql PGConnection]))

(def schema-ddl
  ["CREATE TABLE IF NOT EXISTS __audit_entity (
      seq           BIGINT NOT NULL,
      ts            TIMESTAMPTZ NOT NULL,
      tenant_xid    TEXT,
      record_xid    TEXT NOT NULL,
      entity_xid    TEXT,
      attribute_xid TEXT NOT NULL,
      value         TEXT,
      op            TEXT NOT NULL,
      actor_xid     TEXT,
      request_id    TEXT,
      scope_xid     TEXT,
      txid          BIGINT)"
   "CREATE TABLE IF NOT EXISTS __audit_relation (
      seq           BIGINT NOT NULL,
      ts            TIMESTAMPTZ NOT NULL,
      tenant_xid    TEXT,
      relation_xid  TEXT,
      from_xid      TEXT NOT NULL,
      to_xid        TEXT NOT NULL,
      op            TEXT NOT NULL,
      actor_xid     TEXT,
      request_id    TEXT,
      scope_xid     TEXT,
      txid          BIGINT)"
   "CREATE INDEX IF NOT EXISTS __audit_entity_record_ts ON __audit_entity (record_xid, ts)"
   "CREATE INDEX IF NOT EXISTS __audit_entity_seq ON __audit_entity (seq)"
   "CREATE INDEX IF NOT EXISTS __audit_entity_ts ON __audit_entity USING BRIN (ts)"
   "CREATE INDEX IF NOT EXISTS __audit_relation_from_ts ON __audit_relation (from_xid, ts)"
   "CREATE INDEX IF NOT EXISTS __audit_relation_to_ts ON __audit_relation (to_xid, ts)"
   "CREATE INDEX IF NOT EXISTS __audit_relation_seq ON __audit_relation (seq)"
   "CREATE INDEX IF NOT EXISTS __audit_relation_ts ON __audit_relation USING BRIN (ts)"])

(def entity-columns
  ["ts" "tenant_xid" "record_xid" "entity_xid" "attribute_xid" "value" "op"
   "actor_xid" "request_id" "scope_xid" "txid" "seq"])

(def relation-columns
  ["ts" "tenant_xid" "relation_xid" "from_xid" "to_xid" "op"
   "actor_xid" "request_id" "scope_xid" "txid" "seq"])

(defn exec! [^Connection c & sqls]
  (with-open [st (.createStatement c)]
    (doseq [^String s sqls] (.execute st s))))

(defn apply-schema! [^DataSource ds]
  (with-open [c (.getConnection ds)]
    (apply exec! c schema-ddl)))

(defn drop-schema! [^DataSource ds]
  (with-open [c (.getConnection ds)]
    (exec! c "DROP TABLE IF EXISTS __audit_entity" "DROP TABLE IF EXISTS __audit_relation")))

;;; ============================================================================
;;; Writes
;;; ============================================================================

(defn ->odt ^OffsetDateTime [ts]
  (cond
    (nil? ts)                     nil
    (instance? Instant ts)        (.atOffset ^Instant ts ZoneOffset/UTC)
    (instance? java.util.Date ts) (.atOffset (.toInstant ^java.util.Date ts) ZoneOffset/UTC)
    (string? ts)                  (.atOffset (Instant/parse ts) ZoneOffset/UTC)
    :else                         ts))

(defn ->txid [t]
  (cond (nil? t) nil (number? t) (long t) :else (parse-long (str t))))

(defn pg-row [row]
  (-> row
      (update 0 ->odt)
      (update (- (count row) 2) ->txid)))

(defn copy-escape [v]
  (if (nil? v)
    "\\N"
    (-> (str v)
        (str/replace "\\" "\\\\")
        (str/replace "\t" "\\t")
        (str/replace "\n" "\\n")
        (str/replace "\r" "\\r"))))

(defn copy-rows! [^Connection c table columns rows]
  (let [sb (StringBuilder.)]
    (doseq [r rows]
      (loop [[v & more] r first? true]
        (when-not first? (.append sb \tab))
        (.append sb ^String (copy-escape (if (instance? OffsetDateTime v) (str v) v)))
        (when more (recur more false)))
      (.append sb \newline))
    (.copyIn (.getCopyAPI ^PGConnection (.unwrap c PGConnection))
             (str "COPY " table " (" (str/join "," columns) ") FROM STDIN")
             (StringReader. (.toString sb)))))

(defn write!
  "COPY rows in on the drainer's transaction when bound, else in a transaction of their own."
  [table columns rows]
  (when (seq rows)
    (let [rows (mapv pg-row rows)]
      (if-let [^Connection tx audit/*audit-tx*]
        (copy-rows! tx table columns rows)
        (with-open [^Connection c (.getConnection ^DataSource (:datasource db/*db*))]
          (.setAutoCommit c false)
          (try
            (copy-rows! c table columns rows)
            (.commit c)
            (catch Throwable t (.rollback c) (throw t))))))))

;;; ============================================================================
;;; Reads
;;; ============================================================================

(defn read-ts [^ResultSet rs]
  (some-> ^OffsetDateTime (.getObject rs "ts" OffsetDateTime) .toInstant str))

(defn read-txid [^ResultSet rs]
  (let [v (.getLong rs "txid")] (when-not (.wasNull rs) (str v))))

(defn read-entity-event [^ResultSet rs]
  {:track         :entity
   :seq           (.getLong rs "seq")
   :ts            (read-ts rs)
   :tenant-xid    (.getString rs "tenant_xid")
   :record-xid    (.getString rs "record_xid")
   :entity-xid    (.getString rs "entity_xid")
   :attribute-xid (.getString rs "attribute_xid")
   :value         (try (json/read-str (.getString rs "value")) (catch Throwable _ nil))
   :op            (.getString rs "op")
   :actor         (.getString rs "actor_xid")
   :request       (.getString rs "request_id")
   :scope         (.getString rs "scope_xid")
   :txid          (read-txid rs)})

(defn read-relation-event [^ResultSet rs]
  {:track        :relation
   :seq          (.getLong rs "seq")
   :ts           (read-ts rs)
   :tenant-xid   (.getString rs "tenant_xid")
   :relation-xid (.getString rs "relation_xid")
   :from-xid     (.getString rs "from_xid")
   :to-xid       (.getString rs "to_xid")
   :op           (.getString rs "op")
   :actor        (.getString rs "actor_xid")
   :request      (.getString rs "request_id")
   :scope        (.getString rs "scope_xid")
   :txid         (read-txid rs)})

(defn query [sql params read-row]
  (with-open [^Connection c (.getConnection ^DataSource (:datasource db/*db*))
              ps (.prepareStatement c sql)]
    (doseq [[i v] (map-indexed vector params)] (.setObject ps (int (inc i)) v))
    (with-open [rs (.executeQuery ps)]
      (loop [acc (transient [])]
        (if (.next rs) (recur (conj! acc (read-row rs))) (persistent! acc))))))

(defn compile-where [track {:keys [record-xid between cursor seq tenant]}]
  (let [parts (cond-> []
                record-xid (conj (if (= track :relation)
                                   ["(from_xid = ? OR to_xid = ?)" [record-xid record-xid]]
                                   ["record_xid = ?" [record-xid]]))
                between    (into [["ts >= ?" [(->odt (first between))]]
                                  ["ts <= ?" [(->odt (second between))]]])
                cursor     (conj ["ts > ?" [(->odt cursor)]])
                seq        (conj ["seq > ?" [(long seq)]])
                tenant     (conj ["tenant_xid = ?" [(str tenant)]]))]
    [(str/join " AND " (map first parts)) (vec (mapcat second parts))]))

(defn events [track opts order]
  (let [[where params] (compile-where track opts)
        table (if (= track :relation) "__audit_relation" "__audit_entity")]
    (query (str "SELECT * FROM " table
                (when-not (str/blank? where) (str " WHERE " where))
                " ORDER BY " (or order "ts DESC, seq DESC")
                (when-let [l (:limit opts)] (str " LIMIT " (long l))))
           params
           (if (= track :relation) read-relation-event read-entity-event))))

(defn get-at [record-xid at tenant include-deleted?]
  (let [rows     (query (str "SELECT DISTINCT ON (attribute_xid) attribute_xid, value FROM __audit_entity"
                             " WHERE record_xid = ? AND ts <= ?" (when tenant " AND tenant_xid = ?")
                             " ORDER BY attribute_xid, ts DESC, seq DESC")
                        (cond-> [record-xid (->odt at)] tenant (conj (str tenant)))
                        (fn [^ResultSet rs] [(.getString rs 1) (.getString rs 2)]))
        deleted? (some #(= "__delete__" (first %)) rows)
        attrs    (into {} (keep (fn [[a v]]
                                  (when-not (= "__delete__" a)
                                    [(keyword a) (try (json/read-str v) (catch Throwable _ nil))])))
                       rows)]
    (if (and deleted? (not include-deleted?)) nil (not-empty attrs))))

(defn erase! [record-xid tenant]
  (with-open [^Connection c (.getConnection ^DataSource (:datasource db/*db*))]
    (.setAutoCommit c false)
    (try
      (let [run (fn [sql params]
                  (with-open [ps (.prepareStatement c (str sql (when tenant " AND tenant_xid = ?")))]
                    (doseq [[i v] (map-indexed vector (cond-> params tenant (conj (str tenant))))]
                      (.setObject ps (int (inc i)) v))
                    (.executeUpdate ps)))
            n   {:entity   (run "DELETE FROM __audit_entity WHERE record_xid = ?" [record-xid])
                 :relation (run "DELETE FROM __audit_relation WHERE (from_xid = ? OR to_xid = ?)" [record-xid record-xid])}]
        (.commit c)
        n)
      (catch Throwable t
        (.rollback c)
        (throw t)))))

(defrecord PostgresAudit []
  audit/AuditProvider
  (write-entity-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (write! "__audit_entity" entity-columns (policy/entity-rows envelopes identity))))

  (write-relation-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (write! "__audit_relation" relation-columns (policy/relation-rows envelopes identity))))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (get-at record-xid at tenant (boolean include-deleted?)))

  (events [_ {:keys [track] :or {track :entity} :as opts}]
    (events track opts nil))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (let [before (or (get-at record-xid from-ts tenant true) {})
          after  (or (get-at record-xid to-ts tenant true) {})
          ks     (into #{} (concat (keys before) (keys after)))]
      {:before  before
       :after   after
       :changed (vec (sort (filter #(not= (get before %) (get after %)) ks)))}))

  (timeline [_ {:keys [group-by] :or {group-by :request} :as opts}]
    (clojure.core/group-by (case group-by :actor :actor :scope :scope :request)
                           (events :entity opts nil)))

  (since [_ {:keys [track] sq :seq :or {track :entity} :as opts}]
    (events track opts (if sq "seq ASC" "ts ASC, seq ASC")))

  (erase! [_ {:keys [record-xid tenant]}]
    (erase! record-xid tenant)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(def model-watch-key ::audit-policy-watch)

(defn setup []
  (apply-schema! (:datasource db/*db*))
  (log/info {:id ::setup-complete
             :data {:action :setup-complete :subject :audit-provider :backend :postgres}}
            "Postgres audit tables ready"))

(defn start []
  (apply-schema! (:datasource db/*db*))
  (patch/level! :synthigy.audit/postgres)
  (alter-var-root #'audit/*audit-provider* (constantly (->PostgresAudit)))
  (policy/recompile-policy! (dataset/deployed-model))
  (dataset/add-model-watch! model-watch-key (fn [_ _ _ new-model] (policy/recompile-policy! new-model)))
  (log/info {:id ::started
             :data {:action :started :subject :audit-provider :backend :postgres}}
            "Postgres audit provider started"))

(defn stop []
  (dataset/remove-model-watch! model-watch-key)
  (policy/recompile-policy! nil)
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (log/info {:id ::stopped
             :data {:action :stopped :subject :audit-provider :backend :postgres}}
            "Postgres audit provider stopped"))

(defn cleanup []
  (drop-schema! (:datasource db/*db*)))

(patch/current-version :synthigy.audit/postgres "1.0.0")
