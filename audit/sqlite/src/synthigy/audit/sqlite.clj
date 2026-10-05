;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.audit.sqlite
  "Audit provider in its own SQLite file (`audit.db`), beside the data database."
  (:require
   [clojure.string :as str]
   [patcho.patch :as patch]
   [synthigy.audit.protocol :as audit]
   [synthigy.audit.policy :as policy]
   [synthigy.dataset :as dataset]
   synthigy.env
   [synthigy.json :as json]
   [synthigy.log :as log])
  (:import
   [java.io File]
   [java.sql Connection DriverManager PreparedStatement ResultSet]
   [java.time Instant]))

;;; ============================================================================
;;; Connection
;;; ============================================================================

(defn audit-path []
  (str synthigy.env/db-dir "/audit.db"))

(defn memory? [path]
  (or (nil? path) (str/blank? path) (= ":memory:" path)))

(defn jdbc-url
  "JDBC URL for `path`; `:memory:` becomes a uniquely named shared-cache database."
  [path]
  (if (memory? path)
    (str "jdbc:sqlite:file:synthigy-audit-" (random-uuid) "?mode=memory&cache=shared")
    (str "jdbc:sqlite:" path)))

(defn exec! [^Connection c & sqls]
  (with-open [st (.createStatement c)]
    (doseq [^String s sqls] (.execute st s))))

(defn open! ^Connection [^String url]
  (let [c (DriverManager/getConnection url)]
    (exec! c "PRAGMA busy_timeout = 30000" "PRAGMA synchronous = NORMAL")
    (if (str/includes? url "mode=memory")
      (exec! c "PRAGMA read_uncommitted = true")
      (exec! c "PRAGMA journal_mode = WAL"))
    c))

(defn bind-params! [^PreparedStatement ps params]
  (doseq [[i v] (map-indexed vector params)]
    (.setObject ps (int (inc i)) v)))

(defn query-rows [url ^String sql params read-row]
  (with-open [c  (open! url)
              ps (.prepareStatement c sql)]
    (bind-params! ps params)
    (with-open [rs (.executeQuery ps)]
      (loop [acc (transient [])]
        (if (.next rs) (recur (conj! acc (read-row rs))) (persistent! acc))))))

;;; ============================================================================
;;; Time — INTEGER epoch microseconds, UTC
;;; ============================================================================

(defn ->micros [x]
  (when-let [^Instant i (cond
                          (instance? Instant x)        x
                          (instance? java.util.Date x) (.toInstant ^java.util.Date x)
                          (string? x)                  (try (Instant/parse x) (catch Throwable _ nil)))]
    (+ (* (.getEpochSecond i) 1000000) (quot (.getNano i) 1000))))

(defn micros->iso [^ResultSet rs ^String col]
  (let [m (.getLong rs col)]
    (when-not (.wasNull rs)
      (str (Instant/ofEpochSecond (Math/floorDiv m 1000000) (* 1000 (Math/floorMod m 1000000)))))))

;;; ============================================================================
;;; Schema
;;; ============================================================================

(def schema-ddl
  ["CREATE TABLE IF NOT EXISTS audit_entity (
      ts            INTEGER,
      tenant_xid    TEXT,
      record_xid    TEXT,
      entity_xid    TEXT,
      attribute_xid TEXT,
      value         TEXT,
      op            TEXT,
      actor_xid     TEXT,
      request_id    TEXT,
      scope_xid     TEXT,
      txid          TEXT,
      seq           INTEGER)"
   "CREATE TABLE IF NOT EXISTS audit_relation (
      ts            INTEGER,
      tenant_xid    TEXT,
      relation_xid  TEXT,
      from_xid      TEXT,
      to_xid        TEXT,
      op            TEXT,
      actor_xid     TEXT,
      request_id    TEXT,
      scope_xid     TEXT,
      txid          TEXT,
      seq           INTEGER)"
   "CREATE INDEX IF NOT EXISTS audit_entity_record_ts ON audit_entity(record_xid, ts)"
   "CREATE INDEX IF NOT EXISTS audit_entity_seq ON audit_entity(seq)"
   "CREATE INDEX IF NOT EXISTS audit_entity_ts ON audit_entity(ts)"
   "CREATE INDEX IF NOT EXISTS audit_relation_from_ts ON audit_relation(from_xid, ts)"
   "CREATE INDEX IF NOT EXISTS audit_relation_to_ts ON audit_relation(to_xid, ts)"
   "CREATE INDEX IF NOT EXISTS audit_relation_seq ON audit_relation(seq)"
   "CREATE INDEX IF NOT EXISTS audit_relation_ts ON audit_relation(ts)"])

;;; ============================================================================
;;; Writes
;;; ============================================================================

(defn insert-rows!
  "Insert rows in one transaction, first deleting the batch's seq range so a retried batch stays idempotent."
  [^Connection c table rows]
  (when (seq rows)
    (let [seqs (keep peek rows)]
      (locking c
        (.setAutoCommit c false)
        (try
          (when (seq seqs)
            (with-open [ps (.prepareStatement c (str "DELETE FROM " table " WHERE seq BETWEEN ? AND ?"))]
              (.setLong ps 1 (long (apply min seqs)))
              (.setLong ps 2 (long (apply max seqs)))
              (.executeUpdate ps)))
          (with-open [ps (.prepareStatement
                          c (str "INSERT INTO " table " VALUES ("
                                 (str/join "," (repeat (count (first rows)) "?")) ")"))]
            (doseq [r rows]
              (bind-params! ps r)
              (.addBatch ps))
            (.executeBatch ps))
          (.commit c)
          (catch Throwable t
            (try (.rollback c) (catch Throwable _))
            (throw t))
          (finally
            (.setAutoCommit c true)))))))

;;; ============================================================================
;;; Reads
;;; ============================================================================

(defn read-entity-event [^ResultSet rs]
  {:track         :entity
   :seq           (.getLong rs "seq")
   :ts            (micros->iso rs "ts")
   :tenant-xid    (.getString rs "tenant_xid")
   :record-xid    (.getString rs "record_xid")
   :entity-xid    (.getString rs "entity_xid")
   :attribute-xid (.getString rs "attribute_xid")
   :value         (try (json/read-str (.getString rs "value")) (catch Throwable _ nil))
   :op            (.getString rs "op")
   :actor         (.getString rs "actor_xid")
   :request       (.getString rs "request_id")
   :scope         (.getString rs "scope_xid")
   :txid          (.getString rs "txid")})

(defn read-relation-event [^ResultSet rs]
  {:track        :relation
   :seq          (.getLong rs "seq")
   :ts           (micros->iso rs "ts")
   :tenant-xid   (.getString rs "tenant_xid")
   :relation-xid (.getString rs "relation_xid")
   :from-xid     (.getString rs "from_xid")
   :to-xid       (.getString rs "to_xid")
   :op           (.getString rs "op")
   :actor        (.getString rs "actor_xid")
   :request      (.getString rs "request_id")
   :scope        (.getString rs "scope_xid")
   :txid         (.getString rs "txid")})

(defn compile-where [track {:keys [record-xid between cursor seq tenant]}]
  (let [parts (cond-> []
                record-xid (conj (if (= track :relation)
                                   ["(from_xid = ? OR to_xid = ?)" [record-xid record-xid]]
                                   ["record_xid = ?" [record-xid]]))
                between    (into [["ts >= ?" [(->micros (first between))]]
                                  ["ts <= ?" [(->micros (second between))]]])
                cursor     (conj ["ts > ?" [(->micros cursor)]])
                seq        (conj ["seq > ?" [seq]])
                tenant     (conj ["tenant_xid = ?" [tenant]]))]
    [(str/join " AND " (map first parts)) (vec (mapcat second parts))]))

(defn events [url track opts order]
  (let [[where params] (compile-where track opts)
        table (if (= track :relation) "audit_relation" "audit_entity")]
    (query-rows url (str "SELECT * FROM " table
                         (when-not (str/blank? where) (str " WHERE " where))
                         " ORDER BY " (or order "ts DESC, seq DESC")
                         (when-let [l (:limit opts)] (str " LIMIT " (long l))))
                params
                (if (= track :relation) read-relation-event read-entity-event))))

(defn get-at [url record-xid at tenant include-deleted?]
  (let [rows     (query-rows url (str "SELECT attribute_xid, value FROM ("
                                      "SELECT attribute_xid, value, ROW_NUMBER() OVER "
                                      "(PARTITION BY attribute_xid ORDER BY ts DESC, seq DESC) AS rn FROM audit_entity"
                                      " WHERE record_xid = ? AND ts <= ?" (when tenant " AND tenant_xid = ?")
                                      ") WHERE rn = 1")
                             (cond-> [record-xid (->micros at)] tenant (conj tenant))
                             (fn [^ResultSet rs] [(.getString rs 1) (.getString rs 2)]))
        deleted? (some #(= "__delete__" (first %)) rows)
        attrs    (into {} (keep (fn [[a v]]
                                  (when-not (= "__delete__" a)
                                    [(keyword a) (try (json/read-str v) (catch Throwable _ nil))])))
                       rows)]
    (if (and deleted? (not include-deleted?)) nil (not-empty attrs))))

(defn erase! [^Connection c record-xid tenant]
  (locking c
    (.setAutoCommit c false)
    (try
      (let [run (fn [sql params]
                  (with-open [ps (.prepareStatement c (str sql (when tenant " AND tenant_xid = ?")))]
                    (bind-params! ps (cond-> params tenant (conj tenant)))
                    (.executeUpdate ps)))
            n   {:entity   (run "DELETE FROM audit_entity WHERE record_xid = ?" [record-xid])
                 :relation (run "DELETE FROM audit_relation WHERE (from_xid = ? OR to_xid = ?)" [record-xid record-xid])}]
        (.commit c)
        n)
      (catch Throwable t
        (try (.rollback c) (catch Throwable _))
        (throw t))
      (finally
        (.setAutoCommit c true)))))

;;; ============================================================================
;;; Provider
;;; ============================================================================

(defrecord SQLiteAudit [^Connection writer url]
  audit/AuditProvider
  (write-entity-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (insert-rows! writer "audit_entity" (policy/entity-rows envelopes ->micros))))

  (write-relation-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (insert-rows! writer "audit_relation" (policy/relation-rows envelopes ->micros))))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (get-at url record-xid at tenant (boolean include-deleted?)))

  (events [_ {:keys [track] :or {track :entity} :as opts}]
    (events url track opts nil))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (let [before (or (get-at url record-xid from-ts tenant true) {})
          after  (or (get-at url record-xid to-ts tenant true) {})
          ks     (into #{} (concat (keys before) (keys after)))]
      {:before  before
       :after   after
       :changed (vec (sort (filter #(not= (get before %) (get after %)) ks)))}))

  (timeline [_ {:keys [group-by] :or {group-by :request} :as opts}]
    (clojure.core/group-by (case group-by :actor :actor :scope :scope :request)
                           (events url :entity opts nil)))

  (since [_ {:keys [track] sq :seq :or {track :entity} :as opts}]
    (events url track opts (if sq "seq ASC" "ts ASC, seq ASC")))

  (erase! [_ {:keys [record-xid tenant]}]
    (erase! writer record-xid tenant)))

(defn open-provider
  "Open (creating if needed) the audit database at `path` and return the provider."
  [path]
  (when-not (memory? path)
    (when-let [parent (.getParentFile (File. ^String path))]
      (when-not (.exists parent) (.mkdirs parent))))
  (let [url    (jdbc-url path)
        writer (open! url)]
    (apply exec! writer schema-ddl)
    (->SQLiteAudit writer url)))

(defn close-provider! [{:keys [^Connection writer]}]
  (try (.close writer) (catch Throwable _)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(defonce state (atom nil))

(def model-watch-key ::audit-policy-watch)

(defn setup []
  (close-provider! (open-provider (audit-path)))
  (log/info {:id ::setup-complete
             :data {:action :setup-complete :subject :audit-provider :backend :sqlite :path (audit-path)}}
            "SQLite audit database ready"))

(defn start []
  (let [p (open-provider (audit-path))]
    (patch/level! :synthigy.audit/sqlite)
    (reset! state {:provider p})
    (alter-var-root #'audit/*audit-provider* (constantly p))
    (policy/recompile-policy! (dataset/deployed-model))
    (dataset/add-model-watch! model-watch-key (fn [_ _ _ new-model] (policy/recompile-policy! new-model)))
    (log/info {:id ::started
               :data {:action :started :subject :audit-provider :backend :sqlite :path (audit-path)}}
              "SQLite audit provider started")))

(defn stop []
  (dataset/remove-model-watch! model-watch-key)
  (policy/recompile-policy! nil)
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (when-let [{:keys [provider]} @state] (close-provider! provider))
  (reset! state nil)
  (log/info {:id ::stopped :data {:action :stopped :subject :audit-provider :backend :sqlite}}
            "SQLite audit provider stopped"))

(defn cleanup []
  (with-open [c (open! (jdbc-url (audit-path)))]
    (exec! c "DROP TABLE IF EXISTS audit_entity" "DROP TABLE IF EXISTS audit_relation")))

(patch/current-version :synthigy.audit/sqlite "1.0.0")
