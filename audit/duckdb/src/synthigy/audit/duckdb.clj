;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.audit.duckdb
  "Audit history in its own embedded DuckDB file."
  (:require
   [clojure.string :as str]
   [patcho.patch :as patch]
   [synthigy.audit.policy :as policy]
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.duckdb :as duck]
   synthigy.env
   [synthigy.json :as json]
   [synthigy.log :as log])
  (:import
   [java.sql Connection ResultSet]
   [org.duckdb DuckDBAppender DuckDBConnection]))

(defn audit-path []
  (str synthigy.env/db-dir "/audit.duckdb"))

(def schema-ddl
  ["CREATE TABLE IF NOT EXISTS audit_entity (
      ts            TIMESTAMP,
      tenant_xid    VARCHAR,
      record_xid    VARCHAR,
      entity_xid    VARCHAR,
      attribute_xid VARCHAR,
      value         JSON,
      op            VARCHAR,
      actor_xid     VARCHAR,
      request_id    VARCHAR,
      scope_xid     VARCHAR,
      txid          VARCHAR,
      seq           BIGINT
    )"
   "CREATE TABLE IF NOT EXISTS audit_relation (
      ts            TIMESTAMP,
      tenant_xid    VARCHAR,
      relation_xid  VARCHAR,
      from_xid      VARCHAR,
      to_xid        VARCHAR,
      op            VARCHAR,
      actor_xid     VARCHAR,
      request_id    VARCHAR,
      scope_xid     VARCHAR,
      txid          VARCHAR,
      seq           BIGINT
    )"])

(defn with-tx
  "Run `f` on `conn` inside one transaction; rollback and rethrow so a failed drainer batch stays queued."
  [^Connection conn f]
  (locking conn
    (.setAutoCommit conn false)
    (try
      (let [r (f)] (.commit conn) r)
      (catch Throwable t
        (try (.rollback conn) (catch Throwable _))
        (throw t))
      (finally
        (.setAutoCommit conn true)))))

(defn delete-seq-range!
  "Idempotency for a retried drainer batch: its seqs are contiguous and owned by it alone."
  [^Connection conn table rows]
  (when-let [seqs (seq (keep peek rows))]
    (with-open [ps (.prepareStatement conn (str "DELETE FROM " table " WHERE seq BETWEEN ? AND ?"))]
      (.setLong ps 1 (long (apply min seqs)))
      (.setLong ps 2 (long (apply max seqs)))
      (.execute ps))))

(defn append-row! [^DuckDBAppender a row]
  (.beginRow a)
  (let [last-i (dec (count row))
        txid-i (dec last-i)]
    (doseq [[i v] (map-indexed vector row)]
      (cond
        (zero? i)    (.appendLocalDateTime a v)
        (= i last-i) (if (nil? v) (.append a ^String v) (.append a (long v)))
        (= i txid-i) (.append a ^String (some-> v str))
        :else        (.append a ^String v))))
  (.endRow a))

(defn insert-rows! [^Connection conn table rows]
  (when (seq rows)
    (with-tx conn
      (fn []
        (delete-seq-range! conn table rows)
        (with-open [a (.createAppender ^DuckDBConnection conn "main" table)]
          (doseq [r rows] (append-row! a r)))))))

(defn read-entity-event [^ResultSet rs]
  {:track         :entity
   :seq           (.getLong rs "seq")
   :ts            (duck/utc->iso (duck/read-utc rs "ts"))
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
   :ts           (duck/utc->iso (duck/read-utc rs "ts"))
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
                between    (into [["ts >= ?" [(duck/inst->utc (first between))]]
                                  ["ts <= ?" [(duck/inst->utc (second between))]]])
                cursor     (conj ["ts > ?" [(duck/inst->utc cursor)]])
                seq        (conj ["seq > ?" [seq]])
                tenant     (conj ["tenant_xid = ?" [tenant]]))]
    [(str/join " AND " (map first parts)) (vec (mapcat second parts))]))

(defn events [conn track opts order]
  (let [[where params] (compile-where track opts)
        table  (if (= track :relation) "audit_relation" "audit_entity")
        reader (if (= track :relation) read-relation-event read-entity-event)
        sql    (str "SELECT * FROM " table
                    (when-not (str/blank? where) (str " WHERE " where))
                    " ORDER BY " (or order "ts DESC, seq DESC")
                    (when-let [l (:limit opts)] (str " LIMIT " (long l))))]
    (duck/with-duplicate conn
      (fn [^Connection rc]
        (with-open [ps (.prepareStatement rc sql)]
          (duck/bind-params! ps params)
          (with-open [rs (.executeQuery ps)]
            (loop [acc (transient [])]
              (if (.next rs) (recur (conj! acc (reader rs))) (persistent! acc)))))))))

(defn get-at [conn record-xid at tenant include-deleted?]
  (let [sql (str "SELECT attribute_xid, value FROM audit_entity"
                 " WHERE record_xid = ? AND ts <= ?" (when tenant " AND tenant_xid = ?")
                 " QUALIFY ROW_NUMBER() OVER (PARTITION BY attribute_xid ORDER BY ts DESC, seq DESC) = 1")
        rows (duck/with-duplicate conn
               (fn [^Connection rc]
                 (with-open [ps (.prepareStatement rc sql)]
                   (duck/bind-params! ps (cond-> [record-xid (duck/inst->utc at)] tenant (conj tenant)))
                   (with-open [rs (.executeQuery ps)]
                     (loop [acc (transient [])]
                       (if (.next rs)
                         (recur (conj! acc [(.getString rs 1) (.getString rs 2)]))
                         (persistent! acc)))))))
        deleted? (some #(= "__delete__" (first %)) rows)
        attrs    (into {} (keep (fn [[a v]]
                                  (when-not (= "__delete__" a)
                                    [(keyword a) (try (json/read-str v) (catch Throwable _ nil))])))
                       rows)]
    (if (and deleted? (not include-deleted?)) nil (not-empty attrs))))

(defn erase! [^Connection conn record-xid tenant]
  (with-tx conn
    (fn []
      (let [run (fn [sql params]
                  (with-open [ps (.prepareStatement conn (str sql (when tenant " AND tenant_xid = ?")))]
                    (duck/bind-params! ps (cond-> params tenant (conj tenant)))
                    (.executeUpdate ps)))]
        {:entity   (run "DELETE FROM audit_entity WHERE record_xid = ?" [record-xid])
         :relation (run "DELETE FROM audit_relation WHERE (from_xid = ? OR to_xid = ?)" [record-xid record-xid])}))))

(defrecord DuckDBAudit [^Connection conn path]
  audit/AuditProvider
  (write-entity-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (insert-rows! conn "audit_entity" (policy/entity-rows envelopes duck/inst->utc))))

  (write-relation-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (insert-rows! conn "audit_relation" (policy/relation-rows envelopes duck/inst->utc))))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (get-at conn record-xid at tenant (boolean include-deleted?)))

  (events [_ {:keys [track] :or {track :entity} :as opts}]
    (events conn track opts nil))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (let [before (or (get-at conn record-xid from-ts tenant true) {})
          after  (or (get-at conn record-xid to-ts tenant true) {})
          ks     (into #{} (concat (keys before) (keys after)))]
      {:before  before
       :after   after
       :changed (vec (sort (filter #(not= (get before %) (get after %)) ks)))}))

  (timeline [_ {:keys [group-by] :or {group-by :request} :as opts}]
    (clojure.core/group-by (case group-by :actor :actor :scope :scope :request)
                           (events conn :entity opts nil)))

  (since [_ {:keys [track] sq :seq :or {track :entity} :as opts}]
    (events conn track opts (if sq "seq ASC" "ts ASC, seq ASC")))

  (erase! [_ {:keys [record-xid tenant]}]
    (erase! conn (str record-xid) tenant)))

(defn open-provider
  "Open (creating if needed) the audit database at `path` and return the provider."
  [path]
  (let [conn (duck/open-connection path (duck/memory-limit))]
    (apply duck/exec! conn schema-ddl)
    (->DuckDBAudit conn path)))

(defn close-provider! [{:keys [^Connection conn]}]
  (try (.close conn) (catch Throwable _)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(defonce state (atom nil))

(def model-watch-key ::audit-policy-watch)

(defn setup []
  (close-provider! (open-provider (audit-path)))
  (log/info {:id ::setup-complete
             :data {:action :setup-complete :subject :audit-provider :backend :duckdb :path (audit-path)}}
            "DuckDB audit database ready"))

(defn start []
  (let [p (open-provider (audit-path))]
    (patch/level! :synthigy.audit/duckdb)
    (reset! state {:provider p})
    (alter-var-root #'audit/*audit-provider* (constantly p))
    (policy/recompile-policy! (dataset/deployed-model))
    (dataset/add-model-watch! model-watch-key (fn [_ _ _ new-model] (policy/recompile-policy! new-model)))
    (log/info {:id ::started
               :data {:action :started :subject :audit-provider :backend :duckdb :path (audit-path)}}
              "DuckDB audit provider started")))

(defn stop []
  (dataset/remove-model-watch! model-watch-key)
  (policy/recompile-policy! nil)
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (when-let [{:keys [provider]} @state]
    (close-provider! provider))
  (reset! state nil)
  (log/info {:id ::stopped :data {:action :stopped :subject :audit-provider :backend :duckdb}}
            "DuckDB audit provider stopped"))

(defn cleanup []
  (with-open [conn (duck/open-connection (audit-path) (duck/memory-limit))]
    (duck/exec! conn "DROP TABLE IF EXISTS audit_entity" "DROP TABLE IF EXISTS audit_relation")))

(patch/current-version :synthigy.audit/duckdb "1.0.0")
