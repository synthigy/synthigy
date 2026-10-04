;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.audit.clickhouse
  "Audit provider keeping `audit_entity` / `audit_relation` in ClickHouse."
  (:require
   [clojure.string :as str]
   [patcho.patch :as patch]
   [synthigy.audit.policy :as policy]
   [synthigy.audit.protocol :as audit]
   [synthigy.clickhouse :as ch]
   [synthigy.dataset :as dataset]
   [synthigy.json :as json]
   [synthigy.log :as log]))

;;; ============================================================================
;;; Schema
;;; ============================================================================

(defn audit-entity-ddl [database]
  (str "CREATE TABLE IF NOT EXISTS " database ".audit_entity (
          ts             DateTime64(3, 'UTC'),
          tenant_xid     String,
          record_xid     String,
          entity_xid     LowCardinality(String),
          attribute_xid  LowCardinality(String),
          value          String,
          op             LowCardinality(String),
          actor_xid      String,
          request_id     String,
          scope_xid      String,
          txid           String,
          seq            Int64 DEFAULT 0,
          INDEX idx_entity_seq seq TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (entity_xid, record_xid, ts)
        SETTINGS index_granularity = 8192"))

(defn audit-relation-ddl [database]
  (str "CREATE TABLE IF NOT EXISTS " database ".audit_relation (
          ts             DateTime64(3, 'UTC'),
          tenant_xid     String,
          relation_xid   LowCardinality(String),
          from_xid       String,
          to_xid         String,
          op             LowCardinality(String),
          actor_xid      String,
          request_id     String,
          scope_xid      String,
          txid           String,
          seq            Int64 DEFAULT 0,
          INDEX idx_relation_seq seq TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (relation_xid, ts)
        SETTINGS index_granularity = 8192"))

(defn migrations-ddl [db]
  [(str "ALTER TABLE " db ".audit_entity ADD COLUMN IF NOT EXISTS seq Int64 DEFAULT 0")
   (str "ALTER TABLE " db ".audit_relation ADD COLUMN IF NOT EXISTS seq Int64 DEFAULT 0")])

(defn apply-schema! [client {:keys [database] :as config}]
  (ch/exec-ddl! client config (ch/create-db-ddl database))
  (ch/exec-ddl! client config (audit-entity-ddl database))
  (ch/exec-ddl! client config (audit-relation-ddl database))
  (doseq [ddl (migrations-ddl database)]
    (try (ch/exec-ddl! client config ddl) (catch Throwable _))))

(defn drop-schema! [client {:keys [database] :as config}]
  (ch/exec-ddl! client config (str "DROP TABLE IF EXISTS " database ".audit_entity"))
  (ch/exec-ddl! client config (str "DROP TABLE IF EXISTS " database ".audit_relation")))

;;; ============================================================================
;;; Writes — synchronous: the persister acks pending rows on return
;;; ============================================================================

(def entity-columns
  [:ts :tenant_xid :record_xid :entity_xid :attribute_xid :value :op
   :actor_xid :request_id :scope_xid :txid :seq])

(def relation-columns
  [:ts :tenant_xid :relation_xid :from_xid :to_xid :op
   :actor_xid :request_id :scope_xid :txid :seq])

(defn ->ch-row [columns row]
  (into {}
        (map (fn [k v] [k (case k
                            :ts  (ch/inst->ch-string v)
                            :seq (or v 0)
                            (ch/str-or-empty v))])
             columns row)))

;;; ============================================================================
;;; Reads
;;; ============================================================================

(defn blank->nil [s] (when-not (str/blank? s) s))

(defn read-entity-event [r]
  {:track         :entity
   :seq           (:seq r)
   :ts            (ch/ch->iso (:ts r))
   :tenant-xid    (blank->nil (:tenant_xid r))
   :record-xid    (:record_xid r)
   :entity-xid    (blank->nil (:entity_xid r))
   :attribute-xid (:attribute_xid r)
   :value         (try (json/read-str (:value r)) (catch Throwable _ nil))
   :op            (:op r)
   :actor         (blank->nil (:actor_xid r))
   :request       (blank->nil (:request_id r))
   :scope         (blank->nil (:scope_xid r))
   :txid          (blank->nil (:txid r))})

(defn read-relation-event [r]
  {:track        :relation
   :seq          (:seq r)
   :ts           (ch/ch->iso (:ts r))
   :tenant-xid   (blank->nil (:tenant_xid r))
   :relation-xid (blank->nil (:relation_xid r))
   :from-xid     (:from_xid r)
   :to-xid       (:to_xid r)
   :op           (:op r)
   :actor        (blank->nil (:actor_xid r))
   :request      (blank->nil (:request_id r))
   :scope        (blank->nil (:scope_xid r))
   :txid         (blank->nil (:txid r))})

(defn ts-ph [state v]
  (str "parseDateTime64BestEffort(" (ch/bind! state (ch/inst->ch-string v) :String) ", 3)"))

(defn compile-where [state track {:keys [record-xid between cursor seq tenant]}]
  (str/join " AND "
            (cond-> []
              record-xid (conj (if (= track :relation)
                                 (let [ph (ch/bind! state record-xid :String)]
                                   (str "(from_xid = " ph " OR to_xid = " ph ")"))
                                 (str "record_xid = " (ch/bind! state record-xid :String))))
              between    (into [(str "ts >= " (ts-ph state (first between)))
                                (str "ts <= " (ts-ph state (second between)))])
              cursor     (conj (str "ts > " (ts-ph state cursor)))
              seq        (conj (str "seq > " (ch/bind! state seq :Int64)))
              tenant     (conj (str "tenant_xid = " (ch/bind! state tenant :String))))))

(defn events [client config track opts order]
  (let [state (ch/new-params)
        where (compile-where state track opts)
        sql   (str "SELECT * FROM " (:database config) "."
                   (if (= track :relation) "audit_relation" "audit_entity")
                   (when (seq where) (str " WHERE " where))
                   " ORDER BY " (or order "ts DESC, seq DESC")
                   (when-let [l (:limit opts)] (str " LIMIT " (long l)))
                   " FORMAT JSONEachRow")]
    (mapv (if (= track :relation) read-relation-event read-entity-event)
          (ch/parse-rows (ch/query! client config sql (:params @state))))))

(defn get-at [client config record-xid at tenant include-deleted?]
  (let [state (ch/new-params)
        sql   (str "SELECT attribute_xid, argMax(value, (ts, seq)) AS value"
                   " FROM " (:database config) ".audit_entity"
                   " WHERE record_xid = " (ch/bind! state record-xid :String)
                   " AND ts <= " (ts-ph state at)
                   (when tenant (str " AND tenant_xid = " (ch/bind! state tenant :String)))
                   " GROUP BY attribute_xid FORMAT JSONEachRow")
        rows  (ch/parse-rows (ch/query! client config sql (:params @state)))
        deleted? (some #(= "__delete__" (:attribute_xid %)) rows)
        attrs (into {} (keep (fn [{a :attribute_xid v :value}]
                               (when-not (= "__delete__" a)
                                 [(keyword a) (try (json/read-str v) (catch Throwable _ nil))])))
                    rows)]
    (if (and deleted? (not include-deleted?)) nil (not-empty attrs))))

(defn erase! [client {:keys [database] :as config} record-xid tenant]
  (let [run (fn [table match]
              (let [state  (ch/new-params)
                    ph     (ch/bind! state record-xid :String)
                    where  (str (match ph) (when tenant (str " AND tenant_xid = " (ch/bind! state tenant :String))))
                    n      (some-> (ch/parse-rows (ch/query! client config
                                                             (str "SELECT count() AS c FROM " database "." table
                                                                  " WHERE " where " FORMAT JSONEachRow")
                                                             (:params @state)))
                                   first :c ch/parse-count)]
                (when (pos? (or n 0))
                  (ch/command! client config (str "ALTER TABLE " database "." table " DELETE WHERE " where)
                               (:params @state) {"mutations_sync" 1}))
                (or n 0)))]
    {:entity   (run "audit_entity" #(str "record_xid = " %))
     :relation (run "audit_relation" #(str "(from_xid = " % " OR to_xid = " % ")"))}))

;;; ============================================================================
;;; Provider
;;; ============================================================================

(defrecord ClickHouseAudit [client config]
  audit/AuditProvider
  (write-entity-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (ch/insert! client config "audit_entity"
                  (mapv #(->ch-row entity-columns %) (policy/entity-rows envelopes identity)) nil)))

  (write-relation-deltas! [_ envelopes]
    (when (policy/persistence-enabled?)
      (ch/insert! client config "audit_relation"
                  (mapv #(->ch-row relation-columns %) (policy/relation-rows envelopes identity)) nil)))

  (get-at [_ {:keys [record-xid at tenant include-deleted?]}]
    (get-at client config record-xid at tenant (boolean include-deleted?)))

  (events [_ {:keys [track] :or {track :entity} :as opts}]
    (events client config track opts nil))

  (diff [_ {:keys [record-xid from-ts to-ts tenant]}]
    (let [before (or (get-at client config record-xid from-ts tenant true) {})
          after  (or (get-at client config record-xid to-ts tenant true) {})
          ks     (into #{} (concat (keys before) (keys after)))]
      {:before  before
       :after   after
       :changed (vec (sort (filter #(not= (get before %) (get after %)) ks)))}))

  (timeline [_ {:keys [group-by] :or {group-by :request} :as opts}]
    (clojure.core/group-by (case group-by :actor :actor :scope :scope :request)
                           (events client config :entity opts nil)))

  (since [_ {:keys [track] sq :seq :or {track :entity} :as opts}]
    (events client config track opts (if sq "seq ASC" "ts ASC, seq ASC")))

  (erase! [_ {:keys [record-xid tenant]}]
    (erase! client config (str record-xid) tenant)))

(defn open-provider
  "Connect with `config` over the CLICKHOUSE_* environment and apply the schema."
  [config]
  (let [cfg    (merge (ch/env-config) config)
        client (ch/new-client cfg)]
    (apply-schema! client cfg)
    (->ClickHouseAudit client cfg)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(def model-watch-key ::audit-policy-watch)

(defn setup []
  (let [cfg (ch/env-config)]
    (apply-schema! (ch/new-client cfg) cfg)
    (log/info {:id ::setup-complete
               :data {:action :setup-complete :subject :audit-provider :backend :clickhouse
                      :url (:url cfg) :database (:database cfg)}}
              "ClickHouse audit schema applied")))

(defn start []
  (let [p (open-provider {})]
    (patch/level! :synthigy.audit/clickhouse)
    (alter-var-root #'audit/*audit-provider* (constantly p))
    (policy/recompile-policy! (dataset/deployed-model))
    (dataset/add-model-watch! model-watch-key (fn [_ _ _ new-model] (policy/recompile-policy! new-model)))
    (log/info {:id ::started
               :data {:action :started :subject :audit-provider :backend :clickhouse
                      :url (get-in p [:config :url]) :database (get-in p [:config :database])}}
              "ClickHouse audit provider started")))

(defn stop []
  (dataset/remove-model-watch! model-watch-key)
  (policy/recompile-policy! nil)
  (alter-var-root #'audit/*audit-provider* (constantly nil))
  (log/info {:id ::stopped :data {:action :stopped :subject :audit-provider :backend :clickhouse}}
            "ClickHouse audit provider stopped"))

(defn cleanup []
  (let [cfg (ch/env-config)]
    (drop-schema! (ch/new-client cfg) cfg)))

(patch/current-version :synthigy.audit/clickhouse "1.0.0")
