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

(ns synthigy.dataset.sql.lock
  "Row lock runtime: claim, release and sweep SQL shared by the SQL engines."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.access :as access]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.protocol :as proto]
   [synthigy.dataset.sql.query :as query]
   [synthigy.db :refer [*db*]]
   [synthigy.db.sql :as sql]
   [synthigy.log :as log]))

(defn col [k] (str "\"" (name k) "\""))

(defn lockable-schema!
  "Deployed schema of `entity-id`, throwing LOCK_NOT_LOCKABLE when it has no lock attribute."
  [entity-id]
  (let [schema (query/deployed-schema-entity entity-id)]
    (when-not (:lock schema)
      (throw (ex-info (str (:name schema) " has no Lock attribute")
                      {:code "LOCK_NOT_LOCKABLE"
                       :rule "record_lock"
                       :entity-name (:name schema)})))
    schema))

(defn assert-holder! []
  (or (access/principal-eid)
      (throw (ex-info "Claim needs a signed-in user to hold the lock"
                      {:code "LOCK_NO_PRINCIPAL"
                       :rule "record_lock"}))))

(defn assert-claim-args! [args]
  (when-not (nat-int? (:_limit args))
    (throw (ex-info "Claim needs _limit"
                    {:code "LOCK_LIMIT_REQUIRED"
                     :rule "record_lock"})))
  (when (> (:_limit args) (query/chunk-rows-for 1))
    (throw (ex-info (str "Claim _limit is at most " (query/chunk-rows-for 1))
                    {:code "LOCK_LIMIT_TOO_LARGE"
                     :rule "record_lock"
                     :max (query/chunk-rows-for 1)})))
  (when (some? (:_distinct args))
    (throw (ex-info "Claim does not take _distinct"
                    {:code "LOCK_DISTINCT"
                     :rule "record_lock"}))))

(defn free-sql
  "[sql params]: `alias`'s row is free, expired or held by `me`."
  [alias {:keys [holder until]} me]
  (let [c #(str alias "." (col %))]
    [(str "(" (c holder) " is null or " (c holder) " = ?"
          (when until (str " or " (c until) " is null or " (c until) " < " (proto/now-expr *db*)))
          ")")
     [me]]))

(defn claim-set-sql
  "[sql params] of the SET that takes the lock for `me`."
  [{:keys [holder until ttl]} me]
  [(str (col holder) " = ?"
        (when until (str ", " (col until) " = " (proto/now-plus-seconds-expr *db*))))
   (cond-> [me] until (conj ttl))])

(defn guard-sql
  "[sql params] keeping `alias`'s rows inside the caller's write scope, or nil."
  [entity-id table alias]
  (when-let [{:keys [sql params]} (query/write-scope-guard entity-id table)]
    [(str alias "._eid in (select _eid from \"" table "\" where " sql ")") (vec params)]))

(defn claim!
  "Locks `table`'s rows `eids` to `me`; returns the `eids` it took, in their given order."
  [tx {:keys [table lock]} me eids]
  (if-let [eids (seq (distinct eids))]
    (let [[set-sql set-params] (claim-set-sql lock me)
          taken (into #{}
                      (map :_eid)
                      (sql/execute! tx (into [(str "update \"" table "\" set " set-sql " where _eid in ("
                                                   (str/join "," (repeat (count eids) "?")) ") returning _eid")]
                                             (concat set-params eids))
                                    :raw))]
      (filterv taken eids))
    []))

(defn xid-lock-state
  "{xid row} of the rows `xids` names, locked until commit."
  [tx {:keys [table lock]} xids]
  (let [id-col (name (id/key))
        eids (map :_eid (sql/execute! tx (into [(str "select _eid from \"" table "\" where \"" id-col "\" in ("
                                                     (str/join "," (repeat (count xids) "?")) ")")]
                                               xids)
                                      :raw))]
    (into {} (map (juxt :id identity)) (vals (query/lock-state tx table lock eids)))))

(defn release-xids
  "Validated xids a release names: a vector of xids, or one unique-key map like delete's."
  [tx entity-schema data]
  (if (map? data)
    (let [{:keys [filtered-args]} (query/delete-unique-args entity-schema data)
          q (str "select \"" (name (id/key)) "\" as id from \"" (:table entity-schema) "\" where "
                 (str/join " and " (map #(str (col %) " = ?") (keys filtered-args))))
          found (map (comp str :id) (sql/execute! tx (into [q] (vals filtered-args)) :raw))]
      (query/assert-single-match! entity-schema filtered-args found)
      (vec found))
    (query/check-delete-batch! data)))

(defn release!
  "Clears the locks of the rows `data` names in `tx`; another holder's lock needs scope data:unlock."
  [tx entity-id data]
  (query/entity-accessible? entity-id #{:update :owns})
  (let [{:keys [table lock] :as schema} (lockable-schema! entity-id)
        single? (map? data)
        xids (release-xids tx schema data)
        me (access/principal-eid)
        rows (when (seq xids) (xid-lock-state tx schema xids))
        writable (if-let [[g params] (and (seq rows) (guard-sql entity-id table (str "\"" table "\"")))]
                   (set (map :_eid (sql/execute!
                                    tx (into [(str "select _eid from \"" table "\" where _eid in ("
                                                   (str/join "," (repeat (count rows) "?")) ") and " g)]
                                             (concat (map :_eid (vals rows)) params))
                                    :raw)))
                   (set (map :_eid (vals rows))))
        hidden (remove (comp writable :_eid) (vals rows))
        held (remove query/lock-free? (filter (comp writable :_eid) (vals rows)))
        others (remove #(and me (= me (:holder %))) held)]
    (when (seq hidden)
      (throw (ex-info (format "You don't have permission to release %d %s record(s)" (count hidden) (:name schema))
                      {:code "ROW_FORBIDDEN"
                       :entity-name (:name schema)
                       :xids (mapv :id hidden)})))
    (when (and (seq others) (not (access/superuser?)) (not (access/scope-allowed? "data:unlock")))
      (query/throw-locked tx (:name schema) others))
    (when (and (seq others) (some? (access/current-principal)))
      (log/info {:id ::lock-broken
                 :data {:action :unlocked :subject :record-lock
                        :entity (:name schema)
                        :xids (mapv :id others)
                        :holders (mapv str (distinct (vals (query/holder-ids tx (map :holder others)))))}}
                "Released records locked by another user"))
    (when-let [eids (seq (map :_eid held))]
      (sql/execute! tx (into [(str "update \"" table "\" set " (col (:holder lock)) " = null"
                                   (when (:until lock) (str ", " (col (:until lock)) " = null"))
                                   " where _eid in (" (str/join "," (repeat (count eids) "?")) ")")]
                             eids)
                    :raw))
    (if single? true (count held))))

(defn sweep-sql
  "UPDATE clearing every expired lock of `entity-schema`, or nil when its locks never expire."
  [{:keys [table] {:keys [holder until]} :lock}]
  (when until
    (str "update \"" table "\" set " (col holder) " = null, " (col until) " = null"
         " where " (col holder) " is not null and (" (col until) " is null or " (col until) " < " (proto/now-expr *db*) ")")))

(defn next-expiry-sql [{:keys [table] {:keys [until]} :lock}]
  (when until
    (str "select " (proto/seconds-until-expr *db* (str "min(" (col until) ")")) " as s"
         " from \"" table "\" where " (col until) " >= " (proto/now-expr *db*))))

(defn lockable-schemas []
  (filter (comp :until :lock) (vals (query/deployed-schema))))

(defn claim-entity
  "Locks up to `_limit` free rows matching `args` to the caller in one `lock-tx`, then pulls them per `selection`."
  [{:keys [roots-sql lock-tx]} entity-id args selection]
  (query/entity-accessible? entity-id #{:update :owns})
  (let [{:keys [table lock] :as entity-schema} (lockable-schema! entity-id)
        me (assert-holder!)]
    (assert-claim-args! args)
    (lock-tx
     (fn [tx]
       (binding [access/*operation-rules* #{:search :read}]
         (let [schema (query/selection->schema entity-id selection args)
               alias (:entity/as (query/focus-order schema))
               [free free-params] (free-sql alias lock me)
               [guard guard-params] (guard-sql entity-id table alias)
               [roots roots-params] (roots-sql schema {:where (str/join " and " (remove nil? [free guard]))
                                                       :where-params (into free-params guard-params)
                                                       :suffix (proto/claim-lock-clause *db* alias)})
               ordered (keep #(get % (keyword alias)) (sql/execute! tx (into [roots] roots-params) :raw))
               eids (claim! tx entity-schema me ordered)]
           (if (seq eids)
             (query/pull-roots tx schema {(keyword alias) eids})
             [])))))))

(defn sweep-locks
  "Clears every expired lock in one `lock-tx`; returns the seconds to the next expiry, or nil."
  [lock-tx]
  (when-let [schemas (seq (lockable-schemas))]
    (lock-tx
     (fn [tx]
       (doseq [s schemas]
         (sql/execute! tx [(sweep-sql s)] :raw))
       (some->> schemas
                (keep #(:s (first (sql/execute! tx [(next-expiry-sql %)] :raw))))
                seq
                (map double)
                (apply min))))))
