;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.iam.connector.dataset
  "Dataset-backed CredentialsProvider — connectors are normal
   `:iam/auth-connector` entities, cache-invalidated via `delta/subscribe!`;
   replaces the old Postgres/SQLite/Cockroach per-backend providers. Patch 2.0.0
   migrates rows out of the legacy `__iam_auth_connector` side-table."
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.db :refer [*db*]]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.iam.access :refer [with-principal]]
   [synthigy.iam.connector :as connector]
   [synthigy.json :as json]
   [synthigy.log :as log]))

;; =============================================================================
;; Entity <-> connector-map translation
;; =============================================================================

(def ^:private chain-selections
  {:xid nil :name nil :type nil :priority nil :domain nil :enabled nil :config nil})

(defn parse-config
  "Json-attribute value -> keyword-keyed map, re-encoding string-keyed maps
   through the same reader as legacy <-jsonb."
  [v]
  (cond
    (nil? v) {}
    (map? v) (if (some string? (keys v))
                (try (json/read-str (json/->json v)) (catch Throwable _ v))
                v)
    :else (try (json/read-str (str v)) (catch Throwable _ {}))))

(defn row->connector
  "Entity record -> flat connector map for the multimethod dispatch, merging
   `:config` under the top-level columns."
  [row]
  (when row
    (merge
     (parse-config (:config row))
     (-> (select-keys row [:xid :name :type :priority :domain :enabled])
         (update :type #(cond (keyword? %) % (string? %) (keyword %) :else %))))))

(defn connector->row
  "Inbound connector spec -> entity data; known columns stay top-level,
   everything else folds into `:config`."
  [c]
  (let [known #{:euuid :xid :name :type :priority :domain :enabled :config}
        extra (apply dissoc c known)]
    (cond-> {:name     (or (:name c) (some-> (:type c) name) "unnamed")
             :type     (some-> (:type c) name)
             :priority (or (:priority c) 1000)
             :domain   (:domain c)
             :enabled  (if (some? (:enabled c)) (boolean (:enabled c)) true)
             :config   (merge (:config c) extra)}
      (:xid c) (assoc :xid (:xid c)))))

;; =============================================================================
;; Provider
;; =============================================================================

(defrecord DatasetCredentialsProvider [cache]
  connector/CredentialsProvider

  (-list-chain [_]
    (or @cache
        ;; pre-authentication login-path read — runs as system (nil principal)
        (let [chain (with-principal nil
                      (->> (dataset/search-entity :iam/auth-connector
                                                  {:enabled true}
                                                  chain-selections)
                           (mapv row->connector)
                           (sort-by (fn [c] [(:priority c 1000) (:xid c "")]))
                           vec))]
          (reset! cache chain)
          chain)))

  (-find-connector [_ xid]
    (with-principal nil
      (row->connector
       (dataset/get-entity :iam/auth-connector {:xid (str xid)} chain-selections))))

  (-save-connector! [this c]
    (let [row (with-principal nil
                (dataset/sync-entity :iam/auth-connector (connector->row c)))]
      (connector/-refresh! this)
      (row->connector (select-keys row [:xid :name :type :priority :domain :enabled :config]))))

  (-delete-connector! [this xid]
    (with-principal nil
      (dataset/delete-entity :iam/auth-connector {:xid (str xid)}))
    (connector/-refresh! this))

  (-refresh! [_] (reset! cache nil))

  (-start! [this]
    ;; delta subscription catches API writes AND out-of-band SQL
    (delta/subscribe!
     ::cache-invalidator
     {:entity-xids #{(id/entity :iam/auth-connector)}}
     (fn [_env] (connector/-refresh! this)))
    nil)

  (-stop! [_]
    (delta/unsubscribe! ::cache-invalidator)
    nil))

;; =============================================================================
;; Migration — legacy __iam_auth_connector side-table (v1.x) -> entity
;; =============================================================================

(defn migrate-legacy-table!
  "Move rows out of the legacy side-table into the entity, then drop the table +
   its Postgres NOTIFY plumbing; no-op on fresh installs."
  []
  (when-let [ds (:datasource *db*)]
    (let [rows (try
                 (jdbc/execute! ds
                   ["SELECT xid, name, type, priority, domain, enabled, config FROM __iam_auth_connector"]
                   {:builder-fn rs/as-unqualified-maps})
                 (catch Throwable _ nil))]
      (when rows
        (log/info {:id ::migrating-legacy-rows
                   :data {:action :migrating :subject :iam-connector :count (count rows)}}
                  "Migrating __iam_auth_connector rows to Auth Connector entity")
        (doseq [{:keys [xid name type priority domain enabled config]} rows]
          (dataset/sync-entity
           :iam/auth-connector
           {:xid      xid
            :name     name
            :type     type
            :priority priority
            :domain   domain
            :enabled  (if (number? enabled) (pos? enabled) (boolean enabled))
            :config   (parse-config config)}))
        (doseq [sql ["DROP TRIGGER IF EXISTS __iam_auth_connector_notify_trg ON __iam_auth_connector"
                     "DROP TABLE IF EXISTS __iam_auth_connector"
                     "DROP FUNCTION IF EXISTS __iam_auth_connector_notify_fn()"]]
          (try (jdbc/execute! ds [sql])
               (catch Throwable _)))       ; pg-only DDL
        (log/info {:id ::legacy-table-dropped
                   :data {:action :migrated :subject :iam-connector}}
                  "__iam_auth_connector retired")))))

(defn seed-default!
  "Fresh install (or post-migration empty set): seed the local-database
   connector so password login works out of the box."
  []
  (when (empty? (dataset/search-entity :iam/auth-connector {} {:xid nil}))
    (dataset/sync-entity :iam/auth-connector
                         {:name "Local database" :type "database"
                          :priority 1000 :enabled true :config {}})))

(patch/current-version :synthigy.iam/connector "2.0.0")

(patch/upgrade :synthigy.iam/connector
               "2.0.0"
               (with-principal nil
                 (migrate-legacy-table!)
                 (seed-default!)))

;; =============================================================================
;; Lifecycle module
;; =============================================================================

(defn entity-deployed?
  "True when the Auth Connector entity (IAM model >= 0.81.0) is part of the
   deployed model."
  []
  (contains? (:entities (dataset/deployed-model))
             (id/entity :iam/auth-connector)))

(lifecycle/register-module!
 :synthigy.iam/connector
 {:depends-on [:synthigy/iam]
  :doc "IAM credentials provider — dataset-backed connector chain, delta-invalidated cache"
  :start (fn []
           (if (entity-deployed?)
             (do
               (patch/level! :synthigy.iam/connector)
               (connector/set-credentials-provider! (->DatasetCredentialsProvider (atom nil)))
               (log/info {:id ::provider-ready
                          :data {:action :started :subject :iam-connector}}
                         "Dataset credentials provider ready"))
             (log/warn {:id ::entity-not-deployed
                        :data {:action :not-initialized :subject :iam-connector}}
                       "Auth Connector entity not in deployed model (needs IAM model >= 0.81.0) — using built-in database chain")))
  :stop (fn []
          (when (instance? DatasetCredentialsProvider connector/*credentials-provider*)
            (connector/-stop! connector/*credentials-provider*))
          (log/info {:id ::provider-stopped
                     :data {:action :stopped :subject :iam-connector}}
                    "Dataset credentials provider stopped"))})
