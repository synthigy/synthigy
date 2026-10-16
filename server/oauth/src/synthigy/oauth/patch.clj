;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.patch
  "Model patches and migrations for the :synthigy.iam.oauth/model topic."
  (:require
    [buddy.hashers :as hashers]
    [clojure.string :as str]
    [synthigy.log :as log]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.iam.service-user :as service-user]
    [synthigy.iam.access :refer [with-principal]]))

(defn hash-client-secrets!
  "Hash plaintext confidential-client secrets; returns {:total :hashed :skipped}."
  []
  (log/info {:id ::hash-secrets-start :data {:action :migrating :subject :client-secrets}} "Migrating client secrets to hashed format")
  (let [clients (dataset/search-entity
                  :iam/app
                  nil
                  {(id/key) nil
                   :id nil
                   :secret nil
                   :type nil})

        plaintext-clients (filter
                            (fn [{:keys [secret type]}]
                              (and secret
                                   (#{"confidential" :confidential} type)
                                   (not (re-matches #"^\$2[aby]\$.*|^bcrypt\+sha512\$.*" secret))))
                            clients)

        total-count (count clients)
        plaintext-count (count plaintext-clients)]

    (log/info {:id ::hash-secrets-survey
               :data {:action :migrating :subject :client-secrets
                      :total total-count
                      :plaintext plaintext-count}}
              "Surveyed clients for plaintext secrets")

    (doseq [client plaintext-clients
            :let [entity-id (id/extract client)
                  {:keys [id secret]} client]]
      (try
        (let [hashed-secret (hashers/derive secret)]
          (log/info {:id ::hash-client-secret
                     :data {:action :migrating :subject :client-secret :client-id id}}
                    "Hashing secret for client")
          (dataset/sync-entity
            :iam/app
            {(id/key) entity-id
             :secret hashed-secret}))
        (catch Throwable ex
          (log/error! {:id ::hash-client-secret-failed
                       :msg "Failed to hash secret for client"
                       :data {:action :migrating :subject :client-secret :client-id id}}
                      ex))))

    (log/info {:id ::hash-secrets-complete
               :data {:action :migrated :subject :client-secrets :hashed plaintext-count}}
              "Client-secret migration complete")
    {:total total-count
     :hashed plaintext-count
     :skipped (- total-count plaintext-count)}))

(patch/current-version :synthigy.iam.oauth/model "1.0.5")

(patch/upgrade :synthigy.iam.oauth/model
               "1.0.4"
               (log/info {:id ::upgrade-1-0-4-start :data {:action :upgrading :subject :oauth-store :version "1.0.4"}}
                         "Upgrading OAuth model to v1.0.4 (hashing client secrets)")
               (let [{:keys [total hashed skipped]} (hash-client-secrets!)]
                 (log/info {:id ::upgrade-1-0-4-summary
                            :data {:action :upgrading :subject :oauth-store :version "1.0.4"
                                   :total total :hashed hashed :skipped skipped}}
                           "OAuth model migration to v1.0.4 complete"))
               (log/info {:id ::upgrade-1-0-4-done :data {:action :upgraded :subject :oauth-store :version "1.0.4"}}
                         "OAuth model v1.0.4 upgrade complete"))

(patch/downgrade :synthigy.iam.oauth/model
                 "1.0.4"
                 (log/warn {:id ::downgrade-1-0-4-rejected}
                           "Downgrade from OAuth model v1.0.4 not supported; bcrypt hashes cannot be reverted to plaintext — you must regenerate client secrets manually")
                 (throw (ex-info "Downgrade from v1.0.4 not supported - hashed secrets cannot be reverted"
                                 {:version "1.0.4"
                                  :reason "bcrypt is one-way hash"})))

(defn unique-names
  "Rows whose name is blank or shared, renamed to `(fallback row)` / `name (fallback)`."
  [rows fallback]
  (let [blank? #(str/blank? (:name %))
        dups (->> (remove blank? rows)
                  (group-by :name)
                  vals
                  (mapcat rest))]
    (concat (map #(assoc % :name (fallback %)) (filter blank? rows))
            (map #(assoc % :name (str (:name %) " (" (fallback %) ")")) dups))))

(defn normalize-names!
  "Make OAuth Client and OAuth API names unique and non-blank."
  []
  (with-principal nil
    (into {}
          (for [[entity fallback] [[:iam/app :id] [:iam/api :audience]]
                :let [rows (sort-by :_eid (dataset/search-entity entity nil {:_eid nil (id/key) nil :name nil fallback nil}))
                      renamed (unique-names rows fallback)]]
            (do (doseq [row renamed]
                  (dataset/stack-entity entity {(id/key) (id/extract row) :name (:name row)}))
                [entity (mapv :name renamed)])))))

(defn adopt-service-users!
  "Link every client to the SERVICE user named after its client id, where one exists."
  []
  (with-principal nil
    (->> (dataset/search-entity :iam/app nil {(id/key) nil})
         (keep #(service-user/adopt-service-user (id/extract %)))
         count)))

(defn sync-service-users!
  "Name, activate and (for confidential clients) create every client's service user."
  []
  (with-principal nil
    (let [clients (dataset/search-entity :iam/app nil {(id/key) nil :id nil})
          synced (doall
                  (keep (fn [client]
                          (try
                            (service-user/sync-service-user (id/extract client))
                            (catch Throwable e
                              (log/error! {:id ::sync-service-user-failed
                                           :msg "Failed to sync service user"
                                           :data (merge {:action :migrating :subject :service-user
                                                         :client-id (:id client)}
                                                        (ex-data e))}
                                          e)
                              (service-user/get-service-user (id/extract client)))))
                        clients))
          linked-xids (conj (set (map id/extract synced)) (id/data :data/synthigy-user))
          orphans (->> (dataset/search-entity :iam/user {:type {:_eq :SERVICE}} {(id/key) nil :name nil})
                       (remove #(linked-xids (id/extract %)))
                       (map :name))]
      (when (seq orphans)
        (log/warn {:id ::orphan-service-users
                   :data {:action :migrating :subject :service-user :names (vec orphans)}}
                  "SERVICE users with no client left untouched"))
      {:clients (count clients) :linked (count synced) :orphans (vec orphans)})))

(patch/upgrade :synthigy.iam.oauth/model
               "1.0.5"
               (let [adopted (adopt-service-users!)
                     renamed (normalize-names!)
                     summary (sync-service-users!)]
                 (log/info {:id ::upgrade-1-0-5-done
                            :data (merge {:action :upgraded :subject :oauth-store :version "1.0.5"
                                          :adopted adopted :renamed renamed}
                                         summary)}
                           "OAuth model v1.0.5 upgrade complete")))
