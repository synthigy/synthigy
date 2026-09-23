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

(ns synthigy.oauth.patch
  "OAuth-specific model patches and migrations for the :synthigy.iam.oauth/model
   topic. See docs/core/synthigy/oauth/patch.md."
  (:require
    [buddy.hashers :as hashers]
    [synthigy.log :as log]
    [patcho.lifecycle :as lifecycle]
    [patcho.patch :as patch]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.id :as id]
    [synthigy.db :refer [*db*]]))

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn hash-client-secrets!
  "Migrate all OAuth clients with plaintext secrets to hashed (bcrypt) secrets;
   returns {:total :hashed :skipped}."
  []
  (log/info {:id ::hash-secrets-start :data {:action :migrating :subject :client-secrets}} "Migrating client secrets to hashed format")
  (let [clients (dataset/search-entity
                  :iam/app
                  nil
                  {(id/key) nil
                   :id nil
                   :secret nil
                   :type nil})

        ;; Bcrypt hashes start with "$2a$", "$2b$", "$2y$" or synthigy's
        ;; "bcrypt+sha512$"
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

    ;; Hash and update each client
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

(patch/current-version :synthigy.iam.oauth/model "1.0.4")

;; Patch 1.0.4 - Hash client secrets
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

;; Downgrade for 1.0.4 (reverting hashed secrets to plaintext is not supported)
(patch/downgrade :synthigy.iam.oauth/model
                 "1.0.4"
                 (log/warn {:id ::downgrade-1-0-4-rejected}
                           "Downgrade from OAuth model v1.0.4 not supported; bcrypt hashes cannot be reverted to plaintext — you must regenerate client secrets manually")
                 (throw (ex-info "Downgrade from v1.0.4 not supported - hashed secrets cannot be reverted"
                                 {:version "1.0.4"
                                  :reason "bcrypt is one-way hash"})))
