(ns synthigy.oauth.patch
  "OAuth-specific model patches and migrations.

  This namespace defines patches for the :synthigy.iam.oauth/model topic.
  These patches handle OAuth schema evolution and data migrations.

  Examples of patches:
  - Client secret hashing migration (plaintext → bcrypt)
  - Token revocation schema updates
  - Session management improvements

  To level OAuth:
    (require '[patcho.patch :as patch])
    (patch/level! :synthigy.iam.oauth/model)"
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
  "Migrates all OAuth clients to use hashed secrets.

  This function:
  1. Searches for all OAuth clients (app entities)
  2. Identifies clients with plaintext secrets (not starting with bcrypt prefix)
  3. Hashes those secrets using bcrypt
  4. Updates the client records with hashed secrets

  Returns:
    Map with :total, :hashed, :skipped counts"
  []
  (log/info {:id ::hash-secrets-start :data {:action :migrating :subject :client-secrets}} "Migrating client secrets to hashed format")
  (let [;; Query all clients with secrets
        clients (dataset/search-entity
                  :iam/app
                  nil
                  {(id/key) nil
                   :id nil
                   :secret nil
                   :type nil})

        ;; Filter to confidential clients with plaintext secrets
        ;; Bcrypt hashes start with "$2a$", "$2b$", "$2y$" or "bcrypt+sha512$" (synthigy format)
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

;;; ============================================================================
;;; OAuth Model Versioning (:synthigy.iam.oauth/model)
;;; ============================================================================
;;
;; This topic tracks the version of the OAuth model and related features.
;;
;; OAuth model includes:
;; - Client credentials (app entities)
;; - Session management
;; - Token storage (access, refresh, id tokens)
;; - Authorization codes
;;
;; Patches handle data migrations and schema evolution.
;;

;; Current model version (hardcoded - represents target version)
(patch/current-version :synthigy.iam.oauth/model "1.0.4")


;;; ============================================================================
;;; OAuth Model Patches
;;; ============================================================================

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
