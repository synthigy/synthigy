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
    [clojure.tools.logging :as log]
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
  (log/info "[OAuth] Migrating client secrets to hashed format...")
  (let [;; Query all clients with secrets
        clients (dataset/search-entity
                  :iam/app
                  nil
                  {(id/key) nil
                   :id nil
                   :secret nil
                   :type nil})

        ;; Filter to confidential clients with plaintext secrets
        ;; Bcrypt hashes start with "$2a$", "$2b$", or "$2y$"
        plaintext-clients (filter
                            (fn [{:keys [secret type]}]
                              (and secret
                                   (#{"confidential" :confidential} type)
                                   (not (re-matches #"^\$2[aby]\$.*" secret))))
                            clients)

        total-count (count clients)
        plaintext-count (count plaintext-clients)]

    (log/infof "[OAuth] Found %d total clients, %d with plaintext secrets"
               total-count plaintext-count)

    ;; Hash and update each client
    (doseq [client plaintext-clients
            :let [entity-id (id/extract client)
                  {:keys [id secret]} client]]
      (try
        (let [hashed-secret (hashers/derive secret)]
          (log/infof "[OAuth] Hashing secret for client: %s" id)
          (dataset/sync-entity
            :iam/app
            {(id/key) entity-id
             :secret hashed-secret}))
        (catch Throwable ex
          (log/errorf ex "[OAuth] Failed to hash secret for client: %s" id))))

    (log/infof "[OAuth] Migration complete: %d secrets hashed" plaintext-count)
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
               (log/info "[OAuth Model] Upgrading to v1.0.4: Hashing client secrets...")
               (let [{:keys [total hashed skipped]} (hash-client-secrets!)]
                 (log/infof "[OAuth Model] Migration complete: %d/%d secrets hashed (%d already hashed)"
                            hashed total skipped))
               (log/info "[OAuth Model] v1.0.4 upgrade complete"))

;; Downgrade for 1.0.4 (reverting hashed secrets to plaintext is not supported)
(patch/downgrade :synthigy.iam.oauth/model
                 "1.0.4"
                 (log/warn "[OAuth Model] Downgrade from v1.0.4 not supported!")
                 (log/warn "[OAuth Model] Cannot convert hashed secrets back to plaintext")
                 (log/warn "[OAuth Model] You will need to regenerate client secrets manually")
                 (throw (ex-info "Downgrade from v1.0.4 not supported - hashed secrets cannot be reverted"
                                 {:version "1.0.4"
                                  :reason "bcrypt is one-way hash"})))
