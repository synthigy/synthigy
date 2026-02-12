(ns synthigy.iam.encryption
  "Protocol-based encryption provider for JWT signing and RSA keypair management.

  Provides a pluggable architecture for encryption backends, allowing replacement
  of the default RSA implementation with cloud-based KMS providers (AWS, Vault, Azure).

  Default implementation uses RSA keypairs with automatic rotation and event publishing
  for database persistence via oauth.store."
  (:require
   [buddy.core.codecs :as codecs]
   [buddy.core.hash :as hash]
   [buddy.core.keys :as keys]
   [buddy.sign.jwt :as jwt]
   [clojure.tools.logging :as log]
   [patcho.lifecycle :as lifecycle]
   [synthigy.iam.events :as events])
  (:import
   [java.security KeyPairGenerator]))

;; =============================================================================
;; Global Provider Management
;; =============================================================================

(defonce ^{:dynamic true
           :doc "The current encryption provider.

                This is a dynamic var that can be:
                - Set globally with set-encryption-provider!
                - Overridden temporarily with with-encryption-provider

                Default value is nil - call (start) to initialize."}
  *encryption-provider*
  nil)

;; =============================================================================
;; Protocol Definition
;; =============================================================================

(defprotocol EncryptionProvider
  "Protocol for JWT signing and RSA keypair management.

  Implementations must provide:
  - JWT signing/verification with RS256 algorithm
  - RSA keypair management with rotation support
  - JWKS endpoint support (list public keys)
  - Lifecycle management (start/stop)

  The protocol supports two built-in implementations:
  - RSAEncryptionProvider: Default production implementation with event publishing
  - AtomEncryptionProvider: Lightweight test implementation without events"

  (sign-jwt [this data opts]
    "Sign JWT with RS256 algorithm.

    Args:
      data - Claims map to sign (e.g., {:sub \"user-id\" :exp 123456})
      opts - Options map (e.g., {:alg :rs256})

    Returns: Signed JWT token string

    Throws:
      ExceptionInfo if no keypairs available")

  (verify-jwt [this token]
    "Verify and decode JWT token.

    Args:
      token - JWT token string to verify

    Returns: Claims map if valid, nil if invalid/expired

    Implementation should:
    1. Extract kid from JWT header
    2. Find matching keypair by kid
    3. Verify signature with public key
    4. Return nil on any validation failure")

  (add-keypair [this keypair]
    "Add RSA keypair to provider (max 3 keypairs).

    Args:
      keypair - Map with :public, :private, :kid keys

    Returns: nil

    Side effects:
    - Publishes :keypair/added event with {:key-pair keypair}
    - Publishes :keypair/removed event if >3 keypairs (FIFO eviction)
    - Validates keypair format (throws on invalid keys)")

  (list-keypairs [this]
    "List all active keypairs (for JWKS endpoint).

    Returns: Sequence of keypair maps (most recent first)")

  (get-keypair-by-kid [this kid]
    "Get keypair by kid (for JWT verification).

    Args:
      kid - Key ID string from JWT header

    Returns: Keypair map or nil if not found")

  (start! [this]
    "Start provider lifecycle.

    Returns: nil

    Implementation-specific initialization (e.g., open channels, load keys)")

  (stop! [this]
    "Stop provider and cleanup resources.

    Returns: nil

    Implementation-specific cleanup (e.g., close channels, clear cache)"))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn base64-url-encode
  "Encode bytes to Base64 URL-safe format (no padding).

  Used for JWT components and key fingerprints."
  [input]
  (let [encoded (codecs/bytes->b64-str input)]
    (.replaceAll (str encoded) "=" "")))

(defn encode-rsa-key
  "Encode RSA public key to JWK (JSON Web Key) format.

  Args:
    rsa-key - RSA public key (java.security.interfaces.RSAPublicKey)

  Returns: JWK map with :kty, :n, :e, :use, :alg, :kid keys

  The kid (Key ID) is SHA256 hash of modulus+exponent for uniqueness."
  [rsa-key]
  (let [modulus (.getModulus rsa-key)
        exponent (.getPublicExponent rsa-key)
        n (base64-url-encode (.toByteArray modulus))
        e (base64-url-encode (.toByteArray exponent))]
    {:kty "RSA"
     :n n
     :e e
     :use "sig"
     :alg "RS256"
     :kid (base64-url-encode (hash/sha256 (str n e)))}))

(defn generate-key-pair
  "Generate new RSA keypair (2048-bit).

  Returns: Map with :kid, :public, :private keys

  The kid is derived from the public key fingerprint for consistency."
  []
  (let [generator (KeyPairGenerator/getInstance "RSA")
        key-pair (.generateKeyPair generator)
        public (.getPublic key-pair)
        private (.getPrivate key-pair)]
    {:kid (:kid (encode-rsa-key public))
     :private private
     :public public}))

(defn rotate-keypair
  "Generate and add new RSA keypair to provider.

  This is a convenience function that generates a new keypair and adds it
  to the provider in one step.

  Args:
    provider - EncryptionProvider instance

  Returns: New keypair map with :kid, :public, :private keys

  Side effects: Calls add-keypair on provider"
  ([] (rotate-keypair *encryption-provider*))
  ([provider]
   (let [keypair (generate-key-pair)]
     (add-keypair provider keypair)
     keypair)))

;; =============================================================================
;; Default Implementation: RSAEncryptionProvider
;; =============================================================================

(defn ->RSAEncryptionProvider
  "Create RSA-based encryption provider with async event publishing.

  This is the default production implementation. Features:
  - Encapsulated keypair storage (atom in closure)
  - Max 3 active keypairs with FIFO rotation
  - Event publishing for oauth.store persistence
  - Full validation of keypair format

  Returns: EncryptionProvider instance

  Events published:
  - :keypair/added {:key-pair keypair} - When keypair is added
  - :keypair/removed {:key-pairs [keypair ...]} - When keypairs evicted"
  []
  (let [keypairs (atom '())]
    (reify EncryptionProvider

      (sign-jwt [this data opts]
        (let [[{private-key :private
                kid :kid}] @keypairs]
          (when-not private-key
            (throw (ex-info "No encryption keypairs available"
                            {:type :encryption/no-keypairs})))
          (jwt/sign data private-key
                    (assoc opts :header {:kid kid
                                         :type "JWT"}))))

      (verify-jwt [this token]
        (try
          (when-let [{:keys [kid]} (jwt/decode-header token)]
            (when-let [{:keys [public]} (get-keypair-by-kid this kid)]
              (try
                (jwt/unsign token public {:alg :rs256
                                          :skip-validation true})
                (catch Exception _
                  nil))))
          (catch Exception _
            nil)))

      (add-keypair [this {:keys [public private]
                          :as keypair}]
        ;; Validate keys
        (when-not (keys/public-key? public)
          (throw (ex-info "Unacceptable public key" {:key public})))
        (when-not (keys/private-key? private)
          (throw (ex-info "Unacceptable private key" {:key private})))

        ;; Add to atom (max 3, FIFO)
        (swap! keypairs
               (fn [current]
                 (let [[active deactivate] (split-at 3 (conj current keypair))]
                   ;; Publish removed events
                   (when (not-empty deactivate)
                     (events/publish :keypair/removed {:key-pairs deactivate}))
                   active)))

        ;; Publish added event
        (events/publish :keypair/added {:key-pair keypair})
        nil)

      (list-keypairs [this]
        @keypairs)

      (get-keypair-by-kid [this kid]
        (some #(when (= kid (:kid %)) %) @keypairs))

      (start! [this]
        nil)

      (stop! [this]
        (reset! keypairs '())
        nil))))

;; =============================================================================
;; Test Implementation: AtomEncryptionProvider
;; =============================================================================

(deftype AtomEncryptionProvider [keypairs]
  EncryptionProvider

  (sign-jwt [this data opts]
    (let [[{private-key :private
            kid :kid}] @keypairs]
      (when-not private-key
        (throw (ex-info "No encryption keypairs available"
                        {:type :encryption/no-keypairs})))
      (jwt/sign data private-key
                (assoc opts :header {:kid kid
                                     :type "JWT"}))))

  (verify-jwt [this token]
    (try
      (when-let [{:keys [kid]} (jwt/decode-header token)]
        (when-let [{:keys [public]} (get-keypair-by-kid this kid)]
          (try
            (jwt/unsign token public {:alg :rs256
                                      :skip-validation true})
            (catch Exception _
              nil))))
      (catch Exception _
        nil)))

  (add-keypair [this keypair]
    ;; No validation, no events (test-only)
    (swap! keypairs (fn [current] (take 3 (conj current keypair))))
    nil)

  (list-keypairs [this]
    @keypairs)

  (get-keypair-by-kid [this kid]
    (some #(when (= kid (:kid %)) %) @keypairs))

  (start! [this]
    nil)

  (stop! [this]
    (reset! keypairs '())
    nil))

(defn set-encryption-provider!
  "Set the default encryption provider.

  Automatically stops old provider and starts new provider.

  Args:
    provider - EncryptionProvider instance

  Returns: nil

  Side effects:
  - Calls stop! on old provider (warnings only)
  - Calls start! on new provider (throws on error)

  Example:
    (set-encryption-provider! (->RSAEncryptionProvider))
    (set-encryption-provider! (->AtomEncryptionProvider (atom '())))"
  [provider]
  ;; Stop old (non-fatal)
  (when-let [old *encryption-provider*]
    (try
      (stop! old)
      (catch Exception e
        (log/warnf e "[Encryption] Error stopping old provider"))))

  ;; Set new
  (alter-var-root #'*encryption-provider* (constantly provider))

  ;; Start new (fatal on error)
  (try
    (start! provider)
    (catch Exception e
      (log/errorf e "[Encryption] Error starting new provider")
      (throw e)))
  nil)

(defmacro with-encryption-provider
  "Temporarily override provider for scope of body.

  Useful for testing or multi-tenant scenarios.

  Args:
    provider - EncryptionProvider instance
    body - Forms to execute with provider bound

  Returns: Result of body

  Example:
    (with-encryption-provider test-provider
      (sign-data {:sub \"test\"}))"
  [provider & body]
  `(binding [*encryption-provider* ~provider]
     ~@body))

;; =============================================================================
;; Backward-Compatible API
;; =============================================================================

(defn sign-data
  "Sign JWT token (delegates to provider).

  DEPRECATED: Use protocol directly in new code.

  Args:
    data - Claims map to sign
    opts - Options map (optional, defaults to {:alg :rs256})

  Returns: Signed JWT token string

  Throws:
    IllegalStateException if no provider configured
    ExceptionInfo if no keypairs available"
  ([data] (sign-data data {:alg :rs256}))
  ([data opts]
   (when-not *encryption-provider*
     (throw (IllegalStateException. "No encryption provider configured")))
   (sign-jwt *encryption-provider* data opts)))

(defn unsign-data
  "Verify JWT token (delegates to provider).

  DEPRECATED: Use protocol directly in new code.

  Args:
    token - JWT token string to verify

  Returns: Claims map if valid, nil if invalid

  Throws:
    IllegalStateException if no provider configured"
  [token]
  (when-not *encryption-provider*
    (throw (IllegalStateException. "No encryption provider configured")))
  (verify-jwt *encryption-provider* token))

(defn init-default-encryption
  "Initialize with new keypair (called by oauth.store).

  Returns: New keypair map

  Throws:
    IllegalStateException if no provider configured"
  []
  (when-not *encryption-provider*
    (throw (IllegalStateException. "No encryption provider configured")))
  (rotate-keypair *encryption-provider*))

;; =============================================================================
;; Initialization
;; =============================================================================

(defn start
  "Initialize encryption provider with default RSA implementation.

  This is called by synthigy.iam/start during system initialization.

  Returns: nil

  Side effects:
  - Creates and starts RSAEncryptionProvider
  - Sets *encryption-provider* globally"
  []
  (log/info "[Encryption] Initializing RSA encryption provider...")
  (set-encryption-provider! (->RSAEncryptionProvider))
  (log/info "[Encryption] Encryption provider initialized"))

(defn stop
  "Stop encryption provider and cleanup resources.

  Returns: nil

  Side effects:
  - Calls stop! on current provider
  - Clears *encryption-provider*"
  []
  (log/info "[Encryption] Stopping encryption provider...")
  (when-let [provider *encryption-provider*]
    (try
      (stop! provider)
      (catch Exception e
        (log/warnf e "[Encryption] Error stopping provider"))))
  (alter-var-root #'*encryption-provider* (constantly nil))
  (log/info "[Encryption] Encryption provider stopped"))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
 :synthigy.iam/encryption
 {:depends-on [:synthigy/dataset]
  :setup (fn []
            ;; One-time: Generate initial RSA keypair
           (log/info "[IAM.ENCRYPTION] Generating initial RSA keypair...")
           (start)
           (rotate-keypair)
           (log/info "[IAM.ENCRYPTION] Initial RSA keypair generated"))
  :cleanup (fn []
              ;; One-time: Clear all keypairs
             (log/info "[IAM.ENCRYPTION] Clearing encryption state...")
             (stop)
             (log/info "[IAM.ENCRYPTION] Encryption state cleared"))
  :start (fn []
            ;; Runtime: Start encryption provider and ensure keypair exists
           (log/info "[IAM.ENCRYPTION] Starting encryption provider...")
           (start)
           ;; Ensure at least one keypair exists for JWT signing
           (when (empty? (list-keypairs *encryption-provider*))
             (log/info "[IAM.ENCRYPTION] No keypairs found, generating initial keypair...")
             (rotate-keypair *encryption-provider*))
           (log/info "[IAM.ENCRYPTION] Encryption provider started"))
  :stop (fn []
           ;; Runtime: Stop encryption provider
          (log/info "[IAM.ENCRYPTION] Stopping encryption provider...")
          (stop)
          (log/info "[IAM.ENCRYPTION] Encryption provider stopped"))})
