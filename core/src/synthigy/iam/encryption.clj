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

(ns synthigy.iam.encryption
  "Pluggable encryption provider for JWT signing and RSA keypair management."
  (:require
   [buddy.core.codecs :as codecs]
   [buddy.core.hash :as hash]
   [buddy.core.keys :as keys]
   [buddy.sign.jwt :as jwt]
   [synthigy.log :as log]
   [patcho.lifecycle :as lifecycle]
   [synthigy.iam.events :as events])
  (:import
   [java.security KeyPairGenerator]))

(defonce ^{:dynamic true
           :doc "Current encryption provider; nil until (start)."}
  *encryption-provider*
  nil)

(defprotocol EncryptionProvider
  "JWT signing and RSA keypair management contract."

  (sign-jwt [this data opts]
    "Sign claims as a JWT with the current keypair.")

  (verify-jwt [this token]
    "Verify a JWT's signature; returns claims or nil.")

  (add-keypair [this keypair]
    "Add an RSA keypair, evicting the oldest beyond three.")

  (list-keypairs [this]
    "List active keypairs, most recent first.")

  (get-keypair-by-kid [this kid]
    "Get a keypair by kid, or nil.")

  (start! [this]
    "Start the provider lifecycle.")

  (stop! [this]
    "Stop the provider and clean up resources."))

(defn base64-url-encode
  [input]
  (let [encoded (codecs/bytes->b64-str input)]
    (.replaceAll (str encoded) "=" "")))

(defn encode-rsa-key
  "Encode an RSA public key to a JWK map."
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
  "Generate a new 2048-bit RSA keypair as {:kid :public :private}."
  []
  (let [generator (KeyPairGenerator/getInstance "RSA")
        key-pair (.generateKeyPair generator)
        public (.getPublic key-pair)
        private (.getPrivate key-pair)]
    {:kid (:kid (encode-rsa-key public))
     :private private
     :public public}))

(defn rotate-keypair
  "Generate a new keypair and add it to the provider."
  ([] (rotate-keypair *encryption-provider*))
  ([provider]
   (let [keypair (generate-key-pair)]
     (add-keypair provider keypair)
     keypair)))

(defn ->RSAEncryptionProvider
  "Default production EncryptionProvider — max 3 keypairs, FIFO eviction, publishes keypair events."
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
        (when-not (keys/public-key? public)
          (throw (ex-info "Unacceptable public key" {:key public})))
        (when-not (keys/private-key? private)
          (throw (ex-info "Unacceptable private key" {:key private})))
        (swap! keypairs
               (fn [current]
                 (let [[active deactivate] (split-at 3 (conj current keypair))]
                   (when (not-empty deactivate)
                     (events/publish :keypair/removed {:key-pairs deactivate}))
                   active)))
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
  "Install `provider` globally, stopping the old one (non-fatal) and starting the new (fatal on error)."
  [provider]
  (when-let [old *encryption-provider*]
    (try
      (stop! old)
      (catch Exception e
        (log/error! {:id ::stop-old-provider-failed
                     :msg "Error stopping old encryption provider"} e))))
  (alter-var-root #'*encryption-provider* (constantly provider))
  (try
    (start! provider)
    (catch Exception e
      (log/error! {:id ::start-new-provider-failed
                   :msg "Error starting new encryption provider"} e)
      (throw e)))
  nil)

(defmacro with-encryption-provider
  "Temporarily bind `provider` for the scope of body."
  [provider & body]
  `(binding [*encryption-provider* ~provider]
     ~@body))

(defn sign-data
  "Sign claims as a JWT with the currently-bound provider."
  ([data] (sign-data data {:alg :rs256}))
  ([data opts]
   (when-not *encryption-provider*
     (throw (IllegalStateException. "No encryption provider configured")))
   (sign-jwt *encryption-provider* data opts)))

(defn unsign-data
  "Verify a JWT's signature only — never validates claims here (callers check expiry); claims map or nil."
  [token]
  (when-not *encryption-provider*
    (throw (IllegalStateException. "No encryption provider configured")))
  (verify-jwt *encryption-provider* token))

(defn init-default-encryption
  "Generate and add a fresh keypair on the current provider."
  []
  (when-not *encryption-provider*
    (throw (IllegalStateException. "No encryption provider configured")))
  (rotate-keypair *encryption-provider*))

(defn start
  "Install the default RSA encryption provider."
  []
  (log/info {:id ::initializing-rsa} "Initializing RSA encryption provider")
  (set-encryption-provider! (->RSAEncryptionProvider))
  (log/info {:id ::initialized} "Encryption provider initialized"))

(defn stop
  "Stop the current provider and clear *encryption-provider*."
  []
  (log/info {:id ::stopping :data {:action :stopping :subject :encryption}} "Stopping encryption provider")
  (when-let [provider *encryption-provider*]
    (try
      (stop! provider)
      (catch Exception e
        (log/error! {:id ::stop-provider-failed
                     :msg "Error stopping encryption provider"} e))))
  (alter-var-root #'*encryption-provider* (constantly nil))
  (log/info {:id ::stopped :data {:action :stopped :subject :encryption}} "Encryption provider stopped"))

(lifecycle/register-module!
 :synthigy.iam/encryption
 {:depends-on [:synthigy/dataset]
  :doc "RSA keypair manager — signs/rotates JWT keys"
  :setup (fn []
           (log/info {:id ::setup-generating-keypair}
                     "Generating initial RSA keypair")
           (start)
           (rotate-keypair)
           (log/info {:id ::setup-keypair-generated}
                     "Initial RSA keypair generated"))
  :cleanup (fn []
             (log/info {:id ::cleanup-clearing} "Clearing encryption state")
             (stop)
             (log/info {:id ::cleanup-cleared} "Encryption state cleared"))
  :start (fn []
           ;; provider only — never rotate here; persisted keypairs load later in the lifecycle DAG
           (log/info {:id ::lifecycle-start :data {:action :starting}} "Starting encryption provider")
           (start)
           (log/info {:id ::lifecycle-started :data {:action :started}} "Encryption provider started"))
  :stop (fn []
          (log/info {:id ::lifecycle-stop :data {:action :stopping}} "Stopping encryption provider")
          (stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "Encryption provider stopped"))})
