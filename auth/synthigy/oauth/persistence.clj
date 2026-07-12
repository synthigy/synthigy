(ns synthigy.oauth.persistence
  (:require
    [buddy.sign.jwt :as jwt]
    [clojure.core.async :as async]
    [clojure.string :as str]
    [synthigy.log :as log]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.iam.encryption :as encryption]
    [synthigy.oauth.core :as core]
    [synthigy.oauth.token :as token]
    [synthigy.util :as util])
  (:import
    [java.security KeyFactory]
    [java.security.spec
     X509EncodedKeySpec
     PKCS8EncodedKeySpec]
    [java.util Base64]))

;;; ============================================================================
;;; Entity & Data ID Registration
;;; ============================================================================
;; XIDs are deterministic Base58 conversions of UUIDs via id/uuid->nanoid

(id/defentity :oauth/client
  :euuid #uuid "0757bd93-7abf-45b4-8437-2841283edcba"
  :xid "1ubBUFuDpcpqxY94TECvMw")

(id/defentity :oauth/session
  :euuid #uuid "b2562198-0817-4508-a941-d898373298e5"
  :xid "P2G9jsr3FReSGAs1AfeErp")

(id/defentity :oauth/access-token
  :euuid #uuid "405d7201-a74a-490d-a5f7-669701d1a735"
  :xid "8wzJHRRoGMPWpuEcmGPZW4")

(id/defentity :oauth/refresh-token
  :euuid #uuid "6a6511b9-0616-4eee-99e0-729c6058985c"
  :xid "E91WnbnFDTxdHZLeY4iSEo")

(id/defentity :oauth/key-pair
  :euuid #uuid "fd76f554-1158-4101-9469-98cd70dcbe68"
  :xid "YJLVrcBtVdFHQxqrbAVoiw")

(id/defdata :oauth/dataset-version
  :euuid #uuid "0f9bb720-4b94-445c-9780-a4af09e8536c"
  :xid "2vngxuTiH9YCxBgaD9vXYf")

(defn ->b64 [^bytes value] (.encodeToString (Base64/getEncoder) value))
(defn <-b64 [^String value] (.decode (Base64/getDecoder) value))

(defn encode-rsa [rsa-key] (->b64 (.getEncoded rsa-key)))

(defn decode-public-key [^bytes rsa-key]
  (let [_bytes (<-b64 rsa-key)
        kf (KeyFactory/getInstance "RSA")
        spec (X509EncodedKeySpec. _bytes)]
    (.generatePublic kf spec)))

(defn decode-private-key [^bytes rsa-key]
  (let [_bytes (<-b64 rsa-key)
        kf (KeyFactory/getInstance "RSA")
        spec (PKCS8EncodedKeySpec. _bytes)]
    (.generatePrivate kf spec)))

(defn get-key-pairs
  []
  (map
    (fn [kp]
      (->
        kp
        (update :public decode-public-key)
        (update :private decode-private-key)))
    (dataset/search-entity
      (id/entity :oauth/key-pair)
      {:active {:_eq true}
       :_order_by {:created_on :desc}}
      {(id/key) nil
       :created_on nil
       :kid nil
       :public nil
       :private nil})))

(defn on-key-pair-add
  [{{:keys [kid public private]} :key-pair}]
  (try
    (dataset/stack-entity
      (id/entity :oauth/key-pair)
      {:kid kid
       :active true
       :public (encode-rsa public)
       :private (encode-rsa private)})
    (catch Throwable ex
      (log/error! {:id ::keypair-save-failed
                   :msg "Couldn't save RSA keypair. Check if encryption is enabled."}
                  ex))))

(defn on-key-pair-remove
  [{:keys [key-pairs]}]
  (doseq [{:keys [kid]} key-pairs
          :when kid]
    (dataset/stack-entity (id/entity :oauth/key-pair)
                          {:kid kid
                           :active false})))

(defn on-token-revoke
  [{token-type :token/key
    token :token/data}]
  (dataset/stack-entity
    (if (= token-type :access_token)
      (id/entity :oauth/access-token)
      (id/entity :oauth/refresh-token))
    {:value token
     :revoked true}))

(defn on-tokens-grant
  [{{refresh-token :refresh_token
     access-token :access_token} :tokens
    :keys [session]}]
  (let [{:keys [kid]} (jwt/decode-header access-token)]
    (when access-token
      (dataset/stack-entity
        (id/entity :oauth/access-token)
        {:value access-token
         :session {:id session}
         :expires-at (core/expires-at access-token)
         :signed_by {:kid kid}}))
    (when refresh-token
      (dataset/stack-entity
        (id/entity :oauth/refresh-token)
        {:value refresh-token
         :session {:id session}
         :expires-at (core/expires-at refresh-token)
         :signed_by {:kid kid}}))))

(defn on-session-create
  [{:keys [session audience user scope client]}]
  (dataset/stack-entity
    (id/entity :oauth/session)
    {:id session
     :user {(id/key) (id/extract user)}
     :audience audience
     :active true
     :client {(id/key) client}
     :scope (str/join " " scope)}))

(defn on-session-kill
  [{:keys [session]}]
  (dataset/stack-entity
    (id/entity :oauth/session)
    {:id session
     :active false}))

(defn current-version
  []
  (dataset/<-resource "dataset/oauth_session.json"))

(defn level-store
  []
  (let [{store-version :name
         :as store-dataset} (current-version)
        {deployed-version :name} (dataset/latest-deployed-version (id/data :oauth/dataset-version))]
    (when (and store-version
               (not= store-version deployed-version))
      (log/info {:id ::deploy-newer-store-version
                 :data {:action :deploying :subject :oauth-store
                        :store-version store-version
                        :deployed-version deployed-version}}
                "Store version differs from deployed; deploying")
      (dataset/deploy! store-dataset)
      (dataset/reload))))

(defn load-session
  [{[{access-token :value}] :access_tokens
    [{refresh-token :value}] :refresh_tokens
    session :id
    user :user
    client :client}]
  (if-let [{audience "aud"
            scope "scope"} (and access-token (encryption/unsign-data access-token))]
    (let [scope (set (str/split (or scope "") #" "))
          user-details (core/get-resource-owner (:name user))
          client-id (id/extract client)]
      (core/set-session session {:client client-id
                                 :last-active (java.util.Date.)})
      (token/set-session-tokens session audience
                                {:access_token access-token
                                 :refresh_token refresh-token})
      (core/set-session-audience-scope session audience scope)
      (core/set-session-resource-owner session user-details)
      (core/set-session-authorized-at session (java.util.Date.)))
    (log/warn {:id ::skip-unverifiable-session
               :data {:action :loading :subject :oauth-store
                      :session session}}
              "Skipping persisted session: access token missing or unverifiable")))

(defn load-sessions
  []
  (let [;; "not revoked" preserves 3-valued logic that the legacy
        ;; `:_boolean :NOT_TRUE` carried — match rows where revoked is
        ;; explicitly false OR the column is NULL (default state).
        not-revoked {:_or [{:revoked {:_eq false}}
                           {:revoked :is_null}]}
        sessions
        (dataset/search-entity
          (id/entity :oauth/session)
          {:active {:_eq true}}
          {(id/key) nil
           :id nil
           :client [{:selections {(id/key) nil}}]
           :user [{:selections {:name nil}}]
           :access_tokens [{:selections {:value nil}
                            :args {:_where not-revoked
                                   :_order_by {:expires_at :desc}}}]
           :refresh_tokens [{:selections {:value nil}
                             :args {:_maybe not-revoked}}]})]
    (doseq [session sessions] (load-session session))))

(defn- redact-keypair
  "Strip key material from a keypair map, keeping :kid for traceability.
   RSA*KeyImpl getters expose private exponent via bean-style serialization
   used by JSON sinks — never let raw keypair maps reach a log."
  [kp]
  (when kp
    {:kid (:kid kp) :public :redacted :private :redacted}))

(defn- redact-event
  "Sanitize a publisher event before logging. Token and keypair payloads
   carry credential material that must not appear in log sinks."
  [data]
  (case (:topic data)
    :keypair/added   (update data :key-pair redact-keypair)
    :keypair/removed (update data :key-pairs #(some->> % (mapv redact-keypair)))
    :oauth.grant/tokens
    (update data :tokens (fn [tokens]
                           (when tokens
                             (reduce-kv (fn [m k _] (assoc m k :redacted))
                                        {} tokens))))
    :oauth.revoke/token (assoc data :token/data :redacted)
    data))

(defn open-store
  []
  (level-store)
  (let [kps (not-empty (get-key-pairs))
        store-messages (async/chan (async/sliding-buffer 200))
        topics [:keypair/added :keypair/removed
                :oauth.revoke/token :oauth.grant/tokens
                :oauth.session/created :oauth.session/killed]]
    (doseq [topic topics]
      (log/info {:id ::subscribing-to-topic
                 :data {:topic topic}}
                "Subscribing to publisher topic")
      (async/sub iam/publisher topic store-messages))
    (log/info {:id ::store-loop-ready}
              "OAuth persistence store waiting for messages")
    (letfn [(test-message [_key data]
              (when (= (:topic data) _key)
                data))]
      (async/go-loop
        [data (async/<! store-messages)]
        (log/debug {:id ::message-received
                    :data {:message (redact-event data)}}
                   "Persistence loop received message")
        (try
          (condp test-message data
            :keypair/removed :>> on-key-pair-remove
            :keypair/added :>> on-key-pair-add
            :oauth.session/created :>> on-session-create
            :oauth.session/killed :>> on-session-kill
            :oauth.grant/tokens :>> on-tokens-grant
            :oauth.revoke/token :>> on-token-revoke
            nil)
          (catch Throwable ex
            (log/error! {:id ::message-processing-failed
                         :msg "Couldn't process received message"
                         :data {:message (redact-event data)}}
                        ex)))
        (recur (async/<! store-messages))))
    ;; Hydrate the provider from DB. Anything already in the provider's
    ;; in-memory atom at this point came from a prior in-memory rotation
    ;; (typical case: encryption was sealed at boot, the dev-fallback in
    ;; on-encryption-enabled minted a keypair, and we're now unsealing).
    ;; Capture that pre-load set so we can persist what isn't already in DB.
    (let [pre-load (encryption/list-keypairs encryption/*encryption-provider*)
          db-kids  (into #{} (map :kid) kps)]
      (doseq [kp kps]
        (encryption/add-keypair encryption/*encryption-provider* kp))

      ;; Provider is now initialised. Ask it the question the iam.encryption
      ;; start used to ask prematurely: do we have any keypairs? If not, mint
      ;; one — add-keypair will publish :keypair/added so the store loop
      ;; persists it.
      (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
        (encryption/rotate-keypair encryption/*encryption-provider*))

      ;; Persist any pre-load (in-memory) keypairs that aren't already in DB.
      ;; On normal boot pre-load is empty and this is a no-op. On the
      ;; encryption-unsealed-after-in-memory path it carries over the
      ;; dev-fallback keypair that signed tokens before storage existed.
      (doseq [k pre-load
              :when (not (contains? db-kids (:kid k)))]
        (iam/publish :keypair/added {:key-pair k})))

    ;; Sessions: when DB had no keypairs we're bootstrapping, so flush
    ;; whatever is in *sessions* to storage. Otherwise load from DB.
    (if (empty? kps)
      (doseq [[session {user-id :resource-owner
                        client-id :client
                        :keys [scopes tokens]}] (deref core/*sessions*)
              :let [audiences (keys tokens)]]
        (doseq [audience audiences
                :let [signed-tokens (get tokens audience)]]
          (iam/publish
            :oauth.session/created
            {:session session
             :client client-id
             :audience audience
             :scope (get scopes audience)
             :user {(id/key) user-id}})
          (iam/publish
            :oauth.grant/tokens
            {:tokens signed-tokens
             :session session})))
      (load-sessions))))

(defn on-encryption-enabled
  "Auto-activate persistence when dataset encryption is available.
   Falls back to in-memory only mode if encryption is not initialized."
  []
  (if (dataset-encryption/initialized?)
    (open-store)
    (do
      (log/warn {:id ::encryption-not-initialized :data {:action :ready :subject :encryption}}
                "Dataset encryption not initialized; running in-memory only")
      (encryption/rotate-keypair encryption/*encryption-provider*))))

(defn purge-key-pairs
  ([] (purge-key-pairs 0))
  ([older-than]
   (let [now (util/now)]
     (dataset/purge-entity
       (id/entity :oauth/key-pair)
       {:_where {:modified_on {:_le (java.util.Date. (- now older-than))}}}
       {:kid nil}))))

(defn purge-sessions
  []
  (dataset/purge-entity
    (id/entity :oauth/session)
    nil
    {(id/key) nil
     :access_tokens [{:selections {(id/key) nil}}]
     :refresh_tokens [{:selections {(id/key) nil}}]}))

(defn purge-tokens
  []
  (dataset/purge-entity (id/entity :oauth/access-token) nil {(id/key) nil})
  (dataset/purge-entity (id/entity :oauth/refresh-token) nil {(id/key) nil}))

(defn start
  []
  (on-encryption-enabled)
  (let [sub (async/chan)]
    (async/sub dataset/publisher :encryption/unsealed sub)
    (async/go
      (loop [{:keys [master]} (async/<! sub)]
        (if master
          (on-encryption-enabled)
          (recur (async/<! sub)))))))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/oauth.persistence
  {:depends-on [:synthigy/oauth :synthigy.iam/encryption]
   :doc "Persists OAuth clients/tokens to DB; survives restarts"
   :start (fn []
            ;; Runtime: Subscribe to encryption events, initialize token handlers
            (log/info {:id ::starting :data {:action :starting :subject :oauth-persistence}} "Starting OAuth persistence")
            (start)
            (log/info {:id ::started :data {:action :started :subject :oauth-persistence}} "OAuth persistence started"))
   :stop (fn []
           ;; No stop function needed - async channel cleanup happens automatically
           nil)})


(comment
  (lifecycle/print-system-report)
  (lifecycle/start! :synthigy/oauth.persistence))
