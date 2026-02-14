(ns synthigy.oauth.persistence
  (:require
    [buddy.sign.jwt :as jwt]
    [clojure.core.async :as async]
    [clojure.pprint :refer [pprint]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.iam.encryption :as encryption]
    [synthigy.oauth.core :as core]
    [synthigy.oauth.token :as token]
    [timing.core :as timing])
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
       :_order_by {:modified_on :desc}}
      {(id/key) nil
       :modified_on nil
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
      (log/error ex "[OAuth Persistence] Couldn't save RSA keypair. Check if encryption is enabled"))))

(defn on-key-pair-remove
  [{{:keys [kid]} :key-pair}]
  (dataset/stack-entity (id/entity :oauth/key-pair)
                        {:kid kid
                         :active false}))

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
    (when (and store-version deployed-version
               (not= current-version deployed-version))
      (log/info "[OAuth Persistence] Old version deployed. Deploying newer version!")
      (dataset/deploy! store-dataset)
      (dataset/reload))))

(defn load-session
  [{[{access-token :value}] :access_tokens
    [{refresh-token :value}] :refresh_tokens
    session :id
    user :user
    client :client}]
  (let [{audience "aud"
         scope "scope"} (encryption/unsign-data access-token)
        scope (set (str/split scope #" "))
        user-details (core/get-resource-owner (:name user))
        client-id (id/extract client)]
    (core/set-session session {:client client-id
                               :last-active (timing/date)})
    (token/set-session-tokens session audience
                              {:access_token access-token
                               :refresh_token refresh-token})
    (core/set-session-audience-scope session audience scope)
    (core/set-session-resource-owner session user-details)
    (core/set-session-authorized-at session (timing/date))))

(defn load-sessions
  []
  (let [sessions
        (dataset/search-entity
          (id/entity :oauth/session)
          {:active {:_boolean :TRUE}}
          {(id/key) nil
           :id nil
           :client [{:selections {(id/key) nil}}]
           :user [{:selections {:name nil}}]
           :access_tokens [{:selections {:value nil}
                            :args {:_where {:revoked {:_boolean :NOT_TRUE}}
                                   :_order_by {:expires_at :desc}}}]
           :refresh_tokens [{:selections {:value nil}
                             :args {:_maybe {:revoked {:_boolean :NOT_TRUE}}}}]})]
    (doseq [session sessions] (load-session session))))

(defn open-store
  []
  (level-store)
  (let [kps (not-empty (get-key-pairs))
        store-messages (async/chan (async/sliding-buffer 200))
        topics [:keypair/added :keypair/removed
                :oauth.revoke/token :oauth.grant/tokens
                :oauth.session/created :oauth.session/killed]]
    (doseq [topic topics]
      (log/infof "[OAuth Persistence] Subscribing to: %s" topic)
      (async/sub iam/publisher topic store-messages))
    (log/info "[OAuth Persistence] Store waiting for messages...")
    (letfn [(test-message [_key data]
              (when (= (:topic data) _key)
                data))]
      (async/go-loop
        [data (async/<! store-messages)]
        (log/debugf "[OAuth Persistence] Received message\n%s" (with-out-str (pprint data)))
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
            (log/errorf ex "[OAuth Persistence] Couldn't process received message: %s" (with-out-str (pprint data)))))
        (recur (async/<! store-messages))))
    ;; If there are no keypairs in DB
    (if (empty? kps)
      ;; Than initialize default encryption
      ;; and it should store values in DB
      (do
        (encryption/rotate-keypair encryption/*encryption-provider*)
        ;; Save current sessions
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
        ;; Publish current keypairs to DB
        (doseq [k (encryption/list-keypairs encryption/*encryption-provider*)]
          (iam/publish
            :keypair/added
            {:key-pair k})))
      ;; If there were already keys in DB
      ;; and they weren't loaded... Like
      ;; if encryption wasn't unsealed... than
      ;; load those keys and add to current provider
      (let [current (encryption/list-keypairs encryption/*encryption-provider*)]
        ;; Add DB keypairs to provider
        (doseq [kp kps]
          (encryption/add-keypair encryption/*encryption-provider* kp))
        ;; Notify loop above that it should store
        ;; keys that were initialized (not from DB)
        (doseq [k current]
          (iam/publish
            :keypair/added
            {:key-pair k}))
        (load-sessions)))))

(defn on-encryption-enabled
  "Auto-activate persistence when dataset encryption is available.
   Falls back to in-memory only mode if encryption is not initialized."
  []
  (if (dataset-encryption/initialized?)
    (open-store)
    (do
      (log/warn "[OAuth Persistence] Dataset encryption not initialized. Running in-memory only.")
      (encryption/rotate-keypair encryption/*encryption-provider*))))

(defn purge-key-pairs
  ([] (purge-key-pairs 0))
  ([older-than]
   (let [now (timing/time->value (timing/date))]
     (dataset/purge-entity
       (id/entity :oauth/key-pair)
       {:_where {:modified_on {:_le (timing/value->time
                                      (- now older-than))}}}
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
   :start (fn []
            ;; Runtime: Subscribe to encryption events, initialize token handlers
            (log/info "[OAUTH.PERSISTENCE] Starting OAuth persistence...")
            (start)
            (log/info "[OAUTH.PERSISTENCE] OAuth persistence started"))
   :stop (fn []
           ;; No stop function needed - async channel cleanup happens automatically
           nil)})


(comment
  (lifecycle/print-system-report)
  (lifecycle/start! :synthigy/oauth.persistence))
