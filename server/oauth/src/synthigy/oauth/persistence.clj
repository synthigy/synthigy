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

(ns synthigy.oauth.persistence
  (:require
    [clojure.core.async :as async]
    [synthigy.log :as log]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.iam.access :as access]
    [synthigy.iam.encryption :as encryption]
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

(id/defentity :oauth/authorization-code
  :euuid #uuid "54208f11-3cc1-49c7-b63a-87057e9dbd07"
  :xid "BPXXE6RC9coRP1yMrntzRk")

(id/defentity :oauth/device-code
  :euuid #uuid "5ce63576-6da8-482c-b93a-f323f7fb540f"
  :xid "CUMT1ViWgeBMrDA3AmweY6")

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
      (dataset/reload)
      ;; The store model owns the User Role → scopes relation; IAM access
      ;; starts earlier and skips scope loading when the relation is absent,
      ;; so a level that (first) brings it in must load scopes itself.
      (access/load-scopes))))

;; RSA*KeyImpl getters expose the private exponent via bean-style
;; serialization used by JSON sinks — never let raw keypair maps reach a log.
(defn redact-keypair
  [kp]
  (when kp
    {:kid (:kid kp) :public :redacted :private :redacted}))

(defn redact-event
  [data]
  (case (:topic data)
    :keypair/added   (update data :key-pair redact-keypair)
    :keypair/removed (update data :key-pairs #(some->> % (mapv redact-keypair)))
    data))

(defn open-store
  "Deploy/level the store dataset and wire keypair persistence."
  []
  (level-store)
  (let [kps (not-empty (get-key-pairs))
        store-messages (async/chan (async/sliding-buffer 200))
        topics [:keypair/added :keypair/removed]]
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
            nil)
          (catch Throwable ex
            (log/error! {:id ::message-processing-failed
                         :msg "Couldn't process received message"
                         :data {:message (redact-event data)}}
                        ex)))
        (recur (async/<! store-messages))))
    ;; Order-sensitive: capture the provider's PRE-LOAD in-memory keypairs
    ;; (from a prior in-memory-only rotation) before loading DB keypairs in,
    ;; so anything not yet in DB can be persisted below.
    (let [pre-load (encryption/list-keypairs encryption/*encryption-provider*)
          db-kids  (into #{} (map :kid) kps)]
      (doseq [kp kps]
        (encryption/add-keypair encryption/*encryption-provider* kp))

      (when (empty? (encryption/list-keypairs encryption/*encryption-provider*))
        (encryption/rotate-keypair encryption/*encryption-provider*))

      ;; Persist any pre-load keypair not already in DB (e.g. a dev-fallback
      ;; keypair that signed tokens before storage existed).
      (doseq [k pre-load
              :when (not (contains? db-kids (:kid k)))]
        (iam/publish :keypair/added {:key-pair k})))))

(defn on-encryption-enabled
  "Auto-activate persistence when dataset encryption is available; falls back to
   in-memory only otherwise."
  []
  (if (dataset-encryption/initialized?)
    (open-store)
    (do
      (log/warn {:id ::encryption-not-initialized :data {:action :ready :subject :encryption}}
                "Dataset encryption not initialized; running in-memory only")
      (encryption/rotate-keypair encryption/*encryption-provider*))))

(def session-row-retention
  "How long killed session rows survive for inspection before purge."
  (util/days 7))

(defn clean-expired-rows!
  "Janitor for the DB-first token/session store; deletes token rows only once
   past expiry (+1h slack) so revoked-but-unexpired rows keep surviving
   revocation checks."
  []
  (let [cutoff (java.util.Date. (- (util/now) (util/hours 1)))]
    (doseq [entity [:oauth/access-token :oauth/refresh-token]]
      (dataset/purge-entity (id/entity entity)
                            {:_where {:expires_at {:_le cutoff}}}
                            {:xid nil}))
    (let [retention-cutoff (java.util.Date. (- (util/now) session-row-retention))
          dead (->> (dataset/search-entity
                     (id/entity :oauth/session)
                     {:_where {:_and [{:active {:_eq false}}
                                      {:finished {:_le retention-cutoff}}]}}
                     {:xid nil
                      :access_tokens [{:selections {:xid nil}
                                       :args {:_join :left :_limit 1}}]
                      :refresh_tokens [{:selections {:xid nil}
                                        :args {:_join :left :_limit 1}}]})
                    (filter #(and (empty? (:access_tokens %))
                                  (empty? (:refresh_tokens %))))
                    (mapv :xid))]
      (doseq [chunk (partition-all 500 dead)]
        (dataset/purge-entity (id/entity :oauth/session)
                              {:_where {:xid {:_in (vec chunk)}}}
                              {:xid nil}))
      (when (seq dead)
        (log/info {:id ::janitor-swept
                   :data {:action :cleanup :subject :oauth-store
                          :sessions (count dead)}}
                  "Swept finished token-less session rows")))))

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

(defn purge-codes
  []
  (dataset/purge-entity (id/entity :oauth/authorization-code) nil {:code nil})
  (dataset/purge-entity (id/entity :oauth/device-code) nil {:device_code nil}))

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
   :doc "Levels the OAuth store dataset and persists RSA keypairs; session/token rows are written DB-first at their call sites"
   :start (fn []
            (log/info {:id ::starting :data {:action :starting :subject :oauth-persistence}} "Starting OAuth persistence")
            (start)
            (log/info {:id ::started :data {:action :started :subject :oauth-persistence}} "OAuth persistence started"))
   :stop (fn [] nil)})


(comment
  (lifecycle/print-system-report)
  (lifecycle/start! :synthigy/oauth.persistence))
