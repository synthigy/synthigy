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

(ns synthigy.dataset.encryption
  (:require
   [buddy.core.crypto :as crypto]
   [buddy.core.nonce :as nonce]
   [synthigy.json :as json]
   [clojure.string :as str]
   [synthigy.log :as log]
   [environ.core :refer [env]]
   [next.jdbc :as jdbc]
   [patcho.lifecycle :as lifecycle]
   synthigy.dataset
   [synthigy.dataset.id :as id]
   [synthigy.db.sql :as sql]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db])
  (:import
   java.math.BigInteger
   [java.net URI]
   [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.nio.charset StandardCharsets]
   [java.time Duration]
   [java.util Base64]
   [javax.crypto Mac]
   [javax.crypto.spec SecretKeySpec]))

(defn dek-table-exists?
  []
  (db/table-exists? db/*db* "__deks"))

(defn column-exists?
  [table column]
  (db/column-exists? db/*db* table column))

(defonce ^{:doc "Raw AES key, non-nil only under a Local provider — nil under Vault/Webhook by design, not an initialized flag."}
  ^:dynamic *master-key*
  nil)
(defonce ^:dynamic *dek* nil)

(defprotocol KeyWrapProvider
  "Wraps/unwraps DEKs under a root key the provider holds or reaches."
  (wrap-dek [this dek-bytes]
    "Encrypt raw DEK bytes into an opaque JSON-serializable value for __deks.dek.")
  (unwrap-dek [this wrapped]
    "Decrypt a wrap-dek value back to raw DEK bytes; throws on auth failure.")
  (provider-tag [this]
    "Stable string identifying this provider, stamped on __deks rows."))

(defonce ^{:dynamic true
           :doc "Provider wrapping NEW DEKs; set by start."}
  *key-wrap-provider*
  nil)

(defonce ^{:doc "tag -> KeyWrapProvider, so existing DEKs resolve their own wrapper mid-migration."}
  provider-registry
  (atom {}))

(defn register-provider!
  [provider]
  (swap! provider-registry assoc (provider-tag provider) provider)
  nil)

(defn resolve-provider
  "Provider for a __deks wrap_provider tag: nil falls back to *key-wrap-provider*, an unknown tag throws."
  [tag]
  (cond
    (nil? tag) *key-wrap-provider*
    (contains? @provider-registry tag) (get @provider-registry tag)
    :else
    (throw (ex-info
            (str "No KeyWrapProvider configured for wrap_provider "
                 (pr-str tag) " — this DEK was wrapped under a provider "
                 "that is not active in this boot. Configure it (vault/"
                 "webhook env vars, or the matching manual/default master "
                 "key) before starting, or migrate this row to a "
                 "currently-configured provider first.")
            {:code :unresolvable-wrap-provider :tag tag}))))

(defn parse-master-bytes
  "Decode a hex:/b64:/legacy-decimal master-key string into 32 AES-256 key bytes."
  [^String s]
  (cond
    (str/starts-with? s "hex:")
    (let [hex (subs s 4)
          _ (when (not= 64 (count hex))
              (throw (ex-info "hex-encoded master key must be exactly 64 hex chars (32 bytes)"
                              {:length (count hex)})))
          bs (byte-array 32)]
      (dotimes [i 32]
        (aset-byte bs i
                   (unchecked-byte
                    (Integer/parseInt (.substring hex (* i 2) (+ 2 (* i 2))) 16))))
      bs)

    (str/starts-with? s "b64:")
    (let [decoded (.decode (Base64/getDecoder) ^String (subs s 4))]
      (when (not= 32 (count decoded))
        (throw (ex-info "base64-encoded master key must decode to exactly 32 bytes"
                        {:length (count decoded)})))
      decoded)

    :else
    ;; legacy decimal — old keys only, zero-pads short values, never for new keys
    (byte-array
     (take 32
           (concat (.toByteArray (BigInteger. s))
                   (repeat 0))))))

(defn env-master-key
  "Reads SYNTHIGY_ENCRYPTION_MASTER_KEY."
  []
  (not-empty (env :synthigy-encryption-master-key)))

(defn random-master
  "Generate a fresh 256-bit master key as a b64:-prefixed string."
  []
  (let [bytes (byte-array 32)]
    (.nextBytes (java.security.SecureRandom.) bytes)
    (str "b64:" (.encodeToString (Base64/getEncoder) bytes))))

(defonce deks (atom nil))

(defn get-dek [data]
  (cond
    (number? data) (get @deks data)
    (instance? javax.crypto.spec.SecretKeySpec data) (.getEncoded data)
    :else data))

(defn create-dek-table []
  (let [{:keys [serial-pk json now]} (db/ddl db/*db*)]
    (jdbc/execute-one!
     (:datasource db/*db*)
     [(str/join "\n"
                ["create table __deks("
                 (str "   id " serial-pk ",")
                 (str "   dek " json " not null,")
                 (str "   encryption_barrier " json ",")
                 "   key_algorithm VARCHAR(50) not null,"
                 (str "   created_at TIMESTAMP default " now ",")
                 "   expires_at TIMESTAMP,"
                 "   active BOOLEAN default true,"
                 "   wrap_provider TEXT,"
                 "   wrap_key_version TEXT"
                 ");"])])))

(defn drop-dek-table []
  (jdbc/execute-one! (:datasource db/*db*) ["drop table if exists \"__deks\""]))

(defn ensure-deks-columns!
  "Idempotently ALTER any __deks columns introduced after the original 5-column shape."
  []
  (when (dek-table-exists?)
    (doseq [column ["wrap_provider" "wrap_key_version"]]
      (when-not (column-exists? "__deks" column)
        (log/info {:id ::deks-column-added
                   :data {:action :migrating :subject :dek :column column}}
                  (str "Adding " column " to __deks"))
        (sql/execute! [(format "ALTER TABLE __deks ADD COLUMN %s TEXT" column)])))))

(defn generate-key [key-size]
  (let [key-bytes (nonce/random-bytes (/ key-size 8))]
    (SecretKeySpec. key-bytes "AES")))

(defn ->LocalKeyWrapProvider
  "KeyWrapProvider wrapping DEKs with AES-256-GCM under a locally-held master key."
  [master-key]
  (reify KeyWrapProvider
    (wrap-dek [_ dek-bytes]
      (let [key-string (.encodeToString (Base64/getEncoder) ^bytes dek-bytes)
            iv (nonce/random-bytes 12)
            encrypted (crypto/encrypt
                       (.getBytes key-string "UTF-8")
                       (.getEncoded ^SecretKeySpec master-key)
                       iv
                       {:alg :aes256-gcm})]
        {:key (.encodeToString (Base64/getEncoder) encrypted)
         :iv (.encodeToString (Base64/getEncoder) iv)}))

    (unwrap-dek [_ {aes-key :key iv :iv}]
      (let [aes-key (.decode (Base64/getDecoder) ^String aes-key)
            iv (.decode (Base64/getDecoder) ^String iv)
            decrypted (crypto/decrypt
                       aes-key
                       (.getEncoded ^SecretKeySpec master-key)
                       iv
                       {:alg :aes256-gcm})]
        (.decode (Base64/getDecoder) decrypted)))

    (provider-tag [_] "default")))

(defn hmac-sha256
  "Base64-encoded HMAC-SHA256 of `payload` under `secret`."
  [secret payload]
  (let [mac (Mac/getInstance "HmacSHA256")
        key-spec (SecretKeySpec. (.getBytes ^String secret StandardCharsets/UTF_8) "HmacSHA256")]
    (.init mac key-spec)
    (->> (.getBytes ^String payload StandardCharsets/UTF_8)
         (.doFinal mac)
         (.encodeToString (Base64/getEncoder)))))

(defn http-post-json
  "Synchronous JSON POST: {:status :body} on 2xx/4xx, throws on network failure/timeout/5xx."
  [{:keys [url body headers timeout-ms]}]
  (let [client (-> (HttpClient/newBuilder)
                   (.connectTimeout (Duration/ofMillis (or timeout-ms 1500)))
                   .build)
        req-builder (-> (HttpRequest/newBuilder)
                        (.uri (URI/create url))
                        (.timeout (Duration/ofMillis (or timeout-ms 1500)))
                        (.header "Content-Type" "application/json")
                        (.POST (HttpRequest$BodyPublishers/ofString body)))
        _ (doseq [[k v] headers] (.header req-builder (name k) (str v)))
        resp (.send client (.build req-builder) (HttpResponse$BodyHandlers/ofString))
        status (.statusCode resp)]
    (when (>= status 500)
      (throw (ex-info "Backend 5xx" {:status status :body (.body resp)})))
    {:status status :body (.body resp)}))

(defn vault-token
  "Static :token if configured, else a fresh (uncached) Kubernetes-auth login per call."
  [{:keys [addr token role mount jwt-path timeout-ms] :or {mount "kubernetes"}}]
  (or token
      (let [jwt (str/trim (slurp (or jwt-path "/var/run/secrets/kubernetes.io/serviceaccount/token")))
            {:keys [status body]}
            (http-post-json
             {:url (str addr "/v1/auth/" mount "/login")
              :body (json/write-str {:role role :jwt jwt})
              :timeout-ms timeout-ms})]
        (if (= 200 status)
          (get-in (json/read-str body) [:auth :client_token])
          (throw (ex-info "Vault Kubernetes auth login failed"
                          {:status status :body body}))))))

(defn ->VaultTransitKeyWrapProvider
  "KeyWrapProvider over Vault's Transit engine — the root key never leaves Vault."
  [{:keys [addr transit-key transit-mount timeout-ms] :as config}]
  (let [mount (or (not-empty transit-mount) "transit")]
   (reify KeyWrapProvider
    (wrap-dek [_ dek-bytes]
      (let [{:keys [status body]}
            (http-post-json
             {:url (str addr "/v1/" mount "/encrypt/" transit-key)
              :body (json/write-str {:plaintext (.encodeToString (Base64/getEncoder) ^bytes dek-bytes)})
              :headers {"X-Vault-Token" (vault-token config)}
              :timeout-ms timeout-ms})]
        (if (= 200 status)
          {:ciphertext (get-in (json/read-str body) [:data :ciphertext])}
          (throw (ex-info "Vault transit encrypt failed" {:status status :body body})))))

    (unwrap-dek [_ {:keys [ciphertext]}]
      (let [{:keys [status body]}
            (http-post-json
             {:url (str addr "/v1/" mount "/decrypt/" transit-key)
              :body (json/write-str {:ciphertext ciphertext})
              :headers {"X-Vault-Token" (vault-token config)}
              :timeout-ms timeout-ms})]
        (if (= 200 status)
          (.decode (Base64/getDecoder) ^String (get-in (json/read-str body) [:data :plaintext]))
          (throw (ex-info "Vault transit decrypt failed" {:status status :body body})))))

    (provider-tag [_] (if (= mount "transit")
                        (str "vault-transit:" transit-key)
                        (str "vault-transit:" mount "/" transit-key))))))

(defn ->WebhookKeyWrapProvider
  "KeyWrapProvider over an operator-owned HTTP endpoint (wire contract in docs)."
  [{:keys [url secret timeout-ms]}]
  (letfn [(call [op extra-fields response-key]
            (let [request-id (id/generate-xid)
                  payload (json/write-str (merge {:op op :request_id request-id} extra-fields))
                  signature (when secret (str "hmac-sha256=" (hmac-sha256 secret payload)))
                  {:keys [status body]}
                  (http-post-json
                   {:url url :body payload :timeout-ms timeout-ms
                    :headers (cond-> {} signature (assoc "X-Synthigy-Signature" signature))})
                  parsed (when (seq body) (try (json/read-str body) (catch Throwable _ nil)))]
              (if (and (= 200 status) (contains? parsed response-key))
                (get parsed response-key)
                (throw (ex-info (str "Webhook " op " failed")
                                {:status status :body body :request-id request-id})))))]
    (reify KeyWrapProvider
      (wrap-dek [_ dek-bytes]
        {:wrapped (call "wrap" {:dek (.encodeToString (Base64/getEncoder) ^bytes dek-bytes)} :wrapped)})

      (unwrap-dek [_ {:keys [wrapped]}]
        (.decode (Base64/getDecoder) ^String (call "unwrap" {:wrapped wrapped} :dek)))

      (provider-tag [_] (str "webhook:" url)))))

(defn encrypt-dek
  "Wrap a DEK (SecretKeySpec) via the configured *key-wrap-provider*."
  [dek]
  (wrap-dek *key-wrap-provider* (.getEncoded ^SecretKeySpec dek)))

(defn decrypt-dek
  "Unwrap an encrypt-dek envelope via the provider resolved from `wrap-provider` (the row's tag)."
  ([wrapped] (decrypt-dek wrapped nil))
  ([wrapped wrap-provider]
   (unwrap-dek (resolve-provider wrap-provider) wrapped)))

(def ^:private encryption-barrier "this_was_encrypted")

(defn encrypt-data
  "Encrypt a string under the active DEK."
  [data]
  (if-not *dek*
    (do
      (log/error {:id ::encrypt-no-dek}
                 "Couldn't encrypt data: *dek* isn't specified")
      (throw
       (ex-info
        "Couldn't encrypt data. Encryption is not initialized"
        {:type :encryption/not-initialized})))
    (let [iv (nonce/random-bytes 12)
          current-dek *dek*
          dek (get-dek current-dek)
          encrypted (crypto/encrypt (.getBytes data "UTF-8") dek iv {:alg :aes256-gcm})]
      {:data (.encodeToString (Base64/getEncoder) encrypted)
       :dek current-dek
       :iv (.encodeToString (Base64/getEncoder) iv)})))

(defn decrypt-data
  "Decrypt an encrypt-data envelope."
  [{:keys [data dek iv]}]
  (when (and dek iv)
    (let [aes-key  (get-dek dek)
          iv-bytes (.decode (Base64/getDecoder) iv)
          decrypted (crypto/decrypt (.decode (Base64/getDecoder) data)
                                    aes-key iv-bytes {:alg :aes256-gcm})]
      (String. decrypted "UTF-8"))))

(defn add-dek-barrier
  [id]
  (jdbc/execute-one!
   (:datasource db/*db*)
   ["update __deks set encryption_barrier=? where id=?"
    (db/json-param db/*db*
           (json/write-str
            (binding [*dek* id]
              (encrypt-data encryption-barrier))))
    id]))

(defn set-dek-active
  [id]
  (jdbc/execute-one! (:datasource db/*db*) ["update __deks set active=false where id!=?" id]))

(defn dek->db
  [dek]
  (let [encrypted (encrypt-dek dek)
        {id :__deks/id}
        (jdbc/execute-one!
         (:datasource db/*db*)
         ["insert into __deks (dek, key_algorithm, active, wrap_provider) values (?, ?, ?, ?) returning id"
          (db/json-param db/*db* (json/write-str encrypted))
          "aes256-gcm"
          true
          (provider-tag *key-wrap-provider*)])]
    id))

(defn create-dek
  []
  (let [dek (generate-key 256)
        id (dek->db dek)]
    (swap! deks assoc id (.getEncoded dek))
    (add-dek-barrier id)
    (set-dek-active id)
    (alter-var-root #'*dek* (fn [_] id))
    id))

(defn encrypted-targets
  "Every encrypted-typed attribute of the deployed model as {:entity-name :table :column}."
  [model]
  (vec
   (for [e (vals (:entities model))
         a (:attributes e)
         :when (= "encrypted" (:type a))]
     {:entity-name (:name e)
      :table       (naming/entity->table-name e)
      :column      (naming/normalize-name (:name a))})))

(defn rewrap-cell
  "Re-encrypt one cell envelope from `source-bytes` to `target-bytes`."
  [cell-value source-bytes new-id target-bytes]
  (let [{:keys [data iv]} (json/read-str (db/json-column db/*db* cell-value))
        opts     {:alg :aes256-gcm}
        cipher   (.decode (Base64/getDecoder) ^String data)
        iv-bytes (.decode (Base64/getDecoder) ^String iv)
        plain    (crypto/decrypt cipher source-bytes iv-bytes opts)
        new-iv   (nonce/random-bytes 12)
        new-ct   (crypto/encrypt plain target-bytes new-iv opts)]
    (json/write-str
     {:data (.encodeToString (Base64/getEncoder) ^bytes new-ct)
      :dek  new-id
      :iv   (.encodeToString (Base64/getEncoder) ^bytes new-iv)})))

(defn rewrap-column!
  "Re-encrypt every non-null cell in `table`.`column` under the new DEK; an undecryptable cell throws and aborts the rotation — never silently lose cells."
  [tx table column new-id new-bytes]
  (let [eid-k  (keyword table "_eid")
        cell-k (keyword table column)
        rows   (jdbc/execute!
                tx
                [(format "SELECT _eid, \"%s\" FROM \"%s\" WHERE \"%s\" IS NOT NULL"
                         column table column)])]
    (reduce
     (fn [n row]
       (let [eid  (get row eid-k)
             cell (get row cell-k)
             {src-dek :dek} (json/read-str (db/json-column db/*db* cell))
             src-bytes (or (get-dek src-dek)
                           (throw (ex-info "Cannot rotate: source DEK bytes not in memory"
                                           {:code      :source-dek-missing
                                            :cell-dek  src-dek
                                            :table     table
                                            :column    column
                                            :eid       eid})))
             new-env (try (rewrap-cell cell src-bytes new-id new-bytes)
                          (catch Throwable e
                            (throw (ex-info
                                    (str "Cannot rotate " table "." column
                                         " (_eid " eid "): " (ex-message e))
                                    {:code :cell-rewrap-failed
                                     :table table :column column :eid eid}
                                    e))))]
         (jdbc/execute-one!
          tx
          [(format "UPDATE \"%s\" SET \"%s\" = ? WHERE _eid = ?" table column)
           (db/json-param db/*db* new-env)
           eid])
         (inc n)))
     0
     rows)))

(defn initialized?
  "True once ANY KeyWrapProvider is active — checks the provider, never *master-key* (nil under Vault/Webhook)."
  []
  (some? *key-wrap-provider*))

(defn fully-initialized?
  "Provider active AND an active DEK loaded — encrypt-data will actually work."
  []
  (and (some? *key-wrap-provider*) (some? *dek*)))

(defn rotate-dek!
  "Atomically re-key: fresh DEK, every encrypted cell rewrapped, active flag flipped — one transaction, old DEK row kept inactive."
  []
  (when-not (initialized?)
    (throw (ex-info "Encryption not initialized" {:code :not-initialized})))
  (when-not *dek*
    (throw (ex-info "No active DEK to rotate from" {:code :no-active-dek})))
  (let [started  (System/currentTimeMillis)
        old-id   *dek*
        model    (synthigy.dataset/deployed-model)
        targets  (encrypted-targets model)
        new-dek       (generate-key 256)
        new-dek-bytes (.getEncoded new-dek)]
    (with-open [con (jdbc/get-connection (:datasource db/*db*))]
      (let [result
            (jdbc/with-transaction [tx con]
              (let [encrypted-new (encrypt-dek new-dek)
                    {new-id :__deks/id}
                    (jdbc/execute-one!
                     tx
                     ["insert into __deks (dek, key_algorithm, active, wrap_provider) values (?, ?, ?, ?) returning id"
                      (db/json-param db/*db* (json/write-str encrypted-new))
                      "aes256-gcm"
                      false
                      (provider-tag *key-wrap-provider*)])
                    b-iv  (nonce/random-bytes 12)
                    b-ct  (crypto/encrypt
                           (.getBytes encryption-barrier "UTF-8")
                           new-dek-bytes b-iv {:alg :aes256-gcm})
                    b-env {:data (.encodeToString (Base64/getEncoder) ^bytes b-ct)
                           :dek  new-id
                           :iv   (.encodeToString (Base64/getEncoder) ^bytes b-iv)}
                    _ (jdbc/execute-one!
                       tx
                       ["update __deks set encryption_barrier=? where id=?"
                        (db/json-param db/*db* (json/write-str b-env))
                        new-id])
                    per-column
                    (mapv
                     (fn [{:keys [entity-name table column]}]
                       [entity-name column
                        (rewrap-column! tx table column new-id new-dek-bytes)])
                     targets)
                    _ (jdbc/execute-one! tx ["update __deks set active=false where id!=?" new-id])
                    _ (jdbc/execute-one! tx ["update __deks set active=true where id=?" new-id])]
                {:old-dek-id      old-id
                 :new-dek-id      new-id
                 :columns         per-column
                 :total-rewrapped (reduce + 0 (map #(nth % 2) per-column))}))]
        (swap! deks assoc (:new-dek-id result) new-dek-bytes)
        (alter-var-root #'*dek* (constantly (:new-dek-id result)))
        (let [elapsed (- (System/currentTimeMillis) started)]
          (log/info {:id ::dek-rotated
                     :data {:action :rotated :subject :dek
                            :old-dek-id (:old-dek-id result)
                            :new-dek-id (:new-dek-id result)
                            :total-rewrapped (:total-rewrapped result)
                            :column-count (count (:columns result))
                            :elapsed-ms elapsed}}
                    "Rotated DEK")
          (assoc result :duration-ms elapsed))))))

(defn db-deks
  []
  (jdbc/execute! (:datasource db/*db*)
                 ["select id,dek,active,encryption_barrier,wrap_provider,created_at from __deks"]))

(defn migrate-provider!
  "Rewrap every DEK under `target` in one transaction (data cells untouched) and make it the active provider for this process."
  [target & {:keys [rotate-dek?]}]
  (let [target-tag (provider-tag target)
        _ (when (contains? (into #{} (map :__deks/wrap_provider) (db-deks)) target-tag)
            (throw (ex-info
                    (str "Refusing to migrate to provider tag " (pr-str target-tag)
                         " — at least one __deks row is ALREADY tagged with it. "
                         "migrate-provider! resolves each row's CURRENT wrapper by "
                         "that same tag before rewrapping; registering `target` "
                         "under a tag that's already in use would replace the "
                         "in-use wrapper BEFORE those rows are unwrapped, "
                         "corrupting them. This includes local master-key rotation "
                         "— two different LocalKeyWrapProvider instances both "
                         "tag as \"default\" — which is not migrate-provider!'s "
                         "job; use migrate-local-key! for that.")
                    {:code :migrate-provider-tag-collision :target-tag target-tag})))
        _ (register-provider! target)
        migrated
        (with-open [con (jdbc/get-connection (:datasource db/*db*))]
          (jdbc/with-transaction [tx con]
            (let [rows (jdbc/execute! tx ["select id,dek,wrap_provider from __deks"])]
              (doseq [{id :__deks/id
                       dek :__deks/dek
                       wrap-provider :__deks/wrap_provider} rows]
                (let [source     (resolve-provider wrap-provider)
                      raw-bytes  (unwrap-dek source (json/read-str (db/json-column db/*db* dek)))
                      rewrapped  (wrap-dek target raw-bytes)]
                  (jdbc/execute-one!
                   tx
                   ["update __deks set dek = ?, wrap_provider = ? where id = ?"
                    (db/json-param db/*db* (json/write-str rewrapped))
                    target-tag
                    id])))
              (count rows))))]
    (alter-var-root #'*key-wrap-provider* (constantly target))
    ;; always scrub *master-key* when leaving local custody — a stale key must not stay in heap
    (when-not (= target-tag "default")
      (alter-var-root #'*master-key* (constantly nil)))
    (log/info {:id ::provider-migrated
               :data {:action :migrated :subject :encryption
                      :provider target-tag :row-count migrated}}
              "Migrated DEKs to new key wrap provider")
    (when rotate-dek? (rotate-dek!))
    {:migrated-count migrated :provider target-tag}))

(defn ->local-provider
  "LocalKeyWrapProvider from a master-key string (hex:/b64:/legacy decimal)."
  [master-string]
  (->LocalKeyWrapProvider (SecretKeySpec. ^bytes (parse-master-bytes master-string) "AES")))

(defn migrate-local-key!
  "Rewrap every DEK under a NEW locally-held master key — the master-rotation path migrate-provider! refuses."
  [new-master]
  (when-not (initialized?)
    (throw (ex-info "Encryption not initialized" {:code :not-initialized})))
  ;; never accept legacy decimal for a NEW key — it silently zero-pads
  (when-not (or (str/starts-with? new-master "hex:")
                (str/starts-with? new-master "b64:"))
    (throw (ex-info
            (str "A new master key must be \"hex:\" (64 hex chars) or \"b64:\" "
                 "(base64 of 32 bytes) — use random-master to generate one.")
            {:code :weak-master-key-encoding})))
  (let [master-key (SecretKeySpec. ^bytes (parse-master-bytes new-master) "AES")
        target     (->LocalKeyWrapProvider master-key)
        migrated
        (with-open [con (jdbc/get-connection (:datasource db/*db*))]
          (jdbc/with-transaction [tx con]
            (let [rows (jdbc/execute! tx ["select id,dek,wrap_provider from __deks"])]
              (doseq [{id :__deks/id
                       dek :__deks/dek
                       wrap-provider :__deks/wrap_provider} rows]
                (let [source    (resolve-provider wrap-provider)
                      raw-bytes (unwrap-dek source (json/read-str (db/json-column db/*db* dek)))
                      rewrapped (wrap-dek target raw-bytes)]
                  (jdbc/execute-one!
                   tx
                   ["update __deks set dek = ?, wrap_provider = ? where id = ?"
                    (db/json-param db/*db* (json/write-str rewrapped))
                    "default"
                    id])))
              (count rows))))]
    (register-provider! target)
    (alter-var-root #'*key-wrap-provider* (constantly target))
    (alter-var-root #'*master-key* (constantly master-key))
    (log/info {:id ::local-key-migrated
               :data {:action :migrated :subject :encryption :row-count migrated}}
              "Rewrapped DEKs under a new local master key")
    {:migrated-count migrated :provider "default"}))

(defn master-key-mismatch?
  "True when `e`'s cause chain is the AES-GCM auth-tag failure of a wrong master key — an expected sealed state, not a code error."
  [^Throwable e]
  (loop [t e]
    (cond
      (nil? t)                                                   false
      (= :authtag (:cause (ex-data t)))                          true
      (some-> (.getMessage t) (.contains "mac check in GCM"))    true
      :else                                                      (recur (.getCause t)))))

(defn init-deks
  []
  (reset! deks nil)
  (ensure-deks-columns!)
  (if-not (dek-table-exists?)
    (do
      (log/info {:id ::deks-table-missing}
                "No __deks table found; creating table and first DEK")
      (create-dek-table)
      (create-dek))
    (try
      (if-let [known-deks (not-empty (db-deks))]
        (reduce
         (fn [r {id :__deks/id
                 dek :__deks/dek
                 encryption_barrier :__deks/encryption_barrier
                 active? :__deks/active
                 wrap-provider :__deks/wrap_provider}]
           (let [db-dek (json/read-str (db/json-column db/*db* dek))
                 _encryption-barrier (json/read-str (db/json-column db/*db* encryption_barrier))
                 dek (decrypt-dek db-dek wrap-provider)
                 _ (swap! deks assoc id dek)
                 valid? (= encryption-barrier
                           (try
                             (binding [*dek* dek]
                               (decrypt-data _encryption-barrier))
                             (catch Throwable _
                               (log/error {:id ::barrier-decrypt-failed
                                           :data {:dek-id id}}
                                          "Couldn't decrypt encryption barrier")
                               nil)))]
             (if-not valid? r
                     (do
                       (when active?
                         (alter-var-root #'*dek* (fn [_] id)))
                       (assoc r id dek)))))
         nil
         known-deks)
        (create-dek))
      (catch Throwable ex
        ;; master-key mismatch = expected sealed state, no ERROR here — start raises the one actionable WARN
        (when-not (master-key-mismatch? ex)
          (log/error! {:id ::dek-init-failed
                       :msg "Couldn't initialize all DEKs"
                       :data {:action :starting :subject :dek
                              :initialized-count (count (keys @deks))}}
                      ex))
        (reset! deks nil)
        (throw ex))))
  ;; no active DEK => throw — never a half-initialized system
  (when-not *dek*
    (let [loaded (count (keys @deks))]
      (log/error! {:id ::no-active-dek
                   :msg (str "__deks yielded no ACTIVE, barrier-valid DEK — refusing to "
                             "report encryption as initialized. Either no row is marked "
                             "active, or the active row's encryption barrier can't be "
                             "decrypted under this provider.")
                   :data {:action :not-initialized :subject :dek
                          :deks-loaded loaded
                          :deks-rows (try (count (db-deks)) (catch Throwable _ nil))}}
                  (ex-info "No active DEK after init" {:code :no-active-dek}))
      (throw (ex-info
              (str "Encryption init found no active DEK (" loaded " decryptable of "
                   (try (count (db-deks)) (catch Throwable _ "?")) " rows)")
              {:code :no-active-dek
               :deks-loaded loaded})))))

(defn encryption-required?
  "True when __deks exists and has rows — encrypted data may exist."
  []
  (and (dek-table-exists?)
       (not-empty (db-deks))))

(defn encryption-state
  "One of :not-configured, :configured-not-initialized, :initialized."
  []
  (cond
    (initialized?) :initialized
    (encryption-required?) :configured-not-initialized
    :else :not-configured))

(defn start-with-provider!
  "Register `provider`, load/verify existing DEKs under it, install it; ANY failure rethrows — no runtime unseal, no degraded mode."
  [provider]
  (try
    (register-provider! provider)
    (binding [*key-wrap-provider* provider]
      (init-deks))
    (alter-var-root #'*key-wrap-provider* (fn [_] provider))
    nil
    (catch Throwable ex
      (if (master-key-mismatch? ex)
        (log/error! {:id ::master-key-mismatch
                     :msg (str "Configured provider can't decrypt existing DEKs — "
                               "refusing to boot sealed. Either this database was "
                               "sealed under a different provider/key, or the "
                               "correct one isn't configured for this boot.")
                     :data {:action :not-initialized :subject :encryption
                            :provider (provider-tag provider)
                            :deks-count (try (count (db-deks)) (catch Throwable _ nil))}}
                    ex)
        (log/error! {:id ::start-init-failed
                     :data {:action :starting :subject :encryption
                            :provider (provider-tag provider)}
                     :msg "Couldn't initialize dataset encryption"}
                    ex))
      (throw ex))))

(defn start
  "Activate the Local provider from a master-key string; 0-arity reads SYNTHIGY_ENCRYPTION_MASTER_KEY."
  ([] (start (env-master-key)))
  ([master]
   (when (not-empty master)
     (when-not (or (str/starts-with? master "hex:") (str/starts-with? master "b64:"))
       (log/warn {:id ::legacy-master-key-encoding
                  :data {:action :starting :subject :encryption}}
                 (str "The master key uses the LEGACY decimal encoding — it still "
                      "works, but zero-pads short values and is deprecated. Rotate "
                      "to a generated b64: key (console: Migrate custody -> "
                      "Operator key) and update SYNTHIGY_ENCRYPTION_MASTER_KEY.")))
     (let [bs (parse-master-bytes master)
           master-key (SecretKeySpec. bs "AES")]
       ;; never gate on initialized? here — *master-key* is nil under Vault/Webhook and would false-throw
       (when (and (some? *master-key*) (not= master-key *master-key*))
         (throw
          (ex-info
           "Encryption already initialized with different master key!"
           {:master master
            :master/key master-key})))
       (start-with-provider! (->LocalKeyWrapProvider master-key))
       (alter-var-root #'*master-key* (fn [_] master-key))))))

(defn vault-config
  "Vault Transit config from env, or nil when not configured."
  []
  (let [addr        (not-empty (env :synthigy-vault-addr))
        transit-key (not-empty (env :synthigy-vault-transit-key))]
    (when (and addr transit-key)
      {:addr addr
       :transit-key transit-key
       :transit-mount (not-empty (env :synthigy-vault-transit-mount))
       :token (not-empty (env :synthigy-vault-token))
       :role (not-empty (env :synthigy-vault-role))
       :mount (not-empty (env :synthigy-vault-mount))
       :jwt-path (not-empty (env :synthigy-vault-jwt-path))
       ;; not-empty, not just some->: the operator console CLEARS a custody
       ;; key by writing "KEY=" into .env, which environ reads as "" — and
       ;; (Long/parseLong "") throws, failing the encryption module and
       ;; SEALING the engine on the next boot. Found walking local -> vault,
       ;; where clearing is exactly what the previous step did.
       :timeout-ms (some-> (not-empty (env :synthigy-vault-timeout-ms)) Long/parseLong)})))

(defn webhook-config
  "Webhook key-wrap config from env, or nil when not configured."
  []
  (when-let [url (not-empty (env :synthigy-encryption-webhook-url))]
    {:url url
     :secret (not-empty (env :synthigy-encryption-webhook-secret))
     :timeout-ms (some-> (not-empty (env :synthigy-encryption-webhook-timeout-ms)) Long/parseLong)}))

(defn manual-key-configured?
  []
  (boolean (not-empty (env-master-key))))

(defn configured-source
  "Provider the CURRENT env would select on next boot — read-only, can disagree with the active provider."
  []
  (cond
    (vault-config) :vault
    (webhook-config) :webhook
    (manual-key-configured?) :manual
    :else :default))

(defn env-master-key-matches?
  "Does the env master key still unwrap the active DEKs? nil when the question doesn't apply."
  []
  (when (and (manual-key-configured?)
             (= "default" (some-> *key-wrap-provider* provider-tag)))
    (if-let [row (first (filter #(contains? #{nil "default"} (:__deks/wrap_provider %))
                                (db-deks)))]
      (try
        (some? (unwrap-dek (->local-provider (env-master-key))
                           (json/read-str (db/json-column db/*db* (:__deks/dek row)))))
        (catch Throwable _ false))
      true)))

;; "encrypted" seals HERE at INSERT/SELECT, never via TypeCodec — encode needs the row xid, which doesn't exist yet
(defn seal-cell
  "Encrypt `value` for storage under the active DEK; nil for nil."
  [value]
  (when value
    (db/json-param db/*db*
           (json/write-str (encrypt-data value)))))

(defn unseal-cell
  "Decrypt a stored encrypted cell (backend JSON value, JSON string, or pre-parsed map) back to plaintext, or nil."
  [cell]
  (when cell
    (decrypt-data (if (map? cell) cell (json/read-str (db/json-column db/*db* cell))))))

(defn ensure-initialized!
  "Initialize encryption under exactly ONE provider: vault > webhook > SYNTHIGY_ENCRYPTION_MASTER_KEY; throws when none is configured."
  []
  (if (fully-initialized?)
    {:initialized? true
     :source nil
     :master nil}
    (cond
      (vault-config)
      (do (log/info {:id ::selecting-provider
                     :data {:action :starting :subject :encryption :provider :vault}}
                    "Booting dataset encryption under Vault Transit")
          (start-with-provider! (->VaultTransitKeyWrapProvider (vault-config)))
          {:initialized? true :source :vault :master nil})

      (webhook-config)
      (do (log/info {:id ::selecting-provider
                     :data {:action :starting :subject :encryption :provider :webhook}}
                    "Booting dataset encryption under a webhook key-wrap provider")
          (start-with-provider! (->WebhookKeyWrapProvider (webhook-config)))
          {:initialized? true :source :webhook :master nil})

      (env-master-key)
      (let [master (env-master-key)]
        (start master)
        {:initialized? true :source :env :master master})

      :else
      (throw (ex-info
              (str "No dataset encryption custody configured — set "
                   "SYNTHIGY_ENCRYPTION_MASTER_KEY (random-master generates one; "
                   "a daemon-launched instance does this for you), or configure "
                   "Vault/webhook custody. The key is never written to disk by "
                   "this process.")
              {:code :no-encryption-custody})))))

(defn stop
  []
  (reset! deks nil)
  (reset! provider-registry {})
  (alter-var-root #'*master-key* (fn [_] nil))
  (alter-var-root #'*key-wrap-provider* (fn [_] nil))
  (alter-var-root #'*dek* (fn [_] nil)))

(defn vault-ciphertext-version
  [ct]
  (some->> ct (re-find #"^vault:v(\d+):") second parse-long))

(defonce vault-latest-cache (atom nil))

(defn vault-status
  "Vault key-version report when custody is vault-transit: stored DEK wrapper versions vs the transit key's latest (probed by wrapping throwaway bytes — encrypt returns the latest version, so no extra Vault policy is needed), cached 60s."
  []
  (let [tag (some-> *key-wrap-provider* provider-tag)]
    (when (and tag (str/starts-with? tag "vault-transit:"))
      (try
        (let [stored (->> (db-deks)
                          (keep #(some-> (json/read-str (db/json-column db/*db* (:__deks/dek %)))
                                         :ciphertext
                                         vault-ciphertext-version))
                          distinct sort vec)
              now (System/currentTimeMillis)
              latest (let [{:keys [at latest]} @vault-latest-cache]
                       ;; 15s: one throwaway transit encrypt per expiry — cheap
                       ;; enough that the operator never catches the panel blind
                       ;; after rotating inside Vault
                       (if (and at (< (- now at) 15000))
                         latest
                         (let [v (-> (wrap-dek *key-wrap-provider* (byte-array 32))
                                     :ciphertext vault-ciphertext-version)]
                           (reset! vault-latest-cache {:at now :latest v})
                           v)))]
          {:path tag
           :stored_versions stored
           :latest_version latest
           :lagging (boolean (and latest (seq stored) (some #(< % latest) stored)))})
        (catch Throwable _ nil)))))

(defn rewrap-deks!
  "Rewrap every DEK under the CURRENT provider — same custody, fresh wrap; this is how vault-wrapped rows pick up a rotated transit key's latest version. Never use migrate-provider! for this: its tag-collision guard refuses a target whose tag is already on a row."
  []
  (let [target *key-wrap-provider*
        target-tag (provider-tag target)
        migrated
        (with-open [con (jdbc/get-connection (:datasource db/*db*))]
          (jdbc/with-transaction [tx con]
            (let [rows (jdbc/execute! tx ["select id,dek,wrap_provider from __deks"])]
              (doseq [{id :__deks/id
                       dek :__deks/dek
                       wrap-provider :__deks/wrap_provider} rows]
                (let [source (resolve-provider wrap-provider)
                      raw-bytes (unwrap-dek source (json/read-str (db/json-column db/*db* dek)))
                      rewrapped (wrap-dek target raw-bytes)]
                  (jdbc/execute-one!
                   tx
                   ["update __deks set dek = ?, wrap_provider = ? where id = ?"
                    (db/json-param db/*db* (json/write-str rewrapped))
                    target-tag
                    id])))
              (count rows))))]
    (reset! vault-latest-cache nil)
    (log/info {:id ::deks-rewrapped
               :data {:action :rotated :subject :dek
                      :provider target-tag :row-count migrated}}
              "Rewrapped DEKs under current provider")
    {:migrated-count migrated :provider target-tag}))

(defn encryption-status
  "Current status map; :master_key_present is correctly false under vault/webhook even while initialized."
  []
  (merge {:initialized (initialized?)
          :deks_count (count (keys @deks))
          :master_key_present (some? *master-key*)
          :provider (some-> *key-wrap-provider* provider-tag)}
         (when-let [v (vault-status)] {:vault v})))

(lifecycle/register-module!
 :synthigy.dataset/encryption
 {:doc "key vault, zero deps"
  :start (fn []
           (log/info {:id ::lifecycle-start :data {:action :starting}} "Starting dataset encryption")
           ;; any misconfiguration throws here on purpose — encryption boots correctly or boot fails
           (let [{:keys [source]} (ensure-initialized!)]
             (log/info {:id ::lifecycle-initialized
                        :data {:action :initialized :subject :encryption :source source}}
                       "Dataset encryption initialized")))
  :stop (fn []
          (log/info {:id ::lifecycle-stop :data {:action :stopping}} "Stopping dataset encryption")
          (stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "Dataset encryption stopped"))})
