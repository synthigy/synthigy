(ns synthigy.dataset.encryption
  (:require
    [buddy.core.crypto :as crypto]
    [buddy.core.nonce :as nonce]
    [clojure.data.json :as json]
    clojure.java.io
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia.resolve :as resolve]
    [environ.core :refer [env]]
    [next.jdbc :as jdbc]
    [patcho.lifecycle :as lifecycle]
    [synthigy.dataset.shamir
     :refer [create-shares
             reconstruct-secret]]
    [synthigy.dataset.sql.compose
     :as compose]
    [synthigy.db :as db])
  (:import
    java.math.BigInteger
    [java.util Base64]
    [java.util Random]
    [javax.crypto.spec SecretKeySpec]
    [synthigy.db Postgres SQLite]))

;; Database-agnostic PGobject handling (avoids compile-time dependency)
(defn- pgobject?
  "Check if value is a PostgreSQL PGobject (without compile-time dependency)."
  [data]
  (when data
    (= "org.postgresql.util.PGobject" (.getName (class data)))))

(defn- pgobject-value
  "Get the value of a PGobject using reflection."
  [data]
  (when (pgobject? data)
    (.invoke (.getMethod (class data) "getValue" (into-array Class []))
             data
             (into-array Object []))))

(defn- create-pgobject
  "Create a PGobject for JSONB storage (fails gracefully if Postgres driver not available)."
  [json-str]
  (try
    (let [pg-class (Class/forName "org.postgresql.util.PGobject")
          pg-obj (.newInstance pg-class)]
      (.invoke (.getMethod pg-class "setType" (into-array Class [String]))
               pg-obj
               (into-array Object ["jsonb"]))
      (.invoke (.getMethod pg-class "setValue" (into-array Class [String]))
               pg-obj
               (into-array Object [json-str]))
      pg-obj)
    (catch ClassNotFoundException _
      json-str)))

(defn- json-value->str
  "Extracts JSON string from database value.
   Handles both PGobject (Postgres) and String (SQLite)."
  [value]
  (cond
    (nil? value) nil
    (string? value) value
    (pgobject? value) (pgobject-value value)
    :else (str value)))

(defn- str->json-value
  "Converts JSON string to database-appropriate format.
   Uses PGobject for Postgres, plain string for SQLite."
  [json-str]
  (if (instance? Postgres db/*db*)
    (create-pgobject json-str)
    json-str))

(defonce ^:dynamic *master-key* nil)
(defonce ^:dynamic *dek* nil)

(defn random-master [] (.toString (BigInteger. 128 (Random.))))

(defn gen-key
  ([] (gen-key (.toString (BigInteger. 128 (Random.)))))
  ([data]
   (let [bs (take 32
                  (concat
                    (.toByteArray (java.math.BigInteger. data))
                    (repeat 0)))]
     (SecretKeySpec. (byte-array bs) "AES"))))

(defonce deks (atom nil))

(defn get-dek [data]
  (cond
    (number? data) (get @deks data)
    (instance? javax.crypto.spec.SecretKeySpec data) (.getEncoded data)
    :else data))

(defmethod compose/prepare [::dek-exists? Postgres]
  [_]
  ["SELECT to_regclass('public.__deks')"])

(defmethod compose/prepare [::dek-exists? SQLite]
  [_]
  ["SELECT name FROM sqlite_master WHERE type='table' AND name='__deks'"])

(defn dek-table-exists?
  []
  (let [result (first (compose/execute! (compose/prepare ::dek-exists?)))]
    (boolean (or (:to_regclass result) (:name result)))))

(defmethod compose/prepare [::create-dek-table Postgres]
  [_]
  [(str/join
     "\n"
     ["create table __deks("
      "   id SERIAL PRIMARY KEY,"
      "   dek jsonb not null,"
      "   encryption_barrier jsonb,"
      "   key_algorithm VARCHAR(50) not null,"
      "   created_at TIMESTAMP default now(),"
      "   expires_at timestamp,"
      "   active boolean default true"
      ");"])])

(defmethod compose/prepare [::create-dek-table SQLite]
  [_]
  [(str/join
     "\n"
     ["create table __deks("
      "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
      "   dek TEXT not null,"
      "   encryption_barrier TEXT,"
      "   key_algorithm VARCHAR(50) not null,"
      "   created_at TIMESTAMP default CURRENT_TIMESTAMP,"
      "   expires_at timestamp,"
      "   active boolean default true"
      ");"])])

(defn create-dek-table
  []
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute-one! con (compose/prepare ::create-dek-table))))

(defn drop-dek-table
  []
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute-one! con ["drop table if exists \"__deks\""])))

(defn generate-key [key-size]
  (let [key-bytes (nonce/random-bytes (/ key-size 8))] ;; Generate random bytes for key
    (SecretKeySpec. key-bytes "AES")))

(defn encrypt-dek
  [dek]
  (let [key-string (.encodeToString (Base64/getEncoder) (.getEncoded dek))
        iv (nonce/random-bytes 12)
        encrypted (crypto/encrypt
                    (.getBytes key-string "UTF-8")
                    (.getEncoded *master-key*)
                    iv
                    {:alg :aes256-gcm})]
    {:key (.encodeToString (Base64/getEncoder) encrypted)
     :iv (.encodeToString (Base64/getEncoder) iv)}))

(defn decrypt-dek
  [{aes-key :key
    iv :iv}]
  (let [aes-key (.decode (Base64/getDecoder) aes-key)
        iv (.decode (Base64/getDecoder) iv)
        decrypted (crypto/decrypt
                    aes-key
                    (.getEncoded *master-key*)
                    iv
                    {:alg :aes256-gcm})
        decoded (.decode (Base64/getDecoder) decrypted)]
    decoded))

(def ^:private encryption-barrier "this_was_encrypted")

(defn encrypt-data
  [data]
  (if-not *dek*
    (do
      (log/error "Couldn't encrypt data since *dek* isn't specified...")
      (throw
        (ex-info
          "Couldn't encrypt data. Encryptiion is not initialized"
          {:type :encryption/not-initialized})))
    (let [iv (nonce/random-bytes 12)
          current-dek *dek*
          dek (get-dek current-dek)
          encrypted (crypto/encrypt
                      (.getBytes data "UTF-8")
                      dek iv
                      {:alg :aes256-gcm})
          payload {:data (.encodeToString (Base64/getEncoder) encrypted)
                   :dek current-dek
                   :iv (.encodeToString (Base64/getEncoder) iv)}]
      payload)))

(defn decrypt-data
  [{:keys [data dek iv]}]
  (when (and dek iv)
    (let [aes-key (get-dek dek)
          iv (.decode (Base64/getDecoder) iv)
          decrypted (crypto/decrypt
                      (.decode (Base64/getDecoder) data)
                      aes-key
                      iv
                      {:alg :aes256-gcm})]
      (String. decrypted "UTF-8"))))

(defn add-dek-barrier
  [id]
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute-one!
      con
      ["update __deks set encryption_barrier=? where id=?"
       (str->json-value
         (json/write-str
           (binding [*dek* id]
             (encrypt-data encryption-barrier)))) id])))

(defn set-dek-active
  [id]
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute-one!
      con
      ["update __deks set active=false where id!=?" id])))

(defn dek->db
  [dek]
  (let [encrypted (encrypt-dek dek)
        {id :__deks/id} (with-open [con (jdbc/get-connection (:datasource db/*db*))]
                          (jdbc/execute-one!
                            con
                            ["insert into __deks (dek, key_algorithm, active) values (?, ?, ?) returning id"
                             (str->json-value (json/write-str encrypted :key-fn name))
                             "aes256-gcm"
                             true]))]
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

(defn db-deks
  []
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute!
      con
      ["select id,dek,active,encryption_barrier from __deks"])))

(defn init-deks
  []
  (reset! deks nil)
  (try
    ;; If there are some DEK in deks table
    ;; that means that some data has may have been
    ;; encrypted... So we check if master key can
    ;; can decrypt deks and encryption_barrier as well
    (if-let [known-deks (not-empty (db-deks))]
      (reduce
        (fn [r {id :__deks/id
                dek :__deks/dek
                encryption_barrier :__deks/encryption_barrier
                active? :__deks/active}]
          (let [db-dek (json/read-str (json-value->str dek) :key-fn keyword)
                _encryption-barrier (json/read-str (json-value->str encryption_barrier) :key-fn keyword)
                dek (decrypt-dek db-dek)
                _ (swap! deks assoc id dek)
                valid? (= encryption-barrier
                          (try
                            (binding [*dek* dek]
                              (decrypt-data _encryption-barrier))
                            (catch Throwable _
                              (log/errorf "[ENCRYPTION] Couldn't decrypt encryption barrier: %s" id)
                              nil)))]
            (if-not valid? r
                    (do
                      (when active?
                        (alter-var-root #'*dek* (fn [_] id)))
                      (assoc r id dek)))))
        nil
        known-deks)
      ;; If there are no encrypted deks, than create new DEK
      ;; and mark it active...
      (create-dek))
    (catch Throwable ex
      (log/errorf
        ex "[ENCRYPTION] [%s] DEK initialized! Couldn't initialize all DEKs... Master key not valid"
        (count (keys @deks)))
      (reset! deks nil)
      (throw ex))))

(defn initialized? [] (some? *master-key*))

(defn save-master-to-env!
  "Save master key to .env file for persistence.

  Args:
    master - Master key string
    env-path - Path to .env file (default: 'core/.env' or '.env')

  Returns: true if saved, false if already exists"
  ([master] (save-master-to-env! master nil))
  ([master env-path]
   (let [env-file (or env-path
                      (first (filter #(.exists (clojure.java.io/file %))
                                     ["core/.env" ".env"]))
                      "core/.env")
         file (clojure.java.io/file env-file)
         content (if (.exists file) (slurp file) "")
         key-line "SYNTHIGY_ENCRYPTION_MASTER_KEY"]
     (if (str/includes? content key-line)
       (do
         (log/info "[ENCRYPTION] Master key already exists in .env")
         false)
       (do
         (spit file (str content
                         (when-not (str/ends-with? content "\n") "\n")
                         "\n# Encryption Master Key (auto-generated)\n"
                         key-line "=" master "\n"))
         (log/infof "[ENCRYPTION] Master key saved to %s" env-file)
         true)))))

(defn start
  ([] (start (env :synthigy-encryption-master-key)))
  ([master]
   (when (not-empty master)
     (let [bs (take 32
                    (concat
                      (.toByteArray (java.math.BigInteger. master))
                      (repeat 0)))
           master-key (SecretKeySpec. (byte-array bs) "AES")]
       (when (and (initialized?) (not= master-key *master-key*))
         (throw
           (ex-info
             "Encryption already initialized with different master key!"
             {:master master
              :master/key master-key})))
       (try
         (binding [*master-key* master-key]
           (init-deks))
         (alter-var-root #'*master-key* (fn [_] master-key))
         ;; NOTE: Subscription system not yet ported to Synthigy
         ;; (async/put! dataset/subscription
         ;;             {:topic :encryption/unsealed
         ;;              :master master})
         (catch Throwable ex
           (log/error ex "[ENCRYPTION] Couldn't initialize dataset encryption")
           nil))))))

(defn ensure-initialized!
  "Ensure encryption is initialized, generating and saving master key if needed.

  This is useful for test setup - it will:
  1. Check if already initialized from env var
  2. If not, generate a new master key
  3. Initialize encryption with it
  4. Optionally save to .env for persistence

  Args:
    opts - Optional map:
           :save-to-env? - If true, save generated key to .env (default: true)

  Returns: {:initialized? bool :generated? bool :master string}"
  ([] (ensure-initialized! {}))
  ([{:keys [save-to-env?]
     :or {save-to-env? true}}]
   (if (initialized?)
     {:initialized? true
      :generated? false
      :master nil}
     (let [master (or (env :synthigy-encryption-master-key)
                      (random-master))
           from-env? (some? (env :synthigy-encryption-master-key))]
       (start master)
       (if (initialized?)
         (do
           (when (and save-to-env? (not from-env?))
             (save-master-to-env! master))
           {:initialized? true
            :generated? (not from-env?)
            :master master})
         {:initialized? false
          :generated? false
          :master nil})))))

;;316714828082109243757432512254285214989459387048765934065582062858114433024

(defonce ^:private available-shares (atom nil))

(defn stop
  []
  (doseq [_atom [available-shares deks]]
    (reset! _atom nil))
  (alter-var-root #'*master-key* (fn [_] nil))
  (alter-var-root #'*dek* (fn [_] nil)))

;;; ============================================================================
;;; GraphQL Resolver Functions
;;; ============================================================================

(defn generate-master
  "GraphQL resolver: Generates a new master encryption key

  Returns a master key string that should be stored securely"
  [context args value]
  (try
    (let [master (random-master)]
      (log/info "Generated new master encryption key")
      (resolve/resolve-as master))
    (catch Throwable e
      (log/error e "Failed to generate master key")
      (resolve/resolve-as nil {:message "Failed to generate master key"}))))

(defn generate-shares
  "GraphQL resolver: Generates Shamir secret shares from a master key

  Args:
    shares - number of shares to generate (default 5)
    threshold - minimum shares needed to reconstruct (default 3)

  Returns a list of {:id Int :value String} shares"
  [context {:keys [shares threshold]
            :or {shares 5
                 threshold 3}} value]
  (try
    (let [master (random-master)
          share-data (create-shares master shares threshold)
          formatted-shares (map-indexed
                             (fn [idx share]
                               {:id (inc idx)
                                :value share})
                             share-data)]
      (log/infof "Generated %d Shamir shares (threshold: %d)" shares threshold)
      (resolve/resolve-as formatted-shares))
    (catch Throwable e
      (log/error e "Failed to generate shares")
      (resolve/resolve-as nil {:message "Failed to generate shares"}))))

(defn unseal-with-master
  "GraphQL resolver: Unseals encryption using master key

  Args:
    master - the master encryption key string

  Returns {:status EncryptionStatus :message String}"
  [context {:keys [master]} value]
  (try
    (if (initialized?)
      (resolve/resolve-as
        {:status :ERROR
         :message "Encryption already initialized"})
      (do
        (start master)
        (if (initialized?)
          (do
            (log/info "Encryption unsealed successfully with master key")
            (resolve/resolve-as
              {:status :INITIALIZED
               :message "Encryption unsealed successfully"}))
          (resolve/resolve-as
            {:status :ERROR
             :message "Failed to initialize encryption"}))))
    (catch Throwable e
      (log/error e "Failed to unseal with master key")
      (resolve/resolve-as
        {:status :ERROR
         :message (str "Error: " (.getMessage e))}))))

(defn unseal-with-share
  "GraphQL resolver: Unseals encryption using a Shamir share

  Collects shares until threshold is reached, then reconstructs master key

  Args:
    share - {:id Int :value String} the share to add

  Returns {:status EncryptionStatus :message String}"
  [context {:keys [share]} value]
  (try
    (if (initialized?)
      (resolve/resolve-as
        {:status :ERROR
         :message "Encryption already initialized"})
      (do
        ;; Add share to available shares
        (swap! available-shares conj (:value share))
        (let [share-count (count @available-shares)]
          (log/infof "Received share %d/%d" share-count 3)
          ;; TODO: Make threshold configurable
          (if (>= share-count 3)
            (try
              (let [master (reconstruct-secret @available-shares)]
                (start master)
                (if (initialized?)
                  (do
                    (reset! available-shares nil)
                    (log/info "Encryption unsealed successfully with shares")
                    (resolve/resolve-as
                      {:status :INITIALIZED
                       :message "Encryption unsealed successfully"}))
                  (do
                    (reset! available-shares nil)
                    (resolve/resolve-as
                      {:status :ERROR
                       :message "Failed to initialize encryption"}))))
              (catch Throwable e
                (log/error e "Failed to reconstruct secret from shares")
                (reset! available-shares nil)
                (resolve/resolve-as
                  {:status :ERROR
                   :message "Failed to reconstruct secret"})))
            (resolve/resolve-as
              {:status :WAITING_FOR_MORE_SHARES
               :message (str "Waiting for more shares (" share-count "/3)")})))))
    (catch Throwable e
      (log/error e "Failed to unseal with share")
      (resolve/resolve-as
        {:status :ERROR
         :message (str "Error: " (.getMessage e))}))))

;;; ============================================================================
;;; Utility Functions for Admin/CLI
;;; ============================================================================

(defn unseal-master!
  "Unseal dataset encryption with a master key.

  This is a utility function for CLI/admin usage.

  Args:
    master - Master key string

  Returns:
    {:success boolean :message string}"
  [master]
  (try
    (if (initialized?)
      {:success false
       :message "Encryption already initialized"}
      (do
        (start master)
        (if (initialized?)
          (do
            (log/info "[DATASET.ENCRYPTION] Unsealed with master key")
            {:success true
             :message "Encryption unsealed successfully"})
          {:success false
           :message "Failed to initialize encryption"})))
    (catch Throwable e
      (log/error e "Failed to unseal with master key")
      {:success false
       :message (str "Error: " (.getMessage e))})))

(defn unseal-share!
  "Add a Shamir share to unseal encryption.

  Collects shares until threshold is reached (default: 3).

  Args:
    share - Share string value

  Returns:
    {:success boolean :message string :shares_collected int :shares_needed int}"
  [share]
  (try
    (if (initialized?)
      {:success false
       :message "Encryption already initialized"}
      (do
        (swap! available-shares conj share)
        (let [share-count (count @available-shares)
              threshold 3] ; TODO: Make configurable
          (if (>= share-count threshold)
            (try
              (let [master (reconstruct-secret @available-shares)]
                (start master)
                (if (initialized?)
                  (do
                    (reset! available-shares nil)
                    (log/info "[DATASET.ENCRYPTION] Unsealed with Shamir shares")
                    {:success true
                     :message "Encryption unsealed successfully"
                     :shares_collected threshold
                     :shares_needed threshold})
                  (do
                    (reset! available-shares nil)
                    {:success false
                     :message "Failed to reconstruct master key"
                     :shares_collected share-count
                     :shares_needed threshold})))
              (catch Throwable e
                (log/error e "Failed to reconstruct secret from shares")
                (reset! available-shares nil)
                {:success false
                 :message "Failed to reconstruct secret from shares"
                 :shares_collected share-count
                 :shares_needed threshold}))
            {:success false
             :message (str "Waiting for more shares (" share-count "/" threshold ")")
             :shares_collected share-count
             :shares_needed threshold}))))
    (catch Throwable e
      (log/error e "Failed to process share")
      {:success false
       :message (str "Error: " (.getMessage e))})))

(defn encryption-status
  "Get current encryption status.

  Returns:
    {:initialized boolean :deks_count int :master_key_present boolean}"
  []
  {:initialized (initialized?)
   :deks_count (count (keys @deks))
   :master_key_present (some? *master-key*)})

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy.dataset/encryption
  {:start (fn []
            (log/info "[DATASET.ENCRYPTION] Starting dataset encryption...")
            ;; Try to initialize from environment variable
            (when-let [master (env :synthigy-encryption-master-key)]
              (log/info "[DATASET.ENCRYPTION] Master key found in environment")
              (try
                (start master)
                (when (initialized?)
                  (log/info "[DATASET.ENCRYPTION] Dataset encryption initialized"))
                (catch Throwable ex
                  (log/error ex "[DATASET.ENCRYPTION] Failed to initialize from environment"))))
            ;; Log status regardless of initialization
            (if (initialized?)
              (log/info "[DATASET.ENCRYPTION] Encryption ready")
              (log/warn "[DATASET.ENCRYPTION] Encryption not initialized - use admin API to unseal")))
   :stop (fn []
           (log/info "[DATASET.ENCRYPTION] Stopping dataset encryption...")
           (stop)
           (log/info "[DATASET.ENCRYPTION] Dataset encryption stopped"))})
