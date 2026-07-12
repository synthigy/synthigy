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
   [synthigy.dataset.shamir
    :refer [create-shares
            reconstruct-secret]]
   synthigy.dataset
   [synthigy.db.sql :as sql]
   [synthigy.dataset.sql.naming :as naming]
   [synthigy.db :as db])
  (:import
   java.math.BigInteger
   [java.util Base64]
   [javax.crypto.spec SecretKeySpec]
   [synthigy.db Postgres SQLite Cockroach]))

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
   Uses PGobject for Postgres/Cockroach, plain string for SQLite."
  [json-str]
  (if (or (instance? Postgres db/*db*)
          (instance? synthigy.db.Cockroach db/*db*))
    (create-pgobject json-str)
    json-str))

(defonce ^:dynamic *master-key* nil)
(defonce ^:dynamic *dek* nil)

;; Master-key encoding
;; --------------------
;; New masters are emitted as "b64:<base64>" with a full 256-bit random key.
;; Legacy masters (bare decimal strings) are still accepted at parse time so
;; DEKs created under them can still be decrypted. See parse-master-bytes.

(defn- parse-master-bytes
  "Decode a master-key string into a 32-byte AES-256 key.

   Accepts:
     \"hex:<64-hex-chars>\"    — preferred, explicit encoding
     \"b64:<44-base64-chars>\" — preferred, explicit encoding (32 bytes)
     \"<decimal>\"             — LEGACY BigInteger decimal encoding; kept for
                                backward compatibility with DEKs created under
                                the old format. Note: the BigInteger path has
                                quirks (sign-byte shift, right-zero padding)
                                and should be migrated off."
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
    ;; Legacy BigInteger decimal. Preserves compatibility with masters generated
    ;; before the hex/b64 prefix convention. Do not use for new keys.
    (byte-array
     (take 32
           (concat (.toByteArray (BigInteger. s))
                   (repeat 0))))))

(defn random-master
  "Generate a fresh 256-bit master encryption key, base64-encoded with the
   \"b64:\" prefix. Suitable for AES-256 and accepted by parse-master-bytes."
  []
  (let [bytes (byte-array 32)]
    (.nextBytes (java.security.SecureRandom.) bytes)
    (str "b64:" (.encodeToString (Base64/getEncoder) bytes))))

;; NOTE: a `gen-key` helper used to live here. It derived an AES key from
;; `java.util.Random` (a non-cryptographic PRNG) or from zero-padded
;; BigInteger bytes. It was unused in production — removed. New keys come
;; from `generate-key` (CSPRNG) / `random-master`.

(defonce deks (atom nil))

(defn get-dek [data]
  (cond
    (number? data) (get @deks data)
    (instance? javax.crypto.spec.SecretKeySpec data) (.getEncoded data)
    :else data))

(defn- dek-exists-sql
  []
  (if (instance? SQLite db/*db*)
    ["SELECT name FROM sqlite_master WHERE type='table' AND name='__deks'"]
    ;; Postgres + Cockroach: to_regclass (PG-catalog compat).
    ["SELECT to_regclass('public.__deks')"]))

(defn dek-table-exists?
  []
  (let [result (first (sql/execute! (dek-exists-sql)))]
    (boolean (or (:to_regclass result) (:name result)))))

(defn- create-dek-table-sql
  []
  [(str/join
    "\n"
    (if (instance? SQLite db/*db*)
      ["create table __deks("
       "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
       "   dek TEXT not null,"
       "   encryption_barrier TEXT,"
       "   key_algorithm VARCHAR(50) not null,"
       "   created_at TIMESTAMP default CURRENT_TIMESTAMP,"
       "   expires_at timestamp,"
       "   active boolean default true"
       ");"]
      ;; Postgres + Cockroach: SERIAL + jsonb + now() all work on both.
      ["create table __deks("
       "   id SERIAL PRIMARY KEY,"
       "   dek jsonb not null,"
       "   encryption_barrier jsonb,"
       "   key_algorithm VARCHAR(50) not null,"
       "   created_at TIMESTAMP default now(),"
       "   expires_at timestamp,"
       "   active boolean default true"
       ");"]))])

(defn create-dek-table
  []
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute-one! con (create-dek-table-sql))))

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
  "Encrypt a string under the active DEK (AES-256-GCM).

   With `aad` (a string — e.g. \"table|column|xid\") the ciphertext is bound
   to that context: GCM authenticates the AAD into the tag, so decryption
   then *requires* the same `aad` or fails. The envelope is stamped `:v 2`.
   Without `aad` the envelope is the legacy shape (no `:v`) and carries no
   location binding — an `aad`-less cell can be relocated between rows."
  ([data] (encrypt-data data nil))
  ([data aad]
   (if-not *dek*
     (do
       (log/error {:id ::encrypt-no-dek}
                  "Couldn't encrypt data: *dek* isn't specified")
       (throw
        (ex-info
         "Couldn't encrypt data. Encryptiion is not initialized"
         {:type :encryption/not-initialized})))
     (let [iv (nonce/random-bytes 12)
           current-dek *dek*
           dek (get-dek current-dek)
           opts (cond-> {:alg :aes256-gcm}
                  aad (assoc :aad (.getBytes ^String aad "UTF-8")))
           encrypted (crypto/encrypt (.getBytes data "UTF-8") dek iv opts)]
       (cond-> {:data (.encodeToString (Base64/getEncoder) encrypted)
                :dek current-dek
                :iv (.encodeToString (Base64/getEncoder) iv)}
         aad (assoc :v 2))))))

(defn decrypt-data
  "Decrypt an envelope produced by `encrypt-data`.

   A `:v 2` envelope was sealed with location AAD — the same `aad` string
   MUST be supplied or decryption fails (fail-closed). Legacy envelopes
   (no `:v`) were sealed without AAD and decrypt via the 1-arg form."
  ([envelope] (decrypt-data envelope nil))
  ([{:keys [data dek iv v]} aad]
   (when (and dek iv)
     (when (and (= v 2) (nil? aad))
       (throw (ex-info "v2 encrypted envelope requires AAD context to decrypt"
                       {:type :encryption/aad-required})))
     (let [aes-key  (get-dek dek)
           iv-bytes (.decode (Base64/getDecoder) iv)
           opts     (cond-> {:alg :aes256-gcm}
                      (= v 2) (assoc :aad (.getBytes ^String aad "UTF-8")))
           decrypted (crypto/decrypt (.decode (Base64/getDecoder) data)
                                     aes-key iv-bytes opts)]
       (String. decrypted "UTF-8")))))

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
                            (str->json-value (json/write-str encrypted))
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

;; ============================================================================
;; DEK Rotation
;; ============================================================================
;;
;; Admin-triggered (never automatic): generate a fresh DEK under the current
;; master, re-encrypt every cell of every `encrypted`-typed column across the
;; deployed model under the new DEK, and flip the active flag — all in one
;; transaction. On failure, nothing changes.
;;
;; Master rotation is NOT supported. Master key changes require a separate
;; ceremony (decrypt every DEK with old master, re-encrypt with new) that this
;; function does not implement.

(defn- encrypted-targets
  "Walk the deployed model and return a vector of {:entity-name, :table, :column}
   maps for every attribute typed \"encrypted\". Empty when no encrypted columns
   exist — rotation then touches only the __deks table (metadata-only flip)."
  [model]
  (vec
   (for [e (vals (:entities model))
         a (:attributes e)
         :when (= "encrypted" (:type a))]
     {:entity-name (:name e)
      :table       (naming/entity->table-name e)
      :column      (naming/normalize-name (:name a))})))

(defn- rewrap-cell
  "Decrypt the JSON envelope with `source-bytes`, re-encrypt with `target-bytes`,
   stamp the new DEK id. Returns the new JSON envelope string."
  [cell-value source-bytes new-id target-bytes]
  (let [{:keys [data iv]} (json/read-str (json-value->str cell-value))
        cipher   (.decode (Base64/getDecoder) ^String data)
        iv-bytes (.decode (Base64/getDecoder) ^String iv)
        plain    (crypto/decrypt cipher source-bytes iv-bytes {:alg :aes256-gcm})
        new-iv   (nonce/random-bytes 12)
        new-ct   (crypto/encrypt plain target-bytes new-iv {:alg :aes256-gcm})]
    (json/write-str
     {:data (.encodeToString (Base64/getEncoder) ^bytes new-ct)
      :dek  new-id
      :iv   (.encodeToString (Base64/getEncoder) ^bytes new-iv)})))

(defn- rewrap-column!
  "Re-encrypt every non-null cell in `table`.`column` under the new DEK.
   Runs inside the caller's transaction. Returns count of cells rewrapped.

   Cells referencing other (historical) DEKs are decrypted using whatever bytes
   are present in the `deks` atom for that id; a missing id throws and aborts
   the whole rotation (as intended — you don't silently lose cells)."
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
              ;; Peek at the envelope's dek id to look up the right source key
             {src-dek :dek} (json/read-str (json-value->str cell))
             src-bytes (or (get-dek src-dek)
                           (throw (ex-info "Cannot rotate: source DEK bytes not in memory"
                                           {:code      :source-dek-missing
                                            :cell-dek  src-dek
                                            :table     table
                                            :column    column
                                            :eid       eid})))
             new-env (rewrap-cell cell src-bytes new-id new-bytes)]
         (jdbc/execute-one!
          tx
          [(format "UPDATE \"%s\" SET \"%s\" = ? WHERE _eid = ?" table column)
           (str->json-value new-env)
           eid])
         (inc n)))
     0
     rows)))

(defn initialized? [] (some? *master-key*))

(defn rotate-dek!
  "Atomically rotate the Data Encryption Key.

   Encryption must already be initialized. A fresh DEK is generated under the
   current master, every `encrypted`-typed cell in the deployed model is
   re-encrypted under it, and the active flag flips — all in one transaction.
   On any failure, nothing changes.

   The old DEK row is NOT deleted: it stays in __deks with active=false so
   that any row missed by rotation (shouldn't happen, but defence in depth)
   remains decryptable. Pruning is a separate manual operation.

   Returns:
     {:old-dek-id N
      :new-dek-id M
      :columns [[entity-name column rewrapped-count] ...]
      :total-rewrapped K
      :duration-ms D}"
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
              (let [;; 1. Insert new DEK row (inactive until end)
                    encrypted-new (encrypt-dek new-dek)
                    {new-id :__deks/id}
                    (jdbc/execute-one!
                     tx
                     ["insert into __deks (dek, key_algorithm, active) values (?, ?, ?) returning id"
                      (str->json-value (json/write-str encrypted-new))
                      "aes256-gcm"
                      false])
                    ;; 2. Encryption barrier under new DEK
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
                        (str->json-value (json/write-str b-env))
                        new-id])
                    ;; 3. Re-wrap every cell in every encrypted column.
                    ;;    `deks` atom must contain source DEK bytes — it does,
                    ;;    loaded by init-deks at startup.
                    per-column
                    (mapv
                     (fn [{:keys [entity-name table column]}]
                       [entity-name column
                        (rewrap-column! tx table column new-id new-dek-bytes)])
                     targets)
                    ;; 4. Atomic active-flag flip
                    _ (jdbc/execute-one! tx ["update __deks set active=false where id!=?" new-id])
                    _ (jdbc/execute-one! tx ["update __deks set active=true where id=?" new-id])]
                {:old-dek-id      old-id
                 :new-dek-id      new-id
                 :columns         per-column
                 :total-rewrapped (reduce + 0 (map #(nth % 2) per-column))}))]
        ;; Commit succeeded — update in-memory state. No need to unwind
        ;; anything on failure; with-transaction rolls back automatically.
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
  (with-open [con (jdbc/get-connection (:datasource db/*db*))]
    (jdbc/execute!
     con
     ["select id,dek,active,encryption_barrier from __deks"])))

(defn- master-key-mismatch?
  "True when `e` (or a cause in its chain) is the AES-GCM auth-tag validation
   failure buddy throws when a key can't decrypt the ciphertext — i.e. the
   configured master key doesn't match the DEKs sealed in this database. This
   is an expected operational state (`:configured-not-initialized`), not a code
   error: the operator supplied the wrong key, or the DB was sealed elsewhere."
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
  ;; Check if __deks table exists - if not, create it and initial DEK
  (if-not (dek-table-exists?)
    (do
      (log/info {:id ::deks-table-missing}
                "No __deks table found; creating table and first DEK")
      (create-dek-table)
      (create-dek))
    ;; Table exists - try to load existing DEKs
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
           (let [db-dek (json/read-str (json-value->str dek))
                 _encryption-barrier (json/read-str (json-value->str encryption_barrier))
                 dek (decrypt-dek db-dek)
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
        ;; If there are no encrypted deks, than create new DEK
        ;; and mark it active...
        (create-dek))
      (catch Throwable ex
        ;; A master-key mismatch is an expected sealed state — don't ERROR-log
        ;; here; `start` raises a single actionable WARN. Genuine failures
        ;; (corrupt DEK JSON, DB errors) keep their ERROR + stacktrace.
        (when-not (master-key-mismatch? ex)
          (log/error! {:id ::dek-init-failed
                       :msg "Couldn't initialize all DEKs"
                       :data {:action :starting :subject :dek
                              :initialized-count (count (keys @deks))}}
                      ex))
        (reset! deks nil)
        (throw ex)))))

(defn encryption-required?
  "Check if encryption is required (DEK table exists with data).
  Returns true if __deks table exists and has entries, meaning
  encrypted data may exist in the database."
  []
  (and (dek-table-exists?)
       (not-empty (db-deks))))

(defn encryption-state
  "Get detailed encryption state.
  Returns one of:
    :not-configured - No __deks table, encryption not set up
    :configured-not-initialized - __deks exists but master key invalid/missing
    :initialized - Encryption ready to use"
  []
  (cond
    (initialized?) :initialized
    (encryption-required?) :configured-not-initialized
    :else :not-configured))

;; NOTE: a `save-master-to-env!` helper used to live here — it wrote the
;; master key in plaintext into a `.env` file. Removed: the master key is
;; the keys-to-the-kingdom and the platform must never persist it to disk on
;; its own. Supplying/storing the master is the operator's responsibility
;; (env var / secrets manager).

(defn start
  ([] (start (env :synthigy-encryption-master-key)))
  ([master]
   (when (not-empty master)
     (let [bs (parse-master-bytes master)
           master-key (SecretKeySpec. bs "AES")]
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
           (if (master-key-mismatch? ex)
             ;; Expected: the configured master can't decrypt the stored DEKs.
             ;; Seal cleanly with one actionable WARN — no crash, no stacktrace.
             (log/warn {:id ::master-key-mismatch
                        :data {:action :not-initialized :subject :encryption
                               :deks-count (try (count (db-deks)) (catch Throwable _ nil))}}
                       (str "Configured master key can't decrypt existing DEKs — "
                            "encryption left sealed. Unseal with the correct master "
                            "via the admin API, or this database was sealed under a "
                            "different key."))
             (log/error! {:id ::start-init-failed
                          :msg "Couldn't initialize dataset encryption"}
                         ex))
           nil))))))

(defn ensure-initialized!
  "Ensure encryption is initialized: initialize from the env master key, or
  generate a fresh one if none is set.

  A generated master key is returned to the caller (the `:master` field) and
  is NEVER written to disk — persisting it is the operator's responsibility
  (env var / secrets manager). A generated master not captured by the caller
  is lost on restart, taking any DEKs created under it with it.

  Returns: {:initialized? bool :generated? bool :master string}"
  []
  (if (initialized?)
    {:initialized? true
     :generated? false
     :master nil}
    (let [from-env? (some? (env :synthigy-encryption-master-key))
          master    (or (env :synthigy-encryption-master-key)
                        (random-master))]
      (start master)
      (if (initialized?)
        {:initialized? true
         :generated? (not from-env?)
         :master master}
        {:initialized? false
         :generated? false
         :master nil}))))

;;316714828082109243757432512254285214989459387048765934065582062858114433024

(defonce ^:private available-shares (atom nil))

(defn stop
  []
  (doseq [_atom [available-shares deks]]
    (reset! _atom nil))
  (alter-var-root #'*master-key* (fn [_] nil))
  (alter-var-root #'*dek* (fn [_] nil)))

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
            (log/info {:id ::admin-unsealed-with-master}
                      "Unsealed dataset encryption with master key")
            {:success true
             :message "Encryption unsealed successfully"})
          {:success false
           :message "Failed to initialize encryption"})))
    (catch Throwable e
      (log/error! {:id ::admin-unseal-with-master-failed
                   :msg "Failed to unseal with master key"} e)
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
                    (log/info {:id ::admin-unsealed-with-shares
                               :data {:shares threshold}}
                              "Unsealed dataset encryption with Shamir shares")
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
                (log/error! {:id ::reconstruct-secret-failed
                             :msg "Failed to reconstruct secret from shares"} e)
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
      (log/error! {:id ::process-share-failed
                   :msg "Failed to process share"} e)
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
 {:doc "key vault, zero deps"
  :start (fn []
           (log/info {:id ::lifecycle-start :data {:action :starting}} "Starting dataset encryption")
            ;; Try to initialize from environment variable
           (when-let [master (env :synthigy-encryption-master-key)]
             (log/info {:id ::env-master-key-found}
                       "Master key found in environment")
             (try
               (start master)
               (when (initialized?)
                 (log/info {:id ::lifecycle-initialized :data {:action :initialized}}
                           "Dataset encryption initialized"))
               (catch Throwable ex
                 (log/error! {:id ::env-init-failed
                              :msg "Failed to initialize encryption from environment"}
                             ex))))
            ;; Log status regardless of initialization
           (if (initialized?)
             (log/info {:id ::lifecycle-ready :data {:action :ready}} "Encryption ready")
             (log/warn {:id ::lifecycle-not-initialized :data {:action :not-initialized}}
                       "Encryption not initialized; use admin API to unseal")))
  :stop (fn []
          (log/info {:id ::lifecycle-stop :data {:action :stopping}} "Stopping dataset encryption")
          (stop)
          (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "Dataset encryption stopped"))})
