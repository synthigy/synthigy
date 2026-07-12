(ns synthigy.iam.connector
  "Pluggable credential-verification chain for OAuth login.

  Two layers:

  1. **Multimethod** `verify-credentials` dispatches on a connector's `:type`
     keyword. Built-ins are `:database` and `:webhook`. Clojure embedders extend
     the dispatch by adding their own `defmethod`:

     ```
     (defmethod synthigy.iam.connector/verify-credentials :acme/legacy-mainframe
       [{:keys [endpoint]} {:keys [username password]}]
       (when (mainframe/check endpoint username password)
         {:ok true :user (mainframe/lookup username)}))
     ```

  2. **Protocol** `CredentialsProvider` abstracts how connectors are persisted
     and refreshed. Each database backend (Postgres, SQLite) ships a record
     implementation that is wired into `*credentials-provider*` at lifecycle
     start via `set-credentials-provider!`. A `MemoryCredentialsProvider` is
     included for tests and REPL.

  ## Connector contract

  Every `defmethod verify-credentials` receives `[connector creds]` and must
  return one of:

      {:ok true :user CLAIMS}
      {:ok false :reason :unknown-user}        ; not in this backend → try next
      {:ok false :reason :invalid-credentials} ; wrong password → stop, deny
      {:ok false :reason :error :error e}      ; backend unreachable → fail-closed
      nil                                      ; equivalent to :unknown-user

  CLAIMS is a map describing the authenticated user. Required field:

      :name            ; the username — used to look up / JIT-create the
                       ; local user row. This MUST match what Synthigy uses
                       ; as the user's primary identifier.

  Optional claims (informational today; framework may auto-sync in future
  versions — feel free to include them so the contract is forward-compatible):

      :email           ; user's email address
      :given_name      ; first name
      :family_name     ; last name
      :avatar          ; not currently forwarded to JIT (see below)
      :type            ; defaults to :PERSON if absent
      :groups          ; collection of group identifiers
      :roles           ; collection of role identifiers

  Plus any additional claims your connector wants to round-trip.

  After a `:ok true` response the framework looks up the local user by
  `:name`. If missing, it JIT-creates a record via `dataset/sync-entity`
  using only `:name`, `:active true`, and `:type`. The value returned to
  OAuth is always the LOCAL row (with `:_eid`, `:euuid`, audit fields,
  etc.) — ready for session creation. Roles and groups are NOT
  auto-applied yet; operators wire those up downstream of JIT.

  ## Webhook JSON shape

  The `:webhook` connector POSTs to `:url`:

      {\"username\":\"alice\",\"password\":\"...\",\"request_id\":\"<uuid>\"}

  HMAC-SHA256 signed in `X-Synthigy-Signature` when `:secret` is set. The
  webhook responds with JSON mirroring the defmethod contract:

      {\"ok\": true, \"user\": {\"name\": \"alice\", \"email\": \"a@b\"}}
      {\"ok\": false, \"reason\": \"unknown_user\"}
      {\"ok\": false, \"reason\": \"invalid_credentials\"}

  Network errors / timeouts / 5xx are treated as `:error` — the chain
  stops (fail-closed). 4xx with non-conforming body is also `:error`.

  ## Security

  - Plaintext passwords leave Synthigy only over HTTPS to the webhook URL.
    The webhook contract assumes the operator controls both ends.
  - HMAC signing protects against tampering / replay; verify it on the
    webhook side and reject mismatches.
  - On error/timeout the chain stops — prevents an attacker DoSing the
    webhook to bypass it and fall through to local DB."
  (:require
   [synthigy.log :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.json :as json])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.nio.charset StandardCharsets]
   [java.time Duration]
   [java.util Base64]
   [javax.crypto Mac]
   [javax.crypto.spec SecretKeySpec]))

;; =============================================================================
;; Multimethod — open dispatch on connector :type
;; =============================================================================

(defmulti verify-credentials
  "Verify {:username :password} against a backend described by `connector`.
   Dispatch is on `(:type connector)`. See ns docstring for the return contract."
  (fn [connector _creds] (:type connector)))

(defmethod verify-credentials :default
  [connector _]
  (log/warn {:id ::unknown-connector-type
             :data {:action :rejected
                    :subject :iam-connector
                    :reason :unknown-connector-type
                    :type (:type connector)}}
            "Unknown connector type — failing closed")
  {:ok false :reason :error :error :unknown-connector-type})

;; =============================================================================
;; Built-in: local database
;; =============================================================================

(defmethod verify-credentials :database
  [_ {:keys [username password]}]
  (let [{db-password :password
         active :active
         :as user} (iam/get-user-details username)]
    (cond
      (nil? user)
      {:ok false :reason :unknown-user}

      (not active)
      {:ok false :reason :invalid-credentials}

      (iam/validate-password password db-password)
      {:ok true :user (dissoc user :password)}

      :else
      {:ok false :reason :invalid-credentials})))

;; =============================================================================
;; Built-in: HTTP webhook (the "external small service" pattern)
;; =============================================================================

(defn- hmac-sha256
  "Returns base64-encoded HMAC-SHA256 of `payload` (string) under `secret`."
  [secret payload]
  (let [mac (Mac/getInstance "HmacSHA256")
        key-spec (SecretKeySpec. (.getBytes ^String secret StandardCharsets/UTF_8) "HmacSHA256")]
    (.init mac key-spec)
    (->> (.getBytes ^String payload StandardCharsets/UTF_8)
         (.doFinal mac)
         (.encodeToString (Base64/getEncoder)))))

(defn- http-post-json
  "Synchronous JSON POST. Returns {:status :body} on a 2xx/4xx response, throws
   on network failure / timeout / 5xx."
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
      (throw (ex-info "Webhook 5xx" {:status status :body (.body resp)})))
    {:status status :body (.body resp)}))

(defmethod verify-credentials :webhook
  [{:keys [url secret timeout-ms]} {:keys [username password]}]
  (let [request-id (str (java.util.UUID/randomUUID))
        payload (json/->json {:username username
                              :password password
                              :request_id request-id})
        signature (when secret (str "hmac-sha256=" (hmac-sha256 secret payload)))]
    (try
      (let [{:keys [status body]} (http-post-json
                                   {:url url
                                    :body payload
                                    :timeout-ms timeout-ms
                                    :headers (cond-> {}
                                               signature (assoc "X-Synthigy-Signature" signature))})
            parsed (when (seq body) (try (json/read-str body) (catch Throwable _ nil)))]
        (cond
          (and (= 200 status) (:ok parsed))
          {:ok true :user (:user parsed)}

          (and (= 200 status) (false? (:ok parsed)))
          (case (some-> parsed :reason keyword)
            :unknown-user        {:ok false :reason :unknown-user}
            :invalid-credentials {:ok false :reason :invalid-credentials}
            ;; locked / expired / anything else → invalid-credentials (stop chain)
            {:ok false :reason :invalid-credentials})

          :else
          (do
            (log/warn {:id ::webhook-bad-response
                       :data {:action :webhook-bad-response
                              :subject :iam-connector
                              :request-id request-id
                              :status status
                              :body-byte-length (count (str body))
                              :body-preview (some-> body str (#(subs % 0 (min 200 (count %)))))}}
                      "Webhook returned unexpected response")
            {:ok false :reason :error :error :webhook-bad-response})))
      (catch Throwable ex
        (log/error! {:id ::webhook-call-failed
                     :msg "Webhook call failed"
                     :data {:action :webhook-failed
                            :subject :iam-connector
                            :request-id request-id}}
                    ex)
        {:ok false :reason :error :error (or (ex-message ex) "webhook-failed")}))))

;; =============================================================================
;; Chain runner — controls execution order
;; =============================================================================

(defn- normalize-connector
  "Coerce a connector map (which may have come from JSON env config with string
   keys/values) into the keyword shape the multimethod expects."
  [connector]
  (let [c (if (map? connector) connector {})
        with-kw-keys (into {} (map (fn [[k v]] [(if (string? k) (keyword k) k) v]) c))]
    (update with-kw-keys :type #(if (keyword? %) % (keyword %)))))

(defn run-chain
  "Walk `connectors` in order, calling `verify-credentials` on each until one
   resolves the auth attempt. Returns the resolving response, or nil if every
   connector reported `:unknown-user`.

   Chain semantics:
   - First `:ok true`           → return immediately (auth successful)
   - First `:invalid-credentials` → return immediately (deny — wrong password)
   - First `:error`             → return immediately (deny — fail-closed)
   - `:unknown-user` / nil      → try the next connector"
  [connectors creds]
  (loop [[c & more] (map normalize-connector connectors)]
    (when c
      (let [result (verify-credentials c creds)
            reason (:reason result)]
        (cond
          (:ok result) result
          (= reason :invalid-credentials) result
          (= reason :error) result
          ;; nil result or :unknown-user → fall through
          :else (recur more))))))

;; =============================================================================
;; CredentialsProvider protocol — pluggable per database backend
;; =============================================================================

(defprotocol CredentialsProvider
  "Storage backend for the connector chain. Each database backend (Postgres,
   SQLite, ...) ships its own record. The active provider is set on the
   `*credentials-provider*` dynamic var via `set-credentials-provider!`.

   Methods are prefixed `-` to mark them internal — call the sugar fns below
   in normal code (`(list-chain)`, `(save-connector! …)`, etc.)."

  (-list-chain [this]
    "Return the ordered, enabled connector chain. Sorted by :priority asc.
     Each entry is a normalised connector map ready to feed `run-chain`.")

  (-find-connector [this id]
    "Fetch one connector by xid. Returns nil if not found.")

  (-save-connector! [this connector]
    "Insert (when no id) or update. Triggers refresh and emits
     :iam.connector/changed. Returns the persisted entity.")

  (-delete-connector! [this id]
    "Remove by id. Triggers refresh and emits :iam.connector/changed.")

  (-refresh! [this]
    "Force the in-memory cache to reload on the next list-chain. Idempotent.
     Called automatically on save!/delete!, plus externally on the
     :iam.connector/changed event from any source (in-process pub/sub,
     Postgres NOTIFY, manual admin trigger).")

  (-start! [this]
    "Provider lifecycle — open connections, install table/triggers, start
     change-listeners. Called by `set-credentials-provider!`.")

  (-stop! [this]
    "Provider lifecycle — close listeners and clean up. Called by
     `set-credentials-provider!` when the provider is replaced or stopped."))

(defonce ^{:dynamic true
           :doc "Active CredentialsProvider. Set via `set-credentials-provider!`
                or rebound via `with-credentials-provider`. Default nil."}
  *credentials-provider*
  nil)

(defn set-credentials-provider!
  "Install `provider` as the global CredentialsProvider. Stops any previous
   provider (best-effort), then starts the new one. Mirrors the
   `synthigy.iam.encryption/set-encryption-provider!` lifecycle."
  [provider]
  (when-let [old *credentials-provider*]
    (try (-stop! old)
         (catch Throwable ex
           (log/error! {:id ::stop-previous-provider-failed
                        :msg "Error stopping previous credentials provider"
                        :data {:action :stop-failed
                               :subject :iam-connector}}
                       ex))))
  (alter-var-root #'*credentials-provider* (constantly provider))
  (try
    (-start! provider)
    (catch Throwable ex
      (log/error! {:id ::start-provider-failed
                   :msg "Failed to start credentials provider"
                   :data {:action :start-failed
                          :subject :iam-connector}}
                  ex)
      (throw ex)))
  nil)

(defmacro with-credentials-provider
  "Temporarily rebind `*credentials-provider*` for the body's dynamic scope.
   Useful in tests."
  [provider & body]
  `(binding [*credentials-provider* ~provider]
     ~@body))

;; =============================================================================
;; Sugar layer — what callers actually use
;; =============================================================================

(defn- require-provider []
  (or *credentials-provider*
      (throw (ex-info "No CredentialsProvider configured. Boot the database backend or call set-credentials-provider!" {}))))

(defn list-chain
  "Ordered enabled chain from the active provider."
  []
  (-list-chain (require-provider)))

(defn find-connector [id]
  (-find-connector (require-provider) id))

(defn save-connector!
  "Persist a connector. New connectors omit :xid (one is generated); updates
   carry the existing :xid."
  [connector]
  (-save-connector! (require-provider) connector))

(defn delete-connector! [id]
  (-delete-connector! (require-provider) id))

(defn refresh!
  "Invalidate the active provider's cache. The next `list-chain` re-reads
   from the underlying store."
  []
  (-refresh! (require-provider)))

(def ^:private default-chain
  "Fallback chain when no CredentialsProvider has been installed yet — runs
   the local-database connector. Preserves Synthigy's pre-provider behaviour
   in tests, embedded usage, and any environment where the DB backend
   lifecycle hasn't booted."
  [{:type :database}])

(defn- ensure-local-user
  "Find or JIT-create the local `:iam/user` row matching the connector's
   claims. Returns the local user (with `:_eid`, `:euuid`, …) ready for
   session creation, or nil if JIT failed.

   Required claim: `:name`.
   Compatible optional claims forwarded to sync-entity: `:type`."
  [claims]
  (let [username (:name claims)]
    (when-not username
      (throw (ex-info "Connector returned :ok with no :name in :user claims"
                      {:claims claims})))
    (or (iam/get-user-details username)
        (try
          (dataset/sync-entity
           :iam/user
           {:name username
            :active true
            :type (or (:type claims) :PERSON)})
          (iam/get-user-details username)
          (catch Throwable ex
            (log/error! {:id ::jit-user-create-failed
                         :msg "JIT-create failed for user"
                         :data {:action :jit-create-failed
                                :subject :iam-connector
                                :username username}}
                        ex)
            nil)))))

(defn authenticate
  "Run the configured chain against `creds` (a map with at least `:username`
   and `:password`). On success, returns the **local** user record (with
   `:_eid`, `:euuid`, audit fields). The framework JIT-creates the row on
   first successful authentication via an external connector — see the
   namespace docstring for the claim contract. Returns nil on any failure
   (deny).

   Falls back to a single `:database` connector when no provider is installed."
  [creds]
  (let [chain (if *credentials-provider*
                (-list-chain *credentials-provider*)
                default-chain)
        result (run-chain chain creds)]
    (when (:ok result)
      (some-> (ensure-local-user (:user result))
              (dissoc :password)))))

;; =============================================================================
;; MemoryCredentialsProvider — in-process default for tests / REPL / single-node
;; =============================================================================

(defrecord MemoryCredentialsProvider [state]
  CredentialsProvider
  (-list-chain [_]
    (->> (vals (:connectors @state))
         (filter :enabled)
         (sort-by (fn [c] [(:priority c 1000) (:xid c "")]))
         (mapv normalize-connector)))

  (-find-connector [_ id]
    (let [m (:connectors @state)]
      (or (get m id)
          (some (fn [c] (when (= id (:xid c)) c))
                (vals m)))))

  (-save-connector! [this connector]
    (let [id (or (:xid connector) (id/generate-xid))
          persisted (assoc connector :xid id)]
      (swap! state assoc-in [:connectors id] persisted)
      (-refresh! this)
      (try (iam/publish :iam.connector/changed {:id id})
           (catch Throwable _))
      persisted))

  (-delete-connector! [this id]
    (swap! state update :connectors dissoc id)
    (-refresh! this)
    (try (iam/publish :iam.connector/changed {:id id})
         (catch Throwable _)))

  (-refresh! [_] nil)  ; no cache; reads always go straight to `state`

  (-start! [_] nil)
  (-stop! [_] (reset! state {:connectors {}})))

(defn make-memory-provider
  "Construct an in-memory CredentialsProvider. Optional `seed` is a list of
   connector maps used to populate the initial state.

   Useful as a default when no DB backend is wired and for tests:
   ```
   (set-credentials-provider!
     (make-memory-provider [{:type :database :priority 1000 :enabled true}]))
   ```"
  ([] (make-memory-provider [{:type :database :priority 1000 :enabled true}]))
  ([seed]
   (let [state (atom {:connectors {}})
         provider (->MemoryCredentialsProvider state)]
     (doseq [c seed] (-save-connector! provider c))
     provider)))
