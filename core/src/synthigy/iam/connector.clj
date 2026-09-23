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

(ns synthigy.iam.connector
  "Pluggable credential-verification chain for OAuth login: multimethod
   `verify-credentials` dispatches on connector `:type` (built-ins `:database`,
   `:webhook`; embedders add their own defmethod), backed by the
   `CredentialsProvider` protocol for persistence/refresh. See docs for the
   connector return contract and webhook JSON shape."
  (:require
   [synthigy.log :as log]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.context :as iam.context]
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
  "Verify {:username :password} against a backend described by `connector`;
   dispatches on `(:type connector)`."
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
  ;; raw fetch, not iam.context/get-user-details — that strips :password, but we
  ;; need the hash
  (let [{db-password :password
         active :active
         :as user} (dataset/get-entity :iam/user {:name username}
                                       {:name nil :password nil :active nil})]
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
  "Synchronous JSON POST; returns {:status :body} on 2xx/4xx, throws on network
   failure/timeout/5xx."
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

;; HMAC-SHA256-signs the payload in X-Synthigy-Signature when :secret is set —
;; verify it webhook-side.
;; Network errors / timeouts / 5xx deny (fail-closed) rather than falling
;; through to local DB.
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
            ;; locked / expired / anything else → invalid-credentials (stop
            ;; chain)
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

(defn normalize-connector
  "Coerce a connector map (may come from JSON/env config with string
   keys/values) into the keyword shape the multimethod expects."
  [connector]
  (let [c (if (map? connector) connector {})
        with-kw-keys (into {} (map (fn [[k v]] [(if (string? k) (keyword k) k) v]) c))]
    (update with-kw-keys :type #(if (keyword? %) % (keyword %)))))

(defn run-chain
  "Walk `connectors` in order; first :ok wins, first :invalid-credentials or
   :error denies immediately (fail-closed), :unknown-user/nil tries the next
   connector."
  [connectors creds]
  (loop [[c & more] (map normalize-connector connectors)]
    (when c
      (let [result (verify-credentials c creds)
            reason (:reason result)]
        (cond
          (:ok result) result
          (= reason :invalid-credentials) result
          (= reason :error) result
          :else (recur more))))))

;; =============================================================================
;; CredentialsProvider protocol — pluggable per database backend
;; =============================================================================

(defprotocol CredentialsProvider
  "Storage backend for the connector chain, bound to `*credentials-provider*`.
   Methods are `-`-prefixed as internal — call the sugar fns below."

  (-list-chain [this]
    "Ordered, enabled connector chain, sorted by :priority asc.")

  (-find-connector [this id]
    "Fetch one connector by xid, or nil.")

  (-save-connector! [this connector]
    "Insert or update; triggers refresh and emits :iam.connector/changed.")

  (-delete-connector! [this id]
    "Remove by id; triggers refresh and emits :iam.connector/changed.")

  (-refresh! [this]
    "Force the in-memory cache to reload on the next list-chain. Idempotent.")

  (-start! [this]
    "Provider lifecycle — open connections, install table/triggers, start change-listeners.")

  (-stop! [this]
    "Provider lifecycle — close listeners and clean up."))

(defonce ^{:dynamic true} *credentials-provider* nil)

(defn set-credentials-provider!
  "Install `provider` as the global CredentialsProvider, stopping any previous
   one first."
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

(defn require-provider []
  (or *credentials-provider*
      (throw (ex-info "No CredentialsProvider configured. Boot the database backend or call set-credentials-provider!" {}))))

(defn list-chain
  "Ordered enabled chain from the active provider."
  []
  (-list-chain (require-provider)))

(defn find-connector
  "One connector by xid, normalized to the keyword shape `verify-credentials`
   dispatches on; nil when not found."
  [id]
  (some-> (-find-connector (require-provider) id) normalize-connector))

(defn save-connector!
  "Persist a connector; new connectors omit :xid (one is generated), updates
   carry the existing :xid."
  [connector]
  (-save-connector! (require-provider) connector))

(defn delete-connector! [id]
  (-delete-connector! (require-provider) id))

(defn refresh!
  "Invalidate the active provider's cache; the next `list-chain` re-reads from
   the underlying store."
  []
  (-refresh! (require-provider)))

(def ^:private default-chain
  "Fallback chain when no CredentialsProvider is installed — runs the local-database connector."
  [{:type :database}])

(defn ensure-local-user
  "Find or JIT-create the local `:iam/user` row for the connector's claims
   (requires `:name`); nil if JIT failed."
  [claims]
  (let [username (:name claims)]
    (when-not username
      (throw (ex-info "Connector returned :ok with no :name in :user claims"
                      {:claims claims})))
    (or (iam.context/get-user-details {:name username})
        (try
          (dataset/sync-entity
           :iam/user
           {:name username
            :active true
            :type (or (:type claims) :PERSON)})
          (iam.context/get-user-details {:name username})
          (catch Throwable ex
            (log/error! {:id ::jit-user-create-failed
                         :msg "JIT-create failed for user"
                         :data {:action :jit-create-failed
                                :subject :iam-connector
                                :username username}}
                        ex)
            nil)))))

(defn authenticate
  "Run the configured chain against `creds`; on success returns the local user
   record (JIT-created on first login via an external connector), nil on any
   failure."
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
      persisted))

  (-delete-connector! [this id]
    (swap! state update :connectors dissoc id)
    (-refresh! this))

  (-refresh! [_] nil)  ; no cache; reads always go straight to `state`

  (-start! [_] nil)
  (-stop! [_] (reset! state {:connectors {}})))

(defn make-memory-provider
  "Construct an in-memory CredentialsProvider; optional `seed` populates the
   initial state."
  ([] (make-memory-provider [{:type :database :priority 1000 :enabled true}]))
  ([seed]
   (let [state (atom {:connectors {}})
         provider (->MemoryCredentialsProvider state)]
     (doseq [c seed] (-save-connector! provider c))
     provider)))
