(ns synthigy.admin.core
  "Pure Ring handlers for admin API.

  Provides localhost-only administration endpoints:
  - System information
  - Encryption management
  - Superuser management
  - System initialization
  - Health checks
  - Graceful shutdown
  - Optional service management (GraphQL, Frontend)"
  (:require
    [clojure.string :as str]
    [synthigy.log :as log]
    [synthigy.log.config :as log-config]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]
    [synthigy.data :as data]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]
    [synthigy.iam.connector :as connector]
    [synthigy.json :as json]))

;;; ============================================================================
;;; Response Helpers
;;; ============================================================================

(defn json-response
  "Create a JSON HTTP response."
  ([data] (json-response 200 data))
  ([status data]
   {:status status
    :headers {"Content-Type" "application/json"}
    :body (json/write-str data)}))

(defn error-response
  "Create an error JSON response."
  ([message] (error-response 400 message))
  ([status message]
   (json-response status {:error message})))

;;; ============================================================================
;;; Handler Functions
;;; ============================================================================

(defn handle-system-info
  "GET /__admin/info - Get system information."
  [_request]
  (try
    (let [enc-status (dataset-encryption/encryption-status)]
      (json-response
        {:version "0.1.0"
         :encryption enc-status
         :message "Synthigy admin service"}))
    (catch Exception e
      (log/error! {:id ::system-info-failed
                   :msg "Error getting system info"} e)
      (error-response 500 "Internal server error"))))

(defn handle-encryption-status
  "GET /__admin/encryption/status - Get encryption status."
  [_request]
  (try
    (json-response (dataset-encryption/encryption-status))
    (catch Exception e
      (log/error! {:id ::encryption-status-failed
                   :msg "Error getting encryption status"} e)
      (error-response 500 "Internal server error"))))

(defn handle-unseal-master
  "POST /__admin/encryption/unseal - Unseal with master key.

  Body: {\"master\": \"<master-key-string>\"}"
  [request]
  (try
    (let [body (when-let [b (:body request)]
                 (json/read-str (slurp b)))
          master (:master body)]
      (if-not master
        (error-response "Missing 'master' field in request body")
        (let [result (dataset-encryption/unseal-master! master)]
          (if (:success result)
            (json-response result)
            (error-response 400 (:message result))))))
    (catch Exception e
      (log/error! {:id ::unseal-master-failed
                   :msg "Error unsealing with master key"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-unseal-share
  "POST /__admin/encryption/unseal-share - Add Shamir share.

  Body: {\"share\": \"<share-string>\"}"
  [request]
  (try
    (let [body (when-let [b (:body request)]
                 (json/read-str (slurp b)))
          share (:share body)]
      (if-not share
        (error-response "Missing 'share' field in request body")
        (let [result (dataset-encryption/unseal-share! share)]
          (if (:success result)
            (json-response result)
            (json-response 202 result))))) ; 202 Accepted for partial progress
    (catch Exception e
      (log/error! {:id ::unseal-share-failed
                   :msg "Error processing Shamir share"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-add-superuser
  "POST /__admin/superuser - Add or update superuser.

  Body: {\"username\": \"<username>\", \"password\": \"<password>\"}"
  [request]
  (try
    ;; Check if IAM is started
    (when-not (patcho.lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started})))

    (let [body (when-let [b (:body request)]
                 (json/read-str (slurp b)))
          {:keys [username password]} body]
      (if (or (str/blank? username) (str/blank? password))
        (error-response "Missing 'username' or 'password' in request body")
        (do
          (iam/set-user {:name username
                         :password password
                         :active true
                         :roles [data/*ROOT*]})
          (json-response {:success true
                          :message (str "Superuser '" username "' created/updated")}))))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 "IAM service not started. Run: synthigy init iam")
        (do
          (log/error! {:id ::add-superuser-failed
                       :msg "Error adding superuser"} e)
          (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::add-superuser-failed
                   :msg "Error adding superuser"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-list-superusers
  "GET /__admin/superuser - List all superusers."
  [_request]
  (try
    ;; Check if dataset and IAM are started
    (when-not (patcho.lifecycle/started? :synthigy/dataset)
      (throw (ex-info "Dataset not started" {:type :service-not-started
                                             :service :dataset})))
    (when-not (patcho.lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started
                                         :service :iam})))

    (let [{users :users} (dataset/get-entity
                           :iam/user-role
                           {(id/key) (id/extract synthigy.data/*ROOT*)}
                           {(id/key) nil
                            :users [{:selections {:name nil
                                                  :active nil}}]})]
      (json-response {:superusers (mapv :name users)}))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (let [service (:service (ex-data e))]
          (error-response 503 (str (name service) " service not started. Run: synthigy init " (name service))))
        (do
          (log/error! {:id ::list-superusers-failed
                       :msg "Error listing superusers"} e)
          (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::list-superusers-failed
                   :msg "Error listing superusers"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-init
  "POST /__admin/init - Initialize system (create tables, deploy schemas).

   This is ONE-TIME setup that creates database structure.
   Idempotent - safe to call multiple times (lifecycle tracks setup-complete?)."
  [_request]
  (try
    (log/info {:id ::init-start} "Running system initialization (lifecycle/setup!)")

    ;; Setup in dependency order (lifecycle handles this automatically)
    ;; This will: create database, create dataset tables, create IAM tables
    (patcho.lifecycle/setup! :synthigy/iam)

    (json-response {:success true
                    :message "System initialized"
                    :modules [:synthigy/database
                              :synthigy/dataset
                              :synthigy/iam]})
    (catch Exception e
      (log/error! {:id ::init-failed
                   :msg "Initialization failed"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-doctor
  "GET /__admin/doctor - Run system health checks."
  [_request]
  (try
    (let [dataset-started? (patcho.lifecycle/started? :synthigy/dataset)
          iam-started? (patcho.lifecycle/started? :synthigy/iam)
          encryption-started? (patcho.lifecycle/started? :synthigy.dataset/encryption)
          server-started? (patcho.lifecycle/started? :synthigy/server)

          enc-status (when encryption-started?
                       (try
                         (dataset-encryption/encryption-status)
                         (catch Exception _ nil)))

          db-connected (when (and dataset-started? iam-started?)
                         (try
                           (dataset/get-entity
                             :iam/user
                             {:name "nonexistent"}
                             {(id/key) nil})
                           true
                           (catch Exception _ false)))

          all-ok? (and dataset-started? iam-started? encryption-started?
                       (or (not encryption-started?) (:initialized enc-status))
                       (or (not db-connected) db-connected))]
      (json-response
        {:status (if all-ok? "ok" "degraded")
         :checks {:dataset dataset-started?
                  :iam iam-started?
                  :encryption encryption-started?
                  :encryption_initialized (get enc-status :initialized false)
                  :database (or db-connected false)
                  :server server-started?}}))
    (catch Exception e
      (log/error! {:id ::doctor-failed
                   :msg "Error running doctor"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-initialize
  "POST /__admin/initialize - Initialize Synthigy system (first-time setup).

  Body: {\"encryption\": {\"master_key\": \"...\", \"shares\": [...]},
         \"superuser\": {\"username\": \"...\", \"password\": \"...\"}}"
  [request]
  (try
    (let [body (when-let [b (:body request)]
                 (json/read-str (slurp b)))
          {:keys [encryption superuser]} body
          master-key (get encryption :master_key)
          shares (get encryption :shares)
          username (get superuser :username)
          password (get superuser :password)
          steps (atom [])]

      ;; Validate request
      (when (and (nil? master-key) (empty? shares))
        (throw (ex-info "Missing encryption credentials: provide master_key or shares"
                        {:type :validation-error})))

      (when (or (str/blank? username) (str/blank? password))
        (throw (ex-info "Missing superuser credentials: provide username and password"
                        {:type :validation-error})))

      ;; Step 1: Check if already initialized
      (let [enc-status (dataset-encryption/encryption-status)]
        (when (and (:initialized enc-status) (not (:sealed enc-status)))
          (swap! steps conj {:name "already-initialized"
                             :status "skipped"
                             :message "System already initialized"})
          (throw (ex-info "System already initialized"
                          {:type :already-initialized
                           :steps @steps}))))

      ;; Step 2: Unseal encryption
      (let [unseal-result (if master-key
                            (dataset-encryption/unseal-master! master-key)
                            (loop [remaining-shares shares
                                   result nil]
                              (if (empty? remaining-shares)
                                result
                                (let [share (first remaining-shares)
                                      r (dataset-encryption/unseal-share! share)]
                                  (if (:success r)
                                    r
                                    (recur (rest remaining-shares) r))))))]
        (if (:success unseal-result)
          (swap! steps conj {:name "encryption"
                             :status "complete"
                             :message "Encryption unsealed successfully"})
          (do
            (swap! steps conj {:name "encryption"
                               :status "failed"
                               :message (:message unseal-result)})
            (throw (ex-info "Failed to unseal encryption"
                            {:type :unseal-failed
                             :details unseal-result
                             :steps @steps})))))

      ;; Step 3: Verify encryption initialized
      (let [enc-status (dataset-encryption/encryption-status)]
        (if (:initialized enc-status)
          (swap! steps conj {:name "encryption-verify"
                             :status "complete"
                             :message "Encryption verified"})
          (do
            (swap! steps conj {:name "encryption-verify"
                               :status "failed"
                               :message "Encryption not initialized"})
            (throw (ex-info "Encryption verification failed"
                            {:type :encryption-not-initialized
                             :steps @steps})))))

      ;; Step 4: Create superuser
      (try
        (iam/set-user {:name username
                       :password password
                       :active true
                       :roles [data/*ROOT*]})
        (swap! steps conj {:name "superuser"
                           :status "complete"
                           :message (str "Superuser '" username "' created")})
        (catch Exception e
          (swap! steps conj {:name "superuser"
                             :status "failed"
                             :message (.getMessage e)})
          (throw (ex-info "Failed to create superuser"
                          {:type :superuser-creation-failed
                           :steps @steps}
                          e))))

      ;; Step 5: Run health checks
      (let [db-check (try
                       (dataset/get-entity
                         :iam/user
                         {:name username}
                         {(id/key) nil})
                       true
                       (catch Exception _ false))
            enc-check (get (dataset-encryption/encryption-status) :initialized)]
        (if (and db-check enc-check)
          (swap! steps conj {:name "health"
                             :status "complete"
                             :message "All health checks passed"})
          (swap! steps conj {:name "health"
                             :status "warning"
                             :message (str "Health checks: database=" db-check
                                           ", encryption=" enc-check)})))

      ;; Return success with all steps
      (json-response {:success true
                      :message "Initialization complete"
                      :steps @steps}))

    (catch clojure.lang.ExceptionInfo e
      (let [data (ex-data e)]
        (case (:type data)
          :validation-error
          (error-response 400 (.getMessage e))

          :already-initialized
          (json-response 200 {:success true
                              :message "System already initialized"
                              :steps (:steps data)})

          (:unseal-failed :encryption-not-initialized :superuser-creation-failed)
          (json-response 500 {:success false
                              :message (.getMessage e)
                              :steps (:steps data)})

          ;; Unknown ExceptionInfo
          (do
            (log/error! {:id ::init-failed
                         :msg "Initialization failed"} e)
            (error-response 500 (.getMessage e))))))

    (catch Exception e
      (log/error! {:id ::init-error
                   :msg "Initialization error"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-server-start
  "POST /__admin/server/start - Start HTTP server.

  Starts the server via lifecycle."
  [_request]
  (try
    (log/info {:id ::server-start-request} "Starting server via admin API")
    (patcho.lifecycle/start! :synthigy/server)

    (json-response {:success true
                    :message "Server started"})
    (catch Exception e
      (log/error! {:id ::server-start-failed
                   :msg "Error starting server"
                   :data {:action :starting :subject :http-server}} e)
      (error-response 500 (.getMessage e)))))

(defn handle-server-stop
  "POST /__admin/server/stop - Stop HTTP server."
  [_request]
  (try
    (log/info {:id ::server-stop-request} "Stopping server via admin API")
    (patcho.lifecycle/stop! :synthigy/server)
    (json-response {:success true
                    :message "Server stopped"})
    (catch Exception e
      (log/error! {:id ::server-stop-failed
                   :msg "Error stopping server"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-server-status
  "GET /__admin/server/status - Get server status."
  [_request]
  (try
    (let [server-started? (lifecycle/started? :synthigy/server)]
      (json-response {:running server-started?
                      :message (if server-started?
                                 "Server is running"
                                 "Server is stopped")}))
    (catch Exception e
      (log/error! {:id ::server-status-failed
                   :msg "Error checking server status"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-shutdown
  "POST /__admin/shutdown - Gracefully shutdown the system."
  [_request]
  (try
    (log/warn {:id ::shutdown-requested} "Shutdown requested via admin API")
    (json-response {:success true
                    :message "Shutdown initiated"})
    ;; Shutdown in background thread to allow response to be sent
    (future
      (Thread/sleep 1000)
      (System/exit 0))
    (catch Exception e
      (log/error! {:id ::shutdown-failed
                   :msg "Error during shutdown"} e)
      (error-response 500 (.getMessage e)))))

;;; ============================================================================
;;; Optional Services
;;; ============================================================================

(defn- try-lifecycle!
  "Attempt a lifecycle operation via requiring-resolve. Returns true on success."
  [op module-key]
  (try
    (require 'patcho.lifecycle)
    (let [lifecycle-fn (requiring-resolve (symbol "patcho.lifecycle" (name op)))]
      (lifecycle-fn module-key)
      true)
    (catch Exception e
      (log/warn {:id ::lifecycle-op-failed
                 :data {:op op :module module-key :error-message (.getMessage e)}}
                "Lifecycle operation failed")
      false)))

;;; ============================================================================
;;; Client Management Handlers
;;; ============================================================================

(defn handle-list-clients
  "GET /__admin/client - List all OAuth clients."
  [_request]
  (try
    (when-not (lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started})))
    (let [clients (iam/list-clients)]
      (json-response
        {:clients (mapv (fn [c]
                          {:id (:id c)
                           :name (:name c)
                           :type (:type c)
                           :active (:active c)
                           :settings (:settings c)})
                        clients)}))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 "IAM service not started")
        (do (log/error! {:id ::list-clients-failed
                         :msg "Error listing clients"} e)
            (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::list-clients-failed
                   :msg "Error listing clients"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-get-client
  "GET /__admin/client/:id - Get a specific OAuth client."
  [_request client-id]
  (try
    (when-not (lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started})))
    (if-let [client (iam/get-client client-id)]
      (json-response {:id (:id client)
                      :name (:name client)
                      :type (:type client)
                      :active (:active client)
                      :settings (:settings client)})
      (error-response 404 (str "Client not found: " client-id)))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 "IAM service not started")
        (do (log/error! {:id ::get-client-failed
                         :msg "Error getting client"} e)
            (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::get-client-failed
                   :msg "Error getting client"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-add-client
  "POST /__admin/client - Create a new OAuth client.

  Body: {\"name\": \"...\", \"type\": \"public|confidential\", \"settings\": {...}}"
  [request]
  (try
    (when-not (lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started})))
    (let [body (when-let [b (:body request)]
                 (json/read-str (slurp b)))
          {:keys [name type settings]} body]
      (if (str/blank? name)
        (error-response "Missing 'name' in request body")
        (let [client (iam/add-client {:name name
                                      :type (keyword (or type "public"))
                                      :settings (or settings {})})]
          (json-response {:success true
                          :message (str "Client '" name "' created")
                          :client {:id (:id client)
                                   :name (:name client)
                                   :type (:type client)
                                   :active (:active client)
                                   :secret (:secret client)
                                   :settings (:settings client)}}))))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 "IAM service not started")
        (do (log/error! {:id ::add-client-failed
                         :msg "Error adding client"} e)
            (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::add-client-failed
                   :msg "Error adding client"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-remove-client
  "DELETE /__admin/client/:id - Remove an OAuth client."
  [_request client-id]
  (try
    (when-not (lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started})))
    (if-let [client (iam/get-client client-id)]
      (do (iam/remove-client client)
          (json-response {:success true
                          :message (str "Client '" client-id "' removed")}))
      (error-response 404 (str "Client not found: " client-id)))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 "IAM service not started")
        (do (log/error! {:id ::remove-client-failed
                         :msg "Error removing client"} e)
            (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::remove-client-failed
                   :msg "Error removing client"} e)
      (error-response 500 (.getMessage e)))))

(defn- update-client-redirections
  "Helper to add/remove redirect URIs from a client's settings."
  [client-id settings-key uri action]
  (try
    (when-not (lifecycle/started? :synthigy/iam)
      (throw (ex-info "IAM not started" {:type :service-not-started})))
    (if-let [client (iam/get-client client-id)]
      (let [settings (or (:settings client) {})
            current (get settings settings-key [])
            updated (case action
                      :add (if (some #{uri} current)
                             current
                             (conj (vec current) uri))
                      :remove (vec (remove #{uri} current)))
            new-settings (assoc settings settings-key updated)]
        (dataset/sync-entity :iam/app
                             {(id/key) (id/extract client)
                              :settings new-settings})
        (json-response {:success true
                        :message (str (name action) " " uri)
                        settings-key updated}))
      (error-response 404 (str "Client not found: " client-id)))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 "IAM service not started")
        (do (log/error! {:id ::update-client-redirections-failed
                         :msg "Error updating client redirections"} e)
            (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::update-client-redirections-failed
                   :msg "Error updating client redirections"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-client-redirections
  "POST/DELETE /__admin/client/:id/redirections - Add or remove a redirect URI.

  Body: {\"uri\": \"https://example.com/callback\"}"
  [request client-id action]
  (let [body (when-let [b (:body request)]
               (json/read-str (slurp b)))
        uri (:uri body)]
    (if (str/blank? uri)
      (error-response "Missing 'uri' in request body")
      (update-client-redirections client-id "redirections" uri action))))

(defn handle-client-logout-redirections
  "POST/DELETE /__admin/client/:id/logout-redirections - Add or remove a logout redirect URI.

  Body: {\"uri\": \"https://example.com/\"}"
  [request client-id action]
  (let [body (when-let [b (:body request)]
               (json/read-str (slurp b)))
        uri (:uri body)]
    (if (str/blank? uri)
      (error-response "Missing 'uri' in request body")
      (update-client-redirections client-id "logout-redirections" uri action))))

;;; ============================================================================
;;; IAM Connector Management Handlers
;;; ============================================================================

(defn- want-reveal?
  "True when the URL has `?reveal=true` (or &reveal=true). The admin chain
   doesn't run wrap-params, so we check :query-string directly."
  [request]
  (boolean (some-> (:query-string request)
                   (->> (re-find #"(?:^|&)reveal=true(?:&|$)")))))

(defn- redact-secret
  "Replace `:secret` with \"***\" unless reveal? is true. The connector spec
   may stash type-specific secrets under other keys too — extend this list
   if/when other connector types use named secret fields."
  [conn reveal?]
  (if (or reveal? (nil? (:secret conn)))
    conn
    (assoc conn :secret "***")))

(defn handle-list-connectors
  "GET /__admin/iam/connectors[?reveal=true] - List the chain in execution order."
  [request]
  (try
    (let [reveal? (want-reveal? request)
          chain (mapv #(redact-secret % reveal?) (connector/list-chain))]
      (when reveal?
        (log/warn {:id ::connector-list-revealed
                   :data {:count (count chain)}}
                  "Connector list returned with secrets revealed"))
      (json-response {:connectors chain}))
    (catch Exception e
      (log/error! {:id ::list-connectors-failed
                   :msg "Error listing connectors"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-get-connector
  "GET /__admin/iam/connectors/:xid[?reveal=true]"
  [request xid]
  (try
    (let [reveal? (want-reveal? request)]
      (if-let [conn (connector/find-connector xid)]
        (do (when reveal?
              (log/warn {:id ::connector-secret-revealed
                         :data {:xid xid}}
                        "Connector secret revealed"))
            (json-response (redact-secret conn reveal?)))
        (error-response 404 (str "Connector not found: " xid))))
    (catch Exception e
      (log/error! {:id ::get-connector-failed
                   :msg "Error getting connector"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-create-connector
  "POST /__admin/iam/connectors

  Body shape (type-specific extra fields are passed through):
    {\"type\":     \"webhook\",          ; required
     \"name\":     \"Corp AD via webhook\",
     \"url\":      \"https://...\",
     \"secret\":   \"...\",
     \"priority\": 100,
     \"domain\":   \"corp.com\",         ; optional
     \"enabled\":  true}                  ; default true"
  [request]
  (try
    (let [body (when-let [b (:body request)] (json/read-str (slurp b)))]
      (if (str/blank? (some-> body :type str))
        (error-response "Missing 'type' in request body")
        (let [conn (connector/save-connector!
                    (-> body
                        (update :type keyword)
                        (assoc :enabled (if (some? (:enabled body))
                                          (boolean (:enabled body))
                                          true))))]
          (json-response 201 (redact-secret conn false)))))
    (catch Exception e
      (log/error! {:id ::create-connector-failed
                   :msg "Error creating connector"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-update-connector
  "PUT /__admin/iam/connectors/:xid

  Body has the same shape as create. If `secret` is omitted, the existing
  secret is preserved — so operators can edit a webhook's URL or priority
  without re-entering the shared secret."
  [request xid]
  (try
    (let [body (when-let [b (:body request)] (json/read-str (slurp b)))]
      (if-let [existing (connector/find-connector xid)]
        (let [merged (cond-> body
                       :always (assoc :xid xid)
                       :always (update :type #(if (keyword? %) % (some-> % keyword)))
                       (not (contains? body :secret))
                       (assoc :secret (:secret existing)))
              conn (connector/save-connector! merged)]
          (json-response (redact-secret conn false)))
        (error-response 404 (str "Connector not found: " xid))))
    (catch Exception e
      (log/error! {:id ::update-connector-failed
                   :msg "Error updating connector"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-delete-connector
  "DELETE /__admin/iam/connectors/:xid"
  [_request xid]
  (try
    (if (connector/find-connector xid)
      (do (connector/delete-connector! xid)
          (json-response {:success true :message (str "Deleted " xid)}))
      (error-response 404 (str "Connector not found: " xid)))
    (catch Exception e
      (log/error! {:id ::delete-connector-failed
                   :msg "Error deleting connector"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-refresh-connectors
  "POST /__admin/iam/connectors/refresh - Force the active provider to invalidate
  its in-memory cache. Rarely needed (Postgres NOTIFY handles this automatically);
  useful in SQLite multi-process scenarios or for debugging stale chains."
  [_request]
  (try
    (connector/refresh!)
    (json-response {:success true :message "Cache invalidated"})
    (catch Exception e
      (log/error! {:id ::refresh-connector-cache-failed
                   :msg "Error refreshing connector cache"
                   :data {:action :refreshing :subject :connector-cache}} e)
      (error-response 500 (.getMessage e)))))

(defn handle-test-connector
  "POST /__admin/iam/connectors/:xid/test

  Body: {\"username\": \"...\", \"password\": \"...\"}

  Runs ONLY the named connector against the supplied credentials and returns
  the raw `verify-credentials` response. Lets operators sanity-check a
  webhook is wired correctly without going through a full OAuth login. Does
  NOT touch the chain order or perform JIT user creation — pure dry-run."
  [request xid]
  (try
    (let [body (when-let [b (:body request)] (json/read-str (slurp b)))
          {:keys [username password]} body
          conn (connector/find-connector xid)]
      (cond
        (nil? conn) (error-response 404 (str "Connector not found: " xid))
        (str/blank? username) (error-response "Missing 'username'")
        (str/blank? password) (error-response "Missing 'password'")
        :else
        (let [result (connector/verify-credentials conn
                                                   {:username username
                                                    :password password})]
          ;; Defensive: never echo a password back, even if a connector did.
          (json-response (cond-> result
                           (:user result) (update :user dissoc :password))))))
    (catch Exception e
      (log/error! {:id ::test-connector-failed
                   :msg "Error testing connector"} e)
      (error-response 500 (.getMessage e)))))


;;; ============================================================================
;;; Log Config Handlers
;;; ============================================================================

(defn handle-get-log-config
  "GET /__admin/log/config — current routing config.

   Returns the live routing snapshot merged with the stored DB config.
   The `source` field indicates whether routing came from env-var bootstrap
   (\"env\") or from a previous DB save (\"db\")."
  [_request]
  (try
    (let [snapshot  (log/routing-snapshot)
          db-config (when (lifecycle/started? :synthigy/log.config)
                      (try (log-config/get-config) (catch Throwable _ nil)))]
      (json-response
        (cond
          snapshot  (log-config/internal->wire snapshot)
          db-config db-config
          :else     {:root_level "info" :ns_overrides []
                     :console {} :store {} :source "default"})))
    (catch Exception e
      (log/error! {:id ::get-log-config-failed :msg "Error reading log config"} e)
      (error-response 500 "Internal server error"))))

(defn handle-put-log-config
  "PUT /__admin/log/config — replace routing config.

   Body (all fields optional):
     {\"root_level\":   \"debug\",
      \"ns_overrides\": [{\"pattern\": \"synthigy.dataset.*\", \"level\": \"trace\"}],
      \"sinks\":        {\"file\": {\"level\": \"info\", \"ns\": null},
                       \"vector\": {\"level\": \"warn\", \"ns\": \"synthigy.*\"}}}

   Validates the body, persists to DB, and applies immediately. The change
   propagates to other nodes within one poll interval (~10 s)."
  [request]
  (try
    (let [body (when-let [b (:body request)] (json/read-str (slurp b)))]
      (when-not (lifecycle/started? :synthigy/log.config)
        (throw (ex-info "Log config module not started — DB not available"
                        {:type :service-not-started})))
      (if-let [err (log-config/validate-config body)]
        (error-response 400 err)
        (do
          (log-config/set-config! body)
          (log/info {:id ::log-config-updated
                     :data {:action :modified :subject :log-config}}
                    "Log routing config updated via admin API")
          (json-response {:success true :config body}))))
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 (.getMessage e))
        (do (log/error! {:id ::put-log-config-failed :msg "Error saving log config"} e)
            (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error! {:id ::put-log-config-failed :msg "Error saving log config"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-delete-log-config
  "DELETE /__admin/log/config — remove stored routing config.

   Clears the DB row and immediately reverts routing to the env-var bootstrap.
   The `routing` key in the response shows the restored state."
  [_request]
  (try
    (when-not (lifecycle/started? :synthigy/log.config)
      (throw (ex-info "Log config module not started" {:type :service-not-started})))
    (log-config/clear-config!)
    (json-response {:success true
                    :message "Stored log config cleared; routing reverted to env defaults"
                    :routing (some-> (log/routing-snapshot) log-config/internal->wire)})
    (catch clojure.lang.ExceptionInfo e
      (if (= :service-not-started (:type (ex-data e)))
        (error-response 503 (.getMessage e))
        (error-response 500 (.getMessage e))))
    (catch Exception e
      (log/error! {:id ::delete-log-config-failed :msg "Error clearing log config"} e)
      (error-response 500 (.getMessage e)))))

(defn handle-get-log-sinks
  "GET /__admin/log/sinks — active sinks with routing and health.

   Post-observability-substrate refactor: there are exactly two routing
   surfaces, `console` and `store` (the bridge that forwards to the bound
   `*log-store*`). Listing them with their min-level + ns-filter + health
   gives operators the same per-handler view they had under the old
   per-sink registry."
  [_request]
  (try
    (let [snapshot (log/routing-snapshot)]
      (json-response
        {:sinks (vec
                  (keep (fn [[key id]]
                          (when-let [{:keys [min-level ns-filter]} (get snapshot key)]
                            {:id        (name key)
                             :min_level (some-> min-level name)
                             :ns_filter ns-filter
                             :health    (log/sink-health id)}))
                        [[:console log/console-handler-id]
                         [:store   log/store-handler-id]]))}))
    (catch Exception e
      (log/error! {:id ::get-log-sinks-failed :msg "Error reading log sinks"} e)
      (error-response 500 "Internal server error"))))

;;; ============================================================================
;;; Router
;;; ============================================================================

(defn router
  "Main request router for admin endpoints."
  [request]
  (let [uri (:uri request)
        method (:request-method request)]
    (cond
      ;; System info
      (and (= method :get) (= uri "/__admin/info"))
      (handle-system-info request)

      ;; Encryption
      (and (= method :get) (= uri "/__admin/encryption/status"))
      (handle-encryption-status request)

      (and (= method :post) (= uri "/__admin/encryption/unseal"))
      (handle-unseal-master request)

      (and (= method :post) (= uri "/__admin/encryption/unseal-share"))
      (handle-unseal-share request)

      ;; Superuser management
      (and (= method :post) (= uri "/__admin/superuser"))
      (handle-add-superuser request)

      (and (= method :get) (= uri "/__admin/superuser"))
      (handle-list-superusers request)

      ;; Health check
      (and (= method :get) (= uri "/__admin/doctor"))
      (handle-doctor request)

      ;; Init (lifecycle/setup! - creates tables)
      (and (= method :post) (= uri "/__admin/init"))
      (handle-init request)

      ;; Initialize (full initialization: unseal + superuser)
      (and (= method :post) (= uri "/__admin/initialize"))
      (handle-initialize request)

      ;; Server control
      (and (= method :post) (= uri "/__admin/server/start"))
      (handle-server-start request)

      (and (= method :post) (= uri "/__admin/server/stop"))
      (handle-server-stop request)

      (and (= method :get) (= uri "/__admin/server/status"))
      (handle-server-status request)

      ;; Shutdown
      (and (= method :post) (= uri "/__admin/shutdown"))
      (handle-shutdown request)

      ;; Client management
      (and (= method :get) (= uri "/__admin/client"))
      (handle-list-clients request)

      (and (= method :post) (= uri "/__admin/client"))
      (handle-add-client request)

      (and (= method :get) (str/starts-with? uri "/__admin/client/")
           (not (str/includes? (subs uri (count "/__admin/client/")) "/")))
      (let [client-id (subs uri (count "/__admin/client/"))]
        (handle-get-client request client-id))

      (and (= method :delete) (str/starts-with? uri "/__admin/client/")
           (not (str/includes? (subs uri (count "/__admin/client/")) "/")))
      (let [client-id (subs uri (count "/__admin/client/"))]
        (handle-remove-client request client-id))

      (and (= method :post) (str/starts-with? uri "/__admin/client/")
           (str/ends-with? uri "/redirections"))
      (let [client-id (-> uri
                          (subs (count "/__admin/client/"))
                          (str/replace "/redirections" ""))]
        (handle-client-redirections request client-id :add))

      (and (= method :delete) (str/starts-with? uri "/__admin/client/")
           (str/ends-with? uri "/redirections"))
      (let [client-id (-> uri
                          (subs (count "/__admin/client/"))
                          (str/replace "/redirections" ""))]
        (handle-client-redirections request client-id :remove))

      (and (= method :post) (str/starts-with? uri "/__admin/client/")
           (str/ends-with? uri "/logout-redirections"))
      (let [client-id (-> uri
                          (subs (count "/__admin/client/"))
                          (str/replace "/logout-redirections" ""))]
        (handle-client-logout-redirections request client-id :add))

      (and (= method :delete) (str/starts-with? uri "/__admin/client/")
           (str/ends-with? uri "/logout-redirections"))
      (let [client-id (-> uri
                          (subs (count "/__admin/client/"))
                          (str/replace "/logout-redirections" ""))]
        (handle-client-logout-redirections request client-id :remove))

      ;; IAM Connector management
      ;; NOTE: more-specific paths (/refresh, /:xid/test) must come before the
      ;; generic /:xid handlers below — `cond` matches the first true clause.
      (and (= method :get) (= uri "/__admin/iam/connectors"))
      (handle-list-connectors request)

      (and (= method :post) (= uri "/__admin/iam/connectors"))
      (handle-create-connector request)

      (and (= method :post) (= uri "/__admin/iam/connectors/refresh"))
      (handle-refresh-connectors request)

      (and (= method :post) (str/starts-with? uri "/__admin/iam/connectors/")
           (str/ends-with? uri "/test"))
      (let [xid (-> uri
                    (subs (count "/__admin/iam/connectors/"))
                    (str/replace "/test" ""))]
        (handle-test-connector request xid))

      (and (= method :get) (str/starts-with? uri "/__admin/iam/connectors/")
           (not (str/includes? (subs uri (count "/__admin/iam/connectors/")) "/")))
      (handle-get-connector request (subs uri (count "/__admin/iam/connectors/")))

      (and (= method :put) (str/starts-with? uri "/__admin/iam/connectors/")
           (not (str/includes? (subs uri (count "/__admin/iam/connectors/")) "/")))
      (handle-update-connector request (subs uri (count "/__admin/iam/connectors/")))

      (and (= method :delete) (str/starts-with? uri "/__admin/iam/connectors/")
           (not (str/includes? (subs uri (count "/__admin/iam/connectors/")) "/")))
      (handle-delete-connector request (subs uri (count "/__admin/iam/connectors/")))

      ;; Log routing config
      (and (= method :get) (= uri "/__admin/log/sinks"))
      (handle-get-log-sinks request)

      (and (= method :get) (= uri "/__admin/log/config"))
      (handle-get-log-config request)

      (and (= method :put) (= uri "/__admin/log/config"))
      (handle-put-log-config request)

      (and (= method :delete) (= uri "/__admin/log/config"))
      (handle-delete-log-config request)

      ;; Not found
      :else
      (error-response 404 "Endpoint not found"))))

;;; ============================================================================
;;; Middleware
;;; ============================================================================

(defn wrap-localhost-only
  "Middleware: Only allow requests from localhost."
  [handler]
  (fn [request]
    (let [remote-addr (:remote-addr request)]
      (if (or (= remote-addr "127.0.0.1")
              (= remote-addr "::1")
              (= remote-addr "localhost"))
        (handler request)
        (do
          (log/warn {:id ::non-localhost-rejected
                     :data {:remote-addr remote-addr}}
                    "Rejected non-localhost request")
          {:status 403
           :body "Forbidden: Admin API only accepts localhost connections"})))))

(defn wrap-logging
  "Middleware: Log all requests."
  [handler]
  (fn [request]
    (let [method (:request-method request)
          uri (:uri request)]
      (log/info {:id ::admin-request
                 :data {:method method :uri uri}}
                "Admin request")
      (handler request))))

(defn wrap-exception
  "Middleware: Catch uncaught exceptions."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error! {:id ::uncaught-exception
                     :msg "Uncaught exception in admin handler"} e)
        (error-response 500 "Internal server error")))))

;;; ============================================================================
;;; Application
;;; ============================================================================

(def app
  "Complete admin application with middleware stack."
  (-> router
      wrap-localhost-only
      wrap-logging
      wrap-exception))
