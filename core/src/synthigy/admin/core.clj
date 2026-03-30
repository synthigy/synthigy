(ns synthigy.admin.core
  "Pure Ring handlers for admin API.

  Provides localhost-only administration endpoints:
  - System information
  - Encryption management
  - Superuser management
  - System initialization
  - Health checks
  - Graceful shutdown"
  (:require
    [synthigy.json :as json]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [patcho.lifecycle :as lifecycle]
    [synthigy.data :as data]
    [synthigy.dataset :as dataset]
    [synthigy.dataset.encryption :as dataset-encryption]
    [synthigy.dataset.id :as id]
    [synthigy.iam :as iam]))

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
      (log/error e "[ADMIN] Error getting system info")
      (error-response 500 "Internal server error"))))

(defn handle-encryption-status
  "GET /__admin/encryption/status - Get encryption status."
  [_request]
  (try
    (json-response (dataset-encryption/encryption-status))
    (catch Exception e
      (log/error e "[ADMIN] Error getting encryption status")
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
      (log/error e "[ADMIN] Error unsealing with master key")
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
      (log/error e "[ADMIN] Error processing share")
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
          (log/error e "[ADMIN] Error adding superuser")
          (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error e "[ADMIN] Error adding superuser")
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
          (log/error e "[ADMIN] Error listing superusers")
          (error-response 500 (.getMessage e)))))
    (catch Exception e
      (log/error e "[ADMIN] Error listing superusers")
      (error-response 500 (.getMessage e)))))

(defn handle-init
  "POST /__admin/init - Initialize system (create tables, deploy schemas).

   This is ONE-TIME setup that creates database structure.
   Idempotent - safe to call multiple times (lifecycle tracks setup-complete?)."
  [_request]
  (try
    (log/info "[ADMIN] Running system initialization (lifecycle/setup!)...")

    ;; Setup in dependency order (lifecycle handles this automatically)
    ;; This will: create database, create dataset tables, create IAM tables
    (patcho.lifecycle/setup! :synthigy/iam)

    (json-response {:success true
                    :message "System initialized"
                    :modules [:synthigy/database
                              :synthigy/dataset
                              :synthigy/iam]})
    (catch Exception e
      (log/error e "[ADMIN] Initialization failed")
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
      (log/error e "[ADMIN] Error running doctor")
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
            (log/error e "[ADMIN] Initialization failed")
            (error-response 500 (.getMessage e))))))

    (catch Exception e
      (log/error e "[ADMIN] Initialization error")
      (error-response 500 (.getMessage e)))))

(defn handle-server-start
  "POST /__admin/server/start - Start HTTP server.

  Starts the server via lifecycle."
  [_request]
  (try
    (log/info "[ADMIN] Starting server via admin API")
    (patcho.lifecycle/start! :synthigy/server)

    (json-response {:success true
                    :message "Server started"})
    (catch Exception e
      (log/error e "[ADMIN] Error starting server")
      (error-response 500 (.getMessage e)))))

(defn handle-server-stop
  "POST /__admin/server/stop - Stop HTTP server."
  [_request]
  (try
    (log/info "[ADMIN] Stopping server via admin API")
    (patcho.lifecycle/stop! :synthigy/server)
    (json-response {:success true
                    :message "Server stopped"})
    (catch Exception e
      (log/error e "[ADMIN] Error stopping server")
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
      (log/error e "[ADMIN] Error checking server status")
      (error-response 500 (.getMessage e)))))

(defn handle-shutdown
  "POST /__admin/shutdown - Gracefully shutdown the system."
  [_request]
  (try
    (log/warn "[ADMIN] Shutdown requested via admin API")
    (json-response {:success true
                    :message "Shutdown initiated"})
    ;; Shutdown in background thread to allow response to be sent
    (future
      (Thread/sleep 1000)
      (System/exit 0))
    (catch Exception e
      (log/error e "[ADMIN] Error during shutdown")
      (error-response 500 (.getMessage e)))))

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
          (log/warnf "[ADMIN] Rejected non-localhost request from: %s" remote-addr)
          {:status 403
           :body "Forbidden: Admin API only accepts localhost connections"})))))

(defn wrap-logging
  "Middleware: Log all requests."
  [handler]
  (fn [request]
    (let [method (:request-method request)
          uri (:uri request)]
      (log/infof "[ADMIN] %s %s" (str/upper-case (name method)) uri)
      (handler request))))

(defn wrap-exception
  "Middleware: Catch uncaught exceptions."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e "[ADMIN] Uncaught exception")
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
