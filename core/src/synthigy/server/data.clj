(ns synthigy.server.data
  "Server-agnostic /data endpoint handler.

   Provides direct dataset operations via JSON, bypassing GraphQL.
   Designed for service-to-service communication where services use
   client credentials + acting_as for user impersonation.

   ## Wire Format

   Request:
   ```json
   POST /data
   Authorization: Bearer <token>
   Content-Type: application/json

   {
     \"acting_as\": \"user-euuid\",     // optional, for impersonation
     \"operations\": [
       {\"op\": \"search\", \"entity\": \"user\",
        \"args\": {\"_where\": {\"active\": {\"_eq\": true}}},
        \"selection\": {\"name\": null, \"email\": null}}
     ]
   }
   ```

   Response:
   ```json
   {\"results\": [
     {\"data\": [...], \"ok\": true},
     {\"error\": {\"message\": \"...\", \"code\": \"...\"}, \"ok\": false}
   ]}
   ```

   See SDK.md for full specification."
  (:require
   [synthigy.json :as json]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.dataset.sql.template :as template]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.oauth.core :as oauth]
   [synthigy.server.auth :as auth]))

;;; ============================================================================
;;; Operation Dispatch
;;; ============================================================================

(defn- execute-op
  "Execute a single dataset operation.

   Args:
     op - Operation map with :op, :entity, and operation-specific keys

   Returns:
     {:data <result> :ok true} or {:error {:message <msg>} :ok false}"
  [{:keys [op entity args data selection template params]
    :as operation}]
  (try
    (let [result (if (= op "query")
                   ;; Query operation — SQL template
                   (template/execute-template template params
                                              {:cached (get operation :cached true)})
                   ;; Dataset operations — resolve entity from deployed schema
                   (let [entity-id (sql-query/resolve-entity entity)]
                     (case op
                       "search"    (dataset/search-entity entity-id args selection)
                       "get"       (dataset/get-entity entity-id args selection)
                       "sync"      (dataset/sync-entity entity-id data)
                       "stack"     (dataset/stack-entity entity-id data)
                       "slice"     (dataset/slice-entity entity-id args selection)
                       "delete"    (dataset/delete-entity entity-id data)
                       "purge"     (dataset/purge-entity entity-id args selection)
                       "aggregate" (dataset/aggregate-entity entity-id args selection)
                       (throw (ex-info (str "Unknown operation: " op)
                                       {:code "UNKNOWN_OP" :op op})))))]
      {:data result :ok true})
    (catch clojure.lang.ExceptionInfo e
      (let [data (ex-data e)]
        {:error {:message (ex-message e)
                 :code (or (:code data) "OPERATION_ERROR")}
         :ok false}))
    (catch Exception e
      {:error {:message (ex-message e)
               :code "INTERNAL_ERROR"}
       :ok false})))

;;; ============================================================================
;;; Body Preservation Middleware
;;; ============================================================================

(defn wrap-preserve-body
  "Ring middleware that preserves the raw body for JSON endpoints.
   Must be applied BEFORE wrap-params which consumes the InputStream.
   Only activates for POST requests with application/json content type."
  [handler]
  (fn [request]
    (if (and (= :post (:request-method request))
             (:body request)
             (some-> (get-in request [:headers "content-type"])
                     (.startsWith "application/json")))
      (let [raw (if (string? (:body request))
                  (:body request)
                  (slurp (:body request)))]
        (handler (assoc request
                        :raw-body raw
                        :body (java.io.ByteArrayInputStream. (.getBytes raw "UTF-8")))))
      (handler request))))

;;; ============================================================================
;;; Request Parsing
;;; ============================================================================

(defn- parse-json-body
  "Parse JSON request body. Returns parsed map or nil."
  [request]
  (try
    (let [body (or (:raw-body request) (:body request))]
      (when body
        (let [body-str (if (string? body)
                         body
                         (slurp (io/reader body)))]
          (when-not (empty? body-str)
            (json/read-str body-str)))))
    (catch Exception e
      (log/debugf e "Failed to parse JSON body")
      nil)))

(defn- json-response
  "Create a JSON Ring response."
  [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/write-str body)})

;;; ============================================================================
;;; Acting-As Resolution
;;; ============================================================================

(defn- resolve-acting-as
  "Resolve the acting_as user from the request body.

   Returns user context map or throws if user not found/inactive."
  [acting-as]
  (if-let [user-ctx (iam.context/get-user-context acting-as)]
    {:user user-ctx
     :roles (:roles user-ctx)
     :groups (:groups user-ctx)}
    (throw (ex-info "User not found or inactive"
                    {:code "USER_NOT_FOUND" :acting-as acting-as}))))

;;; ============================================================================
;;; Handler
;;; ============================================================================

(defn handler
  "Ring handler for the /data endpoint.

   Authentication:
   - Bearer token required (validated against active tokens)
   - If acting_as present: service must be trusted, user must be active
   - If no acting_as: executes as the token owner

   Args:
     request - Ring request map

   Returns:
     Ring response map"
  [request]
  (let [;; Authenticate service/user token (skip if IAM not started)
        iam-active? (lifecycle/started? :synthigy/iam)
        iam (when iam-active? (auth/authenticate-request request))]
    (if (and iam-active? (not iam))
      (json-response 401 {:error {:message "Unauthorized"
                                  :code "UNAUTHORIZED"}})
      ;; Parse body
      (let [body (or (not-empty (dissoc (:params request) :datastar))
                     (parse-json-body request))]
        (if-not body
          (json-response 400 {:error {:message "Invalid or missing JSON body"
                                      :code "INVALID_BODY"}})
          (let [{:keys [acting_as operations]} body]
            (if (empty? operations)
              (json-response 400 {:error {:message "No operations provided"
                                          :code "NO_OPERATIONS"}})
              ;; Resolve user context
              (try
                (let [;; Check trusted flag for impersonation
                      _ (when acting_as
                          (let [client_id (get-in iam [:claims :client_id])
                                client (when client_id (oauth/get-client client_id))]
                            (when (and client_id
                                       (not (get-in client [:settings "trusted"])))
                              (throw (ex-info "Client not authorized for impersonation"
                                              {:code "NOT_TRUSTED"
                                               :client-id client_id})))))
                      user-ctx (if acting_as
                                 ;; Impersonation: load acting_as user
                                 (resolve-acting-as acting_as)
                                 ;; Direct: use token owner
                                 iam)]
                  ;; Bind IAM context and execute operations
                  (binding [access/*user* (:user user-ctx)
                            access/*roles* (:roles user-ctx)
                            access/*groups* (:groups user-ctx)]
                    (let [results (mapv execute-op operations)]
                      (json-response 200 {:results results}))))
                (catch clojure.lang.ExceptionInfo e
                  (json-response 403
                                 {:error {:message (ex-message e)
                                          :code (:code (ex-data e))}}))))))))))
