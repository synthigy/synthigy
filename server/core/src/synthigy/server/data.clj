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

(ns synthigy.server.data
  "Server-agnostic /data endpoint handler — direct dataset operations via JSON,
   for service-to-service use with acting_as impersonation. See SDK.md."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [synthigy.log :as log]
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.access :as daccess]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.runtime :as runtime]
   [synthigy.engine :as engine]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.iam.transfer :as transfer]
   [synthigy.json :as json]
   [synthigy.oauth.core :as oauth]
   [synthigy.oauth.authentication :as auth]
   [synthigy.transit :as transit]))

(defn preserve-body
  "Stamp :raw-body on a JSON POST and hand back a FRESH :body stream.

   Every adapter must run this before anything that reads the body — an
   adapter that slurps the InputStream without replacing it leaves /data with
   nothing to read, and every SDK gets INVALID_BODY. Shared rather than
   reimplemented per adapter because that is exactly how pedestal drifted."
  [request]
  (if (and (= :post (:request-method request))
           (:body request)
           (some-> (get-in request [:headers "content-type"])
                   (str/starts-with? "application/json")))
    (let [raw (if (string? (:body request))
                (:body request)
                (slurp (:body request)))]
      (assoc request
             :raw-body raw
             :body (java.io.ByteArrayInputStream. (.getBytes raw "UTF-8"))))
    request))

(defn wrap-preserve-body
  "Ring middleware over `preserve-body`; must run BEFORE wrap-params, which
   consumes the InputStream."
  [handler]
  (fn [request] (handler (preserve-body request))))

(defn content-type
  "Content type from request, ignoring charset params."
  [request]
  (some-> (get-in request [:headers "content-type"])
          (str/split #";")
          first
          str/trim
          str/lower-case))

(defn accept-type [request]
  (some-> (get-in request [:headers "accept"])
          (str/split #",")
          first
          str/trim
          str/lower-case))

(defn read-body-str [request]
  (let [body (or (:raw-body request) (:body request))]
    (when body
      (if (string? body)
        body
        (slurp (io/reader body))))))

(defn parse-request-body
  "Parse request body per Content-Type: application/json,
   application/transit+json, or application/edn."
  [request]
  (try
    (when-let [body-str (read-body-str request)]
      (when-not (str/blank? body-str)
        (case (content-type request)
          "application/transit+json" (transit/<-transit body-str)
          "application/edn" (edn/read-string body-str)
              ;; Default: JSON
          (json/read-str body-str))))
    (catch Exception e
      (log/debug {:id ::body-parse-failed :error e}
                 "Failed to parse request body")
      nil)))

(defn format-response
  "Ring response with content negotiation based on Accept; top-level :error
   bodies get the request-id stamped on."
  [request status body]
  (let [body (cond-> body
               (map? (:error body)) (update :error engine/with-request-id))]
    (case (accept-type request)
    "application/transit+json"
    {:status status
     :headers {"Content-Type" "application/transit+json"}
     :body (transit/->transit body)}

    "application/edn"
    {:status status
     :headers {"Content-Type" "application/edn"}
     :body (pr-str body)}

    ;; Default: JSON
    {:status status
     :headers {"Content-Type" "application/json"}
     :body (json/write-str body)})))

(defn json-response
  "JSON Ring response, no content negotiation."
  [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/write-str body)})

(defn schema-handler
  "Ring handler for GET /schema — the IAM-filtered model schema, optionally
   projected onto ?roles."
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request engine/platform-audience))]
    (if (and iam-active? (not iam))
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (try
        (access/with-principal (:principal iam)
          (let [raw-roles    (get-in request [:query-params "roles"])
                role-names   (when (seq raw-roles)
                               (->> (str/split raw-roles #",")
                                    (map str/trim)
                                    (filter seq)))
                projected    (when (seq role-names)
                               (access/roles->schema-principal role-names))]
            (access/with-principal (or projected (:principal iam))
              (let [model         (-> (dataset/deployed-model)
                                      daccess/protect-model
                                      runtime/build)
                    raw-entities  (get-in request [:query-params "entities"])
                    entity-filter (when (seq raw-entities)
                                    (->> (str/split raw-entities #",")
                                         (map str/trim)
                                         (filter seq)))
                    key-fmt       (get-in request [:query-params "key_format"])
                    name-fn       (get daccess/name-fn-for key-fmt daccess/*name-fn*)
                    result        (dataset/stamp-schema
                                    (binding [daccess/*name-fn* name-fn]
                                      (daccess/schema model entity-filter)))]
                (json-response 200 result)))))
        (catch clojure.lang.ExceptionInfo e
          (let [status (case (:code (ex-data e))
                         "UNKNOWN_ROLE" 400
                         "FORBIDDEN"    403
                         500)]
            (json-response status {:error {:message (ex-message e)
                                           :code    (or (:code (ex-data e)) "INTERNAL_ERROR")}})))
        (catch Throwable e
          (json-response 500 {:error {:message (ex-message e)
                                      :code    "INTERNAL_ERROR"}}))))))

(defn lint-handler
  "Ring handler for POST /lint — schema-aware XSQL diagnostics; computation
   lives in engine/lint-source."
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request engine/platform-audience))]
    (if (and iam-active? (not iam))
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (let [body (parse-request-body request)]
        (if-not body
          (json-response 400 {:error {:message "Invalid or missing JSON body"
                                      :code    "INVALID_BODY"}})
          (try
            (access/with-principal (:principal iam)
              (let [opts {:source (or (:source body) (get body "source"))
                          :entity (or (:entity body) (get body "entity"))
                          :op     (or (:op body) (get body "op"))}
                    diags (engine/lint-source opts)]
                (json-response 200 {:diagnostics diags})))
            (catch Throwable e
              (log/error! {:id ::lint-failed
                           :data {:op (or (:op body) (get body "op"))
                                  :entity (or (:entity body) (get body "entity"))
                                  :source-bytes (count (str (or (:source body)
                                                                (get body "source"))))}}
                          e)
              (json-response 500 {:error {:message (ex-message e)
                                          :code    "INTERNAL_ERROR"}}))))))))

(defn compile-handler
  "Ring handler for POST /compile — the same body /data takes, returning the
   wire ops the engine would execute instead of executing them."
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request engine/platform-audience))]
    (if (and iam-active? (not iam))
      (format-response request 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (let [body (parse-request-body request)]
        (if-not body
          (format-response request 400 {:error {:message "Invalid or missing JSON body"
                                                :code    "INVALID_BODY"}})
          (try
            (format-response request 200 {:results (engine/compile-operations body)})
            (catch clojure.lang.ExceptionInfo e
              (format-response request 400 {:error {:message (ex-message e)
                                                    :code    (or (:code (ex-data e)) "BAD_REQUEST")}}))
            (catch Throwable e
              (log/error! {:id ::compile-failed
                           :data {:op-count (count (:operations body))}}
                          e)
              (let [[clean _] (engine/safe-error-message e)]
                (format-response request 500 {:error {:message clean
                                                      :code    "INTERNAL_ERROR"}})))))))))

;;; ============================================================================
;;; IAM Configuration Transfer (export / import)
;;; ============================================================================
;;;
;;; `synthigy.iam.transfer` runs in SYSTEM context — the export is complete
;;; rather than RBAC-filtered, so these handlers gate on ROOT (access/superuser?)
;;; regardless of the caller's own grants, not just on being authenticated.

(defn- transfer-type
  "Parse and validate the `type` query param shared by /iam/export and
   /iam/import."
  [request]
  (let [raw (get-in request [:query-params "type"])]
    (when (str/blank? raw)
      (throw (ex-info "Missing required query parameter: type"
                      {:code "BAD_REQUEST"
                       :valid (vec (sort (map name (keys transfer/transferable))))})))
    (let [t (keyword raw)]
      (when-not (contains? transfer/transferable t)
        (throw (ex-info (str "Unknown transfer type: " raw)
                        {:code "BAD_REQUEST"
                         :valid (vec (sort (map name (keys transfer/transferable))))})))
      t)))

(defn- require-root!
  []
  (when-not (access/superuser?)
    (throw (ex-info "IAM configuration transfer requires ROOT" {:code "FORBIDDEN"}))))

(defn- transfer-error-response
  [e]
  (let [data (ex-data e)
        status (case (:code data)
                 "BAD_REQUEST" 400
                 "FORBIDDEN"   403
                 500)]
    (json-response status (cond-> {:error {:message (ex-message e)
                                           :code (or (:code data) "INTERNAL_ERROR")}}
                            (:valid data) (assoc-in [:error :valid] (:valid data))))))

(defn iam-export-handler
  "Ring handler for GET /iam/export?type=<role|group|user|app|api>[&name=<name>] —
   ROOT-only; returns `{:type ... :records [...]}`, all of `type` or just `name`."
  [request]
  (let [iam (auth/authenticate-request request engine/platform-audience)]
    (if-not iam
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (try
        (access/with-principal (:principal iam)
          (require-root!)
          (let [t    (transfer-type request)
                nm   (get-in request [:query-params "name"])
                args (when-not (str/blank? nm) {:name {:_eq nm}})]
            (json-response 200 {:type (name t) :records (transfer/export t args)})))
        (catch clojure.lang.ExceptionInfo e (transfer-error-response e))
        (catch Throwable e
          (json-response 500 {:error {:message (ex-message e) :code "INTERNAL_ERROR"}}))))))

(defn iam-import-handler
  "Ring handler for POST /iam/import?type=<...>[&mode=sync|stack][&dry-run=true] —
   ROOT-only. Body is a record or array of records, the same shape /iam/export
   returns under `:records`. `mode` defaults to `sync` (payload-mentioned
   relations are matched exactly); `stack` is additive."
  [request]
  (let [iam (auth/authenticate-request request engine/platform-audience)]
    (if-not iam
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (try
        (access/with-principal (:principal iam)
          (require-root!)
          (let [t       (transfer-type request)
                mode    (keyword (or (get-in request [:query-params "mode"]) "sync"))
                dry?    (= "true" (get-in request [:query-params "dry-run"]))
                payload (parse-request-body request)]
            (when (nil? payload)
              (throw (ex-info "Missing request body" {:code "BAD_REQUEST"})))
            (when-not (#{:sync :stack} mode)
              (throw (ex-info (str "Unknown mode: " (name mode))
                              {:code "BAD_REQUEST" :valid ["sync" "stack"]})))
            (let [{:keys [records missing]} (transfer/validate-records t payload)]
              (cond
                (seq missing)
                (json-response 409 {:error {:message "Payload references ids that do not exist"
                                            :code "MISSING_REFS"}
                                    :type (name t) :records records :missing missing})

                dry?
                (json-response 200 {:dry-run true :type (name t) :records records :missing {}})

                :else
                (do (transfer/import-records! t payload :mode mode :validate? false
                                              :source "iam-transfer-api")
                    (json-response 200 {:type (name t) :mode (name mode) :records records}))))))
        (catch clojure.lang.ExceptionInfo e (transfer-error-response e))
        (catch Throwable e
          (json-response 500 {:error {:message (ex-message e) :code "INTERNAL_ERROR"}}))))))

(defn by-name!
  "Resolves entity names to refs in ONE query, naming every miss — callers
   create nothing until all of them resolve."
  [entity-id names label]
  (when (seq names)
    (let [wanted (set names)
          found (dataset/search-entity entity-id
                                       {:name {:_in (vec wanted)}}
                                       {(id/key) nil :name nil})
          missing (remove (set (map :name found)) wanted)]
      (when (seq missing)
        (throw (ex-info (str "Unknown " label "(s): " (str/join ", " missing))
                        {:code "BAD_REQUEST"})))
      (vec found))))

(defn iam-add-client-handler
  "Ring handler for POST /iam/clients — ROOT-only. Body `{:id? :name :type?
   :secret? :settings? :roles? [name ...] :apis? [name ...]}`. `:secret` is
   only ever returned here, at creation — export/import never carry it."
  [request]
  (let [iam (auth/authenticate-request request engine/platform-audience)]
    (if-not iam
      (json-response 401 {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})
      (try
        (access/with-principal (:principal iam)
          (require-root!)
          (let [{:keys [id name type secret settings roles apis]
                 :or {type "public"}} (parse-request-body request)]
            (when (str/blank? name)
              (throw (ex-info "Missing required field: name" {:code "BAD_REQUEST"})))
            (when-not (#{"public" "confidential"} type)
              (throw (ex-info (str "Unknown type: " type)
                              {:code "BAD_REQUEST" :valid ["public" "confidential"]})))
            (when (and (or (seq roles) (seq apis)) (not= type "confidential"))
              (throw (ex-info "roles/apis require type=confidential" {:code "BAD_REQUEST"})))
            (let [roles (by-name! :iam/user-role roles "role")
                  apis (by-name! :iam/api apis "API")
                  client (iam/add-client {:id id :name name :type (keyword type)
                                          :secret secret :settings settings :apis apis})]
              (when (seq roles)
                (iam/set-user {(id/key) (id/extract (:service-user client)) :roles roles}))
              (json-response 200 {:id (:id client) :secret (:secret client) :type type
                                  :roles (mapv :name roles) :apis (mapv :name apis)}))))
        (catch clojure.lang.ExceptionInfo e (transfer-error-response e))
        (catch Throwable e
          (json-response 500 {:error {:message (ex-message e) :code "INTERNAL_ERROR"}}))))))

;;; ============================================================================
;;; Acting-As Resolution
;;; ============================================================================

(def ^:private observability-ops
  "Ops served by /logs, and ONLY /logs — the operator plane is a separate
   endpoint from the /data plane (Robert, 2026-07-22). The defmethods are
   shared; only the mounting differs."
  #{"log-query" "log-histogram" "traffic-stats" "log-config"})

(defn endpoint-op-violation
  "Error map when one of operations is mounted on the other endpoint; nil when
   legal."
  [endpoint operations]
  (let [names (map #(str (or (:op %) (get % "op"))) operations)]
    (case endpoint
      :logs (when-let [bad (first (remove observability-ops names))]
              {:message (str "Operation `" bad "` is not served by /logs — use /data")
               :code "BAD_OP"})
      :data (when-let [bad (first (filter observability-ops names))]
              {:message (str "Operation `" bad "` moved to the /logs endpoint")
               :code "BAD_OP"}))))

(defn plane-handler
  "Shared Ring handler body for /data and /logs; endpoint gates which ops the
   batch may carry."
  [request endpoint]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam (when iam-active? (auth/authenticate-request request engine/platform-audience))]
    (if (and iam-active? (not iam))
      (format-response request 401 {:error {:message "Unauthorized"
                                            :code "UNAUTHORIZED"}})
      (let [body (parse-request-body request)]
        (if-not body
          (format-response request 400 {:error {:message "Invalid or missing JSON body"
                                                :code "INVALID_BODY"}})
          (let [{:keys [acting_as operations]} body]
            (if (empty? operations)
              (format-response request 400 {:error {:message "No operations provided"
                                                    :code "NO_OPERATIONS"}})
            (if-let [violation (endpoint-op-violation endpoint operations)]
              (format-response request 400 {:error violation})
              (try
                ;; Trusted-client cascade, severity order, first match wins —
                ;; gates which REMOTE
                ;; client may impersonate (embedded callers have no OAuth
                ;; client, trusted by
                ;; construction), so it lives here rather than in
                ;; engine/execute.
                (when acting_as
                  (let [client_id (get-in iam [:claims :client_id])
                        client (when client_id (oauth/get-client client_id))
                        public? (boolean (#{:public "public"} (:type client)))
                        trusted? (boolean (get-in client [:settings "trusted"]))]
                    (cond
                      (and client_id (nil? client))
                      (throw (ex-info "Client not found"
                                      {:code "CLIENT_NOT_FOUND"
                                       :client-id client_id}))

                      (and client (false? (:active client)))
                      (throw (ex-info "Client is inactive"
                                      {:code "CLIENT_INACTIVE"
                                       :client-id client_id}))

                      (and client public?)
                      (throw (ex-info "Public clients cannot impersonate users"
                                      {:code "PUBLIC_CLIENT_FORBIDDEN"
                                       :client-id client_id}))

                      (and client_id (not trusted?))
                      (throw (ex-info "Client not authorized for impersonation"
                                      {:code "NOT_TRUSTED"
                                       :client-id client_id})))))
                (format-response request 200 (engine/execute body iam))
                (catch clojure.lang.ExceptionInfo e
                  (let [code (:code (ex-data e))
                        status (if (= code "USER_NOT_FOUND") 404 403)]
                    (format-response request status
                                     {:error {:message (ex-message e)
                                              :code code}})))
                (catch Throwable e
                  ;; Last-resort: log raw server-side, return a sanitized
                  ;; message so Java internals never reach the wire.
                  (let [[clean raw] (engine/safe-error-message e)]
                    (log/error! {:id ::data-handler-uncaught
                                 :msg "Uncaught exception in /data handler"
                                 :data {:raw-message raw}}
                                e)
                    (format-response request 500
                                     {:error {:message clean
                                              :code "INTERNAL_ERROR"}}))))))))))))

(defn handler
  "Ring handler for the /data endpoint — the DATA plane; observability ops are
   rejected toward /logs."
  [request]
  (plane-handler request :data))

(defn logs-handler
  "Ring handler for the /logs endpoint — the OBSERVABILITY plane; every
   non-observability op is rejected toward /data."
  [request]
  (plane-handler request :logs))
