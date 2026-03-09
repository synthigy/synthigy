(ns synthigy.oauth.device-code
  (:require
   [clojure.data.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [nano-id.core :as nano-id]
   [ring.util.codec :as codec]
   [synthigy.dataset.id :as id]
   [synthigy.iam
    :refer [validate-password]]
   [synthigy.oauth.core :as core
    :refer [pprint
            get-client
            encrypt
            decrypt]]
   [synthigy.oauth.page.device :as device]
   [synthigy.oauth.token :as token
    :refer [grant-token
            token-error
            client-id-missmatch]]
   [timing.core :as timing]))

(defonce ^:dynamic *device-codes* (atom nil))

(defn delete [code]
  (swap! *device-codes* dissoc code))

(def gen-device-code (nano-id/custom "ACDEFGHIJKLMNOPQRSTUVWXYZ" 40))
; (def gen-user-code (nano-id/custom "0123456789" 6))
(let [gen-par (nano-id/custom "ACDEFGHIJKLMNOPQRSTUVWXYZ" 4)]
  (defn gen-user-code []
    (str (gen-par) \- (gen-par))))

(defn get-device-code-data [device-code]
  (get @*device-codes* device-code))

(defn get-code-client [device-code]
  (get-client (get-in @*device-codes* [device-code :request :client_id])))

(def grant "urn:ietf:params:oauth:grant-type:device_code")

(defn validate-client [request]
  (let [{:keys [client_id]
         request-secret :client_secret} request
        {:keys [secret type]
         {:strs [allowed-grants]} :settings
         :as client} (get-client client_id)
        grants (set allowed-grants)
        client-id (id/extract client)]
    (log/debugf "Validating client: %s" (pprint client))
    (cond
      ;;
      (nil? client-id)
      (throw
       (ex-info
        "Client not registered"
        {:type "client_not_registered"
         :request request}))
      ;;
      (or (some? request-secret) (some? secret))
      (if (validate-password request-secret secret)
        client
        (throw
         (ex-info
          "Client secret missmatch"
          {:type "access_denied"
           :request request})))
      ;;
      (and (= type :public) (nil? secret))
      client
      ;;
      (not (contains? grants grant))
      (throw
       (ex-info
        "Client doesn't support device_code flow"
        {:type "access_denied"
         :request request}))
      ;;
      :else
      (do
        (log/errorf "Couldn't validate client\n%s" (pprint request))
        (throw
         (ex-info "Unknown client error"
                  {:request request
                   :type "server_error"}))))))

(defn code-expired? [code]
  (when-some [{:keys [expires-at]} (get-in @*device-codes* code)]
    (< expires-at (System/currentTimeMillis))))

(defn clean-expired-codes
  []
  (let [now (System/currentTimeMillis)
        expired (keep
                 (fn [[device-code {:keys [expires-at]}]]
                   (when (< expires-at now) device-code))
                 @*device-codes*)]
    (when-not (empty? expired)
      (log/infof
       "Revoking expired device codes:\n%s"
       (str/join "\n" expired)))
    (swap! *device-codes* (fn [codes] (apply dissoc codes expired)))))

(defmethod grant-token "urn:ietf:params:oauth:grant-type:device_code"
  [request]
  (let [{:keys [device_code client_secret]} request
        {{:keys [client_id]
          :as original-request
          id :client_id} :request
         :keys [session]} (get @*device-codes* device_code)
        client (core/get-client client_id)]
    (log/debugf
     "[%s] Processing token code grant for code: %s\n%s"
     id device_code (pprint request))
    ; (def request request)
    ; (def device_code (:device_code request))
    ; (def client_id (:client_id request))
    (let [{_secret :secret
           {:strs [allowed-grants]} :settings} (core/get-client client_id)
          grants (set allowed-grants)]
      (cond
        ;;
        (not (contains? @*device-codes* device_code))
        (token-error
         "invalid_request"
         "Provided device code is illegal!"
         "Your request will be logged"
         "and processed")
        ;;
        (not (contains? grants grant))
        (token-error
         "unauthorized_grant"
         "Client sent access token request"
         "for grant type that is outside"
         "of client configured privileges")
        ;;
        (nil? session)
        (token-error
         403
         "authorization_pending"
         "The authorization request is still pending as"
         "the end user hasn't yet completed the user-interaction steps")
        ;;
        (and (some? _secret) (empty? client_secret))
        (token-error
         "invalid_client"
         "Client secret wasn't provided")
        ;; If client has secret, than
        (and (some? _secret) (not (validate-password client_secret _secret)))
        (token-error
         "invalid_client"
         "Provided client secret is wrong")
        ;;
        (not= id client_id)
        client-id-missmatch
        ;;
        (nil? session)
        {:status 403
         :headers {"Content-Type" "application/json"}
         :body (json/write-str
                {:error "authorization_pending"
                 :error_description "The authorization request is still pending as the end user hasn't yet completed the user-interaction steps"})}
        ;; Issue that token
        :else
        (let [response (json/write-str (token/generate client session original-request))]
          (swap! *device-codes* dissoc device_code)
          ; (core/set-session-audience-scope session audience scope)
          {:status 200
           :headers {"Content-Type" "application/json;charset=UTF-8"
                     "Pragma" "no-cache"
                     "Cache-Control" "no-store"}
           :body response})))))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn device-authorization-handler
  "OAuth 2.0 Device Authorization handler (RFC 8628).

   Initiates device code flow for devices with limited input capabilities
   (smart TVs, CLI tools, IoT devices).

   Returns device_code, user_code, and verification URIs."
  [request]
  (letfn [(split-spaces [req k]
            (if-some [val (get req k)]
              (assoc req k (set (str/split val #"\s+")))
              req))]
    (let [{:keys [params remote-addr]} request
          {user-agent "user-agent"} (:headers request)
          params (-> params (split-spaces :scope))
          device-code (gen-device-code)
          user-code (gen-user-code)]
      (log/debugf "Device code request:\\n%s" request)
      (binding [core/*domain* (core/original-uri request)]
        (try
          (let [client (validate-client params)
                client-id (id/extract client)
                expires-at (+ (System/currentTimeMillis) (timing/minutes 5))]
            (swap! core/*clients* assoc client-id client)
            (swap! *device-codes* assoc device-code
                   {:user-code user-code
                    :expires-at expires-at
                    :request params
                    :device/agent user-agent
                    :device/ip remote-addr
                    :interval 5
                    :client client-id})
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (json/write-str
                    {:device_code device-code
                     :user_code user-code
                     :verification_uri (core/domain+ "/oauth/device/activate")
                     :verification_uri_complete (core/domain+ (str "/oauth/device/activate?user_code=" user-code))
                     :interval 5
                     :expires_in 900})})
          (catch clojure.lang.ExceptionInfo ex
            ;; Device authorization endpoint returns JSON errors (RFC 8628)
            ;; This is an API endpoint called by devices, not browsers
            (let [{:keys [type description]} (ex-data ex)
                  error-map {"client_not_registered" "invalid_client"
                             "access_denied" "access_denied"
                             "server_error" "server_error"}
                  error-code (get error-map type "invalid_request")]
              {:status 400
               :headers {"Content-Type" "application/json;charset=UTF-8"
                         "Cache-Control" "no-cache"}
               :body (json/write-str
                      (cond-> {:error error-code}
                        description (assoc :error_description description)))})))))))

(defn device-activation-handler
  "OAuth 2.0 Device Activation handler.

   Handles both GET and POST methods:
   - GET: Display activation form (with or without user_code pre-filled)
   - POST: Process activation (confirm/cancel) and redirect to login

   Security validations:
   - User code validation
   - IP address verification
   - User agent verification
   - Expiration check"
  [request]
  (letfn [(find-device-code [user_code]
            (some
             (fn [[device-code {:keys [user-code]}]]
               (when (= user_code user-code)
                 device-code))
             @*device-codes*))
          (redirect-to-login [{:keys [device-code] :as state}]
            (let [challenge (nano-id/nano-id 20)
                  client (get-code-client device-code)
                  login-url (core/get-client-login-url client)]
              (swap! *device-codes* update device-code
                     (fn [data]
                       (assoc-in data [:challenges challenge] (dissoc state :device-code))))
              {:status 302
               :headers {"Location" (str login-url "?"
                                         (codec/form-encode
                                          {:state (encrypt
                                                   (assoc state
                                                          :flow "device_code"
                                                          :challenge challenge))}))
                         "Cache-Control" "no-cache"}}))
          (redirect-to-canceled [{:keys [user-code device-code]}]
            (swap! *device-codes* dissoc device-code)
            {:status 302
             :headers {"Location" (str "/oauth/device/status?value=canceled&user_code=" user-code)
                       "Cache-Control" "no-cache"}})]
    (let [{:keys [remote-addr query-params params]} request
          {:keys [challenge action user_code]} params
          {user-agent "user-agent"} (:headers request)
          method (:request-method request)
          complete? (boolean (:user_code query-params))
          ;; For GET requests, user_code comes from query-params
          ;; For POST requests, user_code comes from params
          user_code (or (:user_code query-params) user_code)]
      (case method
        ;; GET: Display activation form
        :get
        (if (some? user_code)
          ;; verification_uri_complete - user_code provided
          (if-let [device-code (find-device-code user_code)]
            {:status (if device-code 200 400)
             :headers {"Content-Type" "text/html"}
             :body (str (device/authorize
                         {::complete? complete?
                          ::device-code device-code
                          ::user-code user_code
                          ::challenge (encrypt {:user-code user_code
                                                :device-code device-code
                                                :ip remote-addr
                                                :user-agent user-agent})}))}
            {:status 400
             :headers {"Content-Type" "text/html"}
             :body (str (device/authorize {::error :device-code/not-available}))})
          ;; verification_uri - manual entry
          {:status 200
           :headers {"Content-Type" "text/html"}
           :body (str (device/authorize {::complete? complete?}))})

        ;; POST: Process activation
        :post
        (cond
          ;; Missing challenge for verification_uri_complete
          (and complete? (nil? challenge))
          {:status 400
           :headers {"Content-Type" "text/html"}
           :body (str (device/authorize {::error :no-challenge}))}

          ;; verification_uri_complete flow
          complete?
          (let [{:keys [device-code user-code ip]
                 :as decrypted-challenge} (decrypt challenge)
                {real-code :user-code
                 :keys [expires-at]} (get @*device-codes* device-code)
                now (System/currentTimeMillis)]
            (cond
              ;; Expired
              (< expires-at now)
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::error :expired}))}

              ;; Invalid user code
              (not= real-code user-code user_code)
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::error :malicous-code}))}

              ;; IP mismatch
              (not= remote-addr ip)
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::error :malicious-ip}))}

              ;; User agent mismatch
              (not= user-agent (:user-agent decrypted-challenge))
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::error :malicous-user-agent}))}

              ;; Confirm action
              (= action "confirm")
              (redirect-to-login {:device-code device-code
                                  :ip ip
                                  :user-agent user-agent})

              ;; Cancel action
              (= action "cancel")
              (redirect-to-canceled {:user-code user-code
                                     :device-code device-code})

              ;; Unknown action
              :else
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::user-code user-code
                                             ::error :unknown-action}))}))

          ;; verification_uri flow - manual entry
          :else
          (if-let [device-code (find-device-code user_code)]
            (redirect-to-login {:device-code device-code
                                :ip remote-addr
                                :user-agent user-agent})
            {:status 400
             :headers {"Content-Type" "text/html"}
             :body (str (device/authorize {::user-code user_code
                                           ::error :not-available}))}))))))

;; =============================================================================
;; Legacy Pedestal Interceptors (will be removed after full conversion)
;; =============================================================================

