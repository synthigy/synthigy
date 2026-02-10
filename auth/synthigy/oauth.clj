(ns synthigy.oauth
  (:require
    [buddy.core.hash :as hash]
    clojure.pprint
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [patcho.lifecycle :as lifecycle]
    [ring.util.codec :as codec]
    [synthigy.dataset.id :as id]
    [synthigy.oauth.authorization-code
     :as ac
     :refer [*authorization-codes*
             gen-authorization-code
             get-code-session
             get-code-request
             validate-client
             mark-code-issued]]
    [synthigy.oauth.core :as core]
    [synthigy.oauth.device-code :as device-code]
    [synthigy.oauth.page.status :refer [status-page]]
    [synthigy.oauth.token :as token]
    [timing.core :as timing]))

(defn generate-code-challenge
  ([code-verifier] (generate-code-challenge code-verifier "S256"))
  ([code-verifier code-challenge-method]
   (case code-challenge-method
     "plain" code-verifier
     "S256"
     (let [bs (.getBytes code-verifier)
           hashed (hash/sha256 bs)]
       (-> hashed
           codec/base64-encode
           (.replace "+" "-")
           (.replace "/" "_")
           (.replace "=" ""))))))

;; =============================================================================
;; Ring Middleware (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn wrap-pkce-validation
  "Ring middleware to validate PKCE (Proof Key for Code Exchange).

   Validates code_verifier against stored code_challenge for authorization_code grants.
   Returns error response if PKCE validation fails."
  [handler]
  (fn [request]
    (let [{:keys [code code_verifier grant_type]} (:params request)
          code-request (ac/get-code-request code)
          {:keys [code_challenge code_challenge_method]} code-request
          is-pkce? (and code_challenge code_challenge_method)]
      (log/debugf "[PKCE] code=%s grant_type=%s" code grant_type)
      (log/debugf "[PKCE] code-request=%s" code-request)
      (log/debugf "[PKCE] code_challenge=%s method=%s is-pkce?=%s" code_challenge code_challenge_method is-pkce?)
      (log/debugf "[PKCE] code_verifier=%s" code_verifier)
      (if (or (not is-pkce?) (not= "authorization_code" grant_type))
        (do
          (log/debugf "[PKCE] Skipping validation (is-pkce?=%s grant_type=%s)" is-pkce? grant_type)
          (handler request))
        ;; PKCE is required - verify code_verifier is present
        (if (nil? code_verifier)
          (do
            (log/errorf "[PKCE] ❌ Missing code_verifier when PKCE is required")
            (core/json-error
              "invalid_request"
              "code_verifier is required when code_challenge was provided"))
          (let [current-challenge (generate-code-challenge code_verifier code_challenge_method)]
            (log/debugf "[PKCE] Generated challenge=%s Expected=%s Match=%s" current-challenge code_challenge (= current-challenge code_challenge))
            (if (= current-challenge code_challenge)
              (do
                (log/debugf "[PKCE] ✅ PKCE validation passed")
                (handler request))
              (do
                (log/errorf "[PKCE] ❌ PKCE validation FAILED")
                (core/json-error
                  "invalid_request"
                  "Proof Key for Code Exchange failed")))))))))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn authorization-handler
  "OAuth 2.0 authorization endpoint handler.

   Initiates authorization code flow. Either:
   1. Returns authorization code immediately (silent flow with prompt=none)
   2. Redirects to login page to collect user credentials

   Supports PKCE (code_challenge/code_challenge_method)."
  [request]
  (let [{:keys [remote-addr params]} request
        {user-agent "user-agent"} (:headers request)]
    (log/debugf "Authorizing request:\n%s" params)
    (letfn [(split-spaces [req k]
              (if-some [val (get req k)]
                (assoc req k (set (str/split val #"\s+")))
                req))]
      (let [{:keys [response_type redirect_uri]
             :as req-params}
            (-> params
                (split-spaces :scope)
                (split-spaces :response_type))]
        (cond
          (empty? redirect_uri)
          (core/handle-request-error
            {:type "missing_redirect"
             :request req-params})

          (empty? response_type)
          (core/handle-request-error
            {:type "missing_response_type"
             :request req-params})

          (contains? response_type "code")
          (let [{cookie-session :idsrv/session
                 :keys [prompt state]} req-params
                silent? (and (some? cookie-session) (= prompt "none"))]
            (cond
              ;; Check that there isn't some other code active
              (and silent? (contains? (core/get-session cookie-session) :code))
              (core/handle-request-error
                {:request req-params
                 :type "invalid_request"
                 :description "Your session has unused access code active"})

              ;; Silent flow - return code immediately
              silent?
              (try
                (let [client (validate-client req-params)
                      client-id (id/extract client)
                      code (gen-authorization-code)]
                  (swap! *authorization-codes*
                         (fn [codes]
                           (assoc codes code {:issued? true
                                              :client client-id
                                              :created-on (System/currentTimeMillis)
                                              :user/agent user-agent
                                              :user/ip remote-addr
                                              :request req-params})))
                  (mark-code-issued cookie-session code)
                  {:status 302
                   :headers {"Location" (str redirect_uri "?"
                                             (codec/form-encode
                                               (cond->
                                                 {:code code}
                                                 (not-empty state) (assoc :state state))))}})
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))

              ;; Interactive flow - redirect to login
              :else
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      location (str "/oauth/login?"
                                    (codec/form-encode
                                      {:state (core/encrypt
                                                {:authorization-code code
                                                 :flow "authorization_code"})}))]
                  (swap! *authorization-codes*
                         (fn [codes]
                           (assoc codes code {:client client-id
                                              :created-on (System/currentTimeMillis)
                                              :user/agent user-agent
                                              :user/ip remote-addr
                                              :request req-params})))
                  {:status 302
                   :headers {"Location" location
                             "Cache-Control" "no-cache"}})
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))))

          :else
          (core/handle-request-error
            {:type "server_error"
             :request req-params}))))))

;; =============================================================================
;; Legacy Pedestal Interceptors (will be removed after full conversion)
;; =============================================================================

(defonce maintenance-agent (agent {:running true
                                   :period (timing/seconds 30)}))

(comment
  (agent-error maintenance-agent)
  (restart-agent maintenance-agent @maintenance-agent)
  (start)
  (stop))

(defn maintenance
  [{:keys [running period]
    :as data}]
  (when (and running period)
    (log/debug "[OAuth] Maintenance start")
    (send-off *agent* maintenance)
    (core/clean-sessions)
    (ac/clean-codes)
    (device-code/clean-expired-codes)
    (core/monitor-client-change)
    (log/debug "[OAuth] Maintenance finish")
    (Thread/sleep period))
  data)

(defn start
  []
  (send-off maintenance-agent assoc :running true :period (timing/seconds 30))
  (send-off maintenance-agent maintenance))

(defn stop
  []
  (send-off maintenance-agent assoc :running false)
  (doseq [x [core/*resource-owners* core/*clients*
             core/*sessions* token/*tokens*]]
    (reset! x nil)))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/oauth
  {:depends-on [:synthigy/iam]
   :start (fn []
            ;; Runtime: Start maintenance agent, initialize caches
            (log/info "[OAUTH] Starting OAuth...")
            (start)
            (log/info "[OAUTH] OAuth started"))
   :stop (fn []
           ;; Runtime: Stop maintenance agent, clear caches
           (log/info "[OAUTH] Stopping OAuth...")
           (stop)
           (log/info "[OAUTH] OAuth stopped"))})

