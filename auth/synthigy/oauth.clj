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

(defn form-post-response
  "Generate an HTML page that auto-submits a form via POST (response_mode=form_post).

   Per OpenID Connect Core 1.0 Section 3.3.2.5:
   - Returns HTML document with auto-submitting form
   - Form POSTs authorization response parameters to redirect_uri
   - More secure than query/fragment as parameters don't appear in browser history"
  [redirect-uri params]
  (let [hidden-inputs (str/join "\n"
                        (for [[k v] params]
                          (format "      <input type=\"hidden\" name=\"%s\" value=\"%s\"/>"
                                  (name k) (str v))))]
    {:status 200
     :headers {"Content-Type" "text/html;charset=UTF-8"
               "Cache-Control" "no-cache, no-store"
               "Pragma" "no-cache"}
     :body (str "<!DOCTYPE html>\n"
                "<html>\n"
                "<head><title>Submitting Authorization</title></head>\n"
                "<body onload=\"document.forms[0].submit()\">\n"
                "  <noscript>\n"
                "    <p>JavaScript is required. Please click the button below.</p>\n"
                "  </noscript>\n"
                "  <form method=\"post\" action=\"" redirect-uri "\">\n"
                hidden-inputs "\n"
                "    <noscript><button type=\"submit\">Continue</button></noscript>\n"
                "  </form>\n"
                "</body>\n"
                "</html>")}))

(defn authorization-response
  "Generate authorization response based on response_mode.

   response_mode values:
   - 'query' (default) - Parameters in query string via redirect
   - 'fragment' - Parameters in URL fragment via redirect
   - 'form_post' - Parameters via auto-submitting HTML form POST"
  [redirect-uri params response-mode]
  (case response-mode
    "form_post"
    (form-post-response redirect-uri params)

    "fragment"
    {:status 302
     :headers {"Location" (str redirect-uri "#" (codec/form-encode params))
               "Cache-Control" "no-cache"}}

    ;; Default: query
    {:status 302
     :headers {"Location" (str redirect-uri "?" (codec/form-encode params))
               "Cache-Control" "no-cache"}}))

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
   Returns error response if PKCE validation fails.

   Per OAuth 2.1: PKCE is REQUIRED for public clients (clients without a secret).
   For confidential clients, PKCE is validated if code_challenge was provided."
  [handler]
  (fn [request]
    (let [{:keys [code code_verifier grant_type]} (:params request)
          code-request (ac/get-code-request code)
          {:keys [code_challenge code_challenge_method]} code-request
          is-pkce? (and code_challenge code_challenge_method)
          ;; Check if client is public (no secret) - PKCE is mandatory for public clients
          client (ac/get-code-client code)
          is-public-client? (nil? (:secret client))]
      (log/debugf "[PKCE] code=%s grant_type=%s" code grant_type)
      (log/debugf "[PKCE] code-request=%s" code-request)
      (log/debugf "[PKCE] code_challenge=%s method=%s is-pkce?=%s" code_challenge code_challenge_method is-pkce?)
      (log/debugf "[PKCE] code_verifier=%s" code_verifier)
      (log/debugf "[PKCE] client=%s is-public?=%s" (:id client) is-public-client?)
      (cond
        ;; Not authorization_code grant - skip PKCE validation
        (not= "authorization_code" grant_type)
        (do
          (log/debugf "[PKCE] Skipping validation (grant_type=%s)" grant_type)
          (handler request))

        ;; Public client without PKCE - reject per OAuth 2.1
        (and is-public-client? (not is-pkce?))
        (do
          (log/errorf "[PKCE] ❌ Public client MUST use PKCE (RFC 7636 / OAuth 2.1)")
          (core/json-error
            "invalid_request"
            "Public clients must use PKCE. Provide code_challenge and code_challenge_method in authorization request."))

        ;; No PKCE provided for confidential client - allow (backward compatible)
        (not is-pkce?)
        (do
          (log/debugf "[PKCE] Skipping validation (confidential client, no PKCE)")
          (handler request))

        ;; PKCE is required - verify code_verifier is present
        (nil? code_verifier)
        (do
          (log/errorf "[PKCE] ❌ Missing code_verifier when PKCE is required")
          (core/json-error
            "invalid_request"
            "code_verifier is required when code_challenge was provided"))

        ;; Validate PKCE
        :else
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
                "Proof Key for Code Exchange failed"))))))))

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
                 :keys [prompt state max_age response_mode]} req-params
                ;; OIDC prompt parameter handling (RFC OpenID Connect Core 1.0 Section 3.1.2.1)
                ;; prompt=none: Silent auth, return immediately or error
                ;; prompt=login: Force re-authentication, ignore existing session
                ;; prompt=consent: Force consent screen (TODO: implement consent flow)
                ;; prompt=select_account: Account selection (treated as login for now)
                prompt-none? (= prompt "none")
                prompt-login? (= prompt "login")
                prompt-consent? (= prompt "consent")
                prompt-select-account? (= prompt "select_account")
                ;; OIDC max_age parameter (Section 3.1.2.1)
                ;; If max_age is provided, check if authentication is still fresh
                max-age-seconds (when max_age
                                  (try (Long/parseLong (str max_age))
                                       (catch Exception _ nil)))
                session-auth-time (when cookie-session
                                    (core/get-session-authorized-at cookie-session))
                auth-time-ms (when session-auth-time
                               (if (instance? java.util.Date session-auth-time)
                                 (.getTime ^java.util.Date session-auth-time)
                                 (try (long session-auth-time) (catch Exception _ nil))))
                session-too-old? (when (and max-age-seconds auth-time-ms)
                                   (> (- (System/currentTimeMillis) auth-time-ms)
                                      (* max-age-seconds 1000)))
                ;; For silent flow, need existing session that's not too old
                silent? (and (some? cookie-session) prompt-none? (not session-too-old?))
                ;; Force login if session exists but is too old per max_age
                force-login? (or prompt-login? prompt-select-account? session-too-old?)]
            (cond
              ;; prompt=none but no session - return login_required error
              (and prompt-none? (nil? cookie-session))
              (core/handle-request-error
                {:request req-params
                 :type "login_required"
                 :description "User is not authenticated and prompt=none was requested"})

              ;; prompt=none but session too old per max_age - return login_required
              (and prompt-none? session-too-old?)
              (core/handle-request-error
                {:request req-params
                 :type "login_required"
                 :description (str "Authentication is older than max_age (" max_age " seconds)")})

              ;; Check that there isn't some other code active (for silent flow)
              (and silent? (contains? (core/get-session cookie-session) :code))
              (core/handle-request-error
                {:request req-params
                 :type "invalid_request"
                 :description "Your session has unused access code active"})

              ;; Silent flow - return code immediately (prompt=none with valid session)
              silent?
              (try
                (let [client (validate-client req-params)
                      client-id (id/extract client)
                      code (gen-authorization-code)
                      response-params (cond->
                                        {:code code
                                         :iss (core/domain+)}  ; RFC 9207 - Issuer Identification
                                        (not-empty state) (assoc :state state))]
                  (swap! *authorization-codes*
                         (fn [codes]
                           (assoc codes code {:issued? true
                                              :client client-id
                                              :created-on (System/currentTimeMillis)
                                              :user/agent user-agent
                                              :user/ip remote-addr
                                              :request req-params})))
                  (mark-code-issued cookie-session code)
                  ;; Support response_mode: query (default), fragment, or form_post
                  (authorization-response redirect_uri response-params response_mode))
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))

              ;; prompt=login or prompt=select_account - force re-authentication
              ;; Even if session exists, redirect to login
              force-login?
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      login-url (core/get-client-login-url client)
                      ;; Include prompt in state so login handler knows to force re-auth
                      location (str login-url "?"
                                    (codec/form-encode
                                      {:state (core/encrypt
                                                {:authorization-code code
                                                 :flow "authorization_code"
                                                 :prompt prompt})}))]
                  (swap! *authorization-codes*
                         (fn [codes]
                           (assoc codes code {:client client-id
                                              :created-on (System/currentTimeMillis)
                                              :user/agent user-agent
                                              :user/ip remote-addr
                                              :request req-params})))
                  {:status 302
                   :headers {"Location" location
                             "Cache-Control" "no-cache"}
                   ;; Clear the session cookie to force fresh login
                   :cookies {"idsrv.session" {:value ""
                                              :max-age 0
                                              :path "/"}}})
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))

              ;; prompt=consent - force consent screen
              ;; TODO: Implement full consent flow. For now, treat like normal login
              ;; which will show consent if scope requires it
              prompt-consent?
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      login-url (core/get-client-login-url client)
                      location (str login-url "?"
                                    (codec/form-encode
                                      {:state (core/encrypt
                                                {:authorization-code code
                                                 :flow "authorization_code"
                                                 :prompt prompt
                                                 :force-consent true})}))]
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
                  (core/handle-request-error (ex-data ex))))

              ;; Interactive flow - redirect to login (default case)
              :else
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      login-url (core/get-client-login-url client)
                      location (str login-url "?"
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

