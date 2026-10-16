;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth
  (:require
   [buddy.core.hash :as hash]
   clojure.pprint
   [clojure.string :as str]
   [synthigy.cors :as cors]
   [synthigy.log :as log]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [ring.util.codec :as codec]
   [synthigy.dataset.id :as id]
   [synthigy.oauth.authorization-code
    :as ac
    :refer [gen-authorization-code
            validate-client
            mark-code-issued]]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.device-code :as device-code]
   [synthigy.oauth.onboarding :as onboarding]
   synthigy.oauth.patch
   [synthigy.oauth.persistence :as persistence]
   ;; side effects: grant-token/sign-token/session-kill-hook defmethods
   [synthigy.oauth.token]
   [synthigy.util :as util]))

(defn form-post-response
  "Auto-submitting HTML form POST response (response_mode=form_post)."
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
  "Build the authorization response for the given response_mode."
  [redirect-uri params response-mode]
  (case response-mode
    "form_post"
    (form-post-response redirect-uri params)

    "fragment"
    {:status 302
     :headers {"Location" (str redirect-uri "#" (codec/form-encode params))
               "Cache-Control" "no-cache"}}

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

(defn wrap-pkce-validation
  "Ring middleware enforcing PKCE (S256 only) on authorization_code token
   requests."
  [handler]
  (fn [request]
    (let [{:keys [code code_verifier grant_type]} (:params request)
          code-request (ac/get-code-request code)
          {:keys [code_challenge code_challenge_method]} code-request
          client (ac/get-code-client code)]
      (log/debug {:id ::pkce-request
                  :data {:grant-type grant_type
                         :client-id (:id client)
                         :code-challenge-method code_challenge_method}}
                 "PKCE inputs")
      (cond
        (not= "authorization_code" grant_type)
        (do
          (log/debug {:id ::pkce-skip-grant-type
                      :data {:grant-type grant_type}}
                     "Skipping PKCE validation (non-code grant)")
          (handler request))

        ;; PKCE is required for every client (OAuth 2.1 §4.1.3)
        (or (nil? code_challenge) (nil? code_challenge_method))
        (do
          (log/error {:id ::pkce-missing-challenge
                      :data {:client-id (:id client)}}
                     "PKCE is required (OAuth 2.1) — no code_challenge on file")
          (core/json-error
           "invalid_request"
           "PKCE is required. Provide code_challenge and code_challenge_method in the authorization request."))

        ;; 'plain' removed by OAuth 2.1 — the stored challenge would BE the
        ;; verifier
        (= "plain" code_challenge_method)
        (do
          (log/error {:id ::pkce-plain-rejected
                      :data {:client-id (:id client)}}
                     "code_challenge_method=plain rejected (removed by OAuth 2.1 / RFC 9700)")
          (core/json-error
           "invalid_request"
           "code_challenge_method \"plain\" is not supported. Use S256."))

        (nil? code_verifier)
        (do
          (log/error {:id ::pkce-missing-verifier
                      :data {:client-id (:id client)}}
                     "Missing code_verifier when PKCE is required")
          (core/json-error
           "invalid_request"
           "code_verifier is required when code_challenge was provided"))

        :else
        (let [current-challenge (generate-code-challenge code_verifier code_challenge_method)
              match? (= current-challenge code_challenge)]
          (log/debug {:id ::pkce-challenge-compare
                      :data {:match? match?}}
                     "Comparing generated PKCE challenge")
          (if match?
            (do
              (log/debug {:id ::pkce-passed
                          :data {:client-id (:id client)}}
                         "PKCE validation passed")
              (handler request))
            (do
              (log/error {:id ::pkce-failed
                          :data {:client-id (:id client)
                                 :code-challenge-method code_challenge_method}}
                         "PKCE validation failed")
              (core/json-error
               "invalid_request"
               "Proof Key for Code Exchange failed"))))))))

(defn login-location
  "The login page URL for one authorization: the encrypted state, plus the client's OIDC login_hint for pages that pre-fill it."
  [login-url state req-params]
  (let [hint (core/login-hint (:login_hint req-params))]
    (str login-url "?"
         (codec/form-encode
          (cond-> {:state (core/encrypt state)}
            hint (assoc :login_hint hint))))))

(defn authorization-handler
  "OAuth 2.0 authorization endpoint handler."
  [request]
  (let [{:keys [remote-addr params]} request
        {user-agent "user-agent"} (:headers request)]
    (log/info {:id ::authorize-requested
               :data {:action :requested
                      :subject :authorization-code
                      :flow "authorization_code"
                      :client (:client_id params)
                      :response-type (:response_type params)
                      :scope (:scope params)
                      :remote-addr remote-addr}}
              "Authorization flow started")
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
                session (core/get-session cookie-session)
                ;; never renew a killed or unknown session silently — revocation and session TTLs rely on it
                session-live? (and (:active session) (some? (:resource-owner session)))
                prompt-none? (= prompt "none")
                prompt-login? (= prompt "login")
                prompt-consent? (= prompt "consent")
                prompt-select-account? (= prompt "select_account")
                max-age-seconds (when max_age
                                  (try (Long/parseLong (str max_age))
                                       (catch Exception _ nil)))
                session-auth-time (:authorized-at session)
                auth-time-ms (when session-auth-time
                               (if (instance? java.util.Date session-auth-time)
                                 (.getTime ^java.util.Date session-auth-time)
                                 (try (long session-auth-time) (catch Exception _ nil))))
                session-too-old? (when (and max-age-seconds auth-time-ms)
                                   (> (- (System/currentTimeMillis) auth-time-ms)
                                      (* max-age-seconds 1000)))
                silent? (and session-live? prompt-none? (not session-too-old?))
                force-login? (or prompt-login? prompt-select-account? session-too-old?)]
            (cond
              (and prompt-none? (not session-live?))
              (core/handle-request-error
               {:request req-params
                :type "login_required"
                :description "User is not authenticated and prompt=none was requested"})

              (and prompt-none? session-too-old?)
              (core/handle-request-error
               {:request req-params
                :type "login_required"
                :description (str "Authentication is older than max_age (" max_age " seconds)")})

              (and silent? (contains? session :code))
              (core/handle-request-error
               {:request req-params
                :type "invalid_request"
                :description "Your session has unused access code active"})

              silent?
              (try
                (let [client (validate-client req-params)
                      client-id (id/extract client)
                      code (gen-authorization-code)
                      response-params (cond->
                                       {:code code
                                        :iss (core/domain+)}  ; RFC 9207
                                        (not-empty state) (assoc :state state))]
                  (ac/create-code! code {:issued? true
                                         :client client-id
                                         :agent user-agent
                                         :ip remote-addr
                                         :request req-params})
                  (mark-code-issued cookie-session code)
                  ;; the SPA's heartbeat — without this an actively renewing
                  ;; session reads as idle
                  (core/touch-session! cookie-session)
                  (authorization-response redirect_uri response-params response_mode))
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))

              force-login?
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      login-url (core/get-client-login-url client)
                      location (login-location login-url
                                               {:authorization-code code
                                                :flow "authorization_code"
                                                :prompt prompt}
                                               req-params)]
                  (ac/create-code! code {:client client-id
                                         :agent user-agent
                                         :ip remote-addr
                                         :request req-params})
                  {:status 302
                   :headers {"Location" location
                             "Cache-Control" "no-cache"}
                   :cookies {"idsrv.session" {:value ""
                                              :max-age 0
                                              :path "/"}}})
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))

              ;; TODO: full consent flow — for now treated like normal login
              prompt-consent?
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      login-url (core/get-client-login-url client)
                      location (login-location login-url
                                               {:authorization-code code
                                                :flow "authorization_code"
                                                :prompt prompt
                                                :force-consent true}
                                               req-params)]
                  (ac/create-code! code {:client client-id
                                         :agent user-agent
                                         :ip remote-addr
                                         :request req-params})
                  {:status 302
                   :headers {"Location" location
                             "Cache-Control" "no-cache"}})
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))

              :else
              (try
                (let [code (gen-authorization-code)
                      client (validate-client req-params)
                      client-id (id/extract client)
                      login-url (core/get-client-login-url client)
                      location (login-location login-url
                                               {:authorization-code code
                                                :flow "authorization_code"}
                                               req-params)]
                  (ac/create-code! code {:client client-id
                                         :agent user-agent
                                         :ip remote-addr
                                         :request req-params})
                  {:status 302
                   :headers {"Location" location
                             "Cache-Control" "no-cache"}})
                (catch clojure.lang.ExceptionInfo ex
                  (core/handle-request-error (ex-data ex))))))

          :else
          (core/handle-request-error
           {:type "server_error"
            :request req-params}))))))

(defonce maintenance-agent (agent {:running true
                                   :period (util/seconds 30)}))

(defonce ^:private last-janitor-run (atom 0))

(defn run-janitor!
  "Hourly row janitor on the 30s maintenance cadence."
  []
  (when (> (- (System/currentTimeMillis) @last-janitor-run) (util/hours 1))
    (reset! last-janitor-run (System/currentTimeMillis))
    (try
      (persistence/clean-expired-rows!)
      (catch Throwable e
        (log/error! {:id ::janitor-failed
                     :msg "OAuth store janitor failed"
                     :data {:action :cleanup :subject :oauth-store}}
                    e)))))

(comment
  (agent-error maintenance-agent)
  (restart-agent maintenance-agent @maintenance-agent)
  (start)
  (stop))

(defn maintenance
  [{:keys [running period]
    :as data}]
  (when (and running period)
    (log/debug {:id ::maintenance-start} "OAuth maintenance start")
    (send-off *agent* maintenance)
    (core/clean-sessions)
    (ac/clean-codes)
    (device-code/clean-expired-codes)
    (run-janitor!)
    (core/monitor-client-change)
    (onboarding/start-deactivation-watcher!)
    (log/debug {:id ::maintenance-finish} "OAuth maintenance finish")
    (Thread/sleep period))
  data)

(defn start
  []
  (patch/level! :synthigy.iam.oauth/model)
  (alter-var-root #'cors/*origin-allowed?* (constantly #'core/origin-allowed?))
  (send-off maintenance-agent assoc :running true :period (util/seconds 30))
  (send-off maintenance-agent maintenance))

(defn stop
  []
  (send-off maintenance-agent assoc :running false)
  (alter-var-root #'cors/*origin-allowed?* (constantly cors/allow-all))
  (core/evict-clients!)
  (onboarding/stop-deactivation-watcher!))

(lifecycle/register-module!
 :synthigy/oauth
 {:depends-on [:synthigy/iam :synthigy.iam/connector :synthigy/plug]
  :headline true
  :doc "OAuth 2.1 / OIDC provider — in-memory token & session store"
  :start (fn []
           (log/info {:id ::starting :data {:action :starting :subject :oauth}} "Starting OAuth")
           (start)
           (log/info {:id ::started :data {:action :started :subject :oauth}} "OAuth started"))
  :stop (fn []
          (log/info {:id ::stopping :data {:action :stopping :subject :oauth}} "Stopping OAuth")
          (stop)
          (log/info {:id ::stopped :data {:action :stopped :subject :oauth}} "OAuth stopped"))})

