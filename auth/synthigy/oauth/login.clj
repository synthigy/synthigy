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

(ns synthigy.oauth.login
  (:require
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.set :as set]
   [clojure.string :as str]
   [synthigy.log :as log]
   [environ.core :refer [env]]
   [ring.util.codec :as codec]
   [synthigy.iam :as iam]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.device-code :as dc]
   [synthigy.oauth.page.login :refer [login-html]]
   [synthigy.oauth.token :as token]))

;; =============================================================================
;; Response Mode Helpers
;; =============================================================================

(defn form-post-response
  "Generate an HTML page that auto-submits a form via POST
   (response_mode=form_post)."
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
  "Generate authorization response based on response_mode with cookies."
  [redirect-uri params response-mode cookies]
  (let [base-response
        (case response-mode
          "form_post"
          (form-post-response redirect-uri params)

          "fragment"
          {:status 302
           :headers {"Location" (str redirect-uri "#" (codec/form-encode params))}}

          ;; Default: query
          {:status 302
           :headers {"Location" (str redirect-uri "?" (codec/form-encode params))}})]
    (if cookies
      (assoc base-response :cookies cookies)
      base-response)))

;; Omits :expires entirely — a cookie with no Max-Age/Expires IS a session
;; cookie (RFC 6265); the old literal "Expires=Session" was malformed and
;; tripped strict parsers.
(defn session-cookie
  [session]
  {"idsrv.session" {:value session
                    :path "/"
                    :http-only true
                    :secure true
                    :same-site :none}})

(defn abort-authorization!
  "RFC 6749 §4.1.2.1 — hand the client its failure the same way a code would
   have been handed over, so an abandoned login stops leaving it waiting."
  [authorization-code error]
  (when-let [{{redirect-uri :redirect_uri
               :keys [state response_mode]} :request}
             (ac/get-code authorization-code)]
    (when (not-empty (str redirect-uri))
      (ac/revoke-authorization-code authorization-code)
      (log/info {:id ::authorization-aborted
                 :data {:action :revoked :subject :authorization-code :error error}}
                "Authorization abandoned by the resource owner")
      (authorization-response
       redirect-uri
       (cond-> {:error error
                :iss (core/domain+)}          ; RFC 9207 — also on error responses
         (not-empty state) (assoc :state state))
       response_mode
       nil))))

(defn complete-authorization-code-login!
  "Finish a pending authorization_code flow once a resource-owner is
   authenticated by ANY mechanism (password or federated); caller MUST have
   verified the code is still live."
  ([authorization-code resource-owner amr]
   (complete-authorization-code-login! authorization-code resource-owner amr nil))
  ([authorization-code resource-owner amr client-info]
  (let [{{redirect-uri :redirect_uri
          :keys [state audience scope response_mode]} :request
         :keys [client]}
        (ac/get-code authorization-code)
        session (core/gen-session-id)
        now (java.util.Date.)]
    (core/create-session! session
                          (merge
                           {:flow "authorization_code"
                            :code authorization-code
                            :client client
                            :user resource-owner
                            :audience audience
                            :scope scope
                            :authorized-at now
                            :amr amr
                            :acr (core/derive-acr-from-amr amr)}
                           client-info))
    (ac/mark-code-issued session authorization-code)
    (log/info {:id ::login-authenticated
               :user-xid (:xid resource-owner)
               :data {:action :authenticated
                      :subject :resource-owner
                      :flow "authorization_code"
                      :client (:id client)
                      :session (core/short-id session)
                      :code (core/short-id authorization-code)
                      :method (str/join "+" amr)}}
              "User authenticated via authorization_code flow")
    (iam/publish
     :oauth.session/created
     {:session session
      :code authorization-code
      :client client
      :audience audience
      :scope scope
      :user resource-owner})
    {:redirect-uri redirect-uri
     :response-mode response_mode
     :client client
     :cookies (session-cookie session)
     :params (cond-> {:code authorization-code
                      :iss (core/domain+)}  ; RFC 9207 Issuer Identification
               (not-empty state) (assoc :state state))})))

;; =============================================================================
;; JSON Login API (for custom login pages)
;; =============================================================================

(defn wants-json?
  "True when a custom login page called POST /oauth/login via fetch with Accept:
   application/json."
  [request]
  (when-let [accept (get-in request [:headers "accept"])]
    (str/includes? accept "application/json")))

(defn json-body
  ([status body] (json-body status body nil))
  ([status body cookies]
   (cond-> {:status status
            :headers {"Content-Type" "application/json"}
            :body (json/write-str body)}
     cookies (assoc :cookies cookies))))

(defn json-success-redirect
  "Redirect-target portion of a JSON success payload; form_post returns
   url+params separately since the page must build and submit the form."
  [redirect-uri params response-mode]
  (case response-mode
    "form_post"
    {:form_post {:url redirect-uri :params params}}

    "fragment"
    {:redirect (str redirect-uri "#" (codec/form-encode params))}

    ;; default: query
    {:redirect (str redirect-uri "?" (codec/form-encode params))}))

;; =============================================================================
;; Security Checks
;; =============================================================================

(defn security-check
  "IP/user-agent/challenge mismatch check against the decrypted state, guarding
   session hijack; nil if all checks pass."
  [{:keys [params form-params headers remote-addr] :as request}]
  (let [data (merge params form-params)
        {{:keys [flow ip user-agent challenge device-code]} :state}
        (update data :state (fn [x] (when x (core/decrypt x))))
        current-ip remote-addr
        current-user-agent (get headers "user-agent")]
    (cond
      (and ip (not= ip current-ip))
      "ip_address"

      (and user-agent (not= user-agent current-user-agent))
      "user_agent"

      (and (= flow "device_code")
           challenge
           (not (contains?
                 (:challenges (dc/get-device-code-data device-code))
                 challenge)))
      "challenge"

      :else nil)))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn login-handler
  "OAuth login page handler; GET displays the form, POST authenticates and
   creates the session (cookie set inline with the response)."
  [request]
  (binding [core/*domain* (core/original-uri request)]  ; RFC 9207 issuer identification
    (let [{:keys [params request-method form-params]} request
          data (merge params form-params)
          {:keys [username password]
           {:keys [flow device-code authorization-code] :as flow-state} :state}
          (update data :state (fn [x] (when x (core/decrypt x))))]
      (case request-method
        :get
        (if (and (:cancel data) (or authorization-code device-code))
          (or (when authorization-code
                (abort-authorization! authorization-code "access_denied"))
              (do (when device-code (dc/deny! device-code))
                  {:status 302
                   :headers {"Location"
                             (str "/oauth/status?"
                                  (codec/form-encode
                                   {:value "error"
                                    :flow (if device-code "device_code" "login")
                                    :error "cancelled"}))}}))
          {:status 200
           :headers {"Content-Type" "text/html"}
           :body (str (login-html {::state flow-state ::raw-state (:state data)
                                   ::error (not-empty (str (:error data)))}))})

        :post
        (let [resource-owner (core/validate-resource-owner username password)
              json? (wants-json? request)]
          (case flow
          ;; Authorization Code Flow
            "authorization_code"
            (let [{{response_type :response_type} :request
                   :as prepared-code}
                  (ac/get-code authorization-code)]
              (cond
                (nil? prepared-code)
                (if json?
                  (json-body 400 {:ok false :error "expired_code"})
                  {:status 302
                   :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                                  {:value "error"
                                                                   :flow "authorization_code"
                                                                   :error "expired_code"
                                                                   :error_description "Your login grace period has expired. Return to your client application and retry login procedure"}))}})

                (nil? resource-owner)
                (do
                  (log/warn {:id ::login-credentials-rejected
                             :data {:action :credentials-rejected
                                    :subject :resource-owner
                                    :flow "authorization_code"
                                    :username username
                                    :client (:id (:client prepared-code))}}
                            "Authentication failed: invalid credentials")
                  (if json?
                    (json-body 400 {:ok false :error "credentials"})
                    {:status 400
                     :headers {"Content-Type" "text/html"}
                     :body (str (login-html {::state flow-state ::error :credentials ::raw-state (:state data)}))}))

                (and resource-owner (set/intersection #{"code" "authorization_code"} response_type))
                (let [{:keys [redirect-uri response-mode cookies params]}
                      (complete-authorization-code-login! authorization-code resource-owner ["pwd"]
                                                          (core/client-info request))]
                  (if json?
                    (json-body 200
                               (merge {:ok true}
                                      (json-success-redirect redirect-uri params response-mode))
                               cookies)
                    (authorization-response redirect-uri params response-mode cookies)))

                :else
                (if json?
                  (json-body 400 {:ok false :error "unknown"})
                  {:status 400
                   :headers {"Content-Type" "text/html"}
                   :body (str (login-html {::state flow-state ::error :unknown ::raw-state (:state data)}))})))

          ;; Device Code Flow
            "device_code"
            (let [{:keys [session client expires-at]
                   {:keys [audience scope]} :request} (dc/get-device-code-data device-code)
                  security-error (security-check request)]
              (cond
                (some? session)
                (if json?
                  (json-body 400 {:ok false :error "already_authorized"})
                  {:status 302
                   :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                                  {:value "error"
                                                                   :flow "device_code"
                                                                   :error "already_authorized"}))}})

                (< expires-at (System/currentTimeMillis))
                (if json?
                  (json-body 400 {:ok false :error "device_code_expired"})
                  {:status 302
                   :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                                  {:value "error"
                                                                   :flow "device_code"
                                                                   :error "device_code_expired"}))}})

                (some? security-error)
                (if json?
                  (json-body 400 {:ok false :error security-error})
                  {:status 302
                   :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                                  {:value "error"
                                                                   :flow "device_code"
                                                                   :error security-error}))}})

                (nil? resource-owner)
                (do
                  (log/warn {:id ::login-credentials-rejected
                             :data {:action :credentials-rejected
                                    :subject :resource-owner
                                    :flow "device_code"
                                    :username username
                                    :client (:id client)}}
                            "Authentication failed: invalid credentials")
                  (if json?
                    (json-body 400 {:ok false :error "credentials"})
                    {:status 400
                     :headers {"Content-Type" "text/html"}
                     :body (str (login-html {::error :credentials}))}))

                :else
                (let [session (core/gen-session-id)
                      now (java.util.Date.)]
                  (dc/bind-session! device-code session)
                  (core/create-session! session
                                        (merge
                                         {:flow "device_code"
                                          :code device-code
                                          :client client
                                          :user resource-owner
                                          :audience audience
                                          :scope scope
                                          :authorized-at now
                                          :amr ["pwd"]
                                          :acr (core/derive-acr-from-amr ["pwd"])}
                                         (core/client-info request)))
                  (log/info {:id ::login-authenticated
                             :user-xid (:xid resource-owner)
                             :data {:action :authenticated
                                    :subject :resource-owner
                                    :flow "device_code"
                                    :username username
                                    :client (:id client)
                                    :session (core/short-id session)
                                    :code (core/short-id device-code)
                                    :method "pwd"}}
                            "User authenticated via device_code flow")
                  (iam/publish
                   :oauth.session/created
                   {:session session
                    :code device-code
                    :client client
                    :audience audience
                    :scope scope
                    :user resource-owner})
                  (let [redirect (format "/oauth/status?value=success&client=%s&user=%s"
                                         client (:name resource-owner))]
                    (if json?
                      (json-body 200 {:ok true :redirect redirect})
                      {:status 302
                       :headers {"Location" redirect}})))))

          ;; Unknown flow
            (if json?
              (json-body 400 {:ok false :error "broken_flow"})
              {:status 302
               :headers {"Location" "/oauth/status?value=error&error=broken_flow"}})))))))  ; Extra paren for binding

(defn logout-handler
  "OAuth logout handler; terminates the session and optionally redirects to
   post_logout_redirect_uri."
  [request]
  (let [{:keys [params]} request
        {:keys [post_logout_redirect_uri id_token_hint state]
         idsrv-session :idsrv/session} params
        session (or (token/get-token-session :id_token id_token_hint) idsrv-session)
        {client_id :id} (core/get-session-client session)
        {{valid-redirections "logout-redirections"} :settings} (core/get-client client_id)
        post-redirect-ok? (some #(when (= % post_logout_redirect_uri) true) valid-redirections)]
    (cond
      (nil? session)
      {:status 400
       :headers {"Content-Type" "text/html"}
       :body (json/write-str "Session is not active")}

      (and (nil? id_token_hint) (nil? idsrv-session))
      {:status 400
       :headers {"Content-Type" "text/html"}
       :body (json/write-str "Token is not valid")}

      ;; Invalid redirect URI (localhost always allowed per RFC 8252)
      (and (some? post_logout_redirect_uri)
           (not post-redirect-ok?)
           (not (core/localhost-redirect? post_logout_redirect_uri)))
      {:status 400
       :headers {"Content-Type" "text/html"}
       :body (json/write-str "Provided 'post_logout_redirect_uri' is not valid")}

      (some? post_logout_redirect_uri)
      (let [{:keys [code flow]} (core/get-session session)
            resource-owner (core/get-session-resource-owner session)]
        ((case flow
           "authorization_code" ac/delete
           "device_code" dc/delete
           identity) code)
        (log/info {:id ::logged-out
                   :user-xid (:xid resource-owner)
                   :data {:action :logged-out
                          :subject :session
                          :session (core/short-id session)
                          :client client_id
                          :flow flow
                          :redirect? true}}
                  "User logged out")
        (core/kill-session session)
        {:status 302
         :headers {"Location" (str post_logout_redirect_uri
                                   (when (not-empty state)
                                     (str "?" (codec/form-encode {:state state}))))
                   "Cache-Control" "no-cache"}
         :cookies {"idsrv.session" {:value ""
                                    :max-age 0
                                    :path "/"}}})

      :else
      (let [{:keys [code flow]} (core/get-session session)
            resource-owner (core/get-session-resource-owner session)]
        ((case flow
           "authorization_code" ac/delete
           "device_code" dc/delete) code)
        (log/info {:id ::logged-out
                   :user-xid (:xid resource-owner)
                   :data {:action :logged-out
                          :subject :session
                          :session (core/short-id session)
                          :client client_id
                          :flow flow
                          :redirect? false}}
                  "User logged out")
        (core/kill-session session)
        {:status 200
         :headers {"Content-Type" "text/html"}
         :body "User logged out!"
         :cookies {"idsrv.session" {:value ""
                                    :max-age 0
                                    :path "/"}}}))))
