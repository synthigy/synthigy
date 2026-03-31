(ns synthigy.oauth.login
  (:require
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [environ.core :refer [env]]
   [ring.util.codec :as codec]
   [synthigy.iam :as iam]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.device-code :as dc]
   [synthigy.oauth.page.login :refer [login-html]]
   [synthigy.oauth.token :as token]
   [timing.core :as timing]))

;; =============================================================================
;; Response Mode Helpers
;; =============================================================================

(defn- form-post-response
  "Generate an HTML page that auto-submits a form via POST (response_mode=form_post)."
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

(defn- authorization-response
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

;; =============================================================================
;; Security Checks
;; =============================================================================

(defn security-check
  "Validate IP address, user agent, and challenge to prevent suspicious logins.

  Returns error keyword if check fails, nil if all checks pass.

  Protects against:
  - Session hijacking from different IPs
  - Device code authorization from different browsers/devices
  - Invalid challenge codes"
  [{:keys [params form-params headers remote-addr] :as request}]
  (let [data (merge params form-params)
        {{:keys [flow ip user-agent challenge device-code]} :state}
        (update data :state (fn [x] (when x (core/decrypt x))))
        current-ip remote-addr
        current-user-agent (get headers "user-agent")]
    (cond
      ;; IP address mismatch
      (and ip (not= ip current-ip))
      "ip_address"

      ;; User agent mismatch
      (and user-agent (not= user-agent current-user-agent))
      "user_agent"

      ;; Invalid challenge (device code flow only)
      (and (= flow "device_code")
           challenge
           (not (contains?
                 (get-in @dc/*device-codes* [device-code :challenges])
                 challenge)))
      "challenge"

      ;; All checks passed
      :else nil)))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn login-handler
  "OAuth login page handler (GET/POST).

   GET: Display login form
   POST: Authenticate user and create session

   This is the most delicate conversion because it merges :enter/:leave logic
   for session cookie management. The cookie is set inline with response creation."
  [request]
  ;; Bind *domain* for proper issuer identification (RFC 9207)
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [params request-method form-params]} request
        data (merge params form-params)
        {:keys [username password]
         {:keys [flow device-code authorization-code] :as flow-state} :state}
        (update data :state (fn [x] (when x (core/decrypt x))))]
    (case request-method
      ;; GET: Display login form
      :get
      {:status 200
       :headers {"Content-Type" "text/html"}
       :body (str (login-html {::state flow-state}))}

      ;; POST: Authenticate user
      :post
      (let [resource-owner (core/validate-resource-owner username password)]
        (case flow
          ;; Authorization Code Flow
          "authorization_code"
          (let [{{response_type :response_type
                  redirect-uri :redirect_uri
                  :keys [state audience scope response_mode]} :request
                 :keys [client]
                 :as prepared-code}
                (get @ac/*authorization-codes* authorization-code)
                session (core/gen-session-id)
                now (timing/date)]
            (cond
              ;; Code expired
              (nil? prepared-code)
              {:status 302
               :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                              {:value "error"
                                                               :flow "authorization_code"
                                                               :error "expired_code"
                                                               :error_description "Your login grace period has expired. Return to your client application and retry login procedure"}))}}

              ;; Invalid credentials - show error
              (nil? resource-owner)
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (login-html {::state flow-state ::error :credentials}))}

              ;; Success - create session and redirect
              (and resource-owner (set/intersection #{"code" "authorization_code"} response_type))
              (do
                (log/debugf "[%s] Binding code %s to session" session authorization-code)
                (core/set-session session {:flow "authorization_code"
                                           :code authorization-code
                                           :client client
                                           :last-active now})
                (core/set-session-audience-scope session audience scope)
                (core/set-session-resource-owner session resource-owner)
                (core/set-session-authorized-at session now)
                ;; Set authentication context (OIDC acr/amr claims)
                ;; Currently only password auth, so amr=["pwd"], acr="1"
                ;; When MFA is added, update this to include additional methods
                (core/set-session-amr session ["pwd"])
                (core/set-session-acr session (core/derive-acr-from-amr ["pwd"]))
                (ac/mark-code-issued session authorization-code)
                (log/debugf "[%s] Validating resource owner: %s" session username)
                (iam/publish
                 :oauth.session/created
                 {:session session
                  :code authorization-code
                  :client client
                  :audience audience
                  :scope scope
                  :user resource-owner})
                ;; CRITICAL: Set cookie inline with redirect response
                ;; Support response_mode: query (default), fragment, or form_post
                (authorization-response
                  redirect-uri
                  (cond->
                    {:code authorization-code
                     :iss (core/domain+)}  ; RFC 9207 - Issuer Identification
                    (not-empty state) (assoc :state state))
                  response_mode
                  {"idsrv.session" {:value session
                                    :path "/"
                                    :http-only true
                                    :secure true
                                    :same-site :none
                                    :expires "Session"}}))

              ;; Unknown error
              :else
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (login-html {::state flow-state ::error :unknown}))}))

          ;; Device Code Flow
          "device_code"
          (let [{:keys [session client expires-at]
                 {:keys [audience scope]} :request} (get @dc/*device-codes* device-code)
                security-error (security-check request)]
            (cond
              ;; Already authorized
              (some? session)
              {:status 302
               :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                              {:value "error"
                                                               :flow "device_code"
                                                               :error "already_authorized"}))}}

              ;; Expired
              (< expires-at (System/currentTimeMillis))
              {:status 302
               :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                              {:value "error"
                                                               :flow "device_code"
                                                               :error "device_code_expired"}))}}

              ;; Security check failed
              (some? security-error)
              {:status 302
               :headers {"Location" (str "/oauth/status" "?" (codec/form-encode
                                                              {:value "error"
                                                               :flow "device_code"
                                                               :error security-error}))}}

              ;; Invalid credentials
              (nil? resource-owner)
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (login-html {::error :credentials}))}

              ;; Success - create session
              :else
              (let [session (core/gen-session-id)
                    now (timing/date)]
                (log/debugf "[%s] Device authorized for session id %s" device-code session)
                (swap! dc/*device-codes* update device-code
                       (fn [data]
                         (-> data
                             (assoc :confirmed false
                                    :session session)
                             (dissoc :challenges))))
                (core/set-session session {:flow "device_code"
                                           :code device-code
                                           :client client
                                           :last-active now})
                (core/set-session-audience-scope session audience scope)
                (core/set-session-resource-owner session resource-owner)
                (core/set-session-authorized-at session now)
                ;; Set authentication context (OIDC acr/amr claims)
                (core/set-session-amr session ["pwd"])
                (core/set-session-acr session (core/derive-acr-from-amr ["pwd"]))
                (iam/publish
                 :oauth.session/created
                 {:session session
                  :code device-code
                  :client client
                  :audience audience
                  :scope scope
                  :user resource-owner})
                {:status 302
                 :headers {"Location" (format
                                       "/oauth/status?value=success&client=%s&user=%s"
                                       client (:name resource-owner))}})))

          ;; Unknown flow
          {:status 302
           :headers {"Location" "/oauth/status?value=error&error=broken_flow"}}))))))  ; Extra paren for binding

(defn logout-handler
  "OAuth logout handler.

   Terminates session and optionally redirects to post_logout_redirect_uri.
   Requires either id_token_hint or idsrv/session cookie."
  [request]
  (let [{:keys [params]} request
        {:keys [post_logout_redirect_uri id_token_hint state]
         idsrv-session :idsrv/session} params
        session (or (token/get-token-session :id_token id_token_hint) idsrv-session)
        {client_id :id} (core/get-session-client session)
        {{valid-redirections "logout-redirections"} :settings} (core/get-client client_id)
        post-redirect-ok? (some #(when (= % post_logout_redirect_uri) true) valid-redirections)]
    (cond
      ;; No session
      (nil? session)
      {:status 400
       :headers {"Content-Type" "text/html"}
       :body (json/write-str "Session is not active")}

      ;; Missing token
      (and (nil? id_token_hint) (nil? idsrv-session))
      {:status 400
       :headers {"Content-Type" "text/html"}
       :body (json/write-str "Token is not valid")}

      ;; Invalid redirect URI
      (and (some? post_logout_redirect_uri) (not post-redirect-ok?))
      {:status 400
       :headers {"Content-Type" "text/html"}
       :body (json/write-str "Provided 'post_logout_redirect_uri' is not valid")}

      ;; Redirect after logout
      (some? post_logout_redirect_uri)
      (let [{:keys [code flow]} (core/get-session session)
            tokens (core/get-session-tokens session)]
        ((case flow
           "authorization_code" ac/delete
           "device_code" dc/delete
           identity) code)
        (token/delete tokens)
        (core/kill-session session)
        ;; Remove cookie by setting max-age to 0
        {:status 302
         :headers {"Location" (str post_logout_redirect_uri
                                   (when (not-empty state)
                                     (str "?" (codec/form-encode {:state state}))))
                   "Cache-Control" "no-cache"}
         :cookies {"idsrv.session" {:value ""
                                    :max-age 0
                                    :path "/"}}})

      ;; Logout without redirect
      :else
      (let [{:keys [code flow]} (core/get-session session)
            tokens (core/get-session-tokens session)]
        ((case flow
           "authorization_code" ac/delete
           "device_code" dc/delete) code)
        (token/delete tokens)
        (core/kill-session session)
        ;; Remove cookie by setting max-age to 0
        {:status 200
         :headers {"Content-Type" "text/html"}
         :body "User logged out!"
         :cookies {"idsrv.session" {:value ""
                                    :max-age 0
                                    :path "/"}}}))))

;; =============================================================================
;; LEGACY PEDESTAL ROUTES (Commented out - use synthigy.oauth.handlers for Ring)
;; To re-enable these routes:
;; 1. Add back: [io.pedestal.http.cors :refer [allow-origin]]
;; 2. Uncomment the code below
;;
;; (let [logout (conj core/oauth-common-interceptor core/idsrv-session-remove core/idsrv-session-read logout-interceptor)
;;       only-identity-provider (allow-origin {:allowed-origins
;;                                             (conj
;;                                              (remove empty? (str/split (env :synthigy-allowed-origins "") #"\s*,\s*"))
;;                                               ;; Below is compatibility
;;                                              (env :synthigy-iam-root-url "http://localhost:7887"))})]
;;   (def routes
;;     #{["/oauth/login" :get [only-identity-provider redirect-to-login] :route-name ::short-login]
;;       ["/oauth/login/index.html" :post [only-identity-provider middleware/cookies login-interceptor login-page] :route-name ::handle-login]
;;       ["/oauth/login/index.html" :get (conj core/oauth-common-interceptor only-identity-provider login-interceptor login-page) :route-name ::short-login-redirect]
;;       ;; Login resources
;;       ["/oauth/login/icons/*" :get [only-identity-provider core/serve-resource] :route-name ::login-images]
;;       ["/oauth/icons/*" :get [only-identity-provider core/serve-resource] :route-name ::login-icons]
;;       ["/oauth/css/*" :get [only-identity-provider core/serve-resource] :route-name ::login-css]
;;       ["/oauth/js/*" :get [only-identity-provider core/serve-resource] :route-name ::login-js]
;;       ;; Logout logic
;;       ["/oauth/logout" :post logout :route-name ::post-logout]
;;       ["/oauth/logout" :get logout :route-name ::get-logout]}))

;; Placeholder for backward compatibility - routes are now in synthigy.oauth.handlers
(def routes #{})
