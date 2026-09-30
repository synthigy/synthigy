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

(ns synthigy.oauth.device-code
  (:require
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.string :as str]
   [synthigy.log :as log]
   [nano-id.core :as nano-id]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam
    :refer [validate-password]]
   [synthigy.oauth.core :as core
    :refer [get-client
            encrypt
            decrypt]]
   [synthigy.oauth.page.device :as device]
   [synthigy.oauth.token :as token
    :refer [grant-token
            token-error
            client-id-missmatch]]
   [synthigy.oauth.page.custom :as login-page]
   [synthigy.util :as util]))

(def gen-device-code (nano-id/custom "ACDEFGHIJKLMNOPQRSTUVWXYZ" 40))
(let [gen-par (nano-id/custom "ACDEFGHIJKLMNOPQRSTUVWXYZ" 4)]
  (defn gen-user-code []
    (str (gen-par) \- (gen-par))))

(defn delete [code]
  (when code
    (dataset/delete-entity (id/entity :oauth/device-code) {:device_code code})
    nil))

(defn device-row [device-code]
  (when device-code
    (dataset/get-entity (id/entity :oauth/device-code)
                        {:device_code device-code}
                        {:device_code nil :user_code nil :data nil :expires_at nil
                         :session [{:selections {:id nil} :args {:_join :left}}]})))

(defn row->entry [row]
  (when row
    (let [{:strs [client agent ip interval request challenges confirmed denied]} (:data row)]
      (cond-> {:user-code (:user_code row)
               :client client
               :device/agent agent
               :device/ip ip
               :interval interval}
        (:expires_at row) (assoc :expires-at (.getTime ^java.util.Date (:expires_at row)))
        request (assoc :request (core/decode-stored-request request))
        challenges (assoc :challenges
                          (into {}
                                (map (fn [[c state]]
                                       [c (into {} (map (fn [[k v]] [(keyword k) v])) state)]))
                                challenges))
        (some? confirmed) (assoc :confirmed confirmed)
        denied (assoc :denied true)
        (get-in row [:session :id]) (assoc :session (get-in row [:session :id]))))))

(defn get-device-code-data [device-code]
  (row->entry (device-row device-code)))

(defn find-device-code
  "Device code bound to a user code."
  [user_code]
  (when user_code
    (:device_code (dataset/get-entity (id/entity :oauth/device-code)
                                      {:user_code user_code}
                                      {:device_code nil}))))

(defn add-challenge!
  "Record a login-handoff challenge under the device code's data json."
  [device-code challenge state]
  (let [{:keys [data] :as row} (device-row device-code)]
    (when row
      (dataset/stack-entity (id/entity :oauth/device-code)
                            {:device_code device-code
                             :data (assoc-in data ["challenges" challenge] state)}))
    nil))

(defn deny!
  "RFC 8628 §3.5 — record that the person refused, so the device stops polling."
  [device-code]
  (let [{:keys [data] :as row} (device-row device-code)]
    (when row
      (dataset/stack-entity (id/entity :oauth/device-code)
                            {:device_code device-code
                             :data (-> data
                                       (dissoc "challenges")
                                       (assoc "denied" true))})
      (log/info {:id ::device-code-denied
                 :data {:action :revoked :subject :device-code
                        :code (core/short-id device-code)}}
                "Device authorization denied by the resource owner")
      true)))

(defn bind-session!
  "Login completed: attach the session and drop challenges."
  [device-code session]
  (let [{:keys [data] :as row} (device-row device-code)]
    (when row
      (dataset/stack-entity (id/entity :oauth/device-code)
                            {:device_code device-code
                             :session {:id session}
                             :data (-> data
                                       (dissoc "challenges")
                                       (assoc "confirmed" false))}))
    nil))

(defn get-code-client [device-code]
  (get-client (get-in (get-device-code-data device-code) [:request :client_id])))

(def grant "urn:ietf:params:oauth:grant-type:device_code")

(defn validate-client [request]
  (let [{:keys [client_id]
         request-secret :client_secret} request
        {:keys [secret type]
         {:strs [allowed-grants]} :settings
         :as client} (get-client client_id)
        grants (set allowed-grants)
        client-id (id/extract client)]
    (log/debug {:id ::validate-client
                :data {:client-id client_id}}
               "Validating device-code client")
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
        (log/error {:id ::validate-client-unknown
                    :data {:client-id client_id}}
                   "Couldn't validate device-code client")
        (throw
         (ex-info "Unknown client error"
                  {:request request
                   :type "server_error"}))))))

(defn code-expired? [code]
  (when-some [{:keys [expires-at]} (get-device-code-data code)]
    (< expires-at (System/currentTimeMillis))))

(defn clean-expired-codes
  "Janitor: delete device-code rows past expires_at."
  []
  (dataset/purge-entity (id/entity :oauth/device-code)
                        {:_where {:expires_at {:_le (java.util.Date.)}}}
                        {:device_code nil}))

(defmethod grant-token "urn:ietf:params:oauth:grant-type:device_code"
  [request]
  (let [{:keys [device_code client_secret]} request
        {{:keys [client_id]
          :as original-request
          id :client_id} :request
         :keys [session denied]
         :as entry} (get-device-code-data device_code)
        client (core/get-client client_id)]
    (log/debug {:id ::token-grant-request
                :data {:client-id id}}
               "Processing device-code token grant")
    (let [{_secret :secret
           {:strs [allowed-grants]} :settings} (core/get-client client_id)
          grants (set allowed-grants)]
      (cond
        ;;
        (nil? entry)
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
        denied
        (token-error
         "access_denied"
         "The resource owner denied the authorization request")
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
        :else
        (let [response (json/write-str (token/generate client session original-request))
              resource-owner (core/get-session-resource-owner session)]
          (log/info {:id ::code-exchanged
                     :user-xid (:xid resource-owner)
                     :data {:action :exchanged
                            :subject :device-code
                            :code (core/short-id device_code)
                            :session (core/short-id session)
                            :client client_id
                            :flow "device_code"}}
                    "Device code exchanged for access token")
          (delete device_code)
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
      (binding [core/*domain* (core/original-uri request)]
        (try
          (let [client (validate-client params)
                client-id (id/extract client)
                expires-at (+ (util/now) (util/minutes 5))]
            (dataset/stack-entity
             (id/entity :oauth/device-code)
             {:device_code device-code
              :user_code user-code
              :expires_at (java.util.Date. ^long expires-at)
              :data {"request" params
                     "agent" user-agent
                     "ip" remote-addr
                     "interval" 5
                     "client" client-id}})
            (log/info {:id ::device-code-issued
                       :data {:action :issued
                              :subject :device-code
                              :code (core/short-id device-code)
                              :user-code user-code
                              :client client-id
                              :scope (when-let [s (:scope params)] (str/join " " s))}}
                      "Device code issued")
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

;; -----------------------------------------------------------------------------
;; Device-confirmation token — a short-lived, SameSite=Lax cookie.
;;
;; Replaces the old hidden `challenge` form field so the confirm page can be a
;; plain static file: the server mints this cookie on the confirm-page GET (its
;; one server touchpoint), the browser POSTs only `action`, and the cookie
;; carries device-code + user-code + freshness. SameSite=Lax is the CSRF defense
;; — the browser withholds the cookie on any cross-site POST, so a malicious
;; page
;; can't forge a confirm. Anti-phishing (RFC 8628 §3.3/§5.4) stays the explicit
;; Confirm button. IP/User-Agent binding is intentionally dropped here (it was
;; beyond-spec and caused false rejects on IP shifts); the device→login handoff
;; still binds ip/ua via redirect-to-login's separate login challenge.
;; -----------------------------------------------------------------------------

(def ^:private confirm-cookie-name "device_confirm")
(def ^:private confirm-ttl-ms 300000) ; 5 minutes

(defn- confirm-cookie [device-code user-code]
  {confirm-cookie-name
   {:value (encrypt {:device-code device-code
                     :user-code user-code
                     :exp (+ (System/currentTimeMillis) confirm-ttl-ms)})
    :path "/oauth/device"
    :http-only true
    :secure true
    :same-site :lax
    :max-age (quot confirm-ttl-ms 1000)}})

(def ^:private clear-confirm-cookie
  {confirm-cookie-name {:value "" :path "/oauth/device" :max-age 0}})

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
  (letfn [(redirect-to-login [{:keys [device-code] :as state}]
            (let [challenge (nano-id/nano-id 20)
                  client (get-code-client device-code)
                  login-url (core/get-client-login-url client)]
              (add-challenge! device-code challenge (dissoc state :device-code))
              {:status 302
               :headers {"Location" (str login-url "?"
                                         (codec/form-encode
                                          {:state (encrypt
                                                   (assoc state
                                                          :flow "device_code"
                                                          :challenge challenge))}))
                         "Cache-Control" "no-cache"}}))
          (redirect-to-canceled [{:keys [user-code device-code]}]
            (deny! device-code)
            {:status 302
             :headers {"Location" (str "/oauth/device/status?value=canceled&user_code=" user-code)
                       "Cache-Control" "no-cache"}})]
    (let [{:keys [remote-addr query-params params]} request
          {:keys [action user_code]} params
          {user-agent "user-agent"} (:headers request)
          method (:request-method request)
          ;; For GET requests, user_code comes from query-params
          ;; For POST requests, user_code comes from params
          user_code (or (:user_code query-params) user_code)]
      (case method
        ;; GET: Display activation form
        :get
        (if (some? user_code)
          ;; verification_uri_complete — code pre-filled → confirm step.
          ;; Mint the SameSite confirm cookie here (the server's one
          ;; touchpoint),
          ;; then hand off to a branded device.html if the folder has one, else
          ;; render the built-in confirm page. Both carry the same cookie.
          (if-let [device-code (find-device-code user_code)]
            (let [cookie (confirm-cookie device-code user_code)]
              (if-let [redirect (login-page/custom-page-redirect
                                 "device.html" {:user_code user_code})]
                (assoc redirect :cookies cookie)
                {:status 200
                 :headers {"Content-Type" "text/html"}
                 :cookies cookie
                 :body (str (device/authorize {::complete? true
                                               ::device-code device-code
                                               ::user-code user_code}))}))
            {:status 400
             :headers {"Content-Type" "text/html"}
             :body (str (device/authorize {::error :device-code/not-available}))})
          ;; verification_uri — manual entry (no code yet, no cookie needed)
          (or (login-page/custom-page-redirect "device.html" {})
              {:status 200
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::complete? false}))}))

        ;; POST: Process activation
        :post
        (let [confirm (get-in request [:cookies confirm-cookie-name :value])]
          (cond
            ;; Confirm / cancel step — driven by the SameSite confirm cookie.
            ;; `action` present ⟺ a Confirm/Cancel button was clicked.
            (some? action)
            (if (nil? confirm)
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::error :no-confirm-session}))}
              (let [{:keys [device-code user-code exp]} (decrypt confirm)
                    {real-code :user-code :keys [expires-at]} (get-device-code-data device-code)
                    now (System/currentTimeMillis)]
                (cond
                  ;; Confirm cookie expired
                  (or (nil? exp) (< exp now))
                  {:status 400
                   :headers {"Content-Type" "text/html"}
                   :cookies clear-confirm-cookie
                   :body (str (device/authorize {::error :expired}))}

                  ;; Device code itself expired / gone
                  (or (nil? expires-at) (< expires-at now))
                  {:status 400
                   :headers {"Content-Type" "text/html"}
                   :cookies clear-confirm-cookie
                   :body (str (device/authorize {::error :expired}))}

                  ;; Cookie's user-code no longer matches the device code's
                  (not= real-code user-code)
                  {:status 400
                   :headers {"Content-Type" "text/html"}
                   :cookies clear-confirm-cookie
                   :body (str (device/authorize {::error :malicous-code}))}

                  ;; Confirm — bind ip/ua into the separate login handoff
                  ;; challenge
                  (= action "confirm")
                  (assoc (redirect-to-login {:device-code device-code
                                             :ip remote-addr
                                             :user-agent user-agent})
                         :cookies clear-confirm-cookie)

                  ;; Cancel
                  (= action "cancel")
                  (assoc (redirect-to-canceled {:user-code user-code
                                                :device-code device-code})
                         :cookies clear-confirm-cookie)

                  :else
                  {:status 400
                   :headers {"Content-Type" "text/html"}
                   :body (str (device/authorize {::user-code user-code
                                                 ::error :unknown-action}))})))

            ;; Manual entry — user typed the code (no confirm cookie yet)
            :else
            (if-let [device-code (find-device-code user_code)]
              (redirect-to-login {:device-code device-code
                                  :ip remote-addr
                                  :user-agent user-agent})
              {:status 400
               :headers {"Content-Type" "text/html"}
               :body (str (device/authorize {::user-code user_code
                                             ::error :not-available}))})))))))
