(ns synthigy.oauth.authorization-code
  (:require
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [synthigy.log :as log]
   [nano-id.core :as nano-id]
   [synthigy.dataset.id :as id]
   [synthigy.iam :refer [publish validate-password]]
   [synthigy.oauth.core :as core
    :refer [get-client]]
   [synthigy.oauth.token :as token
    :refer [grant-token
            token-error
            client-id-missmatch
            owner-not-authorized]]))

(defonce ^:dynamic *authorization-codes* (atom nil))

(def grant "authorization_code")

(defn delete [code]
  (swap! *authorization-codes* dissoc code))

(let [alphabet "ACDEFGHJKLMNOPQRSTUVWXYZ"]
  (def gen-authorization-code (nano-id/custom alphabet 30)))

(defn bind-authorization-code
  [session]
  (let [code (gen-authorization-code)]
    (swap! *authorization-codes* assoc code {:session session
                                             :at (System/currentTimeMillis)})
    (swap! core/*sessions* update session
           (fn [current]
             (->
              current
              (assoc :code code)
              (dissoc :authorization-code-used?))))
    (publish :oauth.grant/code {:session session
                                :code code})
    code))

(defn set-session-authorized-at
  [session timestamp]
  (swap! core/*sessions* assoc-in [session :authorized-at] timestamp))

(defn get-session-authorized-at
  [session]
  (get-in @core/*sessions* [session :authorized-at]))

(defn get-session-code
  ([session]
   (get-in @core/*sessions* [session :code])))

(defn get-code-request
  [code]
  (get-in @*authorization-codes* [code :request]))

(defn get-code-session
  [code]
  (get-in @*authorization-codes* [code :session]))

(defn get-code-client
  [code]
  (get-client (:client_id (get-code-request code))))

(defn revoke-authorization-code
  ([code]
   (when code
     (let [session (get-code-session code)]
       (swap! *authorization-codes* dissoc code)
       (swap! core/*sessions* update session dissoc :code)
       (publish :oauth.revoke/code {:code code
                                    :session session})))))

(defn code-was-issued? [code] (true? (get-in @*authorization-codes* [code :issued?])))

(defn validate-client [request]
  (let [{:keys [client_id redirect_uri]} request
        base-redirect-uri (core/get-base-uri redirect_uri)
        client (get-client client_id)
        client-id (id/extract client)
        {{redirections "redirections"
          allowed-grants "allowed-grants"} :settings} client
        grants (set allowed-grants)]
    (log/debug {:id ::validate-client
                :data {:client-id client_id}}
               "Validating authorization-code client")
    (cond
      (nil? client-id)
      (throw
       (ex-info
        "Client not registered"
        {:type "client_not_registered"
         :request request}))
      ;;
      (empty? redirections)
      (throw
       (ex-info
        "Client missing redirections"
        {:type "no_redirections"
         :request request}))
      ;;
      (empty? redirect_uri)
      (throw
       (ex-info
        "Client hasn't provided redirect URI"
        {:type "missing_redirect"
         :request request}))
      ;;
      (and (not (core/localhost-redirect? redirect_uri))
           (not-any? #(= base-redirect-uri %) redirections))
      (throw
       (ex-info
        "Client provided uri doesn't match available redirect URI(s)"
        {:type "redirect_missmatch"
         :request request}))
      ;;
      (not (contains? grants grant))
      (throw
       (ex-info
        "Client doesn't support authorization_code flow"
        {:type "access_denied"
         :request request}))
      ;;
      :else
      (do
        (swap! core/*clients* assoc client-id client)
        client))))

(defmethod grant-token "authorization_code"
  [request]
  (let [{:keys [code redirect_uri client_id client_secret]} request
        ;; Atomically CLAIM the code in ONE swap: mark it :claimed? and capture
        ;; its prior value. Concurrent redemptions of the same code can't both
        ;; win — only the caller whose `prior` shows it present-and-unclaimed
        ;; proceeds; the rest see :claimed? (or the code already gone once the
        ;; winner revokes it) and are rejected. We MARK rather than remove so the
        ;; code stays readable during token generation — the openid scope reads
        ;; the nonce via get-code-request (see oidc.clj). The winner removes it at
        ;; the very end via revoke-authorization-code. Closes the TOCTOU
        ;; double-mint race (old path read here and deleted only at the end).
        [prior _] (swap-vals! *authorization-codes*
                              (fn [m] (cond-> m
                                        (contains? m code) (assoc-in [code :claimed?] true))))
        already-claimed? (get-in prior [code :claimed?])
        {{request-redirect-uri :redirect_uri
          :as original-request} :request
         client-id :client
         :keys [session expires-at]} (get prior code)
        {id :id
         :as client
         _secret :secret
         _type :type
         {:strs [allowed-grants]} :settings
         session-client :id} (get @core/*clients* client-id)
        grants (set allowed-grants)
        {:keys [active]} (core/get-session-resource-owner session)]
    (log/debug {:id ::token-grant-request
                :data {:client-id client_id}}
               "Processing authorization-code token grant")
    (if-not session
      ;; If session isn't available, that is if somebody
      ;; is trying to hack in
      (token-error
       "invalid_request"
       "Trying to abuse token endpoint for code that"
       "doesn't exsist or has expired. Further actions"
       "will be logged and processed")
      ;; If there is some session than check other requirements
      (cond
        ;; Lost the claim race, or a reuse arrived while the winner was still
        ;; mid-exchange — another redemption already claimed this code.
        already-claimed?
        (token-error
         "invalid_request"
         "Authorization code is already being redeemed"
         "or has been used. Your request will be logged"
         "and processed")
        ;;
        (not (contains? grants "authorization_code"))
        (token-error
         "unauthorized_grant"
         "Client sent access token request"
         "for grant type that is outside"
         "of client configured privileges")
        ;;
        (< expires-at (System/currentTimeMillis))
        (token-error
         "invalid_request"
         "This authorization code has expired. Restart"
         "authentication process.")
        ;; If redirect uri doesn't match
        (not= request-redirect-uri redirect_uri)
        (token-error
         "invalid_request"
         "Redirect URI that you provided doesn't"
         "match URI that was issued to provided authorization code")
        ;; If client ids don't match
        (not= session-client client_id)
        (token-error
         "invalid_client"
         "Client ID that was provided doesn't"
         :w "match client ID that was used in authorization request")
        ;; Public clients don't require a secret (RFC 6749 §2.1, OAuth 2.1 §2.1)
        ;; They rely on PKCE (code_verifier) for proof instead
        (and (not= _type :public) (some? _secret) (empty? client_secret))
        (token-error
         "invalid_client"
         "Client secret wasn't provided")
        ;; If client has secret and is not public, validate it
        (and (not= _type :public) (some? _secret) (not (validate-password client_secret _secret)))
        (token-error
         "invalid_client"
         "Provided client secret is wrong")
        (not= id client_id)
        (do
          (log/debug {:id ::client-mismatch
                      :data {:request-client id :token-client client_id}}
                     "Client doesn't match client from authorization request")
          client-id-missmatch)
        ;;
        (not active)
        (do
          (log/debug {:id ::resource-owner-inactive
                      :data {:session session}}
                     "Resource owner is not active")
          (delete code)
          (core/kill-session session)
          owner-not-authorized)
        ;; Issue that token
        :else
        (let [tokens (token/generate client session original-request)
              response (json/write-str tokens)
              resource-owner (core/get-session-resource-owner session)]
          (log/info {:id ::code-exchanged
                     :user-xid (:xid resource-owner)
                     :data {:action :exchanged
                            :subject :authorization-code
                            :code (core/short-id code)
                            :session (core/short-id session)
                            :client client_id
                            :flow "authorization_code"}}
                    "Authorization code exchanged for access token")
          ;; Code is still present (we only marked it :claimed?), so the normal
          ;; revoke removes it AND does the session cleanup + publish.
          (revoke-authorization-code code)
          {:status 200
           :headers {"Content-Type" "application/json;charset=UTF-8"
                     "Pragma" "no-cache"
                     "Cache-Control" "no-store"}
           :body response})))))

(defn minutes [x] (* 1000 60 x))

(defn mark-code-issued [session code]
  (swap! *authorization-codes* update code
         (fn [data]
           (assoc data
                  :issued? true
                  :session session
                  :expires-at (->
                               (System/currentTimeMillis)
                               (+ (minutes 5))))))
  (let [resource-owner (core/get-session-resource-owner session)
        {client-id :id} (core/get-session-client session)]
    (log/info {:id ::code-issued
               :user-xid (:xid resource-owner)
               :data {:action :issued
                      :subject :authorization-code
                      :code (core/short-id code)
                      :session (core/short-id session)
                      :client client-id
                      :flow "authorization_code"
                      :ttl-s (* 60 5)}}
              "Authorization code issued and bound to session")))

(defn clean-codes
  ([] (clean-codes (minutes 8)))
  ([timeout]
   (let [now (System/currentTimeMillis)]
     (swap! *authorization-codes*
            (fn [codes]
              (reduce-kv
               (fn [result code {:keys [created-on]
                                 :as data}]
                 (if (or (nil? created-on) (> (- now created-on) timeout)) result
                     (assoc result code data)))
               nil
               codes))))))

(comment
  (clean-codes))
