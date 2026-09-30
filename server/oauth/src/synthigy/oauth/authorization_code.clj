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

(ns synthigy.oauth.authorization-code
  (:require
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [next.jdbc :as jdbc]
   [synthigy.log :as log]
   [nano-id.core :as nano-id]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.schema :as schema]
   [synthigy.db :as db]
   [synthigy.iam :as iam :refer [publish validate-password]]
   [synthigy.oauth.core :as core
    :refer [get-client]]
   [synthigy.oauth.token :as token
    :refer [grant-token
            token-error
            client-id-missmatch
            owner-not-authorized]]))

(def grant "authorization_code")

(let [alphabet "ACDEFGHJKLMNOPQRSTUVWXYZ"]
  (def gen-authorization-code (nano-id/custom alphabet 30)))

(defn minutes [x] (* 1000 60 x))

(defn create-code!
  "Insert a pending code row at /oauth/authorize time."
  [code {:keys [client agent ip request issued?]}]
  (dataset/stack-entity
   (id/entity :oauth/authorization-code)
   {:code code
    :issued (boolean issued?)
    :expires_at (java.util.Date. (+ (System/currentTimeMillis) (minutes 8)))
    :data {"client" client
           "agent" agent
           "ip" ip
           "request" request}})
  nil)

(defn get-code
  "Decoded code entry in the legacy atom shape, or nil."
  [code]
  (when code
    (when-let [row (dataset/get-entity
                    (id/entity :oauth/authorization-code)
                    {:code code}
                    {:code nil :data nil :issued nil :claimed nil :expires_at nil
                     :session [{:selections {:id nil} :args {:_join :left}}]})]
      (let [{:strs [client agent ip request]} (:data row)]
        (cond-> {:client client
                 :user/agent agent
                 :user/ip ip
                 :issued? (boolean (:issued row))
                 :claimed? (boolean (:claimed row))}
          request (assoc :request (core/decode-stored-request request))
          (:expires_at row) (assoc :expires-at (.getTime ^java.util.Date (:expires_at row)))
          (get-in row [:session :id]) (assoc :session (get-in row [:session :id])))))))

(defn delete [code]
  (when code
    (dataset/delete-entity (id/entity :oauth/authorization-code) {:code code})
    nil))

(defn get-code-request [code] (:request (get-code code)))

(defn get-code-session [code] (:session (get-code code)))

(defn get-code-client
  [code]
  (get-client (:client_id (get-code-request code))))

(defn code-was-issued? [code] (true? (:issued? (get-code code))))

(defn revoke-authorization-code
  ([code]
   (when code
     (let [session (get-code-session code)]
       (delete code)
       (core/update-session-context! session dissoc "code")
       (publish :oauth.revoke/code {:code code
                                    :session session})))))

(defn claim-code!
  "Atomically flip claimed false→true; true iff this caller won the claim."
  [code]
  (let [{:keys [table]} (schema/deployed-schema-entity
                         (id/entity :oauth/authorization-code))]
    (pos? (:next.jdbc/update-count
           (jdbc/execute-one!
            (:datasource db/*db*)
            [(str "UPDATE " table
                  " SET claimed = TRUE WHERE code = ? AND claimed IS NOT TRUE")
             code])))))

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
      (and (not-any? #(= base-redirect-uri %) redirections)
           (not (core/loopback-redirect-matches? redirect_uri redirections)))
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
      client)))

(defmethod grant-token "authorization_code"
  [request]
  (let [{:keys [code redirect_uri client_id client_secret]} request
        {{request-redirect-uri :redirect_uri
          :as original-request} :request
         client-key :client
         :keys [session expires-at]
         :as entry} (get-code code)
        ;; CAS: only the claim winner proceeds; row stays readable until revoked
        ;; at the end
        already-claimed? (when entry
                           (or (:claimed? entry)
                               (not (claim-code! code))))
        {id :id
         :as client
         _secret :secret
         _type :type
         {:strs [allowed-grants]} :settings
         session-client :id} (iam/get-client-by-key client-key)
        grants (set allowed-grants)
        {:keys [active]} (core/get-session-resource-owner session)]
    (log/debug {:id ::token-grant-request
                :data {:client-id client_id}}
               "Processing authorization-code token grant")
    (if-not session
      (token-error
       "invalid_request"
       "Trying to abuse token endpoint for code that"
       "doesn't exsist or has expired. Further actions"
       "will be logged and processed")
      (cond
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
        (not= request-redirect-uri redirect_uri)
        (token-error
         "invalid_request"
         "Redirect URI that you provided doesn't"
         "match URI that was issued to provided authorization code")
        (not= session-client client_id)
        (token-error
         "invalid_client"
         "Client ID that was provided doesn't"
         :w "match client ID that was used in authorization request")
        ;; Public clients need no secret (RFC 6749 §2.1) — PKCE is their proof
        (and (not= _type :public) (some? _secret) (empty? client_secret))
        (token-error
         "invalid_client"
         "Client secret wasn't provided")
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
          (revoke-authorization-code code)
          {:status 200
           :headers {"Content-Type" "application/json;charset=UTF-8"
                     "Pragma" "no-cache"
                     "Cache-Control" "no-store"}
           :body response})))))

(defn mark-code-issued [session code]
  (dataset/stack-entity
   (id/entity :oauth/authorization-code)
   {:code code
    :issued true
    :session {:id session}
    :expires_at (java.util.Date. (+ (System/currentTimeMillis) (minutes 5)))})
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
  "Janitor: delete rows past expires_at (set at create, tightened at issue)."
  []
  (dataset/purge-entity (id/entity :oauth/authorization-code)
                        {:_where {:expires_at {:_le (java.util.Date.)}}}
                        {:code nil}))

(comment
  (clean-codes))
