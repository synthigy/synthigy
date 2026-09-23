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

(ns synthigy.oauth.introspect
  "RFC 7662 Token Introspection endpoint. See
   docs/core/synthigy/oauth/introspect.md."
  (:require
   [buddy.hashers :as hashers]
   [synthigy.json :as json]
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.token :as token]))

;; =============================================================================
;; Introspection Logic
;; =============================================================================

(defn inactive-response
  []
  {:active false})

(defn extract-token-claims
  "Decode a JWT token's claims, or nil if invalid."
  [token-string]
  (try
    (encryption/unsign-data token-string)
    (catch Exception _
      nil)))

(defn token-expired?
  [claims]
  (when-let [exp (:exp claims)]
    (< (* 1000 exp) (System/currentTimeMillis))))

(defn introspect-token
  "Introspect an access or refresh token; returns {:active false} or {:active
   true ...metadata}."
  [token token-type-hint]
  (log/debug {:id ::introspect-start
              :data {:action :introspecting
                     :subject :access-token
                     :hint token-type-hint}}
             "Introspecting token")

  (cond
    (or (nil? token) (empty? token))
    (do
      (log/debug {:id ::introspect-empty-token
                  :data {:action :introspected
                         :subject :access-token
                         :status :inactive
                         :reason :empty-token}}
                 "Empty token")
      (inactive-response))

    :else
    (let [[found-key session] (token/find-token
                               (when token-type-hint (keyword token-type-hint))
                               token)]

      (cond
        (nil? session)
        (do
          (log/info {:id ::introspect-not-in-store
                     :data {:action :introspected
                            :subject :access-token
                            :status :inactive
                            :reason :not-in-store}}
                    "Token not found in store")
          (inactive-response))

        :else
        (let [claims (extract-token-claims token)
              expired? (token-expired? claims)]

          (cond
            (nil? claims)
            (do
              (log/info {:id ::introspect-no-claims
                         :data {:action :introspected
                                :subject :access-token
                                :status :inactive
                                :reason :no-claims
                                :session (core/short-id session)}}
                        "Could not extract claims from token")
              (inactive-response))

            expired?
            (do
              (log/info {:id ::introspect-expired
                         :data {:action :introspected
                                :subject :access-token
                                :status :inactive
                                :reason :expired
                                :exp (:exp claims)
                                :session (core/short-id session)}}
                        "Token is expired")
              (inactive-response))

            :else
            (let [{:keys [scope aud iss sub xid exp iat jti client_id sid]} claims
                  {:keys [name]} (core/get-session-resource-owner session)]
              (log/info {:id ::introspect-active
                         :data {:action :introspected
                                :subject :access-token
                                :status :active
                                :user (or name sub)
                                :token-key found-key
                                :client client_id
                                :session (core/short-id session)}}
                        "Token is active")
              {:active true
               :scope (or scope "")
               :client_id (or client_id aud)
               :username (or name sub)
               :token_type "Bearer"
               :exp exp
               :iat iat
               :sub (or sub name)
               :xid xid
               :iss (or iss (core/domain+))
               :aud (or aud client_id)
               :jti jti
               :sid sid})))))))

;; =============================================================================
;; Client Authentication (RFC 7662 §2.1)
;; =============================================================================

;; client_id alone is NOT authentication — it's echoed on every authorization
;; redirect, so it's public by design; a real client_secret match is required
;; (public clients, which have no secret, are rejected outright).
(defn authenticated-client
  [client_id client_secret]
  (when-let [{:keys [secret] :as client} (core/get-client client_id)]
    (when (and secret client_secret (hashers/check client_secret secret))
      client)))

;; =============================================================================
;; Ring Handler
;; =============================================================================

(defn introspect-handler
  "OAuth 2.0 Token Introspection endpoint handler (RFC 7662); client
   authentication is required."
  [request]
  (let [{:keys [token token_type_hint client_id client_secret]} (:params request)]

    (log/debug {:id ::introspect-request
                :data {:action :requested
                       :subject :access-token
                       :client client_id
                       :hint token_type_hint}}
               "Introspect request received")

    (cond
      (or (nil? client_id) (empty? client_id))
      (do
        (log/warn {:id ::introspect-missing-client
                   :data {:action :rejected
                          :subject :access-token
                          :reason :missing-client}}
                  "Missing client credentials")
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" "Basic realm=\"OAuth\""
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str {:error "invalid_client"
                                :error_description "Client authentication required"})})

      ;; Same 401 for unknown client and wrong secret — distinguishing them
      ;; would let a caller enumerate valid client_ids.
      (nil? (authenticated-client client_id client_secret))
      (do
        (log/warn {:id ::introspect-client-auth-failed
                   :data {:action :rejected
                          :subject :access-token
                          :reason :client-auth-failed
                          :client client_id}}
                  "Client authentication failed")
        {:status 401
         :headers {"Content-Type" "application/json"
                   "WWW-Authenticate" "Basic realm=\"OAuth\""
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str {:error "invalid_client"
                                :error_description "Client authentication failed"})})

      (or (nil? token) (empty? token))
      (do
        (log/warn {:id ::introspect-missing-token-param
                   :data {:action :rejected
                          :subject :access-token
                          :reason :missing-token
                          :client client_id}}
                  "Missing token parameter")
        {:status 400
         :headers {"Content-Type" "application/json"
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str {:error "invalid_request"
                                :error_description "Missing required parameter: token"})})

      :else
      (let [result (introspect-token token token_type_hint)]
        {:status 200
         :headers {"Content-Type" "application/json"
                   "Cache-Control" "no-store"
                   "Pragma" "no-cache"}
         :body (json/write-str result)}))))
