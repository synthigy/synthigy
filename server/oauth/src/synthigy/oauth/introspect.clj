;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.introspect
  "RFC 7662 Token Introspection endpoint."
  (:require
   [buddy.hashers :as hashers]
   [synthigy.json :as json]
   [clojure.string :as str]
   [synthigy.log :as log]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.authentication :as authentication]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.token :as token]))

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

(defn active-response
  [{:keys [scope aud iss sub xid exp iat jti client_id sid]} username]
  {:active true
   :scope (or scope "")
   :client_id (or client_id aud)
   :username (or username sub)
   :token_type "Bearer"
   :exp exp
   :iat iat
   :sub (or sub username)
   :xid xid
   :iss (or iss (core/domain+))
   :aud (or aud client_id)
   :jti jti
   :sid sid})

(defn stateless-claims
  "Claims of a valid row-less token without a `sid` (client_credentials), else nil."
  [token]
  (when-let [claims (extract-token-claims token)]
    (when (nil? (:sid claims))
      (let [{:keys [rejected]} (authentication/verdict token (:aud claims))]
        (when-not rejected claims)))))

(defn introspect-token
  "Introspect an access or refresh token: {:active false} or {:active true ...metadata}."
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
                               token)
          stateless (when (nil? session) (stateless-claims token))]

      (cond
        stateless
        (let [{:keys [sub client_id]} stateless]
          (log/info {:id ::introspect-active
                     :data {:action :introspected
                            :subject :access-token
                            :status :active
                            :user sub
                            :client client_id}}
                    "Stateless token is active")
          (active-response stateless sub))

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
            (let [{:keys [sub client_id]} claims
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
              (active-response claims name))))))))

;; client_id alone is NOT authentication (it's public) — require a real secret match.
(defn authenticated-client
  [client_id client_secret]
  (when-let [{:keys [secret] :as client} (core/get-client client_id)]
    (when (and secret client_secret (hashers/check client_secret secret))
      client)))

(defn introspect-handler
  "OAuth 2.0 Token Introspection endpoint (RFC 7662); client authentication required."
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

      ;; Same 401 for unknown client and wrong secret — never let a caller enumerate client_ids.
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
