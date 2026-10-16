;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.authentication
  "Server-agnostic authentication middleware and utilities."
  (:require
   [buddy.sign.jwt :as jwt]
   [clojure.core.cache :as cache]
   [synthigy.log :as log]
   [synthigy.dataset.id :as id]
   [synthigy.iam.context :as iam.context]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as oauth]))

(defn extract-bearer-token
  "Extract the Bearer token from the Authorization header."
  [request]
  (when-let [authorization (get-in request [:headers "authorization"])]
    (second (re-find #"(?i)^Bearer\s+(.+)" authorization))))

(defonce verified (atom (cache/ttl-cache-factory {} :ttl 60000)))

(defn verified-claims
  "Signature-checked claims of `token`; the RSA check is memoised per token while its signing key is live."
  [token]
  (when (and token (> (count token) 6))
    (let [hit (cache/lookup @verified token)]
      (if (and hit (encryption/get-keypair-by-kid encryption/*encryption-provider* (:kid hit)))
        (:claims hit)
        (when-let [claims (encryption/unsign-data token)]
          (swap! verified cache/miss token {:claims claims :kid (:kid (jwt/decode-header token))})
          claims)))))

(defn validate-token
  "Validate a bearer token by signature and expiry; stateless — revocation is enforced at refresh."
  [token]
  (boolean
   (when-let [{:keys [exp]} (verified-claims token)]
     (or (nil? exp)
         (> exp (quot (System/currentTimeMillis) 1000))))))

(def first-party-audiences
  "Platform + identity audiences, for log attribution only — NEVER pass it to authorize a data-plane request."
  #{oauth/platform-audience oauth/oidc-audience})

(defn verdict
  "Judge `token` for `audience`: {:principal :claims}, or {:rejected reason} with claims once the signature holds."
  [token audience]
  (if-let [claims (verified-claims token)]
    (try
      (let [{:keys [exp aud client_id sub xid]} claims
            accepted (cond
                       (nil? audience) #{oauth/platform-audience}
                       (set? audience) audience
                       :else           #{audience})
            user-id  (or xid sub)
            refuse   (fn [reason] {:rejected reason :claims claims})]
        (cond
          (and exp (<= exp (quot (System/currentTimeMillis) 1000))) (refuse :expired)
          (not (contains? accepted aud))                             (refuse :audience)
          (and client_id (not (:active (oauth/get-client client_id)))) (refuse :client)
          :else (if-let [user-ctx (and user-id (iam.context/get-user-context user-id))]
                  (if (false? (:active user-ctx))
                    (refuse :inactive)
                    {:principal user-ctx :claims claims})
                  (refuse :user))))
      (catch Exception e
        (log/error! {:id ::token-context-failed
                     :msg "Error extracting context from token"} e)
        {:rejected :error :claims claims}))
    {:rejected :signature}))

(defn token->user-context
  "User/service context of an acceptable token, or nil; `:audience` defaults to the platform audience."
  [token & {:keys [audience]}]
  (let [v (verdict token audience)]
    (when-not (:rejected v) v)))

(defn log-rejection
  [{:keys [rejected claims]}]
  (let [{:keys [jti client_id sid aud exp xid sub]} claims
        user (or xid sub)]
    (log/warn {:id ::token-rejected
               :data (cond-> {:action :rejected :subject :token :reason rejected}
                       jti       (assoc :jti jti)
                       client_id (assoc :client client_id)
                       user      (assoc :user user)
                       ;; short form only — the full id IS the idsrv.session cookie
                       sid       (assoc :session (oauth/short-id sid))
                       aud       (assoc :audience aud)
                       (= :expired rejected)
                       (assoc :expired-at (str (java.time.Instant/ofEpochSecond (long exp)))))}
              "Bearer token rejected")
    nil))

(defn authenticate-request
  "Authenticate request to {:principal :claims} or nil; no audience means platform, never 'any'."
  ([request] (authenticate-request request nil))
  ([request audience]
   (when-let [token (extract-bearer-token request)]
     (let [v (verdict token audience)]
       (if (:rejected v)
         (log-rejection v)
         v)))))

(defn challenge
  "RFC 6750 §3 WWW-Authenticate value for a refused request."
  [request]
  (if (extract-bearer-token request) "Bearer error=\"invalid_token\"" "Bearer"))

(defn session-cookie
  [request]
  (some->> (get-in request [:headers "cookie"])
           (re-find #"(?:^|;\s*)idsrv\.session=([^;]+)")
           second
           not-empty))

(defn log-context
  "Who a request is, for its log lines: principal, client, and session/token ids — never a credential."
  [request]
  (let [{{:keys [xid tenant]} :principal
         {:keys [sid jti client_id]} :claims}
        (try (token->user-context (extract-bearer-token request) :audience first-party-audiences)
             (catch Throwable _ nil))
        session (or sid (session-cookie request))]
    (cond-> {}
      xid       (assoc :user-xid xid)
      tenant    (assoc :tenant tenant)
      client_id (assoc :client client_id)
      jti       (assoc :jti jti)
      ;; short form only — the full id IS the idsrv.session cookie
      session   (assoc :session (oauth/short-id session)))))

(defn wrap-authenticate
  "Ring middleware adding ::principal + ::claims for a valid bearer token."
  [handler]
  (fn [request]
    (if-let [{:keys [principal claims]} (authenticate-request request)]
      (handler (assoc request
                      ::principal principal
                      ::claims claims))
      (handler request))))

(def authenticate-interceptor
  "Pedestal interceptor for OAuth token-based authentication. Adds ::principal + ::claims to the context."
  {:name ::authenticate
   :enter
   (fn [ctx]
     (if-let [{:keys [principal claims]} (authenticate-request (:request ctx))]
       (assoc ctx
              ::principal principal
              ::claims claims)
       ctx))})

(defn assoc-iam
  "Associate IAM context {:principal :claims} into a context map."
  [ctx iam]
  (if iam
    (assoc ctx
           ::principal (:principal iam)
           ::claims (:claims iam))
    ctx))

(defn get-principal
  "Get the materialized principal map from context."
  [ctx]
  (::principal ctx))

(defn get-claims
  "Get the JWT claims map from context."
  [ctx]
  (::claims ctx))

(defn authenticated?
  "Check if context has an authenticated principal."
  [ctx]
  (some? (::principal ctx)))

(def valid-token?
  "Alias for validate-token."
  validate-token)

(def get-token-context
  "Alias for token->user-context."
  token->user-context)
