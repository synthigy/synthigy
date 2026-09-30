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

(ns synthigy.oauth.token
  (:require
   [buddy.core.codecs]
   [buddy.hashers :as hashers]
   [buddy.sign.jwt :as jwt]
   [camel-snake-kebab.core :as csk]
   [synthigy.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.set :as set]
   [clojure.string :as str]
   [synthigy.log :as log]
   [nano-id.core :as nano-id]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.iam.service-user :as service-user]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.core :as core
    :refer [get-client
            session-kill-hook
            access-token-expiry
            refresh-token-expiry
            process-scope
            defscope
            sign-token]]))

(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"]
  (def gen-token (nano-id/custom alphabet 50)))

(defn token-entity [token-key]
  (if (= token-key :access_token)
    (id/entity :oauth/access-token)
    (id/entity :oauth/refresh-token)))

(defn token-relation [token-key]
  (if (= token-key :access_token) :access_tokens :refresh_tokens))

(defn live-token-row
  "Row for a non-revoked token value, with its session id; nil when the token is
   unknown or revoked."
  [token-key token]
  (when token
    (let [row (dataset/get-entity (token-entity token-key) {:value token}
                                  {:value nil :audience nil :revoked nil
                                   :session [{:selections {:id nil} :args {:_join :left}}]})]
      (when (and row (not (:revoked row)))
        row))))

(defn find-token
  "Locate a live token trying `token-key` hint first, then both kinds; returns
   [token-key session-id] or nil."
  [token-key token]
  (some (fn [tk]
          (when-some [row (live-token-row tk token)]
            [tk (get-in row [:session :id])]))
        (distinct (filter some? [token-key :access_token :refresh_token]))))

(defn token-revoked?
  "True only when a row for this value exists AND is flagged revoked; row-less
   tokens (client_credentials) stay acceptable on a valid signature."
  [token-key token]
  (boolean
   (when token
     (:revoked (dataset/get-entity (token-entity token-key) {:value token}
                                   {:revoked nil})))))

(defn token-error [status code & description]
  {:status (if (number? status) status 400)
   :headers {"Content-Type" "application/json;charset=UTF-8"
             "Pragma" "no-cache"
             "Cache-Control" "no-store"}
   :body (json/write-str
          {:error (if (number? status) code status)
           :error_description (str/join "\n"
                                        (if (number? status) description
                                            (conj description code)))})})

(defn set-session-tokens
  "Persist freshly signed tokens as rows nested under the session in one stack
   write."
  [session audience {access-token :access_token
                     refresh-token :refresh_token}]
  (letfn [(token-doc [signed]
            {:value signed
             :audience audience
             :expires_at (core/expires-at signed)
             :signed_by {:kid (:kid (jwt/decode-header signed))}})]
    (dataset/stack-entity
     (id/entity :oauth/session)
     (cond-> {:id session}
       access-token (assoc :access_tokens [(token-doc access-token)])
       refresh-token (assoc :refresh_tokens [(token-doc refresh-token)])))
    nil))

(defn get-token-session
  [token-key token]
  (get-in (live-token-row token-key token) [:session :id]))

(defn get-token-audience
  [token-key token]
  (:audience (live-token-row token-key token)))

(defn session-token-values
  "Values of live tokens of one kind for a session, newest first; audience nil
   matches NULL audience, :any skips the filter."
  [session token-key audience]
  (let [rel (token-relation token-key)
        audience-where (case audience
                         :any nil
                         nil {:audience :is_null}
                         {:audience {:_eq audience}})]
    (->> (core/session-row session
                           {rel [{:selections {:value nil}
                                  :args {:_where (if audience-where
                                                   {:_and [core/not-revoked audience-where]}
                                                   core/not-revoked)
                                         :_order_by {:expires_at :desc}
                                         :_join :left}}]})
         rel
         (map :value))))

(defn get-session-access-token
  ([session] (get-session-access-token session nil))
  ([session audience]
   (first (session-token-values session :access_token audience))))

(defn get-session-refresh-token
  ([session] (get-session-refresh-token session nil))
  ([session audience]
   (first (session-token-values session :refresh_token audience))))

(defn revoke-token
  [token-key token]
  (when-let [row (live-token-row token-key token)]
    (dataset/stack-entity (token-entity token-key) {:value token :revoked true})
    (iam/publish
     :oauth.revoke/token
     {:token/key token-key
      :token/data token
      :audience (:audience row)
      :session (get-in row [:session :id])}))
  nil)

(defn revoke-session-tokens
  ([session] (revoke-session-tokens session :any))
  ([session audience]
   (doseq [token-key [:access_token :refresh_token]
           token (session-token-values session token-key audience)]
     (revoke-token token-key token))
   nil))

(defmethod session-kill-hook 0
  [_ session]
  (revoke-session-tokens session))

(defmethod sign-token :refresh_token
  [session _ data]
  (let [client (core/get-session-client session)]
    (encryption/sign-data
     (hash-map :value data
               :session session
               :exp (->
                     (System/currentTimeMillis)
                     (quot 1000)
                     (+ (refresh-token-expiry client))))
     {:alg :rs256})))

(defmethod sign-token :access_token
  [_ _ data]
  (encryption/sign-data data {:alg :rs256}))

(def unsupported (core/json-error 500 "unsupported" "This feature isn't supported at the moment"))

(def client-id-missmatch
  (token-error
   "unauthorized_client"
   "Refresh token that you have provided"
   "doesn't belong to given client"))

(def owner-not-authorized
  (token-error
   "resource_owner_unauthorized"
   "Provided refresh token doesn't have active user"))

(def refresh-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token refresh requests."))

(def authorization-code-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token authorization code requests"))

(def device-code-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token device code requests"))

(def client-credentials-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token client credentials requests"))

(def cookie-session-missmatch
  (token-error
   "invalid_request"
   "You session is not provided by this server."
   "This action will be logged and processed!"))

(defmulti grant-token (fn [{:keys [grant_type]}] grant_type))

(defmethod grant-token :default [_] unsupported)

;; =============================================================================
;; Client Credentials Token Generation
;; =============================================================================

;; Probes the deployed Scope entity for the `Confidential Only` attribute
;; before querying it — querying an unknown attribute throws, and older
;; deployments (no flag column) must keep issuing tokens.
(defn confidential-only-live?
  []
  (boolean
   (some #(and (:active %) (= "Confidential Only" (:name %)))
         (:attributes (dataset/deployed-entity (id/entity :iam/scope))))))

;; Deliberately uncached — issuance is low-rate and staleness here would be a
;; security bug.
(defn confidential-only-scope-names
  []
  (if-not (confidential-only-live?)
    #{}
    (into #{} (keep :name)
          (dataset/search-entity :iam/scope
                                 {:confidential_only {:_eq true}}
                                 {:name nil}))))

(defn confidential-client?
  [client]
  (boolean (#{:confidential "confidential"} (:type client))))

;; A confidential-only scope never enters a token issued to a PUBLIC client —
;; a browser-held token is one XSS away from exfiltration, and privileged
;; scopes in a stolen token are tenant takeover. Silently narrowed, not an
;; error (OAuth convention).
(defn filter-public-client-scopes
  [client scopes]
  (if (confidential-client? client)
    scopes
    (let [privileged (confidential-only-scope-names)
          dropped (set/intersection (set scopes) privileged)]
      (when (seq dropped)
        (log/info {:id ::scopes-narrowed-for-public-client
                   :data {:action :denied
                          :subject :request
                          :client (:id client)
                          :dropped (vec dropped)}}
                  "Confidential-only scopes dropped from public-client token"))
      (set/difference (set scopes) privileged))))

(defn resolve-client-api-scopes
  "{scope-id scope-name} for the client's API at `audience`, or nil if not
   linked; keyed by id since Scope.name isn't unique across APIs."
  [client-id audience]
  (let [{[api] :apis} (dataset/get-entity
                       :iam/app
                       {:id client-id}
                       {:apis [{:selections
                                {:audience [{:args {:_eq audience}}]
                                 :scopes [{:selections {(id/key) nil
                                                        :name nil}}]}}]})]
    (when api
      (into {} (map (juxt id/extract :name)) (:scopes api)))))

(defn client-linked-audiences
  "Every audience the client (by OAuth client_id) is linked to via its
   :apis relation, as a vector — [] if none. See
   docs/plans/PLAN-AUDIENCE-BINDING.md."
  [client-id]
  (let [{:keys [apis]} (dataset/get-entity
                        :iam/app
                        {:id client-id}
                        {:apis [{:selections {:audience nil}}]})]
    (mapv :audience apis)))

(defn resolve-audience
  "The EFFECTIVE audience for a token request, or nil when the request
   must be rejected (caller renders `invalid-target-error`). `requested`
   nil, OR equal to `core/oidc-audience`: resolves to `core/oidc-audience`
   unconditionally — the universal identity audience needs no
   `:iam/app`→`:apis` link (Robert, 2026-09-09: '/id could be default for
   OIDC requiring only info about user, not access to data endpoint').
   Any OTHER requested audience: must resolve via `resolve-client-api-scopes`
   (the client is linked to that API) or nil. Every non-nil return is an
   audience a real token may carry — /data access is opt-in via an
   EXPLICIT, entitled audience, never a default. See
   docs/plans/PLAN-AUDIENCE-BINDING.md decisions 1-2."
  [client-id requested]
  (cond
    (or (nil? requested) (= requested core/oidc-audience)) core/oidc-audience
    (resolve-client-api-scopes client-id requested) requested
    :else nil))

(defn invalid-target-error
  "The `invalid_target` (RFC 8707 §2) response for a `resolve-audience`
   rejection — names what was requested and, when the client has other
   links, what it IS entitled to."
  [client-id requested]
  (token-error
   400
   "invalid_target"
   (let [linked (client-linked-audiences client-id)
         api (when requested
               (dataset/get-entity :iam/api {:audience requested} {:name nil}))]
     (cond-> (str "Client is not authorized for audience " requested)
       (seq linked) (str " (linked to: " (str/join ", " linked) ")")
       (nil? api) (str " — no API is registered with that audience")
       api (str " — link the client to the \"" (:name api) "\" API: "
                "synthigy iam add-client --api \"" (:name api) "\", "
                "or the console Apps page")))))

(defn generate-client-token
  "Generate a token for client_credentials grant — stateless, no session, no
   refresh token. `audience` MUST already be resolved (see
   synthigy.oauth.token/resolve-audience) — nil is never a valid audience
   here."
  [client service-user {:keys [audience scope client_id]}]
  (let [roles (:roles service-user)
        ;; `{}` when unresolved (the audience carries no capability scopes —
        ;; oidc-audience, or somehow an unlinked one slipped through), NEVER
        ;; a bare-role fallback: an id->name of nil used to mean "grant every
        ;; role scope unfiltered" — the exact leak this whole plan exists to
        ;; close. See docs/plans/PLAN-AUDIENCE-BINDING.md.
        id->name (or (resolve-client-api-scopes client_id audience) {})
        user-scopes (into #{} (keep id->name) (access/roles-scope-ids roles))
        granted-scopes (cond-> user-scopes
                         (seq scope) (set/intersection scope))
        access-exp (-> (System/currentTimeMillis)
                       (quot 1000)
                       (+ (access-token-expiry client)))
        access-token {:aud audience
                      :exp access-exp
                      :iss (core/domain+)
                      :sub (:name service-user)
                      :xid (id/extract service-user)
                      :iat (quot (System/currentTimeMillis) 1000)
                      :jti (gen-token)
                      :client_id client_id
                      :scope (str/join " " granted-scopes)}
        signed (sign-token nil :access_token access-token)]
    (iam/publish
     :oauth.grant/tokens
     {:tokens {:access_token signed}
      :session nil})
    {:access_token signed
     :token_type "Bearer"
     :scope (str/join " " granted-scopes)
     :expires_in (access-token-expiry client)}))

(defn generate
  "`audience` may be nil, `core/oidc-audience`, or an explicit request —
   resolved here via `resolve-audience` (never nil past this point;
   defaults to the universal identity audience). A caller-controlled
   audience this client isn't linked to throws rather than mint the
   nil-audience token this whole plan exists to stop — the PROPER 400
   `invalid_target` response belongs at the front door (authorization_code/
   device_code, BEFORE user interaction), not built yet; this is the
   stopgap until then. See docs/plans/PLAN-AUDIENCE-BINDING.md steps 1/3."
  [{{allowed-grants "allowed-grants"} :settings
    :as client} session {:keys [audience scope client_id sub nonce]}]
  (let [audience (or (resolve-audience client_id audience)
                     (throw (ex-info "invalid_target: client not authorized for requested audience"
                                      {:client_id client_id :requested-audience audience})))
        ;; Applied before ANY use of scope, so every downstream use sees the
        ;; narrowed set — see filter-public-client-scopes.
        scope (filter-public-client-scopes client scope)
        access-exp (->
                    (System/currentTimeMillis)
                    (quot 1000)
                    (+ (access-token-expiry client)))
        resource-owner (core/get-session-resource-owner session)
        user-name (or sub (:name resource-owner))
        ;; Resolved ONCE and threaded through every process-scope call below.
        ;; `{}` (never nil) when the audience carries no capability scopes
        ;; (oidc-audience) — process-scope :default's `(contains? api-scopes
        ;; scope)` then correctly grants NOTHING capability-shaped, instead
        ;; of nil being misread as "no filter, grant everything".
        ctx {:session session
             :client client
             :audience audience
             :roles (:roles resource-owner)
             :api-scopes (set (vals (resolve-client-api-scopes client_id audience)))}
        access-token {:session session
                      :aud audience
                      :exp access-exp
                      :iss (core/domain+)
                      :sub user-name
                      :xid (id/extract resource-owner)
                      :iat (quot (System/currentTimeMillis) 1000)
                      :jti (gen-token)
                      :client_id client_id
                      :sid session
                      ;; Seeded empty — process-scope is the SOLE writer
                      ;; (a string seed here breaks :default's `conj`).
                      :scope []}
        refresh? (some #(= "refresh_token" %) allowed-grants)]
    (log/debug {:id ::generated-access-token
                :data {:action :generated
                       :subject :access-token
                       :token-key :access_token
                       :client client_id
                       :session (when session (core/short-id session))
                       :audience audience}}
               "Generated access token")
    (if (pos? access-exp)
      (let [refresh-token (when (and refresh? session (contains? scope "offline_access"))
                            (log/debug {:id ::creating-refresh-token
                                        :data {:action :generated
                                               :subject :access-token
                                               :token-key :refresh_token
                                               :client client_id
                                               :session (core/short-id session)}}
                                       "Creating refresh token")
                            (gen-token))
            tokens (reduce
                    (fn [tokens scope]
                      (process-scope ctx tokens scope))
                    (if refresh-token
                      {:access_token access-token
                       :refresh_token refresh-token}
                      {:access_token access-token})
                    scope)
            tokens (cond-> tokens
                     (and nonce (:id_token tokens)) (assoc-in [:id_token :nonce] nonce))
            granted (get-in tokens [:access_token :scope])
            granted-str (str/join " " granted)
            tokens (assoc-in tokens [:access_token :scope] granted-str)
            signed-tokens (reduce-kv
                           (fn [tokens token data]
                             (assoc tokens token (sign-token session token data)))
                           tokens
                           tokens)]
        (when session
          (revoke-session-tokens session audience)
          (set-session-tokens session audience signed-tokens)
          ;; GRANTED, not requested — a refresh must not be able to widen
          ;; scope beyond what actually cleared process-scope this time.
          (core/set-session-audience-scope session audience granted))
        (iam/publish
         :oauth.grant/tokens
         {:tokens signed-tokens
          :session session})
        (assoc signed-tokens
               :token_type "Bearer"
               :scope granted-str
               :expires_in (access-token-expiry client)))
      (let [tokens (reduce
                    (fn [tokens scope]
                      (process-scope ctx tokens scope))
                    {:access_token access-token}
                    scope)
            tokens (cond-> tokens
                     (and nonce (:id_token tokens)) (assoc-in [:id_token :nonce] nonce))
            granted (get-in tokens [:access_token :scope])
            granted-str (str/join " " granted)
            tokens (assoc-in tokens [:access_token :scope] granted-str)
            signed-tokens (reduce-kv
                           (fn [tokens token data]
                             (assoc tokens token (sign-token session token data)))
                           tokens
                           tokens)]
        (iam/publish
         :oauth.grant/tokens
         {:tokens signed-tokens
          :session session})
        (assoc signed-tokens
               :expires_in (access-token-expiry client)
               :scope granted-str
               :token_type "Bearer")))))

(def ^:dynamic *refresh-reuse-grace-ms* 10000)

(defn reused-refresh-session
  "Session of a revoked refresh token when its rotation is older than the grace window, or nil."
  [token]
  (when token
    (let [row (dataset/get-entity (token-entity :refresh_token) {:value token}
                                  {:revoked nil :audience nil
                                   :session [{:selections {:id nil} :args {:_join :left}}]})
          session (get-in row [:session :id])]
      (when (and (:revoked row) session)
        (let [current (get-session-refresh-token session (:audience row))
              rotated-at (when current
                           (- (inst-ms (core/expires-at current))
                              (* 1000 (refresh-token-expiry (core/get-session-client session)))))]
          (when (or (nil? rotated-at)
                    (> (- (System/currentTimeMillis) rotated-at) *refresh-reuse-grace-ms*))
            session))))))

(defmethod grant-token "refresh_token"
  [{:keys [refresh_token scope audience]
    cookie-session :idsrv/session
    :as request}]
  (if (core/expired? refresh_token)
    (do
      (core/kill-session (get-token-session :refresh_token refresh_token))
      (token-error
       400
       "invalid_request"
       "Provided token is expired!"))
    (if-let [session (get-token-session :refresh_token refresh_token)]
      (let [{{:strs [allowed-grants]} :settings
             :as client} (core/get-session-client session)
            {:keys [active]} (core/get-session-resource-owner session)
            audience (or
                      audience
                      (get-token-audience :refresh_token refresh_token))
            ;; A client MAY narrow via `scope` (RFC 6749 §6) but can never
            ;; widen past the session's stored (GRANTED) scope — intersect,
            ;; never trust the request verbatim.
            original-scope (core/get-session-audience-scope session audience)
            scope (if scope
                    (set/intersection (set scope) (set original-scope))
                    original-scope)
            current-refresh-token (get-session-refresh-token session audience)
            grants (set allowed-grants)]
        (cond
          (not (contains? grants "refresh_token"))
          (do
            (core/kill-session session)
            refresh-not-supported)

          (not active)
          (do
            (core/kill-session session)
            owner-not-authorized)

          (and cookie-session (not= cookie-session session))
          cookie-session-missmatch

          (not= refresh_token current-refresh-token)
          (token-error
           400
           "invalid_request"
           "Provided token doesn't match session refresh token"
           "Your request will be logged and processed")

          ;; Validated — NOW rotate. Revoking any earlier (before every
          ;; predicate above passed) would burn the caller's still-valid
          ;; refresh token as a side effect of a request that merely failed
          ;; validation.
          :else
          (let [resource-owner (core/get-session-resource-owner session)]
            (when current-refresh-token (revoke-token :refresh_token current-refresh-token))
            (revoke-session-tokens session audience)
            (core/touch-session! session)
            (log/info {:id ::token-refreshed
                       :user-xid (:xid resource-owner)
                       :data {:action :refreshed
                              :subject :access-token
                              :session (core/short-id session)
                              :client (:id client)
                              :audience audience
                              :scope (when scope (str/join " " scope))}}
                      "Access token refreshed")
            {:status 200
             :headers {"Content-Type" "application/json;charset=UTF-8"
                       "Pragma" "no-cache"
                       "Cache-Control" "no-store"}
             :body (json/write-str (generate client session (-> request (dissoc :nonce) (assoc :scope scope))))})))
      (do
        (when-let [session (reused-refresh-session refresh_token)]
          (log/warn {:id ::refresh-token-reused
                     :data {:action :rejected
                            :subject :refresh-token
                            :reason :reuse-detected
                            :session (core/short-id session)}}
                    "Rotated refresh token presented again — session killed")
          (core/kill-session session))
        (token-error
         400
         "invalid_grant"
         "There is no valid session for refresh token that"
         "was provided")))))

(defn validate-client-credentials
  "Validate client credentials for client_credentials grant; public clients are
   explicitly blocked."
  [{:keys [client_id client_secret]}]
  (when-let [client (get-client client_id)]
    (let [{client-secret :secret
           client-type :type
           {allowed-grants "allowed-grants"} :settings} client
          grants (set allowed-grants)]
      (cond
        (#{:public "public"} client-type)
        (do
          (log/debug {:id ::cc-public-client-rejected
                      :data {:action :rejected
                             :subject :access-token
                             :flow "client_credentials"
                             :reason :public-client
                             :client client_id}}
                     "Public clients cannot use client_credentials grant")
          nil)

        (not (contains? grants "client_credentials"))
        (do
          (log/debug {:id ::cc-grant-not-allowed
                      :data {:action :rejected
                             :subject :access-token
                             :flow "client_credentials"
                             :reason :grant-not-allowed
                             :client client_id}}
                     "Client credentials grant not allowed for client")
          nil)

        (and (some? client-secret) (empty? client_secret))
        (do
          (log/debug {:id ::cc-secret-missing
                      :data {:action :rejected
                             :subject :access-token
                             :flow "client_credentials"
                             :reason :secret-missing
                             :client client_id}}
                     "Client secret required but not provided")
          nil)

        (and (some? client-secret)
             (not (hashers/check client_secret client-secret)))
        (do
          (log/debug {:id ::cc-secret-invalid
                      :data {:action :rejected
                             :subject :access-token
                             :flow "client_credentials"
                             :reason :secret-invalid
                             :client client_id}}
                     "Invalid client secret provided")
          nil)

        :else
        (do
          (log/debug {:id ::cc-validated
                      :data {:action :validated
                             :subject :access-token
                             :flow "client_credentials"
                             :client client_id}}
                     "Client credentials validated successfully")
          client)))))

(defmethod grant-token "client_credentials"
  [{:keys [client_id client_secret audience]
    :as request}]
  (log/debug {:id ::cc-grant-request
              :data {:action :requested
                     :subject :access-token
                     :flow "client_credentials"
                     :client client_id
                     :audience audience}}
             "Processing client credentials grant request")
  (if-let [client (validate-client-credentials request)]
    (let [service-user (some-> (service-user/get-service-user (id/extract client))
                               id/extract
                               (as-> x (iam.context/get-user-details {(id/key) x}))
                               (update :roles #(set (keys %)))
                               (update :groups #(set (keys %))))
          ;; Resolved ONCE — defaults an omitted audience to the client's
          ;; sole linked API (see resolve-audience), never nil past this
          ;; point. docs/plans/PLAN-AUDIENCE-BINDING.md decisions 1-2.
          resolved-audience (resolve-audience client_id audience)]
      (cond
        (nil? service-user)
        (do
          (log/warn {:id ::cc-service-user-missing
                     :data {:action :rejected
                            :subject :access-token
                            :flow "client_credentials"
                            :reason :service-user-missing
                            :client client_id}}
                    "Service user not found for client")
          (token-error
           401
           "invalid_client"
           "Service user not configured for this client"))

        (not (:active service-user))
        (do
          (log/warn {:id ::cc-service-user-inactive
                     :data {:action :rejected
                            :subject :access-token
                            :flow "client_credentials"
                            :reason :service-user-inactive
                            :client client_id}}
                    "Service user inactive for client")
          (token-error
           401
           "invalid_client"
           "Service user for this client is inactive"))

        (nil? resolved-audience)
        (do
          (log/warn {:id ::cc-audience-not-authorized
                     :data {:action :rejected
                            :subject :access-token
                            :flow "client_credentials"
                            :reason :audience-not-authorized
                            :client client_id
                            :audience audience}}
                    "Client not authorized for audience")
          (invalid-target-error client_id audience))

        :else
        (try
          (let [tokens (generate-client-token client service-user
                                               (assoc request :audience resolved-audience))
                response (json/write-str tokens)]
            (log/info {:id ::cc-tokens-generated
                       :data {:action :issued
                              :subject :access-token
                              :flow "client_credentials"
                              :client client_id
                              :audience resolved-audience}}
                      "Client credentials tokens issued")
            {:status 200
             :headers {"Content-Type" "application/json;charset=UTF-8"
                       "Pragma" "no-cache"
                       "Cache-Control" "no-store"}
             :body response})
          (catch Exception e
            (log/error! {:id ::cc-token-generation-failed
                         :msg "Error generating tokens for client credentials"
                         :data {:action :failed
                                :subject :access-token
                                :flow "client_credentials"
                                :reason :generation-failed
                                :client client_id
                                :audience resolved-audience}}
                        e)
            (token-error
             500
             "server_error"
             "An error occurred while generating tokens")))))

    (do
      (log/warn {:id ::cc-validation-failed
                 :data {:action :rejected
                        :subject :access-token
                        :flow "client_credentials"
                        :reason :validation-failed
                        :client client_id}}
                "Client credentials validation failed")
      (token-error
       401
       "invalid_client"
       "Client authentication failed"))))

(defn token-endpoint
  [{{:keys [grant_type]
     :as oauth-request} :params
    :as request}]
  (log/debug {:id ::token-endpoint-request
              :data {:action :requested
                     :subject :access-token
                     :flow grant_type}}
             "Received token endpoint request")
  (binding [core/*domain* (core/original-uri request)]
    (case grant_type
      ("authorization_code" "refresh_token" "urn:ietf:params:oauth:grant-type:device_code" "client_credentials")
      (try
        (grant-token oauth-request)
        ;; `generate`/`generate-client-token` throw rather than return a
        ;; response (they're also called from the authorize/device code
        ;; paths, which expect a token map) — this is the one choke point
        ;; every grant passes through, so the RFC 8707 rendering happens here.
        (catch clojure.lang.ExceptionInfo ex
          (let [{:keys [client_id requested-audience]} (ex-data ex)]
            (if-not requested-audience
              (throw ex)
              (do
                (log/warn {:id ::audience-denied
                           :data {:action :denied
                                  :subject :token
                                  :client client_id
                                  :audience requested-audience
                                  :flow grant_type}}
                          "Token request denied — client not authorized for requested audience")
                (invalid-target-error client_id requested-audience))))))

      (core/handle-request-error
       {:type "unsupported_grant_type"
        :grant_type grant_type}))))

;; =============================================================================
;; Ring Handlers (Pure Ring, no Pedestal dependencies)
;; =============================================================================

(defn token-handler
  "OAuth 2.0 token endpoint handler for all grant types."
  [request]
  (token-endpoint request))

(defn revoke-token-handler
  "OAuth 2.0 token revocation endpoint (RFC 7009); 200 for successful revocation
   OR unknown/invalid tokens, 400 for missing params or client auth failure."
  [request]
  (let [invalid-client (core/json-error "invalid_client" "Client ID is not valid")
        invalid-request (core/json-error "invalid_request" "Missing required parameter: token")
        {:keys [token_type_hint token] :as params} (:params request)
        ok-response {:status 200
                     :body ""
                     :headers {"Content-Type" "application/json"
                               "Cache-Control" "no-store"
                               "Pragma" "no-cache"}}]
    (cond
      (or (nil? token) (empty? token))
      (do
        (log/debug {:id ::revoke-missing-token
                    :data {:action :rejected
                           :subject :access-token
                           :reason :missing-token}}
                   "Missing required token parameter")
        invalid-request)

      :else
      (let [[token-key session] (find-token (when token_type_hint (keyword token_type_hint))
                                            token)]
        (cond
          (nil? session)
          (do
            (log/debug {:id ::revoke-token-not-found
                        :data {:action :revoked
                               :subject :access-token
                               :status :not-found}}
                       "Token not found (already revoked or never existed); returning 200 per RFC 7009")
            ok-response)

          (core/clients-doesnt-match? session params)
          (do
            (log/error {:id ::revoke-client-mismatch
                        :data {:action :rejected
                               :subject :access-token
                               :reason :client-mismatch
                               :session (core/short-id session)}}
                       "Couldn't revoke token — client mismatch")
            invalid-client)

          :else
          (do
            (log/info {:id ::revoke-token
                       :data {:action :revoked
                              :subject :access-token
                              :session (core/short-id session)
                              :token-key token-key}}
                      "Token revoked")
            (revoke-token token-key token)
            ok-response))))))

;; =============================================================================
;; Custom Scopes (access_token)
;; =============================================================================

(defscope roles [:roles]
  :token :access_token
  :description "Your assigned roles"
  :resolve (fn [session]
             (let [{:keys [roles]} (core/get-session-resource-owner session)
                   dataset-roles (dataset/search-entity
                                  :iam/user-role
                                  {(id/key) {:_in roles}}
                                  {:name nil})]
               {:roles (map (comp csk/->snake_case_keyword :name) dataset-roles)})))

(defscope groups [:groups]
  :token :access_token
  :description "Your group memberships"
  :resolve (fn [session]
             (let [{:keys [groups]} (core/get-session-resource-owner session)
                   dataset-groups (dataset/search-entity
                                   :iam/user-group
                                   {(id/key) {:_in groups}}
                                   {:name nil})]
               {:groups (map (comp csk/->snake_case_keyword :name) dataset-groups)})))

(defscope permissions [:permissions]
  :token :access_token
  :description "Your permissions"
  :resolve (fn [session]
             (let [{:keys [roles]} (core/get-session-resource-owner session)]
               {:permissions (access/roles-scopes roles)})))

(defscope super [:super]
  :token :access_token
  :description "Superuser status"
  :resolve (fn [session]
             (let [{:keys [roles]} (core/get-session-resource-owner session)]
               {:super (access/superuser? roles)})))

;; =============================================================================
;; Identifier scopes — resolve roles and groups to XIDs (the canonical
;; Synthigy id). Registered under both `*:xid` names and legacy `*:uuid`
;; aliases (predate the euuid→xid convergence); both forms return XIDs.
;; The owner's own id is NOT here — `:xid` rides every token unconditionally.
;; =============================================================================

(defn scope-roles-xid
  "Resolve the session's roles to user-role entities and emit their XIDs."
  [ctx tokens scope]
  (let [{:keys [roles]} (core/get-session-resource-owner (:session ctx))
        role-xids (->> (dataset/search-entity :iam/user-role
                                              {(id/key) {:_in roles}}
                                              {(id/key) nil})
                       (mapv (id/key)))]
    (-> tokens
        (assoc-in [:access_token :roles] role-xids)
        (update-in [:access_token :scope] (fnil conj []) scope))))

(defn scope-groups-xid
  "Resolve the session's groups to user-group entities and emit their XIDs."
  [ctx tokens scope]
  (let [{:keys [groups]} (core/get-session-resource-owner (:session ctx))
        group-xids (->> (dataset/search-entity :iam/user-group
                                               {(id/key) {:_in groups}}
                                               {(id/key) nil})
                        (mapv (id/key)))]
    (-> tokens
        (assoc-in [:access_token :groups] group-xids)
        (update-in [:access_token :scope] (fnil conj []) scope))))

(defmethod process-scope "roles:xid"   [ctx tokens scope] (scope-roles-xid ctx tokens scope))
(defmethod process-scope "roles:uuid"  [ctx tokens scope] (scope-roles-xid ctx tokens scope))  ; legacy alias → xid
(defmethod process-scope "groups:xid"  [ctx tokens scope] (scope-groups-xid ctx tokens scope))
(defmethod process-scope "groups:uuid" [ctx tokens scope] (scope-groups-xid ctx tokens scope)) ; legacy alias → xid

(comment
  (def tokens nil)
  (access/roles-scopes #{#uuid "8ebc60f1-8df0-48c8-a9b6-747a140df021"})
  (def session "RkJDHRzznXwlkatsVQnLWMmJHRWdyg"))
