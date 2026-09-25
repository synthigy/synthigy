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

(ns synthigy.oauth.core
  (:require
   [buddy.hashers :as hashers]
   [clojure.core.cache :as cache]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [synthigy.json :as json]
   clojure.pprint
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [synthigy.transit :as transit]
   [synthigy.log :as log]
   [clojure.walk :refer [keywordize-keys]]
   [nano-id.core :as nano-id]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.core :as core]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.encryption :as dataset-encryption]
   [synthigy.dataset.id :as id]
   [synthigy.env :as env]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.iam.connector :as connector]
   [synthigy.iam.encryption :as encryption]
   [synthigy.oauth.page.error :as error-page]
   [synthigy.util :as util])
  (:import
   [java.util Base64]))

(defn pprint [data] (with-out-str (clojure.pprint/pprint data)))

(defonce ^:dynamic *domain* nil)

(def ^:private client-cache-ttl-ms (* 30 1000))

(defonce ^:private client-cache
  (atom (cache/ttl-cache-factory {} :ttl client-cache-ttl-ms)))

(defonce browser-origins (atom nil))

(defn evict-clients!
  "Drop every cached client — delta invalidation and lifecycle stop."
  []
  (reset! client-cache (cache/ttl-cache-factory {} :ttl client-cache-ttl-ms))
  (reset! browser-origins nil)
  nil)

(defn domain+
  ([] (domain+ ""))
  ([path]
   (str (or env/iam-root-url *domain*) path)))

;; Rides the dataset DEK — fails closed when encryption is sealed, no node-local
;; fallback
(defn encrypt
  [data]
  (json/write-str (dataset-encryption/encrypt-data (json/write-str data))))

(defn decrypt [encrypted-data]
  (try
    (json/read-str (dataset-encryption/decrypt-data (json/read-str encrypted-data)))
    (catch Throwable _ nil)))

; (comment
;   (time
;    (decrypt
;     (encrypt
;      {:device-code 100
;       :user-code 200
;       :ip "a"
;       :user-agent "jfioq"}))))
;
(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"]
  (def gen-session-id (nano-id/custom alphabet 30)))

(defn short-id
  "First 6 chars of an opaque id for log correlation; nil for nil/blank."
  [s]
  (when (and (string? s) (pos? (count s)))
    (subs s 0 (min 6 (count s)))))

;; Settings AND defaults are SECONDS — never util/hours|days (milliseconds):
;; that was the ~83-day-token bug
(let [default (* 60 15)]
  (defn access-token-expiry
    [{{{expiry "access"} "token-expiry"} :settings}]
    (or expiry default)))

(let [default (* 60 60 24)]
  (defn refresh-token-expiry
    [{{{expiry "refresh"} "token-expiry"} :settings}]
    (long (or expiry default))))

(defn expired?
  [token]
  (try
    (let [{:keys [exp]} (encryption/unsign-data token)]
      (if (some? exp)
        (< (* 1000 exp) (System/currentTimeMillis))
        true))
    (catch clojure.lang.ExceptionInfo ex
      (let [{:keys [cause]} (ex-data ex)]
        (if (= cause :exp)
          true
          (throw ex))))))

(defn expires-at
  [token]
  (try
    (let [{:keys [exp]} (encryption/unsign-data token)]
      (java.util.Date. (* 1000 exp)))
    (catch clojure.lang.ExceptionInfo ex
      (let [{:keys [cause]} (ex-data ex)]
        (if (= cause :exp)
          (java.util.Date. 0)
          (throw ex))))))

(def not-revoked
  "Match token rows where revoked is explicitly false OR NULL (default state)."
  {:_or [{:revoked {:_eq false}} {:revoked :is_null}]})

(def ^:private no-audience
  "json object keys can't be nil — nil audience is keyed as empty string."
  "")

(defn audience-key [audience] (or audience no-audience))

(def oidc-audience
  "The UNIVERSAL, IMPLICIT identity audience — 'who is this user', nothing
   more. Every registered client gets it automatically: unlike
   `synthigy.engine/platform-audience` (and any future per-integration
   audience), it needs no `:iam/app`→`:apis` link, because it grants no
   capability scopes to filter. A token request naming NO audience
   resolves here (`synthigy.oauth.token/resolve-audience`) — /data access
   is opt-in via an EXPLICIT, entitled audience, never the default.
   /oauth/userinfo, /oauth/jwks, /.well-known/openid-configuration check
   against this constant (see docs/plans/PLAN-AUDIENCE-BINDING.md)."
  "https://synthigy.com/id")

(def platform-audience
  "THE audience a bearer token must carry to be answered at all. Mirrors
   `synthigy.engine/platform-audience` — engine may not depend on oauth, so
   both literals exist and `synthigy.iam.scope-contract-test` pins them to
   each other and to resources/exports/api_synthigy.json."
  "https://synthigy.com")

(defn session-row
  [session selections]
  (when session
    (dataset/get-entity (id/entity :oauth/session) {:id session} selections)))

(defn session-context
  [session]
  (:context (session-row session {:context nil})))

(defn session-exists?
  "Whether a row for `session` is present at all; stamps upsert, so guard them."
  [session]
  (some? (session-row session {:id nil})))

(defn decode-stored-request
  "Rehydrate an authorization request stored in a `data` json column."
  [m]
  (when m
    (-> (into {} (map (fn [[k v]] [(keyword k) v])) m)
        (update :scope #(some-> % not-empty set))
        (update :response_type #(some-> % not-empty set)))))

(defn update-session-context!
  "Read-modify-write of the session's context json."
  [session f & args]
  (when (and session (session-exists? session))
    (dataset/stack-entity
     (id/entity :oauth/session)
     {:id session :context (apply f (session-context session) args)})
    nil))

(defmulti sign-token (fn [_ token-key _] token-key))

(defmethod sign-token :default
  [session token-key data]
  (log/error {:id ::sign-token-no-method
              :data {:session session :token-key token-key}}
             "Couldn't sign token (no method)")
  data)

(defn get-session-client [session]
  (:client
   (session-row session
                {:client [{:selections {(id/key) nil :id nil :name nil :type nil
                                        :secret nil :active nil :settings nil}}]})))

(defn resolve-resource-owner
  "Fetch a full resource-owner (person_info + roles/groups as xid sets) for any
   identifier."
  [identifier]
  (when-let [xid (:xid (iam.context/get-user-context identifier))]
    (some-> (iam.context/get-user-details {:xid xid})
            (update :roles  #(set (keys %)))
            (update :groups #(set (keys %))))))

(defn get-resource-owner
  "Resolve a resource-owner by any identifier (id/xid/username) through IAM."
  [identifier]
  (resolve-resource-owner identifier))

(comment
  (def identifier "rgersak"))

(defn get-session-resource-owner [session]
  (when-let [user-id (-> (session-row session {:user [{:selections {(id/key) nil}}]})
                         :user
                         (get (id/key)))]
    (get-resource-owner user-id)))

(defmulti process-scope (fn [_ _ scope] scope))

(defmethod process-scope :default
  [{:keys [roles api-scopes]} tokens scope]
  (let [user-scopes (access/roles-scopes roles)]
    (if (and (contains? user-scopes scope)
             (or (nil? api-scopes) (contains? api-scopes scope)))
      (update-in tokens [:access_token :scope] (fnil conj []) scope)
      tokens)))

(defmethod process-scope "offline_access"
  [_ tokens scope]
  (update-in tokens [:access_token :scope] (fnil conj []) scope))

(defmulti claims-for
  "Scope descriptor {:claims [...] :resolve (fn [session] {...})}; nil :resolve
   reads person_info."
  identity)

(defmethod claims-for :default [_] nil)

(defmacro defscope
  "Define an OIDC scope: generates its claims-for and process-scope methods."
  [scope-name claims & {:keys [resolve description token] :or {token :id_token}}]
  (let [scope-str (name scope-name)
        tokens (if (coll? token) token #{token})
        claims-vec (vec (map keyword claims))]
    `(do
       (defmethod claims-for ~scope-str [~'_]
         (hash-map :claims ~claims-vec
                   :token ~tokens
                   ~@(when resolve [:resolve resolve])
                   ~@(when description [:description description])))

       (defmethod process-scope ~scope-str [~'ctx ~'tokens ~'_]
         (let [~'claims (resolve-scope-claims ~scope-str (:session ~'ctx))]
           (-> ~'tokens
               ~@(for [t tokens]
                   `(update ~t merge ~'claims))
               (update-in [:access_token :scope] (fnil conj []) ~scope-str)))))))

(def ^:private public-profile-claims
  "Claims that moved to user-public-profile in OAuth 0.1.8, as claim → attribute key."
  {:name               :name
   :given_name         :given_name
   :family_name        :family_name
   :nickname           :nickname
   :preferred_username :preferred_username
   :profile            :profile
   :picture            :picture
   :website            :website
   :zoneinfo           :zone_info})

(defn user-claims
  "Assemble a user's OIDC claims; public-profile wins, person-info is the
   fallback."
  [{:keys [person_info public_profile]}]
  (merge person_info
         (reduce-kv (fn [m claim k]
                      (if-some [v (get public_profile k)]
                        (assoc m claim v)
                        m))
                    {}
                    public-profile-claims)))

(defn resolve-scope-claims
  "Resolve claims for a scope via its :resolve fn or the user-claims lookup."
  [scope session]
  (when-let [{:keys [claims resolve]} (claims-for scope)]
    (if resolve
      (resolve session)
      (select-keys (user-claims (get-session-resource-owner session))
                   claims))))

(defn all-supported-scopes []
  (vec (remove #{:default} (keys (methods claims-for)))))

(defn all-supported-claims []
  (into #{}
        (mapcat (comp :claims claims-for)
                (all-supported-scopes))))

(defn scope-info-for-consent
  "Scope info for the consent UI."
  [requested-scopes]
  (for [scope requested-scopes
        :let [{:keys [claims description]} (claims-for scope)]
        :when claims]
    {:scope scope
     :description (or description scope)
     :claims claims}))

(defn token-error [code & description]
  {:status 400
   :headers {"Content-Type" "application/json;charset=UTF-8"
             "Pragma" "no-cache"
             "Cache-Control" "no-store"}
   :body (json/write-str
          {:error code
           :error_description (str/join "\n" description)})})

(defn get-base-uri
  "Returns the base URI without query parameters from the given URL."
  [url]
  (when (not-empty url)
    (let [uri (java.net.URI. url)]
      (str (java.net.URI. (.getScheme uri) (.getAuthority uri) (.getPath uri) nil nil)))))

(defn localhost-redirect?
  "Returns true if the URI is a loopback redirect (localhost or 127.0.0.1)."
  [url]
  (when (not-empty url)
    (try
      (let [uri (java.net.URI. url)
            host (.getHost uri)]
        (or (= host "localhost") (= host "127.0.0.1")))
      (catch Exception _ false))))

(defn strip-port
  "scheme://host/path with the port dropped; nil on a blank/malformed URI."
  [uri]
  (when (not-empty uri)
    (try
      (let [u (java.net.URI. uri)]
        (str (java.net.URI. (.getScheme u) nil (.getHost u) -1 (.getPath u) nil nil)))
      (catch Exception _ nil))))

(defn loopback-redirect-matches?
  "RFC 8252 §7.3: loopback redirect_uri must match a registered entry on
   scheme+host+path — only the port is exempt."
  [redirect-uri redirections]
  (when (localhost-redirect? redirect-uri)
    (when-let [target (strip-port redirect-uri)]
      (boolean (some #(= target (strip-port %)) redirections)))))

(defn uri-origin
  "scheme://host[:port] of a URI, or nil."
  [uri]
  (when (not-empty uri)
    (try
      (let [u (java.net.URI. uri)]
        (when (and (.getScheme u) (.getHost u))
          (str (.getScheme u) "://" (.getHost u) (when (pos? (.getPort u)) (str ":" (.getPort u))))))
      (catch Exception _ nil))))

(defn load-browser-origins
  []
  (let [redirects (->> (dataset/search-entity :iam/app {} {:active nil :settings nil})
                       (filter :active)
                       (mapcat #(get-in % [:settings "redirections"])))]
    {:exact (into #{} (keep uri-origin) (concat redirects env/allowed-origins [env/iam-root-url (domain+)]))
     :loopback (into #{} (comp (filter localhost-redirect?) (keep strip-port) (keep uri-origin)) redirects)
     :loaded-at (System/currentTimeMillis)}))

(defn origin-allowed?
  "True for an origin of an active client's redirect URI (any port on loopback, RFC 8252) or SYNTHIGY_SERVER_ALLOWED_ORIGINS."
  [origin]
  (let [{:keys [exact loopback loaded-at]} @browser-origins
        {:keys [exact loopback]} (if (and loaded-at (< (- (System/currentTimeMillis) loaded-at) client-cache-ttl-ms))
                                   {:exact exact :loopback loopback}
                                   (reset! browser-origins (load-browser-origins)))]
    (boolean
     (or (contains? exact origin)
         (and (localhost-redirect? origin)
              (contains? loopback (some-> (strip-port origin) uri-origin)))))))

(defn validate-resource-owner
  "Verify resource-owner credentials via the connector chain; user map on
   success, nil on deny."
  [username password]
  (connector/authenticate {:username username :password password}))

(defn client-info
  "Client IP + user-agent, for recording where a session was minted from."
  [request]
  ;; advisory only — http-kit's :remote-addr already honours X-Forwarded-For
  {:ip (or (when env/trust-proxy
             (some-> (get-in request [:headers "x-forwarded-for"])
                     (str/split #",") first str/trim not-empty))
           (:remote-addr request))
   :agent (not-empty (get-in request [:headers "user-agent"]))})

(defn create-session!
  "DB-first session create: one row write carrying relations + context."
  [session {:keys [client user flow code authorized-at amr acr audience scope ip agent]}]
  (let [client-id (if (map? client) (id/extract client) client)
        user-id (when user (id/extract user))]
    (dataset/stack-entity
     (id/entity :oauth/session)
     (cond-> {:id session
              :active true
              :started (java.util.Date.)
              :context (cond-> {"flow" flow}
                         code (assoc "code" code)
                         amr (assoc "amr" (vec amr))
                         acr (assoc "acr" acr)
                         ip (assoc "ip" ip)
                         agent (assoc "agent" agent)
                         (seq scope) (assoc "scopes" {(audience-key audience) (vec scope)}))}
       authorized-at (assoc :authorized_at authorized-at)
       client-id (assoc :client {(id/key) client-id})
       user-id (assoc :user {(id/key) user-id})))
    nil))

(defn set-session-audience-scope
  ([session scope] (set-session-audience-scope session nil scope))
  ([session audience scope]
   (update-session-context! session assoc-in ["scopes" (audience-key audience)] (vec scope))))

(defn get-session-audience-scope
  ([session] (get-session-audience-scope session nil))
  ([session audience]
   (some-> (session-context session)
           (get-in ["scopes" (audience-key audience)])
           set
           not-empty)))

(defn clients-match?
  "Validate client credentials against the session's client."
  [session {:keys [client_id client_secret]}]
  (let [{known-id :id
         known-secret :secret} (get-session-client session)]
    (cond
      (not= client_id known-id)
      false

      (nil? known-secret)
      true

      (nil? client_secret)
      false

      :else
      (hashers/check client_secret known-secret))))

(def clients-doesnt-match? (complement clients-match?))

(defn get-client
  "Resolve an OAuth client by client_id through the TTL cache."
  [id]
  (when id
    (let [c @client-cache]
      (if (cache/has? c id)
        (do (swap! client-cache cache/hit id)
            (cache/lookup c id))
        (when-some [client (iam/get-client id)]
          (swap! client-cache assoc id client)
          client)))))

(defn server-login-page-url
  "Mount URL of the server-level custom login page, or nil when unset."
  []
  (when (seq env/login-page-path)
    "/login/"))

(defn get-client-login-url
  "Login URL for a client; per-client setting must be a relative path or this
   throws (credential-phishing guard)."
  [client]
  (let [custom-page (get-in client [:settings "login-page"])
        client-name (:name client)]
    (cond
      (or (nil? custom-page) (empty? custom-page))
      (or (server-login-page-url) "/oauth/login")

      (and (string? custom-page)
           (str/starts-with? custom-page "/")
           (not (str/includes? custom-page "..")))
      custom-page

      :else
      (throw
       (ex-info
        (str "Client '" client-name "' has invalid login-page setting. "
             "Only relative URLs starting with '/' are allowed.")
        {:type "invalid_login_page"
         :client-name client-name
         :invalid-value custom-page})))))

(defn get-session
  "Assemble the legacy session map from the row; keys are absent (not nil) when
   unset."
  [id]
  (when id
    (when-let [row (session-row id
                                {:id nil :active nil :authorized_at nil :context nil
                                 :last_seen nil
                                 :user [{:selections {(id/key) nil :name nil} :args {:_join :left}}]
                                 :client [{:selections {(id/key) nil} :args {:_join :left}}]})]
      (let [{amr "amr" acr "acr" flow "flow" code "code" scopes "scopes"} (:context row)]
        (cond-> {:active (:active row)}
          (:authorized_at row) (assoc :authorized-at (:authorized_at row))
          (:last_seen row) (assoc :last-seen (:last_seen row))
          (:client row) (assoc :client (get-in row [:client (id/key)]))
          (:user row) (assoc :resource-owner (get-in row [:user (id/key)])
                             :resource-owner/name (get-in row [:user :name]))
          flow (assoc :flow flow)
          code (assoc :code code)
          amr (assoc :amr (vec amr))
          acr (assoc :acr acr)
          scopes (assoc :scopes scopes))))))

(defn remove-session
  "Hard-delete a session row (pre-auth/error cleanup); authenticated sessions go
   through kill-session."
  [id]
  (when id
    (dataset/delete-entity (id/entity :oauth/session) {:id id})
    nil))

(defn get-redirection-uris [session]
  (let [{{:strs [redirections]} :settings} (get-session-client session)]
    redirections))

(def request-errors
  (reduce-kv
   (fn [r k v]
     (assoc r k (str/join "\n" v)))
   nil
   {"invalid_request"
    ["The request is missing a required parameter, includes an"
     "invalid parameter value, includes a parameter more than"
     "once, or is otherwise malformed."]
     ;;
    "unauthorized_client"
    ["The client is not authorized to request an authorization"
     "code using this method."]
     ;;
    "access_denied"
    ["The resource owner or authorization server denied the"
     "request."]
     ;;
    "unsupported_response_type"
    ["The authorization server does not support obtaining an"
     "authorization code using this method."]
     ;;
    "invalid_scope"
    ["The requested scope is invalid, unknown, or malformed."]
     ;;
    "server_error"
    ["The authorization server encountered an unexpected"
     "condition that prevented it from fulfilling the request."]
     ;;
    "temporarily_unavailable"
    ["The authorization server is currently unable to handle"
     "the request due to a temporary overloading or maintenance"
     "of the server."]}))

(defn render-error-page
  "Render the OAuth error page directly (400) when redirecting would be unsafe."
  ([error-type] (render-error-page error-type nil))
  ([error-type context]
   (error-page/render-error-page error-type context)))

(defn handle-request-error
  "Handle OAuth authorization errors per RFC 6749."
  [{t :type
    session :session
    request :request
    description :description
    :as error-data}]
  (let [{:keys [state redirect_uri]} (or
                                      request
                                      (:request (get-session session)))
        base-redirect-uri (get-base-uri redirect_uri)
        context (select-keys error-data [:client-name :invalid-value])]
    (when session (remove-session session))
    (case t
      ;; RFC 6749 §4.1.2.1: invalid/missing redirect_uri MUST NOT redirect —
      ;; render error page
      ("no_redirections"
       "missing_redirect"
       "redirect_missmatch"
       "missing_response_type"
       "client_not_registered"
       "corrupt_session"
       "unsupported_grant_type"
       "invalid_login_page")
      (render-error-page t context)

      {:status 302
       :headers {"Location" (str base-redirect-uri "?"
                                 (codec/form-encode
                                  (cond->
                                   {:error t}
                                    description (assoc :error_description description)
                                    state (assoc :state state))))
                 "Cache-Control" "no-cache"}})))

(def session-touch-ms
  "How stale `last_seen` may get before a request re-stamps it."
  ;; ponytail: sized off session-idle-ttl — 5m is ~288x the resolution a
  ;; 24h idle window needs; widen it with the TTL, don't tune it by feel
  (util/minutes 5))

(defonce ^:private session-touch-cache
  (atom (cache/ttl-cache-factory {} :ttl session-touch-ms)))

(defn touch-session!
  "Record activity on `session`; throttled, so most calls do no IO at all."
  [session]
  (when (and session (not (cache/has? @session-touch-cache session)))
    ;; recorded BEFORE the write so a dead cookie hammering us costs one
    ;; existence check per window, not one per request
    (swap! session-touch-cache cache/miss session true)
    (when (session-exists? session)
      (dataset/stack-entity (id/entity :oauth/session)
                            {:id session :last_seen (java.util.Date.)})
      true)))

(defn set-session-authorized-at
  [session timestamp]
  (when (session-exists? session)
    (dataset/stack-entity (id/entity :oauth/session)
                          {:id session :authorized_at timestamp}))
  nil)

(defn get-session-authorized-at
  [session]
  (:authorized_at (session-row session {:authorized_at nil})))


(def ^:const acr-levels
  "Standard ACR levels; higher = stronger authentication."
  {"0" {:level 0 :name "No assurance" :methods #{}}
   "1" {:level 1 :name "Password-based" :methods #{"pwd"}}
   "2" {:level 2 :name "Multi-factor" :methods #{"pwd" "otp" "sms" "hwk" "bio"}}
   "urn:mace:incommon:iap:bronze" {:level 1 :name "Bronze" :methods #{"pwd"}}
   "urn:mace:incommon:iap:silver" {:level 2 :name "Silver" :methods #{"pwd" "otp"}}
   "urn:mace:incommon:iap:gold"   {:level 3 :name "Gold" :methods #{"pwd" "hwk"}}})

(defn set-session-amr
  [session amr]
  (update-session-context! session assoc "amr" (vec amr)))

(defn get-session-amr
  [session]
  (or (some-> (session-context session) (get "amr") vec) ["pwd"]))

(defn set-session-acr
  [session acr]
  (update-session-context! session assoc "acr" acr))

(defn get-session-acr
  [session]
  (or (get (session-context session) "acr") "1"))

(defn derive-acr-from-amr
  "Derive ACR level from AMR methods."
  [amr]
  (let [methods (set amr)]
    (cond
      (and (contains? methods "pwd")
           (some methods #{"otp" "sms" "hwk" "bio" "mfa"}))
      "2"

      (contains? methods "hwk")
      "2"

      (contains? methods "bio")
      "2"

      (contains? methods "pwd")
      "1"

      :else
      "0")))

(defmulti session-kill-hook (fn [priority _] priority))

(defn kill-session
  [session]
  (when-let [session-data (get-session session)]
    (let [resource-owner (get-session-resource-owner session)
          {client-id :id} (get-session-client session)]
      (doseq [p (sort (keys (methods session-kill-hook)))]
        (session-kill-hook p session))
      (dataset/stack-entity (id/entity :oauth/session)
                            {:id session :active false :finished (java.util.Date.)})
      (log/info {:id ::session-killed
                 :user-xid (:xid resource-owner)
                 :data {:action :killed
                        :subject :session
                        :session (short-id session)
                        :client client-id
                        :flow (:flow session-data)}}
                "OAuth session killed")
      (iam/publish
       :oauth.session/killed
       {:session session
        :data session-data}))))

(defn active-sessions
  "Ids of all active session rows."
  []
  (map :id (dataset/search-entity (id/entity :oauth/session)
                                  {:active {:_eq true}}
                                  {:id nil})))

(defn kill-sessions
  []
  (doseq [session (active-sessions)]
    (kill-session session)))

(s/def ::authorization-code-grant #{"authorization_code"})
(s/def ::password-grant #{"password"})
(s/def ::implict-grant #{"implicit"})
(s/def ::refresh-token-grant #{"refresh_token"})
(s/def ::client-credentials-grant #{"client_credentials"})

(s/def ::grant_type
  (s/or
   :authorization-code ::authorization-code-grant
   :password ::password-grant
   :refresh-token ::refresh-token-grant
   :implicit ::implict-grant))

(defn json-error
  [status & description]
  (let [_status (if (number? status) status 400)
        [code & description] (if (number? status)
                               description
                               (concat [status] description))]
    {:status _status
     :headers {"Content-Type" "application/json;charset=UTF-8"
               "Pragma" "no-cache"
               "Cache-Control" "no-store"}
     :body (json/write-str
            {:error code
             :error_description (str/join "\n" description)})}))

(defn decode-base64-credentials
  [data]
  (when data
    (let [decoded-bytes (.decode (Base64/getDecoder) (.getBytes data "UTF-8"))
          decoded (String. decoded-bytes)]
      (str/split decoded #":" 2))))

(defn wrap-basic-authorization
  "Ring middleware parsing HTTP Basic Authorization into
   :client_id/:client_secret params."
  [handler]
  (fn [request]
    (let [authorization (get-in request [:headers "authorization"])]
      (if-not authorization
        (handler request)
        (let [[_ credentials] (re-find #"Basic\s+(.*)" authorization)
              [id secret] (decode-base64-credentials credentials)]
          (handler (update request :params assoc
                           :client_id id
                           :client_secret secret)))))))

(defn original-uri
  "Original absolute URI of the request, honoring X-Forwarded-* headers."
  [{original-uri :uri
    {forwarded-host "x-forwarded-host"
     forwarded-proto "x-forwarded-proto"
     host "host"} :headers
    :keys [scheme]}]
  (format
   "%s://%s"
   (or forwarded-proto (name scheme))
   (or forwarded-host host original-uri)))

(defn parse-body
  "Parse a request body by Content-Type (transit/edn/JSON); nil on any bad body."
  [request]
  (try
    (let [body (or (:raw-body request) (:body request))
          body-str (when body (if (string? body) body (slurp (io/reader body))))
          ctype (some-> (get-in request [:headers "content-type"])
                        (str/split #";") first str/trim str/lower-case)]
      (when-not (str/blank? body-str)
        (case ctype
          "application/transit+json" (transit/<-transit body-str)
          "application/edn" (edn/read-string body-str)
          (json/read-str body-str))))
    (catch Exception e
      (log/debug {:id ::body-parse-failed :error e} "Failed to parse request body")
      nil)))

(defn wrap-keywordize-params
  "Ring middleware keywordizing :params keys."
  [handler]
  (fn [request]
    (handler (update request :params keywordize-keys))))

(defn wrap-scope->set
  "Ring middleware converting the space-separated scope string to a set."
  [handler]
  (fn [request]
    (handler (update-in request [:params :scope]
                        (fn [scope]
                          (when scope
                            (set (str/split scope #"\s+"))))))))

(defn wrap-session-read
  "Ring middleware mapping the idsrv.session cookie to the :idsrv/session param."
  [handler]
  (fn [request]
    (let [idsrv-session (get-in request [:cookies "idsrv.session" :value])]
      (if (empty? idsrv-session)
        (handler request)
        (handler (assoc-in request [:params :idsrv/session] idsrv-session))))))

(def session-idle-ttl
  "Sliding idle window for authorized sessions — last_seen older than this kills."
  ;; ponytail: fixed 24h; make per-deployment configurable when someone asks
  (util/hours 24))

(def session-absolute-ttl
  "Hard cap on any authorized session regardless of activity."
  (util/days 30))

(defn clean-sessions
  "Kill sessions that never reached authorization past `timeout`, or authorized
   sessions past the idle/absolute TTLs."
  ([] (clean-sessions (util/minutes 1)))
  ([timeout]
   (let [cutoff (java.util.Date. (- (util/now) timeout))
         idle-cutoff (java.util.Date. (- (util/now) session-idle-ttl))
         absolute-cutoff (java.util.Date. (- (util/now) session-absolute-ttl))
         candidates (dataset/search-entity
                     (id/entity :oauth/session)
                     {:_where {:_and [{:active {:_eq true}}
                                      {:_or [{:started :is_null}
                                             {:started {:_le cutoff}}]}]}}
                     {:id nil :authorized_at nil :last_seen nil})]
     (doseq [{session :id :keys [authorized_at last_seen]} candidates
             :let [idle-since (or last_seen authorized_at)
                   reason (cond
                            (nil? authorized_at)
                            :never-authorized
                            (.before ^java.util.Date idle-since idle-cutoff)
                            :idle-expired
                            (.before ^java.util.Date authorized_at absolute-cutoff)
                            :absolute-expired)]
             :when reason]
       (log/debug {:id ::session-timed-out
                   :data {:session session :reason reason}}
                  "Session timed out")
       (kill-session session)))))

(def ^:private client-monitor-sub-key ::client-cache-monitor)

(defn monitor-client-change
  "Idempotent: (re)registers the App-entity delta subscription that evicts the
   client cache."
  []
  (when-let [app-xid (some-> (id/entity :iam/app) str)]
    (delta/subscribe! client-monitor-sub-key
                      {:entity-xids #{app-xid}}
                      (fn [_env]
                        (log/debug {:id ::client-cache-evicted
                                    :data {:action :cleanup :subject :oauth-clients}}
                                   "App delta — client cache evicted")
                        (evict-clients!)))))
