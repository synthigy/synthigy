(ns synthigy.oauth.federated
  "Identity brokering — Synthigy as an OAuth Relying Party to upstream IdPs.

   Synthigy stays the OIDC Provider for its own clients; federation is just an
   ALTERNATE credential collector inside the existing authorization_code flow.
   The login page already holds the pending downstream code in its encrypted
   `state`; \"Sign in with X\" hands that state to `start-handler`, which bounces
   the browser to the upstream IdP. On `callback-handler` we validate the
   upstream response, resolve it to a Synthigy resource-owner, and finish the
   SAME flow via `login/complete-authorization-code-login!`. Downstream clients
   only ever see Synthigy tokens — never a Google/MS token.

   PROVIDER DISPATCH: per-provider multimethods (`authorize-url`/`fetch-identity`)
   mapped to a protocol FAMILY via `provider-families`. `:oidc` is live (Google,
   Azure, any discovery provider); `:oauth2` (GitHub, Facebook) is stubbed.

   DATA: provider config is read from the ID Federation dataset by
   `resolve-provider` (the Federation Provider entity: name + provider enum +
   active + an encrypted `configuration` JSON blob holding client_id/secret/etc).
   The remaining PLUG seam is `*resolve-identity*` — (iss,sub)->resource-owner,
   backed by ExternalIdentity; returns nil until wired, so federated login fails
   closed (\"not_linked\") and never auto-provisions.

   Transport security: Auth Code + PKCE upstream; full ID-token validation
   (RS256 pinned, JWKS-by-kid, exact iss, aud==our client_id, exp, nonce bound
   to our signed state); RFC 9207 `iss` callback check (mix-up defense); exact
   redirect_uri. PAR/DPoP deferred."
  (:require
   [buddy.core.hash :as hash]
   [buddy.core.keys :as keys]
   [buddy.core.nonce :as nonce]
   [buddy.sign.jws :as jws]
   [buddy.sign.jwt :as jwt]
   [clojure.core.cache :as cache]
   [clojure.string :as str]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.login :as login])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.util Base64]))

;; =============================================================================
;; ID Federation entity references (pinned — deployed + stable)
;; =============================================================================
;; The ID Federation dataset is the home for federation config; admins add
;; provider/identity ROWS freely via the IAM web component. Only the entity
;; DEFINITIONS are pinned here so the broker can resolve them by a stable key
;; (same pattern as synthigy.iam/keys for :iam/user etc.).

(id/defentity :id-federation/provider
  :euuid #uuid "5bc157c3-fa96-4ea2-9bf0-be2020b49bf8" :xid "CLAE8TtQwCKDKy5v6y5Qw1")

(id/defentity :id-federation/external-identity
  :euuid #uuid "174ad1a0-480a-48bb-970a-283d51cb9cd7" :xid "3spdYL3uzZXgTbJn9Rxd58")

;; =============================================================================
;; Data-model seams (PLUG these into datasets later)
;; =============================================================================

;; Per-provider defaults baked in code: the big providers' endpoints are
;; well-known constants, so the encrypted Configuration only needs client_id +
;; client_secret. Config keys override these (Azure tenant / self-hosted IdP).
(def ^:private provider-defaults
  {:google {:discovery-url "https://accounts.google.com/.well-known/openid-configuration"
            :issuer        "https://accounts.google.com"
            :scopes        "openid email"}
   :azure  {:scopes "openid email"}        ; discovery-url + issuer are tenant-specific
   :github {:authorize-url "https://github.com/login/oauth/authorize"
            :token-url     "https://github.com/login/oauth/access_token"
            :userinfo-url  "https://api.github.com/user"
            :scopes        "read:user user:email"}})

(defn- kebab-keys
  "Normalize snake_case JSON config keys to the kebab cfg keys the multimethods
   read (client_id -> :client-id), so admins can author conventional JSON."
  [m]
  (update-keys m (fn [k] (keyword (str/replace (name k) "_" "-")))))

(defn resolve-provider
  "Load a federation provider config by its `name` slug from the ID Federation
   dataset. The row carries the dispatch `provider` enum, an `active` flag, and
   an encrypted `configuration` JSON string (client_id / client_secret / scopes /
   urls — provider-shaped). The read path decrypts it; we parse it and merge
   under per-provider code defaults. `provider`/`enabled` are set authoritatively
   from the columns (last in the merge) so a stray config key can't hijack
   dispatch. Returns the cfg map the multimethods expect, or nil if absent /
   unparseable / read fails (fails closed — runs pre-authentication)."
  [name]
  (try
    (when-let [{:keys [provider active configuration]}
               (dataset/get-entity
                :id-federation/provider
                {:name name}
                {:name nil :provider nil :active nil :configuration nil})]
      ;; enum comes back as a keyword (:GOOGLE) — (str :GOOGLE) keeps the colon,
      ;; so strip a leading ':' before lower-casing to the dispatch key :google.
      (let [pkw    (keyword (str/lower-case (str/replace (str provider) #"^:" "")))
            config (some-> configuration json/read-str kebab-keys)]
        (merge (provider-defaults pkw)
               config
               {:provider pkw :enabled (boolean active)})))
    (catch Throwable e
      (log/warn {:id ::resolve-provider-failed :data {:provider name :err (.getMessage e)}}
                "Could not load federation provider")
      nil)))

(defn default-resolve-identity
  "(iss, sub, claims) -> Synthigy resource-owner, or nil if unlinked. Looks up
   the External Identity row for this (iss, sub) and loads the linked user as a
   full resource-owner (same shape as password login, via iam/get-user-details).

   EXPLICIT-LINK-ONLY: an unknown (iss, sub) returns nil -> federated login fails
   closed (\"not_linked\"); it NEVER auto-provisions or merges on email. Creating
   the link is a separate authenticated action."
  [iss sub _claims]
  (when (and iss sub)
    (some-> (dataset/get-entity
             :id-federation/external-identity
             {:iss iss :sub sub}
             {:user [{:selections {:name nil}}]})
            :user :name
            iam/get-user-details)))

;; Overridable seam (defonce so a runtime override survives ns reloads; swap via
;; alter-var-root). Default reads the External Identity entity.
(defonce ^:dynamic *resolve-identity* default-resolve-identity)

(defonce ^:dynamic *signup-policy*
  ;; :closed — an unknown federated identity is rejected ("not_linked"); accounts
  ;; are pre-provisioned or linked from an existing session. :open — an unknown
  ;; identity self-registers a fresh user via provision-user!. Default :closed
  ;; (opt into open per deployment).
  :closed)

(defn- provision-user!
  "First-time federated identity (no existing (iss,sub) link) -> a fresh Synthigy
   user + the External Identity link. Mirrors connector JIT: a bare user (roles/
   person_info wired downstream). Returns {:user resource-owner} on success, or
   {:error <code>}.

   EMAIL GUARD: if an account already owns this email, do NOT create or merge —
   return {:error \"email_exists\"} so the caller routes the person to 'log in
   with your password, then link'. Email is never an identity key; only (iss,sub)
   is. This is the line between safe signup and account hijacking."
  [provider iss sub claims]
  (let [email    (:email claims)
        username (or email sub)]
    (cond
      (and email (iam/get-user-details email))
      {:error "email_exists"}

      (nil? username)
      {:error "provision_failed"}

      :else
      (try
        ;; NOTE: not yet transactional — a failure between these two writes would
        ;; orphan the user. Wrap in one tx when this moves past review.
        (let [u (dataset/sync-entity :iam/user
                                     {:name username :active true :type :PERSON})]
          (dataset/sync-entity :id-federation/external-identity
                               {:provider (-> provider name str/upper-case)
                                :iss iss :sub sub :email email
                                :user {:xid (:xid u)}})
          (log/info {:id ::user-provisioned
                     :data {:action :created :subject :federated-user
                            :provider provider :username username}}
                    "Provisioned new user from federated identity")
          {:user (iam/get-user-details username)})
        (catch Throwable e
          (log/error! {:id ::provision-failed
                       :data {:action :created :subject :federated-user
                              :provider provider :sub sub}}
                      e)
          {:error "provision_failed"})))))

;; =============================================================================
;; Discovery + JWKS (cached, 1h TTL)
;; =============================================================================

(def ^:private ttl-ms (* 60 60 1000))
;; TTL cache for upstream discovery docs + JWKS — same idiom as the SQL template
;; cache in synthigy.dataset.sql.query (atom over a core.cache ttl factory).
(defonce ^:private discovery-cache
  (atom (cache/ttl-cache-factory {} :ttl ttl-ms)))

;; HTTP via the JDK's built-in java.net.http.HttpClient — no external HTTP
;; dependency (clj-http is dev/test-only here and auth/ is always on the
;; classpath). Mirrors synthigy.iam.connector's approach.
(def ^:private http-client
  (delay (-> (HttpClient/newBuilder)
             (.connectTimeout (Duration/ofSeconds 5))
             .build)))

(defn- send-json
  "Send an HttpRequest, parse a JSON body. Throws on >=400 (caught by handlers,
   which fail closed to an error redirect)."
  [^HttpRequest req]
  (let [resp (.send ^HttpClient @http-client req (HttpResponse$BodyHandlers/ofString))]
    (when (>= (.statusCode resp) 400)
      (throw (ex-info "Upstream HTTP error"
                      {:status (.statusCode resp) :body (.body resp)})))
    (json/read-str (.body resp))))

(defn- http-json [url]
  (send-json (-> (HttpRequest/newBuilder (URI/create url))
                 (.timeout (Duration/ofSeconds 5))
                 (.header "Accept" "application/json")
                 .GET
                 .build)))

(defn- http-get-bearer
  "Authenticated JSON GET for OAuth2 userinfo. (GitHub's API rejects requests
   without a User-Agent.)"
  [url token]
  (send-json (-> (HttpRequest/newBuilder (URI/create url))
                 (.timeout (Duration/ofSeconds 5))
                 (.header "Accept" "application/json")
                 (.header "User-Agent" "synthigy")
                 (.header "Authorization" (str "Bearer " token))
                 .GET
                 .build)))

(defn- cached
  "Read-through TTL cache (mirrors query.clj's has?/hit/lookup + assoc pattern).
   The double-miss race is benign here — the fetch is idempotent."
  [url]
  (let [c @discovery-cache]
    (if (cache/has? c url)
      (do (swap! discovery-cache cache/hit url)
          (cache/lookup c url))
      (let [v (http-json url)]
        (swap! discovery-cache assoc url v)
        v))))

(defn- discovery [cfg] (cached (:discovery-url cfg)))
(defn- jwks-keys [jwks-uri] (:keys (cached jwks-uri)))

;; =============================================================================
;; PKCE + signed state + base64url
;; =============================================================================

(defn- b64url [^bytes bs]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bs))

(defn- pkce-pair []
  (let [verifier (b64url (nonce/random-bytes 32))]
    {:verifier verifier
     ;; S256: challenge = base64url(sha256(verifier))
     :challenge (b64url (hash/sha256 verifier))}))

(defn- callback-uri []
  ;; Exact, stable redirect_uri — identical on start + callback, registered at
  ;; the IdP. Depends on core/*domain* (bind it per request, like login-handler).
  (str (core/domain+) "/oauth/federated/callback"))

(defn- error-redirect [err]
  (log/warn {:id ::federated-error :data {:action :credentials-rejected
                                          :subject :federated
                                          :error err}}
            "Federated login failed")
  {:status 302
   :headers {"Location" (str "/oauth/status?"
                             (codec/form-encode {:value "error"
                                                 :flow "federated"
                                                 :error err}))}})

;; =============================================================================
;; ID token validation
;; =============================================================================

(defn- public-key-for [keys-vec kid]
  (some-> (first (filter #(= kid (:kid %)) keys-vec))
          keys/jwk->public-key))

(defn- verify-id-token
  "Validate an upstream ID token. Returns claims map on success, nil on any
   failure. Pins RS256 (alg-confusion defense), resolves the signing key by
   `kid` from the provider JWKS, and lets buddy enforce iss/aud/exp; nonce is
   matched against the value we bound into our signed state."
  [disc id-token aud issuer nonce]
  (try
    (let [kid (:kid (jws/decode-header id-token))
          pub (public-key-for (jwks-keys (:jwks_uri disc)) kid)]
      (when pub
        (let [claims (jwt/unsign id-token pub {:alg :rs256 :iss issuer :aud aud})]
          (when (= nonce (:nonce claims)) claims))))
    (catch Throwable e
      (log/warn {:id ::id-token-rejected :data {:err (.getMessage e)}}
                "Upstream ID token rejected")
      nil)))

(defn- exchange-code [cfg disc code redirect-uri verifier]
  (send-json
   (-> (HttpRequest/newBuilder (URI/create (:token_endpoint disc)))
       (.timeout (Duration/ofSeconds 5))
       (.header "Content-Type" "application/x-www-form-urlencoded")
       (.header "Accept" "application/json")
       (.POST (HttpRequest$BodyPublishers/ofString
               (codec/form-encode {:grant_type "authorization_code"
                                   :code code
                                   :redirect_uri redirect-uri
                                   :client_id (:client-id cfg)
                                   :client_secret (:client-secret cfg)
                                   :code_verifier verifier})))
       .build)))

;; =============================================================================
;; Provider dispatch (per-provider, families share one implementation)
;; =============================================================================
;;
;; Dispatch is per-PROVIDER (:google, :azure, :github, ...) so a provider with
;; genuine quirks gets its own home — but each provider is mapped to a protocol
;; FAMILY, so standards-compliant providers reuse ONE implementation instead of
;; copy-pasting the security-critical validation path. Google and Azure are both
;; plain OIDC: they resolve to the :oidc methods and need zero provider-specific
;; code — they differ only in config DATA (discovery URL, issuer, scopes).
;;
;; Add a provider: one line in `provider-families`. Override a provider: write a
;; `(defmethod fetch-identity :azure ...)` ONLY for the part it does differently
;; (e.g. multi-tenant issuer pattern, GitHub's verified-email rule); it still
;; inherits the family method for everything else.

(def ^:private provider-families
  (-> (make-hierarchy)
      (derive :google   :oidc)
      (derive :azure    :oidc)
      (derive :github   :oauth2)
      (derive :facebook :oauth2)))

(defmulti authorize-url
  "Upstream authorize-redirect URL.
   ctx: {:redirect-uri :state :nonce :challenge}."
  (fn [cfg _ctx] (:provider cfg))
  :hierarchy #'provider-families)

(defmulti fetch-identity
  "Exchange the callback code and return {:iss :sub :claims}, or nil on failure.
   ctx: {:code :redirect-uri :verifier :nonce :iss}."
  (fn [cfg _ctx] (:provider cfg))
  :hierarchy #'provider-families)

;; --- OIDC family (Google, Azure, and any OIDC-discovery provider) ------------

(defmethod authorize-url :oidc
  [cfg {:keys [redirect-uri state nonce challenge]}]
  (let [disc (discovery cfg)]
    (str (:authorization_endpoint disc) "?"
         (codec/form-encode
          {:client_id (:client-id cfg)
           :response_type "code"
           :redirect_uri redirect-uri
           :scope (or (:scopes cfg) "openid email")
           :state state
           :nonce nonce
           :code_challenge challenge
           :code_challenge_method "S256"}))))

(defmethod fetch-identity :oidc
  [cfg {:keys [code redirect-uri verifier nonce iss]}]
  (let [disc   (discovery cfg)
        issuer (or (:issuer cfg) (:issuer disc))]
    (when (or (nil? iss) (= iss issuer))            ; RFC 9207 mix-up defense
      (let [tokens (exchange-code cfg disc code redirect-uri verifier)
            claims (verify-id-token disc (:id_token tokens) (:client-id cfg) issuer nonce)]
        (when claims
          {:iss issuer :sub (:sub claims) :claims claims})))))

;; --- OAuth2 family (GitHub, Facebook) ---------------------------------------
;; No ID token: authorize (shared) then exchange code + call the provider's
;; userinfo API over TLS. authorize-url is generic; fetch-identity is per-provider
;; (each maps userinfo differently). GitHub is implemented below; Facebook falls
;; through to the unsupported fetch-identity.

(defmethod authorize-url :oauth2
  [cfg {:keys [redirect-uri state]}]
  ;; No nonce/PKCE-challenge: plain OAuth2 has no ID token, and the confidential
  ;; client authenticates the exchange with client_secret; `state` carries CSRF.
  (str (:authorize-url cfg) "?"
       (codec/form-encode {:client_id (:client-id cfg)
                           :redirect_uri redirect-uri
                           :scope (or (:scopes cfg) "read:user user:email")
                           :state state
                           :response_type "code"})))

(defn- oauth2-token
  "Exchange an OAuth2 authorization code for an access token (confidential
   client, no PKCE). Returns the access-token string, or nil."
  [cfg code redirect-uri]
  (:access_token
   (send-json (-> (HttpRequest/newBuilder (URI/create (:token-url cfg)))
                  (.timeout (Duration/ofSeconds 5))
                  (.header "Content-Type" "application/x-www-form-urlencoded")
                  (.header "Accept" "application/json")
                  (.POST (HttpRequest$BodyPublishers/ofString
                          (codec/form-encode {:grant_type "authorization_code"
                                              :code code
                                              :redirect_uri redirect-uri
                                              :client_id (:client-id cfg)
                                              :client_secret (:client-secret cfg)})))
                  .build))))

(defmethod fetch-identity :github
  [cfg {:keys [code redirect-uri]}]
  (when-let [token (oauth2-token cfg code redirect-uri)]
    (let [user   (http-get-bearer (:userinfo-url cfg) token)
          ;; GitHub email is private/null by default AND must be verified. Pull
          ;; /user/emails and require a PRIMARY + VERIFIED address — the
          ;; GitHub-specific hijack defense (unverified-email takeover).
          emails (http-get-bearer (str (:userinfo-url cfg) "/emails") token)
          email  (some #(when (and (:primary %) (:verified %)) (:email %)) emails)]
      (when email
        {:iss    (or (:issuer cfg) "https://github.com")
         :sub    (str (:id user))    ; numeric id is stable; `login` is renameable
         :claims (assoc user :email email :email_verified true)}))))

;; Facebook (and any OAuth2 provider without its own defmethod) — userinfo
;; mapping differs (Graph API, app-scoped id). Build per provider when needed.
(defmethod fetch-identity :oauth2 [cfg _]
  (throw (ex-info "OAuth2 fetch-identity not implemented for this provider"
                  {:provider (:provider cfg)})))

;; --- Unknown provider --------------------------------------------------------

(defmethod authorize-url :default [cfg _]
  (throw (ex-info "Unsupported federation provider" {:provider (:provider cfg)})))
(defmethod fetch-identity :default [cfg _]
  (throw (ex-info "Unsupported federation provider" {:provider (:provider cfg)})))

;; =============================================================================
;; Ring handlers + mode helpers (login vs link)
;; =============================================================================

(defn- begin-upstream
  "Build PKCE + nonce, sign `extra` into our state, 302 to the IdP. `extra`
   carries the mode payload — login: {:ac auth-code}; link: {:link session-id}."
  [cfg provider extra]
  (let [n (b64url (nonce/random-bytes 16))
        {:keys [verifier challenge]} (pkce-pair)
        our-state (core/encrypt (merge {:p provider :n n :v verifier} extra))]
    {:status 302
     :headers {"Location" (authorize-url cfg {:redirect-uri (callback-uri)
                                              :state our-state
                                              :nonce n
                                              :challenge challenge})}}))

(defn- handle-login
  "Login mode: resolve (case 1) or provision (case 3) the user, then finish the
   downstream authorization_code flow."
  [id provider ac-code]
  (let [resolved (*resolve-identity* (:iss id) (:sub id) (:claims id))
        outcome  (cond
                   resolved                  {:user resolved}
                   (= :open *signup-policy*) (provision-user! provider (:iss id) (:sub id) (:claims id))
                   :else                     {:error "not_linked"})]
    (cond
      (:error outcome)
      (error-redirect (:error outcome))

      (nil? (get @ac/*authorization-codes* ac-code))
      (error-redirect "expired_code")

      :else
      (let [{:keys [redirect-uri response-mode cookies params]}
            (login/complete-authorization-code-login! ac-code (:user outcome) [(name provider)])]
        (login/authorization-response redirect-uri params response-mode cookies)))))

(defn- create-link!
  [provider iss sub claims owner]
  (dataset/sync-entity :id-federation/external-identity
                       {:provider (-> provider name str/upper-case)
                        :iss iss :sub sub :email (:email claims)
                        :user {:xid (:xid owner)}})
  (log/info {:id ::identity-linked
             :data {:action :created :subject :external-identity
                    :provider provider :user-xid (:xid owner)}}
            "Linked external identity to account"))

(defn- link-success [provider]
  {:status 302
   :headers {"Location" (str "/oauth/status?"
                             (codec/form-encode {:value "success"
                                                 :flow "federated_link"
                                                 :provider (name provider)}))}})

(defn- handle-link
  "Link mode: attach (iss,sub) to the CURRENTLY authenticated account. Hijack/
   CSRF defenses: the callback session must equal the session that started the
   link (state-bound), the link targets that session's user only (never a named
   target), and an identity already linked elsewhere is refused.

   TODO step-up: a stolen session could still link a new method — require fresh
   re-auth (password re-entry / recent authorized-at) before allowing a link."
  [request id provider link-session]
  (let [current (get-in request [:cookies "idsrv.session" :value])
        owner   (some-> current core/get-session-resource-owner)]
    (cond
      ;; CSRF: callback session must match the one that initiated the link
      (or (nil? current) (not= current link-session))
      (error-redirect "link_session_mismatch")

      (nil? owner)
      (error-redirect "link_requires_login")

      :else
      (let [existing (*resolve-identity* (:iss id) (:sub id) (:claims id))]
        (cond
          ;; already linked to THIS user — idempotent success
          (and existing (= (:xid existing) (:xid owner)))
          (link-success provider)

          ;; linked to ANOTHER account — refuse (one identity, one account)
          existing
          (error-redirect "identity_already_linked")

          :else
          (do (create-link! provider (:iss id) (:sub id) (:claims id) owner)
              (link-success provider)))))))

(defn start-handler
  "GET /oauth/federated/start?provider=<name> — bounce to the upstream IdP.
   LOGIN mode (default): carries the pending downstream auth-code (from `state`).
   LINK mode (`mode=link`): requires an authenticated session; carries that
   session id so the callback attaches the identity to the current account."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [provider state mode]} (:params request)
          cfg (resolve-provider provider)]
      (cond
        (nil? cfg)           (error-redirect "provider_unknown")
        (not (:enabled cfg)) (error-redirect "provider_disabled")

        (= mode "link")
        (let [session (get-in request [:cookies "idsrv.session" :value])]
          (if-not (some-> session core/get-session-resource-owner)
            (error-redirect "link_requires_login")
            (try (begin-upstream cfg provider {:link session})
                 (catch Throwable e
                   (log/warn {:id ::start-failed :data {:provider provider :err (.getMessage e)}}
                             "Federated link start failed")
                   (error-redirect "provider_unsupported")))))

        :else
        (let [ac-code (:authorization-code (when state (core/decrypt state)))]
          (if (nil? ac-code)
            (error-redirect "no_flow")
            (try (begin-upstream cfg provider {:ac ac-code})
                 (catch Throwable e
                   (log/warn {:id ::start-failed :data {:provider provider :err (.getMessage e)}}
                             "Federated start failed")
                   (error-redirect "provider_unsupported")))))))))

(defn callback-handler
  "GET /oauth/federated/callback — upstream redirect target. Branches on the
   signed state: LINK mode (state carries :link) attaches the identity to the
   current account; LOGIN mode completes the downstream authorization_code flow."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [code state iss error]} (:params request)
          {provider :p nonce :n verifier :v ac-code :ac link-session :link} (when state (core/decrypt state))
          cfg (resolve-provider provider)]
      (cond
        error       (error-redirect (str "idp_" error))
        (nil? cfg)  (error-redirect "state_invalid")
        (nil? code) (error-redirect "no_code")
        :else
        (try
          (let [id (fetch-identity cfg {:code code
                                        :redirect-uri (callback-uri)
                                        :verifier verifier
                                        :nonce nonce
                                        :iss iss})]
            ;; downstream needs the provider TYPE (:google), not the URL slug —
            ;; (:provider cfg) is the normalized dispatch keyword.
            (cond
              (nil? id)    (error-redirect "token_invalid")
              link-session (handle-link request id (:provider cfg) link-session)
              :else        (handle-login id (:provider cfg) ac-code)))
          (catch Throwable e
            (log/error {:id ::callback-failed :data {:provider provider :err (.getMessage e)}}
                       "Federated callback failed")
            (error-redirect "callback_error")))))))
