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

(ns synthigy.oauth.federated
  "Identity brokering — Synthigy as an OAuth Relying Party to upstream IdPs;
   federation is an alternate credential collector inside the existing
   authorization_code flow, and downstream clients only ever see Synthigy
   tokens, never an upstream IdP token. See docs/core/synthigy/oauth/federated.md."
  (:require
   [buddy.core.hash :as hash]
   [buddy.core.nonce :as nonce]
   [clojure.string :as str]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.iam.context :as iam.context]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.login :as login]
   [synthigy.oauth.onboarding :as onboarding])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.util Base64]))

;; =============================================================================
;; Data-model seams (PLUG these into datasets later)
;; =============================================================================
;;
;; Provider config (entity pins, `resolve-provider`, `list-providers`) lives in
;; `synthigy.oauth.federated.registry` — below this ns so the login page and
;; onboarding can read it without cycling back through the broker.

(defn default-resolve-identity
  "(iss, sub, claims) -> Synthigy resource-owner via the linked External
   Identity row, or nil if unlinked (fails closed, never auto-provisions)."
  [iss sub _claims]
  (when (and iss sub)
    (some-> (dataset/get-entity
             :id-federation/external-identity
             {:iss iss :sub sub}
             {:user [{:selections {:name nil}}]})
            :user :name
            (#(iam.context/get-user-details {:name %})))))

(defonce ^:dynamic *resolve-identity* default-resolve-identity)

;; Session must be freshly authenticated within this window to link a new
;; identity — closes the silent-link-via-stolen-session hole. Nil disables.
(defonce ^:dynamic *link-max-age-ms*
  (* 5 60 1000))

(defn session-fresh?
  "True if `session` authenticated within *link-max-age-ms*; fails closed."
  [session]
  (if-let [max-age *link-max-age-ms*]
    (if-let [^java.util.Date at (some-> session core/get-session-authorized-at)]
      (< (- (System/currentTimeMillis) (.getTime at)) max-age)
      false)
    true))

(defn provision-user!
  "First-time federated identity -> a fresh Synthigy user + External Identity
   link; refuses ({:error \"email_exists\"}) if the email is already owned."
  [provider iss sub claims]
  (let [email    (:email claims)
        username (or email sub)]
    (cond
      (and email (iam.context/get-user-details {:name email}))
      {:error "email_exists"}

      (nil? username)
      {:error "provision_failed"}

      :else
      (if-let [u (try (dataset/sync-entity :iam/user
                                           {:name username :active true :type :PERSON})
                       (catch Throwable e
                         (log/error! {:id ::provision-failed
                                      :data {:action :created :subject :federated-user
                                             :provider provider :sub sub}}
                                     e)
                         nil))]
        (try
          (dataset/sync-entity :id-federation/external-identity
                               {:provider (str/upper-case (name provider))
                                :iss iss :sub sub :email email
                                :linked_at (java.util.Date.)
                                :user {:xid (:xid u)}})
          (log/info {:id ::user-provisioned
                     :data {:action :created :subject :federated-user
                            :provider provider :username username}}
                    "Provisioned new user from federated identity")
          {:user (iam.context/get-user-details {:name username})}
          (catch Throwable e
            (log/error! {:id ::provision-failed
                         :data {:action :created :subject :federated-user
                                :provider provider :sub sub}}
                        e)
            (try (dataset/delete-entity :iam/user {:xid (:xid u)})
                 (catch Throwable e2
                   (log/error! {:id ::provision-rollback-failed
                                :data {:action :deleted :subject :federated-user
                                       :provider provider :sub sub}}
                               e2)))
            {:error "provision_failed"}))
        {:error "provision_failed"}))))

;; =============================================================================
;; Shared low-level HTTP (used by both provider families)
;; =============================================================================

(def ^:private http-client
  (delay (-> (HttpClient/newBuilder)
             (.connectTimeout (Duration/ofSeconds 5))
             .build)))

(defn send-json
  "Send an HttpRequest, parse JSON body; throws on >=400 (callers fail closed to
   an error redirect)."
  [^HttpRequest req]
  (let [resp (.send ^HttpClient @http-client req (HttpResponse$BodyHandlers/ofString))]
    (when (>= (.statusCode resp) 400)
      (throw (ex-info "Upstream HTTP error"
                      {:status (.statusCode resp) :body (.body resp)})))
    (json/read-str (.body resp))))

(defn http-json [url]
  (send-json (-> (HttpRequest/newBuilder (URI/create url))
                 (.timeout (Duration/ofSeconds 5))
                 (.header "Accept" "application/json")
                 .GET
                 .build)))

;; =============================================================================
;; PKCE + signed state + base64url
;; =============================================================================

(defn b64url [^bytes bs]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bs))

(defn pkce-pair []
  (let [verifier (b64url (nonce/random-bytes 32))]
    {:verifier verifier
     ;; S256: challenge = base64url(sha256(verifier))
     :challenge (b64url (hash/sha256 verifier))}))

(defn callback-uri []
  ;; Exact, stable redirect_uri — identical on start + callback, registered at
  ;; the IdP.
  (str (core/domain+) "/oauth/federated/callback"))

(defn error-redirect
  "302 to /oauth/status, or to `target` (+ `params`) when the caller has a page
   the person can actually retry from."
  ([err] (error-redirect err nil nil))
  ([err target] (error-redirect err target nil))
  ([err target params]
   (log/warn {:id ::federated-error :data {:action :credentials-rejected
                                           :subject :federated
                                           :error err}}
             "Federated login failed")
   {:status 302
    :headers {"Location" (str (or target "/oauth/status") "?"
                              (codec/form-encode
                               (merge (if target
                                        {:error err}
                                        {:value "error" :flow "federated" :error err})
                                      params)))}}))

(defn login-retry
  "Send a still-recoverable login failure back to the login page, carrying the
   flow's own state."
  [err-code ac-code]
  (error-redirect err-code "/oauth/login"
                  {:state (core/encrypt {:authorization-code ac-code})}))

;; =============================================================================
;; Provider dispatch (per-provider, families share one implementation)
;; =============================================================================

(def ^:private provider-families
  (-> (make-hierarchy)
      (derive :google    :oidc)
      (derive :microsoft :oidc)
      (derive :linkedin  :oidc)
      (derive :oidc_1    :oidc)
      (derive :oidc_2    :oidc)
      (derive :oidc_3    :oidc)
      (derive :github    :oauth2)
      (derive :facebook  :oauth2)
      (derive :discord   :oauth2)))

(defmulti authorize-url
  "Upstream authorize-redirect URL; ctx: {:redirect-uri :state :nonce
   :challenge}."
  (fn [cfg _ctx] (:provider cfg))
  :hierarchy #'provider-families)

(defmulti fetch-identity
  "Exchange the callback code for {:iss :sub :claims}, or nil on failure; ctx:
   {:code :redirect-uri :verifier :nonce :iss}."
  (fn [cfg _ctx] (:provider cfg))
  :hierarchy #'provider-families)

;; --- Unknown provider --------------------------------------------------------

(defmethod authorize-url :default [cfg _]
  (throw (ex-info "Unsupported federation provider" {:provider (:provider cfg)})))
(defmethod fetch-identity :default [cfg _]
  (throw (ex-info "Unsupported federation provider" {:provider (:provider cfg)})))

;; Plain `require` (not a static :require) avoids a load cycle: the families
;; require this ns for the multimethod declarations above.
(require 'synthigy.oauth.federated.oidc)
(require 'synthigy.oauth.federated.oauth2)

;; =============================================================================
;; Ring handlers + mode helpers (login vs link)
;; =============================================================================

(defn begin-upstream
  "Build PKCE + nonce, sign `extra` (the mode payload) into state, 302 to the
   IdP."
  ([cfg provider extra] (begin-upstream cfg provider extra nil))
  ([cfg provider extra prompt]
   (let [n (b64url (nonce/random-bytes 16))
         {:keys [verifier challenge]} (pkce-pair)
         our-state (core/encrypt (merge {:p provider :n n :v verifier} extra))]
     {:status 302
      :headers {"Location" (authorize-url cfg (cond-> {:redirect-uri (callback-uri)
                                                       :state our-state
                                                       :nonce n
                                                       :challenge challenge}
                                                prompt (assoc :prompt prompt)))}})))

(defn handle-login
  "Login mode: resolve or provision (if the client allows signup) the user, then
   finish the downstream authorization_code flow."
  [id provider ac-code client-info]
  (if (nil? (ac/get-code ac-code))
    (error-redirect "expired_code")
    (let [resolved (*resolve-identity* (:iss id) (:sub id) (:claims id))
          client   (ac/get-code-client ac-code)
          outcome  (cond
                     resolved
                     {:user resolved}

                     (get-in client [:settings "allow-signup"])
                     (provision-user! provider (:iss id) (:sub id) (:claims id))

                     :else
                     {:error "not_linked"})]
      (if (:error outcome)
        (login-retry (:error outcome) ac-code)
        (let [{:keys [redirect-uri response-mode cookies params]}
              (login/complete-authorization-code-login! ac-code (:user outcome) [(name provider)]
                                                        client-info)]
          (login/authorization-response redirect-uri params response-mode cookies))))))

(defn console-return
  "Sanitize a caller-supplied console return path; nil unless site-relative."
  [r]
  (when (and (string? r) (str/starts-with? r "/") (not (str/starts-with? r "//")))
    r))

(def allowed-prompts
  "`prompt` values a caller may ask for; anything else is dropped."
  #{"select_account" "login"})

(defn safe-prompt
  "A caller-supplied prompt, or nil — this lands in the upstream authorize URL."
  [p]
  (allowed-prompts (some-> p str)))

(defn return-target
  "`return` reduced to its path, for use as an error redirect target."
  [r]
  (some-> (console-return r) (str/replace #"[?#].*" "") not-empty))

(defn handle-console-login
  "Console mode: mint a console session directly for an already-linked identity;
   deliberately no provisioning branch."
  [id provider return client-info]
  (if-let [resolved (*resolve-identity* (:iss id) (:sub id) (:claims id))]
    (let [sid (core/gen-session-id)]
      (core/create-session! sid (merge {:user resolved :flow "console"
                                        :authorized-at (java.util.Date.)
                                        :amr [(name provider)]}
                                       client-info))
      (log/info {:id ::console-login :data {:action :authenticated :subject :console
                                            :provider provider :user (:name resolved)}}
                "Console login via federated identity")
      {:status 303
       :headers {"Location" (or (console-return return) "/console")}
       :cookies (login/session-cookie sid)})
    (error-redirect "not_linked" "/console/login")))

(defn handle-reauth
  "Step-up through an ALREADY-LINKED identity: stamp the current session as
   freshly authenticated rather than minting a second one."
  [request id provider reauth-session return]
  (let [current (get-in request [:cookies "idsrv.session" :value])
        owner   (some-> current core/get-session-resource-owner)
        err     #(error-redirect % (return-target return))]
    (cond
      (or (nil? current) (not= current reauth-session))
      (err "link_session_mismatch")

      (nil? owner)
      (err "link_requires_login")

      :else
      (let [resolved (*resolve-identity* (:iss id) (:sub id) (:claims id))]
        (if (and resolved (= (:xid resolved) (:xid owner)))
          (do
            (core/set-session-authorized-at current (java.util.Date.))
            (log/info {:id ::reauthenticated
                       :data {:action :authenticated :subject :console
                              :provider provider :user (:name owner)}}
                      "Session stepped up via a linked federated identity")
            {:status 303 :headers {"Location" (or (console-return return) "/console")}})
          (err "reauth_identity_mismatch"))))))

(defn create-link!
  [provider iss sub claims owner]
  (dataset/sync-entity :id-federation/external-identity
                       {:provider (str/upper-case (name provider))
                        :iss iss :sub sub :email (:email claims)
                        :linked_at (java.util.Date.)
                        :user {:xid (:xid owner)}})
  (log/info {:id ::identity-linked
             :data {:action :created :subject :external-identity
                    :provider provider :user-xid (:xid owner)}}
            "Linked external identity to account"))

(defn link-success [provider return]
  {:status 302
   :headers {"Location"
             (or (console-return return)
                 (str "/oauth/status?"
                      (codec/form-encode {:value "success"
                                          :flow "federated_link"
                                          :provider (name provider)})))}})

(defn handle-link
  "Link mode: attach (iss,sub) to the currently authenticated account, with
   CSRF/hijack guards."
  [request id provider link-session return]
  (let [current (get-in request [:cookies "idsrv.session" :value])
        owner   (some-> current core/get-session-resource-owner)
        err     #(error-redirect % (return-target return))]
    (cond
      ;; CSRF: callback session must match the one that initiated the link
      (or (nil? current) (not= current link-session))
      (err "link_session_mismatch")

      (nil? owner)
      (err "link_requires_login")

      ;; step-up: the session must have authenticated recently to add a login
      ;; method
      (not (session-fresh? current))
      (err "link_reauth_required")

      :else
      (let [existing (*resolve-identity* (:iss id) (:sub id) (:claims id))]
        (cond
          ;; already linked to THIS user — idempotent success
          (and existing (= (:xid existing) (:xid owner)))
          (link-success provider return)

          ;; linked to ANOTHER account — refuse (one identity, one account)
          existing
          (err "identity_already_linked")

          :else
          (do (create-link! provider (:iss id) (:sub id) (:claims id) owner)
              (link-success provider return)))))))

;; -----------------------------------------------------------------------------
;; Admin onboarding — Auth0 "tickets" pattern: a confidential client mints a
;; one-time claim link, Synthigy owns the token, the client owns delivery.

(defn json-response [status body]
  {:status status :headers {"Content-Type" "application/json"} :body (json/write-str body)})

(defn handle-claim
  "Claim callback: staple the just-authenticated (iss,sub) to the token's target
   user by stable xid, activate the account, and burn the nonce."
  [id provider {:keys [u n r]}]
  (let [target (onboarding/claim-target u)]
    (cond
      (nil? target)
      (error-redirect "claim_invalid")

      ;; identity already tied to another account — never reassign
      (some-> (*resolve-identity* (:iss id) (:sub id) (:claims id)) :xid (not= u))
      (error-redirect "identity_already_linked")

      ;; CAS gate FIRST (see onboarding/finish-claim!): only a request that
      ;; actually wins the claim may link the identity — a losing concurrent
      ;; request (nonce already burned by whoever won first) must not
      ;; mutate the account at all.
      (onboarding/finish-claim! target n)
      (do
        (create-link! provider (:iss id) (:sub id) (:claims id) target)
        (log/info {:id ::account-claimed
                   :data {:action :created :subject :federated-user
                          :provider provider :user (:name target)}}
                  "Account claimed via onboarding link")
        (onboarding/claim-success-redirect (name provider) r))

      :else
      (error-redirect "claim_invalid"))))

(defn start-handler
  "GET /oauth/federated/start?provider=<type>&mode=<login|link|console|claim> —
   bounce to the upstream IdP for the given mode."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [provider state mode return]} (:params request)
          cfg (registry/resolve-provider provider)
          err (fn [code]
                (cond
                  (= mode "console") (error-redirect code "/console/login")
                  (and (nil? mode) state) (error-redirect code "/oauth/login"
                                                          {:state state})
                  :else (error-redirect code (return-target return))))]
      (cond
        (nil? cfg)           (err "provider_unknown")
        (not (:enabled cfg)) (err "provider_disabled")

        (= mode "console")
        (try (begin-upstream cfg provider (cond-> {:console true}
                                            (console-return return) (assoc :r return))
                             (safe-prompt (:prompt (:params request))))
             (catch Throwable e
               (log/warn {:id ::start-failed :data {:provider provider :err (.getMessage e)}}
                         "Federated console-login start failed")
               (err "provider_unsupported")))

        (= mode "reauth")
        (let [session (get-in request [:cookies "idsrv.session" :value])]
          (if-not (some-> session core/get-session-resource-owner)
            (err "link_requires_login")
            (try (begin-upstream cfg provider (cond-> {:reauth session}
                                                (console-return return) (assoc :r return))
                                  "login")
                 (catch Throwable e
                   (log/warn {:id ::start-failed
                              :data {:provider provider :err (.getMessage e)}}
                             "Federated reauth start failed")
                   (err "provider_unsupported")))))

        (= mode "link")
        (let [session (get-in request [:cookies "idsrv.session" :value])]
          (cond
            (not (some-> session core/get-session-resource-owner))
            (err "link_requires_login")

            ;; step-up: fail fast before bouncing to the IdP if auth is stale
            (not (session-fresh? session))
            (err "link_reauth_required")

            :else
            (try (begin-upstream cfg provider (cond-> {:link session}
                                                (console-return return) (assoc :r return))
                                  "select_account")
                 (catch Throwable e
                   (log/warn {:id ::start-failed :data {:provider provider :err (.getMessage e)}}
                             "Federated link start failed")
                   (err "provider_unsupported")))))

        ;; claim mode: `state` here IS the onboarding token (not a downstream
        ;; flow)
        (= mode "claim")
        (if-let [{:keys [u n m r]} (onboarding/valid-claim state)]
          ;; `m` is keyed by dispatch FAMILY ("google"), not the row's URL slug.
          (if (and m (not (contains? m (name (:provider cfg)))))
            (error-redirect "claim_method_not_allowed")
            (try (begin-upstream cfg provider {:claim {:u u :n n :r r}})
                 (catch Throwable e
                   (log/warn {:id ::start-failed :data {:provider provider :err (.getMessage e)}}
                             "Federated claim start failed")
                   (error-redirect "provider_unsupported"))))
          (error-redirect "claim_invalid"))

        :else
        (let [ac-code (:authorization-code (when state (core/decrypt state)))]
          (if (nil? ac-code)
            (error-redirect "no_flow")
            (try (begin-upstream cfg provider {:ac ac-code})
                 (catch Throwable e
                   (log/warn {:id ::start-failed :data {:provider provider :err (.getMessage e)}}
                             "Federated start failed")
                   (err "provider_unsupported")))))))))

(defn callback-handler
  "GET /oauth/federated/callback — upstream redirect target; branches on the
   signed state to link/console/login mode."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [code state iss error]} (:params request)
          {provider :p nonce :n verifier :v ac-code :ac link-session :link
           claim :claim console? :console reauth-session :reauth return :r}
          (when state (core/decrypt state))
          cfg (registry/resolve-provider provider)
          client-info (core/client-info request)
          err (fn [code]
                (cond
                  console?     (error-redirect code "/console/login")
                  (or link-session reauth-session) (error-redirect code (return-target return))
                  ac-code      (login-retry code ac-code)
                  :else        (error-redirect code (return-target return))))]
      (cond
        error       (err (str "idp_" error))
        (nil? cfg)  (err "state_invalid")
        (nil? code) (err "no_code")
        :else
        (try
          (let [id (fetch-identity cfg {:code code
                                        :redirect-uri (callback-uri)
                                        :verifier verifier
                                        :nonce nonce
                                        :iss iss})]
            (cond
              (nil? id)    (err "token_invalid")
              claim        (handle-claim id (:provider cfg) claim)
              reauth-session (handle-reauth request id (:provider cfg) reauth-session return)
              link-session (handle-link request id (:provider cfg) link-session return)
              console?     (handle-console-login id (:provider cfg) return client-info)
              :else        (handle-login id (:provider cfg) ac-code client-info)))
          (catch Throwable e
            (log/error {:id ::callback-failed :data {:provider provider :err (.getMessage e)}}
                       "Federated callback failed")
            (err "callback_error")))))))

(defn providers-handler
  "GET /oauth/federated/providers — public JSON list of active providers (never
   secrets)."
  [_request]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/write-str (registry/list-providers))})

;; -----------------------------------------------------------------------------
;; Authenticated account-management: list + unlink the current user's
;; identities.
;; (Add is the existing link-mode: start?provider=X&mode=link.)

(defn session-user
  "Current user (name + password + linked identities) from the idsrv.session
   cookie, or nil."
  [request]
  (when-let [owner (some-> (get-in request [:cookies "idsrv.session" :value])
                           core/get-session-resource-owner)]
    (dataset/get-entity :iam/user {:name (:name owner)}
                        {:name nil :password nil
                         :external_identities
                         [{:args {:_join :left}
                           :selections {:xid nil :provider nil :email nil :linked_at nil}}]})))

(defn identities-handler
  "GET /oauth/federated/identities — the current user's linked sign-in methods."
  [request]
  (if-let [user (session-user request)]
    (json-response 200 {:identities (mapv #(select-keys % [:xid :provider :email :linked_at])
                                          (:external_identities user))})
    (json-response 401 {:error "link_requires_login"})))

(defn unlink-identity!
  "Shared unlink logic for session- and client-scoped routes; refuses if `xid`
   is the user's last login method."
  [user xid]
  (cond
    (nil? xid)
    (json-response 400 {:error "missing_xid"})

    :else
    (let [ids    (:external_identities user)
          target (some #(when (= xid (:xid %)) %) ids)]
      (cond
        ;; not among THIS user's identities — don't confirm existence
        (nil? target)
        (json-response 404 {:error "identity_not_found"})

        ;; last login method — refuse (federated-only user with one identity)
        (and (nil? (:password user)) (<= (count ids) 1))
        (json-response 409 {:error "last_login_method"})

        :else
        (do (dataset/delete-entity :id-federation/external-identity {:xid xid})
            (log/info {:id ::identity-unlinked
                       :data {:action :deleted :subject :external-identity
                              :provider (:provider target) :user (:name user)}}
                      "Unlinked external identity from account")
            (json-response 200 {:ok true}))))))

(defn unlink-handler
  "POST /oauth/federated/unlink {xid} — remove one of the current user's linked
   identities; requires fresh auth."
  [request]
  (let [session (get-in request [:cookies "idsrv.session" :value])
        xid     (get-in request [:params :xid])
        user    (session-user request)]
    (cond
      (nil? user)                    (json-response 401 {:error "link_requires_login"})
      (not (session-fresh? session)) (json-response 403 {:error "link_reauth_required"})
      :else                          (unlink-identity! user xid))))

;; -----------------------------------------------------------------------------
;; Client-scoped identity management (P5) — the BFF/support-desk face, a
;; separate route from the session-scoped handlers above (P0 two-faces rule).

(defn target-user
  "Resolve an explicit target user by xid — same shape `session-user` returns."
  [user-xid]
  (when user-xid
    (dataset/get-entity :iam/user {:xid user-xid}
                        {:xid nil :name nil :password nil
                         :external_identities
                         [{:args {:_join :left}
                           :selections {:xid nil :provider nil :email nil :linked_at nil}}]})))

(defn client-identities-handler
  "GET /oauth/federated/client/identities?user=<xid> — a provisioning
   principal lists the linked sign-in methods of a user it administers."
  [request]
  (if-let [{:keys [principal]} (onboarding/provisioner request)]
    (if-let [user (target-user (get-in request [:params :user]))]
      (if (onboarding/administers? principal user)
        (json-response 200 {:identities (mapv #(select-keys % [:xid :provider :email :linked_at])
                                              (:external_identities user))})
        (json-response 403 {:error "provision_forbidden"}))
      (json-response 404 {:error "user_not_found"}))
    (json-response 403 {:error "provision_forbidden"})))

(defn client-unlink-handler
  "POST /oauth/federated/client/unlink {user, xid} — a provisioning principal
   unlinks an identity of a user it administers."
  [request]
  (if-let [{:keys [principal]} (onboarding/provisioner request)]
    (let [{:keys [user xid]} (:params request)]
      (if-let [target (target-user user)]
        (if (onboarding/administers? principal target)
          (unlink-identity! target xid)
          (json-response 403 {:error "provision_forbidden"}))
        (json-response 404 {:error "user_not_found"})))
    (json-response 403 {:error "provision_forbidden"})))
