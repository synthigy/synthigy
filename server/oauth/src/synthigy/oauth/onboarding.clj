;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.onboarding
  "Admin-provisioned onboarding: one-time, account-bound claim tokens redeemed with a first credential."
  (:require
   [buddy.core.nonce :as nonce]
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.id :as id]
   [synthigy.dataset.sql.query :as query]
   [synthigy.dataset.sql.schema :as schema]
   [synthigy.db :as db]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.custom :as custom]
   [synthigy.oauth.page.login :as page.login]
   [synthigy.oauth.authentication :as auth])
  (:import
   [java.util Base64]))

(defn b64url [^bytes bs]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bs))

(defn json-response [status body]
  {:status status :headers {"Content-Type" "application/json"} :body (json/write-str body)})

(defn error-redirect [err]
  (log/warn {:id ::onboarding-error :data {:action :credentials-rejected
                                           :subject :onboarding
                                           :error err}}
            "Onboarding claim failed")
  {:status 302
   :headers {"Location" (str "/oauth/status?"
                             (codec/form-encode {:value "error"
                                                 :flow "onboarding"
                                                 :error err}))}})

(def ^:private invite-ttl-ms (* 24 60 60 1000)) ; 24h default

(defn now-ms [] (System/currentTimeMillis))

(defn provisioner
  "Confidential active client and principal behind the request's bearer, or nil."
  [request]
  (when-let [{:keys [principal claims]} (auth/authenticate-request request)]
    (let [client (some-> (:client_id claims) core/get-client)]
      (when (and client
                 (:active client)
                 (not (#{:public "public"} (:type client))))
        {:client client :principal principal}))))

(defn administers?
  "Whether `principal` may update `user`: RBAC :update on User and the row in its write scope."
  [principal user]
  (access/with-principal principal
    (and (access/entity-allows? (id/entity :iam/user) [:update])
         (query/row-writable? (id/entity :iam/user) (id/extract user)))))

(defn user-identity-xids
  "XIDs of a user's external-identity rows."
  [username]
  (->> (dataset/get-entity :iam/user {:name username}
                           {:external_identities [{:args {:_join :left}
                                                   :selections {:xid nil}}]})
       :external_identities (map :xid) (remove nil?)))

(defn kill-user-sessions!
  "Revoke every live OAuth session (and their DB-tracked tokens) of `user-xid`."
  [user-xid]
  ;; :_join :inner is load-bearing — flat-LEFT would match every active session in the system.
  (doseq [{sid :id} (dataset/search-entity
                     (id/entity :oauth/session)
                     {:active {:_eq true}}
                     {:id nil
                      :user [{:selections {(id/key) nil}
                              :args {:_join :inner
                                     :_where {(id/key) {:_eq user-xid}}}}]})]
    (core/kill-session sid)))

(def ^:private deactivation-watcher-key ::deactivation-watcher)

(defn user-still-inactive?
  "Re-read the current `active` flag rather than decoding the delta envelope."
  [record-xid]
  (false? (:active (dataset/get-entity :iam/user {:xid record-xid} {:active nil}))))

(defn start-deactivation-watcher!
  "Idempotently subscribe to :iam/user updates and kill the sessions of users left inactive."
  []
  (when-let [user-xid (some-> (id/entity :iam/user) str)]
    (delta/subscribe!
     deactivation-watcher-key
     {:entity-xids #{user-xid} :ops #{:update}}
     (fn [env]
       (when-let [record-xid (some-> env :delta :data :record-xid)]
         (when (user-still-inactive? record-xid)
           (kill-user-sessions! record-xid)
           (log/info {:id ::user-deactivated
                      :data {:action :killed :subject :onboarding-user :user record-xid}}
                     "User deactivated — live sessions and their tokens revoked")))))))

(defn stop-deactivation-watcher!
  []
  (delta/unsubscribe! deactivation-watcher-key))

(defn reset-account!
  "Soft recycle before re-onboarding: strip identities, null the password, kill sessions; never touches `active`."
  [user]
  (doseq [xid (user-identity-xids (:name user))]
    (dataset/delete-entity :id-federation/external-identity {:xid xid}))
  (dataset/sync-entity :iam/user {:name (:name user) :password nil})
  (kill-user-sessions! (:xid user))
  (log/info {:id ::account-reset
             :data {:action :deleted :subject :onboarding-user :user (:name user)}}
            "Reset account for re-onboarding"))

(defn issue-onboard-token!
  "Stamp a fresh nonce on the user and return the signed claim token, invalidating any prior one."
  [user ttl-ms methods client-id return-url]
  (let [nonce (b64url (nonce/random-bytes 16))]
    (dataset/sync-entity :iam/user
                         {:name (:name user)
                          :settings (assoc (or (:settings user) {}) "onboard-nonce" nonce)})
    {:token (core/encrypt (cond-> {:claim (:xid user) :n nonce :exp (+ (now-ms) ttl-ms) :c client-id}
                             methods (assoc :m methods)
                             return-url (assoc :r return-url)))
     :exp   (+ (now-ms) ttl-ms)}))

(defn app-url
  "The client's App URL setting, where its people land after setting up an account."
  [client]
  (not-empty (get-in client [:settings "app-url"])))

(defn return-url-allowed?
  "Open-redirect guard: `return-url` must match a registered redirection or loopback path (RFC 8252 §7.3)."
  [client return-url]
  (let [{{redirections "redirections"} :settings} client]
    (or (some #(= (core/get-base-uri return-url) %) redirections)
        (core/loopback-redirect-matches? return-url redirections))))

(defn onboard-handler
  "POST /oauth/onboard {xid, reset?, ttl_seconds?, methods?, return_url?} — mint a one-time claim URL."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (if-let [{:keys [client principal]} (provisioner request)]
      (let [{:keys [xid reset ttl_seconds methods return_url]} (core/parse-body request)
            ttl     (if ttl_seconds (* 1000 (parse-long (str ttl_seconds))) invite-ttl-ms)
            methods (when (seq methods) (set (map name (if (string? methods) [methods] methods))))
            reset?  (contains? #{"true" true "1"} reset)
            user    (when-not (str/blank? xid) (iam.context/get-user-details {:xid xid}))]
        (cond
          (str/blank? xid)
          (json-response 400 {:error "xid_required"})

          (nil? user)
          (json-response 404 {:error "user_not_found"})

          (not (administers? principal user))
          (json-response 403 {:error "provision_forbidden"})

          (and (not (str/blank? return_url)) (not (return-url-allowed? client return_url)))
          (json-response 400 {:error "return_url_not_registered"})

          :else
          (let [_ (when reset? (reset-account! user))
                {:keys [token exp]} (issue-onboard-token! user ttl methods (:id client)
                                                          (if (str/blank? return_url)
                                                            (app-url client)
                                                            return_url))]
            (log/info {:id ::onboard-link-issued
                       :data {:action :created :subject :onboarding-user
                              :user (:name user) :client (:id client)
                              :principal (:name principal) :reset reset?}}
                      "Issued onboarding claim link")
            (json-response 200 {:onboard_url (str (core/domain+)
                                                  "/oauth/claim?"
                                                  (codec/form-encode {:token token}))
                                :expires_at exp
                                :user {:xid (:xid user)}}))))
      (json-response 403 {:error "provision_forbidden"}))))

(defn valid-claim
  "Claim token -> {:u :n :m :c :r} when signed, unexpired and its nonce is current; else nil."
  [token]
  (when-let [{:keys [claim n exp m c r]} (some-> token core/decrypt)]
    (when (and claim n exp (< (now-ms) exp))
      (let [m       (when (seq m) (set m))
            current (-> (dataset/get-entity :iam/user {:xid claim} {:settings nil})
                        :settings (get "onboard-nonce"))]
        (when (= n current) {:u claim :n n :m m :c c :r r})))))

(defn claim-target
  [u]
  (dataset/get-entity :iam/user {:xid u} {:name nil :xid nil :settings nil}))

;; CAS, never a blind write — true iff THIS call burned the current nonce; false = claim_invalid.
(defn finish-claim!
  [target nonce]
  (let [{:keys [table]} (schema/deployed-schema-entity (id/entity :iam/user))
        sql (str "UPDATE \"" table "\""
                 " SET active = TRUE, settings = " (db/json-remove db/*db* "settings" "onboard-nonce")
                 " WHERE name = ? AND " (db/json-get-text db/*db* "settings" "onboard-nonce") " = ?")]
    (pos? (:next.jdbc/update-count
           (jdbc/execute-one!
            (:datasource db/*db*)
            [sql (:name target) nonce])))))

(defn claim-success-redirect
  "Redirect to the mint-time-validated `return-url`, else the status page."
  ([method-name] (claim-success-redirect method-name nil))
  ([method-name return-url]
   (if return-url
     {:status 302 :headers {"Location" return-url}}
     {:status 302
      :headers {"Location" (str "/oauth/status?"
                                (codec/form-encode {:value "success"
                                                    :flow "onboarding_claim"
                                                    :provider method-name}))}})))

(def ^:private min-password-length
  8) ; ponytail: length-only policy; add a real password-strength check if compliance requires

(def claim-errors
  {"password_too_short" (str "Password must be at least " min-password-length " characters.")})

(defn render-claim-page
  "Claim page offering only the `allowed` methods: the default bundle's claim.html when it has one, else built-in."
  ([token allowed] (render-claim-page token allowed nil))
  ([token allowed error]
   (or (custom/custom-page-redirect "claim.html"
                                    (cond-> {:token token}
                                      allowed (assoc :methods (str/join "," (sort allowed)))
                                      error (assoc :error error)))
       {:status 200
        :headers {"Content-Type" "text/html"}
        :body (str (page.login/claim-html
                    token
                    (cond->> (registry/list-providers)
                      allowed (filter #(contains? allowed (name (:provider %)))))
                    (or (nil? allowed) (contains? allowed "password"))
                    (claim-errors error)))})))

(defn claim-page-handler
  "GET /oauth/claim — claim page; an invalid token never reveals whether the account exists."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [token (get-in request [:params :token])]
      (if-let [{:keys [m]} (valid-claim token)]
        (render-claim-page token m)
        (error-redirect "claim_invalid")))))

(defn claim-password-handler
  "POST /oauth/claim/password {token, password} — complete a claim with a first password."
  [request]
  (binding [core/*domain* (core/original-uri request)]
    (let [{:keys [token password]} (:params request)
          claim (valid-claim token)]
      (cond
        (nil? claim)
        (error-redirect "claim_invalid")

        (and (:m claim) (not (contains? (:m claim) "password")))
        (error-redirect "claim_method_not_allowed")

        (or (str/blank? password) (< (count password) min-password-length))
        (render-claim-page token (:m claim) "password_too_short")

        :else
        (let [target (claim-target (:u claim))]
          (cond
            (nil? target)
            (error-redirect "claim_invalid")

            ;; CAS gate FIRST — a losing request must not mutate the account, not even the password.
            (finish-claim! target (:n claim))
            (do
              (dataset/sync-entity :iam/user
                                   {:name (:name target) :password password})
              (log/info {:id ::account-claimed
                         :data {:action :created :subject :onboarding-user
                                :provider :password :user (:name target)}}
                        "Account claimed via password bootstrap")
              (claim-success-redirect "password" (:r claim)))

            :else
            (error-redirect "claim_invalid")))))))

(defn onboard-complete-handler
  "POST /oauth/onboard/complete {ticket} — the minting client activates the account; never sets a credential."
  [request]
  (if-let [{:keys [client principal]} (provisioner request)]
    (let [{:keys [ticket]} (core/parse-body request)
          claim (valid-claim ticket)]
      (cond
        (nil? claim)
        (json-response 400 {:error "claim_invalid"})

        ;; "no such ticket" and "not your ticket" must read the same: claim_invalid.
        (not= (:id client) (:c claim))
        (json-response 400 {:error "claim_invalid"})

        :else
        (let [target (claim-target (:u claim))]
          (cond
            (nil? target)
            (json-response 400 {:error "claim_invalid"})

            (not (administers? principal target))
            (json-response 403 {:error "provision_forbidden"})

            ;; CAS gate FIRST — a losing request must not mutate the account.
            (finish-claim! target (:n claim))
            (do
              (log/info {:id ::account-claimed
                         :data {:action :created :subject :onboarding-user
                                :provider :none :client (:id client) :user (:name target)}}
                        "Account claimed via indirect onboarding completion — no credential set")
              (json-response 200 {:user {:xid (:u claim)} :active true}))

            :else
            (json-response 400 {:error "claim_invalid"})))))
    (json-response 403 {:error "provision_forbidden"})))
