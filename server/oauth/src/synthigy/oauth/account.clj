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

(ns synthigy.oauth.account
  "Self-service account: every user reads and changes their own profile, password, sign-in methods and sessions."
  (:require
   [clojure.string :as str]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.id :as id]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.authentication :as authentication]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.credentials :as credentials]
   [synthigy.oauth.federated :as federated]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.login :as login]
   [synthigy.oauth.page.account :as page]
   [synthigy.oauth.page.custom :as custom]))

(def max-display-name 64)

(defn account-user
  [xid]
  (when xid
    (let [user (dataset/get-entity :iam/user {:xid xid}
                                   {:xid nil :name nil :display_name nil :active nil :password nil
                                    :external_identities
                                    [{:args {:_join :left}
                                      :selections {:xid nil :provider nil :email nil :linked_at nil}}]})]
      (when (:active user) user))))

(defn caller
  "Who asks: the bearer token's subject (platform audience), else the session cookie's owner; {:user :session} or nil."
  [request]
  (if (authentication/extract-bearer-token request)
    (when-let [{:keys [principal claims]} (authentication/authenticate-request request core/platform-audience)]
      (when-let [user (account-user (:xid principal))]
        {:user user :session (core/sid-session (:sid claims))}))
    (let [sid (get-in request [:cookies (core/session-cookie-name request) :value])]
      (when-let [user (some-> sid core/live-session-owner :xid account-user)]
        {:user user :session sid}))))

(defn fresh? [{:keys [session]}] (federated/session-fresh? session))

(defn owned-sessions
  [user-xid]
  ;; :_join :inner is load-bearing — flat-LEFT would match every active session in the system.
  (dataset/search-entity
   (id/entity :oauth/session)
   {:_where {:active {:_eq true}}}
   {:id nil :xid nil :started nil :last_seen nil :context nil
    :client [{:selections {:name nil} :args {:_join :left}}]
    :user [{:selections {:xid nil}
            :args {:_join :inner :_where {:xid {:_eq user-xid}}}}]}))

(defn sessions
  "Live sessions of `user-xid`, newest first, `current` marking `session`; the session id never leaves."
  [user-xid session]
  (->> (owned-sessions user-xid)
       (sort-by :started #(compare %2 %1))
       (mapv (fn [{:keys [id xid started last_seen context client]}]
               {:xid xid
                :client (:name client)
                :flow (get context "flow")
                :started started
                :last_seen last_seen
                :ip (get context "ip")
                :agent (get context "agent")
                :current (= id session)}))))

(defn profile
  [user]
  {:xid (:xid user)
   :login (:name user)
   :display_name (:display_name user)
   :has_password (not (str/blank? (:password user)))})

(defn view
  "The account as its owner sees it."
  [{:keys [user session] :as c}]
  {:profile (profile user)
   :identities (mapv #(select-keys % [:xid :provider :email :linked_at]) (:external_identities user))
   :sessions (sessions (:xid user) session)
   :fresh (fresh? c)})

(defn html? [request]
  (some-> (get-in request [:headers "accept"]) (str/includes? "text/html")))

(defn form-post? [request]
  (some-> (get-in request [:headers "content-type"]) str/lower-case
          (str/starts-with? "application/x-www-form-urlencoded")))

(defn body-params
  [request]
  (merge (:params request)
         (when-not (form-post? request)
           (let [b (core/parse-body request)] (when (map? b) b)))))

(defn json-response [status body]
  {:status status
   :headers {"Content-Type" "application/json" "Cache-Control" "no-store"}
   :body (json/write-str body)})

(defn back-link
  "Where \"Back to <app>\" goes: `return` only when it is a registered redirect URI of the active app `client_id`."
  [{:keys [client_id return]}]
  (when-let [client (and (string? client_id) (string? return) (core/get-client client_id))]
    (when (and (:active client)
               (core/redirect-allowed? return (get-in client [:settings "redirections"])
                                       (core/native-client? client)))
      {:name (:name client) :uri return})))

(defn back-params
  "The `client_id` and `return` of `params` worth carrying along, or nil."
  [params]
  (when (back-link params) (select-keys params [:client_id :return])))

(defn to-page
  [request query]
  {:status 303
   :headers {"Location" (login/account-path (merge (back-params (body-params request)) query))}})

(defn respond
  "JSON for API callers; a form post from the account page goes back to it with `done` or the error."
  [request status body done]
  (if (form-post? request)
    (to-page request (if (< status 300) (when done {:done done}) {:error (:error body)}))
    (json-response status body)))

(defn unauthorized
  [request]
  (let [login-url (login/account-login-url (back-params (body-params request)) nil)]
    (cond
      (form-post? request)
      {:status 303 :headers {"Location" login-url}}

      (authentication/extract-bearer-token request)
      (assoc-in (json-response 401 {:error "invalid_token"})
                [:headers "WWW-Authenticate"] "Bearer error=\"invalid_token\"")

      :else
      (assoc-in (json-response 401 {:error "login_required" :login_url login-url})
                [:headers "WWW-Authenticate"] "Bearer"))))

(defn with-caller
  "Run `f` on the caller of an account POST: a cookie only counts from the engine's own pages, and someone must be signed in."
  [request f]
  (if (and (nil? (authentication/extract-bearer-token request))
           (not (core/same-origin? request)))
    (do (log/warn {:id ::cross-origin-refused
                   :data {:action :refused :subject :account :uri (:uri request)
                          :origin (get-in request [:headers "origin"])}}
                  "Cross-origin account request refused")
        (json-response 403 {:error "cross_origin"}))
    (if-let [c (caller request)]
      (f c)
      (unauthorized request))))

(defn account-handler
  "GET /oauth/account: the caller's account as JSON, or the account page for a browser; `client_id` and `return` add the way back to that app."
  [request]
  (let [c     (caller request)
        q     (:params request)
        back  (back-params q)
        shown (fn [c] (cond-> (view c) back (assoc :back (back-link back))))]
    (cond
      (not (html? request))
      (if c (json-response 200 (shown c)) (unauthorized request))

      :else
      (or (custom/custom-page-redirect "account.html" (merge back (select-keys q [:done :error])))
          (if c
            {:status 200
             :headers {"Content-Type" "text/html; charset=utf-8" "Cache-Control" "no-store"}
             :body (str (page/render {:account (shown c)
                                      :carry back
                                      :providers (registry/list-providers)
                                      :params q
                                      :max-display-name max-display-name}))}
            {:status 303 :headers {"Location" (login/account-login-url back nil)}})))))

(defn clean-display-name
  "Display name as stored: whitespace collapsed and trimmed, control and format characters (bidi overrides) removed."
  [s]
  (-> (str s)
      (str/replace #"(?U)\s" " ")
      (str/replace #"[\p{Cc}\p{Cf}]" "")
      (str/replace #" {2,}" " ")
      str/trim))

(defn profile-handler
  "POST /oauth/account/profile {display_name}: the caller sets its own display name; blank goes back to the default."
  [request]
  (with-caller request
    (fn [{:keys [user]}]
      (let [params (body-params request)
            v      (clean-display-name (:display_name params))]
        (cond
          (not (contains? params :display_name))
          (respond request 400 {:error "invalid_request"} nil)

          (> (.codePointCount ^String v 0 (count v)) max-display-name)
          (respond request 422 {:error "display_name_too_long" :max_length max-display-name} nil)

          :else
          (do (dataset/stack-entity :iam/user {:xid (:xid user) :display_name v})
              (log/info {:id ::profile-changed
                         :user-xid (:xid user)
                         :data {:action :modified :subject :account-profile}}
                        "Display name changed")
              (respond request 200 {:profile (profile (account-user (:xid user)))} "profile")))))))

(defn password-handler
  "POST /oauth/account/password {current_password new_password}: the caller replaces its own password; every session ends, this one included."
  [request]
  (with-caller request
    (fn [{:keys [user]}]
      (let [{:keys [current_password new_password]} (body-params request)
            [status body] (credentials/change-password! user current_password new_password)]
        (respond request status body "password")))))

(defn reauth-handler
  "POST /oauth/account/reauth {password}: prove presence again, which opens the window for unlinking and signing out everywhere."
  [request]
  (with-caller request
    (fn [{:keys [user session]}]
      (let [{:keys [password]} (body-params request)]
        (cond
          (nil? session)
          (respond request 409 {:error "no_session"} nil)

          (str/blank? password)
          (respond request 400 {:error "invalid_request"} nil)

          (not= (:xid user) (:xid (core/validate-resource-owner (:name user) password)))
          (do (log/warn {:id ::reauth-rejected
                         :user-xid (:xid user)
                         :data {:action :credentials-rejected :subject :account}}
                        "Account step-up rejected")
              (respond request 403 {:error "invalid_credentials"} nil))

          :else
          (do (core/set-session-authorized-at session (java.util.Date.))
              (log/info {:id ::reauthenticated
                         :user-xid (:xid user)
                         :data {:action :authenticated :subject :account
                                :session (core/short-id session)}}
                        "Account step-up by password")
              (respond request 200 {:fresh true} "reauth")))))))

(defn unlink!
  "Remove sign-in method `xid` from caller `c`, which needs a fresh sign-in; returns [status body]."
  [c xid]
  (if (fresh? c)
    (federated/unlink-identity! (:user c) xid)
    [403 {:error "reauth_required"}]))

(defn unlink-handler
  "POST /oauth/account/identities/unlink {xid}: remove one of the caller's sign-in methods; needs a fresh sign-in."
  [request]
  (with-caller request
    (fn [c]
      (let [[status body] (unlink! c (:xid (body-params request)))]
        (respond request status body "unlinked")))))

(defn yes? [v] (contains? #{true "true" "on" "1"} v))

(defn revoke-handler
  "POST /oauth/account/sessions/revoke {xid} or {all_others true}: end one of the caller's sessions, or every one but this."
  [request]
  (with-caller request
    (fn [{:keys [user session] :as c}]
      (let [{:keys [xid all_others]} (body-params request)
            mine (owned-sessions (:xid user))]
        (cond
          (yes? all_others)
          (if-not (fresh? c)
            (respond request 403 {:error "reauth_required"} nil)
            (let [others (remove #(= session (:id %)) mine)]
              (doseq [{sid :id} others] (core/kill-session sid))
              (log/info {:id ::sessions-revoked
                         :user-xid (:xid user)
                         :data {:action :killed :subject :session :count (count others)}}
                        "Signed out everywhere else")
              (respond request 200 {:revoked (count others)} "signed_out_others")))

          (str/blank? xid)
          (respond request 400 {:error "missing_xid"} nil)

          :else
          (if-let [{sid :id} (some #(when (= xid (:xid %)) %) mine)]
            (let [own? (= sid session)]
              (core/kill-session sid)
              (log/info {:id ::session-revoked
                         :user-xid (:xid user)
                         :data {:action :killed :subject :session :session (core/short-id sid) :own own?}}
                        "Session ended by its owner")
              (cond-> (respond request 200 {:revoked 1} (if own? "signed_out" "revoked"))
                own? (assoc :cookies {(core/session-cookie-name request) {:value "" :path "/" :max-age 0}})))
            (respond request 404 {:error "session_not_found"} nil)))))))
