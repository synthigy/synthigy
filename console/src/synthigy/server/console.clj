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

(ns synthigy.server.console
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [patcho.lifecycle :as lifecycle]
   [ring.middleware.cookies :refer [wrap-cookies]]
   [ring.util.codec :as codec]
   [synthigy.dataset :as dataset]
   [synthigy.dataset.encryption :as denc]
   [synthigy.iam :as iam]
   [synthigy.iam.access :as access]
   [synthigy.info :as info]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.oauth.core :as oauth]
   [synthigy.oauth.federated :as federated]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.login :as oauth.login]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.live :as live]
   [synthigy.server.console.pages :as pages]
   [synthigy.server.console.pages.identities :as identities]
   [synthigy.server.console.pages.login :as login-page]
   [synthigy.server.console.pages.profile :as profile-page]
   [synthigy.server.console.pages.sessions :as sessions]
   [synthigy.server.console.pages.encryption :as encryption]
   [synthigy.server.console.pages.system :as system]
   [synthigy.server.console.pages.topology :as topology]
   [synthigy.server.console.session :as session]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.routes :as routes]))

(declare query-params)

(defn html-response [body]
  {:status 200 :headers {"Content-Type" "text/html; charset=utf-8"} :body body})

(defn form-params
  [request]
  (if-let [params (not-empty (:params request))]
    (update-keys params keyword)
    (let [body (some-> (:body request) slurp)]
      (when-not (str/blank? body)
        (update-keys (codec/form-decode body) keyword)))))

(defn signals
  [request]
  (when-let [ct (get-in request [:headers "content-type"])]
    (when (str/includes? ct "json")
      (try (some-> (:body request) slurp not-empty json/read-str)
           (catch Exception _ nil)))))

(defn reauthenticated?
  [request]
  (let [session (:console/session request)]
    (or (session/fresh? session)
        (let [pwd (some-> (or (::signals request) (signals request))
                          :confirmPassword not-empty)
              who (:resource-owner/name session)]
          ;; SYSTEM: the chain reads :iam/user's password hash — denied to an ordinary principal
          (boolean (and pwd who
                        (access/with-principal nil
                          (oauth/validate-resource-owner who pwd))))))))

(defn target-xid
  [request signal-map form-key]
  (or (some-> signal-map :confirmTarget not-empty)
      (get (form-params request) form-key)))

(def ^:private federated-login-errors
  {"not_linked" (str "That sign-in method isn't linked to any account here. "
                     "Sign in with your password, then link it under Sign-in methods.")
   "link_reauth_required" "Sign in again before changing your sign-in methods."
   "provider_unknown" "That sign-in method isn't configured."
   "provider_disabled" "That sign-in method is currently disabled."
   "provider_unsupported" "Could not start that sign-in method."
   "state_invalid" "That sign-in link has expired. Try again."
   "no_code" "That sign-in link is missing required information. Try again."
   "token_invalid" "Could not verify that sign-in. Try again."
   "callback_error" "Something went wrong signing in. Try again."})

(defn login-page [request]
  (if (session/current-session request)
    {:status 303 :headers {"Location" "/console"}}
    (let [{:strs [error logged_out]} (query-params request)]
      (html-response
       (login-page/render (or (federated-login-errors error)
                              (when error
                                "Could not sign in. Try again or use your password."))
                          (some? logged_out))))))

(defn login!
  [request]
  (cond
    (not (session/same-origin? request))
    {:status 403 :headers {"Content-Type" "text/plain"} :body "Cross-origin request rejected"}

    :else
    (let [{:keys [username password]} (form-params request)]
      (if-let [user (and (not (str/blank? username))
                         (not (str/blank? password))
                         (oauth/validate-resource-owner username password))]
        (let [sid (oauth/gen-session-id)]
          (oauth/create-session! sid (merge {:user user :flow "console"
                                             :authorized-at (java.util.Date.)
                                             :amr ["pwd"]}
                                            (oauth/client-info request)))
          (log/info {:id ::login :data {:action :authenticated :subject :console
                                        :user (:name user)}}
                    "Console login")
          {:status 303
           :headers {"Location" "/console"}
           :cookies (oauth.login/session-cookie sid)})
        (html-response (login-page/render "Invalid username or password."))))))

(defn logout!
  [request]
  (if-not (session/same-origin? request)
    {:status 403 :headers {"Content-Type" "text/plain"} :body "Cross-origin request rejected"}
    (do (some-> (session/current-session request) :id oauth/kill-session)
        {:status 303
         :headers {"Location" "/console/login?logged_out=1"}
         :cookies {"idsrv.session" {:value "" :path "/" :max-age 0}}})))

(defn kill-session!
  [request]
  (let [sig       (signals request)
        request   (assoc request ::signals sig)
        user-xid  (get-in request [:console/session :resource-owner])
        target    (target-xid request sig :session)
        datastar? (some? (get-in request [:headers "datastar-request"]))]
    (cond
      (not (reauthenticated? request))
      (if datastar?
        {:status 403}
        {:status 303 :headers {"Location" "/console/sessions"}})

      :else
      (do (if (= "all" target)
            (data/kill-other-sessions! user-xid (get-in request [:console/session :id]))
            (data/kill-my-session! user-xid target))
          (if datastar?
            {:status 204}
            {:status 303 :headers {"Location" "/console/sessions"}})))))

(defn system-action!
  "POST /console/system/<verb> — role-gated module lifecycle control; the
   handler is the guard, the page's buttons are only affordances."
  [request verb]
  (let [{:strs [module]} (query-params request)
        topic     (system/parse-module module)
        datastar? (some? (get-in request [:headers "datastar-request"]))
        act!      (fn [f]
                    (try (f)
                         (catch Throwable e
                           (log/error! {:id ::system-action-failed
                                        :msg (str "Console system action " verb " failed")
                                        :data {:action :failed :subject :module
                                               :module topic :verb verb
                                               :user (get-in request [:console/session :resource-owner/name])}}
                                       e)))
                    (system/bump!)
                    (if datastar?
                      {:status 204}
                      {:status 303 :headers {"Location" "/console/system"}}))]
    (cond
      (= "clear-errors" verb)
      (act! lifecycle/clear-errors!)

      (nil? topic) nil

      (= "start" verb)
      (act! #(lifecycle/start! topic)))))

(defn rotate-dek!
  "POST /console/encryption/rotate-dek — key ROLLOVER, Robert's call: cut a
   fresh active DEK (denc/create-dek); existing cells stay readable under
   their own DEKs and re-encrypt organically on write. The eager
   walk-every-cell denc/rotate-dek! stays a REPL/compromise-response tool.
   Step-up gated; every outcome lands back on the page as a visible notice."
  [request]
  (let [datastar? (some? (get-in request [:headers "datastar-request"]))
        land      (fn [q]
                    (let [uri (str "/console/encryption?" q)]
                      (if datastar?
                        (live/redirect-response request uri)
                        {:status 303 :headers {"Location" uri}})))]
    (if-not (reauthenticated? (assoc request ::signals (signals request)))
      (land "error=verify")
      (let [outcome (try (denc/create-dek)
                         (catch Throwable e
                           (log/error! {:id ::rotate-dek-failed
                                        :msg "Console key rollover failed"
                                        :data {:action :failed :subject :encryption
                                               :user (get-in request [:console/session :resource-owner/name])}}
                                       e)
                           ::failed))]
        (if (= ::failed outcome)
          (land "error=rotate")
          (land (str "rotated=" outcome)))))))

(def ^:private unlink-messages
  {"link_reauth_required" [:warn "Sign in again before changing your sign-in methods."]
   "last_login_method"    [:warn "That's your only way to sign in — link another method or set a password first."]
   "identity_not_found"   [:warn "That sign-in method is no longer linked."]
   "missing_xid"          [:warn "Nothing selected."]
   "link_requires_login"  [:warn "Please sign in again."]})

(defn mark-authenticated!
  "Stamp the session as freshly authenticated, after presence was proven."
  [request]
  (access/with-principal nil
    (oauth/set-session-authorized-at (get-in request [:console/session :id])
                                     (java.util.Date.))))

(defn unlink-identity!
  [request]
  ;; the body reads once — pull signals up front, both checks share the read
  (let [sig     (signals request)
        request (assoc request ::signals sig)
        target  (target-xid request sig :xid)]
    (if-not (reauthenticated? request)
      (html-response (identities/render request (unlink-messages "link_reauth_required")))
      (do
        (when-not (session/fresh? (:console/session request))
          (mark-authenticated! request))
        (let [{:keys [status body]} (access/with-principal nil
                                      (federated/unlink-handler
                                       (assoc-in request [:params :xid] target)))
              code   (when (string? body)
                       (try (some-> (json/read-str body) :error)
                            (catch Exception _ nil)))
              notice (cond
                       (= 200 status) [:ok "Sign-in method unlinked."]
                       code           (get unlink-messages code
                                           [:warn "Could not unlink that sign-in method."])
                       :else          [:warn "Could not unlink that sign-in method."])]
          (html-response (identities/render request notice)))))))

(defn link-identity!
  [request]
  (let [sig      (signals request)
        provider (some-> sig :linkOpen not-empty)
        pwd      (some-> sig :linkPassword not-empty)
        session  (:console/session request)
        who      (:resource-owner/name session)
        known?   (boolean (some #(= provider (:name %))
                                (access/with-principal nil (registry/list-providers))))]
    ;; NOT reauthenticated? — it passes on freshness alone, skipping the prompt
    (if (and known? pwd who
             (access/with-principal nil (oauth/validate-resource-owner who pwd)))
      (do
        (mark-authenticated! request)
        (log/info {:id ::link-verified :data {:action :authenticated :subject :console
                                              :provider provider :user who}}
                  "Step-up verified for federated link")
        (live/redirect-response
         request
         (str "/oauth/federated/start?"
              (codec/form-encode {:provider provider :mode "link"
                                  :return "/console/identities?linked=1"}))))
      (live/redirect-response request "/console/identities?error=verify_failed"))))

(defn set-password!
  [request]
  (let [sig      (signals request)
        session  (:console/session request)
        user-xid (:resource-owner session)
        who      (:resource-owner/name session)
        current  (some-> sig :pwCurrent not-empty)
        pw       (some-> sig :pwNew not-empty)
        confirm  (some-> sig :pwConfirm not-empty)
        has?     (data/has-password? user-xid)
        proven?  (if has?
                   (boolean (and current
                                 (access/with-principal nil
                                   (oauth/validate-resource-owner who current))))
                   (session/fresh? session))]
    (cond
      (or (nil? pw) (not= pw confirm))
      (live/redirect-response request "/console/identities?error=pw_mismatch")

      (< (count pw) 12)
      (live/redirect-response request "/console/identities?error=pw_short")

      (not proven?)
      (live/redirect-response request
                              (if has? "/console/identities?error=pw_wrong"
                                  "/console/identities?error=pw_stale"))

      :else
      (let [[status] (data/set-password! user-xid pw)]
        (if (= :ok status)
          (do (log/info {:id ::password-set
                         :data {:action :modified :subject :password :user who}}
                        "Console password set")
              (live/redirect-response request "/console/identities?pw=1"))
          (live/redirect-response request "/console/identities?error=pw_failed"))))))

(def ^:private link-errors
  {"verify_failed" [:warn "Could not verify that password."]
   "pw_mismatch"   [:warn "The two new passwords didn't match."]
   "pw_short"      [:warn "Use at least 12 characters."]
   "pw_wrong"      [:warn "That current password isn't right."]
   "pw_stale"      [:warn "Sign in again before setting a password."]
   "pw_failed"     [:warn "Could not set the password."]
   "link_session_mismatch"   [:warn (str "That didn't match the session it started from. "
                                         "Try connecting again.")]
   "link_requires_login"     [:warn "Sign in again, then connect the provider."]
   "reauth_identity_mismatch" [:warn (str "That provider account isn't linked to this "
                                          "user, so it can't confirm who you are.")]
   "link_reauth_required"    [:warn (str "Confirm it's you again before adding a "
                                         "sign-in method.")]
   "identity_already_linked" [:warn (str "That account is already linked to a different "
                                         "Synthigy user.")]
   "provider_unknown"        [:warn "That provider is no longer configured."]
   "provider_disabled"       [:warn "That provider is switched off."]
   "provider_unsupported"    [:warn "Could not reach that provider. Try again."]
   "token_invalid"           [:warn "The provider's response could not be verified."]
   "no_code"                 [:warn "The provider sent no authorization code."]
   "state_invalid"           [:warn "That link expired. Try connecting again."]
   "callback_error"          [:warn "Something went wrong connecting that provider."]})

(def ^:private link-cancelled
  "IdP error codes that mean the person backed out, not that anything broke."
  #{"idp_access_denied" "idp_consent_required" "idp_user_cancelled_authorize"
    "idp_login_required" "idp_interaction_required"})

(defn link-notice
  "Error code from a federated link round-trip -> notice, or nil."
  [code]
  (when code
    (or (link-errors code)
        (when (contains? link-cancelled code)
          [:warn "Connecting was cancelled — nothing changed."])
        (when (str/starts-with? code "idp_")
          [:warn (str "The provider refused: " (str/replace (subs code 4) "_" " ") ".")])
        [:warn "Could not connect that sign-in method."])))

(defn identities-page
  [request]
  (let [q (query-params request)]
    (html-response
     (identities/render request (or (link-notice (get q "error"))
                                    (when (get q "linked")
                                      [:ok "Sign-in method linked."])
                                    (when (get q "pw")
                                      [:ok "Password updated."]))))))

(def ^:private asset-types
  {"js" "application/javascript"
   "css" "text/css"
   "woff2" "font/woff2"
   "svg" "image/svg+xml"
   "png" "image/png"})

(defn asset
  [request]
  (let [[_ kind file] (re-matches #"/console/assets/(js|css|fonts|img)/([^/]+)"
                                  (:uri request))
        ctype (get asset-types (last (str/split (or file "") #"\.")))]
    (when (and kind file ctype)
      (when-let [r (io/resource (str "synthigy/console/" kind "/" file))]
        {:status 200
         :headers {"Content-Type" ctype
                   "Cache-Control" "max-age=86400"}
         :body (io/input-stream r)}))))

(defn detail-page
  [request spec xid]
  (when-let [row (data/detail-row spec xid)]
    (html-response (ui/detail request spec row))))

(def ^:private save-notices
  {:ok     [:ok "Saved."]
   :denied [:warn "You don't have permission to change this."]})

(defn card-values
  "What the add/edit card currently holds, straight off the posted form."
  [k fields raw]
  (into {}
        (map (fn [[fk]]
               [fk (get raw (keyword (str (name k) "_new_" (name fk))))]))
        fields))

(defn panel-link
  "The owned link named by `link-key`, from `:detail :links` or `:create :owned`."
  [spec link-key]
  (some (fn [[k :as l]] (when (= link-key (name k)) l))
        (concat (data/owned-links (get-in spec [:detail :links]))
                (get-in spec [:create :owned]))))

(defn panel-action!
  "POST /console/iam/<slug>/panel/<link> — the one pure handler behind Add, Edit, Delete and Cancel."
  [request spec link-key]
  (when-let [[k _ _ _ _ show :as link] (panel-link spec link-key)]
    (let [fields  (get-in show [:create :fields])
          raw     (form-params request)
          {:strs [add target cancel] drop-q "drop"} (query-params request)
          rows    (data/panel-rows k fields raw)
          deleted (data/panel-deleted k raw)
          idx     (some-> (get raw (keyword (str (name k) "_editing")))
                          str not-empty parse-long)
          card    (card-values k fields raw)
          nm      (str/trim (str (:name card)))
          drop-i  (some-> drop-q not-empty parse-long)
          targ    (some-> target not-empty parse-long)
          state
          (cond
            drop-i
            {:rows (into (subvec rows 0 (min drop-i (count rows)))
                         (subvec rows (min (inc drop-i) (count rows))))
             :deleted (into deleted
                            (remove str/blank?)
                            [(str (:xid (nth rows drop-i nil)))])}

            add
            (let [dup? (some (fn [[i r]]
                               (and (not= i idx) (= nm (str/trim (str (:name r))))))
                             (map-indexed vector rows))]
              (cond
                (str/blank? nm)
                {:rows rows :editing idx :values card :notice [:warn "Name is required."]}

                dup?
                {:rows rows :editing idx :values card
                 :notice [:warn (str "\"" nm "\" is already in the list.")]}

                idx   {:rows (assoc rows idx (merge (nth rows idx) card))}
                :else {:rows (conj rows card)}))

            (and targ (not cancel))
            {:rows rows :editing targ :values (nth rows targ nil)}

            :else {:rows rows})]
      (live/link-panel-response request spec link (:rows state)
                               (update state :deleted #(or % deleted))))))

(defn run-action!
  [request spec xid verb]
  (when-let [{:keys [reauth? run]} (get-in spec [:actions verb])]
    (when-let [row (data/detail-row spec xid)]
      (let [sig     (signals request)
        request (assoc request ::signals sig)
        params  (or (form-params request) {})]
        (if (and reauth? (not (reauthenticated? request)))
          (html-response
           (ui/detail request spec row
                      [:warn "Please confirm your password and try again."]))
          (let [{:keys [notice panel-notice]} (run spec row params)
                row (or (data/detail-row spec xid) row)]
            (html-response
             (ui/detail request spec row notice panel-notice))))))))

(defn owned-params
  "Forward the owned panels' own form state (`<k>__*`) through untouched — omitting it silently orphans children."
  [links raw]
  (let [prefixes (mapv #(str (name (first %)) "__") (data/owned-links links))]
    (into {}
          (filter (fn [[k]] (some #(str/starts-with? (name k) %) prefixes)))
          raw)))

(defn delete-row!
  "POST /console/iam/<slug>/<xid>/delete — step-up gated, refused by the spec's `:delete :guard` if set."
  [request spec xid]
  (when-let [{:keys [guard]} (:delete spec)]
    (let [sig     (signals request)
          request (assoc request ::signals sig)]
      (cond
        (not (reauthenticated? request))
        (when-let [row (data/detail-row spec xid)]
          (html-response
           (ui/detail request spec row
                      [:warn "Please confirm your password and try again."])))

        :else
        (let [row     (data/detail-row spec xid)
              locked  (get-in spec [:detail :locked])
              blocked (when row
                        (cond
                          (and locked (locked row)) "This record is managed elsewhere and can't be deleted here."
                          guard (guard row (:console/principal request))))]
          (if blocked
            (html-response (ui/detail request spec row [:warn blocked]))
            (let [[status msg] (data/delete-row! spec xid)]
              (if (= :ok status)
                (live/redirect-response request (str "/console/iam/" (:slug spec)))
                (when-let [row (data/detail-row spec xid)]
                  (html-response
                   (ui/detail request spec row
                              (if (= :denied status)
                                [:warn "You don't have permission to delete this."]
                                [:warn (str msg)]))))))))))))

(defn field-params
  "Scalar fields + multi-valued link grants, straight off the posted form."
  [{:keys [fields links]} raw]
  (into (into {} (map (fn [[k]] [k (get raw k)])) fields)
        (map (fn [[k]] [k (data/listy (get raw k))]))
        links))

(defn section-params
  "`settings__*`/`config__*` params, forwarded UNTRANSFORMED for the sections this spec declares."
  [detail raw]
  (into {}
        (filter (fn [[k]]
                  (let [n (name k)]
                    (or (and (:settings detail) (str/starts-with? n "settings__"))
                        (and (:config detail) (str/starts-with? n "config__"))))))
        raw))

(defn save-detail!
  [request spec xid]
  (let [detail (:detail spec)
        raw    (form-params request)
        params (merge (field-params detail raw)
                      (section-params detail raw)
                      (owned-params (:links detail) raw))
        [status row-or-msg typed] (data/save! spec xid params)
        ;; A refusal re-renders what was POSTED over the stored row — re-reading
        ;; alone would revert every edit while claiming to preserve it. Scalars
        ;; only: settings/config are rebuilt by a fn that throws on the bad
        ;; value, so those fields still fall back to what is stored.
        row    (if (= :ok status)
                 row-or-msg
                 (merge (data/detail-row spec xid) typed))]
    (cond
      ;; Success leaves nothing to keep on screen, so land where the change is
      ;; visible — the index. The verdict rides the URL because a 303 cannot
      ;; carry an inline notice.
      (= :ok status)
      {:status 303 :headers {"Location" (str "/console/iam/" (:slug spec) "?saved=1")}}

      ;; A refusal keeps the edited values on screen rather than throwing the
      ;; work away — the original reason this re-rendered at all.
      row
      (html-response
       (ui/detail request spec row
                  (or (save-notices status) [:warn (str row-or-msg)])))

      :else
      {:status 303 :headers {"Location" (str "/console/iam/" (:slug spec))}})))

(defn save-profile!
  [request]
  (let [user-xid    (get-in request [:console/session :resource-owner])
        raw         (form-params request)
        [status msg] (data/save-profile! user-xid raw)]
    (html-response
     (profile-page/render request (or (save-notices status) [:warn (str msg)])))))

(defn create-params
  [spec raw]
  (let [create (:create spec)]
    (merge (field-params create raw)
           (section-params (:detail spec) raw)
           (owned-params (:owned create) raw))))

(defn wizard-values
  "The wizard's chosen discriminator, off the query string — only ever a value
   the spec itself declares a config layout for."
  [spec request]
  (when-let [by (and (get-in spec [:create :wizard]) (get-in spec [:detail :config :by]))]
    (let [v (get (query-params request) (name by))]
      (when (contains? (get-in spec [:detail :config :layouts]) v)
        {by v}))))

(defn create-row!
  [request spec]
  (let [raw    (form-params request)
        params (create-params spec raw)
        [status row-or-msg] (data/create-row! spec params)]
    (if (= :ok status)
      (if-let [after (get-in spec [:create :after])]
        (html-response
         (ui/detail request spec (data/detail-row spec (:xid row-or-msg))
                    (after spec row-or-msg)))
        {:status 303
         :headers {"Location" (str "/console/iam/" (:slug spec) "/" (:xid row-or-msg))}})
      (html-response
       (ui/create-page request spec
                       (if (= :denied status)
                         [:warn "You don't have permission to create this."]
                         [:warn (str row-or-msg)])
                       params)))))

(def ^:private transfer-type-set
  (into #{} (map :type) data/transfer-types))

(defn export-filename
  [t records]
  (if-let [nm (and (= 1 (count records)) (not-empty (:name (first records))))]
    (str (name t) "_"
         (-> nm str/lower-case
             (str/replace #"[^a-z0-9]+" "_")
             (str/replace #"^_+|_+$" ""))
         ".json")
    (str (name t) "s.json")))

(defn requested-filename
  [n]
  (when-not (str/blank? n)
    (let [clean (-> n str/trim
                    (str/replace #"[^A-Za-z0-9._-]+" "_")
                    (str/replace #"(?i)\.json$" "")
                    (str/replace #"^[._]+|[._]+$" ""))]
      (when-not (str/blank? clean)
        (str clean ".json")))))

(defn transfer-export
  [request]
  (let [{:strs [type ids name]} (query-params request)
        t   (some-> type keyword transfer-type-set)
        ids (when-not (str/blank? ids)
              (vec (remove str/blank? (str/split ids #","))))]
    (when (and t (data/transfer-exportable? t))
      (let [records (data/transfer-export t ids)]
        {:status 200
         :headers {"Content-Type" "application/json"
                   "Content-Disposition" (str "attachment; filename=\""
                                              (or (requested-filename name)
                                                  (export-filename t records)) "\"")}
         :body (json/write-str records)}))))

(defn render-missing [missing]
  (str/join "; "
            (for [[rel ids] missing]
              (str (name rel) ": " (str/join ", " (sort ids))))))

(defn import-action!
  [request spec]
  (when-let [t (:type (data/transfer-by-slug (:slug spec)))]
    (let [{:keys [action mode payload confirmPassword]} (form-params request)
          m      (if (= mode "stack") :stack :sync)
          page   (fn [notice]
                   (html-response
                    (ui/browse request spec {:notice notice :mode m :payload payload})))
          parsed (when-not (str/blank? payload)
                   (try (json/read-str payload) (catch Exception _ ::bad-json)))]
      (cond
        (not (data/transfer-importable? t))
        (page [:warn "You don't have permission to import here."])

        (nil? parsed)
        (page [:warn "Paste an export payload first."])

        (= ::bad-json parsed)
        (page [:warn "That payload isn't valid JSON."])

        (= action "validate")
        (let [[status result] (data/transfer-validate t parsed)]
          (page
           (case status
             :ok (let [{:keys [records missing]} result]
                   (if (seq missing)
                     [:warn (str records " record" (when (not= 1 records) "s")
                                 ", missing references — " (render-missing missing))]
                     [:ok (str records " record" (when (not= 1 records) "s")
                               ", every reference resolves. Safe to import.")]))
             [:warn (str result)])))

        (= action "import")
        (let [session (:console/session request)
              fresh?  (or (session/fresh? session)
                          (boolean
                           (and (not (str/blank? confirmPassword))
                                (oauth/validate-resource-owner
                                 (:resource-owner/name session) confirmPassword))))]
          (if-not fresh?
            (page [:warn "Please confirm your password and try again."])
            (let [[status result] (data/transfer-import! t parsed m)]
              (page
               (case status
                 :ok      [:ok (str "Imported " result " record"
                                    (when (not= 1 result) "s") ".")]
                 :missing [:warn (str "Nothing written — missing references: "
                                      (render-missing result))]
                 [:warn (str result)])))))

        :else
        (page nil)))))

(defn under
  [uri prefix]
  (when (str/starts-with? uri prefix)
    (let [segs (remove str/blank? (str/split (subs uri (count prefix)) #"/"))]
      (when (<= 1 (count segs) 4) (vec segs)))))

(defn query-params
  [request]
  (let [decoded (some-> (:query-string request) codec/form-decode)]
    (if (map? decoded) decoded {})))

(defn filter-params
  [spec params]
  (into {}
        (keep (fn [[k]]
                (when-let [xids (seq (remove str/blank?
                                             (str/split (str (get params (name k))) #",")))]
                  [k (vec xids)])))
        (:filters spec)))

(def ^:private authed
  (session/wrap-session
   (fn [request]
     (let [uri (:uri request)]
       (case uri
         ("/console" "/console/")  {:status 303 :headers {"Location" "/console/sessions"}}
         "/console/sessions"       (html-response (sessions/render request))
         "/console/sessions/kill"  (when (= :post (:request-method request))
                                     (kill-session! request))
         "/console/identities"     (identities-page request)
         "/console/identities/unlink" (when (= :post (:request-method request))
                                        (unlink-identity! request))
         "/console/identities/link" (when (= :post (:request-method request))
                                      (link-identity! request))
         "/console/identities/password" (when (= :post (:request-method request))
                                          (set-password! request))
         "/console/profile"        (case (:request-method request)
                                     :get  (html-response (profile-page/render request))
                                     :post (save-profile! request)
                                     nil)
         "/console/live/sessions"  (live/sessions-stream request)
         "/console/live/identities" (live/identities-stream request)
         "/console/system"         (when (data/system-operator?)
                                     (html-response (system/render request)))
         "/console/system/topology" (when (data/system-operator?)
                                      (html-response (topology/render request)))
         "/console/live/system"    (when (data/system-operator?)
                                     (live/system-stream request))
         ("/console/system/start" "/console/system/clear-errors")
         (when (and (= :post (:request-method request))
                    (data/system-operator?))
           (system-action! request (subs uri (count "/console/system/"))))
         "/console/encryption"
         (when (data/system-operator?)
           (let [{:strs [rotated error]} (query-params request)]
             (html-response
              (encryption/render
               request
               (cond
                 rotated
                 [:ok (str "New encryption key active (DEK " rotated "). Existing "
                           "values stay readable under their previous keys and "
                           "re-encrypt as they are written.")]

                 (= error "verify")
                 [:warn "Could not verify that password — nothing was rotated."]

                 (= error "rotate")
                 [:warn "Key rollover failed — nothing was changed. Details are in the system log."])))))
         "/console/encryption/rotate-dek"
         (when (and (= :post (:request-method request))
                    (data/system-operator?))
           (rotate-dek! request))
         "/console/transfer/export" (when (= :get (:request-method request))
                                      (transfer-export request))
         (or (when-let [[slug tail link-key action] (under uri "/console/iam/")]
               (when-let [spec (pages/by-slug slug)]
                 (cond
                   (nil? tail)
                   (html-response
                    (ui/browse request spec nil
                               (when (get (query-params request) "saved")
                                 [:ok "Saved."])))

                   (= tail "rows")
                   (let [{:strs [offset q] sort-k "sort" dir "dir" :as qp} (query-params request)]
                     (live/browse-rows-response
                      request spec (max 0 (or (parse-long (str offset)) 0)) q
                      (filter-params spec qp) sort-k dir))

                   (= tail "options")
                   (let [{:strs [link q sel offset]} (query-params request)]
                     (when (and (:detail spec) (string? link))
                       (live/link-options-response
                        request spec link q sel
                        (max 0 (or (parse-long (str offset)) 0)))))

                   (= tail "filter-options")
                   (let [{:strs [link q sel offset]} (query-params request)]
                     (when (and (:filters spec) (string? link))
                       (live/filter-options-response
                        request spec link q sel
                        (max 0 (or (parse-long (str offset)) 0)))))

                   (and (= tail "import") (= :post (:request-method request)))
                   (import-action! request spec)

                   (and (= tail "panel") link-key (= :post (:request-method request)))
                   (panel-action! request spec link-key)

                   (and (= tail "new") (nil? link-key))
                   (when (:create spec)
                     (case (:request-method request)
                       ;; A wizard has no chooser step — the index's cards ARE
                       ;; the chooser — so arriving without a valid discriminator
                       ;; means there is nothing to render. Send them to pick.
                       :get  (let [values (wizard-values spec request)]
                               (if (and (get-in spec [:create :wizard]) (nil? values))
                                 {:status 303
                                  :headers {"Location" (str "/console/iam/" (:slug spec))}}
                                 (html-response (ui/create-page request spec nil values))))
                       :post (create-row! request spec)
                       nil))


                   (and (= link-key "delete") (nil? action)
                        (= :post (:request-method request)))
                   (when (:detail spec) (delete-row! request spec tail))

                   (and link-key
                        (= :post (:request-method request))
                        (get-in spec [:actions (cond-> link-key
                                                 action (str "/" action))]))
                   (run-action! request spec tail
                                (cond-> link-key action (str "/" action)))

                   ;; `tail` is a row xid ONLY when nothing follows it — an
                   ;; unrecognized sub-path must 404, never fall through to a
                   ;; WRITE on that row. A form POST here carries no
                   ;; `<link>__*` params, and `sync` reads a missing owned
                   ;; relation as "unlink everything", so this branch used to
                   ;; orphan every owned child of the record (200, silently).
                   (nil? link-key)
                   (when (:detail spec)
                     (case (:request-method request)
                       :get  (detail-page request spec tail)
                       :post (save-detail! request spec tail)
                       nil)))))
             (when-let [[slug tail] (under uri "/console/live/iam/")]
               (when-let [spec (pages/by-slug slug)]
                 (cond
                   (= tail "rows")
                   (let [{:strs [limit q] sort-k "sort" dir "dir" :as qp} (query-params request)]
                     (live/browse-range-stream
                      request spec (max 0 (or (parse-long (str limit)) 0)) q
                      (filter-params spec qp) sort-k dir)))))))))))

(defn route
  [request]
  (let [{:keys [uri request-method]} request]
    (cond
      (str/starts-with? uri "/console/assets/") (when (= :get request-method) (asset request))
      (= uri "/console/login")  (case request-method
                                  :get (login-page request)
                                  :post (login! request)
                                  nil)
      (= uri "/console/logout") (when (= :post request-method) (logout! request))
      ;; Unauthenticated on purpose: it carries no data, and a silent-renew
      ;; iframe must reach it even when the console session has lapsed.
      (= uri "/console/tools/callback") (when (and (= :get request-method)
                                                   (ui/tools-available?))
                                          (html-response (ui/tools-callback-page)))
      :else (authed request))))

(defn handler
  [request]
  (try
    (route request)
    (catch Throwable e
      (let [{:keys [code entity-name]} (ex-data e)]
        (log/error! {:id ::request-failed
                     :msg "Console request failed"
                     :data {:action :failed :subject :console
                            :uri (:uri request) :method (:request-method request)
                            :code code :entity entity-name
                            :user (get-in request [:console/session :resource-owner/name])}}
                    e)
        {:status 500
         :headers {"Content-Type" "text/html; charset=utf-8"}
         :body (ui/error-page (if (= "ENTITY_FORBIDDEN" code)
                                "You don't have access to that."
                                "Something went wrong."))}))))

(def wrapped-handler (wrap-cookies #'handler))

(defn sync-tools-redirect
  "Allow-list with `uri` as the only console tools entry, keeping every other entry."
  [urls uri]
  (conj (vec (remove #(or (str/ends-with? % "/console/tools/callback")
                          (str/includes? % "/console/login"))
                     urls))
        uri))

(defn register-tools-redirect!
  "Registers this deployment's tools callback and logout landing with the Synthigy Tools client."
  []
  (when-let [uri (and (ui/tools-available?) (ui/tools-redirect-uri))]
    (try
      (when-let [client (iam/get-client info/modeler-public-client-id)]
        (let [settings (or (:settings client) {})
              updated (-> settings
                          (update "redirections" sync-tools-redirect uri)
                          (update "logout-redirections" sync-tools-redirect (ui/tools-logout-uri)))]
          (when (not= settings updated)
            (dataset/stack-entity :iam/app {:id info/modeler-public-client-id :settings updated})
            (log/info {:id ::tools-redirect-registered :data {:uri uri}}
                      "Registered console tools callback"))))
      (catch Throwable e
        (log/error! {:id ::tools-redirect-failed :msg "Registering console tools callback failed"
                     :data {:uri uri}}
                    e)))))

(lifecycle/register-module!
 :synthigy/console
 {:depends-on [:synthigy/iam]
  :doc "Server-rendered admin + self-service console at /console. Opt-in;
        requires IAM (it authenticates users) — break-glass stays with
        __admin and the CLI."
  :start (fn []
           (system/watch-lifecycle!)
           (register-tools-redirect!)
           (routes/register-prefix! ::console "/console" #'wrapped-handler)
           (log/info {:id ::started :data {:action :started :subject :console}}
                     "Console mounted at /console"))
  :stop (fn []
          (routes/unregister-prefix! ::console)
          (system/unwatch-lifecycle!)
          (log/info {:id ::stopped :data {:action :stopped :subject :console}}
                    "Console unmounted"))})
