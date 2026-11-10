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

(ns synthigy.oauth.page.account
  "The built-in account page: the signed-in user's profile, password, sign-in methods and sessions."
  (:require
   [clojure.string :as str]
   [ring.util.codec :as codec]
   [synthigy.oauth.credentials :as credentials]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.oauth.page.login :as page.login])
  (:import
   [java.time Instant OffsetDateTime ZoneOffset]
   [java.time.format DateTimeFormatter]))

(def stamp (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm 'UTC'"))

(defn ts
  [d]
  (when-let [^Instant i (cond
                          (instance? java.util.Date d) (.toInstant ^java.util.Date d)
                          (instance? Instant d) d
                          (instance? OffsetDateTime d) (.toInstant ^OffsetDateTime d))]
    (.format ^DateTimeFormatter stamp (.atOffset i ZoneOffset/UTC))))

(def ua-browsers
  [[#"Edg[e/]" "Edge"] [#"OPR/|Opera" "Opera"] [#"Chrome/" "Chrome"]
   [#"Firefox/" "Firefox"] [#"Safari/" "Safari"] [#"curl/" "curl"]])

(def ua-systems
  [[#"Windows" "Windows"] [#"iPhone|iPad" "iOS"] [#"Macintosh|Mac OS X" "macOS"]
   [#"Android" "Android"] [#"Linux" "Linux"]])

(defn device-label
  "User agent as \"Chrome on macOS\"; an unknown one keeps a short raw string."
  [ua]
  (let [pick (fn [table] (some (fn [[re label]] (when (re-find re ua) label)) table))]
    (if (str/blank? ua)
      "Unknown device"
      (let [browser (pick ua-browsers)
            system  (pick ua-systems)]
        (cond
          (and browser system) (str browser " on " system)
          browser              browser
          system               system
          :else                (subs ua 0 (min 40 (count ua))))))))

(def flow-labels
  {"authorization_code" "App sign-in"
   "device_code"        "Device sign-in"
   "account"            "This account page"
   "console"            "Console"})

(def done-text
  {"profile"           "Display name saved."
   "reauth"            "Confirmed. For the next few minutes you can change sign-in methods and sign out everywhere."
   "linked"            "Sign-in method linked."
   "unlinked"          "Sign-in method removed."
   "revoked"           "Session ended."
   "signed_out_others" "Signed out everywhere else."})

(def error-text
  {"reauth_required"          "Confirm it's you first."
   "link_reauth_required"     "Confirm it's you first."
   "invalid_credentials"      "Wrong password."
   "invalid_request"          "Fill in every field."
   "password_too_short"       (str "Use at least " credentials/min-password-length " characters.")
   "display_name_too_long"    "That display name is too long."
   "no_local_credential"      "This account has no password to change."
   "last_login_method"        "That is your only way to sign in. Link another method first."
   "identity_not_found"       "That sign-in method is no longer linked."
   "identity_already_linked"  "That account is already linked to someone else here."
   "reauth_identity_mismatch" "That account isn't the one linked here."
   "session_not_found"        "That session has already ended."
   "link_session_mismatch"    "That started in another session. Try again."
   "link_requires_login"      "Sign in again."
   "no_session"               "Sign in on this device first."
   "missing_xid"              "Nothing selected."})

(defn notice
  [{:keys [done error]}]
  (cond
    error [(if (#{"reauth_required" "link_reauth_required"} error) :warn :error)
           (or (error-text error)
               (page.login/federated-errors error)
               "Something went wrong. Try again.")]
    done  (when-let [t (done-text done)] [:success t])))

(def ^:dynamic *carry*
  "The `client_id` and `return` of the app that sent the user here, kept on every link and form."
  nil)

(defn at
  [path & [query]]
  (let [q (merge *carry* query)]
    (cond-> path (seq q) (str "?" (codec/form-encode q)))))

(defn start-url
  [provider mode return]
  (str "/oauth/federated/start?" (codec/form-encode {:provider provider :mode mode :return return})))

(defn post-button
  "A one-button form posting `fields` to `action`."
  [action fields label & [primary?]]
  [:form.sy-inline {:method "post" :action (at action) :data-sy-busy ""}
   (for [[k v] fields] [:input {:type "hidden" :name (name k) :value (str v)}])
   [:button {:type "submit" :class (str "sy-btn sy-btn--sm " (if primary? "sy-btn--primary" "sy-btn--secondary"))}
    [:span.sy-btn-label label]
    [:span.sy-spin {:aria-hidden "true"}]]])

(defn action-button
  [label]
  [:button.sy-btn.sy-btn--secondary.sy-btn--sm {:type "submit"}
   [:span.sy-btn-label label]
   [:span.sy-spin {:aria-hidden "true"}]])

(defn section
  [title sub & body]
  [:section.sy-section
   [:h2.sy-section-title title]
   (when sub [:p.sy-section-sub sub])
   (into [:div.sy-section-body] body)])

(defn row
  [title sub & actions]
  [:div.sy-row
   [:div.sy-row-main
    [:div.sy-row-title title]
    (when sub [:div.sy-row-sub sub])]
   (into [:div.sy-row-actions] actions)])

(defn profile-section
  [{:keys [display_name]} max-display-name]
  (section "Display name" "What others see next to your work. Leave it empty to use your sign-in name."
           [:form.sy-form {:method "post" :action (at "/oauth/account/profile") :data-sy-busy ""}
            (page.login/text-field {:id "display_name" :name "display_name" :label "Display name"
                                    :icon :user-round :value (or display_name "")
                                    :maxlength (str max-display-name)
                                    :autocomplete "nickname" :spellcheck "false"})
            (action-button "Save")]))

(defn password-section
  []
  (section "Password" "Changing it signs you out everywhere, this device too."
           [:form.sy-form {:method "post" :action (at "/oauth/account/password") :data-sy-busy ""}
            (page.login/password-field {:id "current_password" :name "current_password"
                                        :label "Current password" :icon :lock
                                        :autocomplete "current-password" :spellcheck "false"})
            (page.login/password-field {:id "new_password" :name "new_password"
                                        :label "New password" :icon :lock
                                        :minlength (str credentials/min-password-length)
                                        :autocomplete "new-password" :spellcheck "false"})
            (action-button "Change password")]))

(defn confirm-section
  [{:keys [has_password]} linked]
  (section "Confirm it's you" "Changing sign-in methods or signing out everywhere needs a recent sign-in."
           (when (or has_password (empty? linked))
             [:form.sy-form {:method "post" :action (at "/oauth/account/reauth") :data-sy-busy ""}
              (page.login/password-field {:id "reauth_password" :name "password" :label "Password"
                                          :icon :lock :autocomplete "current-password"
                                          :spellcheck "false"})
              (action-button "Confirm")])
           (when (seq linked)
             (list
              (when (or has_password (empty? linked)) [:div.sy-divider "or"])
              (page.login/provider-buttons
               linked
               (fn [{:keys [name]}] (start-url name "reauth" (at "/oauth/account" {:done "reauth"}))))))))

(defn identities-section
  [identities providers fresh?]
  (let [labels (into {} (map (juxt :provider :label)) providers)
        linked (into #{} (map (comp registry/provider-keyword :provider)) identities)
        open   (remove (comp linked :provider) providers)]
    (section "Sign-in methods" (when-not fresh? "Confirm it's you to add or remove one.")
             (if (empty? identities)
               [:p.sy-row-sub "You sign in with your password."]
               [:div.sy-rows
                (for [{:keys [xid provider email linked_at]} identities
                      :let [k (registry/provider-keyword provider)]]
                  (row (or (labels k) (str/capitalize (name k)))
                       (str/join " · " (remove str/blank? [email (some->> (ts linked_at) (str "linked "))]))
                       (when fresh? (post-button "/oauth/account/identities/unlink" {:xid xid} "Unlink"))))])
             (when (and fresh? (seq open))
               [:div.sy-stack.sy-section-more
                (for [{:keys [name label] :as p} open]
                  [:a.sy-btn.sy-btn--secondary {:href (start-url name "link" (at "/oauth/account" {:done "linked"}))}
                   (assets/provider-mark p)
                   [:span.sy-btn-label "Link " label]])]))))

(defn sessions-section
  [sessions fresh?]
  (section "Where you're signed in" "Every browser, app and device signed in to this account."
           [:div.sy-rows
            (for [{:keys [xid client flow ip agent started last_seen current]} sessions]
              (row (list (device-label agent)
                         (when current [:span.sy-pill "This device"]))
                   (str/join " · " (remove str/blank? [(or client (flow-labels flow))
                                                       ip
                                                       (some->> (ts (or last_seen started)) (str "active "))]))
                   (post-button "/oauth/account/sessions/revoke" {:xid xid} "Sign out")))]
           (when (< 1 (count sessions))
             (if fresh?
               [:div.sy-section-more
                (post-button "/oauth/account/sessions/revoke" {:all_others true} "Sign out everywhere else")]
               [:p.sy-row-sub.sy-section-more "Confirm it's you to sign out everywhere else at once."]))))

(defn render
  "The account page for `account` (as `GET /oauth/account` answers it), with the outcome of the last action from `params`."
  [{:keys [account carry providers params max-display-name]}]
  (binding [*carry* carry]
    (let [{:keys [profile identities sessions fresh back]} account
          {:keys [login display_name has_password]} profile
          linked-kws (into #{} (map (comp registry/provider-keyword :provider)) identities)
          linked     (filter (comp linked-kws :provider) providers)
          [tone text] (notice params)]
      (assets/document
       {:title "Your account · Synthigy"
        :css "account.css"
        :chip "Your account"
        :status [:span.sy-status-item (assets/icon :user-round) [:span login]]}
       [:div.sy-card.sy-card--account
        [:div.sy-card-head
         [:h1.sy-card-title (or (not-empty display_name) login)]
         (assets/voice {:id "voice" :tone tone} (list "Signed in as " [:b login]) text)]
        [:div.sy-card-body
         (profile-section profile max-display-name)
         (when has_password (password-section))
         (when-not fresh (confirm-section profile linked))
         (identities-section identities providers fresh)
         (sessions-section sessions fresh)]
        (when back
          [:div.sy-card-foot
           [:a.sy-link {:href (:uri back)} (assets/icon :arrow-left) [:span "Back to " (:name back)]]])]))))
