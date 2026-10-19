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

(ns synthigy.server.console.pages.identities
  (:require
   [ring.util.codec :as codec]
   [synthigy.json :as json]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.server.routes :as routes]))

(defn start-uri
  [provider-name mode & [extra]]
  (str "/oauth/federated/start?"
       (codec/form-encode (merge {:provider provider-name :mode mode} extra))))

(defn connect-buttons
  [available]
  (when (seq available)
    [:div.console-connect
     [:div.console-settings-head "Add a sign-in method"]
     [:p.console-field-hint
      "You'll be asked to confirm it's you before the provider is linked."]
     [:div.console-federated.console-federated-row
      (for [{:keys [name label] :as p} available]
        [:ty-button {:type "button" :appearance "outlined"
                     "data-on:click" (str "$linkOpen = " (json/write-str name))}
         [:span.console-fed-mark {:slot "start"} (assets/provider-mark p)]
         (str "Connect " label)])]]))

(defn verify-links
  "Re-auth through an already-linked provider — for accounts with no password."
  [linked]
  (when (seq linked)
    (list
     [:div.console-fed-divider [:span "or"]]
     [:div.console-federated
      (for [{:keys [name label] :as p} linked]
        [:a.console-fed-btn
         {(keyword "data-attr:href")
          (str (json/write-str (str (start-uri name "reauth") "&return="))
               " + encodeURIComponent('/oauth/federated/start?mode=link&provider=' + $linkOpen"
               " + '&return=" (codec/url-encode "/console/identities?linked=1") "')")}
         [:span.console-fed-mark (assets/provider-mark p)]
         [:span "Verify with " label]])])))

(defn connect-modal
  [linked password?]
  [:ty-modal {(keyword "data-attr:open") "$linkOpen !== ''"
              :backdrop "true" :close-on-outside-click true :close-on-escape true
              (keyword "data-on:close") "$linkOpen = ''"}
   [:div.console-confirm
    [:h2 "Confirm it's you"]
    [:p "Adding a way to sign in is security-sensitive, so it needs proof "
     "it's really you — not just an open session."]
    (when (and (not password?) (seq linked))
      [:p.console-field-hint
       "This account has no password set. Verify through a provider you've "
       "already linked."])
    [:div.console-modal-field
     [:ty-input (merge {:type "password" :label "Password" :size "sm"
                        :autocomplete "current-password"
                        :data-bind "linkPassword"}
                       (ui/on-enter "@post('/console/identities/link')"))]]
    (verify-links linked)
    [:div.console-confirm-actions
     [:ty-button {:type "button" :size "sm" :appearance "ghost"
                  "data-on:click" "$linkOpen = ''"}
      "Cancel"]
     [:ty-button {:type "button" :size "sm" :flavor "primary"
                  "data-on:click" "@post('/console/identities/link')"}
      "Confirm"]]]])

(defn password-section
  [password?]
  [:div.console-connect
   [:div.console-settings-head "Password"]
   [:p.console-field-hint
    (if password?
      "You can sign in with a username and password."
      "No password set — you sign in through a linked provider only.")]
   [:ty-button {:type "button" :appearance "outlined"
                "data-on:click" "$pwOpen = true"}
    (icon/icon :key {:size "12" :slot "start"})
    (if password? "Change password" "Set a password")]])

(defn password-modal
  [password?]
  [:ty-modal {(keyword "data-attr:open") "$pwOpen"
              :backdrop "true" :close-on-outside-click true :close-on-escape true
              (keyword "data-on:close") "$pwOpen = false"}
   [:div.console-confirm
    [:h2 (if password? "Change password" "Set a password")]
    (when-not password?
      [:p.console-field-hint
       "Because this account has no password yet, this uses your recent sign-in "
       "as proof. If it's been a while, sign in again first."])
    (when password?
      [:div.console-modal-field
       [:ty-input {:type "password" :label "Current password" :size "sm"
                   :autocomplete "current-password" :data-bind "pwCurrent"}]])
    [:div.console-modal-field
     [:ty-input {:type "password" :label "New password" :size "sm"
                 :autocomplete "new-password" :data-bind "pwNew"}]]
    [:div.console-modal-field
     [:ty-input (merge {:type "password" :label "Repeat new password" :size "sm"
                        :autocomplete "new-password" :data-bind "pwConfirm"}
                       (ui/on-enter "@post('/console/identities/password')"))]
     [:p.console-field-hint "At least 12 characters."]]
    [:div.console-confirm-actions
     [:ty-button {:type "button" :size "sm" :appearance "ghost"
                  "data-on:click" "$pwOpen = false"}
      "Cancel"]
     [:ty-button {:type "button" :size "sm" :flavor "primary"
                  "data-on:click" "@post('/console/identities/password')"}
      "Save"]]]])

(defn render
  [request & [notice]]
  (let [user-xid (get-in request [:console/session :resource-owner])
        {:keys [linked available]} (data/provider-split user-xid)
        password? (data/has-password? user-xid)]
    (ui/admin-shell
     {:title "Sign-in methods"
      :user (:console/principal request)
      :uri "/console/identities"
      :stream (when (routes/sse-available?) "/console/live/identities")
      :confirm "/console/identities/unlink"
      :session (:console/session request)
      :body
      [:div.console-page {:data-signals (str "{linkOpen: '', linkPassword: '', pwOpen: false, "
                                             "pwCurrent: '', pwNew: '', pwConfirm: ''}")}
       [:div.console-page-head
        [:div.console-eyebrow "Your account"]
        [:h1.console-title "Sign-in methods"]
        [:p.console-subtitle
         "Identity providers linked to your account. Unlinking is refused if "
         "it would leave you no way to sign in."]]
       ;; outside #identities-panel — the live stream would wipe it
       (widgets/notice notice true)
       (widgets/identities-table (data/my-identities user-xid))
       (connect-buttons available)
       (password-section password?)
       (connect-modal linked password?)
       (password-modal password?)]})))
