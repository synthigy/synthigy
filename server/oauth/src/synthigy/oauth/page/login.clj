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

(ns synthigy.oauth.page.login
  (:require
    [clojure.string :as str]
    [hiccup2.core :refer [html]]
    [ring.util.codec :as codec]
    [synthigy.oauth.authorization-code :as ac]
    [synthigy.oauth.device-code :as dc]
    [synthigy.oauth.federated.registry :as registry]
    [synthigy.oauth.page.assets :as assets]))

;; Lucide icons (MIT) — stroke-based; color/size come from .ficon in login.css.
(def password-icon
  [:svg.ficon
   {:id "password-icon"
    :viewBox "0 0 24 24"
    :fill "none"
    :stroke "currentColor"
    :stroke-width "2"
    :stroke-linecap "round"
    :stroke-linejoin "round"}
   [:rect {:width "18" :height "11" :x "3" :y "11" :rx "2" :ry "2"}]
   [:path {:d "M7 11V7a5 5 0 0 1 10 0v4"}]])

(def user-icon
  [:svg.ficon
   {:id "username-icon"
    :viewBox "0 0 24 24"
    :fill "none"
    :stroke "currentColor"
    :stroke-width "2"
    :stroke-linecap "round"
    :stroke-linejoin "round"}
   [:path {:d "M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"}]
   [:circle {:cx "12" :cy "7" :r "4"}]])

(defn federated-buttons
  "Render a \"Continue with X\" link per active federation provider, carrying
   the same opaque `state` the login page was handed."
  [raw-state]
  (when raw-state
    (when-let [providers (seq (registry/list-providers))]
      (list
        [:div.sy-fed-divider [:span "or"]]
        [:div.sy-federated
         (for [{:keys [name label] :as p} providers]
           [:a.sy-fed-btn
            {:href (str "/oauth/federated/start?"
                        (codec/form-encode {:provider name :state raw-state}))}
            (assets/provider-mark p)
            [:span "Continue with " label]])]))))

(def federated-errors
  "Codes `oauth.federated` redirects back here with, in words."
  {"idp_access_denied"     "Sign-in was cancelled. Try again, or use your password."
   "idp_consent_required"  "Sign-in was cancelled. Try again, or use your password."
   "idp_login_required"    "Sign-in was cancelled. Try again, or use your password."
   "idp_interaction_required" "Sign-in was cancelled. Try again, or use your password."
   "provider_unknown"      "That sign-in method is no longer available."
   "provider_disabled"     "That sign-in method is switched off."
   "provider_unsupported"  "Could not reach that provider. Try again."
   "token_invalid"         "That provider's response could not be verified."
   "no_code"               "That provider sent no authorization code."
   "state_invalid"         "That sign-in link expired. Try again."
   "expired_code"          "This sign-in took too long. Start again from the app."
   "not_linked"            "That account isn't linked here. Use another method."
   "email_exists"          "An account already uses that email address."
   "provision_failed"      "Could not create an account from that provider."
   "callback_error"        "Something went wrong signing in. Try again."})

(defn error-text
  "Render an error however it arrived: a keyword from the password POST, or a
   federated code from a `?error=` redirect."
  [error]
  (cond
    (nil? error) ""
    (keyword? error) (case error
                       :credentials "Wrong credentials. Check your username and password"
                       :already-authorized "User has already authorized this device"
                       "Unknown error... Contact support")
    :else (or (federated-errors (str error))
              (when (str/starts-with? (str error) "idp_")
                (str "The provider refused: "
                     (str/replace (subs (str error) 4) "_" " ") "."))
              "Could not sign in. Try again.")))

(defn login-html
  [{error     :synthigy.oauth.login/error
    raw-state :synthigy.oauth.login/raw-state
    {:keys [authorization-code device-code]} :synthigy.oauth.login/state}]
  (let [client (cond
                 authorization-code (ac/get-code-client authorization-code)
                 device-code (dc/get-code-client device-code))
        ;; Per-client logo wins; otherwise show the Synthigy brand lockup.
        logo (get-in client [:settings "logo-url"])]
    (html
      [:head
       [:meta {:charset "UTF-8"}]
       [:meta {:name "viewport"
               :content "width=device-width, initial-scale=1.0"}]
       [:title "Synthigy Login"]
       [:link {:rel "icon"
               :type "image/png"
               :href "/oauth/images/favicon.png"}]
       [:link {:rel "preconnect"
               :href "https://fonts.googleapis.com"}]
       [:link {:rel "preconnect"
               :href "https://fonts.gstatic.com"
               :crossorigin true}]
       [:link {:rel "stylesheet"
               :href "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"}]
       [:link {:rel "stylesheet"
               :href "/oauth/css/login.css"}]]
      [:body
       [:div.sy-login
        (if logo
          [:img#logo-image {:src logo}]
          [:div.sy-lockup
           [:span.sy-login-mark assets/figurative]
           [:span.sy-login-verbal assets/verbal]])
        [:form {:method "post"}
         [:div.row
          user-icon
          [:input
           {:id "username"
            :name "username"
            :placeholder "Username"
            :autoComplete "new-password"}]]
         [:div.row
          password-icon
          [:input
           {:id "password"
            :name "password"
            :type "password"
            :placeholder "Password"
            :autocomplete "new-password"
            :autocorrect "off"
            :spellcheck false}]]
         [:div.row
          [:span.error (error-text error)]]
         [:button
          [:h4 "SIGN IN"]]]
        (federated-buttons raw-state)
        (when (and (or authorization-code device-code) raw-state)
          [:a.sy-cancel
           {:href (str "/oauth/login?"
                       (codec/form-encode {:state raw-state :cancel 1}))}
           (cond
             device-code "Cancel this sign-in"
             (not-empty (str (:name client))) (str "Cancel and return to " (:name client))
             :else "Cancel")])]
       [:script {:src "/oauth/js/starfield.js"}]
       [:script {:src "/oauth/js/login.js"}]])))

(defn claim-html
  "Onboarding claim page: the invited user finishes the account `token` targets
   by linking an IdP or setting a password, whichever `providers`/`password?`
   allow."
  [token providers password? & [error]]
  (html
    [:head
     [:meta {:charset "UTF-8"}]
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
     [:title "Set up your account"]
     [:link {:rel "icon" :type "image/png" :href "/oauth/images/favicon.png"}]
     [:link {:rel "stylesheet"
             :href "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"}]
     [:link {:rel "stylesheet" :href "/oauth/css/login.css"}]]
    [:body
     [:div.sy-login
      [:div.sy-lockup
       [:span.sy-login-mark assets/figurative]
       [:span.sy-login-verbal assets/verbal]]
      [:h4 {:style "margin:0;font-weight:600;"} "Set up your account"]
      [:p {:style "margin:0;font-size:0.85rem;opacity:0.7;text-align:center;"}
       "Choose how you'll sign in from now on."]
      (when password?
        (list
          [:form {:method "post" :action "/oauth/claim/password" :id "claim-password-form"}
           [:input {:type "hidden" :name "token" :value token}]
           [:div.row
            password-icon
            [:input {:id "claim-password" :name "password" :type "password"
                     :placeholder "New password" :autocomplete "new-password"
                     :autocorrect "off" :spellcheck false}]]
           [:div.row
            password-icon
            [:input {:id "claim-password-confirm" :type "password"
                     :placeholder "Confirm password" :autocomplete "new-password"
                     :autocorrect "off" :spellcheck false}]]
           [:div.row [:span.error (or error "")]]
           [:button [:h4 "SET PASSWORD"]]]
          ;; ponytail: match-check only — the real one-time/target guard is
          ;; server-side.
          [:script "document.getElementById('claim-password-form').addEventListener('submit', function (e) {
  var p = document.getElementById('claim-password').value;
  var c = document.getElementById('claim-password-confirm').value;
  if (p !== c) { e.preventDefault(); alert('Passwords do not match.'); }
});"]))
      (when (seq providers)
        (list
          (when password? [:div.sy-fed-divider [:span "or"]])
          [:div.sy-federated
           (for [{:keys [name label] :as p} providers]
             [:a.sy-fed-btn
              {:href (str "/oauth/federated/start?"
                          (codec/form-encode {:provider name :mode "claim" :state token}))}
              (assets/provider-mark p)
              [:span "Continue with " label]])]))]
     [:script {:src "/oauth/js/starfield.js"}]]))
