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

(ns synthigy.oauth.page.login
  (:require
    [clojure.string :as str]
    [ring.util.codec :as codec]
    [synthigy.oauth.authorization-code :as ac]
    [synthigy.oauth.core :as core]
    [synthigy.oauth.device-code :as dc]
    [synthigy.oauth.federated.registry :as registry]
    [synthigy.oauth.page.assets :as assets]))

(defn text-field
  [{:keys [id label icon] :as attrs}]
  [:div.sy-field
   [:div.sy-label-row [:label.sy-label {:for id} label]]
   [:div.sy-control
    (when icon [:span.sy-input-icon (assets/icon icon)])
    [:input.sy-input (dissoc attrs :label :icon)]]])

(defn password-field
  "Password input with a reveal toggle and a Caps Lock tag in its label row."
  [{:keys [id label icon] :as attrs}]
  [:div.sy-field
   [:div.sy-label-row
    [:label.sy-label {:for id} label]
    [:span.sy-tag {:data-sy-caps id} "Caps Lock"]]
   [:div.sy-control
    (when icon [:span.sy-input-icon (assets/icon icon)])
    [:input.sy-input (-> attrs (dissoc :label :icon) (assoc :type "password"))]
    [:button.sy-input-btn {:type "button"
                           :data-sy-reveal id
                           :aria-label "Show password"
                           :aria-pressed "false"}
     (assets/icon :eye "when-hidden")
     (assets/icon :eye-off "when-shown")]]])

(defn submit-button [label]
  [:button.sy-btn.sy-btn--primary {:type "submit"}
   [:span.sy-btn-label label]
   [:span.sy-spin {:aria-hidden "true"}]])

(defn provider-buttons
  [providers href-fn]
  [:div.sy-stack
   (for [{:keys [label] :as p} providers]
     [:a.sy-btn.sy-btn--secondary {:href (href-fn p)}
      (assets/provider-mark p)
      [:span.sy-btn-label "Continue with " label]])])

(defn federated-buttons
  "Render a \"Continue with X\" link per active federation provider, carrying
   the same opaque `state` the login page was handed."
  [raw-state]
  (when raw-state
    (when-let [providers (seq (registry/list-providers))]
      (list
        [:div.sy-divider "or"]
        (provider-buttons
          providers
          (fn [{:keys [name]}]
            (str "/oauth/federated/start?"
                 (codec/form-encode {:provider name :state raw-state}))))))))

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
                       :credentials "Wrong username or password."
                       :already-authorized "This device is already authorized."
                       "Something went wrong. Try again.")
    :else (or (federated-errors (str error))
              (when (str/starts-with? (str error) "idp_")
                (str "The provider refused: "
                     (str/replace (subs (str error) 4) "_" " ") "."))
              "Could not sign in. Try again.")))


(defn login-html
  [{error     :synthigy.oauth.login/error
    username  :synthigy.oauth.login/username
    raw-state :synthigy.oauth.login/raw-state
    {:keys [authorization-code device-code flow]} :synthigy.oauth.login/state}]
  (let [client (cond
                 authorization-code (ac/get-code-client authorization-code)
                 device-code (dc/get-code-client device-code))
        client-name (not-empty (str (:name client)))
        account? (= "account" flow)
        logo (get-in client [:settings "logo-url"])
        invalid (when (= error :credentials) "true")]
    (assets/document
      {:title (str "Sign in · " (or client-name "Synthigy"))
       :css "login.css"
       :chip (or client-name (when account? "Your account"))
       :status [:span.sy-status-item [:span.sy-key "Enter"] [:span "sign in"]]
       :scripts ["/oauth/js/login.js"]}
      [:div.sy-card
       [:div.sy-card-head
        (when logo [:img.sy-card-logo {:src logo :alt ""}])
        [:h1.sy-card-title "Sign in"]
        (assets/voice {:id "voice"}
                      (cond
                        client-name (list "to continue to " [:b client-name])
                        account?    "to manage your account."
                        :else       "Enter your credentials to continue.")
                      (error-text error))]
       [:div.sy-card-body
        [:form.sy-form {:method "post" :data-sy-busy ""}
         (text-field
           {:id "username"
            :name "username"
            :label "Username"
            :icon :user-round
            :value (or username
                       (some-> authorization-code ac/get-code-request :login_hint core/login-hint)
                       "")
            :autocomplete "username"
            :autocapitalize "off"
            :spellcheck "false"
            :aria-invalid invalid})
         (password-field
           {:id "password"
            :name "password"
            :label "Password"
            :icon :lock
            :autocomplete "current-password"
            :autocorrect "off"
            :spellcheck "false"
            :aria-invalid invalid})
         (submit-button "Sign In")]
        (federated-buttons raw-state)]
       (when (and (or authorization-code device-code) raw-state)
         [:div.sy-card-foot
          [:a.sy-link.sy-cancel
           {:href (str "/oauth/login?"
                       (codec/form-encode {:state raw-state :cancel 1}))}
           (assets/icon :arrow-left)
           [:span (cond
                    device-code "Cancel this sign-in"
                    client-name (str "Cancel and return to " client-name)
                    :else "Cancel")]]])])))

(defn claim-html
  "Onboarding claim page: the invited user finishes the account `token` targets
   by linking an IdP or setting a password, whichever `providers`/`password?`
   allow."
  [token providers password? & [error]]
  (assets/document
    {:title "Set up your account · Synthigy"
     :css "login.css"
     :scripts ["/oauth/js/login.js"]}
    [:div.sy-card
     [:div.sy-card-head
      [:h1.sy-card-title "Set up your account"]
      (assets/voice {:id "claim-msg"} "Choose how you'll sign in from now on." error)]
     [:div.sy-card-body
      (when password?
        [:form.sy-form {:method "post"
                        :action "/oauth/claim/password"
                        :id "claim-password-form"
                        :data-sy-busy ""}
         [:input {:type "hidden" :name "token" :value token}]
         (password-field
           {:id "claim-password"
            :name "password"
            :label "New password"
            :icon :lock
            :autocomplete "new-password"
            :autocorrect "off"
            :spellcheck "false"})
         (password-field
           {:id "claim-password-confirm"
            :label "Confirm password"
            :icon :lock
            :autocomplete "new-password"
            :autocorrect "off"
            :spellcheck "false"})
         (submit-button "Set Password")])
      (when (seq providers)
        (list
          (when password? [:div.sy-divider "or"])
          (provider-buttons
            providers
            (fn [{:keys [name]}]
              (str "/oauth/federated/start?"
                   (codec/form-encode {:provider name :mode "claim" :state token}))))))]]))
