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

(ns synthigy.server.console.pages.login
  (:require
   [ring.util.codec :as codec]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.oauth.page.login :as oauth.login]))

(defn render
  [& [{:keys [error tone username invalid? logged-out?]}]]
  (str
   (assets/document
    {:title "Sign in · Console"
     :css "login.css"
     :chip "Console"
     :status [:span.sy-status-item [:span.sy-key "Enter"] [:span "sign in"]]
     :scripts ["/oauth/js/login.js"]}
    [:div.sy-card
     [:div.sy-card-head
      [:h1.sy-card-title "Sign in"]
      (assets/voice {:id "voice" :tone tone}
                    (list "to continue to " [:b "Console"])
                    error)]
     [:div.sy-card-body
      [:form.sy-form {:method "post" :data-sy-busy ""}
       (oauth.login/text-field
        {:id "username"
         :name "username"
         :label "Username"
         :icon :user-round
         :value (or username "")
         :autocomplete "username"
         :autocapitalize "off"
         :spellcheck "false"
         :aria-invalid (when invalid? "true")})
       (oauth.login/password-field
        {:id "password"
         :name "password"
         :label "Password"
         :icon :lock
         :autocomplete "current-password"
         :autocorrect "off"
         :spellcheck "false"
         :aria-invalid (when invalid? "true")})
       (oauth.login/submit-button "Sign In")]
      (when-let [providers (seq (registry/list-providers))]
        (list
         [:div.sy-divider "or"]
         (oauth.login/provider-buttons
          providers
          (fn [{:keys [name]}]
            (str "/oauth/federated/start?"
                 (codec/form-encode
                  (cond-> {:provider name :mode "console"}
                    logged-out? (assoc :prompt "select_account"))))))))]])))
