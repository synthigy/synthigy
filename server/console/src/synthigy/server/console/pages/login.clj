;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.pages.login
  (:require
   [ring.util.codec :as codec]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.server.console.ui :as ui]))

(defn render
  [& [error logged-out?]]
  (ui/bare-shell
   {:title "Sign in"
    :body
    [:div.console-login
     (ui/lockup)
     [:div.console-login-hint "administration"]
     (when error [:div.console-login-error error])
     [:form {:method "post" :action "/console/login"
             :onkeydown (str "if (event.key === 'Enter') { event.preventDefault(); "
                             "this.requestSubmit(); }")}
      [:ty-input {:type "text" :name "username" :label "Username"
                  :autofocus true :autocomplete "username"}]
      [:ty-input {:type "password" :name "password" :label "Password"
                  :autocomplete "current-password"}]
      [:ty-button {:type "submit" :flavor "neutral" :wide true} "Sign in"]]
     (when-let [providers (seq (registry/list-providers))]
       (list
        [:div.console-fed-divider [:span "or"]]
        [:div.console-federated
         (for [{:keys [name label] :as p} providers]
           [:a.console-fed-btn
            {:href (str "/oauth/federated/start?"
                        (codec/form-encode
                         (cond-> {:provider name :mode "console"}
                           logged-out? (assoc :prompt "select_account"))))}
            [:span.console-fed-mark (assets/provider-mark p)]
            [:span "Continue with " label]])]))]}))
