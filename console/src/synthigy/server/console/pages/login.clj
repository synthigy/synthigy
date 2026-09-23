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
                  :autofocus true :autocomplete "username" :required true}]
      [:ty-input {:type "password" :name "password" :label "Password"
                  :autocomplete "current-password" :required true}]
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
