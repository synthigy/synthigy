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

(ns synthigy.oauth.page.logout
  "Sign-out confirmation for a logout request that carries no id_token_hint."
  (:require
   [synthigy.oauth.page.assets :as assets]))

(defn confirm-html
  [fields client-name]
  (assets/document
   {:title "Sign out · Synthigy"
    :css "status.css"
    :chip client-name
    :status [:span.sy-status-item (assets/icon :log-out) [:span "sign_out"]]}
   [:div.sy-card.sy-card--center
    [:div.sy-card-head
     (assets/hero {:glyph :log-out
                   :title "Sign out?"
                   :text (if client-name
                           (list [:b client-name] " asked to end your session on this server.")
                           "An application asked to end your session on this server.")})]
    [:div.sy-card-body
     [:form.sy-form {:method "post" :action "/oauth/logout"}
      (for [[k v] fields
            :when (not-empty v)]
        [:input {:type "hidden" :name (name k) :value v}])
      [:button.sy-btn.sy-btn--primary {:type "submit"}
       [:span.sy-btn-label "Sign Out"]]]]
    [:div.sy-card-foot "If you didn't ask to sign out, close this window and stay signed in."]]))

(defn confirm-page
  [fields client-name]
  {:status 200
   :headers {"Content-Type" "text/html; charset=utf-8"
             "Cache-Control" "no-store"
             "Referrer-Policy" "no-referrer"}
   :body (str (confirm-html fields client-name))})
