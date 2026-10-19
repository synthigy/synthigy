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

(ns synthigy.oauth.page.device
  (:require
   [hiccup2.core :refer [html raw]]
   [synthigy.oauth.page.assets :as assets]))

(defn authorize
  ([] (authorize nil))
  ([{error :synthigy.oauth.device-code/error
     user-code :synthigy.oauth.device-code/user-code
     complete? :synthigy.oauth.device-code/complete?}]
   (html
    [:head
     [:meta {:charset "UTF-8"}]
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
     [:title "Synthigy Code Check"]
     [:link {:rel "icon" :type "image/png" :href "/oauth/images/favicon.png"}]
     [:link {:rel "preconnect" :href "https://fonts.googleapis.com"}]
     [:link {:rel "preconnect" :href "https://fonts.gstatic.com" :crossorigin true}]
     [:link {:rel "stylesheet" :href "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"}]
     [:link {:rel "stylesheet" :href "/oauth/css/device.css"}]]
    [:body
     [:div.oauth-container
      [:div.device-card
       [:div.inner
        [:div.header
         [:span.sy-login-mark assets/figurative]
         [:h1 "Device Confirmation"]]
        [:div.description
         (if complete?
           "Confirm that following user code is used for device authorization"
           "Type in device code displayed on your device")]
        [:form.device-form {:method "POST"}
         (let [chars (when complete? (vec (remove #{\-} (or user-code ""))))]
           (list
            [:div.code-boxes
             (map
              (fn [i]
                (list
                 (when (= i 4) [:span.code-sep])
                 [:input (cond-> {:class "code-box"
                                  :type "text"
                                  :maxlength 1
                                  :inputmode "text"
                                  :autocomplete "off"
                                  :autocapitalize "characters"
                                  :spellcheck false}
                           complete? (assoc :value (str (get chars i ""))
                                            :readonly true
                                            :tabindex "-1")
                           (not complete?) (assoc :aria-label (str "Code character " (inc i))))]))
              (range 8))]
            (when-not complete?
              [:input {:type "hidden" :name "user_code" :id "user_code"}])))
         [:div.error
          (when error
            [:div.message
             (case error
               :expired "This user code has expired"
               :malicous-code "Entered user code doesn't exist"
               :malicous-ip "You are trying to enter code from wrong host"
               :malicious-user-agent "You are trying to enter code from wrong app"
               (:not-available :device-code/not-available)
               "This code doesn't exist — check what's shown on your device and try again"
               :no-confirm-session "Your confirmation session expired — go back and re-enter the code"
               :unknown-action "Something went wrong — please try again"
               "Something went wrong — please try again")])]
         [:div.actions
          (if complete?
            [:div
             [:button.confirm {:type "submit" :name "action" :value "confirm"} "Confirm"]
             [:button.cancel {:type "submit" :name "action" :value "cancel"} "Cancel"]]
            [:div
             [:button.continue {:type "submit"} "Continue"]])]]]]]
     [:script {:src "/oauth/js/starfield.js"}]
     [:script {:src "/oauth/js/device.js"}]])))
