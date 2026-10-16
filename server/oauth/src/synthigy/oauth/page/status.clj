;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.page.status
  "Terminal status page for interactive OAuth flows with no client redirect."
  (:require
   [clojure.string :as str]
   [hiccup2.core :refer [html]]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.oauth.core :as core]
   [synthigy.oauth.page.custom :as login-page]))

(def status-errors
  "Every `error` code /oauth/status can receive, as [kind text]; :retry means the
   person can start again, :support means only an admin can fix it."
  {"cancelled"                [:retry "You cancelled the sign-in."]
   "broken_flow"              [:retry "The authorization flow is broken."]
   "expired_code"             [:retry "Your login grace period has expired."]
   "device_code_expired"      [:retry "The code you entered has expired."]
   "already_authorized"       [:retry "Someone already authenticated using this code."]
   "ip_address"               [:retry "A potentially malicious IP-address change was detected."]
   "user_agent"               [:retry "A potentially malicious app change was detected."]
   "challenge"                [:retry "A potentially malicious challenge change was detected."]
   "not_linked"               [:retry "This external identity isn't linked to any account."]
   "link_requires_login"      [:retry "You must be signed in to link an external account."]
   "link_reauth_required"     [:retry "For your security, please sign in again before linking a new login method."]
   "email_exists"             [:retry "An account with this email already exists. Sign in with your existing method, then link this identity."]
   "state_invalid"            [:retry "That sign-in link expired."]
   "no_code"                  [:retry "The identity provider sent no authorization code."]
   "provider_unsupported"     [:retry "Could not reach that identity provider."]
   "claim_invalid"            [:retry "This invitation link is invalid or has already been used."]
   "reauth_identity_mismatch" [:retry "You verified with a different account than the one signed in."]
   "access_denied"            [:support "Access was denied."]
   "provider_unknown"         [:support "That sign-in method doesn't exist."]
   "provider_disabled"        [:support "That sign-in method is switched off."]
   "claim_method_not_allowed" [:support "This invitation doesn't allow that sign-in method."]
   "no_flow"                  [:support "The sign-in request carried no flow to continue."]
   "provision_failed"         [:support "We couldn't create your account."]
   "token_invalid"            [:support "The identity provider's response couldn't be verified."]
   "callback_error"           [:support "The identity provider returned an error."]
   "link_session_mismatch"    [:support "Account-link verification failed. Please try linking again."]
   "identity_already_linked"  [:support "This external identity is already linked to an account."]})

(defn message-for
  "Human copy for a status outcome; never returns blank — falls back to
   error_description or the raw code."
  [{:keys [value error error_description user client_name provider]}]
  (cond
    error
    (if-let [[kind text] (status-errors error)]
      (str text (if (= :retry kind)
                  "\nPlease restart the authentication process."
                  "\nPlease contact application support."))
      (if (str/starts-with? error "idp_")
        (str "The identity provider refused: " (str/replace (subs error 4) "_" " ") ".")
        (or error_description (str "Authentication error: " error))))

    (and user client_name)
    [:span "Client " [:b client_name] " is authorized by " [:b user] "."]

    (= value "success")
    (if provider
      (str "Your " provider " account is now linked.")
      "You may now return to your application.")

    (= value "canceled")
    "You canceled the authorization. You can close this window."

    :else nil))

(defn state-icon
  [value]
  (let [attrs {:viewBox "0 0 24 24" :fill "none" :stroke "currentColor"
               :stroke-width "1.5" :stroke-linecap "round" :stroke-linejoin "round"}]
    (case value
      "success"  [:svg attrs [:circle {:cx "12" :cy "12" :r "10"}]
                  [:path {:d "m9 12 2 2 4-4"}]]
      "canceled" [:svg attrs [:circle {:cx "12" :cy "12" :r "10"}]
                  [:path {:d "m15 9-6 6"}] [:path {:d "m9 9 6 6"}]]
      "error"    [:svg attrs
                  [:path {:d "m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"}]
                  [:path {:d "M12 9v4"}] [:path {:d "M12 17h.01"}]]
      ;; unknown value → info
      [:svg attrs [:circle {:cx "12" :cy "12" :r "10"}]
       [:path {:d "M12 16v-4"}] [:path {:d "M12 8h.01"}]])))

(defn status
  "Render the themed status page from a plain params map."
  [{:keys [value] :as params}]
  (html
   [:head
    [:meta {:charset "UTF-8"}]
    [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
    [:title "Synthigy OAuth Status"]
    [:link {:rel "icon" :type "image/png" :href "/oauth/images/favicon.png"}]
    [:link {:rel "preconnect" :href "https://fonts.googleapis.com"}]
    [:link {:rel "preconnect" :href "https://fonts.gstatic.com" :crossorigin true}]
    [:link {:rel "stylesheet" :href "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"}]
    [:link {:rel "stylesheet" :href "/oauth/css/status.css"}]]
   [:body
    [:div.oauth-container
     [:div {:class (str "status-card " (or value "unknown"))}
      [:div.header
       [:div.icon (state-icon value)]
       [:h2 (case value
              "success"  "Authentication Complete"
              "canceled" "Authentication Canceled"
              "error"    "Authentication Error"
              "Authentication Status")]]
      [:div.message (message-for params)]]]
    [:script {:src "/oauth/js/starfield.js"}]]))

(defn status-params
  [request]
  (let [{:keys [value error error_description user client provider flow]} (:params request)]
    (cond-> {}
      value (assoc :value value)
      error (assoc :error error)
      error_description (assoc :error_description error_description)
      user (assoc :user user)
      client (assoc :client client
                    :client_name (:name (iam/get-client-by-key (id/coerce-arg client))))
      provider (assoc :provider provider)
      flow (assoc :flow flow))))

(defn status-handler
  "Ring handler for /oauth/status and /oauth/device/status; always 200 — a
   terminal user-facing outcome, not an API."
  [request]
  (let [params (status-params request)]
    (or (login-page/custom-page-redirect "status.html" params)
        {:status 200
         :headers {"Content-Type" "text/html; charset=utf-8"
                   "Cache-Control" "no-cache, no-store, must-revalidate"}
         :body (str (status params))})))
