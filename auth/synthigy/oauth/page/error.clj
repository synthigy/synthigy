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

(ns synthigy.oauth.page.error
  "OAuth error page for cases where the server MUST NOT redirect (RFC 6749
   §4.1.2.1). See docs/core/synthigy/oauth/page/error.md."
  (:require
   [hiccup2.core :refer [html raw]]
   [synthigy.oauth.page.custom :as login]))

(def ^:private security-errors
  #{"redirect_missmatch"
    "client_not_registered"
    "no_redirections"})

(def ^:private error-messages
  {"no_redirections"
   {:title "Authorization Blocked"
    :message "This application has no configured redirect addresses. The authorization request cannot be completed safely."}

   "invalid_login_page"
   {:title "Configuration Error"
    :message-fn (fn [{:keys [client-name invalid-value]}]
                  [:span
                   "Client " [:b client-name] " has an invalid login page setting:"
                   [:br] [:code invalid-value]
                   [:br] [:br]
                   "Only relative URLs (starting with '/') are allowed for the 'login-page' setting. "
                   "Please contact the application developer."])}

   "missing_redirect"
   {:title "Invalid Request"
    :message "The authorization request is missing required information. Please contact the application developer."}

   "redirect_missmatch"
   {:title "Authorization Blocked"
    :message "The return address provided by this application doesn't match its configuration. Do not enter any credentials."}

   "missing_response_type"
   {:title "Invalid Request"
    :message "The authorization request is incomplete. Please contact the application developer."}

   "client_not_registered"
   {:title "Unknown Application"
    :message "This application is not recognized by the authorization server. Do not enter any credentials."}

   "corrupt_session"
   {:title "Session Error"
    :message "Your authorization session could not be verified. Please try again or contact support."}

   "unsupported_grant_type"
   {:title "Unsupported Request"
    :message "The requested authorization method is not supported. Please contact the application developer."}})

(def ^:private default-error
  {:title "Authorization Error"
   :message "An error occurred during authorization. Please contact support."})

(defn shield-icon []
  (raw "<svg xmlns=\"http://www.w3.org/2000/svg\" fill=\"none\" viewBox=\"0 0 24 24\" stroke-width=\"1.5\" stroke=\"currentColor\">
  <path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M12 9v3.75m0-10.036A11.959 11.959 0 0 1 3.598 6 11.99 11.99 0 0 0 3 9.75c0 5.592 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.25-8.25-3.286Zm0 13.036h.008v.008H12v-.008Z\" />
</svg>"))

(defn wrench-icon []
  (raw "<svg xmlns=\"http://www.w3.org/2000/svg\" fill=\"none\" viewBox=\"0 0 24 24\" stroke-width=\"1.5\" stroke=\"currentColor\">
  <path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M11.42 15.17 17.25 21A2.652 2.652 0 0 0 21 17.25l-5.877-5.877M11.42 15.17l2.496-3.03c.317-.384.74-.626 1.208-.766M11.42 15.17l-4.655 5.653a2.548 2.548 0 1 1-3.586-3.586l6.837-5.63m5.108-.233c.55-.164 1.163-.188 1.743-.14a4.5 4.5 0 0 0 4.486-6.336l-3.276 3.277a3.004 3.004 0 0 1-2.25-2.25l3.276-3.276a4.5 4.5 0 0 0-6.336 4.486c.091 1.076-.071 2.264-.904 2.95l-.102.085m-1.745 1.437L5.909 7.5H4.5L2.25 3.75l1.5-1.5L7.5 4.5v1.409l4.26 4.26m-1.745 1.437 1.745-1.437m6.615 8.206L15.75 15.75M4.867 19.125h.008v.008h-.008v-.008Z\" />
</svg>"))

(defn error-html
  "Hiccup structure for an OAuth error page; optional context supplies dynamic
   data (e.g. client-name for invalid_login_page)."
  ([error-type] (error-html error-type nil))
  ([error-type context]
   (let [security? (contains? security-errors error-type)
         {:keys [title message message-fn]} (get error-messages error-type default-error)
         message (if message-fn (message-fn context) message)]
    (html
     [:head
      [:meta {:charset "UTF-8"}]
      [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
      [:title "OAuth Error"]
      [:link {:rel "icon" :type "image/png" :href "/oauth/images/favicon.png"}]
      [:link {:rel "preconnect" :href "https://fonts.googleapis.com"}]
      [:link {:rel "preconnect" :href "https://fonts.gstatic.com" :crossorigin true}]
      [:link {:rel "stylesheet" :href "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Roboto+Mono&display=swap"}]
      [:link {:rel "stylesheet" :href "/oauth/css/error.css"}]]
     [:body
      [:div.oauth-container
       [:div.error-card
        [:div.header
         [:div {:class (str "icon " (if security? "security" "config"))}
          (if security? (shield-icon) (wrench-icon))]
         [:h2 title]]
        [:div {:class (str "message " (when security? "security"))}
         message]
        [:div.error-code
         (str "Error: " error-type)]
        (when security?
          [:div.help-text
           "If you were redirected here unexpectedly, close this window."])]]
      [:script {:src "/oauth/js/starfield.js"}]]))))

(defn render-error-page
  "OAuth error page as a 400 HTTP response, used when redirect_uri is
   invalid/missing."
  ([error-type] (render-error-page error-type nil))
  ([error-type context]
   (or
    ;; Defer to a branded error.html in the custom login-page folder if present.
    (login/custom-page-redirect
     "error.html"
     (cond-> {:error error-type
              :secure (str (contains? security-errors error-type))}
       (map? context) (merge context)))
    {:status 400
     :headers {"Content-Type" "text/html; charset=utf-8"
               "Cache-Control" "no-cache, no-store, must-revalidate"}
     :body (str (error-html error-type context))})))
