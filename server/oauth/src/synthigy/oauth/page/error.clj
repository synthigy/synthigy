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

(ns synthigy.oauth.page.error
  "OAuth error page for cases where the server MUST NOT redirect (RFC 6749 §4.1.2.1)."
  (:require
   [clojure.string :as str]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.oauth.page.custom :as login]))

(def security-errors
  #{"redirect_missmatch"
    "client_not_registered"
    "no_redirections"})

(defn register-hint
  [client-name redirect-uri]
  (when (and client-name (not-empty redirect-uri))
    [:div.sy-dev
     [:div.sy-dev-label "For developers"]
     [:p "To fix this, add this exact address to the Redirect URIs of " [:b client-name] ":"]
     [:code.sy-codeblock {:title "Redirect URI"} redirect-uri]]))

(def error-messages
  {"no_redirections"
   {:title "Authorization blocked"
    :sub "This application has no registered return address."
    :message "The authorization request cannot be completed safely, so sign-in was stopped."
    :register-hint? true}

   "invalid_login_page"
   {:title "Configuration error"
    :sub "This application's login page setting is invalid."
    :foot "Contact the developer of this application to fix it."
    :message-fn (fn [{:keys [client-name invalid-value]}]
                  (list
                   [:p (if client-name
                         (list "Client " [:b client-name] " has an invalid login page setting:")
                         "This client has an invalid login page setting:")
                    (when (not-empty (str invalid-value))
                      [:code.sy-codeblock invalid-value])]
                   [:p "The " [:code "login-page"] " setting must name a login page bundle (lowercase letters, digits and dashes)."]))}

   "missing_redirect"
   {:title "Invalid request"
    :sub "The request does not say where to return after sign-in."
    :foot "Contact the developer of this application to fix it."
    :message "The authorization request is missing required information."}

   "redirect_missmatch"
   {:title "Authorization blocked"
    :sub "This application asked to return to an unregistered address."
    :message "The return address in this request doesn't match the application's configuration, so sign-in was stopped."
    :register-hint? true}

   "missing_response_type"
   {:title "Invalid request"
    :sub "The request does not say what it is asking for."
    :foot "Contact the developer of this application to fix it."
    :message "The authorization request is incomplete: it carries no response type."}

   "client_not_registered"
   {:title "Unknown application"
    :sub "No application with this client ID is registered here."
    :message "This application is not recognized by the authorization server."}

   "corrupt_session"
   {:title "Session error"
    :sub "Your sign-in session could not be verified."
    :foot "If this keeps happening, contact your administrator."
    :message "Your authorization session could not be verified. Start the sign-in again from the application."}

   "unsupported_grant_type"
   {:title "Unsupported request"
    :sub "This application asked for a sign-in method that is not supported."
    :foot "Contact the developer of this application to fix it."
    :message "The requested authorization method is not supported by this server."}})

(def default-error
  {:title "Authorization error"
   :sub "Sign-in could not continue."
   :foot "If this keeps happening, contact support."
   :message "An error occurred during authorization."})

(defn error-html
  "Full error page for `error-type`; `context` carries :client-name, :redirect-uri, :invalid-value."
  ([error-type] (error-html error-type nil))
  ([error-type context]
   (let [security? (contains? security-errors error-type)
         {:keys [title sub foot message message-fn register-hint?]} (get error-messages error-type default-error)
         context (when (map? context) context)
         client-name (not-empty (str (:client-name context)))
         tone (if security? "tone-error" "tone-warn")
         glyph (cond
                 security? :shield-alert
                 (= error-type "client_not_registered") :ban
                 :else :wrench)
         code (or (not-empty (str error-type)) "unknown_error")]
     (assets/document
      {:title (str title " · Synthigy")
       :css "error.css"
       :chip client-name
       :status [:span.sy-status-item {:class tone} (assets/icon glyph) [:span code]]}
      [:div.sy-card.sy-card--wide.sy-card--center
       [:div.sy-card-head
        (assets/hero {:tone tone :glyph glyph :title title :text sub}
                     (assets/code-chip code))]
       [:div.sy-card-body
        (when security?
          [:div.sy-msg.tone-error
           [:span.sy-msg-icon (assets/icon :key-round)]
           [:span.sy-msg-text "Do not enter your password or any other credentials."]])
        [:div.sy-prose
         (if message-fn
           (message-fn context)
           [:p message])]
        (when register-hint?
          (register-hint client-name (:redirect-uri context)))]
       [:div.sy-card-foot
        (cond
          security? "If you were redirected here unexpectedly, close this window."
          (not (str/blank? foot)) foot
          :else "Contact the developer or administrator of this application.")]]))))

(defn render-error-page
  "OAuth error page as a 400 HTTP response, used when redirect_uri is
   invalid/missing."
  ([error-type] (render-error-page error-type nil))
  ([error-type context]
   (or
    (login/custom-page-redirect
     "error.html"
     (cond-> {:error error-type
              :secure (str (contains? security-errors error-type))}
       (map? context) (merge context)))
    {:status 400
     :headers {"Content-Type" "text/html; charset=utf-8"
               "Cache-Control" "no-cache, no-store, must-revalidate"}
     :body (str (error-html error-type context))})))
