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

(ns synthigy.oauth.page.status
  "Terminal status page for interactive OAuth flows with no client redirect."
  (:require
   [clojure.string :as str]
   [synthigy.dataset.id :as id]
   [synthigy.iam :as iam]
   [synthigy.oauth.federated.registry :as registry]
   [synthigy.oauth.page.assets :as assets]
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

(def guidance
  {:retry   "Start the sign-in again from your application."
   :support "Contact your administrator or the application's support."})

(defn message-for
  "Human copy for a status outcome, reason and guidance split by a newline; never
   blank for an error — falls back to error_description or the raw code."
  [{:keys [value error error_description user client_name provider flow]}]
  (cond
    error
    (if-let [[kind text] (status-errors error)]
      (str text "\n" (guidance kind))
      (if (str/starts-with? error "idp_")
        (str "The identity provider refused: " (str/replace (subs error 4) "_" " ") ".")
        (or error_description (str "Authentication error: " error))))

    (and user client_name)
    [:span [:b client_name] " is now signed in as " [:b user] "."]

    (and (= value "success") (= flow "onboarding_claim"))
    (if (or (nil? provider) (= provider "password"))
      "Your password is set. Use it to sign in from now on."
      (str "Your " provider " account is linked. Use it to sign in from now on."))

    (= value "success")
    (if provider
      (str "Your " provider " account is now linked.")
      "You may now return to your application.")

    (= value "canceled")
    "You cancelled the authorization.\nYou can close this window."

    :else nil))

(defn error-kind
  [error]
  (or (first (status-errors error))
      (when (str/starts-with? (str error) "idp_") :retry)))

(defn outcome
  [{:keys [value error]}]
  (cond
    (or (= value "canceled") (= error "cancelled")) :canceled
    (or error (= value "error")) (if (= :support (error-kind error)) :support :retry)
    (= value "success") :success
    :else :unknown))

(def outcome-look
  {:success  {:tone "tone-success" :glyph :check}
   :canceled {:tone nil :glyph :undo-2}
   :retry    {:tone "tone-warn" :glyph :triangle-alert}
   :support  {:tone "tone-error" :glyph :triangle-alert}
   :unknown  {:tone "tone-info" :glyph :info}})

(def error-glyphs
  "An icon that says what went wrong, by error code."
  {"cancelled"                :undo-2
   "expired_code"             :timer-off
   "device_code_expired"      :timer-off
   "state_invalid"            :timer-off
   "broken_flow"              :unlink
   "no_flow"                  :unlink
   "no_code"                  :unlink
   "ip_address"               :shield-alert
   "user_agent"               :shield-alert
   "challenge"                :shield-alert
   "link_session_mismatch"    :shield-alert
   "already_authorized"       :shield-alert
   "not_linked"               :user-x
   "identity_already_linked"  :user-x
   "reauth_identity_mismatch" :user-x
   "provision_failed"         :user-x
   "email_exists"             :mail-warning
   "access_denied"            :ban
   "claim_method_not_allowed" :ban
   "claim_invalid"            :ticket-x
   "link_requires_login"      :log-in
   "link_reauth_required"     :key-round
   "provider_unknown"         :plug-zap
   "provider_disabled"        :plug-zap
   "provider_unsupported"     :plug-zap
   "token_invalid"            :plug-zap
   "callback_error"           :plug-zap})

(defn glyph-for
  [kind {:keys [error user client_name]}]
  (case kind
    :success (if (and user client_name) :monitor-check :check)
    :canceled :undo-2
    (get error-glyphs error (:glyph (outcome-look kind)))))

(defn title
  [kind {:keys [flow provider user client_name]}]
  (case kind
    :success  (cond
                (= flow "onboarding_claim") "Account ready"
                provider "Account linked"
                (and user client_name) "Device authorized"
                :else "Signed in")
    :canceled "Sign-in cancelled"
    (:retry :support) (case flow
                        "federated_link" "Linking failed"
                        ("onboarding" "onboarding_claim") "Setup failed"
                        "Sign-in failed")
    "Authorization status"))

(defn provider-label
  [provider]
  (or (some (fn [{:keys [name label]}] (when (= name provider) label))
            (registry/list-providers))
      provider))

(defn status
  "Render the status page from a plain params map."
  [{:keys [value error provider] :as params}]
  (let [kind (outcome params)
        {:keys [tone]} (outcome-look kind)
        glyph (glyph-for kind params)
        params (cond-> params
                 (and provider (not= provider "password")) (assoc :provider (provider-label provider)))
        message (message-for params)
        [reason hint] (if (string? message) (str/split message #"\n" 2) [message])
        reason (or (not-empty reason)
                   (case kind
                     :success "You are signed in."
                     :canceled "The sign-in was cancelled."
                     (:retry :support) "The sign-in could not be completed."
                     "There is no sign-in result to show."))
        foot (or (when (= kind :canceled)
                   (if (= (:flow params) "device_code")
                     "Your device was not connected. You can close this window."
                     "Nothing was shared with the application. You can close this window."))
                 (not-empty hint)
                 (case kind
                   :retry (guidance :retry)
                   :support (guidance :support)
                   "You can close this window."))
        heading (title kind params)
        code (cond
               error error
               (#{:success :canceled} kind) (name kind)
               :else (or (not-empty value) "no_status"))]
    (assets/document
     {:title (str heading " · Synthigy")
      :css "status.css"
      :chip (:client_name params)
      :status [:span.sy-status-item {:class tone} (assets/icon glyph) [:span code]]}
     [:div.sy-card.sy-card--center
      [:div.sy-card-head
       (assets/hero {:tone tone :glyph glyph :title heading :text reason}
                    (when (and error (not= kind :canceled)) (assets/code-chip error)))]
      [:div.sy-card-foot foot]])))

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
