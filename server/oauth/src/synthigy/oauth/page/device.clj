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
  "Device activation page: code entry, then confirmation of a code that arrived in the link."
  (:require
   [clojure.string :as str]
   [synthigy.oauth.page.assets :as assets]))

(def errors
  "Every `::error` device_code.clj hands the page, as [status-line code, message]."
  {:expired                   ["code_expired" "This code has expired. Get a new one on your device."]
   :malicous-code             ["code_mismatch" "This code doesn't match the sign-in. Enter it again."]
   :malicous-ip               ["host_mismatch" "This code can't be entered from this network."]
   :malicious-user-agent      ["agent_mismatch" "This code can't be entered from this app."]
   :not-available             ["code_unknown" "This code doesn't exist. Check your device and try again."]
   :device-code/not-available ["code_unknown" "This code doesn't exist. Check your device and try again."]
   :no-confirm-session        ["session_expired" "Your confirmation timed out. Enter the code again."]
   :unknown-action            ["unknown_action" "Something went wrong. Try again."]})

(def fallback-error ["error" "Something went wrong. Try again."])

(def code-errors
  #{:expired :malicous-code :not-available :device-code/not-available})

(def code-length 8)

(defn code-chars
  [user-code]
  (vec (take code-length (str/replace (str/upper-case (str user-code)) #"[^A-Z0-9]" ""))))

(defn dashed
  [chars]
  (let [v (apply str chars)]
    (if (> (count v) 4) (str (subs v 0 4) "-" (subs v 4)) v)))

(defn code-box
  [chars readonly? invalid? i]
  [:input (cond-> {:class "code-box"
                   :type "text"
                   :value (str (get chars i ""))
                   :maxlength 1
                   :inputmode "text"
                   :autocomplete "off"
                   :autocapitalize "characters"
                   :autocorrect "off"
                   :spellcheck "false"
                   :aria-label (str "Character " (inc i))}
            readonly? (assoc :readonly true :tabindex "-1")
            invalid? (assoc :aria-invalid "true"))])

(defn code-field
  [chars readonly? invalid?]
  (let [box (partial code-box chars readonly? invalid?)]
    [:div.sy-field
     [:div.sy-label-row [:span.sy-label {:id "code-label"} "Device code"]]
     [:div.sy-otp {:role "group" :aria-labelledby "code-label"}
      [:div.sy-otp-group (map box (range 0 4))]
      [:span.sy-otp-sep {:aria-hidden "true"}]
      [:div.sy-otp-group (map box (range 4 code-length))]]]))

(defn button
  [tone label attrs]
  [:button (merge {:class (str "sy-btn sy-btn--" (name tone)) :type "submit"} attrs)
   [:span.sy-btn-label label]
   [:span.sy-spin {:aria-hidden "true"}]])

(defn authorize
  "Device activation page: editable code boxes, or the read-only code to confirm when `::complete?`."
  ([] (authorize nil))
  ([{error :synthigy.oauth.device-code/error
     user-code :synthigy.oauth.device-code/user-code
     complete? :synthigy.oauth.device-code/complete?}]
   (let [chars (code-chars user-code)
         [code text] (when error (get errors error fallback-error))
         title (if complete? "Confirm device code" "Connect a device")]
     (assets/document
      {:title (str title " · Synthigy")
       :css "device.css"
       :status (cond
                 error [:span.sy-status-item.tone-error (assets/icon :circle-alert) [:span code]]
                 complete? [:span.sy-status-item (assets/icon :monitor-smartphone) [:span "awaiting confirmation"]]
                 :else [:span.sy-status-item [:span.sy-key "Enter"] [:span "continue"]])
       :scripts ["/oauth/js/device.js"]}
      [:div.sy-card.sy-card--center
       [:div.sy-card-head
        (assets/hero {:glyph :monitor-smartphone :title title}
                     (assets/voice {:id "voice"}
                                   (if complete?
                                     "Check that this code matches the one on your device."
                                     "Enter the code shown on your device.")
                                   text))]
       [:div.sy-card-body
        (if complete?
          [:form.sy-form {:method "post" :id "device-form"}
           (code-field chars true false)
           [:div.sy-actions
            (button :secondary "Cancel" {:name "action" :value "cancel"})
            (button :primary "Confirm" {:name "action" :value "confirm"})]]
          ;; never drop :action — a ?user_code= left in the URL would win over the typed code
          [:form.sy-form {:method "post"
                          :action "/oauth/device/activate"
                          :id "device-form"
                          :data-sy-busy ""}
           [:input {:type "hidden" :name "user_code" :id "user_code" :value (dashed chars)}]
           (code-field chars false (contains? code-errors error))
           (button :primary "Continue" nil)])]
       [:div.sy-card-foot
        (if complete?
          "Didn't start this on your own device? Choose Cancel."
          "Only enter codes from devices you're signing in yourself, never one someone sent you.")]]))))
