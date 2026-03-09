(ns synthigy.oauth.page.device
  (:require
   [hiccup2.core :refer [html raw]]))

(defn authorize
  ([] (authorize nil))
  ([{challenge :synthigy.oauth.device-code/challenge
     error :synthigy.oauth.device-code/error
     user-code :synthigy.oauth.device-code/user-code
     complete? :synthigy.oauth.device-code/complete?}]
   (html
    [:head
     [:meta {:charset "UTF-8"}]
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
     [:title "Synthigy Code Check"]
     [:link {:rel "icon" :href "/oauth/images/favicon.png"}]
     [:link {:rel "preconnect" :href "https://fonts.googleapis.com"}]
     [:link {:rel "preconnect" :href "https://fonts.gstatic.com" :crossorigin true}]
     [:link {:rel "stylesheet" :href "https://fonts.googleapis.com/css2?family=Montserrat:wght@200;300;400;500;600;800;900&family=Roboto&display=swap"}]
     [:link {:rel "stylesheet" :href "/oauth/css/device.css"}]]
    [:body
     [:div.oauth-container
      [:div.device-card
       [:div.inner
        [:div.header
         [:div.logo
          [:img {:src "/oauth/images/synthigy_light.png"}]]
         [:div
          [:h1 "Device Confirmation"]]]
        [:div.description
         (if complete?
           "Confirm that following user code is used for device authorization"
           "Type in device code displayed on your device")]
        [:form.device-form {:method "POST"}
         (when complete?
           [:input {:type "hidden"
                    :name "challenge"
                    :value challenge}])
         [:div.input-wrapper
          [:input (cond->
                   {:id "user_code"
                    :type "text"
                    :placeholder "ABCD-WXYZ"
                    :required true
                    :autocomplete "off"
                    :autocapitalize "off"
                    :spellcheck false}
                    complete? (assoc :value user-code
                                     :read-only true
                                     :disabled true)
                    (not complete?) (assoc :name "user_code"))]]
         [:div.error
          (when error
            [:div.message
             (case error
               :expired "This user code has expired"
               :malicous-code "Entered user code doesn't exist"
               :malicous-ip "You are trying to enter code from wrong host"
               :malicious-user-agent "You are trying to enter code from wrong app"
               error)])]
         [:div.actions
          (if complete?
            [:div
             [:button.confirm {:type "submit" :name "action" :value "confirm"} "Confirm"]
             [:button.cancel {:type "submit" :name "action" :value "cancel"} "Cancel"]]
            [:div
             [:button.continue {:type "submit"} "Continue"]])]]]]]
     [:script {:src "/oauth/js/login.js"}]
     [:script
      (raw "window.onload = function () { document.getElementById(\"user_code\").focus() }")]])))
