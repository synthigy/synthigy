(ns synthigy.oauth.page.status
  (:require
   [hiccup2.core :refer [html]]
   [synthigy.oauth.authorization-code :as ac]
   [synthigy.oauth.device-code :as dc]
   [synthigy.oauth.core :as core]))

(defn status
  [{{{:keys [value error error_description user client]} :query-params} :request
    {:keys [authorization-code device-code]} :synthigy.oauth/state}]
  (let [client-data (cond
                      authorization-code (ac/get-code-client authorization-code)
                      device-code (dc/get-code-client device-code))
        logo (get-in client-data [:settings "logo-url"] "/oauth/images/synthigy_light.png")]
    (html
     [:head
      [:meta {:charset "UTF-8"}]
      [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
      [:title "Synthigy OAuth Status"]
      [:link {:rel "icon" :href "/oauth/images/favicon.png"}]
      [:link {:rel "preconnect" :href "https://fonts.googleapis.com"}]
      [:link {:rel "preconnect" :href "https://fonts.gstatic.com" :crossorigin true}]
      [:link {:rel "stylesheet" :href "https://fonts.googleapis.com/css2?family=Montserrat:wght@200;300;400;500;600;800;900&family=Roboto&display=swap"}]
      [:link {:rel "stylesheet" :href "/oauth/css/status.css"}]]
     [:body
      [:div.oauth-container
       [:div.status-card
        [:div.header
         [:div.logo
          [:img {:src logo}]]
         [:div
          [:h2 (case value
                 "success" "Authentication Success"
                 "canceled" "Authentication Canceled"
                 "error" "Authentication Error"
                 "Wrong page")]]]
        [:div.message
         (cond
           error
           (letfn [(with-please [x] (str x "\nPlease restart authentication process"))
                   (contact-support [x] (str x "\nPlease contact application support"))]
             (case error
               "broken_flow" (with-please "Authorization flow is broken.")
               "device_code_expired" (with-please "User code that you have entered has expired.")
               "already_authorized" (with-please "Somebody already authenticated using same code")
               "ip_address" (with-please "Registered potentially malicious IP address change action.")
               "user_agent" (with-please "Registered potentially malicious user agent change action.")
               "challenge" (with-please "Registered potentially malicious challenge action.")
               "corrupt_session" (contact-support "Session id wasn't provided by access server")
               "missing_response_type" (contact-support "Client didn't specify response_type")
               "client_not_registered" (contact-support "Client is not registered")
               "missing_redirect" (contact-support "Client authorization request didn't specify response_type")
               "redirect_missmatch" (contact-support "Couldn't match requested redirect to any of configured redirects for client")
               "no_redirections" (contact-support "Client doesn't have configured redirections")
               "unsupported_grant_type" (contact-support "Grant type specified isn't supported")
               error_description))

           (and user client)
           (let [c (get @core/*clients* (java.util.UUID/fromString client))]
             [:span "Client " [:b (:name c)] " is authorized by " [:b user] " user."])

           :else nil)]]]])))

(def status-page
  {:enter
   (fn [ctx]
     (assoc ctx :response
            {:status 200
             :headers {"Content-Type" "text/html"}
             :body (str (status ctx))}))})
