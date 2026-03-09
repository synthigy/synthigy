(ns synthigy.oauth.page.login
  (:require
    [hiccup2.core :refer [html]]
    [synthigy.oauth.authorization-code :as ac]
    [synthigy.oauth.device-code :as dc]))

(def password-icon
  [:svg.ficon
   {:id "password-icon"
    :viewBox "0 0 24 24"
    :fill "currentColor"}
   [:path
    {:d "M17.7161 8.66667H16.7638V6.7619C16.7638 4.13333 14.6304 2 12.0019 2C9.37329 2 7.23996 4.13333 7.23996 6.7619V8.66667H6.28757C5.23996 8.66667 4.38281 9.52381 4.38281 10.5714V20.0952C4.38281 21.1429 5.23996 22 6.28757 22H17.7161C18.7638 22 19.6209 21.1429 19.6209 20.0952V10.5714C19.6209 9.52381 18.7638 8.66667 17.7161 8.66667ZM12.0019 17.2381C10.9542 17.2381 10.0971 16.381 10.0971 15.3333C10.0971 14.2857 10.9542 13.4286 12.0019 13.4286C13.0495 13.4286 13.9066 14.2857 13.9066 15.3333C13.9066 16.381 13.0495 17.2381 12.0019 17.2381ZM14.9542 8.66667H9.04948V6.7619C9.04948 5.13333 10.3733 3.80952 12.0019 3.80952C13.6304 3.80952 14.9542 5.13333 14.9542 6.7619V8.66667Z"}]])

(def user-icon
  [:svg.ficon
   {:id "username-icon"
    :viewBox "0 0 16 16"
    :fill "currentColor"}
   [:path
    {:d "M7.99984 8.00008C9.47317 8.00008 10.6665 6.80675 10.6665 5.33341C10.6665 3.86008 9.47317 2.66675 7.99984 2.66675C6.5265 2.66675 5.33317 3.86008 5.33317 5.33341C5.33317 6.80675 6.5265 8.00008 7.99984 8.00008ZM7.99984 9.33342C6.21984 9.33342 2.6665 10.2267 2.6665 12.0001V13.3334H13.3332V12.0001C13.3332 10.2267 9.77984 9.33342 7.99984 9.33342Z"}]])

(defn login-html
  [{error :synthigy.oauth.login/error
    {:keys [authorization-code device-code]} :synthigy.oauth.login/state}]
  (let [client (cond
                 authorization-code (ac/get-code-client authorization-code)
                 device-code (dc/get-code-client device-code))
        logo (or
               (get-in client [:settings "logo-url"])
               "/oauth/images/synthigy_light.png")]
    (html
      [:head
       [:meta {:charset "UTF-8"}]
       [:meta {:name "viewport"
               :content "width=device-width, initial-scale=1.0"}]
       [:title "Synthigy Login"]
       [:link {:rel "icon"
               :href "/oauth/images/favicon.png"}]
       [:link {:rel "preconnect"
               :href "https://fonts.googleapis.com"}]
       [:link {:rel "preconnect"
               :href "https://fonts.gstatic.com"
               :crossorigin true}]
       [:link {:rel "stylesheet"
               :href "https://fonts.googleapis.com/css2?family=Montserrat:wght@200;300;400;500;600;800;900&family=Roboto&display=swap"}]
       [:link {:rel "stylesheet"
               :href "/oauth/css/login.css"}]]
      [:body
       [:div
        [:div
         (when logo
           [:img#logo-image {:src logo}])
         (when-not logo
           [:div.name (:name client)])]
        [:form {:method "post"}
         [:div.row
          user-icon
          [:input
           {:id "username"
            :name "username"
            :placeholder "Username"
            :autoComplete "new-password"}]]
         [:div.row
          password-icon
          [:input
           {:id "password"
            :name "password"
            :type "password"
            :placeholder "Password"
            :autocomplete "new-password"
            :autocorrect "off"
            :spellcheck false}]]
         [:div.row
          [:span.error (case error
                         nil ""
                         :credentials "Wrong credentials. Check your username and password"
                         :already-authorized "User has already authorized this device"
                         "Unknown error... Contact support")]]
         [:button
          [:h4 "SUBMIT"]]]]
       [:script {:src "/oauth/js/login.js"}]])))
