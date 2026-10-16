;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.pages.users
  (:require
   [ring.util.codec :as codec]
   [synthigy.embedded :as embedded]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.log :as log]
   [synthigy.oauth.core :as oauth.core]
   [synthigy.oauth.onboarding :as onboarding]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.xsql.console :as cx]))

(def role-link
  [:roles "Roles" :user_role :shield
   "Roles the account holds directly. Group-inherited roles are not shown here — edit the group to change those."
   {:default "Internal Member" :sub :description}])

(def group-link
  [:groups "Groups" :user_group :users
   "Groups the account belongs to. A group's roles apply on top of the ones above."])

(defn sessions-panel
  [row _panel-notice]
  (widgets/sessions-table (data/my-sessions (:xid row) nil) true))

(defn identities-panel
  [row _panel-notice]
  (widgets/identities-table (data/my-identities (:xid row)) nil true))

(defn profile-panel
  [row _panel-notice]
  (let [profile (data/get-profile (:xid row))]
    [:ty-scroll-container {:custom-scrollbar true :shadow true}
     (widgets/profile-fields "Public profile" :user data/public-profile-fields profile)
     (widgets/profile-fields "Personal info" :lock data/person-info-fields profile)]))

(defn managed-by-app
  [row]
  (when-let [{:keys [xid name]} (:authorized-client row)]
    [:span "Managed by app "
     [:a {:href (str "/console/iam/apps/" xid)} name]
     " — edit its roles and groups there. It is active only while the app is "
     "Confidential and allows Client Credentials."]))

(def claim-ttl-ms
  (* 24 60 60 1000))

(defn link-apps
  "Apps the caller can see that declare an App URL — the places a sign-in link can return to."
  []
  (->> (try (embedded/search :oauth_client {} [:id :name :settings])
            (catch clojure.lang.ExceptionInfo _ nil))
       (filter #(onboarding/app-url %))
       (sort-by :name)))

(defn claim-section
  [row]
  (when-not (:authorized-client row)
    [:div.console-field {:data-signals "{claimApp: ''}"}
     [:div.console-field-label "Sign-in link"]
     [:p.console-field-hint
      "A one-time link, valid 24 hours, where this person sets their own password or "
      "connects a sign-in provider. Nothing is sent — copy it to them. A new link "
      "cancels the previous one. With an app picked, they land in it when done."]
     [:div.console-claim-row
      (when-let [apps (seq (link-apps))]
        [:ty-select {:size "sm" :placeholder "No app — end on the status page"
                     (keyword "data-on:change") "$claimApp = evt.detail.value || ''"}
         (for [{:keys [id name]} apps]
           [:ty-option {:value id} name])])
      [:ty-button {:type "button" :size "sm" :appearance "outlined"
                   (keyword "data-on:click")
                   (format (str "$confirmAction = '/console/iam/users/%s/claim-link'; "
                                "$confirmTarget = '%s'; $confirmVerb = 'Generate'; "
                                "$confirmWhat = '%s'")
                           (:xid row) (:xid row)
                           (str "Whoever holds the link can set the password of this "
                                "account. Any earlier link stops working."))}
       (icon/icon :link {:size "12" :slot "start"})
       "Generate sign-in link"]]]))

(defn claim-link!
  [_spec row params]
  (let [user   (iam.context/get-user-details {:xid (:xid row)})
        app-id (not-empty (str (:claimApp params)))
        client (some-> app-id oauth.core/get-client)
        back   (some-> client onboarding/app-url)]
    {:notice
     (cond
       (:authorized-client row)
       [:warn "This account is managed by its app and signs in with the app's secret."]

       (nil? user)
       [:warn "Could not load this account."]

       (not (onboarding/administers? access/*principal* user))
       [:warn "You don't have permission to change this."]

       (and app-id (nil? back))
       [:warn "That app has no App URL. Set one on the app, or generate the link without an app."]

       :else
       (let [{:keys [token exp]} (onboarding/issue-onboard-token!
                                  user claim-ttl-ms nil (:id client) back)
             url (str (oauth.core/domain+ "/oauth/claim?") (codec/form-encode {:token token}))]
         (log/info {:id ::claim-link-issued
                    :data {:action :created :subject :onboarding-user
                           :user (:name user) :client (:id client)
                           :principal (:name access/*principal*)}}
                   "Console issued onboarding claim link")
         [:ok [:span.console-secret-reveal
               (str "Sign-in link for " (:name user)
                    (when client (str " (returns to " (:name client) ")"))
                    ", valid until " (widgets/ts (java.util.Date. (long exp))) " — copy it now: ")
               [:code url]
               (widgets/copy-button url)]]))}))

(def spec
  {:slug "users" :entity :user :key :iam/user :label "Users" :icon :user
   :watch [:oauth_session]
   :table {:query cx/users-table
           :watch cx/watch-users-table
           :chips {:groups "groups" :roles "roles"}
           :sortable #{"name" "active" "sessions" "groups" "roles"}}
   :filters [[:groups "Groups" :user_group :users]
             [:roles "Roles" :user_role :shield]]
   :create {:fields [[:name "Name" :text {:required true}]
                     [:type "Type" :choices
                      {:required true :keyword? true
                       :choices [["PERSON" "Person"] ["SERVICE" "Service"] ["ROBOT" "Robot"]]}]
                     [:active "Active" :switch {:default true}]]
            :links [role-link group-link]}
   :delete {:warning (str "Their sessions and sign-in methods stay in the database "
                          "but become unreachable — not a full cleanup. This can't "
                          "be undone.")
            :guard (fn [row principal]
                     (when (= (:xid row) (:xid principal))
                       "You can't delete your own account from here."))}
   :actions {"claim-link" {:reauth? true :run claim-link!}}
   :subtitle (str "Every account this deployment knows about. The list is "
                  "governed by your own IAM grants — filtered server-side, not hidden.")
   :columns [[:name "Name" :name] [:active "Status" :status] [:type "Type" :type]
             [:roles "Roles" :agg :shield] [:groups "Groups" :agg :users]
             [:sessions "Sessions" :agg :monitor]]
   :selection-extra [{:authorized-client [{:args {:_join :left} :selections [:xid :name]}]}]
   :detail {:locked managed-by-app
            :fields [[:name   "Name"   :text]
                     [:active "Active" :switch]
                     [:type   "Type"   :enum]]
            :links  [role-link group-link]
            :sections [claim-section]
            :panels [["Profile" profile-panel]
                     ["Sessions" sessions-panel]
                     ["Sign-in methods" identities-panel]]}})
