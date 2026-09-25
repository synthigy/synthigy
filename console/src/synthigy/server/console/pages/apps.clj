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

(ns synthigy.server.console.pages.apps
  (:require
   [synthigy.iam.access :as access]
   [synthigy.iam.gen :as iam.gen]
   [synthigy.iam.service-user :as service-user]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.xsql.console :as cx]))

(def grant-types
  [["authorization_code" "Authorization Code"]
   ["refresh_token" "Refresh Token"]
   ["client_credentials" "Client Credentials"]
   ["urn:ietf:params:oauth:grant-type:device_code" "Device Code"]])

(def expiry-defaults
  {"access" 900 "refresh" 86400 "id" 600})

(def apis-link
  [:apis "APIs" :oauth_api :layers
   "Audiences this app may request a token for."])

(def role-link
  [:roles "Roles" :user_role :shield
   (str "What this app may do with its own credentials (client_credentials). "
        "A token carries only the scopes these roles grant on the requested API.")
   {:via [:service-user :user] :sub :description}])

(def group-link
  [:groups "Groups" :user_group :users
   "Groups this app belongs to — their roles apply too, and row rules scoped to a group scope the app."
   {:via [:service-user :user]}])

(def settings
  {:fields
   [[:redirections "Redirect URIs" :uri-list
     (str "One per line. Where an authorization_code response may "
          "send the browser back to. Also the allowlist the "
          "onboarding endpoint checks return_url against.")]
    [:logout-redirections "Logout redirect URIs" :uri-list
     (str "One per line. Where /oauth/logout may send the browser "
          "after a post_logout_redirect_uri.")]
    [:allowed-grants "Allowed grant types" :grants nil
     {:choices grant-types}]
    [:token-expiry "Token lifetime" :expiry nil
     {:defaults expiry-defaults}]
    [:login-page "Login page override" :relative-path
     (str "Relative path to a custom login page for this client, "
          "e.g. /login/acme. Blank uses the deployment default.")]
    [:logo-url "Logo URL" :text nil]
    [:allow-signup "Allow sign-up" :switch
     (str "A federated login may create a new account rather than "
          "requiring one to already be linked.")]]})

(defn client-id-chip
  [row]
  [:div.console-detail-id
   [:ty-copy {:label "Client ID" :value (:id row) :format "code" :size "xs"}]])

(defn secret-section
  "Three states, because the SAVED type and the toggled type can disagree.
   A secret on a PUBLIC client is not a weaker credential — every check in the
   token path branches on whether a secret EXISTS, not on the client's type, so
   a stray one flips the client into confidential mode and breaks device_code
   and the authorization_code exchange alike. `$appType` follows the radio;
   whether the regenerate block EXISTS at all follows the stored row, because
   the handler can only ever act on what is stored."
  [row]
  (let [confidential? (boolean (#{:confidential "confidential"} (:type row)))]
    [:div.console-field
     [:div.console-field-label "Client Secret"]
     (when confidential?
       [:div {(keyword "data-attr:hidden") "$appType !== 'confidential'"}
        [:p.hint "Stored as a one-way hash — the existing secret can "
         "never be displayed, only replaced."]
        [:ty-button {:type "button" :size "sm" :appearance "outlined"
                     :flavor "danger" :muted true
                     (keyword "data-on:click")
                     (format (str "$confirmAction = '/console/iam/apps/%s/secret/regenerate'; "
                                  "$confirmTarget = '%s'; $confirmVerb = 'Regenerate'; "
                                  "$confirmWhat = '%s'")
                             (:xid row)
                             (:xid row)
                             (str "This immediately invalidates the current client secret. "
                                  "Any app using it will fail to authenticate until updated."))}
         "Regenerate secret"]])
     ;; Saved public, toggled confidential — the button would refuse, since the
     ;; handler reads the stored row. Say so instead of showing a dead control.
     (when-not confidential?
       [:p.hint {:hidden true
                 (keyword "data-attr:hidden") "$appType !== 'confidential'"}
        "Save this app as Confidential first — a secret can only be generated "
        "once the change is stored."])
     [:p.hint {:hidden (when confidential? true)
               (keyword "data-attr:hidden") "$appType === 'confidential'"}
      "Public clients don't use a secret — they prove themselves with PKCE."]]))

(defn regenerate!
  [spec row _params]
  (if-not (#{:confidential "confidential"} (:type row))
    ;; The hidden button is an affordance; THIS is the guard. A public client
    ;; that acquires a secret stops being able to authenticate at all — see
    ;; `secret-section`.
    {:notice [:warn "Public clients don't use a secret."]}
    (let [[status secret-or-msg] (data/regenerate-secret! spec (:xid row))]
    {:notice
     (case status
       :ok [:ok [:span.console-secret-reveal
                 "New client secret — copy it now, it will not be shown again: "
                 [:code secret-or-msg]
                 (widgets/copy-button secret-or-msg)]]
       :denied [:warn "You don't have permission to change this."]
       [:warn (str secret-or-msg)])})))

(defn prepare-create
  [data]
  (cond-> (assoc data :id (iam.gen/client-id))
    (and (service-user/confidential? data)
         (empty? (get-in data [:settings "allowed-grants"])))
    (assoc-in [:settings "allowed-grants"] ["client_credentials"])))

(defn stack-service-user
  "Runs SYSTEM: creating an app must not also require a grant to write users."
  [spec {:keys [xid type]}]
  (if-not (service-user/confidential? {:type type})
    [:ok "App created."]
    (try
      (access/with-principal nil
        (service-user/sync-service-user xid))
      (let [[status secret] (data/regenerate-secret! spec xid)]
        (if (= :ok status)
          [:ok [:span.console-secret-reveal
                "App created. Client secret — copy it now, it will not be shown again: "
                [:code secret]
                (widgets/copy-button secret)]]
          [:warn "App created, but no secret could be generated. Regenerate one below."]))
      (catch clojure.lang.ExceptionInfo e
        [:warn (str "App created, but its service user could not be: "
                    (ex-message e)
                    " client_credentials will fail until that is fixed.")]))))

(defn prepare-save
  [data current]
  (when (service-user/confidential? data)
    (access/with-principal nil
      (service-user/assert-name-free!
       (service-user/service-user-name {:id (:id current) :name (:name data)})
       (get-in current [:service-user :xid]))))
  ;; never keep a secret on a public client — the token path branches on its existence
  (cond-> data
    (#{:public "public"} (:type data)) (assoc :secret nil)))

(def spec
  {:slug "apps" :entity :oauth_client :key :iam/app :label "Apps" :icon :box
   :table {:query cx/apps-table
           :watch cx/watch-apps-table
           :sortable #{"name" "active" "apis"}}
   :subtitle (str "OAuth clients registered against this deployment — every "
                  "app, service and device that can ask for a token.")
   :columns [[:name "Name" :name] [:active "Status" :status] [:type "Type" :type]
             [:apis "APIs" :agg :layers]]
   :selection-extra [:id]
   :actions {"secret/regenerate" {:reauth? true :run regenerate!}}
   :create {:prepare prepare-create
            :after stack-service-user
            :fields [[:name "Name" :text {:required true}]
                     [:type "Type" :choices
                      {:required true :keyword? true
                       :choices [["confidential" "Confidential"] ["public" "Public"]]}]
                     [:active "Active" :switch {:default true}]]
            :links [apis-link]}
   :delete {:warning (str "Any live session or token issued to this app stops "
                          "working the next time it's used — nothing revokes it "
                          "outright, it just can no longer be looked up.")}
   :detail {:prepare prepare-save
            :signals (fn [row] (str "{appType: '" (some-> (:type row) name) "'}"))
            :fields [[:name   "Name"   :text]
                     [:active "Active" :switch]
                     [:type   "Type"   :enum {:signal "appType"}]]
            :head client-id-chip
            :sections [secret-section]
            :confirm (fn [row] (str "/console/iam/apps/" (:xid row) "/secret/regenerate"))
            :links  [apis-link role-link group-link]
            :settings settings}})
