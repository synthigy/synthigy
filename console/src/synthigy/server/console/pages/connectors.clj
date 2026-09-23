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

(ns synthigy.server.console.pages.connectors
  (:require
   [clojure.string :as str]
   [synthigy.iam.access :as access]
   [synthigy.iam.connector :as connector]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.xsql.console :as cx]))

(defn test-connector
  [xid creds]
  (try
    (access/with-principal nil
      (if-let [conn (connector/find-connector xid)]
        (let [result (connector/verify-credentials conn creds)]
          (cond-> result
            (:user result) (update :user dissoc :password)))
        {:ok false :reason :error :error "connector_not_found"}))
    (catch Exception e
      {:ok false :reason :error :error (ex-message e)})))

(defn connector-badge
  [row]
  (let [dv     (some-> (:type row) name)
        [g dl] (get widgets/type-glyphs dv)]
    (widgets/badge {:brand? false :glyph (or g :box) :label (or dl dv "—")})))

(defn dry-run-panel
  [row notice]
  (widgets/tool-panel
   {:action (str "/console/iam/connectors/" (:xid row) "/test")
    :heading "Dry-run"
    :hint (str "Runs only this connector against the credentials below and shows "
               "the raw verdict. Nothing is written, no account is created, and "
               "chain order is ignored.")
    :notice notice
    :body [:div.console-fields
           [:ty-input {:name "username" :label "Username" :size "sm"
                       :autocomplete "off"}]
           [:ty-input {:name "password" :label "Password" :type "password" :size "sm"
                       :autocomplete "new-password"}]]
    :submit "Test"}))

(defn run-test
  [_spec row params]
  (let [{:keys [username password]} params]
    (if (or (str/blank? username) (str/blank? password))
      {:panel-notice [:warn "Enter a username and a password to test."]}
      (let [{:keys [ok user reason error]}
            (test-connector (:xid row) {:username username :password password})]
        {:panel-notice
         (cond
           ok
           [:ok [:span "Authenticated. Claims: " [:code (pr-str user)]]]

           (= :unknown-user (keyword reason))
           [:warn (str "Unknown user — this connector doesn't recognise that "
                       "username. The chain would move on to the next connector.")]

           (= :invalid-credentials (keyword reason))
           [:warn (str "Invalid credentials — the connector recognised the "
                       "username and rejected the password. The chain would "
                       "stop and deny.")]

           :else
           [:warn (str "Connector error — the chain would stop and deny "
                       "(fail-closed). " (some-> error str))])}))))

(def spec
  {:slug "connectors" :entity :auth_connector :key :iam/auth-connector
   :label "Connectors" :icon :server
   :table {:query cx/connectors-table
           :watch cx/watch-connectors-table
           :sortable #{"priority" "name" "type" "enabled"}}
   :subtitle (str "The credential-verification chain for password logins, in "
                  "execution order. The first connector that recognises a "
                  "username decides the outcome — later ones never run.")
   :columns [[:name "Name" :name] [:type "Type" :type]
             [:priority "Priority" :plain] [:enabled "Status" :status]]
   :actions {"test" {:run run-test}}
   :create {:hint (str "Adds a webhook connector — created disabled, "
                       "configure the endpoint, dry-run, then enable. "
                       "\"Local database\" is seeded once by the system "
                       "and isn't created here.")
            :fields [[:name "Name" :text {:required true}]
                     [:type nil :hidden {:default "webhook"}]
                     [:priority "Priority" :number
                      {:placeholder "1000" :hint "Lower runs first."}]]}
   :delete {:warning (str "Removed from the login chain immediately; later "
                          "connectors move up to fill the gap.")
            :guard (fn [row _principal]
                     (when (= "database" (:type row))
                       (str "\"Local database\" is what makes local password "
                            "login work — deleting it would lock out every "
                            "non-federated account. It can't be removed here.")))}
   :detail {:fields [[:name "Name" :text]
                     [:priority "Priority" :number]
                     [:enabled "Enabled" :switch]]
            :panels [["Dry-run" dry-run-panel]]
            :config {:attr :config :by :type
                     :badge connector-badge
                     :heading "Connector configuration"
                     :layouts
                     {"database" []
                      "webhook"  [[:url "Webhook URL" :text
                                   (str "HTTPS endpoint that receives "
                                        "{username, password, request_id} as JSON.") nil]
                                  [:secret "Signing secret" :password
                                   (str "When set, each request is HMAC-SHA256 signed "
                                        "in the X-Synthigy-Signature header.") nil]
                                  [:timeout-ms "Timeout (ms)" :number nil
                                   {:placeholder "1500"}]]}}}})
