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

(ns synthigy.server.console.pages.profile
  (:require
   [synthigy.server.console.data :as data]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]))

(defn field
  [entity-key row [_ _ kind :as f]]
  (ui/field-control f row (when (= :enum kind) (data/enum-values {:key entity-key} (first f)))))

(defn render
  [request & [notice]]
  (let [user-xid (get-in request [:console/session :resource-owner])
        row      (data/get-profile user-xid)
        form     (ui/form-page
                  {:spec {}
                   :row row
                   :action "/console/profile"
                   :notice notice
                   :fields
                   (list
                    [:div.console-settings-head (icon/icon :user {:size "13"}) "Public profile"]
                    (for [f data/public-profile-fields] (field :iam/user-public-profile row f))
                    [:div.console-settings-head (icon/icon :lock {:size "13"}) "Personal info"]
                    (for [f data/person-info-fields] (field :iam/person-info row f)))
                   :actions
                   [:ty-button {:type "submit" :size "sm" :flavor "primary"} "Save"]})]
    (ui/admin-shell
     {:title "Profile"
      :user (:console/principal request)
      :uri "/console/profile"
      :session (:console/session request)
      :body
      [:div.console-page
       [:div.console-page-head
        [:div.console-eyebrow "Your account"]
        [:h1.console-title "Profile"]
        [:p.console-subtitle
         "Your public profile and personal info. These are yours to change — "
         "nothing here is shared without your say."]]
       form]})))
