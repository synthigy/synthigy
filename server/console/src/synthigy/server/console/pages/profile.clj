;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.pages.profile
  (:require
   [synthigy.server.console.data :as data]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]))

(defn unit-uri
  [nm]
  (str "/console/profile/unit/" nm))

(defn field-unit
  [row [k _ kind :as f]]
  (let [nm         (name k)
        [rel]      (data/profile-field nm)
        entity-key (if (= :person_info rel) :iam/person-info :iam/user-public-profile)]
    (ui/unit-form (unit-uri nm) nm
                  (ui/field-control f row
                                    (when (= :enum kind) (data/enum-values {:key entity-key} k))
                                    (ui/unit-status nm)))))

(defn patches
  "What the page needs after profile field `nm` was posted."
  [nm [status _ after :as outcome]]
  (let [[_ f] (data/profile-field nm)]
    (cond-> (ui/status-patches (unit-uri nm) nm outcome)
      (= :ok status) (conj [(field-unit after f) :morph]))))

(defn render
  [request]
  (let [user-xid (get-in request [:console/session :resource-owner])
        row      (data/get-profile user-xid)]
    (ui/admin-shell
     {:title "Profile"
      :user (:console/principal request)
      :uri "/console/profile"
      :session (:console/session request)
      :body
      [:div.console-page
       [:div.console-page-head
        [:div.console-eyebrow "Your account"]
        [:div.console-title-row [:h1.console-title "Profile"] (ui/saved-mark)]
        [:p.console-subtitle
         "Your public profile and personal info. These are yours to change — "
         "nothing here is shared without your say."]]
       [:div.console-panel.console-detail
        [:div.console-form
         [:ty-scroll-container {:custom-scrollbar true :shadow true}
          [:div.console-form-body
           [:div.console-fields.console-fields-wrap
            [:div.console-settings-head (icon/icon :user {:size "13"}) "Public profile"]
            (for [f data/public-profile-fields] (field-unit row f))
            [:div.console-settings-head (icon/icon :lock {:size "13"}) "Personal info"]
            (for [f data/person-info-fields] (field-unit row f))]]]]]]})))
