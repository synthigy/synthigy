;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.pages.sessions
  (:require
   [synthigy.server.console.data :as data]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.server.routes :as routes]))

(defn render
  [request]
  (let [user-xid   (get-in request [:console/session :resource-owner])
        current-id (get-in request [:console/session :id])
        rows       (data/my-sessions user-xid current-id)
        others     (count (remove :current? rows))]
    (ui/admin-shell
     {:title "Sessions"
      :user (:console/principal request)
      :uri (:uri request)
      :stream (when (routes/sse-available?) "/console/live/sessions")
      :confirm "/console/sessions/kill"
      :session (:console/session request)
      :body
      [:div.console-page
       [:div.console-page-head
        [:div.console-eyebrow "Your account"]
        [:h1.console-title "Sessions"]
        [:p.console-subtitle
         "Everywhere you're currently signed in. Revoking a session also "
         "revokes its tokens, so an app using it is signed out immediately."]
        (when (pos? others)
          [:ty-button {:type "button" :size "sm" :flavor "danger" :muted true
                       :appearance "outlined"
                       (keyword "data-on:click")
                       (format (str "$confirmTarget = 'all'; $confirmVerb = 'Sign out everywhere'; "
                                    "$confirmWhat = '%s'")
                               (str "This signs out the other " others
                                    (if (= 1 others) " session" " sessions")
                                    " and revokes their tokens. This device stays signed in."))}
           (icon/icon :log-out {:size "12" :slot "start"})
           "Sign out everywhere"])]
       (widgets/sessions-table rows)]})))
