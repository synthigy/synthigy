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

(ns synthigy.server.console.widgets
  (:require
   [clojure.string :as str]
   [synthigy.server.console.icon :as icon]))

(def type-glyphs
  {"PERSON"       [:user   "Person"]
   "SERVICE"      [:server "Service"]
   "SECRET"       [:lock   "Secret"]
   "ROBOT"        [:bot    "Robot"]
   "OAUTH_CLIENT" [:box    "OAuth client"]
   "ACCESS"       [:shield "Access"]
   "ROBOTICS"     [:bot    "Robotics"]
   "public"       [:globe  "Public"]
   "confidential" [:lock   "Confidential"]
   "GOOGLE"       [:globe  "Google"]
   "MICROSOFT"    [:globe  "Microsoft"]
   "GITHUB"       [:globe  "GitHub"]
   "FACEBOOK"     [:globe  "Facebook"]
   "LINKEDIN"     [:globe  "LinkedIn"]
   "DISCORD"      [:globe  "Discord"]
   "OIDC_1"       [:globe  "Custom OIDC 1"]
   "OIDC_2"       [:globe  "Custom OIDC 2"]
   "OIDC_3"       [:globe  "Custom OIDC 3"]
   "database"     [:server "Database"]
   "webhook"      [:link   "Webhook"]})

(defn display-name
  [{:keys [name xid]}]
  (or (not-empty name) (str "unnamed · " xid)))

(defn badge
  [{:keys [brand? glyph label size]}]
  [:div {:class (str "console-badge-row" (when (= size :sm) " sm"))}
   [:div {:class (str "console-badge-swatch " (if brand? "brand" "plain"))}
    (if brand? glyph (icon/icon glyph {:size (if (= size :sm) "11" "13")}))]
   [:div.console-badge-name (or label "—")]])

(defn empty-state
  [icon-k heading detail]
  [:div.console-empty
   (icon/icon icon-k {:size "28"})
   [:h2 heading]
   [:p detail]])

(def ^:private ts-format
  (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm"))

(defn ts
  [d]
  (cond
    (nil? d) nil
    ;; SQLite hands timestamps back as strings — see
    ;; docs/core/synthigy/dataset/timestamp notes; never cast blindly.
    (string? d) (-> ^String d (str/replace "T" " ") (subs 0 (min 16 (count ^String d))))
    (instance? java.util.Date d)
    (.format (java.time.LocalDateTime/ofInstant (.toInstant ^java.util.Date d)
                                                (java.time.ZoneId/systemDefault))
             ts-format)
    (instance? java.time.Instant d)
    (.format (java.time.LocalDateTime/ofInstant ^java.time.Instant d
                                                (java.time.ZoneId/systemDefault))
             ts-format)
    :else (str d)))

(defn copy-button
  [value]
  [:ty-button {:type "button" :action true :pill true :appearance "ghost"
               :size "xs" :title "Copy" :data-v value
               :onclick "navigator.clipboard.writeText(this.dataset.v)"}
   (icon/icon :copy {:size "13"})])

(defn notice
  [[kind msg] & [rounded?]]
  (when kind
    [:div {:class (str "console-notice " (when rounded? "console-rounded-notice ")
                       (name kind))}
     msg]))

(defn tool-panel
  [{:keys [action heading hint body submit] n :notice}]
  [:div.console-panel.console-detail.console-tool-panel
   [:form.console-form {:method "post" :action action}
    [:div.console-form-body
     [:div.console-settings-head (icon/icon :activity {:size "13"}) heading]
     (when hint [:p.console-field-hint hint])
     (notice n true)
     body]
    [:div.console-form-actions
     [:ty-button {:type "submit" :size "sm" :appearance "outlined"}
      (icon/icon :activity {:size "12" :slot "start"})
      submit]]]])

(defn session-origin
  [{:keys [context client]}]
  (cond
    (= "console" (get context "flow")) "Synthigy Console"
    (:name client) (:name client)
    :else "OAuth"))

(def ^:private flow-labels
  {"device_code"        "Device"
   "authorization_code" "Web"})

(def ^:private ua-browsers
  ;; order matters — Edge/Opera/Chrome all carry "Chrome"
  [[#"Edg[e/]" "Edge"] [#"OPR/|Opera" "Opera"] [#"Chrome/" "Chrome"]
   [#"Firefox/" "Firefox"] [#"Safari/" "Safari"] [#"curl/" "curl"]])

(def ^:private ua-systems
  [[#"Windows" "Windows"] [#"iPhone|iPad" "iOS"] [#"Macintosh|Mac OS X" "macOS"]
   [#"Android" "Android"] [#"Linux" "Linux"]])

(defn device-label
  "User-agent → \"Chrome on macOS\"; unrecognised agents keep a truncated raw string."
  [ua]
  (let [pick (fn [table] (some (fn [[re label]] (when (re-find re ua) label)) table))]
    (if (str/blank? ua)
      "Unknown device"
      (let [browser (pick ua-browsers)
            system  (pick ua-systems)]
        (cond
          (and browser system) (str browser " on " system)
          browser              browser
          system               system
          :else                (subs ua 0 (min 40 (count ua))))))))

(defn sessions-table
  "readonly? also drops the #sessions-panel id — set when embedded (possibly
  more than once, see ui/detail's wide/desktop/narrow layout variants)
  rather than rendered standalone on its own live-patched self-service page,
  where the id would collide across copies."
  [rows & [readonly?]]
  [(if readonly? :div.console-panel :div#sessions-panel.console-panel)
   (if (empty? rows)
     (empty-state :monitor "No active sessions"
                  "Nothing signed in right now — which shouldn't be possible while you're reading this.")
     [:ty-scroll-container {:custom-scrollbar true :shadow true}
      [:table.console-table
       [:thead [:tr [:th "Signed in with"] [:th "Device"]
                [:th "Last seen"] [:th "Method"]
                (when-not readonly? [:th.num ""])]]
       [:tbody
        (for [{:keys [xid started last-seen context current?] :as row} rows]
          [:tr {:id (str "session-" xid)}
           [:td.name {:tabindex "0"}
            (session-origin row)
            (when started
              [:ty-tooltip {:placement "top"} "Started " (ts started)])
            (when-let [flow (flow-labels (get context "flow"))]
              [:ty-tag {:size "xs" :flavor (if (= "Device" flow) "warning" "neutral")
                        :style "margin-left:8px"}
               (icon/icon (if (= "Device" flow) :tv :globe) {:size "11" :slot "start"})
               flow])
            (when current?
              [:ty-tag {:size "xs" :flavor "primary" :pill true
                        :style "margin-left:8px"}
               (icon/icon :monitor {:size "11" :slot "start"})
               "this device"])]
           [:td (device-label (get context "agent"))
            (when-let [ip (get context "ip")]
              [:div.console-subtle ip])]
           [:td.mono (or (ts last-seen) "—")]
           [:td (or (some-> (get context "amr") first) "—")]
           (when-not readonly?
             [:td.num
              [:ty-button {:type "button" :size "xs" :flavor "danger" :muted true
                           :appearance "outlined"
                           (keyword "data-on:click")
                           (format "$confirmTarget = '%s'; $confirmVerb = '%s'; $confirmWhat = '%s'"
                                   xid
                                   (if current? "Sign out" "Revoke")
                                   (if current?
                                     "You will be signed out of this device."
                                     (str "That session will be signed out immediately, "
                                          "along with any tokens it issued.")))}
               (if current? "Sign out" "Revoke")]])])]]])])

(defn identities-table
  "readonly? also drops the #identities-panel id — see sessions-table."
  [rows & [n readonly?]]
  [(if readonly? :div.console-panel :div#identities-panel.console-panel)
   (notice n)
   (if (empty? rows)
     (empty-state :link "No linked sign-in methods"
                  (str "You sign in with a username and password. Linking a "
                       "provider lets you sign in with it instead."))
     [:ty-scroll-container {:custom-scrollbar true :shadow true}
      [:table.console-table
       [:thead [:tr [:th "Provider"] [:th "Account"] [:th "Linked"]
                (when-not readonly? [:th.num ""])]]
       [:tbody
        (for [{:keys [xid provider email linked-at]} rows]
          [:tr {:id (str "identity-" xid)}
           [:td.name [:ty-tag {:size "sm" :flavor "neutral"}
                      (icon/icon :link {:size "11" :slot "start"})
                      (some-> provider name str/capitalize)]]
           [:td email]
           [:td.mono (or (ts linked-at) "—")]
           (when-not readonly?
             [:td.num
              [:ty-button {:type "button" :size "xs" :flavor "danger" :muted true
                           :appearance "outlined"
                           (keyword "data-on:click")
                           (format "$confirmTarget = '%s'; $confirmVerb = 'Unlink'; $confirmWhat = '%s'"
                                   xid
                                   (str "You will no longer be able to sign in with "
                                        (some-> provider name str/capitalize) "."))}
               "Unlink"]])])]]])])

(defn profile-fields
  [heading icon-k fields row]
  [:div
   [:div.console-settings-head (icon/icon icon-k {:size "13"}) heading]
   [:table.console-table
    [:tbody
     (for [[k label] fields]
       [:tr [:td.name label] [:td (let [v (get row k)] (if (str/blank? (str v)) "—" v))]])]]])
