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

(ns synthigy.server.console.pages.system
  (:require
   [clojure.string :as str]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.db :as db]
   [synthigy.env :as env]
   [synthigy.license :as license]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.server.routes :as routes]))

(defonce live
  (atom nil))

(defonce started-at
  (atom {}))

(defn bump!
  []
  (reset! live (lifecycle/system-report)))

(defn watch-lifecycle!
  "Push every module transition into `live` and stamp start times — idempotent, composes with the log hook."
  []
  (alter-var-root
   #'lifecycle/*on-lifecycle-event*
   (fn [f]
     (let [inner (or (::inner (meta f)) f)]
       (with-meta
         (fn [event]
           (when inner (inner event))
           (case (:phase event)
             :started (swap! started-at assoc (:topic event) (java.util.Date.))
             :stopped (swap! started-at dissoc (:topic event))
             nil)
           (bump!))
         {::inner inner})))))

(defn unwatch-lifecycle!
  []
  (alter-var-root
   #'lifecycle/*on-lifecycle-event*
   (fn [f] (or (::inner (meta f)) f))))

(defn module-param
  [topic]
  (subs (str topic) 1))

(defn parse-module
  [s]
  (let [topic (some-> s not-empty keyword)]
    (when (contains? (set (lifecycle/registered-modules)) topic)
      topic)))

(defn locked?
  [{:keys [requires-license]}]
  (boolean (and requires-license (not (license/licensed? requires-license)))))

(defn module-rows
  []
  (let [{:keys [modules]} (lifecycle/system-report)
        depth (fn depth [t]
                (let [deps (get-in modules [t :depends-on])]
                  (if (seq deps) (inc (reduce max (map depth deps))) 0)))]
    (->> modules
         (map (fn [[topic m]]
                (assoc m :topic topic :depth (depth topic)
                       :started-at (get @started-at topic))))
         (sort-by (juxt :depth (comp str :topic))))))

(defn status-tag
  [{:keys [status error missing-dependencies] :as row}]
  (cond
    (locked? row)
    [:ty-tag {:size "sm" :flavor "neutral-"}
     (icon/icon :lock {:size "11" :slot "start"}) "Requires license"]

    error
    [:ty-tag {:size "sm" :flavor "danger"}
     (icon/icon :x {:size "11" :slot "start"}) "Failed"]

    (seq missing-dependencies)
    [:ty-tag {:size "sm" :flavor "warning"}
     (icon/icon :minus {:size "11" :slot "start"}) "Missing deps"]

    (= :started status)
    [:ty-tag {:size "sm" :flavor "success"}
     (icon/icon :check {:size "11" :slot "start"}) "Running"]

    :else
    [:ty-tag {:size "sm" :flavor "neutral-"} "Stopped"]))

(defn module-actions
  [{:keys [topic status error] :as row}]
  (when (and (not (locked? row))
             (or (= :stopped status) error))
    [:ty-button {:type "button" :size "xs" :appearance "outlined" :muted true
                 :flavor "success"
                 (keyword "data-on:click")
                 (str "@post('/console/system/start?module=" (module-param topic) "')")}
     (icon/icon :check {:size "11" :slot "start"})
     "Start"]))

(defn profile-label
  []
  (cond
    (lifecycle/started? :synthigy/server) "Full"
    (lifecycle/started? :synthigy/bare-server) "Bare"
    :else "—"))

(defn backend-label
  []
  (if-let [d db/*db*]
    (str (.getSimpleName (class d))
         (when-let [n (or (:db d) (some-> (:path d) (str/split #"/") last))]
           (str " · " n)))
    "—"))

(defn uptime-label
  []
  (let [ms (.getUptime (java.lang.management.ManagementFactory/getRuntimeMXBean))
        m  (quot ms 60000)
        h  (quot m 60)
        d  (quot h 24)]
    (cond
      (pos? d) (str d "d " (rem h 24) "h")
      (pos? h) (str h "h " (rem m 60) "m")
      :else    (str (max m 1) "m"))))

(defn tile
  [icon-k label value & [warn?]]
  [:div.console-tile {:class (when warn? "warn")}
   [:div.console-tile-label (icon/icon icon-k {:size "12"}) label]
   [:div.console-tile-value value]])

(defn tiles
  []
  (let [root    env/iam-root-url
        rows    (module-rows)
        errors? (boolean (some :error rows))
        started (count (filter #(= :started (:status %)) rows))]
    [:div.console-tiles
     (tile :server "Profile" (profile-label))
     (tile :box "Database" (backend-label))
     (tile :monitor "Health" (str started " / " (count rows) " running") errors?)
     (tile :activity "Uptime" (uptime-label))
     (tile :globe "Root URL" (or root "From request headers") (nil? root))]))

(defn versions-panel
  []
  [:div.console-panel.console-versions
   [:div.console-system-head
    [:div.console-settings-head
     (icon/icon :layers {:size "13"}) "Component versions"]]
   [:ty-scroll-container {:custom-scrollbar true :shadow true}
    [:table.console-table
     [:thead [:tr [:th "Component"] [:th "Version"]]]
     [:tbody
      (for [[topic version] (sort-by (comp str key) (patch/available-versions))]
        [:tr {:id (str "version-" (str/replace (module-param topic) #"[./]" "-"))}
         [:td.name (str topic)]
         [:td.mono version]])]]]])

(defn attention-panel
  [rows]
  (when-let [bad (not-empty (filter #(or (:error %) (seq (:missing-dependencies %))) rows))]
    [:div.console-panel.console-attention
     [:div.console-system-head
      [:div.console-settings-head
       (icon/icon :x {:size "13"}) "Needs attention"]
      [:ty-button {:type "button" :size "xs" :appearance "ghost" :muted true
                   (keyword "data-on:click") "@post('/console/system/clear-errors')"}
       (icon/icon :trash-2 {:size "11" :slot "start"})
       "Clear errors"]]
     [:ty-scroll-container {:custom-scrollbar true :shadow true}
      [:table.console-table
       [:thead [:tr [:th "Module"] [:th "Status"] [:th "What happened"] [:th.num ""]]]
       [:tbody
        (for [{:keys [topic doc error] :as row} bad]
          [:tr {:id (str "attn-" (str/replace (module-param topic) #"[./]" "-"))}
           [:td.name (str topic)
            (when doc [:div.console-subtle doc])]
           [:td (status-tag row)]
           [:td (if error
                  [:span.console-subtle (widgets/ts (:timestamp error)) " — " (:message error)]
                  [:span.console-subtle
                   (str "Missing dependencies: "
                        (str/join ", " (map str (:missing-dependencies row))))])]
           [:td.num (module-actions row)]])]]]]))

(defn headline-cards
  [rows]
  (let [headline   (filter :headline rows)
        versions   (patch/available-versions)
        supporting (- (count rows) (count headline))]
    (when (seq headline)
      [:div.console-headline
       [:div.console-headline-cards
        (for [{:keys [topic doc started-at] :as row} headline]
          [:div.console-headline-card
           {:id (str "headline-" (str/replace (module-param topic) #"[./]" "-"))}
           [:div.console-headline-top
            [:span.console-headline-name (str topic)]
            (status-tag row)]
           (when doc [:div.console-headline-doc doc])
           [:div.console-headline-meta
            (when-let [v (get versions topic)] [:span.mono (str "v" v)])
            (when started-at [:span.console-subtle (str "since " (widgets/ts started-at))])]])]
       [:a.console-topo-link {:href "/console/system/topology"}
        (str supporting " supporting modules — view the full topology")]])))

(defn panel
  []
  (let [rows (module-rows)]
    [:div#system-live.console-live-region
     (attention-panel rows)
     (headline-cards rows)]))

(defn render
  [request]
  (ui/admin-shell
   {:title "System"
    :user (:console/principal request)
    :uri (:uri request)
    :stream (when (routes/sse-available?) "/console/live/system")
    :session (:console/session request)
    :search :none
    :body
    [:div.console-page.console-page-scroll
     [:div.console-page-head
      [:div.console-eyebrow "System"]
      [:h1.console-title "Overview"]
      [:p.console-subtitle
       (str "What this deployment is and whether it is healthy. Anything "
            "that needs a hand shows up below the facts; the full module "
            "graph lives on the Topology page.")]]
     (tiles)
     (panel)
     (versions-panel)]}))
