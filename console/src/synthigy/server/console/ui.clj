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

(ns synthigy.server.console.ui
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [hiccup2.core :refer [html raw]]
   [ring.util.codec :as codec]
   [synthigy.env :as env]
   [synthigy.json :as json]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.pages :as pages]
   [synthigy.server.console.session :as session]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.server.routes :as routes]))

(def ^:private search-oninput
  (str "var q=this.value.toLowerCase();"
       "document.querySelectorAll('.console-table tbody tr').forEach(function(r){"
       "r.hidden = q && r.textContent.toLowerCase().indexOf(q)===-1;});"))

(defn rows-uri [slug] (str "/console/iam/" slug "/rows"))

(defn browse-params-expr
  [{:keys [filters]}]
  (str " + '&q=' + encodeURIComponent($browseQuery)"
       " + '&sort=' + $browseSort + '&dir=' + $browseDir"
       (apply str
              (for [[k] filters]
                (str " + '&" (name k) "=' + $flt_" (name k) ".join(',')")))))

(defn live-rows-expr
  [{:keys [slug] :as spec}]
  (str "'/console/live/iam/" slug "/rows?limit=' + $browseOffset" (browse-params-expr spec)))

(defn refresh-expr
  "Re-fetch the browse table honoring the current query/sort/filter signals."
  [{:keys [slug] :as spec}]
  (if (routes/sse-available?)
    (str "@get(" (live-rows-expr spec) ")")
    (str "@get('" (rows-uri slug) "?offset=0'" (browse-params-expr spec) ")")))

(defn server-search
  [{:as spec}]
  {:data-bind "browseQuery"
   (keyword "data-on:input__debounce.300ms")
   (refresh-expr spec)})

(def ^:private search-hotkey
  (str "document.addEventListener('keydown',function(e){"
       "if((e.metaKey||e.ctrlKey)&&e.key==='k'){"
       "var i=document.getElementById('console-search');"
       "if(i){e.preventDefault();i.focus();i.select();}}});"))

(def ^:private live-indicator-script
  ;; ponytail: counts ALL datastar fetches, not just streams — a lone @post
  ;; can read Live for its duration. Discriminate on detail.el if that shows.
  (str "(function(){var n=0;"
       "document.addEventListener('datastar-fetch',function(e){"
       "var el=document.querySelector('.console-live');if(!el)return;"
       "var t=e.detail&&e.detail.type;"
       "if(t==='started'){n++;}"
       "else if(t==='finished'){n=Math.max(0,n-1);}"
       "else{return;}"
       "var on=n>0;"
       "el.classList.toggle('connected',on);"
       "var l=el.querySelector('.console-live-label');"
       "if(l)l.textContent=on?'Live':'Offline';"
       "});})();"))

(defn asset
  [path]
  (let [ts (try (some-> (io/resource (str "synthigy/console/" path))
                        .openConnection
                        .getLastModified)
                (catch Exception _ nil))]
    (str "/console/assets/" path (when (and ts (pos? ts)) (str "?v=" ts)))))

(defn head [title]
  [:head
   [:meta {:charset "utf-8"}]
   [:meta {:name "viewport" :content "width=device-width, initial-scale=1"}]
   [:title (str title " · Synthigy Console")]
   [:link {:rel "icon" :type "image/svg+xml" :href (asset "img/favicon.svg")}]
   [:link {:rel "stylesheet" :href (asset "css/tyrell.css")}]
   [:link {:rel "stylesheet" :href (asset "css/tyrell-brand.css")}]
   [:link {:rel "stylesheet" :href (asset "css/tokens.css")}]
   [:link {:rel "stylesheet" :href (asset "css/console.css")}]
   [:script {:src (asset "js/tyrell.js")}]
   [:script {:type "module" :src (asset "js/datastar.js")}]])

(defn lockup
  ([] (lockup true))
  ([verbal?]
   [:span.sy-lockup
    [:span.sy-mark (raw (str assets/figurative))]
    (when verbal? [:span.sy-verbal (raw (str assets/verbal))])]))

(defn initials
  [nm]
  (let [parts (remove str/blank? (str/split (or nm "?") #"[.\s_-]+"))]
    (->> (take 2 parts) (map (comp str/upper-case str first)) str/join)))

(defn render
  [hiccup]
  (str (html hiccup)))

(defn bare-shell
  [{:keys [title body]}]
  (render
   [:html {:class "dark" :data-theme "dark" :lang "en"}
    (head title)
    [:body
     body
     [:script {:src (asset "js/starfield.js") :defer true}]]]))

(defn error-page
  "Shell for an unhandled failure — says what happened, offers a way out."
  [message]
  (bare-shell
   {:title "Error"
    :body [:div.console-login
           (lockup)
           [:div.console-login-hint "console"]
           [:div.console-login-error message]
           [:a.console-back {:href "/console"} "← Back to console"]]}))

(defn nav
  []
  (let [admin (filterv data/visible? (pages/all))]
    (cond-> [{:title "Your account"
              :items [{:href "/console/profile" :label "Profile" :icon :user}
                      {:href "/console/sessions" :label "Sessions" :icon :monitor}
                      {:href "/console/identities" :label "Sign-in methods" :icon :link}]}]
      (seq admin)
      (conj {:title "Administration"
             :items (for [{:keys [slug label icon]} admin]
                      {:href (str "/console/iam/" slug) :label label :icon icon})})

      (data/system-operator?)
      (conj {:title "System"
             :items [{:href "/console/system" :label "Overview" :icon :server}
                     {:href "/console/system/topology" :label "Topology" :icon :link}
                     {:href "/console/encryption" :label "Encryption" :icon :key}]}))))

(def tools
  [{:key "model" :label "Data modeling" :icon :layers}
   {:key "data" :label "Data console" :icon :database}
   {:key "logs" :label "Log cockpit" :icon :microscope}])

(defn tools-redirect-uri
  "The ONE address the Synthigy Tools client has registered for this
   deployment, derived from the configured root URL — the portal registers
   exactly this string when that URL is set."
  []
  (when-let [root (not-empty (str env/iam-root-url))]
    (str (str/replace root #"/+$" "") "/console/tools/callback")))

(defn tools-available?
  "The web components ship only in the product bundles, and their login can
   only come back to a registered address — no bundle or no root URL, no Tools
   nav, rather than a nav that dead-ends on redirect_missmatch."
  []
  (and (some? (io/resource "synthigy/console/js/tooling.js"))
       (some? (tools-redirect-uri))))

(defn tools-callback-page
  "The registered redirect address. Deliberately bare: a silent-renew iframe
   loads it on every token refresh, and it never needs the component bundle."
  []
  (render
   [:html {:lang "en"}
    [:head
     [:meta {:charset "utf-8"}]
     [:title "Signing in · Synthigy"]]
    [:body
     [:script {:src (asset "js/tools.js")}]]]))

(defn sidebar [uri user]
  [:aside.console-sidebar
   [:div.console-sidebar-brand (lockup)]
   [:nav.console-nav
    (for [{:keys [title items]} (nav)]
      (list
       [:div.console-nav-group title]
       (for [{:keys [href label icon]} items]
         [:a {:href href :class (when (= uri href) "active")}
          (icon/icon icon {:size "16"})
          [:span label]])))
    (when (tools-available?)
      (list
       [:div.console-nav-group "Tools"]
       (for [{:keys [key label icon]} tools]
         [:button {:type "button" :data-syn-tool key}
          (icon/icon icon {:size "16"})
          [:span label]])))]
   (when user
     [:div.console-user
      [:div.console-avatar (initials (:name user))]
      [:div.console-user-meta
       [:div.console-user-name (:name user)]
       [:div.console-user-sub
        (let [n (count (:roles user))]
          (if (pos? n)
            (str n " role" (when (not= 1 n) "s"))
            "Signed in"))]]
      [:form {:method "post" :action "/console/logout"}
       [:ty-button {:type "submit" :size "sm" :appearance "ghost" :action true}
        (icon/icon :log-out {:size "15"})
        [:ty-tooltip {:placement "top"} "Sign out"]]]])])

(defn topbar
  [crumb stream? search nav-filters]
  [:header.console-topbar
   [:div.console-crumb
    [:span.root "Synthigy"]
    [:span.sep (icon/icon :chevron-right {:size "13"})]
    [:span.here crumb]]
   [:div.console-topbar-spacer]
   nav-filters
   (when stream?
     [:ty-tag.console-live
      {:pill true :size "sm" :flavor "neutral-"
       :title "Updates stream in as data changes"}
      [:span.console-live-dot {:slot "start"}]
      [:span.console-live-label "Offline"]])
   ;; :none — a page with nothing to filter (the cards index). The fallback
   ;; below is a DOM filter over `.console-table`, so it would render a box
   ;; that silently does nothing.
   (when-not (= :none search)
     [:div.console-search
      [:ty-input (merge {:id "console-search" :type "search" :placeholder "Filter…"
                         :size "sm" :autocomplete "off"}
                        (or search {:oninput search-oninput}))
       (icon/icon :search {:size "14" :slot "start"})
       [:span.console-kbd {:slot "end"} "⌘K"]]])])

(defn on-enter
  "Enter in a field runs `expr` — ty-input keeps its native input in the shadow
   DOM, so a form never gets implicit submission and a dialog never gets a
   default button."
  [expr]
  {(keyword "data-on:keydown")
   (str "if (evt.key === 'Enter') { evt.preventDefault(); " expr " }")})

(defn confirm-dialog
  "One dialog per page; each opener carries its OWN post target in
   `$confirmAction`, because a page can host more than one destructive action
   and a single baked-in URI silently sends whichever loses the cond to the
   winner's endpoint (Regenerate secret posted to /delete and destroyed the
   app). `action` is the fallback for openers that set no target of their own."
  [action stale?]
  [:ty-modal {(keyword "data-attr:open") "$confirmTarget !== ''"
              :backdrop "true" :close-on-outside-click true :close-on-escape true
              (keyword "data-on:close") "$confirmTarget = ''; $confirmAction = ''"}
   [:div.console-confirm
    [:h2 {:data-text "$confirmVerb"} "Confirm"]
    [:p {:data-text "$confirmWhat"}]
    (when stale?
      [:div.console-confirm-reauth
       [:p.hint "You signed in a while ago. Confirm your password to continue."]
       [:ty-input (merge {:type "password" :label "Password" :size "sm"
                          :autocomplete "current-password"
                          :data-bind "confirmPassword"}
                         (on-enter (str "@post($confirmAction || '" action "'); "
                                           "$confirmTarget = ''")))]])
    [:div.console-confirm-actions
     [:ty-button
      {:type "button"
       :size "sm"
       :appearance "ghost"
       "data-on:click" "$confirmTarget = ''"}
      "Cancel"]
     [:ty-button {:type "button" :size "sm" :flavor "danger"
                  "data-on:click" (str "@post($confirmAction || '" action "'); "
                                       "$confirmTarget = ''")}
      [:span {:data-text "$confirmVerb"} "Confirm"]]]]])

(defn admin-shell
  [{:keys [title user uri body stream stream-expr confirm session paged search
           nav-filters filter-keys]}]
  (let [stream-src (cond
                     stream-expr stream-expr
                     stream (str "'" stream "'"))]
    (render
      ;; :data-signals sits on <html>, not <body> — hiccup2 sorts an
      ;; element's own attributes alphabetically, so "data-init"/
      ;; "data-on-interval" always precede "data-signals" in the rendered
      ;; tag regardless of the map's build order, and Datastar applies a
      ;; single element's own directives in that (attribute-source) order.
      ;; A same-element data-init would therefore read its signals before
      ;; data-signals ever ran. <html> is a genuine ANCESTOR of <body>, so
      ;; its directives are guaranteed to apply first regardless of
      ;; alphabetization within either tag.
      [:html {:class "dark" :data-theme "dark" :lang "en"
              :data-signals
              (str "{confirmTarget: '', confirmVerb: '', confirmWhat: '', confirmPassword: ''"
                   ", confirmAction: ''"
                   (when paged
                     (format (str ", browseOffset: %d, browseMore: true, browseQuery: '', "
                                  "browseSort: '', browseDir: ''") paged))
                   (apply str (for [k filter-keys] (str ", flt_" (name k) ": []")))
                   "}")}
       (head title)
       [:body (cond-> {}
                stream-src (assoc :data-init (str "@get(" stream-src ")")
                                  :data-on-interval__duration.60s
                                  (str "@get(" stream-src ")")))
        [:div.sy-stars]
        [:div.console-shell
         (sidebar uri user)
         (topbar title (boolean stream-src) search nav-filters)
         [:main.console-content body]]
        (when confirm (confirm-dialog confirm (not (session/fresh? session))))
        [:script (raw search-hotkey)]
        (when stream-src [:script (raw live-indicator-script)])
        (when (tools-available?)
          (list
           [:script (raw (str "window.SYNTHIGY_REDIRECT_URI="
                              (json/write-str (tools-redirect-uri)) ";"
                              "window.SYNTHIGY_TOOLING_BUNDLE="
                              (json/write-str (asset "js/tooling.js")) ";"))]
           [:script {:src (asset "js/tools.js") :defer true}]))]])))

(defn cell
  [row [k _ kind ic] & [edit-href]]
  (let [v (get row k)]
    (case kind
      :name   (let [v (if (= :name k) (widgets/display-name row) v)]
                [:td.name (if edit-href [:a.console-row-link {:href edit-href} v] v)])
      :mono   [:td.mono (or v "—")]
      :status [:td [:ty-tag {:size "sm" :flavor (if v "success" "neutral-")}
                    (icon/icon (if v :check :minus) {:size "12" :slot "start"})
                    (if v "Active" "Inactive")]]
      :type   (let [n (some-> v name)
                    [g label] (get widgets/type-glyphs n)]
                [:td (cond
                       g   [:ty-tag {:size "sm" :flavor "neutral-"}
                            (icon/icon g {:size "11" :slot "start"}) label]
                       n   [:ty-tag {:size "sm" :flavor "neutral-"} n]
                       :else "—")])
      :agg    [:td.num [:ty-tag {:size "xs" :flavor "neutral-" :pill true}
                        (when ic (icon/icon ic {:size "11" :slot "start"}))
                        (or v 0)]]
      :brand  (let [n   (some-> v name)
                    lbl (or (second (get widgets/type-glyphs n)) n)]
                [:td (widgets/badge
                      {:brand? true
                       :glyph (get assets/brand-icons (some-> n str/lower-case keyword)
                                   assets/generic-brand-icon)
                       :label lbl :size :sm})])
      [:td (if (some? v) (str v) "—")])))

(defn select-cell
  [xid]
  [:td.sel
   [:ty-checkbox
    {(keyword "data-attr:checked") (format "$exportSel.includes('%s')" xid)
     (keyword "data-on:change")
     (format (str "$exportSel = evt.detail.checked "
                  "? ($exportSel.includes('%s') ? $exportSel : $exportSel.concat(['%s'])) "
                  ": $exportSel.filter(function(v){return v !== '%s'})")
             xid xid xid)}]])

(defn browse-rows-fragment
  [{:keys [slug columns detail]} rows & [sel?]]
  (if (empty? rows)
    [:tr.console-no-match
     [:td {:colspan (cond-> (count columns) sel? inc)} "No matches."]]
    (for [row rows]
      [:tr {:id (str slug "-" (:xid row))}
       (when sel? (select-cell (:xid row)))
       (for [col columns]
         (cell row col (when detail (str "/console/iam/" slug "/" (:xid row)))))])))

(defn browse-table
  [{:keys [slug label icon columns] :as spec} rows & [more? sel?]]
  [:div.console-panel {:id (str slug "-panel")}
   (if (empty? rows)
     (widgets/empty-state icon (str "No " (str/lower-case label) " visible")
                          (str "This account has no IAM grant that exposes the "
                               (str/lower-case label) " table. Ask an administrator "
                               "to grant one."))
     [:ty-scroll-container
      (cond-> {:custom-scrollbar true :shadow true}
        more? (assoc (keyword "data-on:nearend")
                     (str "$browseMore && @get('" (rows-uri slug)
                          "?offset=' + $browseOffset"
                          (browse-params-expr spec) ")")))
      [:table.console-table
       (when sel?
         {(keyword "data-attr:class")
          "'console-table' + ($exportMode ? ' selecting' : '')"})
       [:thead [:tr
                (when sel? [:th.sel ""])
                (for [[k header kind] columns]
                  (let [cls (case kind
                              :agg "num"
                              :status "status-col"
                              nil)
                        sortable? (contains? (:sortable (:table spec)) (name k))
                        [d0 d1] (if (= :agg kind) ["desc" "asc"] ["asc" "desc"])]
                    (if sortable?
                      [:th.sortable
                       {:class cls
                        (keyword "data-on:click")
                        (str "$browseDir = ($browseSort === '" (name k) "'"
                             " && $browseDir === '" d0 "') ? '" d1 "' : '" d0 "'; "
                             "$browseSort = '" (name k) "'; "
                             (refresh-expr spec))}
                       header
                       [:span.console-sort
                        {:data-text (str "$browseSort === '" (name k) "' ? "
                                         "($browseDir === 'desc' ? '↓' : '↑') : ''")}]]
                      [:th {:class cls} header])))]]
       [:tbody (browse-rows-fragment spec rows sel?)]]])])

(defn field-shell
  "The wrapper every form control shares — label, optional hint, and the
  full-row `.wide` rule. One place, so create and edit cannot drift apart."
  [label kind hint control]
  [:div.console-field {:class (when (#{:text :textarea} kind) "wide")}
   [:div.console-field-label label]
   (when hint [:p.console-field-hint hint])
   control])

(defn field-control [[k label kind opts] row enum-vals]
  (field-shell
   label kind nil
   (case kind
     (:switch :flag)
     [:label.console-check
      [:ty-switch {:name (name k) :value "true"
                   :checked (when (get row k) true)}]
      [:span label]]

     ;; :signal lets a spec drive other controls off this field WITHOUT a save
     ;; round-trip (Apps hides the secret block the moment you pick Public).
     ;; ty-radio-group's change carries {:value :formValue :originalEvent},
     ;; verified in the vendored bundle.
     :enum
     (let [cur (some-> (get row k) name)]
       [:ty-radio-group (cond-> {:name (name k) :value cur}
                          (:signal opts)
                          (assoc (keyword "data-on:change")
                                 (str "$" (:signal opts) " = evt.detail.value")))
        [:div.console-radio-row
         (for [v enum-vals]
           [:label.console-check
            [:ty-radio {:value v :checked (when (= v cur) true)}]
            [:span (or (second (get widgets/type-glyphs v)) v)]])]])

     :textarea
     [:ty-textarea {:name (name k) :size "sm" :rows "3"
                    :value (str (get row k))}]

     :number
     [:ty-input {:name (name k) :size "sm" :type "number"
                 :value (str (or (get row k) "")) :autocomplete "off"}]

     [:ty-input {:name (name k) :size "sm"
                 :value (str (get row k)) :autocomplete "off"}])))

(defn settings-field
  [[k label kind hint opts] settings]
  (let [nm (str "settings__" (name k))
        v  (get settings (name k))]
    [:div.console-field
     [:div.console-field-label label]
     (when hint [:p.console-field-hint hint])
     (case kind
       :switch
       [:label.console-check
        [:ty-switch {:name nm :value "true" :checked (when v true)}]
        [:span (if v "Enabled" "Disabled")]]

       :uri-list
       [:ty-textarea {:name nm :size "sm" :rows "3" :placeholder "One per line"
                      :value (str/join "\n" v)}]

       :grants
       (let [held (set v)]
         [:div.console-radio-row
          (for [[gv glabel] (:choices opts)]
            [:label.console-check
             [:ty-checkbox {:name nm :value gv :checked (when (held gv) true)}]
             [:span glabel]])])

       :expiry
       [:div.console-expiry-row
        (for [sub ["access" "refresh" "id"]]
          [:ty-input {:name (str nm "__" sub) :type "number" :size "sm"
                      :label (str/capitalize sub)
                      :placeholder (str (get (:defaults opts) sub))
                      :value (str (or (get v sub) ""))}])]

       [:ty-input {:name nm :size "sm" :autocomplete "off"
                   :value (str (or v ""))}])]))

(defn settings-section
  [{:keys [detail]} row]
  (when-let [{:keys [fields]} (:settings detail)]
    [:div.console-settings
     [:div.console-settings-head (icon/icon :key {:size "13"}) "Client settings"]
     [:div.console-fields
      (for [f fields] (settings-field f (:settings row)))]]))

(defn config-field
  [[k label kind hint {:keys [placeholder]}] cfg]
  (let [nm (str "config__" (name k))
        v  (get cfg (name k))]
    [:div.console-field
     [:div.console-field-label label]
     (when hint [:p.console-field-hint hint])
     (case kind
       :password
       [:ty-input {:name nm :type "password" :size "sm" :autocomplete "new-password"
                   :placeholder (if (str/blank? (str v))
                                  "Not set"
                                  "Set — leave blank to keep")}]

       :number
       [:ty-input {:name nm :type "number" :size "sm"
                   :placeholder (str (or placeholder ""))
                   :value (str (or v ""))}]

       :switch
       [:label.console-check
        [:ty-switch {:name nm :value "true" :checked (when v true)}]
        [:span (if v "Enabled" "Disabled")]]

       [:ty-input {:name nm :size "sm" :autocomplete "off"
                   :placeholder (str (or placeholder ""))
                   :value (str (or v ""))}])]))

(defn config-section
  [{:keys [detail]} row]
  (when-let [{:keys [layouts heading] :as config} (:config detail)]
    (let [kval   (some-> (get row (:by config)) name)
          fields (get layouts kval)
          cfg    (data/config-value config row)]
      [:div.console-settings
       [:div.console-settings-head (icon/icon :key {:size "13"})
        (or heading "Configuration")]
       (cond
         (nil? fields)
         [:p.console-field-hint
          (str "No configuration form for type \"" kval
               "\" — manage this connector through the admin API.")]

         (empty? fields)
         [:p.console-field-hint "This type needs no configuration."]

         :else
         [:div.console-fields (for [f fields] (config-field f cfg))])])))

(defn option-row
  [icon-k show {:keys [xid active] :as row} selected?]
  (let [nm    (widgets/display-name row)
        nm    (if active nm (str nm " · inactive"))
        {:keys [tag sub]} show
        tag-v (some-> (get row tag) :name)
        sub-v (not-empty (get row sub))]
    [:ty-option (cond-> {:value xid
                         :label nm
                         :flavor (if active "neutral" "warning")}
                  selected? (assoc :selected true)
                  show (assoc :data-api (or tag-v "") :data-description (or sub-v "")))
     (icon/icon icon-k {:size "11" :slot "start"})
     (if show
       [:div.console-option-body
        [:div.console-option-name
         nm
         (when tag-v [:span.console-option-tag tag-v])]
        (when sub-v [:div.console-option-desc sub-v])]
       nm)]))

(defn link-more
  [slug link-key q offset & [tail]]
  [:div {:id (str (name link-key) "-more")
         :class "console-option-more"
         :data-on-intersect
         (format (str "@get('/console/iam/%s/%s?link=%s&offset=%d&q=' "
                      "+ encodeURIComponent('%s'))")
                 slug (or tail "options") (name link-key) offset (str/replace (or q "") "'" ""))}
   "Loading more…"])

(defn link-trigger
  [icon-k label]
  [:ty-button {:slot "trigger" :type "button" :size "sm" :appearance "outlined"
               :flavor "primary" :muted true}
   (icon/icon icon-k {:size "12" :slot "start"})
   (str "Manage " (str/lower-case label))])

(defn nav-filter-trigger
  [k icon-k label]
  ;; The clear "x" lives in ty-button's OWN "end" slot (verified against the
  ;; bundle: it builds a genuine `<slot name="end">` alongside "start" and
  ;; the default content slot) rather than as a sibling button — one control,
  ;; not two adjacent ones. evt.stopPropagation() is load-bearing: without
  ;; it, a click starting on this slotted icon still bubbles up through the
  ;; ty-button host (and the enclosing ty-select's own trigger-click
  ;; handling), which would also toggle the dropdown open/closed.
  ;;
  ;; ALWAYS rendered, never hidden/data-attr:hidden — toggling display:none
  ;; removes it from layout, which is its own flicker/shift (found live).
  ;; Dimmed via opacity + pointer-events instead, so the button's own box
  ;; never changes shape; only how the icon looks does.
  [:ty-button {:slot "trigger" :type "button" :size "sm" :appearance "outlined"
               :flavor "primary" :muted true}
   (icon/icon icon-k {:size "12" :slot "start"})
   [:span.console-nav-filter-label
    {:data-text
     (format "$flt_%s.length ? '%s (' + $flt_%s.length + ')' : '%s'"
             (name k) label (name k) label)}
    label]
   [:span {:slot "end" :title "Clear" :style "opacity:.35;pointer-events:none"
           (keyword "data-attr:style")
           (format (str "$flt_%s.length ? 'opacity:1;pointer-events:auto' "
                        ": 'opacity:.35;pointer-events:none'")
                   (name k))
           (keyword "data-on:click")
           (format (str "evt.stopPropagation(); evt.preventDefault(); "
                        "document.querySelectorAll('#%s-select ty-option[selected]')"
                        ".forEach(function(o){o.click()})")
                   (name k))}
    (icon/icon :x {:size "11"})]])

(defn link-option-list
  [icon-k label show held options & [more trigger]]
  (let [seen (into #{} (map :xid) held)]
    (concat
     [(or trigger (link-trigger icon-k label))]
     (for [row held] (option-row icon-k show row true))
     (for [row (remove (comp seen :xid) options)] (option-row icon-k show row false))
     (when more [more]))))

(defn link-page
  [icon-k show options & [more]]
  (concat (for [row options] (option-row icon-k show row false))
          (when more [more])))

(defn singular
  [label]
  (str/replace (str/lower-case label) #"s$" ""))

(defn truthy?
  "Form state carries everything as strings, a DB row carries real types —
  the panel renders both, so booleans have to be read either way."
  [v]
  (or (true? v) (= "true" (str v))))

(defn card-field
  [nm [_ label kind] v]
  (case kind
    :textarea [:ty-textarea {:name nm :label label :size "sm" :rows "2"
                             :value (str (or v ""))}]
    :switch [:label.console-check
             [:ty-switch {:name nm :value "true" :checked (when (truthy? v) true)}]
             [:span label]]
    [:ty-input {:name nm :label label :size "sm" :autocomplete "off"
                :value (str (or v ""))}]))

(defn picker
  [slug k label icon hint show held options]
  (let [sel     (str/join "," (map :xid held))
        pick-id (str (name k) "-select")]
    [[:div.console-picker
      [:div.console-field-label (icon/icon icon {:size "13"}) label]
      (when hint [:p.console-field-hint hint])
      [:ty-select
       {:id pick-id
        :name (name k)
        :multiple true
        :clearable true
        :external-search true
        :debounce "250"
        :size "sm"
        :value sel
        :placeholder (str "Search " (str/lower-case label) "…")
        (keyword "data-on:search")
        (format (str "@get('/console/iam/%s/options?link=%s&offset=0&q=' "
                     "+ encodeURIComponent(evt.detail.query || '') "
                     "+ '&sel=' + encodeURIComponent(evt.target.value || ''))")
                slug (name k))}
       (link-option-list icon label show held options
                         (when (= (count options) data/option-limit)
                           (link-more slug k nil (count options))))]]
     pick-id]))

(defn link-panel
  "The owned-child panel — list plus add/edit card, identical whether the parent exists yet or not; writes nothing."
  [slug label link-key icon show rows {:keys [editing values notice deleted]}]
  (let [ns-    (name link-key)
        fields (get-in show [:create :fields])
        sub    (:sub show)
        flag   (:flag show)
        sing   (singular label)
        ep     (fn [q]
                 (format "@post('/console/iam/%s/panel/%s?%s',{contentType:'form'})"
                         slug ns- q))]
    [:div.console-links.console-links-create {:id (str ns- "-panel")}
     [:div.console-field-label (icon/icon icon {:size "13"}) label]
     [:div.console-link-columns
      [:div.console-link-manage
       [:div.console-selected-list
        (if (empty? rows)
          [:p.console-field-hint "None yet."]
          (for [[i r] (map-indexed vector rows)]
            (let [editing? (= i editing)
                  nm       (or (:name r) "")
                  desc     (str (or (get r sub) (:description r) ""))]
              [:div {:class (str "console-selected-row" (when editing? " editing"))}
               [:div.console-selected-icon (icon/icon icon {:size "15"})]
               [:div.console-selected-body
                [:div.console-selected-name
                 [:span nm]
                 (when (truthy? (get r flag))
                   [:ty-tag {:pill true :size "xs" :flavor "neutral-"} "confidential"])
                 (when editing?
                   [:ty-tag {:pill true :size "xs" :flavor "primary"} "editing"])]
                (when (not-empty desc) [:div.console-selected-desc desc])]
               [:input {:type "hidden" :name (str ns- "__xid")
                        :value (str (:xid r))}]
               (for [[fk] fields]
                 [:input {:type "hidden" :name (str ns- "__" (name fk))
                          :value (str (get r fk))}])
               [:div.console-selected-actions
                [:ty-button
                 {:type "button" :size "xs" :appearance "outlined"
                  (keyword "data-on:click") (ep (str "target=" i))}
                 (icon/icon :pencil {:size "12" :slot "start"})
                 "Edit"]
                [:ty-button
                 {:type "button" :size "xs" :action true :flavor "danger" :muted true
                  (keyword "data-on:click") (ep (str "drop=" i))}
                 (icon/icon :trash-2 {:size "12"})]]])))]]
      [:div.console-link-add
       [:div.console-create
        [:div.console-create-heading
         (str (if editing "Edit " "New ") sing)]
        (when notice
          [:div {:class (str "console-notice console-rounded-notice " (name (first notice)))}
           (second notice)])
        [:div.console-create-fields
         (for [[fk :as f] fields]
           (card-field (str ns- "_new_" (name fk)) f (get values fk)))]
        (when editing
          [:input {:type "hidden" :name (str ns- "_editing") :value (str editing)}])
        [:div.console-create-actions
         (when editing
           [:ty-button {:type "button" :size "sm" :appearance "ghost"
                        (keyword "data-on:click") (ep "cancel=1")}
            "Cancel"])
         [:ty-button {:type "button" :size "sm" :appearance "outlined"
                      (keyword "data-on:click") (ep "add=1")}
          (icon/icon :plus {:size "12" :slot "start"})
          (if editing "Save" (str "Add " sing))]]]]]
     (for [x deleted]
       [:input {:type "hidden" :name (str ns- "__deleted") :value (str x)}])]))

(defn selected-tags
  [pick-id icon show]
  [:ty-selected-tags {:for pick-id
                      :class (if show "console-selected-list" "console-chips")}
   [:template
    (if show
      [:div.console-selected-row
       [:div.console-selected-icon (icon/icon icon {:size "14"})]
       [:div.console-selected-body
        [:div.console-selected-name
         [:span "{label}"]
         [:span.console-option-tag "{data-api}"]]
        [:div.console-selected-desc "{data-description}"]]
       [:ty-button
        {:type "button" :size "xs" :action true :appearance "ghost"
         :flavor "danger" :muted true :pill true
         :onclick "this.dispatchEvent(new CustomEvent('dismiss',{bubbles:true}))"}
        (icon/icon :x {:size "12"})]]

      [:ty-tag {:pill true :size "sm" :flavor "{flavor}" :dismissible "true"}
       (icon/icon icon {:size "11" :slot "start"})
       "{label}"])]])

(defn link-options-for
  "Picker options for a link — nil for an OWNED one, which renders a list
  and never searches, so the query would be pure waste."
  [l]
  (when-not (get-in (last l) [:create :parent])
    (data/link-options (nth l 2) nil 0 (nth l 5 nil))))

(defn link-control
  [slug [k label _ icon hint show] row options]
  (if (:create show)
    (link-panel slug label k icon show (get row k) nil)
    (let [[p pick-id] (picker slug k label icon hint show (get row k) options)]
      [:div.console-links
       p
       (selected-tags pick-id icon show)])))

(defn form-page
  "The one form chassis both `detail` and `create-page` build on."
  [{:keys [spec row action notice fields sections links actions]}]
  [:div.console-panel.console-detail
   [:form.console-form {:method "post" :action action}
    (widgets/notice notice)
    [:ty-scroll-container {:custom-scrollbar true :shadow true}
     [:div.console-form-body
      [:div.console-fields.console-fields-wrap fields]
      (when (seq sections) [:div.console-form-sections sections])
      (when (seq links) [:div.console-link-grid links])
      (settings-section spec row)
      (config-section spec row)]]
    [:div.console-form-actions actions]]])

(defn tab-id
  [label]
  (str/replace (str/lower-case label) #"[^a-z0-9]+" "-"))

(defn render-panels
  [panels row panel-notice]
  (keep (fn [[panel-label f]] (when-let [h (f row panel-notice)] [panel-label h])) panels))

(defn stacked-panels
  "Every panel, one under another — no tab chrome. Fine on a wide/tall
  viewport (.console-detail-side still bounds + scrolls as a safety net);
  the multi-panel case this replaces on narrower viewports is `panel-tabs`."
  [rendered]
  (for [[_ body] rendered] body))

(defn panel-tabs
  "Multiple :panels tabbed instead of stacked — stacked panels ran off the
  bottom of the frame with no scroll on short viewports. A single panel
  (Providers/Connectors) renders exactly as before, no tab chrome.

  `id-prefix` namespaces the generated ty-tab ids — `detail` renders this
  more than once (desktop side column vs. the narrow layout's System tab),
  and duplicate ids across those hidden/shown copies would otherwise be
  invalid HTML even though only one copy is ever visible at a time."
  [rendered id-prefix]
  (case (count rendered)
    0 nil
    1 (second (first rendered))
    [:ty-tabs {:height "100%" :active (str id-prefix (tab-id (ffirst rendered)))}
     (for [[panel-label body] rendered]
       [:ty-tab {:id (str id-prefix (tab-id panel-label)) :label panel-label} body])]))

(defn detail
  [request {:keys [slug label detail] :as spec} row & [notice panel-notice]]
  (let [{:keys [fields links head sections panels confirm]} detail
        title (or (:name row) "Edit")
        form  (form-page
               {:spec spec
                :row row
                :action (str "/console/iam/" slug "/" (:xid row))
                :notice notice
                :fields (cons
                         (when-let [{:keys [by badge]} (:config detail)]
                           [:div.console-field
                            [:div.console-field-label (str/capitalize (clojure.core/name by))]
                            (badge row)])
                         (for [f fields]
                           (field-control f row (when (= :enum (nth f 2))
                                                  (data/enum-values spec (first f))))))
                :sections (for [section sections] (section row))
                :links (for [l links] (link-control slug l row (link-options-for l)))
                :actions
                (list
                 [:ty-button {:type "button" :size "sm" :appearance "ghost"
                              :onclick (str "location.href='/console/iam/" slug "'")}
                  "Cancel"]
                 [:ty-button {:type "submit" :size "sm" :flavor "primary"} "Save"])})]
    (admin-shell
     {:title title
      :user (:console/principal request)
      :uri (str "/console/iam/" slug)
      :confirm (cond
                 (:delete spec) (str "/console/iam/" slug "/" (:xid row) "/delete")
                 confirm (confirm row))
      :session (:console/session request)
      :body
      [:div.console-page
       ;; `:detail :signals` seeds page state from the SAVED row so a field can
       ;; drive other controls without a save round-trip. Declared on this
       ;; ancestor, never on a sibling — Datastar applies one element's own
       ;; directives in attribute order and hiccup2 sorts them, so a reader on
       ;; the same element can run first.
       (when-let [sig (get-in spec [:detail :signals])]
         {:data-signals (sig row)})
       [:div.console-page-head
        [:div.console-eyebrow
         [:a.console-back {:href (str "/console/iam/" slug)}
          (icon/icon :chevron-right {:size "11"}) label]]
        [:h1.console-title title]
        [:div.console-detail-actions
         (when-let [tt (data/transfer-by-slug slug)]
           (when (data/transfer-exportable? (:type tt))
             [:ty-button
              {:type "button" :size "sm" :appearance "outlined" :flavor "primary" :muted true
               :onclick (format "location.href='/console/transfer/export?type=%s&ids=%s'"
                                (name (:type tt)) (:xid row))}
              (icon/icon :inbox {:size "13" :slot "start"})
              "Export"]))
         (when-let [{:keys [warning]} (:delete spec)]
           [:ty-button
            {:type "button" :size "sm" :appearance "outlined"
             :flavor "danger" :muted true
             (keyword "data-on:click")
             (format (str "$confirmAction='/console/iam/%s/%s/delete'; "
                          "$confirmTarget='%s'; $confirmVerb='Delete'; $confirmWhat=%s")
                     slug
                     (:xid row)
                     (:xid row)
                     (json/write-str
                      (str "Delete " (or (:name row) "this record") "? "
                           warning)))}
            (icon/icon :trash-2 {:size "13" :slot "start"})
            "Delete"])]
        (when head (head row))]
       (if (seq panels)
         (let [rendered (render-panels panels row panel-notice)]
           [:div.console-detail-layout
            [:div.console-detail-main form]
            [:div.console-detail-side
             [:div.console-detail-side-wide (stacked-panels rendered)]
             [:div.console-detail-side-desktop (panel-tabs rendered "desktop-")]
             [:div.console-detail-side-narrow (panel-tabs rendered "narrow-")]]])
         form)]})))

(defn create-control
  [[k label kind {:keys [choices placeholder hint default]}] values]
  (let [v (get values k)]
    (if (= kind :hidden)
      [:input {:type "hidden" :name (name k) :value (str (or v default))}]
      (field-shell
       label kind hint
       (case kind
         :switch
         [:label.console-check
          [:ty-switch {:name (name k) :value "true"
                       :checked (when (if values (some? v) default) true)}]
          [:span label]]

         :choices
         [:ty-radio-group {:name (name k) :value (str (or v ""))}
          [:div.console-radio-row
           (for [[cv clabel] choices]
             [:label.console-check
              [:ty-radio {:value cv :checked (when (= cv (str v)) true)}]
              [:span clabel]])]]

         :number
         [:ty-input {:name (name k) :type "number" :size "sm"
                     :placeholder (str (or placeholder ""))
                     :value (str (or v ""))}]

         :textarea
         [:ty-textarea {:name (name k) :size "sm" :rows "3"
                        :placeholder (str (or placeholder ""))
                        :value (str (or v ""))}]

         [:ty-input {:name (name k) :size "sm" :autocomplete "off"
                     :placeholder (str (or placeholder ""))
                     :value (str (or v ""))}])))))

(defn wizard-stage
  [{:keys [steps callback?]} {:keys [where url]} callback first?]
  (list
   (when (and first? url)
     [:p.console-field-hint
      "Open " [:a.console-wizard-link {:href url :target "_blank" :rel "noopener noreferrer"} where]
      " in another tab and work through these, then come back."])
   [:ol.console-wizard-steps (for [s steps] [:li s])]
   (when callback?
     [:div.console-wizard-callback
      [:ty-copy {:label "Redirect URI — register this one, exactly"
                 :value callback :format "code" :size "xs"}]
      (when-not env/iam-root-url
        [:p.console-field-hint
         (str "This URI is derived from the current request because "
              "SYNTHIGY_IAM_ROOT_URL is not set. Behind a load balancer, set it "
              "to the public origin first — otherwise you may register an "
              "internal address at the provider.")])])))

(defn wizard-ids
  "Step ids in order: one per guide stage, then the config form. There is no
   chooser step — the provider is decided by the card you clicked on the index,
   which IS the chooser."
  [stages]
  (conj (mapv #(str "s" %) (range (count stages))) "configure"))

(defn wizard-goto
  [ids idx]
  (str "$wizStep = '" (nth ids idx) "'; "
       "$wizCompleted = '" (str/join "," (take idx ids)) "'"))

(defn wizard-nav
  "Back/Next for one step. At step 0 there is no previous step, so Back is a
   real navigation to `back-href` (the index you came from) rather than nothing."
  [ids idx & [submit back-href]]
  (list
   (if (pos? idx)
     [:ty-button {:type "button" :size "sm" :appearance "ghost"
                  (keyword "data-on:click") (wizard-goto ids (dec idx))}
      "Back"]
     (when back-href
       [:ty-button {:type "button" :size "sm" :appearance "ghost"
                    :onclick (str "location.href='" back-href "'")}
        "Back"]))
   (or submit
       (when (get ids (inc idx))
         [:ty-button {:type "button" :size "sm" :flavor "primary"
                      (keyword "data-on:click") (wizard-goto ids (inc idx))}
          "Next"]))))

(defn wizard-step
  [id body actions]
  [:div.console-wizard-step
   [:ty-scroll-container {:custom-scrollbar true :shadow true}
    [:div.console-wizard-body {:id (str "wiz-" id "-body")} body]]
   (when actions [:div.console-form-actions actions])])

(defn create-wizard
  [request {:keys [slug label create] :as spec} row notice values]
  (let [{:keys [guides submit callback height]} (:wizard create)
        callback (if (fn? callback) (callback request) callback)
        by      (get-in spec [:detail :config :by])
        chosen  (some-> (get values by) name not-empty)
        guide   (get guides chosen)
        stages  (:stages guide)
        ids     (wizard-ids stages)
        last-i  (dec (count ids))
        index   (str "/console/iam/" slug)
        step    (first ids)
        done    ""]
    [:div.console-panel.console-detail.console-wizard-panel
     {:data-signals (str "{wizStep: '" step "', wizCompleted: '" done "'}")}
     [:form.console-form {:method "post" :action (str "/console/iam/" slug "/new")}
      (widgets/notice notice)
      [:ty-wizard {:height (or height "100%")
                   :active step :completed done
                   (keyword "data-attr:active") "$wizStep"
                   (keyword "data-attr:completed") "$wizCompleted"}

       (map-indexed
        (fn [i stage]
          [:ty-step {:id (nth ids i) :label (:label stage)}
           (wizard-step (nth ids i)
                        (wizard-stage stage guide callback (zero? i))
                        (wizard-nav ids i nil index))])
        stages)

       [:ty-step {:id "configure" :label "Connect"}
        (wizard-step
         "configure"
         (if guide
           (list
            (when-let [n (:note guide)] [:p.console-field-hint n])
            [:div.console-fields.console-fields-wrap
             (for [[k :as f] (:fields create)]
               (if (= k by)
                 [:input {:type "hidden" :name (name k) :value chosen}]
                 (create-control f values)))]
            (config-section spec row))
           [:p.console-field-hint.console-wizard-lead "Choose a provider first."])
         (when guide
           (wizard-nav ids last-i
                       [:ty-button {:type "submit" :size "sm" :flavor "primary"}
                        (or submit (str "Create " (singular label)))])))]]]]))

(defn create-page
  [request {:keys [slug label create] :as spec} & [notice values]]
  (let [title (str "New " (singular label))
        row   (into (into {} (map (fn [[k _ _ {:keys [default]}]] [k (or (get values k) default)]))
                          (:fields create))
                    (keep (fn [[k _ entity _ _ show]]
                            (when (and (:default show) (not (get values k)))
                              (when-let [row (data/link-default entity (:default show))]
                                [k [row]]))))
                    (concat (:links create) (:owned create)))
        form  (if (:wizard create)
                (create-wizard request spec row notice values)
                (form-page
               {:spec spec
                :row row
                :action (str "/console/iam/" slug "/new")
                :notice notice
                :fields (for [f (:fields create)] (create-control f values))
                :links (for [l (concat (:links create) (:owned create))]
                         (link-control slug l row (link-options-for l)))
                :actions
                (list
                 [:ty-button {:type "button" :size "sm" :appearance "ghost"
                              :onclick (str "location.href='/console/iam/" slug "'")}
                  "Cancel"]
                 [:ty-button {:type "submit" :size "sm" :flavor "primary"} "Create"])}))]
    (admin-shell
     {:title title
      :user (:console/principal request)
      :uri (str "/console/iam/" slug)
      :session (:console/session request)
      :body
      [:div.console-page
       [:div.console-page-head
        [:div.console-eyebrow
         [:a.console-back {:href (str "/console/iam/" slug)}
          (icon/icon :chevron-right {:size "11"}) label]]
        [:h1.console-title title]
        (when-let [h (:hint create)] [:p.console-subtitle h])]
       form]})))

(defn import-modal
  [{:keys [slug label]} stale? {:keys [notice mode payload]}]
  (let [mode (or mode :sync)]
    [:ty-modal {(keyword "data-attr:open") "$importOpen"
                :backdrop "true" :close-on-outside-click true :close-on-escape true
                (keyword "data-on:close") "$importOpen = false"}
     [:div.console-confirm.console-import-card
      [:h2 "Import " label]
      [:form.console-transfer-form {:method "post"
                                    :action (str "/console/iam/" slug "/import")}
       [:p.console-field-hint
        "Paste an export. Validate reports missing references without writing; "
        "Import refuses to write when any reference is missing."]
       (when-let [[kind msg] notice]
         [:div {:class (str "console-notice console-rounded-notice " (name kind))} msg])
       [:div.console-field
        [:div.console-field-label "Mode"]
        [:ty-radio-group {:name "mode" :value (name mode)}
         [:div.console-radio-col
          [:label.console-check
           [:ty-radio {:value "sync" :checked (when (= mode :sync) true)}]
           [:span "Sync — relations the payload mentions end up matching it exactly"]]
          [:label.console-check
           [:ty-radio {:value "stack" :checked (when (= mode :stack) true)}]
           [:span "Stack — additive, never removes a grant"]]]]]
       [:ty-textarea {:name "payload" :label "Payload" :size "sm" :rows "10"
                      :placeholder "[{\"xid\": …}]"
                      :value (str payload)}]
       (when stale?
         [:div.console-confirm-reauth
          [:p.hint "You signed in a while ago. Confirm your password to import."]
          [:ty-input {:type "password" :name "confirmPassword" :label "Password"
                      :size "sm" :autocomplete "current-password"}]])
       [:div.console-transfer-actions
        [:ty-button {:type "button" :size "sm" :appearance "ghost"
                     (keyword "data-on:click") "$importOpen = false"}
         "Close"]
        [:ty-button {:type "submit" :size "sm" :appearance "outlined"
                     :name "action" :value "validate"}
         "Validate"]
        [:ty-button {:type "submit" :size "sm" :appearance "outlined"
                     :flavor "danger" :muted true
                     :name "action" :value "import"}
         "Import"]]]]]))

(defn export-modal
  [{:keys [label]} tt]
  (let [t (name (:type tt))]
    [:ty-modal {(keyword "data-attr:open") "$exportOpen"
                :backdrop "true" :close-on-outside-click true :close-on-escape true
                (keyword "data-on:close") "$exportOpen = false"}
     [:div.console-confirm
      [:h2 "Export " label]
      [:p {:data-text (str "$exportSel.length "
                           "? $exportSel.length + ' selected record' "
                           "+ ($exportSel.length === 1 ? '' : 's') + ' will be exported.' "
                           ": 'Everything will be exported.'")}
       "Everything will be exported."]
      [:div.console-modal-field
       [:ty-input {:label "File name" :size "sm" :autocomplete "off"
                   :data-bind "exportName"
                   :placeholder (str t "s.json")}]
       [:p.console-field-hint
        "Blank picks a suggested name — a single record exports as "
        [:code (str t "_<name>.json")] "."]]
      [:div.console-confirm-actions
       [:ty-button {:type "button" :size "sm" :appearance "ghost"
                    (keyword "data-on:click") "$exportOpen = false"}
        "Cancel"]
       [:ty-button {:type "button" :size "sm" :flavor "primary" :muted true
                    :appearance "outlined"
                    (keyword "data-on:click")
                    (format (str "window.location = '/console/transfer/export?type=%s' "
                                 "+ ($exportSel.length ? '&ids=' + $exportSel.join(',') : '') "
                                 "+ ($exportName ? '&name=' + encodeURIComponent($exportName) : ''); "
                                 "$exportOpen = false; $exportMode = false; "
                                 "$exportSel = []; $exportName = ''")
                            t)}
        (icon/icon :inbox {:size "13" :slot "start"})
        "Download"]]]]))

(defn nav-filter-picker
  [{:keys [slug] :as spec} [k label entity icon] align]
  [:ty-select
   {:id (str (name k) "-select")
    :name (name k)
    :multiple true
    :compact true
    :external-search true
    :debounce "250"
    :size "sm"
    :align align
    (keyword "data-on:search")
    (format (str "@get('/console/iam/%s/filter-options?link=%s&offset=0&q=' "
                 "+ encodeURIComponent(evt.detail.query || '') "
                 "+ '&sel=' + encodeURIComponent(evt.target.value || ''))")
            slug (name k))
    (keyword "data-on:change")
    (str "$flt_" (name k) " = evt.detail.values; " (refresh-expr spec))}
   (link-option-list icon label nil [] (data/link-options entity "")
                     nil (nav-filter-trigger k icon label))])

(defn nav-filters
  [{:keys [filters] :as spec}]
  (when (seq filters)
    [:div.console-nav-filters
     (for [f filters] (nav-filter-picker spec f "end"))]))

(defn slot-card
  [slug {:keys [by badge]} [value label] row]
  (let [[flavor state] (cond
                         (nil? row)    ["neutral-" "Not configured"]
                         (:active row) ["success" "Active"]
                         :else         ["warning" "Configured, disabled"])]
    [:div.console-card
     [:div.console-card-mark (badge {by value})]
     [:ty-tag {:pill true :size "sm" :flavor flavor} state]
     [:ty-button {:type "button" :size "sm" :appearance "outlined"
                  :flavor "primary" :muted true
                  :onclick (str "location.href='"
                                (if row
                                  (str "/console/iam/" slug "/" (:xid row))
                                  (str "/console/iam/" slug "/new?"
                                       (codec/form-encode {(name by) value})))
                                "'")}
      (icon/icon (if row :pencil :plus) {:size "13" :slot "start"})
      (if row "Configure" (str "Set up " label))]]))

(defn slot-cards
  [{:keys [slug cards]} rows]
  (let [by-value (into {} (map (juxt #(some-> (get % (:by cards)) name) identity)) rows)]
    [:div.console-cards
     (for [[value :as slot] (:slots cards)]
       (slot-card slug cards slot (get by-value value)))]))

(defn browse
  [request {:keys [slug label subtitle] :as spec} & [import-state notice]]
  (let [rows  (data/browse-rows spec)
        more? (= (count rows) data/page-size)
        tt    (data/transfer-by-slug slug)
        exp?  (and tt (data/transfer-exportable? (:type tt)))
        imp?  (and tt (data/transfer-importable? (:type tt)))
        cards? (boolean (:cards spec))
        new?  (boolean (and (:create spec) (not cards?)))]
    (admin-shell
     {:title label
      :user (:console/principal request)
      :uri (str "/console/iam/" slug)
      ;; ponytail: a cards index has no stream — the range stream patches a
      ;; tbody, and a slot only changes when this admin edits it. Add a
      ;; card-shaped push if that stops being true.
      :stream-expr (when (and (not cards?) (routes/sse-available?)) (live-rows-expr spec))
      :paged (when-not cards? (count rows))
      :search (if cards? :none (server-search spec))
      :nav-filters (when-not cards? (nav-filters spec))
      :filter-keys (when-not cards? (map first (:filters spec)))
      :body
      [:div.console-page
       (when (or exp? imp?)
         {:data-signals
          (str "{"
               (str/join ", "
                         (cond-> []
                           exp? (conj "exportMode: false, exportSel: [], exportOpen: false, exportName: ''")
                           imp? (conj (str "importOpen: " (boolean import-state)))))
               "}")})
       ;; A save redirects here, so its verdict has to arrive in the URL —
       ;; a 303 drops an inline notice (the same reason `:create :after`
       ;; renders at 200 instead of redirecting).
       (widgets/notice notice true)
       [:div.console-page-head
        [:div.console-eyebrow "Administration"]
        [:h1.console-title label]
        [:p.console-subtitle subtitle]
        (when (or exp? imp? new?)
          [:div.console-toolbar
           (when new?
             [:ty-button {:type "button" :size "sm" :appearance "outlined"
                          :flavor "primary" :muted true
                          :onclick (str "location.href='/console/iam/" slug "/new'")}
              (icon/icon :plus {:size "13" :slot "start"})
              (str "New " (singular label))])
           (when exp?
             (list
              [:ty-button {:type "button" :size "sm" :appearance "outlined"
                           :flavor "primary" :muted true
                           (keyword "data-attr:hidden") "$exportMode"
                           (keyword "data-on:click") "$exportMode = true"}
               (icon/icon :inbox {:size "13" :slot "start"})
               "Export…"]
              [:ty-button {:type "button" :size "sm" :flavor "primary" :hidden true
                           (keyword "data-attr:hidden") "!$exportMode"
                           (keyword "data-on:click") "$exportOpen = true"}
               (icon/icon :inbox {:size "13" :slot "start"})
               [:span {:data-text (str "$exportSel.length "
                                       "? 'Export ' + $exportSel.length + ' selected' "
                                       ": 'Export all'")}
                "Export all"]]
              [:ty-button {:type "button" :size "sm" :appearance "outlined"
                           :flavor "warning" :muted true :hidden true
                           (keyword "data-attr:hidden") "!$exportMode"
                           (keyword "data-on:click") "$exportMode = false; $exportSel = []"}
               "Cancel"]))
           (when imp?
             [:ty-button {:type "button" :size "sm" :appearance "outlined"
                          :flavor "primary" :muted true
                          (keyword "data-on:click") "$importOpen = true"}
              (icon/icon :plus {:size "13" :slot "start"})
              "Import…"])])]
       (if cards?
         (slot-cards spec rows)
         (browse-table spec rows more? exp?))
       (when exp? (export-modal spec tt))
       (when imp? (import-modal spec
                                (not (session/fresh? (:console/session request)))
                                import-state))]})))
