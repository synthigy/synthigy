;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.oauth.page.assets
  "Shared brand marks, icons and page chrome for the OAuth pages."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [hiccup.util :as hu]
   [hiccup2.core :refer [html raw]]))

(def figurative (hu/raw-string (slurp (io/resource "oauth/svg/figurative.svg"))))
(def verbal     (hu/raw-string (slurp (io/resource "oauth/svg/verbal.svg"))))

;; Lucide (MIT) path data, drawn by `icon`.
(def icons
  {:lock           "<rect width=\"18\" height=\"11\" x=\"3\" y=\"11\" rx=\"2\" ry=\"2\"/><path d=\"M7 11V7a5 5 0 0 1 10 0v4\"/>"
   :eye            "<path d=\"M2.062 12.348a1 1 0 0 1 0-.696 10.75 10.75 0 0 1 19.876 0 1 1 0 0 1 0 .696 10.75 10.75 0 0 1-19.876 0\"/><circle cx=\"12\" cy=\"12\" r=\"3\"/>"
   :eye-off        "<path d=\"M10.733 5.076a10.744 10.744 0 0 1 11.205 6.575 1 1 0 0 1 0 .696 10.747 10.747 0 0 1-1.444 2.49\"/><path d=\"M14.084 14.158a3 3 0 0 1-4.242-4.242\"/><path d=\"M17.479 17.499a10.75 10.75 0 0 1-15.417-5.151 1 1 0 0 1 0-.696 10.75 10.75 0 0 1 4.446-5.143\"/><path d=\"m2 2 20 20\"/>"
   :circle-alert   "<circle cx=\"12\" cy=\"12\" r=\"10\"/><line x1=\"12\" x2=\"12\" y1=\"8\" y2=\"12\"/><line x1=\"12\" x2=\"12.01\" y1=\"16\" y2=\"16\"/>"
   :triangle-alert "<path d=\"m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3\"/><path d=\"M12 9v4\"/><path d=\"M12 17h.01\"/>"
   :info           "<circle cx=\"12\" cy=\"12\" r=\"10\"/><path d=\"M12 16v-4\"/><path d=\"M12 8h.01\"/>"
   :circle-check   "<circle cx=\"12\" cy=\"12\" r=\"10\"/><path d=\"m9 12 2 2 4-4\"/>"
   :circle-x       "<circle cx=\"12\" cy=\"12\" r=\"10\"/><path d=\"m15 9-6 6\"/><path d=\"m9 9 6 6\"/>"
   :shield-alert   "<path d=\"M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z\"/><path d=\"M12 8v4\"/><path d=\"M12 16h.01\"/>"
   :wrench         "<path d=\"M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z\"/>"
   :app-window     "<rect x=\"2\" y=\"4\" width=\"20\" height=\"16\" rx=\"2\"/><path d=\"M10 4v4\"/><path d=\"M2 8h20\"/><path d=\"M6 4v4\"/>"
   :arrow-left     "<path d=\"m12 19-7-7 7-7\"/><path d=\"M19 12H5\"/>"
   :undo-2         "<path d=\"M9 14 4 9l5-5\"/><path d=\"M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5a5.5 5.5 0 0 1-5.5 5.5H11\"/>"
   :timer-off      "<path d=\"M10 2h4\"/><path d=\"M4.6 11a8 8 0 0 0 1.7 8.7 8 8 0 0 0 8.7 1.7\"/><path d=\"M7.4 7.4a8 8 0 0 1 10.3 1 8 8 0 0 1 .9 10.2\"/><path d=\"m2 2 20 20\"/><path d=\"M12 12v-2\"/>"
   :unlink         "<path d=\"m18.84 12.25 1.72-1.71h-.02a5.004 5.004 0 0 0-.12-7.07 5.006 5.006 0 0 0-6.95 0l-1.72 1.71\"/><path d=\"m5.17 11.75-1.71 1.71a5.004 5.004 0 0 0 .12 7.07 5.006 5.006 0 0 0 6.95 0l1.71-1.71\"/><line x1=\"8\" x2=\"8\" y1=\"2\" y2=\"5\"/><line x1=\"2\" x2=\"5\" y1=\"8\" y2=\"8\"/><line x1=\"16\" x2=\"16\" y1=\"19\" y2=\"22\"/><line x1=\"19\" x2=\"22\" y1=\"16\" y2=\"16\"/>"
   :user-x         "<path d=\"M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2\"/><circle cx=\"9\" cy=\"7\" r=\"4\"/><line x1=\"17\" x2=\"22\" y1=\"8\" y2=\"13\"/><line x1=\"22\" x2=\"17\" y1=\"8\" y2=\"13\"/>"
   :mail-warning   "<path d=\"M22 10.5V6a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2v12c0 1.1.9 2 2 2h12.5\"/><path d=\"m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7\"/><path d=\"M20 14v4\"/><path d=\"M20 22v.01\"/>"
   :ban            "<path d=\"M4.929 4.929 19.07 19.071\"/><circle cx=\"12\" cy=\"12\" r=\"10\"/>"
   :ticket-x       "<path d=\"M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z\"/><path d=\"m9.5 14.5 5-5\"/><path d=\"m9.5 9.5 5 5\"/>"
   :monitor-check  "<path d=\"m9 10 2 2 4-4\"/><rect width=\"20\" height=\"14\" x=\"2\" y=\"3\" rx=\"2\"/><path d=\"M12 17v4\"/><path d=\"M8 21h8\"/>"
   :user-round     "<circle cx=\"12\" cy=\"8\" r=\"5\"/><path d=\"M20 21a8 8 0 0 0-16 0\"/>"
   :check          "<path d=\"M20 6 9 17l-5-5\"/>"
   :shield-check   "<path d=\"M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z\"/><path d=\"m9 12 2 2 4-4\"/>"
   :plug-zap       "<path d=\"M6.3 20.3a2.4 2.4 0 0 0 3.4 0L12 18l-6-6-2.3 2.3a2.4 2.4 0 0 0 0 3.4Z\"/><path d=\"m2 22 3-3\"/><path d=\"M7.5 13.5 10 11\"/><path d=\"M10.5 16.5 13 14\"/><path d=\"m18 3-4 4h6l-4 4\"/>"
   :log-in         "<path d=\"m10 17 5-5-5-5\"/><path d=\"M15 12H3\"/><path d=\"M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4\"/>"
   :log-out        "<path d=\"m16 17 5-5-5-5\"/><path d=\"M21 12H9\"/><path d=\"M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4\"/>"
   :monitor-smartphone "<path d=\"M18 8V6a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2v7a2 2 0 0 0 2 2h8\"/><path d=\"M10 19v-3.96 3.15\"/><path d=\"M7 19h5\"/><rect width=\"6\" height=\"10\" x=\"16\" y=\"12\" rx=\"2\"/>"
   :key-round      "<path d=\"M2.586 17.414A2 2 0 0 0 2 18.828V21a1 1 0 0 0 1 1h3a1 1 0 0 0 1-1v-1a1 1 0 0 1 1-1h1a1 1 0 0 0 1-1v-1a1 1 0 0 1 1-1h.172a2 2 0 0 0 1.414-.586l.814-.814a6.5 6.5 0 1 0-4-4z\"/><circle cx=\"16.5\" cy=\"7.5\" r=\".5\" fill=\"currentColor\"/>"})

(defn icon
  ([k] (icon k nil))
  ([k class]
   (hu/raw-string
    (str "<svg" (when class (str " class=\"" class "\""))
         " viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.75\""
         " stroke-linecap=\"round\" stroke-linejoin=\"round\" aria-hidden=\"true\">"
         (icons k) "</svg>"))))

(def tone-icons
  {:error :circle-alert :warn :triangle-alert :info :info :success :circle-check})

(defn message
  [tone text]
  [:div {:class (str "sy-msg tone-" (name tone)) :role "alert"}
   [:span.sy-msg-icon (icon (tone-icons tone :circle-alert))]
   [:span.sy-msg-text (when (string? text) {:title text}) text]])

(defn voice
  "Fixed-height line under a form title: `sub`, replaced by the message `text` when there is one."
  [{:keys [id tone]} sub text]
  (let [msg? (if (string? text) (not (str/blank? text)) (some? text))]
    [:div.sy-voice (cond-> {:aria-live "polite"}
                     id (assoc :id id)
                     msg? (assoc :class "has-msg"))
     [:p.sy-card-sub sub]
     (when msg? (message (or tone :error) text))]))

(defn hero
  "Centered head of an outcome page: toned icon, title, and a message or any extra content."
  [{:keys [tone glyph title text]} & more]
  (into [:div.sy-hero
         [:div.sy-hero-icon {:class tone} (icon glyph)]
         [:h1.sy-hero-title title]
         (when text [:p.sy-hero-text text])]
        more))

(defn code-chip [code]
  [:div.sy-code-chip [:span "Error code"] [:code code]])

(defn lockup []
  [:span.sy-lockup {:role "img" :aria-label "Synthigy"}
   [:span.sy-lockup-mark figurative]
   [:span.sy-lockup-word verbal]])

(defn document
  "Complete OAuth page: brand bar, centered stage holding `body`, status line, starfield."
  [{:keys [title css chip status scripts]} & body]
  (html
   (raw "<!DOCTYPE html>\n")
   [:html {:lang "en"}
    [:head
     [:meta {:charset "UTF-8"}]
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1.0"}]
     [:meta {:name "color-scheme" :content "dark"}]
     [:meta {:name "robots" :content "noindex, nofollow"}]
     [:title title]
     [:link {:rel "icon" :type "image/png" :href "/oauth/images/favicon.png"}]
     [:link {:rel "preload" :href "/oauth/css/fonts/inter-var.woff2"
             :as "font" :type "font/woff2" :crossorigin "anonymous"}]
     [:link {:rel "stylesheet" :href (str "/oauth/css/" css)}]]
    [:body
     [:div.sy-shell
      [:header.sy-bar
       (lockup)
       [:span.sy-spacer]
       (when-not (str/blank? chip)
         [:span.sy-bar-chip {:title chip} (icon :app-window) [:span chip]])]
      (into [:main.sy-stage] body)
      [:footer.sy-status
       [:span.sy-status-item (icon :lock) [:span {:data-sy-host ""}]]
       [:span.sy-spacer]
       status]]
     [:script {:src "/oauth/js/starfield.js"}]
     [:script {:src "/oauth/js/shell.js"}]
     (for [s scripts] [:script {:src s}])]]))

(def brand-icons
  {:google
   [:svg {:viewBox "0 0 48 48"}
    [:path {:fill "#FFC107" :d "M43.611 20.083H42V20H24v8h11.303c-1.649 4.657-6.08 8-11.303 8-6.627 0-12-5.373-12-12s5.373-12 12-12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4 12.955 4 4 12.955 4 24s8.955 20 20 20 20-8.955 20-20c0-1.341-.138-2.65-.389-3.917z"}]
    [:path {:fill "#FF3D00" :d "M6.306 14.691l6.571 4.819C14.655 15.108 18.961 12 24 12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4 16.318 4 9.656 8.337 6.306 14.691z"}]
    [:path {:fill "#4CAF50" :d "M24 44c5.166 0 9.86-1.977 13.409-5.192l-6.19-5.238C29.211 35.091 26.715 36 24 36c-5.202 0-9.619-3.317-11.283-7.946l-6.522 5.025C9.505 39.556 16.227 44 24 44z"}]
    [:path {:fill "#1976D2" :d "M43.611 20.083H42V20H24v8h11.303c-.792 2.237-2.231 4.166-4.087 5.571l6.19 5.238C36.971 39.205 44 34 44 24c0-1.341-.138-2.65-.389-3.917z"}]]
   :microsoft
   [:svg {:viewBox "0 0 24 24"}
    [:path {:fill "#F25022" :d "M11.4 11.4H2V2h9.4z"}]
    [:path {:fill "#7FBA00" :d "M22 11.4h-9.4V2H22z"}]
    [:path {:fill "#00A4EF" :d "M11.4 22H2v-9.4h9.4z"}]
    [:path {:fill "#FFB900" :d "M22 22h-9.4v-9.4H22z"}]]
   :github
   [:svg {:viewBox "0 0 24 24"}
    [:path {:fill "currentColor" :d "M12 .5C5.37.5 0 5.87 0 12.5c0 5.3 3.44 9.8 8.21 11.39.6.11.82-.26.82-.58v-2.03c-3.34.73-4.04-1.61-4.04-1.61-.55-1.39-1.33-1.76-1.33-1.76-1.09-.74.08-.73.08-.73 1.2.08 1.84 1.24 1.84 1.24 1.07 1.83 2.8 1.3 3.49.99.11-.78.42-1.3.76-1.6-2.67-.3-5.47-1.33-5.47-5.93 0-1.31.47-2.38 1.24-3.22-.12-.3-.54-1.52.12-3.18 0 0 1.01-.32 3.3 1.23a11.5 11.5 0 0 1 6 0c2.29-1.55 3.3-1.23 3.3-1.23.66 1.66.24 2.88.12 3.18.77.84 1.24 1.91 1.24 3.22 0 4.61-2.81 5.62-5.49 5.92.43.37.81 1.1.81 2.22v3.29c0 .32.22.7.83.58C20.56 22.29 24 17.79 24 12.5 24 5.87 18.63.5 12 .5z"}]]
   :facebook
   [:svg {:viewBox "0 0 24 24"}
    [:path {:fill "#1877F2" :d "M24 12c0-6.63-5.37-12-12-12S0 5.37 0 12c0 5.99 4.39 10.95 10.13 11.85v-8.38H7.08V12h3.05V9.36c0-3.01 1.79-4.67 4.53-4.67 1.31 0 2.68.23 2.68.23v2.95h-1.51c-1.49 0-1.95.93-1.95 1.87V12h3.32l-.53 3.47h-2.79v8.38C19.61 22.95 24 17.99 24 12z"}]]
   :linkedin
   [:svg {:viewBox "0 0 24 24"}
    [:path {:fill "#0A66C2" :d "M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.225 0z"}]]
   :discord
   [:svg {:viewBox "0 0 24 24"}
    [:path {:fill "#5865F2" :d "M20.317 4.3698a19.7913 19.7913 0 00-4.8851-1.5152.0741.0741 0 00-.0785.0371c-.211.3753-.4447.8648-.6083 1.2495-1.8447-.2762-3.68-.2762-5.4868 0-.1636-.3933-.4058-.8742-.6177-1.2495a.077.077 0 00-.0785-.037 19.7363 19.7363 0 00-4.8852 1.515.0699.0699 0 00-.0321.0277C.5334 9.0458-.319 13.5799.0992 18.0578a.0824.0824 0 00.0312.0561c2.0528 1.5076 4.0413 2.4228 5.9929 3.0294a.0777.0777 0 00.0842-.0276c.4616-.6304.8731-1.2952 1.226-1.9942a.076.076 0 00-.0416-.1057c-.6528-.2476-1.2743-.5495-1.8722-.8923a.077.077 0 01-.0076-.1277c.1258-.0943.2517-.1923.3718-.2914a.0743.0743 0 01.0776-.0105c3.9278 1.7933 8.18 1.7933 12.0614 0a.0739.0739 0 01.0785.0095c.1202.099.246.1981.3728.2924a.077.077 0 01-.0066.1276 12.2986 12.2986 0 01-1.873.8914.0766.0766 0 00-.0407.1067c.3604.698.7719 1.3628 1.225 1.9932a.076.076 0 00.0842.0286c1.961-.6067 3.9495-1.5219 6.0023-3.0294a.077.077 0 00.0313-.0552c.5004-5.177-.8382-9.6739-3.5485-13.6604a.061.061 0 00-.0312-.0286zM8.02 15.3312c-1.1825 0-2.1569-1.0857-2.1569-2.419 0-1.3332.9555-2.4189 2.157-2.4189 1.2108 0 2.1757 1.0952 2.1568 2.419 0 1.3332-.9555 2.4189-2.1569 2.4189zm7.9748 0c-1.1825 0-2.1569-1.0857-2.1569-2.419 0-1.3332.9554-2.4189 2.1569-2.4189 1.2108 0 2.1757 1.0952 2.1568 2.419 0 1.3332-.946 2.4189-2.1568 2.4189Z"}]]})

(def generic-brand-icon
  [:svg {:viewBox "0 0 24 24" :fill "none" :stroke "currentColor" :stroke-width "2"
         :stroke-linecap "round" :stroke-linejoin "round"}
   [:path {:d "M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"}]
   [:polyline {:points "10 17 15 12 10 7"}]
   [:line {:x1 "15" :y1 "12" :x2 "3" :y2 "12"}]])

(defn provider-mark
  "Login-button mark for a `list-providers` entry — an admin logo_url wins
   (rendered as <img>, never inline markup), then the baked brand SVG, then
   the generic mark."
  [{:keys [provider logo]}]
  (if logo
    [:img {:src logo :alt ""
           :style "width:18px;height:18px;object-fit:contain"}]
    (get brand-icons provider generic-brand-icon)))
