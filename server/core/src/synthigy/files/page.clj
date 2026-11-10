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

(ns synthigy.files.page
  "The page a browser gets when it opens a file it may not see."
  (:require
   [clojure.string :as str]
   [synthigy.oauth.page.assets :as assets]
   [synthigy.oauth.page.custom :as custom]))

(defn browser? [{:keys [request-method headers]}]
  (and (= :get request-method)
       (nil? (get headers "authorization"))
       (str/includes? (str (get headers "accept")) "text/html")))

(defn unauthorized-html []
  (assets/document
   {:title "Sign-in required · Synthigy"
    :css "error.css"
    :status [:span.sy-status-item {:class "tone-warn"} (assets/icon :lock) [:span "unauthorized"]]}
   [:div.sy-card.sy-card--wide.sy-card--center
    [:div.sy-card-head
     (assets/hero {:tone "tone-warn" :glyph :lock
                   :title "Sign-in required"
                   :text "This file is private, or it doesn't exist."}
                  (assets/code-chip "unauthorized"))]
    [:div.sy-card-body
     [:div.sy-prose
      [:p "Only files in a Public folder open straight in the browser. Open this one from the app that shared it."]]]
    [:div.sy-card-foot "If you think you should see this file, ask the person who shared it."]]))

(defn unauthorized
  "The default bundle's files.html when it has one, else the built-in page, as a 401."
  [request]
  (or (custom/custom-page-redirect "files.html" {:error "unauthorized" :uri (:uri request)})
      {:status 401
       :headers {"Content-Type" "text/html; charset=utf-8"
                 "Cache-Control" "no-cache, no-store, must-revalidate"}
       :body (str (unauthorized-html))}))

(defn link-gone-html []
  (assets/document
   {:title "Link no longer works · Synthigy"
    :css "error.css"
    :status [:span.sy-status-item {:class "tone-warn"} (assets/icon :lock) [:span "link not found"]]}
   [:div.sy-card.sy-card--wide.sy-card--center
    [:div.sy-card-head
     (assets/hero {:tone "tone-warn" :glyph :lock
                   :title "This link no longer works"
                   :text "It expired, was used up or was withdrawn, or it never existed."}
                  (assets/code-chip "link_not_found"))]
    [:div.sy-card-body
     [:div.sy-prose
      [:p "Ask the person who sent it for a new link."]]]]))

(defn link-gone
  "The default bundle's files.html when it has one, else the built-in page, as a 404."
  [request]
  (or (custom/custom-page-redirect "files.html" {:error "link_not_found" :uri (:uri request)})
      {:status 404
       :headers {"Content-Type" "text/html; charset=utf-8"
                 "Cache-Control" "no-store"}
       :body (str (link-gone-html))}))
