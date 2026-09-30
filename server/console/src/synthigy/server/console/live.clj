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

(ns synthigy.server.console.live
  (:require
   [clojure.core.async :as async]
   [clojure.string :as str]
   [starfederation.datastar.clojure.api :as d*]
   [synthigy.embedded :as embedded]
   [synthigy.json :as json]
   [synthigy.log :as log]
   [synthigy.server.console.data :as data]
   [synthigy.server.console.pages.system :as system]
   [synthigy.server.console.ui :as ui]
   [synthigy.server.console.widgets :as widgets]
   [synthigy.server.routes :as routes]))

(def ^:private heartbeat-ms
  15000)

(def ^:private stream-max-ms
  90000)

(defn fragment
  [hiccup]
  (ui/render hiccup))

(defn render-push
  [render]
  (fn [sse-gen value]
    (d*/patch-elements! sse-gen (fragment (render value)))))

(defn stream
  [request live push!]
  (let [closed? (atom false)
        done    (fn [sse-gen reason]
                  (when (compare-and-set! closed? false true)
                    (remove-watch live ::push)
                    (embedded/close-watch! live)
                    (when sse-gen
                      (try (d*/close-sse! sse-gen) (catch Exception _ nil)))
                    (log/debug {:id ::stream-closed
                                :data {:action :stopped :subject :console-live
                                       :reason reason}}
                               "Console live stream closed")))]
    (routes/sse-response
     request
     {:d*.sse/on-open
      (fn [sse-gen]
        (try
          (push! sse-gen @live)
          (add-watch live ::push
                     (fn [_ _ old new]
                       (when (not= old new)
                         (try
                           (push! sse-gen new)
                           (catch Exception _ (done sse-gen :client-gone))))))
          (async/go-loop [elapsed 0]
            (async/<! (async/timeout heartbeat-ms))
            (let [elapsed (+ elapsed heartbeat-ms)]
              (if (>= elapsed stream-max-ms)
                (done sse-gen :max-lifetime)
                (when-not @closed? (recur elapsed)))))
          (catch Exception e
            (done sse-gen :open-failed)
            (throw e))))

      :d*.sse/on-close
      (fn [sse-gen & _] (done sse-gen :client-gone))})))

(defn sessions-stream
  [request]
  (let [user-xid   (get-in request [:console/session :resource-owner])
        current-id (get-in request [:console/session :id])]
    (stream request
            (embedded/watch-query :oauth_session {:_where {:active {:_eq true}}}
                                  [:xid]
                                  :acting-as nil)
            (render-push (fn [_] (widgets/sessions-table (data/my-sessions user-xid current-id)))))))

(defn identities-stream
  [request]
  (let [user-xid (get-in request [:console/session :resource-owner])]
    (stream request
            (embedded/watch-query :external_identity nil [:xid] :acting-as nil)
            (render-push (fn [_] (widgets/identities-table (data/my-identities user-xid)))))))

(defn system-stream
  "Module status live — `system/live` is a plain atom bumped by the lifecycle hook."
  [request]
  (stream request system/live (render-push (fn [_] (system/panel)))))

(defn browse-rows-response
  [request spec offset q rels sort-k dir]
  (let [rows    (data/browse-rows spec offset q rels sort-k dir)
        n       (count rows)
        more?   (= n data/page-size)
        replace? (zero? offset)
        sel?    (some-> (data/transfer-by-slug (:slug spec)) :type
                        data/transfer-exportable?)]
    (routes/sse-response
     request
     {:d*.sse/on-open
      (fn [sse-gen]
        (try
          (when (or replace? (pos? n))
            (d*/patch-elements! sse-gen
                                (fragment (ui/browse-rows-fragment spec rows sel?))
                                {d*/selector (str "#" (:slug spec) "-panel tbody")
                                 d*/patch-mode (if replace? d*/pm-inner d*/pm-append)}))
          (d*/patch-signals! sse-gen
                             (json/write-str {:browseOffset (+ offset n)
                                              :browseMore more?}))
          (finally
            (try (d*/close-sse! sse-gen) (catch Exception _ nil)))))})))

(defn link-panel-response
  "Re-render the whole owned-child panel from server state via `pm-replace`."
  [request {:keys [slug]} [k label _ icon _ show] rows state]
  (routes/sse-response
   request
   {:d*.sse/on-open
    (fn [sse-gen]
      (try
        (d*/patch-elements!
         sse-gen
         (fragment (ui/link-panel slug label k icon show rows state))
         {d*/selector (str "#" (name k) "-panel")
          d*/patch-mode d*/pm-replace})
        (finally
          (try (d*/close-sse! sse-gen) (catch Exception _ nil)))))}))

(defn redirect-response
  "Navigate, for an action fired by a Datastar `@post` — a plain 303 would be patched, not followed."
  [request uri]
  (routes/sse-response
   request
   {:d*.sse/on-open
    (fn [sse-gen]
      (try
        (d*/execute-script! sse-gen (format "location.href=%s" (json/write-str uri)))
        (finally
          (try (d*/close-sse! sse-gen) (catch Exception _ nil)))))}))

(defn options-response
  [request slug entries link q sel offset & [trigger]]
  (let [[k label link-entity icon _hint show :as found]
        (first (filter #(= link (name (first %))) entries))]
    (when found
      (let [first?  (zero? offset)
            options (data/link-options link-entity q offset show)
            more    (when (= (count options) data/option-limit)
                      (ui/link-more slug k q (+ offset (count options))
                                    (when trigger "filter-options")))]
        (routes/sse-response
         request
         {:d*.sse/on-open
          (fn [sse-gen]
            (try
              (if first?
                (let [held (data/link-selected link-entity (str/split (str sel) #",") show)]
                  (d*/patch-elements!
                   sse-gen
                   (fragment (ui/link-option-list icon label show held options more
                                                  (when trigger (trigger k icon label))))
                   {d*/selector (str "#" (name k) "-select")
                    d*/patch-mode d*/pm-inner}))
                (d*/patch-elements!
                 sse-gen
                 (fragment (ui/link-page icon show options more))
                 {d*/selector (str "#" (name k) "-more")
                  d*/patch-mode d*/pm-replace}))
              (finally
                (try (d*/close-sse! sse-gen) (catch Exception _ nil)))))})))))

(defn link-options-response
  [request spec link q sel offset]
  (options-response request (:slug spec) (get-in spec [:detail :links]) link q sel offset))

(defn filter-options-response
  [request spec link q sel offset]
  (options-response request (:slug spec) (:filters spec) link q sel offset ui/nav-filter-trigger))

(def ^:private range-cap
  (* 5 data/page-size))

(defn browse-range-stream
  [request spec limit q rels sort-k dir]
  (let [user-xid (get-in request [:console/session :resource-owner])
        sel?     (some-> (data/transfer-by-slug (:slug spec)) :type
                         data/transfer-exportable?)
        limit    (min (max limit data/page-size) range-cap)
        watched  (mapv name (into [(:entity spec)] (:watch spec)))
        watch-q  ((:watch (:table spec))
                  (data/table-params spec 0 limit q rels sort-k dir)
                  :acting-as user-xid :entities watched)]
    (stream request
            watch-q
            (fn [sse-gen rows]
              (let [n (count rows)]
                (d*/patch-elements! sse-gen
                                    (fragment (ui/browse-rows-fragment spec rows sel?))
                                    {d*/selector (str "#" (:slug spec) "-panel tbody")
                                     d*/patch-mode d*/pm-inner})
                (d*/patch-signals! sse-gen
                                   (json/write-str {:browseOffset n
                                                    :browseMore (= n limit)})))))))
