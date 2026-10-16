;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.health
  "GET /health for load balancers and supervisors: 200 while the node can serve, 503 when it can't."
  (:require
   [next.jdbc :as jdbc]
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataline :as dataline]
   [synthigy.db :as db]
   [synthigy.json :as json]))

(def ping-timeout-ms 2000)

(def ping-cache-ms 1000)

(defonce db-check (atom nil))

(defn ping []
  (try
    (when-let [ds (:datasource db/*db*)]
      (with-open [c (jdbc/get-connection ds)]
        (.isValid ^java.sql.Connection c 2)))
    (catch Throwable _ false)))

(defn database-ok?
  "One ping in flight at a time, reused for a second — a dead database can't pile up probe threads."
  []
  (let [now  (System/currentTimeMillis)
        mine {:result (promise) :at now}
        {:keys [result] :as s} (swap! db-check
                                      (fn [{:keys [result at] :as s}]
                                        (if (and s (or (not (realized? result))
                                                       (< (- now at) ping-cache-ms)))
                                          s
                                          mine)))]
    ;; never start the ping inside swap! — it retries under contention
    (when (identical? s mine)
      (future (deliver result (ping))))
    (boolean (deref result ping-timeout-ms false))))

(defn report [required]
  (let [modules  (into (sorted-map)
                       (map (fn [m] [(subs (str m) 1) (boolean (lifecycle/started? m))]))
                       required)
        database (database-ok?)
        dataline (name (:status (dataline/health) :starting))
        status   (cond
                   (not (and database (every? val modules))) "unavailable"
                   (= "degraded" dataline) "degraded"
                   :else "ok")]
    {:status status
     :checks {:database database :modules modules :dataline dataline}}))

(defn handler
  "Ring handler checking `required` modules, the database and Dataline; never gated, never authenticated."
  [required]
  (fn [_request]
    (let [{:keys [status] :as body} (report required)]
      {:status  (if (= "unavailable" status) 503 200)
       :headers {"Content-Type"  "application/json"
                 "Cache-Control" "no-store"}
       :body    (json/write-str body)})))
