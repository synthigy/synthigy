;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.log.store.sqlite.traffic
  (:require
   [clojure.string :as str]
   [synthigy.log.store.sqlite.conn :as conn]
   [synthigy.traffic :as traffic])
  (:import
   [java.sql Connection ResultSet]))

(defn hist-col [b] (str "hist_" (if (= b :inf) "inf" b)))

(def hist-bounds (conj (vec traffic/hist-bounds) :inf))

(def columns
  (-> ["node" "minute" "ok" "err"]
      (into (map hist-col hist-bounds))
      (into ["ops" "sse" "subscriptions" "deltas_dropped"])))

(def schema-ddl
  [(str "CREATE TABLE IF NOT EXISTS traffic_minutes (node TEXT, minute INTEGER, "
        (str/join ", " (map #(str % " INTEGER") (rest (rest columns)))) ")")
   "CREATE INDEX IF NOT EXISTS traffic_minutes_minute ON traffic_minutes(minute)"])

(defn apply-schema! [c]
  (apply conn/exec! c schema-ddl))

(defn drop-schema! [c]
  (conn/exec! c "DROP TABLE IF EXISTS traffic_minutes"))

(def retention-minutes (* 90 24 60))

(defn row-values [row]
  (-> [(:node row) (long (:minute row)) (long (:ok row 0)) (long (:err row 0))]
      (into (map #(long (get-in row [:hist %] 0)) hist-bounds))
      (into [(long (:ops row 0)) (long (:sse row 0))
             (long (:subscriptions row 0)) (long (:deltas-dropped row 0))])))

(defn insert! [^Connection c rows]
  (when (seq rows)
    (conn/in-tx! c
      (fn []
        (with-open [ps (.prepareStatement
                        c (str "INSERT INTO traffic_minutes (" (str/join ", " columns) ") VALUES ("
                               (str/join "," (repeat (count columns) "?")) ")"))]
          (doseq [r rows]
            (conn/bind-params! ps (row-values r))
            (.addBatch ps))
          (.executeBatch ps))))))

(defn prune! [^Connection c now-minute]
  (with-open [ps (.prepareStatement c "DELETE FROM traffic_minutes WHERE minute < ?")]
    (.setLong ps 1 (- (long now-minute) retention-minutes))
    (.executeUpdate ps)))

(defn read-row [^ResultSet rs]
  {:node           (.getString rs "node")
   :minute         (.getLong rs "minute")
   :ok             (.getLong rs "ok")
   :err            (.getLong rs "err")
   :hist           (into {} (map (fn [b] [b (.getLong rs ^String (hist-col b))])) hist-bounds)
   :ops            (.getLong rs "ops")
   :sse            (.getLong rs "sse")
   :subscriptions  (.getLong rs "subscriptions")
   :deltas-dropped (.getLong rs "deltas_dropped")})

(defn window [c from to]
  (conn/query-rows c "SELECT * FROM traffic_minutes WHERE minute >= ? AND minute <= ?"
                   [(long from) (long to)] read-row))
