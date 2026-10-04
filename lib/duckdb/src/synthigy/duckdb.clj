;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.duckdb
  "Embedded DuckDB: connections, UTC timestamps and parameter binding — shared by the audit and log-store modules."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]])
  (:import
   [java.io File]
   [java.sql Connection DriverManager PreparedStatement ResultSet Timestamp]
   [java.time Instant LocalDateTime ZoneOffset]
   [org.duckdb DuckDBConnection]))

(try (Class/forName "org.duckdb.DuckDBDriver") (catch Throwable _))

(defn parse-int-or [s fallback]
  (or (try (some-> s str/trim Integer/parseInt) (catch Throwable _ nil))
      fallback))

(defn memory-limit []
  (or (not-empty (env :duckdb-memory-limit)) "256MB"))

(defn ->instant ^Instant [inst]
  (cond
    (nil? inst)                     nil
    (instance? Instant inst)        inst
    (instance? Timestamp inst)      (.toInstant ^Timestamp inst)
    (instance? java.util.Date inst) (.toInstant ^java.util.Date inst)
    (string? inst)                  (try (Instant/parse inst) (catch Throwable _ nil))
    :else                           nil))

(defn inst->utc
  "TIMESTAMP columns hold UTC wall clock; bind and append as LocalDateTime so no driver zone shift applies."
  ^LocalDateTime [inst]
  (some-> (->instant inst) (LocalDateTime/ofInstant ZoneOffset/UTC)))

(defn utc->iso [^LocalDateTime ts]
  (when ts (str (.toInstant ts ZoneOffset/UTC))))

(defn read-utc ^LocalDateTime [^ResultSet rs ^String col]
  (.getObject rs col LocalDateTime))

(defn memory? [path]
  (or (nil? path) (str/blank? path) (= ":memory:" path)))

(defn jdbc-url [path]
  (if (memory? path) "jdbc:duckdb:" (str "jdbc:duckdb:" path)))

(defn ensure-parent-dir! [path]
  (when-not (memory? path)
    (when-let [parent (.getParentFile (File. ^String path))]
      (when-not (.exists parent) (.mkdirs parent)))))

(defn open-connection
  "Open the database at `path` and cap its buffer pool; the limit is database-wide, so duplicates inherit it."
  ^Connection [path limit]
  (ensure-parent-dir! path)
  (let [conn (DriverManager/getConnection (jdbc-url path))]
    (with-open [stmt (.createStatement conn)]
      (.execute stmt (str "SET memory_limit = '" limit "'")))
    conn))

(defn with-duplicate
  "Run `(f conn)` on a private duplicate of `conn` — same database, own statement context."
  [conn f]
  (with-open [rc (.duplicate ^DuckDBConnection conn)]
    (f rc)))

(defn exec! [^Connection conn & statements]
  (with-open [stmt (.createStatement conn)]
    (doseq [^String s statements] (.execute stmt s))))

(defn bind-params! [^PreparedStatement ps params]
  (doseq [[i v] (map-indexed vector params)]
    (.setObject ps (int (inc i)) v)))

(defn row-count [^Connection conn ^String table]
  (with-open [stmt (.createStatement conn)
              rs   (.executeQuery stmt (str "SELECT COUNT(*) AS c FROM " table))]
    (when (.next rs) (.getLong rs "c"))))
