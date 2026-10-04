;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.log.store.sqlite.conn
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   synthigy.env)
  (:import
   [java.io File]
   [java.lang.reflect Method]
   [java.sql Connection DriverManager PreparedStatement ResultSet]
   [java.time Instant]
   [java.util.regex Pattern]
   [org.sqlite Function]))

(def defaults
  {:logs-path          (str synthigy.env/db-dir "/logs.db")
   :buffer-size        8192
   :batch-rows         500
   :batch-ms           250
   :log-max-mb         1024
   :log-retention-days 30
   :retry-attempts     3
   :retry-min-ms       100
   :retry-max-ms       5000
   :close-timeout-ms   2000})

(defn parse-long-or [s fallback]
  (or (try (some-> s str/trim Long/parseLong) (catch Throwable _ nil))
      fallback))

(defn env-config
  "Log store config from SQLITE_LOGS_* env on top of `defaults`."
  []
  {:logs-path          (or (not-empty (env :sqlite-logs-path)) (:logs-path defaults))
   :buffer-size        (parse-long-or (env :sqlite-logs-buffer-size) (:buffer-size defaults))
   :batch-rows         (parse-long-or (env :sqlite-logs-batch-rows) (:batch-rows defaults))
   :batch-ms           (parse-long-or (env :sqlite-logs-batch-ms) (:batch-ms defaults))
   :log-max-mb         (parse-long-or (env :sqlite-logs-max-mb) (:log-max-mb defaults))
   :log-retention-days (parse-long-or (env :sqlite-logs-retention-days) (:log-retention-days defaults))
   :retry-attempts     (:retry-attempts defaults)
   :retry-min-ms       (:retry-min-ms defaults)
   :retry-max-ms       (:retry-max-ms defaults)
   :close-timeout-ms   (:close-timeout-ms defaults)})

;;; ============================================================================
;;; Connections
;;; ============================================================================

(defn memory? [path]
  (or (nil? path) (str/blank? path) (= ":memory:" path)))

(defn jdbc-url
  "JDBC URL for `path`; `:memory:` becomes a uniquely named shared-cache database."
  [path]
  (if (memory? path)
    (str "jdbc:sqlite:file:synthigy-logs-" (random-uuid) "?mode=memory&cache=shared")
    (str "jdbc:sqlite:" path)))

(defn resolve-url [config]
  (assoc config :logs-url (jdbc-url (:logs-path config))))

(defn ensure-parent-dir! [path]
  (when-not (memory? path)
    (when-let [parent (.getParentFile (File. ^String path))]
      (when-not (.exists parent) (.mkdirs parent)))))

(defn exec! [^Connection conn & sqls]
  (with-open [st (.createStatement conn)]
    (doseq [^String s sqls] (.execute st s))))

(def protected-method
  (memoize
   (fn [^String name & types]
     (doto (.getDeclaredMethod Function name (into-array Class types))
       (.setAccessible true)))))

(defn regexp-matches-fn
  "SQL `regexp_matches(value, pattern)`: 1 when `pattern` is found in `value`."
  []
  (let [patterns (atom {})
        compile  (fn [^String p] (or (get @patterns p)
                                     (let [c (Pattern/compile p)] (swap! patterns assoc p c) c)))]
    (proxy [Function] []
      (xFunc []
        (let [value-text ^Method (protected-method "value_text" Integer/TYPE)
              result     ^Method (protected-method "result" Integer/TYPE)
              v          (.invoke value-text this (object-array [(int 0)]))
              p          (.invoke value-text this (object-array [(int 1)]))]
          (.invoke result this (object-array
                                [(int (if (and v p (.find (.matcher ^Pattern (compile p) ^CharSequence v))) 1 0))])))))))

(defn open!
  "Open a connection with the log store pragmas and `regexp_matches` registered."
  ^Connection [^String url]
  (let [conn (DriverManager/getConnection url)]
    ;; auto_vacuum first — once journal_mode writes page 1 it can no longer change
    (exec! conn "PRAGMA auto_vacuum = INCREMENTAL" "PRAGMA busy_timeout = 30000" "PRAGMA synchronous = NORMAL")
    (if (str/includes? url "mode=memory")
      (exec! conn "PRAGMA read_uncommitted = true")
      (exec! conn "PRAGMA journal_mode = WAL"))
    (Function/create conn "regexp_matches" (regexp-matches-fn))
    conn))

(defmacro with-conn [[sym url] & body]
  `(with-open [~sym (open! ~url)] ~@body))

(defn in-tx!
  "Run `f` inside one transaction on `conn`; roll back and rethrow on failure."
  [^Connection conn f]
  (.setAutoCommit conn false)
  (try
    (let [r (f)] (.commit conn) r)
    (catch Throwable t
      (try (.rollback conn) (catch Throwable _))
      (throw t))
    (finally
      (.setAutoCommit conn true))))

(defn bind-params! [^PreparedStatement ps params]
  (doseq [[i v] (map-indexed vector params)]
    (.setObject ps (int (inc i)) v)))

(defn query-rows [^Connection conn ^String sql params read-row]
  (with-open [ps (.prepareStatement conn sql)]
    (bind-params! ps params)
    (with-open [rs (.executeQuery ps)]
      (loop [acc (transient [])]
        (if (.next rs) (recur (conj! acc (read-row rs))) (persistent! acc))))))

(defn scalar [^Connection conn ^String sql]
  (with-open [st (.createStatement conn)
              rs (.executeQuery st sql)]
    (when (.next rs) (.getLong rs 1))))

(defn row-count [conn table]
  (scalar conn (str "SELECT count(*) FROM " table)))

(defn used-bytes
  "Bytes held by live pages; the freelist is excluded."
  [conn]
  (* (- (scalar conn "PRAGMA page_count") (scalar conn "PRAGMA freelist_count"))
     (scalar conn "PRAGMA page_size")))

(defn file-bytes [conn]
  (* (scalar conn "PRAGMA page_count") (scalar conn "PRAGMA page_size")))

(defn incremental-vacuum! [^Connection conn]
  (with-open [st (.createStatement conn)]
    ;; executeUpdate, never execute — only executeUpdate steps the pragma to the end
    (.executeUpdate st "PRAGMA incremental_vacuum")))

;;; ============================================================================
;;; Time — INTEGER epoch microseconds, UTC
;;; ============================================================================

(defn ->instant ^Instant [x]
  (cond
    (nil? x)                     nil
    (instance? Instant x)        x
    (instance? java.util.Date x) (.toInstant ^java.util.Date x)
    (string? x)                  (try (Instant/parse x) (catch Throwable _ nil))
    :else                        nil))

(defn instant->micros [^Instant i]
  (+ (* (.getEpochSecond i) 1000000) (quot (.getNano i) 1000)))

(defn ->micros [x]
  (some-> (->instant x) instant->micros))

(defn micros->iso [m]
  (when m
    (let [m (long m)]
      (str (Instant/ofEpochSecond (Math/floorDiv m 1000000) (* 1000 (Math/floorMod m 1000000)))))))

(defn read-micros [^ResultSet rs ^String col]
  (let [v (.getLong rs col)]
    (when-not (.wasNull rs) v)))
