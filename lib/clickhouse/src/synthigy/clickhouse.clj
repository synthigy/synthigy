;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.clickhouse
  "ClickHouse over plain HTTP: config, client, statements, parameters, JSONEachRow — shared by the audit and log-store modules."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [jsonista.core :as jsonista]
   [synthigy.timestamp :as timestamp])
  (:import
   [java.net URI URLEncoder]
   [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
    HttpResponse$BodyHandlers]
   [java.nio.charset StandardCharsets]
   [java.time Duration Instant]
   [java.util Base64]))

(def default-config
  {:database           "synthigy"
   :buffer-size        8192
   :batch-rows         500
   :batch-ms           1000
   :log-retention-days 30
   :retry-attempts     5
   :retry-min-ms       250
   :retry-max-ms       30000
   :connect-timeout-ms 5000
   :request-timeout-ms 30000
   :close-timeout-ms   2000})

(defn parse-int-or [s fallback]
  (or (try (some-> s str/trim Integer/parseInt) (catch Throwable _ nil))
      fallback))

(defn env-config
  "CLICKHOUSE_* over the defaults; throws without CLICKHOUSE_URL."
  []
  (let [url (env :clickhouse-url)]
    (when (str/blank? url)
      (throw (ex-info "ClickHouse requires CLICKHOUSE_URL" {:cause :missing-config})))
    (merge default-config
           {:url         (str/replace url #"/+$" "")
            :database    (or (env :clickhouse-db) (:database default-config))
            :user        (env :clickhouse-user)
            :password    (env :clickhouse-password)
            :buffer-size (parse-int-or (env :clickhouse-buffer-size) (:buffer-size default-config))
            :batch-rows  (parse-int-or (env :clickhouse-batch-rows) (:batch-rows default-config))
            :batch-ms    (parse-int-or (env :clickhouse-batch-ms) (:batch-ms default-config))
            :log-retention-days (parse-int-or (env :clickhouse-log-retention-days) (:log-retention-days default-config))})))

(defn basic-auth-header [user password]
  (when-not (str/blank? user)
    (str "Basic " (.encodeToString (Base64/getEncoder)
                                   (.getBytes (str user ":" (or password "")) StandardCharsets/UTF_8)))))

(defn new-client ^HttpClient [{:keys [connect-timeout-ms]}]
  ;; process-wide and idempotent: without it the JDK sends h2c upgrade headers and CH silently eats INSERT bodies
  (System/setProperty "jdk.httpclient.HttpClient.protocolVersion" "HTTP_1_1")
  (-> (HttpClient/newBuilder)
      (.version java.net.http.HttpClient$Version/HTTP_1_1)
      (.connectTimeout (Duration/ofMillis (long connect-timeout-ms)))
      .build))

(defn url-encode [^String s]
  (URLEncoder/encode s "UTF-8"))

(defn params-qs [params]
  (str/join (map (fn [[n {:keys [value]}]] (str "&param_" n "=" (url-encode value))) params)))

(defn send! ^String [^HttpClient client {:keys [user password request-timeout-ms]} ^URI uri build-body]
  (let [builder (-> (HttpRequest/newBuilder)
                    (.uri uri)
                    (.timeout (Duration/ofMillis (long request-timeout-ms)))
                    build-body)
        builder (if-let [auth (basic-auth-header user password)]
                  (.header builder "Authorization" auth)
                  builder)
        resp    (.send client (.build builder) (HttpResponse$BodyHandlers/ofString))]
    (when (>= (.statusCode resp) 400)
      (throw (ex-info (str "ClickHouse responded " (.statusCode resp))
                      {:status (.statusCode resp) :body (.body resp) :uri (str uri)})))
    (.body resp)))

(defn post!
  "POST `body` for `query`; `settings` become URL settings."
  [client {:keys [url database] :as config} ^String query ^String body content-type settings]
  ;; insert_deduplicate=0: CH 24.8+ would drop an identical second block
  (send! client config
         (URI/create (str url "/?database=" (url-encode database)
                          "&query=" (url-encode query)
                          "&output_format_json_quote_64bit_integers=0"
                          "&insert_deduplicate=0"
                          (str/join (for [[k v] settings] (str "&" k "=" v)))))
         (fn [^java.net.http.HttpRequest$Builder b]
           ;; an explicit Accept is what stops CH silently dropping an ndjson body
           (-> b
               (.header "Content-Type" content-type)
               (.header "Accept" "*/*")
               (.POST (HttpRequest$BodyPublishers/ofString body StandardCharsets/UTF_8))))))

(defn query!
  "GET a read-only parameterized query; `params` is `{name {:type Kw :value String}}`."
  [client {:keys [url database] :as config} ^String query params]
  (send! client config
         (URI/create (str url "/?database=" (url-encode database)
                          "&query=" (url-encode query)
                          "&output_format_json_quote_64bit_integers=0"
                          (params-qs params)))
         (fn [^java.net.http.HttpRequest$Builder b]
           (-> b (.header "Content-Type" "text/plain; charset=utf-8") (.GET)))))

(defn command!
  "POST a parameterized statement (ALTER, DELETE…) with URL `settings`."
  [client {:keys [url database] :as config} ^String statement params settings]
  (send! client config
         (URI/create (str url "/?database=" (url-encode database)
                          (params-qs params)
                          (str/join (for [[k v] settings] (str "&" k "=" v)))))
         (fn [^java.net.http.HttpRequest$Builder b]
           (.POST b (HttpRequest$BodyPublishers/ofString statement StandardCharsets/UTF_8)))))

(defn exec-ddl! [client {:keys [url] :as config} ^String statement]
  (send! client config (URI/create url)
         (fn [^java.net.http.HttpRequest$Builder b]
           (.POST b (HttpRequest$BodyPublishers/ofString statement StandardCharsets/UTF_8))))
  nil)

(defn create-db-ddl [database]
  (str "CREATE DATABASE IF NOT EXISTS " database))

;; DateTime64(3) JSONEachRow silently drops rows whose inst lacks millis
(def instant-formatter
  (-> (java.time.format.DateTimeFormatterBuilder.) (.appendInstant 3) (.toFormatter)))

(defn inst->ch-string
  "Instant/Date/string as ISO with exactly three millis digits; nil is the epoch (columns are non-Nullable)."
  [inst]
  (cond
    (nil? inst)                         "1970-01-01T00:00:00.000Z"
    (instance? Instant inst)            (.format instant-formatter ^Instant inst)
    (instance? java.sql.Timestamp inst) (.format instant-formatter (.toInstant ^java.sql.Timestamp inst))
    (instance? java.util.Date inst)     (.format instant-formatter (.toInstant ^java.util.Date inst))
    (string? inst)                      (if (re-find #"\.\d{1,9}" inst)
                                          inst
                                          (try (.format instant-formatter (Instant/parse inst))
                                               (catch Throwable _ inst)))
    :else                               (str inst)))

(defn ch->iso
  "A DateTime64 as CH renders it (`2026-09-28 12:00:00.000`, UTC) as an ISO instant string."
  [s]
  (when s (str (timestamp/->instant s))))

(defn str-or-empty [v] (if (some? v) (str v) ""))

(def write-mapper (jsonista/object-mapper {}))

(defn rows->ndjson [rows]
  (let [sb (StringBuilder.)]
    (doseq [r rows]
      (.append sb ^String (jsonista/write-value-as-string r write-mapper))
      (.append sb \newline))
    (.toString sb)))

(defn insert! [client config table rows settings]
  (when (seq rows)
    (post! client config (str "INSERT INTO " (:database config) "." table " FORMAT JSONEachRow")
           (rows->ndjson rows) "application/x-ndjson; charset=utf-8" settings)))

(def read-mapper (jsonista/object-mapper {:decode-key-fn keyword}))

(defn parse-rows [^String body]
  (->> (str/split-lines body)
       (remove str/blank?)
       (mapv #(jsonista/read-value % read-mapper))))

(defn parse-count [x]
  (cond (integer? x) x
        (string? x)  (try (Long/parseLong x) (catch Throwable _ nil))
        :else        nil))

(defn kebab->snake [k]
  (str/replace (name k) "-" "_"))

(defn new-params [] (atom {:idx 0 :params {}}))

(defn bind!
  "Register `v` as a query parameter of `ch-type`; returns its `{pN:Type}` placeholder."
  [state v ch-type]
  (let [n        (:idx (swap! state update :idx inc))
        nm       (str "p" n)
        rendered (cond (boolean? v) (str v)
                       (keyword? v) (subs (str v) 1)
                       :else        (str v))]
    (swap! state assoc-in [:params nm] {:type ch-type :value rendered})
    (str "{" nm ":" (name ch-type) "}")))

(defn row-count [client config table]
  (some-> (parse-rows (query! client config (str "SELECT count() AS c FROM " (:database config) "." table
                                                 " FORMAT JSONEachRow")
                              {}))
          first :c parse-count))
