;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.history
  "Ring handler for the /history endpoint — audit plug query surface,
   active only when the observability plug binds *audit-provider*."
  (:require
    [clojure.string :as str]
    [patcho.lifecycle :as lifecycle]
    [synthigy.audit.protocol :as audit]
    [synthigy.engine.history :as history]
    [synthigy.json :as json]
    [synthigy.log :as log]
    [synthigy.oauth.authentication :as auth]))
(defn parse-body
  "Parse the /history request body, keys preserved as strings so
   json/read-str-raw doesn't keywordize selection keys like \"Movie Actor\"."
  [request]
  (try
    (let [body-val (:body request)
          body-str (cond
                     (nil? body-val) nil
                     (string? body-val) body-val
                     :else (slurp body-val))]
      (when (and body-str (not= body-str ""))
        (json/read-str-raw body-str)))
    (catch Throwable e
      (log/warn {:id ::body-parse-failed} (.getMessage e))
      nil)))

(defn json-response
  [status body]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/->json body)})

(defn error
  [status code msg & [extra]]
  (json-response status (merge {:error {:message msg :code code}} extra)))

(defn handler [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam         (when iam-active? (auth/authenticate-request request))]
    (cond
      (nil? audit/*audit-provider*)
      (error 404 "HISTORY_UNAVAILABLE"
             "No audit provider loaded — /history endpoint is not active")

      (and iam-active? (not iam))
      (-> (error 401 "UNAUTHORIZED" "Unauthorized")
          (assoc-in [:headers "WWW-Authenticate"] (auth/challenge request)))

      :else
      (let [body (parse-body request)
            op   (some-> (get body "op") str)]
        (cond
          (str/blank? op)
          (error 400 "OP_REQUIRED" "op is required")

          :else
          (try
            (json-response 200 (history/execute body (:principal iam)))
            (catch clojure.lang.ExceptionInfo e
              (let [d (ex-data e)
                    status (or (:status d) 400)]
                (log/info {:id ::op-rejected
                           :data {:action :rejecting :subject :history :op op
                                  :code (:code d)}}
                          (.getMessage e))
                (error status (or (:code d) "BAD_REQUEST") (.getMessage e))))
            (catch Throwable e
              (log/error! {:id ::op-failed
                           :data {:action :running :subject :history :op op}} e)
              (error 500 "INTERNAL_ERROR" (.getMessage e)))))))))
