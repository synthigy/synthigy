;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.db.dialect
  "SQL text rewriting for db/Dialect template-sql implementations — templates are
   authored in Postgres dialect and each backend adapts them."
  (:require
   [clojure.string :as str]))

(def quote-chars
  "Opening quote → its closer. A doubled closer escapes itself inside the run."
  {\' \' \" \" \` \`})

(defn map-code
  "Apply `f` to every run of `sql` outside quoted literals and identifiers."
  [^String sql f]
  (let [n  (count sql)
        sb (StringBuilder.)]
    (loop [i 0 start 0]
      (if (>= i n)
        (do (.append sb ^String (f (subs sql start n)))
            (.toString sb))
        (if-let [closer (quote-chars (.charAt sql i))]
          (let [end (loop [j (inc i)]
                      (cond
                        (>= j n) j
                        (and (= (.charAt sql j) closer)
                             (< (inc j) n)
                             (= (.charAt sql (inc j)) closer))
                        (recur (+ j 2))
                        (= (.charAt sql j) closer) (inc j)
                        :else (recur (inc j))))
                end (min end n)]
            (.append sb ^String (f (subs sql start i)))
            (.append sb (subs sql i end))
            (recur end end))
          (recur (inc i) start))))))

(defn strip-casts
  "Drop Postgres `::type` casts."
  [sql]
  (map-code sql #(str/replace % #"::\w+" "")))

(defn ilike->like
  "Rewrite `ILIKE` to `LIKE` for backends whose LIKE is already case-insensitive."
  [sql]
  (map-code sql #(str/replace % #"(?i)\bilike\b" "LIKE")))
