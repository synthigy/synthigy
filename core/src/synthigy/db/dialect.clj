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
