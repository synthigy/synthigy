;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.license
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]))

(def known-entitlements #{:robotics :file-storage :process-modeling})

(defn entitlements
  "Set of entitlements the current license grants."
  []
  ;; ponytail: stub — a non-empty SYNTHIGY_LICENSE grants everything; signed keys with expiry when licensing ships
  (if (not-empty (some-> (env :synthigy-license) str/trim))
    known-entitlements
    #{}))

(defn licensed?
  [entitlement]
  (contains? (entitlements) entitlement))

(defn guard!
  "Throw :license-required unless `entitlement` is licensed — gated modules call this first in :start."
  [entitlement]
  (when-not (licensed? entitlement)
    (throw (ex-info (str "Requires a license covering " (name entitlement)
                         " — set SYNTHIGY_LICENSE and restart.")
                    {:code :license-required :entitlement entitlement}))))
