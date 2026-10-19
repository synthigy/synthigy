;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

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
