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
