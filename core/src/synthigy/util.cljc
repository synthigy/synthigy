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

(ns synthigy.util
  "Shared, dependency-free utility helpers for the Synthigy platform."
  #?(:clj (:require [clojure.pprint :as pprint])))

;; ── Logging ──────────────────────────────────────────────────────────────

#?(:clj
   (defn pprint-str
     "Pretty-print `data` to a string, for embedding a readable dump in a
      log message field — not for UI output."
     [data]
     (with-out-str (pprint/pprint data))))

;; ── Time durations ──────────────────────────────────────────────────────

(defn milliseconds [n] n)
(defn seconds [n] (* n 1000))
(defn minutes [n] (* n 60 1000))
(defn hours   [n] (* n 60 60 1000))
(defn days    [n] (* n 24 60 60 1000))

(defn now
  "Current instant as milliseconds since the Unix epoch
   (1970-01-01T00:00:00Z, UTC)."
  []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

