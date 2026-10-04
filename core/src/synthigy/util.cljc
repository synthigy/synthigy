;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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

