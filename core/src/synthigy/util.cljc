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

