;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.server.console.icon
  (:require
   [hiccup2.core :as hiccup]
   [synthigy.server.console.glyphs :as glyphs]))

(def ^:private raw-glyphs
  (update-vals glyphs/glyphs hiccup/raw))

(defn icon
  ([k] (icon k nil))
  ([k attrs]
   [:ty-icon attrs (get raw-glyphs k)]))
