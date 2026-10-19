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
