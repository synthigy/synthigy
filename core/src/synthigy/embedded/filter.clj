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

(ns synthigy.embedded.filter
  "Filter-condition sugar over plain maps; port of synthigy.client.filter — keep
   operator names in step."
  (:refer-clojure :exclude [and or not in]))

(defn eq          [v] {:_eq v})
(defn neq         [v] {:_neq v})
(defn gt          [v] {:_gt v})
(defn ge          [v] {:_ge v})
(defn lt          [v] {:_lt v})
(defn le          [v] {:_le v})
(defn like        [p] {:_like p})
(defn ilike       [p] {:_ilike p})
(defn is-null     []  {:_is_null true})
(defn is-not-null []  {:_is_not_null true})

(defn in  [& vs] {:_in  (vec (flatten vs))})
(defn nin [& vs] {:_nin (vec (flatten vs))})

(defn and [& clauses] {:_and (vec clauses)})
(defn or  [& clauses] {:_or  (vec clauses)})
(defn not [clause]    {:_not clause})
