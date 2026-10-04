;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
