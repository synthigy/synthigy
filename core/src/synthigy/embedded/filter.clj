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
