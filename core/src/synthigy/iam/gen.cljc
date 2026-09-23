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

(ns synthigy.iam.gen
  (:require
   [nano-id.core :as nano-id]))

(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZ"]
  (def client-id (nano-id/custom alphabet 48)))

(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890-"]
  (def client-secret (nano-id/custom alphabet 48)))

(def scope-name-pattern
  #"^[a-z][a-z0-9]*([._:-][a-z0-9]+)*$")

(defn valid-scope-name?
  "Scope names are lowercase and separator-joined — `dataset.read`, `api:write`."
  [s]
  (boolean (and (string? s) (re-matches scope-name-pattern s))))

(def scope-name-message
  (str "Scope names must be lowercase, with no spaces — words joined by "
       ". _ : or - (for example dataset.read or user_admin)."))
