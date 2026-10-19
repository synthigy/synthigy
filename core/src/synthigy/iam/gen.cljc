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
