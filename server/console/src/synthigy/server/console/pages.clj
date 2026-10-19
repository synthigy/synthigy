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

(ns synthigy.server.console.pages
  (:require
   [synthigy.server.console.pages.apis :as apis]
   [synthigy.server.console.pages.apps :as apps]
   [synthigy.server.console.pages.connectors :as connectors]
   [synthigy.server.console.pages.groups :as groups]
   [synthigy.server.console.pages.providers :as providers]
   [synthigy.server.console.pages.roles :as roles]
   [synthigy.server.console.pages.users :as users]))

(defn all []
  [users/spec
   groups/spec
   roles/spec
   apps/spec
   apis/spec
   providers/spec
   connectors/spec])

(defn by-slug [slug]
  (some #(when (= slug (:slug %)) %) (all)))
