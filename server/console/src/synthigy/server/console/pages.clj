;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
