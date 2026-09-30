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
