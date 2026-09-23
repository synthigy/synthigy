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

(ns synthigy.iam.context.protocols)

(defprotocol UserContextProvider
  "Pluggable user-context lookup — implementations must return the same
   principal shape as `get-user-details`."

  (lookup-user [this identifier]
    "Look up user context by id, username, or _eid; nil when not found.")

  (invalidate-user [this identifier]
    "Invalidate cached user context for the given identifier.")

  (clear-cache [this]
    "Clear all cached user contexts.")

  (start! [this]
    "Start the provider lifecycle.")

  (stop! [this]
    "Stop the provider and clean up resources."))
