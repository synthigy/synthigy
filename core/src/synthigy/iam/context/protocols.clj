;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
