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
