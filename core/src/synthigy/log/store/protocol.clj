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

(ns synthigy.log.store.protocol
  "LogStore contract and the `*log-store*` the store bridge writes to.")

(defprotocol LogStore
  "Contract for a log plug implementation."

  (write-signal! [this signal]
    "Consume one Telemere signal map; must not throw.")

  (recent [this opts]
    "Return up to `:limit` recent wire-schema rows, newest-first.")

  (search [this opts]
    "Filtered query over the store using a validated filter-map from `synthigy.log.query`.")

  (tail [this opts]
    "Cursor-based follow; returns {:rows [...] :next-cursor x}.")

  (clear! [this]
    "Empty the store.")

  (health [this]
    "Snapshot map describing store liveness; must not throw.")

  (snapshot [this]
    "Raw Telemere signals held by this store, oldest-first, for backend handoff."))

(defonce ^:dynamic *log-store* nil)
