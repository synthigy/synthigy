;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
