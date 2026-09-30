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
