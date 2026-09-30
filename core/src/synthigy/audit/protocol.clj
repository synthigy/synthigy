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

(ns synthigy.audit.protocol
  "AuditProvider contract and the dynvars the drainer and /history share.")

(defonce ^:dynamic *audit-provider* nil)

;; Bound by the drainer to (fn [cursor-seq limit track]) → audited envelopes
;; sequenced but not yet persisted; replay reads it before the provider.
(defonce ^:dynamic *pending-envelopes* nil)

;; Bound by the drainer to its open tx; providers must use it (when bound) so
;; queue DELETE + audit INSERTs stay atomic on single-writer backends.
(def ^:dynamic *audit-tx* nil)

;; Bound by the drainer to (fn []) → {:held n :oldest inst-or-nil}, the
;; audited deltas waiting in pending for the provider; Dataline health reads it.
(defonce ^:dynamic *pending-stats* nil)

(defonce persist-state (atom {:failing-since nil :error nil}))

(defn persist-failed!
  "Record that writing a pending batch to the provider failed; keeps the first failure time."
  [^Throwable e]
  (swap! persist-state (fn [s] {:failing-since (or (:failing-since s) (java.time.Instant/now))
                                :error         (or (ex-message e) (str (class e)))})))

(defn persist-ok! []
  (when (:failing-since @persist-state)
    (reset! persist-state {:failing-since nil :error nil})))

(defprotocol AuditProvider
  "Audit plug contract: batch writers called synchronously by the
  drainer, readers serving the `/history` endpoint."

  (write-entity-deltas! [this envelopes]
    "Write a drainer batch of entity-track envelopes durably.")

  (write-relation-deltas! [this envelopes]
    "Write a drainer batch of relation-track envelopes durably.")

  (get-at [this opts]
    "Return a record's state as of timestamp T.")

  (events [this opts]
    "Return events for a record (or any record) over a time range.")

  (diff [this opts]
    "Diff a record's state between two timestamps.")

  (timeline [this opts]
    "Return events grouped by :request, :actor, or :scope.")

  (since [this opts]
    "Return events strictly after a cursor timestamp, oldest-first.")

  (erase! [this opts]
    "Delete a record's history — its entity rows and relation rows naming it — returning {:entity n :relation n}."))
