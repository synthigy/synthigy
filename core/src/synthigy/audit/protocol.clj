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
