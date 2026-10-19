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

(ns synthigy.core
  "PostgreSQL backend entry point for Synthigy.

  This namespace loads all core functionality plus PostgreSQL-specific implementations.

  ## Usage

  Run with :postgres alias:
    clj -M:engine-postgres:dev

  Then:
    (require '[synthigy.core])
    (require '[patcho.lifecycle :as lifecycle])
    (lifecycle/start! :synthigy/database)"
  (:require
    ;; Shared core namespaces
    synthigy.data
    synthigy.dataset
    synthigy.dataset.core
    synthigy.dataset.encryption
    synthigy.dataset.enhance
    synthigy.dataset.operations
    synthigy.dataset.postgres
    synthigy.dataset.postgres.query
    ;; Backend's delta-plug lifecycle module. Registers
    ;; :synthigy/plug at load time — NOT auto-started; brought up as a
    ;; dep of :synthigy/server / :synthigy/bare-server (or directly via
    ;; `(lifecycle/start! :synthigy/plug)`). :synthigy/observability
    ;; and :synthigy/subscriptions both depend on it. The trigger-free
    ;; bench-fast path is now starting :synthigy/dataset alone — both
    ;; server profiles pull the plug.
    synthigy.subscriptions.postgres
    synthigy.dataset.sql.naming
    synthigy.db
    ;; PostgreSQL-specific namespaces
    synthigy.db.postgres
    synthigy.iam.connector.dataset
    synthigy.transit))
