;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.core
  "SQLite backend entry point for Synthigy.

  This namespace loads all core functionality plus SQLite-specific implementations.

  ## Usage

  Run with :sqlite alias:
    clj -M:engine-sqlite:dev

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
    synthigy.dataset.sql.naming
    synthigy.dataset.sqlite
    synthigy.dataset.sqlite.query
    ;; Backend's delta-plug lifecycle module. Registers
    ;; :synthigy/plug at load time — NOT auto-started; brought up as a
    ;; dep of :synthigy/server / :synthigy/bare-server (or directly via
    ;; `(lifecycle/start! :synthigy/plug)`). :synthigy/observability
    ;; and :synthigy/subscriptions both depend on it. The trigger-free
    ;; bench-fast path is now starting :synthigy/dataset alone — both
    ;; server profiles pull the plug.
    synthigy.subscriptions.sqlite
    synthigy.db
    ;; SQLite-specific namespaces
    synthigy.db.sqlite
    synthigy.iam.connector.dataset
    synthigy.transit))
