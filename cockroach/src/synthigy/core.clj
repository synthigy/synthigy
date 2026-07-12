(ns synthigy.core
  "CockroachDB backend entry point for Synthigy.

  This namespace loads all core functionality plus CockroachDB-specific implementations.

  ## Usage

  Run with :crdb alias:
    clj -M:crdb:dev

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
    synthigy.dataset.cockroach
    synthigy.dataset.cockroach.query
    ;; Substrate-owner Patcho lifecycle module. Registers
    ;; :synthigy/subscriptions at load time. NOT auto-started —
    ;; opt-in by `(lifecycle/start! :synthigy/subscriptions)`.
    ;; The observability substrate (`:synthigy/observability`) depends
    ;; on subscriptions and will pull it in transitively when started.
    ;; Bare-server profiles that don't load synthigy.core skip substrate
    ;; entirely.
    synthigy.subscriptions.cockroach
    synthigy.iam.connector.cockroach
    synthigy.dataset.sql.naming
    synthigy.db
    ;; CockroachDB-specific namespaces
    synthigy.db.cockroach
    synthigy.transit))
