(ns synthigy.core
  "SQLite backend entry point for Synthigy.

  This namespace loads all core functionality plus SQLite-specific implementations.

  ## Usage

  Run with :sqlite alias:
    clj -M:sqlite:dev

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
    ;; Substrate-owner Patcho lifecycle module. Registers
    ;; :synthigy/subscriptions.sqlite at load time. NOT auto-started —
    ;; opt-in by `(lifecycle/start! :synthigy/subscriptions.sqlite)`.
    ;; The observability substrate (`:synthigy/observability`) depends
    ;; on subscriptions and will pull it in transitively when started.
    ;; Bare-server profiles that don't load synthigy.core skip
    ;; substrate entirely, which is the bench-fast path.
    synthigy.subscriptions.sqlite
    synthigy.db
    ;; SQLite-specific namespaces
    synthigy.db.sqlite
    synthigy.iam.connector.sqlite
    synthigy.transit))
