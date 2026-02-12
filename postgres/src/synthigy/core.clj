(ns synthigy.core
  "PostgreSQL backend entry point for Synthigy.

  This namespace loads all core functionality plus PostgreSQL-specific implementations.

  ## Usage

  Run with :postgres alias:
    clj -M:postgres:dev

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
    synthigy.db
    synthigy.iam
    synthigy.iam.access
    synthigy.iam.context
    synthigy.iam.encryption
    synthigy.transit
    ;; PostgreSQL-specific namespaces
    synthigy.db.postgres
    synthigy.dataset.postgres
    synthigy.iam.audit))
