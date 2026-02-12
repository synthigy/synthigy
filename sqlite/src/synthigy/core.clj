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
    synthigy.db
    synthigy.iam
    synthigy.iam.access
    synthigy.iam.context
    synthigy.iam.encryption
    synthigy.transit
    ;; SQLite-specific namespaces
    synthigy.db.sqlite
    synthigy.dataset.sqlite
    synthigy.iam.audit))
