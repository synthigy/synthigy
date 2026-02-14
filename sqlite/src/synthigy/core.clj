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
    synthigy.dataset.graphql
    synthigy.dataset.operations
    synthigy.dataset.sql.naming
    synthigy.dataset.sqlite
    synthigy.dataset.sqlite.query
    synthigy.db
    ;; SQLite-specific namespaces
    synthigy.db.sqlite
    synthigy.transit))
