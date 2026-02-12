(ns synthigy.core
  "Stub entry point for compilation without a database backend.

  Use this when you need to compile code that requires synthigy.core
  but don't need actual database functionality.

  ## Usage

  Run with :stub alias:
    clj -M:stub

  For real database functionality, use :sqlite or :postgres alias instead.")

;; Empty stub - no database backend loaded
