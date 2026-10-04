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
  "Stub entry point for compilation without a database backend.

  Use this when you need to compile code that requires synthigy.core
  but don't need actual database functionality.

  ## Usage

  Run with :stub alias:
    clj -M:engine-stub

  For real database functionality, use :sqlite or :postgres alias instead.")

;; Empty stub - no database backend loaded
