;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

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
