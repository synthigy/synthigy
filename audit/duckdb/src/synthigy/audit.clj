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

(ns synthigy.audit
  "Audit dimension: history in its own embedded DuckDB file."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.audit.duckdb :as duckdb]))

(defn open-provider
  "An isolated provider instance (`:path`, default in-memory); the lifecycle module binds its own."
  [{:keys [path]}]
  (duckdb/open-provider (or path ":memory:")))

(defn close-provider! [p]
  (duckdb/close-provider! p))

(lifecycle/register-module!
 :synthigy/audit
 {:depends-on [:synthigy/plug]
  :doc "Audit history in audit.duckdb"
  :setup   duckdb/setup
  :start   duckdb/start
  :stop    duckdb/stop
  :cleanup duckdb/cleanup})
