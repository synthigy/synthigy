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

(ns synthigy.log.store
  "Log-store dimension: logs and traffic in a local, size-capped SQLite file."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.log.store.sqlite :as sqlite]))

(defn open-store
  "An isolated store instance for `config`; the lifecycle module binds its own."
  [config]
  (sqlite/open-store config))

(defn close-store! [s]
  (sqlite/close-store! s))

(lifecycle/register-module!
 :synthigy.log/store
 {:depends-on [:synthigy/log]
  :doc "Logs + traffic in logs.db, capped by size and age"
  :setup   sqlite/setup
  :start   sqlite/start
  :stop    sqlite/stop
  :cleanup sqlite/cleanup})
