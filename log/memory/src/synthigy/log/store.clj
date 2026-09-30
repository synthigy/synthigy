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
  "Log-store dimension: the in-memory ring, with in-memory traffic."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.log.store.protocol :as protocol]
   [synthigy.log.store.ring :as ring]))

(defn open-store
  "An isolated store instance; the lifecycle module binds its own."
  [config]
  (ring/create-ring config))

(defn close-store! [_])

(lifecycle/register-module!
 :synthigy.log/store
 {:depends-on [:synthigy/log]
  :doc "Logs in a bounded in-memory ring; traffic stays in memory"
  :start (fn [] (alter-var-root #'protocol/*log-store* #(or % (ring/create-ring))))
  :stop  (fn [])})
