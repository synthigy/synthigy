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

(ns synthigy.server.profile
  "Canonical dependency lists for the two Synthigy server profiles, shared across backends."
  (:require [patcho.lifecycle :as lifecycle]))

(defn guard-mode!
  "Throw when the OTHER server mode is already running — both modes bind the
   same HTTP instance and port, so the second start silently steals it and
   the lifecycle report lies about what is serving."
  [starting other]
  (when (lifecycle/started? other)
    (throw (ex-info (str "Refusing to start " starting " while " other
                         " is running — same HTTP server instance and port. "
                         "Stop " other " first.")
                    {:code :server-mode-conflict
                     :starting starting
                     :running other}))))

(def full-deps
  "`:synthigy/server` — full HTTP server: data + plug + subscriptions
   + audit + IAM + OAuth. The IAM/OAuth surface comes entirely from
   :synthigy/oauth.persistence (→ :synthigy/oauth + :synthigy.iam/encryption);
   :synthigy/subscriptions itself depends only on dataset + plug and
   pulls NO IAM. For a data-only server without IAM, start
   :synthigy/bare-server (it keeps the live subscription plane)."
  [:synthigy.iam/audit
   :synthigy/plug
   :synthigy/subscriptions
   :synthigy/oauth.persistence])

(def full-doc "Full HTTP server — /data + IAM + OAuth + SSE + SPA")

(def bare-deps
  "`:synthigy/bare-server` — data-only server: dataset + the live delta
   plane (plug + subscriptions). No IAM, no OAuth, no audit module.
   Subscriptions depend only on dataset + plug, so /data/events works
   with allow-all access (identity via x-synthigy-client) and no audit
   persistence — best-effort live, no reconnect replay (replay reads the
   audit store, which is empty here). The History plane still lights up
   for free if the operator also starts :synthigy/audit — the
   shared route table (`synthigy.server.routes`) gates each optional route
   on the module actually being up."
  [:synthigy/log :synthigy/dataset :synthigy/plug :synthigy/subscriptions])

(def bare-doc "Data-only HTTP server — /data + /schema + live subscriptions, no IAM/OAuth")
