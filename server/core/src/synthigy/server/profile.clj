;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
