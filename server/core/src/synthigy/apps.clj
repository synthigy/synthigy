;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.apps
  "The :synthigy/apps module: the Synthigy Apps dataset for deployed clients."
  (:require
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.apps.deploy :as deploy]
   synthigy.apps.guard
   [synthigy.apps.output :as output]
   [synthigy.apps.setup :as setup]
   synthigy.files
   [synthigy.log :as log]
   [synthigy.supervisor :as supervisor]
   synthigy.oauth.persistence))

(defn start []
  (setup/level-model!)
  (patch/level! :synthigy/apps)
  (deploy/start!)
  (output/register!)
  (log/info {:id ::started :data {:action :started :subject :apps}} "Apps module started"))

(defn stop []
  (output/unregister!)
  (deploy/stop!)
  (log/info {:id ::stopped :data {:action :stopped :subject :apps}} "Apps module stopped"))

(lifecycle/register-module!
 :synthigy/apps
 {:depends-on [:synthigy/oauth.persistence :synthigy/files]
  :doc "The Synthigy Apps dataset: deployed client packages, variables and deployers"
  :start start
  :stop stop})

(supervisor/restartable! :synthigy/apps)
