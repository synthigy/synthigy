;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
