;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.env
  (:require
   [babashka.fs :as fs]
   [clojure.string]
   [environ.core :refer [env]]))

(def home (str (fs/expand-home (env :synthigy-home "~/.synthigy"))))

(def db-dir
  "Where local database files live by default: beside SQLITE_PATH when set, else <home>/db."
  (str (if-let [p (not-empty (env :sqlite-path))]
         (fs/parent (fs/absolutize (fs/expand-home p)))
         (str home "/db"))))
(def pid (str home "/pid"))
(def log-dir (str (fs/absolutize (env :synthigy-log-dir (str home "/logs")))))
(def config-dir (str (fs/absolutize (env :synthigy-config-dir (str home "/config")))))
(def git-dir (str (fs/absolutize (env :synthigy-git-dir (str home "/git")))))

(def iam-root-url (env :synthigy-iam-root-url))

(def id-format
  "ID format preference (\"euuid\" or \"xid\"); verified against the stored format on startup."
  (env :synthigy-id-format))

(def trust-proxy
  "Trust X-Forwarded-For for the client IP; set only when a reverse proxy fronts
   this server, since a client can forge the header."
  (boolean (env :synthigy-server-trust-proxy)))

(def allowed-origins
  "Extra browser origins allowed on top of the registered clients' redirect origins."
  (into #{} (remove empty?) (clojure.string/split (env :synthigy-server-allowed-origins "") #"\s*,\s*")))
