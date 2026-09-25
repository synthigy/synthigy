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

(ns synthigy.env
  (:require
   [babashka.fs :as fs]
   [clojure.string]
   [environ.core :refer [env]]))

(def home (str (fs/expand-home (env :synthigy-home "~/.synthigy"))))
(def pid (str home "/pid"))
(def log-dir (str (fs/absolutize (env :synthigy-log-dir (str home "/logs")))))
(def config-dir (str (fs/absolutize (env :synthigy-config-dir (str home "/config")))))
(def git-dir (str (fs/absolutize (env :synthigy-git-dir (str home "/git")))))

(def iam-root-url (env :synthigy-iam-root-url))

(def id-format
  "ID format preference (\"euuid\" or \"xid\"); verified against the stored format on startup."
  (env :synthigy-id-format))

(def login-page-path
  "Fallback custom login surface at /login/*, overridable per-client."
  (env :synthigy-iam-login-page-path))

(def trust-proxy
  "Trust X-Forwarded-For for the client IP; set only when a reverse proxy fronts
   this server, since a client can forge the header."
  (boolean (env :synthigy-server-trust-proxy)))

(def allowed-origins
  "Extra browser origins allowed on top of the registered clients' redirect origins."
  (into #{} (remove empty?) (clojure.string/split (env :synthigy-server-allowed-origins "") #"\s*,\s*")))
