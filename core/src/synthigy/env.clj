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

(ns synthigy.env
  (:require
   [babashka.fs :as fs]
   [clojure.string]
   [environ.core :refer [env]]))

(def home (str (fs/expand-home (env :synthigy-home "~/.synthigy"))))

(def db-dir
  "Where every local database file lives."
  (str home "/db"))

(def iam-root-url (env :synthigy-iam-root-url))

(def id-format
  "ID format preference (\"euuid\" or \"xid\"); verified against the stored format on startup."
  (env :synthigy-id-format))

(def trust-proxy
  "Comma-separated IPs/CIDRs of the reverse proxies whose X-Forwarded-For/-Host/-Proto are believed."
  (env :synthigy-server-trust-proxy))

(def allowed-origins
  "Extra browser origins allowed on top of the registered clients' redirect origins."
  (into #{} (remove empty?) (clojure.string/split (env :synthigy-server-allowed-origins "") #"\s*,\s*")))
