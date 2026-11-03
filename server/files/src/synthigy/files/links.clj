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

(ns synthigy.files.links
  "Browser links to files: /files/<xid>/<v>/<name>, the version in the path."
  (:require
   [clojure.string :as str])
  (:import
   [java.net URLEncoder]
   [java.nio.charset StandardCharsets]))

(def route #"/files/([A-Za-z0-9]{22})/([0-9a-f]{16})(?:/[^/]*)?")

(defn slug
  "The version part of a link: the first 16 hex characters of the file's hash."
  [hash]
  (when (and (string? hash) (>= (count hash) 16))
    (str/lower-case (subs hash 0 16))))

(defn encode-name
  "`file-name` percent-encoded, everything but RFC 3986 unreserved characters."
  [file-name]
  (-> (URLEncoder/encode (str (or (not-empty file-name) "file")) StandardCharsets/UTF_8)
      (str/replace "+" "%20")
      (str/replace "*" "%2A")
      (str/replace "%7E" "~")))

(defn path [xid hash file-name]
  (str "/files/" xid "/" (slug hash) "/" (encode-name file-name)))
