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

(ns synthigy.server.docs
  "The manual as markdown, from the engine it describes: GET /docs, /docs/<topic>, /docs/<topic>/<section>, behind /schema's auth."
  (:require
   [clojure.string :as str]
   [patcho.lifecycle :as lifecycle]
   [synthigy.boot :as boot]
   [synthigy.docs :as docs]
   [synthigy.engine :as engine]
   [synthigy.json :as json]
   [synthigy.oauth.authentication :as auth]))

(def prefix "/docs")

(defn markdown [body]
  {:status 200 :headers {"Content-Type" "text/markdown; charset=utf-8"} :body body})

(defn not-found [what available]
  {:status 404
   :headers {"Content-Type" "application/json"}
   :body (json/write-str {:error {:code "DOC_NOT_FOUND" :message (str "No " what) :available available}})})

(defn page
  "The response for `topic` and `sect` (both may be nil)."
  [topic sect]
  (cond
    (contains? #{nil "index" "readme"} topic)
    (markdown (str "Synthigy " boot/version "\n\n" (docs/rewrite-links (or (docs/source "README.md") "") nil)))

    :else
    (if-let [md (docs/source (docs/topic->file topic))]
      (if sect
        (if-let [s (docs/section md sect)]
          (markdown (docs/rewrite-links s topic))
          (not-found (str "section '" sect "' in " topic) (docs/section-ids md)))
        (markdown (docs/rewrite-links md topic)))
      (not-found (str "topic '" topic "'") (docs/topics)))))

(defn handler
  "GET /docs[/<topic>[/<section>]]; signed in as for /schema when IAM runs."
  [request]
  (let [iam-active? (lifecycle/started? :synthigy/iam)
        iam (when iam-active? (auth/authenticate-request request engine/platform-audience))]
    (cond
      (not= :get (:request-method request))
      {:status 405 :headers {"Content-Type" "application/json" "Allow" "GET"}
       :body (json/write-str {:error {:code "METHOD_NOT_ALLOWED" :message "Read the docs with GET"}})}

      (and iam-active? (not iam))
      {:status 401
       :headers {"Content-Type" "application/json" "WWW-Authenticate" (auth/challenge request)}
       :body (json/write-str {:error {:message "Unauthorized" :code "UNAUTHORIZED"}})}

      :else
      (let [[topic sect] (remove str/blank? (str/split (str/lower-case (subs (:uri request) (count prefix))) #"/"))]
        (page topic sect)))))
