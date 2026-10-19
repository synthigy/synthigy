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

(ns synthigy.server.console.session
  (:require
   [clojure.string :as str]
   [synthigy.iam.access :as access]
   [synthigy.iam.context :as iam.context]
   [synthigy.oauth.core :as oauth]))

(defn session-id
  [request]
  (not-empty (get-in request [:cookies "idsrv.session" :value])))

(defn current-session
  [request]
  (when-let [sid (session-id request)]
    (when-let [session (oauth/get-session sid)]
      (when (:active session)
        (assoc session :id sid)))))

(def ^:dynamic *reauth-max-age-ms*
  (* 5 60 1000))

(defn fresh?
  [session]
  (if-let [max-age *reauth-max-age-ms*]
    (if-let [^java.util.Date at (access/with-principal nil
                                  (some-> session :id oauth/get-session-authorized-at))]
      (< (- (System/currentTimeMillis) (.getTime at)) max-age)
      false)
    true))

(defn same-origin?
  [request]
  (let [sec-fetch (get-in request [:headers "sec-fetch-site"])
        origin    (get-in request [:headers "origin"])]
    (cond
      sec-fetch (contains? #{"same-origin" "none"} sec-fetch)
      origin    (= (str/replace origin #"^https?://" "")
                   (get-in request [:headers "host"]))
      :else     true)))

(def ^:private login-redirect
  {:status 303 :headers {"Location" "/console/login"}})

(defn touch!
  "Record activity on the caller's own session; never let it break the request."
  [session]
  (try
    (access/with-principal nil
      (oauth/touch-session! (:id session)))
    (catch Throwable _ nil)))

(defn wrap-session
  [handler]
  (fn [request]
    (if-let [session (current-session request)]
      (if (and (not= :get (:request-method request))
               (not (same-origin? request)))
        {:status 403 :headers {"Content-Type" "text/plain"}
         :body "Cross-origin request rejected"}
        (if-let [principal (some-> (:resource-owner session)
                                   iam.context/get-user-context)]
          (do
            (touch! session)
            (access/with-principal principal
              (handler (assoc request
                              :console/session session
                              :console/principal principal))))
          login-redirect))
      login-redirect)))
