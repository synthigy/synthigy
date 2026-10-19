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

(ns synthigy.iam.events
  "Async pub/sub for IAM events (keypair rotation, user changes); consumed by
   oauth.store."
  (:require
    [clojure.core.async :as async]))

(defonce subscription
  (async/chan (async/sliding-buffer 10000)))

(defonce publisher
  (async/pub
    subscription
    (fn [{:keys [topic]
          :or {topic ::broadcast}}]
      topic)))

(defn publish
  "Publish an event to the IAM event system, routed to subscribers of :topic."
  [topic data]
  (async/put! subscription (assoc data :topic topic)))
