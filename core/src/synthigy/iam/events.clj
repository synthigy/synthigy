;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

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
