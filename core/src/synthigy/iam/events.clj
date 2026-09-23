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
