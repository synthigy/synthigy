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

(ns synthigy.log.store.ring
  "Bounded in-memory LogStore: the default until a durable store starts, and the memory module's store."
  (:require
   [synthigy.log.store.protocol :as protocol]
   [synthigy.log.store.signal :as signal])
  (:import
   [java.util.concurrent.atomic AtomicLong]))

(defrecord RingLogStore [buffer dropped max-size]
  protocol/LogStore

  (write-signal! [_ signal]
    (when signal
      (try (signal/push! buffer dropped max-size signal)
           (catch Throwable _)))
    nil)

  (recent [this {:keys [limit ns-pattern level since]}]
    (let [opts (cond-> {:order-by [:inst :desc] :limit (or limit signal/default-limit)}
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level)
                 since      (assoc :since since))]
      (protocol/search this opts)))

  (search [_ opts]
    (signal/run-search (seq @buffer) opts))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [opts (cond-> {:order-by [:inst :asc] :limit (or limit signal/default-limit)}
                 cursor     (assoc :since cursor)
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level))
          rows (protocol/search this opts)]
      {:rows rows :next-cursor (or (some-> (last rows) :inst) cursor)}))

  (clear! [_]
    (reset! buffer clojure.lang.PersistentQueue/EMPTY)
    (.set ^AtomicLong dropped 0)
    nil)

  (health [_]
    {:up?      true
     :backend  :ring
     :path     nil
     :rows     (long (count @buffer))
     :capacity (long max-size)
     :dropped  (.get ^AtomicLong dropped)})

  (snapshot [_]
    (vec @buffer)))

(defn create-ring
  ([] (create-ring nil))
  ([{:keys [size]}]
   (->RingLogStore (atom clojure.lang.PersistentQueue/EMPTY)
                   (AtomicLong. 0)
                   (long (or size (signal/env-ring-size))))))
