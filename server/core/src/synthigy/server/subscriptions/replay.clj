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

(ns synthigy.server.subscriptions.replay
  "Reconstruct delta envelopes from the durable audit store so a reconnecting
   SSE subscriber recovers exactly what it missed since its last :seq."
  (:require
    [synthigy.audit.protocol :as audit]))

(defn rows->entity-envelopes
  "Regroup attribute-grain audit rows by :seq into entity envelopes shaped like
   live ones."
  [rows]
  (->> rows
       (group-by :seq)
       (map (fn [[sq grp]]
              (let [r     (first grp)
                    after (into {} (for [{a :attribute-xid v :value} grp
                                         :when (not= "__delete__" a)]
                                     [(keyword a) v]))]
                {:seq   sq
                 :delta {:type (keyword "entity" (:op r))
                         :data {:ts         (:ts r)
                                :record-xid (:record-xid r)
                                :entity-xid (:entity-xid r)
                                :tenant     (:tenant-xid r)
                                :scope      (:scope r)
                                :actor      (:actor r)
                                :request    (:request r)
                                :txid       (:txid r)
                                :before     {}
                                :after      after}}})))))

(defn rows->relation-envelopes [rows]
  (map (fn [r]
         {:seq     (:seq r)
          :element (:relation-xid r)
          :delta   {:type (keyword "relation" (:op r))
                    :data {:ts       (:ts r)
                           :from-xid (:from-xid r)
                           :to-xid   (:to-xid r)
                           :tenant   (:tenant-xid r)
                           :scope    (:scope r)
                           :actor    (:actor r)
                           :request  (:request r)
                           :txid     (:txid r)}}})
       rows))

(defn replay-envelopes
  "Envelopes with :seq > cursor-seq, oldest-first, from the pending queue and both audit tracks."
  [cursor-seq limit]
  (let [pending  audit/*pending-envelopes*
        provider audit/*audit-provider*
        base     (cond-> {:seq cursor-seq} limit (assoc :limit limit))
        ;; pending before the store — a row persisted between the two reads then shows up twice, never zero times
        queued   (when pending
                   (concat (pending cursor-seq (or limit 10000) :entity)
                           (pending cursor-seq (or limit 10000) :relation)))
        stored   (when provider
                   (concat (rows->entity-envelopes (audit/since provider (assoc base :track :entity)))
                           (rows->relation-envelopes (audit/since provider (assoc base :track :relation)))))]
    (vals (reduce (fn [m e] (if (contains? m (:seq e)) m (assoc m (:seq e) e)))
                  (sorted-map)
                  (concat queued stored)))))
