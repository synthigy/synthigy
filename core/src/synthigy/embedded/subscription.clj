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

(ns synthigy.embedded.subscription
  "Interest and event translation for in-process change subscriptions under
   embedded/watch and watch-query."
  (:require
   [synthigy.dataset.delta :as delta]
   [synthigy.dataset.sql.query :as sql-query]
   [synthigy.engine.subscription :as subs]))

(defn assert-ready!
  "Throw unless the delta registry is live — necessary, not sufficient: a live
   registry still doesn't guarantee drainer delivery."
  []
  (when-not (delta/ready?)
    (throw (ex-info (str "Delta registry is not live — start :synthigy/subscriptions "
                         "(or the module that owns it) before subscribing. NOTE: a "
                         "live registry still does not guarantee delivery; that "
                         "needs the plug drainer running.")
                    {:code "DELTA_NOT_READY"}))))

(defn interest
  "Translate a friendly interest into the xid-keyed descriptor
   `delta/subscribe!` matches on; needs a bound principal."
  [{:keys [records relations entities operations] :as spec}]
  (let [known #{:records :relations :entities :operations}
        unknown (seq (remove known (keys spec)))
        _ (when unknown
            (throw (ex-info (str "interest: unsupported key(s) " (pr-str unknown)
                                 ". Supported: " (pr-str (vec (sort known)))
                                 ". For model-deploy events use "
                                 "synthigy.dataset/add-model-watch!.")
                            {:code "INVALID_INTEREST"
                             :unsupported unknown
                             :supported known})))
        _ (when-not (or (seq records) (seq relations) (seq entities))
            (throw (ex-info (str "interest narrows nothing — delta treats an empty "
                                 "interest as a match-everything firehose. Name at "
                                 "least one of :records / :entities / :relations. "
                                 "If you genuinely want every envelope, call "
                                 "synthigy.dataset.delta/subscribe! directly.")
                            {:code "EMPTY_INTEREST"})))
        ops      (when (seq operations) (into #{} (map name) operations))
        ent-xids (when (seq entities)
                   (-> (subs/normalize-entity-item
                        (cond-> {:type "entity" :entities (mapv name entities)}
                          (seq operations) (assoc :operations (mapv name operations))))
                       :name-by-xid keys set))
        rel-xids (when (seq relations)
                   (-> (subs/normalize-relation-item
                        (cond-> {:type "relation" :relations (mapv name relations)}
                          (seq operations) (assoc :operations (mapv name operations))))
                       :name-by-xid keys set))
        recs     (when (seq records) (into #{} (map str) records))]
    (cond-> {}
      (seq ent-xids) (assoc :entity-xids ent-xids)
      (seq rel-xids) (assoc :relation-xids rel-xids)
      (seq recs)     (assoc :record-xids recs :endpoint-xids recs)
      (seq ops)      (assoc :ops ops))))

(defn envelope->events
  "Translate one plug envelope into 0..2 user-facing events; `roles` is a
   role-xid set resolved once at subscribe, never the ambient binding."
  ([envelope] (envelope->events envelope #{} #{}))
  ([envelope records] (envelope->events envelope records #{}))
  ([envelope records roles]
   (mapv :data (subs/translate-delta envelope
                                     (sql-query/attribute-key-index)
                                     (set records)
                                     roles))))
