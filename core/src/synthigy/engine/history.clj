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

(ns synthigy.engine.history
  "Audit-plug query engine — five ops, xid-native wire, no transport. Split
   out of the handler ns so requiring the engine doesn't drag in
   server.auth/OAuth."
  (:require
    [clojure.string :as str]
    [synthigy.audit :as audit]
    [synthigy.dataset.id :as id]
    [synthigy.iam.access :as access]))

(defn wire-get
  "Pull wire-key (kebab-case) from a string-keyed opts map, falling back to its
   snake_case wire variant."
  [opts wire-key]
  (or (get opts wire-key)
      (get opts (str/replace wire-key #"-" "_"))))

(def ^:private default-limit 100)
(def ^:private max-limit 10000)
(def ^:private default-lower-window-ms (* 30 24 60 60 1000)) ;; 30 days

(defn now-iso []
  (.toString (java.time.Instant/now)))

(defn ->iso
  "Coerce a timestamp-shaped value to an ISO-8601 string for TEXT-column
   comparisons."
  [v]
  (cond
    (nil? v) nil
    (string? v) v
    (instance? java.util.Date v) (.toString (.toInstant ^java.util.Date v))
    (instance? java.time.Instant v) (.toString v)
    :else (str v)))

(defn thirty-days-ago-iso []
  (.toString (.minusMillis (java.time.Instant/now) default-lower-window-ms)))

(defn coerce-limit
  [n]
  (-> (or n default-limit) long
      (max 1)
      (min max-limit)))

(defn normalize-between
  "[t1 t2] strings; upper bound required by contract, lower defaults to now-30d."
  [between]
  (let [[t1 t2] (if (sequential? between) between [nil nil])
        t1 (->iso t1)
        t2 (->iso t2)]
    (cond
      (nil? t2)
      (throw (ex-info "between upper bound is required"
                      {:code "BETWEEN_UPPER_REQUIRED"}))

      :else
      [(or t1 (thirty-days-ago-iso)) t2])))

(defmulti execute-op
  "Dispatch on the wire op string; each method invokes the bound
   audit/*audit-provider*."
  (fn [_provider op _opts] op))

(defmethod execute-op :default
  [_ op _]
  (throw (ex-info (str "Unknown history op: " op)
                  {:code "UNKNOWN_OP" :op op})))

(defmethod execute-op "get-at"
  [provider _ opts]
  (let [record-xid       (wire-get opts "record-xid")
        at               (wire-get opts "at")
        tenant           (wire-get opts "tenant")
        include-deleted? (wire-get opts "include-deleted?")]
    (when (str/blank? (str record-xid))
      (throw (ex-info "record-xid is required" {:code "RECORD_XID_REQUIRED"})))
    (audit/get-at provider {:record-xid record-xid
                            :at (or (->iso at) (now-iso))
                            :tenant tenant
                            :include-deleted? include-deleted?})))

(defmethod execute-op "events"
  [provider _ opts]
  (let [record-xid (wire-get opts "record-xid")
        between    (wire-get opts "between")
        tenant     (wire-get opts "tenant")
        limit      (wire-get opts "limit")
        track      (wire-get opts "track")
        [t1 t2]    (normalize-between between)]
    (audit/events provider {:record-xid record-xid
                            :between [t1 t2]
                            :tenant tenant
                            :limit (coerce-limit limit)
                            :track (some-> track keyword)})))

(defmethod execute-op "diff"
  [provider _ opts]
  (let [record-xid (wire-get opts "record-xid")
        from-ts    (->iso (wire-get opts "from-ts"))
        to-ts      (->iso (wire-get opts "to-ts"))
        tenant     (wire-get opts "tenant")]
    (when (str/blank? (str record-xid))
      (throw (ex-info "record-xid is required" {:code "RECORD_XID_REQUIRED"})))
    (when (or (str/blank? (str from-ts)) (str/blank? (str to-ts)))
      (throw (ex-info "from-ts and to-ts are required"
                      {:code "DIFF_TIMESTAMPS_REQUIRED"})))
    (audit/diff provider {:record-xid record-xid
                          :from-ts from-ts
                          :to-ts to-ts
                          :tenant tenant})))

(defmethod execute-op "timeline"
  [provider _ opts]
  (let [between  (wire-get opts "between")
        group-by (wire-get opts "group-by")
        tenant   (wire-get opts "tenant")
        limit    (wire-get opts "limit")
        [t1 t2]  (normalize-between between)]
    (audit/timeline provider {:between [t1 t2]
                              :group-by (or (some-> group-by keyword) :request)
                              :tenant tenant
                              :limit (coerce-limit limit)})))

(defmethod execute-op "since"
  [provider _ opts]
  (let [cursor (->iso (wire-get opts "cursor"))
        tenant (wire-get opts "tenant")
        limit  (wire-get opts "limit")
        track  (wire-get opts "track")]
    (when (str/blank? (str cursor))
      (throw (ex-info "cursor is required" {:code "CURSOR_REQUIRED"})))
    (audit/since provider {:cursor cursor
                           :tenant tenant
                           :limit (coerce-limit limit)
                           :track (some-> track keyword)})))

(defn principal-tenant-xid
  "MVP per Q11: the install's primary dataset xid is the (inescapable) tenant
   boundary."
  [_principal]
  (some-> (id/data :dataset/id) str))

(defn stringify-opt-keys
  "Shallow-convert an opts map's keys to strings so in-process callers can pass
   keyword opts; nested values are left untouched."
  [opts]
  (reduce-kv (fn [m k v] (assoc m (if (keyword? k) (name k) (str k)) v))
             {} (or opts {})))

(defn execute
  "Run a /history request body and return {:op op :result …} — the
   transport-free core shared by the HTTP handler and embedded."
  [body principal]
  (when (nil? audit/*audit-provider*)
    (throw (ex-info "No audit provider loaded — history is not available"
                    {:code "HISTORY_UNAVAILABLE" :status 404})))
  (let [op   (some-> (or (get body "op") (get body :op)) str)
        opts (stringify-opt-keys (or (get body "opts") (get body :opts)))]
    (when (str/blank? op)
      (throw (ex-info "op is required" {:code "OP_REQUIRED" :status 400})))
    (access/with-principal principal
      (let [tenant (principal-tenant-xid principal)
            ;; String key — wire-get only looks up strings; a keyword :tenant
            ;; here silently disables tenant pruning.
            full   (cond-> opts
                     tenant (assoc "tenant" tenant))]
        {:op op :result (execute-op audit/*audit-provider* op full)}))))

