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

(ns synthigy.log.query
  "Filter-map spec + dispatch into the bound observability store."
  (:require
   [clojure.set]
   [synthigy.log.store :as store]))

;;; ============================================================================
;;; Spec — the filter-map vocabulary
;;; ============================================================================

(def allowed-top-level-keys
  "The keys the filter map accepts."
  #{:where :since :until :limit :order-by :group-by :count? :bucket-ms})

(def comparison-ops
  #{:= :!= :> :< :>= :<=})

(def membership-ops
  #{:in})

(def text-ops
  "Take a String value."
  #{:contains :icontains :starts-with :ends-with})

(def regex-ops
  "Take a `java.util.regex.Pattern` value."
  #{:matches})

(def presence-ops
  "No second arg in the tuple — `[:exists?]` / `[:absent?]`."
  #{:exists? :absent?})

(def array-ops
  "Membership against an array-valued field: `[:has :system]`."
  #{:has})

(def all-ops
  (clojure.set/union comparison-ops membership-ops text-ops regex-ops
                     presence-ops array-ops))

(def built-in-columns
  "Top-level wire-schema fields addressable as bare keywords in `:where`."
  #{:level :ns :id :msg :request-id :user-xid :tenant
    :host :error-class :error-msg :error-trace :inst :v :topics})

(def default-limit 100)

;;; ============================================================================
;;; Validation
;;; ============================================================================

(defn field-ref? [x]
  (or (and (keyword? x) (contains? built-in-columns x))
      (and (vector? x)
           (>= (count x) 2)
           (contains? #{:data :ctx} (first x))
           (every? keyword? x))))

(defn tuple? [v]
  (and (vector? v) (keyword? (first v)) (contains? all-ops (first v))))

(defn validate-tuple [field tuple]
  (let [[op arg :as t] tuple
        n-args (dec (count t))]
    (cond
      (contains? presence-ops op)
      (when-not (= 0 n-args)
        (throw (ex-info (str "Operator " op " takes no arguments: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? array-ops op)
      (when-not (and (= 1 n-args) (or (keyword? arg) (string? arg)))
        (throw (ex-info (str "Operator " op " requires one keyword/string argument: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? membership-ops op)
      (when-not (set? arg)
        (throw (ex-info (str "Operator " op " requires a set value: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? regex-ops op)
      (when-not (instance? java.util.regex.Pattern arg)
        (throw (ex-info (str "Operator " op " requires a regex pattern: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? text-ops op)
      (when-not (string? arg)
        (throw (ex-info (str "Operator " op " requires a String: " field " " tuple)
                        {:field field :tuple tuple :op op})))

      (contains? comparison-ops op)
      (when (nil? arg)
        (throw (ex-info (str "Operator " op " requires a non-nil argument: " field " " tuple)
                        {:field field :tuple tuple :op op}))))))

(defn validate-where [where]
  (when (some? where)
    (when-not (map? where)
      (throw (ex-info ":where must be a map" {:where where})))
    (doseq [[field v] where]
      (when-not (field-ref? field)
        (throw (ex-info (str "Unknown field reference: " (pr-str field)
                             ". Use a built-in column keyword "
                             "(" built-in-columns ") "
                             "or a [:data …] / [:ctx …] path vector.")
                        {:field field :where where})))
      (cond
        (tuple? v)  (validate-tuple field v)
        (set? v)    nil          ; bare set = [:in …]
        :else       nil))))      ; bare value = equality

(defn validate-since-until [k v]
  (when (some? v)
    (when-not (string? v)
      (throw (ex-info (str k " must be a duration string (e.g. \"10m\") or ISO instant")
                      {k v})))))

(defn validate-order-by [order-by]
  (when (some? order-by)
    (when-not (and (vector? order-by) (= 2 (count order-by))
                   (#{:asc :desc} (second order-by)))
      (throw (ex-info ":order-by must be [field :asc] or [field :desc]"
                      {:order-by order-by})))
    (when-not (or (and (keyword? (first order-by))
                       (contains? built-in-columns (first order-by)))
                  (and (vector? (first order-by))
                       (contains? #{:data :ctx} (ffirst (vector (first order-by))))))
      (throw (ex-info (str ":order-by field must be a built-in column or "
                           "a [:data …] / [:ctx …] path")
                      {:order-by order-by})))))

(defn validate-limit [limit]
  (when (some? limit)
    (when-not (and (integer? limit) (pos? limit))
      (throw (ex-info ":limit must be a positive integer"
                      {:limit limit})))))

(defn validate-filter-map
  "Throw `ex-info` if `filter-map` violates the spec."
  [filter-map]
  (when-not (map? filter-map)
    (throw (ex-info "Filter must be a map" {:got filter-map})))
  (let [unknown (clojure.set/difference (set (keys filter-map)) allowed-top-level-keys)]
    (when (seq unknown)
      (throw (ex-info (str "Unknown top-level filter keys: " unknown
                           ". Allowed: " allowed-top-level-keys)
                      {:unknown unknown :allowed allowed-top-level-keys}))))
  (validate-where    (:where filter-map))
  (validate-since-until :since (:since filter-map))
  (validate-since-until :until (:until filter-map))
  (validate-limit    (:limit filter-map))
  (validate-order-by (:order-by filter-map))
  (when (and (:count? filter-map) (not (boolean? (:count? filter-map))))
    (throw (ex-info ":count? must be boolean" {:count? (:count? filter-map)})))
  (when-let [b (:bucket-ms filter-map)]
    (when-not (and (integer? b) (pos? b))
      (throw (ex-info ":bucket-ms must be a positive integer (milliseconds)"
                      {:bucket-ms b}))))
  nil)

;;; ============================================================================
;;; Public API — query via *log-store*
;;; ============================================================================

(defn require-store []
  (or @#'store/*log-store*
      (throw (ex-info "No log store bound — call synthigy.log/install! first"
                      {:cause :no-log-store}))))

(defn query
  "Validate `filter-map` and execute it against the currently bound
   `*log-store*`."
  [filter-map]
  (validate-filter-map filter-map)
  (store/search (require-store) filter-map))

(defn current-store-backend
  "Keyword identifying the bound store's backend, or nil."
  []
  (when-let [s @#'store/*log-store*]
    (try (:backend (store/health s)) (catch Throwable _ nil))))

;;; ============================================================================
;;; Sugar — thin shortcuts to common filter shapes
;;; ============================================================================

(defn lifecycle
  "All signals for one request, oldest-first."
  [request-id]
  (query {:where {:request-id request-id} :order-by [:inst :asc]}))

(defn errors-since
  "Error+fatal signals from `since` (duration string or ISO instant) until now."
  ([since] (errors-since since default-limit))
  ([since limit]
   (query {:where {:level #{:error :fatal}}
           :since since
           :limit limit})))

