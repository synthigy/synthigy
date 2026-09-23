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

(ns synthigy.log.store
  "Log plug — the `LogStore` protocol, the shared `*log-store*` dynvar, and
   the in-memory `RingLogStore` default."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]])
  (:import
   [java.io PrintWriter StringWriter]
   [java.time Instant]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.regex Pattern]))

;;; ============================================================================
;;; Protocol
;;; ============================================================================

(defprotocol LogStore
  "Contract for a log plug implementation."

  (write-signal! [this signal]
    "Consume one Telemere signal map; must not throw.")

  (recent [this opts]
    "Return up to `:limit` recent wire-schema rows, newest-first.")

  (search [this opts]
    "Filtered query over the store using a validated filter-map from `synthigy.log.query`.")

  (tail [this opts]
    "Cursor-based follow; returns {:rows [...] :next-cursor x}.")

  (clear! [this]
    "Empty the store.")

  (health [this]
    "Snapshot map describing store liveness; must not throw.")

  (snapshot [this]
    "Raw Telemere signals held by this store, oldest-first, for backend handoff."))

;;; ============================================================================
;;; RingLogStore — bounded in-memory implementation; defonce default
;;; ============================================================================

(def ^:private default-ring-size 10000)

(defn env-ring-size []
  (or (try (some-> (env :synthigy-log-ring-size) str/trim Integer/parseInt)
           (catch Throwable _ nil))
      default-ring-size))

(def ^:private promoted-ctx-keys
  [:request-id :user-xid :tenant])

(defn id->str [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (some? id)              (str id)))

(defn throwable-msg [^Throwable t]
  (some-> t .getMessage))

(defn throwable-trace [^Throwable t]
  (when t
    (let [sw (StringWriter.) pw (PrintWriter. sw)]
      (.printStackTrace t pw) (.flush pw) (.toString sw))))

(defn host-of [signal]
  (let [h (:host signal)]
    (cond (string? h) h
          (map? h)    (:name h)
          :else       nil)))

(defn signal-field
  "Extract the value at `field` from a Telemere signal."
  [signal field]
  (cond
    (= field :level) (some-> (:level signal) name)
    (= field :ns)    (:ns signal)
    (= field :id)    (id->str (:id signal))
    (= field :msg)   (some-> (:msg_ signal) force)
    (= field :inst)  (:inst signal)
    (= field :host)  (host-of signal)
    (= field :topics) (:topics signal)
    (= field :request-id)
    (or (:request-id signal) (get-in signal [:ctx :request-id]))
    (= field :user-xid)
    (or (:user-xid signal)   (get-in signal [:ctx :user-xid]))
    (= field :tenant)
    (or (:tenant signal)     (get-in signal [:ctx :tenant]))
    (= field :v) 1
    (= field :error-class)
    (when-let [e (:error signal)]
      (cond (instance? Throwable e) (.getName (class e))
            :else (some-> e class .getName)))
    (= field :error-msg)
    (when-let [e (:error signal)]
      (if (instance? Throwable e) (throwable-msg e) (str e)))
    (= field :error-trace)
    (when-let [e (:error signal)]
      (if (instance? Throwable e) (throwable-trace e) (str e)))

    (vector? field)
    (let [[head & path] field
          path (vec path)]
      (case head
        :data (get-in (:data signal) path)
        :ctx  (get-in (:ctx signal)  path)
        nil))

    :else nil))

(defn normalize-scalar
  [field v]
  (cond
    (#{:level :id} field) (cond (keyword? v) (name v)
                                (string? v)  v
                                :else        (str v))
    :else v))

(defn normalize-set [field s]
  (into #{} (map #(normalize-scalar field %)) s))

(defn nil-safe-compare
  "Compare that does not throw on nil; nil sorts last regardless of direction."
  [a b]
  (cond (= a b) 0
        (nil? a) 1
        (nil? b) -1
        :else    (try (compare a b) (catch Throwable _ 0))))

(defn cmp-op [op a b]
  (let [c (nil-safe-compare a b)]
    (case op
      :>  (pos? c)
      :<  (and (neg? c) (some? a))
      :>= (and (or (zero? c) (pos? c)) (some? a))
      :<= (and (or (zero? c) (neg? c)) (some? a)))))

(defn str-or-nil [x] (when x (str x)))

(defn tuple-pred [field [op arg]]
  (case op
    := (let [arg' (normalize-scalar field arg)]
         #(= (signal-field % field) arg'))
    :!= (let [arg' (normalize-scalar field arg)]
          #(not= (signal-field % field) arg'))
    :> #(cmp-op :> (signal-field % field) arg)
    :< #(cmp-op :< (signal-field % field) arg)
    :>= #(cmp-op :>= (signal-field % field) arg)
    :<= #(cmp-op :<= (signal-field % field) arg)
    :in (let [s (normalize-set field arg)]
          #(contains? s (signal-field % field)))
    :contains    #(when-let [s (str-or-nil (signal-field % field))]
                    (str/includes? s arg))
    :icontains   (let [needle (str/lower-case arg)]
                   #(when-let [s (str-or-nil (signal-field % field))]
                      (str/includes? (str/lower-case s) needle)))
    :starts-with #(when-let [s (str-or-nil (signal-field % field))]
                    (str/starts-with? s arg))
    :ends-with   #(when-let [s (str-or-nil (signal-field % field))]
                    (str/ends-with? s arg))
    :matches     (let [^Pattern p arg]
                   #(when-let [s (str-or-nil (signal-field % field))]
                      (boolean (re-find p s))))
    :exists?     #(some? (signal-field % field))
    :absent?     #(nil? (signal-field % field))
    ;; (subs (str kw) 1), not `name` — :traffic/sse must stay "traffic/sse"
    :has         (let [topic-str #(if (keyword? %) (subs (str %) 1) (str %))
                       needle    (topic-str arg)]
                   #(contains? (into #{} (map topic-str) (or (signal-field % field) #{}))
                               needle))))

(defn where-pred
  "Compile a `:where` map into a (signal → bool) predicate; implicit AND."
  [where]
  (if (empty? where)
    (constantly true)
    (let [preds (mapv
                  (fn [[field v]]
                    (cond
                      (set? v)
                      (let [s (normalize-set field v)]
                        #(contains? s (signal-field % field)))
                      (and (vector? v) (keyword? (first v)))
                      (tuple-pred field v)
                      :else
                      (let [v' (normalize-scalar field v)]
                        #(= (signal-field % field) v'))))
                  where)]
      (fn [sig]
        (loop [ps preds]
          (cond (empty? ps) true
                ((first ps) sig) (recur (rest ps))
                :else false))))))

(def ^:private duration-re
  #"^(\d+)\s*([smhd])$")

(defn parse-duration-ms
  "Parse a short duration string (`10s`, `5m`, `2h`, `1d`) into milliseconds, or
   nil."
  [s]
  (when-let [[_ n unit] (some->> s str/trim (re-matches duration-re))]
    (* (Long/parseLong n) (case unit "s" 1000 "m" 60000 "h" 3600000 "d" 86400000))))

(defn parse-time-ref
  "Parse an ISO-8601 instant or short duration (interpreted as now - duration)
   into an Instant."
  [s]
  (when (and s (string? s))
    (let [s (str/trim s)]
      (or (try (Instant/parse s) (catch Throwable _ nil))
          (when-let [ms (parse-duration-ms s)]
            (.minusMillis (Instant/now) ms))))))

(defn within-window [since-inst until-inst]
  (cond
    (and since-inst until-inst)
    #(when-let [^Instant t (:inst %)]
       (and (not (.isBefore t since-inst))
            (not (.isAfter  t until-inst))))
    since-inst
    #(when-let [^Instant t (:inst %)]
       (not (.isBefore t since-inst)))
    until-inst
    #(when-let [^Instant t (:inst %)]
       (not (.isAfter t until-inst)))
    :else (constantly true)))

(defn signal->wire-row
  [signal]
  (let [ctx-map      (or (:ctx signal) {})
        residual-ctx (apply dissoc ctx-map promoted-ctx-keys)
        e            (:error signal)
        err-class    (when e (if (instance? Throwable e)
                               (.getName (class e))
                               (some-> e class .getName)))
        err-msg      (when e (if (instance? Throwable e)
                               (throwable-msg e)
                               (str e)))
        err-trace    (when e (if (instance? Throwable e)
                               (throwable-trace e)
                               (str e)))]
    {:v           1
     :inst        (some-> ^Instant (:inst signal) str)
     :level       (some-> (:level signal) name)
     :ns          (:ns signal)
     :id          (id->str (:id signal))
     :msg         (some-> (:msg_ signal) force)
     :request_id  (or (:request-id signal) (get ctx-map :request-id))
     :user_xid    (or (:user-xid signal)   (get ctx-map :user-xid))
     :tenant      (or (:tenant signal)     (get ctx-map :tenant))
     :host        (host-of signal)
     ;; (subs (str kw) 1), not `name` — :traffic/sse must stay "traffic/sse"
     :topics      (->> (:topics signal)
                       (map #(if (keyword? %) (subs (str %) 1) (str %)))
                       sort vec)
     :data        (or (:data signal) {})
     :ctx         residual-ctx
     :error_class err-class
     :error_msg   err-msg
     :error_trace err-trace}))

(def ^:private default-limit 100)

(defn order-by-key-fn [field]
  (fn [sig] (signal-field sig field)))

(defn run-search
  [signals {:keys [where since until limit order-by count? bucket-ms] :as opts}]
  (let [group-field (:group-by opts)
        since-i  (parse-time-ref since)
        until-i  (parse-time-ref until)
        wpred    (where-pred where)
        in-win   (within-window since-i until-i)
        survivors (->> signals
                       (filter in-win)
                       (filter wpred))
        [ofield odir] (or order-by [:inst :desc])
        sorted   (sort-by (order-by-key-fn ofield)
                          (if (= odir :asc)
                            nil-safe-compare
                            #(nil-safe-compare %2 %1))
                          survivors)
        limit    (or limit default-limit)]
    (cond
      bucket-ms
      (let [b (long bucket-ms)]
        (->> survivors
             (keep (fn [s] (when-let [^Instant t (:inst s)]
                             (* b (quot (.toEpochMilli t) b)))))
             frequencies
             (sort-by key)
             (mapv (fn [[bucket cnt]] [bucket (long cnt)]))))

      count?
      (long (count survivors))

      group-field
      (->> survivors
           (clojure.core/group-by #(signal-field % group-field))
           (mapv (fn [[v sigs]]
                   {group-field v :count (long (count sigs))})))

      :else
      (->> sorted (take limit) (mapv signal->wire-row)))))

(defn push!
  [buffer-atom ^AtomicLong dropped max-size signal]
  (let [popped? (volatile! false)]
    (swap! buffer-atom
           (fn [q]
             (let [q' (conj q signal)]
               (if (> (count q') max-size)
                 (do (vreset! popped? true) (pop q'))
                 q'))))
    (when @popped? (.incrementAndGet dropped))))

(defrecord RingLogStore [buffer dropped max-size]
  LogStore

  (write-signal! [_ signal]
    (when signal
      (try (push! buffer dropped max-size signal)
           (catch Throwable _)))
    nil)

  (recent [this {:keys [limit ns-pattern level since]}]
    (let [opts (cond-> {:order-by [:inst :desc] :limit (or limit default-limit)}
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level)
                 since      (assoc :since since))]
      (search this opts)))

  (search [_ opts]
    (run-search (seq @buffer) opts))

  (tail [this {:keys [cursor ns-pattern level limit]}]
    (let [opts (cond-> {:order-by [:inst :asc] :limit (or limit default-limit)}
                 cursor     (assoc :since cursor)
                 ns-pattern (assoc-in [:where :ns] [:starts-with ns-pattern])
                 level      (update :where (fnil assoc {}) :level level))
          rows (search this opts)]
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
                   (long (or size (env-ring-size))))))

;;; ============================================================================
;;; The dynvar — defonce'd to a fresh ring so it's never nil
;;; ============================================================================

(defonce ^:dynamic *log-store* (create-ring))
