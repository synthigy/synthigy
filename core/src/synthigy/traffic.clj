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

(ns synthigy.traffic
  "Traffic metrics provider for the `traffic-stats` op — statistics at the
   source (no per-request log event), PROVIDER pattern with a dev default
   and durable backends taking over via classpath alias, multi-node correct
   via node-tagged minute rows summed at read. Three layers: hot-path
   `LongAdder` cells for the current minute, a `TrafficStore` receiving
   periodic delta flushes, and `report` folding window queries across nodes."
  (:require
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.log :as log]
   [synthigy.log.store :as log.store]
   [synthigy.node :as node])
  (:import
   [java.time Instant ZoneOffset]
   [java.time.format DateTimeFormatter]
   [java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit]
   [java.util.concurrent.atomic LongAdder]))

;;; ============================================================================
;;; Histogram bounds
;;; ============================================================================

(def hist-bounds
  "Latency histogram upper bounds (ms). Durations above the last bound
   count under :inf."
  [1 2 5 10 25 50 100 250 500 1000 2500 5000])

(defn bound-of [duration-ms]
  (or (some #(when (<= duration-ms %) %) hist-bounds) :inf))

(def ^:private count-keys [:ops :sse :subscriptions :deltas-dropped])

;;; ============================================================================
;;; TrafficStore protocol
;;; ============================================================================

(defprotocol TrafficStore
  (flush-deltas! [this rows]
    "Append delta rows; providers APPEND, reads SUM, so flushing the same minute twice stays correct. Never throws.")
  (query-window [this {:keys [from to]}]
    "Return delta rows with `:minute` in `[from,to]`, across all nodes. Never throws — returns [] on failure.")
  (traffic-health [this]
    "Snapshot map describing store liveness; named `traffic-health` (not `health`) to avoid colliding with `LogStore/health`. Must not throw."))

;;; ============================================================================
;;; InMemoryTrafficStore — dev default; bounded 48h ring of summed cells
;;; ============================================================================

(def ^:private retention-minutes (* 48 60))
(def ^:private max-lookback-minutes (* 90 24 60))

(defn merge-cell [a b]
  (-> (merge-with + (dissoc a :hist) (dissoc b :hist))
      (assoc :hist (merge-with + (:hist a) (:hist b)))))

(defn row->cell
  "Strip a delta/query-window row to a plain counter cell — every `merge-cell`
   fold must use cells, never raw rows, or `merge-with +` corrupts
   :node/:minute."
  [row]
  (dissoc row :node :minute))

(defn prune [m now-minute]
  (if (> (count m) (+ retention-minutes 60))
    (let [cutoff (- now-minute retention-minutes)]
      (into {} (filter (fn [[[_node minute] _]] (>= minute cutoff))) m))
    m))

(defrecord InMemoryTrafficStore [state]
  TrafficStore
  (flush-deltas! [_ rows]
    (try
      (let [now-m (quot (System/currentTimeMillis) 60000)]
        (swap! state
               (fn [m]
                 (prune
                  (reduce (fn [m {:keys [node minute] :as row}]
                            (update m [node minute] (fnil merge-cell {}) (row->cell row)))
                          m rows)
                  now-m))))
      (catch Throwable _ nil))
    nil)
  (query-window [_ {:keys [from to]}]
    (try
      (->> @state
           (filter (fn [[[_node minute] _]] (and (>= minute from) (<= minute to))))
           (map (fn [[[node minute] cell]] (assoc cell :node node :minute minute)))
           vec)
      (catch Throwable _ [])))
  (traffic-health [_]
    {:up? true :backend :in-memory :cells (count @state)}))

(defn create-in-memory-store []
  (->InMemoryTrafficStore (atom {})))

(defonce ^:dynamic *traffic-store* (create-in-memory-store))

;;; ============================================================================
;;; Hot path — LongAdder cells for the current minute
;;; ============================================================================

(defn new-cell []
  {:ok (LongAdder.) :err (LongAdder.)
   :hist (into {} (map (fn [b] [b (LongAdder.)])) (conj hist-bounds :inf))
   :ops (LongAdder.) :sse (LongAdder.) :subscriptions (LongAdder.)
   :deltas-dropped (LongAdder.)})

(defonce ^:private cells (atom {}))

(defn minute-now []
  (quot (System/currentTimeMillis) 60000))

(defn cell-for [minute]
  (or (get @cells minute)
      (-> (swap! cells (fn [m] (if (contains? m minute) m (assoc m minute (new-cell)))))
          (get minute))))

(defn record!
  "Count one completed request: ok/err by status (>= 400 = err, missing
   status = ok) + latency histogram. Never throws."
  [status duration-ms]
  (try
    (let [cell (cell-for (minute-now))
          err? (and (number? status) (>= status 400))]
      (.increment ^LongAdder (get cell (if err? :err :ok)))
      (when (number? duration-ms)
        (.increment ^LongAdder (get-in cell [:hist (bound-of duration-ms)]))))
    nil
    (catch Throwable _ nil)))

(defn count!
  "Count one non-request traffic event under `k` — one of
   :ops :sse :subscriptions :deltas-dropped. Never throws."
  [k]
  (try
    (.increment ^LongAdder (get (cell-for (minute-now)) k))
    nil
    (catch Throwable _ nil)))

(defn reset-counters!
  "Test/REPL hygiene: drop hot cells and, if the bound store is the in-memory
   default, its history too."
  []
  (reset! cells {})
  (when (instance? InMemoryTrafficStore *traffic-store*)
    (reset! (:state *traffic-store*) {})))

(defn drain-cell!
  "sumThenReset every LongAdder in a cell — atomic per-adder drain. The
   result is this minute's DELTA since the last flush."
  [cell]
  {:ok (.sumThenReset ^LongAdder (:ok cell))
   :err (.sumThenReset ^LongAdder (:err cell))
   :hist (into {} (map (fn [[b ^LongAdder a]] [b (.sumThenReset a)])) (:hist cell))
   :ops (.sumThenReset ^LongAdder (:ops cell))
   :sse (.sumThenReset ^LongAdder (:sse cell))
   :subscriptions (.sumThenReset ^LongAdder (:subscriptions cell))
   :deltas-dropped (.sumThenReset ^LongAdder (:deltas-dropped cell))})

(defn nonzero-delta? [d]
  (or (pos? (:ok d)) (pos? (:err d)) (pos? (:ops d)) (pos? (:sse d))
      (pos? (:subscriptions d)) (pos? (:deltas-dropped d))
      (some pos? (vals (:hist d)))))

(defn drain-all!
  "Drain every tracked minute except the current one into delta rows, dropping
   drained-empty minutes from `cells`."
  [{:keys [include-current?]}]
  (let [now-m (minute-now)
        snapshot @cells
        to-drain (cond->> (keys snapshot)
                   (not include-current?) (remove #(= % now-m)))]
    (vec
     (keep (fn [minute]
             (let [cell (get snapshot minute)
                   d (drain-cell! cell)]
               (swap! cells dissoc minute)
               (when (nonzero-delta? d)
                 (assoc d :node @node/id :minute minute))))
           to-drain))))

;;; ============================================================================
;;; Flusher — periodic delta drain into the bound TrafficStore
;;; ============================================================================

(def ^:private flush-interval-seconds 15)

(defonce ^:private scheduler (atom nil))

(defn flush-once! [include-current?]
  (let [rows (drain-all! {:include-current? include-current?})]
    (when (seq rows)
      (flush-deltas! *traffic-store* rows))))

(defn start-flusher!
  "Start the daemon scheduler that periodically drains cells into
   `*traffic-store*`. Idempotent — a second call is a no-op while one is
   already running."
  []
  (when (compare-and-set! scheduler nil ::starting)
    (let [^ScheduledExecutorService ex
          (Executors/newSingleThreadScheduledExecutor
           (reify ThreadFactory
             (newThread [_ r] (doto (Thread. ^Runnable r "synthigy-traffic-flusher") (.setDaemon true)))))]
      (.scheduleWithFixedDelay
       ex ^Runnable (fn [] (try (flush-once! false) (catch Throwable _ nil)))
       flush-interval-seconds flush-interval-seconds TimeUnit/SECONDS)
      (reset! scheduler ex))))

(defn stop-flusher!
  "Stop the scheduler and perform a final flush (including the still-open
   current minute) so no counts are lost on a clean shutdown."
  []
  (when-let [^ScheduledExecutorService ex @scheduler]
    (when-not (= ex ::starting)
      (.shutdown ex)
      (try (.awaitTermination ex 2 TimeUnit/SECONDS) (catch Throwable _)))
    (reset! scheduler nil))
  (try (flush-once! true) (catch Throwable _ nil)))

;;; ============================================================================
;;; Report — fold delta rows into the traffic-stats wire shape
;;; ============================================================================

(def ^:private hh-mm
  (.withZone (DateTimeFormatter/ofPattern "HH:mm") ZoneOffset/UTC))

(defn minute-label [minute-epoch]
  (.format hh-mm (Instant/ofEpochMilli (* minute-epoch 60000))))

(defn percentile-from-hist
  "Nearest-rank percentile over a merged histogram: the upper bound of the
   bucket containing rank ceil(p*n). nil when the histogram is empty."
  [hist p]
  (let [n (reduce + 0 (vals hist))]
    (when (pos? n)
      (let [rank (long (Math/ceil (* p n)))]
        (loop [bs hist-bounds acc 0]
          (if-let [b (first bs)]
            (let [acc (+ acc (long (get hist b 0)))]
              (if (>= acc rank) b (recur (rest bs) acc)))
            (last hist-bounds)))))))

(defn merge-hists [minute-cells]
  (apply merge-with + (keep :hist minute-cells)))

(defn sum-across-nodes
  "Fold query-window rows (one per node per minute) into one cell per
   minute, summed across all nodes."
  [rows]
  (->> rows
       (group-by :minute)
       (into {} (map (fn [[minute rs]]
                       [minute (reduce (fn [acc r] (merge-cell acc (row->cell r))) {} rs)])))))

(defn live-cell-snapshot
  "Non-destructive peek (`.sum`, not `.sumThenReset`) of a hot-path cell,
   in delta-row cell shape — safe to fold alongside durable rows without
   disturbing what the flusher will drain later."
  [cell]
  {:ok (.sum ^LongAdder (:ok cell))
   :err (.sum ^LongAdder (:err cell))
   :hist (into {} (map (fn [[b ^LongAdder a]] [b (.sum a)])) (:hist cell))
   :ops (.sum ^LongAdder (:ops cell))
   :sse (.sum ^LongAdder (:sse cell))
   :subscriptions (.sum ^LongAdder (:subscriptions cell))
   :deltas-dropped (.sum ^LongAdder (:deltas-dropped cell))})

(defn by-node-summary
  "One summary row per node, requests/errors/etc summed across the whole window
   and sorted by `:requests` descending; includes this node's still-open current
   minute."
  [rows now-m from-m to-m]
  (let [live-cell (when (<= from-m now-m to-m) (get @cells now-m))
        rows (cond-> rows
               live-cell (conj (assoc (live-cell-snapshot live-cell) :node @node/id :minute now-m)))]
    (->> rows
         (group-by :node)
         (map (fn [[n rs]]
                (let [c (reduce (fn [acc r] (merge-cell acc (row->cell r))) {} rs)
                      ok (long (get c :ok 0)) err (long (get c :err 0))]
                  {:node n
                   :requests (+ ok err)
                   :errors err
                   :ops (long (get c :ops 0))
                   :sse (long (get c :sse 0))
                   :subscriptions (long (get c :subscriptions 0))
                   :deltas-dropped (long (get c :deltas-dropped 0))})))
         (sort-by :requests >)
         vec)))

(defn merge-open-minute
  "Merge this node's still-open current-minute counters (a live peek, no
   drain) on top of the summed-per-minute map so the chart stays current
   without a flush on every request."
  [by-minute now-m]
  (if-let [cell (get @cells now-m)]
    (update by-minute now-m (fnil merge-cell {}) (live-cell-snapshot cell))
    by-minute))

(defn parse-instant
  "Strict absolute-instant parse — nil for anything but a literal ISO-8601
   timestamp; unlike `parse-time-ref`, never resolves durations against
   wall-clock now."
  [s]
  (when (string? s) (try (Instant/parse (.trim ^String s)) (catch Throwable _ nil))))

(defn resolve-window
  "Resolve {:since :until} into [from to] Instants; a duration `since` resolves
   relative to `until`, not wall-clock now."
  [since until]
  (let [to (or (parse-instant until) (Instant/now))
        from (or (parse-instant since)
                 (some->> (or since "15m") log.store/parse-duration-ms (.minusMillis ^Instant to)))]
    [from to]))

(defn minute->iso [minute-epoch]
  (str (Instant/ofEpochMilli (* minute-epoch 60000))))

(defn report
  "Query `*traffic-store*` for the window and fold into the traffic-stats report
   shape; per-minute gap-filled buckets, `:window` is the actual
   resolved-and-clamped range."
  [{:keys [since until]}]
  (let [now-m (minute-now)
        [from to] (resolve-window since until)
        ;; ponytail: clamp reads to durable retention (90d), not the 48h
        ;; in-memory ring. Buckets stay per-minute, so a full 90d window is
        ;; ~130k gap-filled tuples — heavy but bounded, and only at maximum
        ;; zoom-out. Add server-side coarsening (bucket-minutes > 1 for wide
        ;; windows) if that payload ever actually bites; not before.
        from-m (max (- now-m max-lookback-minutes)
                    (if from (quot (.toEpochMilli ^Instant from) 60000) (- now-m 15)))
        to-m (min now-m (if to (quot (.toEpochMilli ^Instant to) 60000) now-m))
        rows (query-window *traffic-store* {:from from-m :to to-m})
        by-minute (-> (sum-across-nodes rows) (merge-open-minute now-m))
        minute-cells (map #(get by-minute % {}) (range from-m (inc to-m)))
        get0 (fn [c k] (long (get c k 0)))
        reqs (reduce + 0 (map #(+ (get0 % :ok) (get0 % :err)) minute-cells))
        errs (reduce + 0 (map #(get0 % :err) minute-cells))
        hist (merge-hists minute-cells)
        sum-key (fn [k] (reduce + 0 (map #(get0 % k) minute-cells)))
        by-node (by-node-summary rows now-m from-m to-m)]
    (cond-> {:requests reqs
             :errors errs
             :p50 (percentile-from-hist hist 0.50)
             :p95 (percentile-from-hist hist 0.95)
             :ops (sum-key :ops)
             :sse (sum-key :sse)
             :subscriptions (sum-key :subscriptions)
             :deltas-dropped (sum-key :deltas-dropped)
             :bucket-minutes 1
             :window [(minute->iso from-m) (minute->iso to-m)]
             :buckets (mapv (fn [m c]
                              (let [h (:hist c)]
                                [(minute-label m) (get0 c :ok) (get0 c :err)
                                 (percentile-from-hist h 0.50)
                                 (percentile-from-hist h 0.95)]))
                            (range from-m (inc to-m)) minute-cells)}
      (seq by-node) (assoc :by-node by-node))))

;;; ============================================================================
;;; Module registration — DEV default; backends re-register the same key
;;; ============================================================================

(patch/current-version :synthigy/traffic "1.0.0")

(lifecycle/register-module!
 :synthigy/traffic
 {:depends-on [:synthigy/log]
  :doc "Traffic metrics provider (in-memory dev default)"
  :start (fn []
           (start-flusher!)
           (log/info {:id ::lifecycle-started
                      :data {:action :started :subject :traffic-flusher
                             :flush-interval-seconds flush-interval-seconds}}
                     "Traffic flusher started"))
  :stop (fn []
          (stop-flusher!)
          (log/info {:id ::lifecycle-stopped
                     :data {:action :stopped :subject :traffic-flusher}}
                    "Traffic flusher stopped"))})
