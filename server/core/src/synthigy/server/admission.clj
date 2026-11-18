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

(ns synthigy.server.admission
  "A fair gate in front of database-bound routes: requests run in arrival order; 503 only when the estimated wait exceeds the budget."
  (:require
   [synthigy.db :as db]
   [synthigy.json :as json])
  (:import
   [java.util.concurrent Semaphore TimeUnit]
   [java.util.concurrent.atomic AtomicInteger AtomicLong]))

(def wait-ms 10000)

(def budget-ms 5000)

(def fallback-permits 40)

(defn busy-response
  ([] (busy-response 1))
  ([retry-after-s]
   {:status 503
    :headers {"Content-Type" "application/json"
              "Retry-After" (str retry-after-s)}
    :body (json/write-str {:error {:code "SERVER_BUSY"
                                   :message "The server is busy; retry shortly"
                                   :retryable true}})}))

(defn pool-size [datasource]
  (when datasource
    (try (.getMaximumPoolSize datasource)
         (catch Exception _ nil))))

(defn make-gate
  "`permits` requests run at once; the rest wait in arrival order while their estimated wait stays within the budget."
  ([permits] (make-gate permits (* 64 permits)))
  ([permits max-waiting]
   {:semaphore (Semaphore. (int permits) true)
    :permits permits
    :waiting (AtomicInteger. 0)
    :max-waiting max-waiting
    :service-ns (AtomicLong. 0)}))

(defn estimated-wait-ms
  "How long the request at queue `position` should wait, from the running average time a request holds a slot."
  [{:keys [permits ^AtomicLong service-ns]} position]
  (/ (* (double position) (.get service-ns)) (* (double permits) 1e6)))

(defn record-service!
  [{:keys [^AtomicLong service-ns]} ns]
  (let [old (.get service-ns)]
    (.set service-ns (if (zero? old) ns (+ old (quot (- ns old) 16))))))

(defonce gate (atom nil))

(defn the-gate
  "The shared gate, sized at twice the current database pool; a new pool gets a new gate."
  []
  (let [ds (some-> db/*db* :datasource)
        current? #(and % (identical? ds (:datasource %)))]
    (or (when (current? @gate) @gate)
        (let [fresh (assoc (make-gate (if-let [n (pool-size ds)] (* 2 n) fallback-permits))
                           :datasource ds)]
          (swap! gate #(if (current? %) % fresh))))))

(defn enter?
  [{:keys [^Semaphore semaphore ^AtomicInteger waiting max-waiting] :as gate} wait-ms budget-ms]
  (try
    (or (.tryAcquire semaphore 0 TimeUnit/MILLISECONDS)
        (let [position (.incrementAndGet waiting)]
          (try
            (boolean (and (<= position max-waiting)
                          (<= (estimated-wait-ms gate position) budget-ms)
                          (.tryAcquire semaphore (long wait-ms) TimeUnit/MILLISECONDS)))
            (finally (.decrementAndGet waiting)))))
    (catch InterruptedException _ false)))

(defn retry-after-s
  [{:keys [^AtomicInteger waiting] :as gate}]
  (-> (estimated-wait-ms gate (inc (.get waiting))) (/ 1000) Math/ceil long (max 1) (min 10)))

(defn admit
  "Run `handler` once `gate` lets the request in within `wait-ms`; answer busy at once when the estimated wait exceeds `budget-ms`."
  ([gate wait-ms handler request] (admit gate wait-ms budget-ms handler request))
  ([gate wait-ms budget-ms handler request]
   (if (enter? gate wait-ms budget-ms)
     (let [t0 (System/nanoTime)]
       (try (handler request)
            (finally
              (record-service! gate (- (System/nanoTime) t0))
              (.release ^Semaphore (:semaphore gate)))))
     (busy-response (retry-after-s gate)))))

(defn wrap
  "Ring handler that admits requests through the shared gate."
  [handler]
  (fn [request]
    (admit (the-gate) wait-ms handler request)))
