;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.log.pipeline
  "Signal-transform pipeline for `synthigy.log`, installed as Telemere's global
   :xfn."
  (:require
   [clojure.string :as str]))

(defn compose-stages
  "Compose stage fns into a single pipeline fn for `taoensso.telemere/set-xfn!`."
  [stages]
  (let [non-empty (vec (remove nil? stages))]
    (case (count non-empty)
      0 identity
      1 (first non-empty)
      (fn pipeline-fn [signal]
        (reduce (fn [sig stage]
                  (if-let [next-sig (stage sig)]
                    next-sig
                    (reduced nil)))
                signal
                non-empty)))))

;;; ============================================================================
;;; Built-in stages
;;; Stages MUST NOT throw — that breaks every signal after it; print to *err* and pass through.
;;; ============================================================================

(def line-key
  "Signal key under which the serialized JSON wire String is cached."
  :_synthigy/line)

(def redacted-marker
  "String substituted for redacted values in :data and :ctx."
  "<redacted>")

(defn walk-redact
  "Walk `x`, replacing values for any key in `keys-to-redact` with
   `redacted-marker`."
  [keys-to-redact x]
  (cond
    (record? x)
    (reduce-kv (fn [m k v]
                 (assoc m k (if (contains? keys-to-redact k)
                              redacted-marker
                              (walk-redact keys-to-redact v))))
               x x)

    (map? x)
    (reduce-kv (fn [m k v]
                 (assoc m k (if (contains? keys-to-redact k)
                              redacted-marker
                              (walk-redact keys-to-redact v))))
               (empty x) x)

    (sequential? x)
    (mapv #(walk-redact keys-to-redact %) x)

    :else x))

(def default-ns-aliases
  "Real code-namespace prefix → logical wire namespace; ordered, first match wins."
  [["synthigy.db"            "synthigy.database"]
   ["synthigy.admin"         "synthigy.admin"]
   ["synthigy.subscriptions" "synthigy.subscriptions"]])

(defn normalize-ns-stage
  "Pipeline stage rewriting the signal's `:ns` to its logical name; runs before
   topic classification."
  ([] (normalize-ns-stage default-ns-aliases))
  ([aliases]
   (fn normalize-ns [signal]
     (let [s (str (:ns signal))]
       (if-let [canonical (some (fn [[prefix canonical]]
                                  (when (str/starts-with? s prefix) canonical))
                                aliases)]
         (assoc signal :ns canonical)
         signal)))))

(def default-promoted-ctx-keys
  "Context keys the default pipeline promotes from `:ctx` to top-level signal fields."
  [:request-id :user-xid :tenant :ip])

(defn enrich-host-stage
  "Pipeline stage normalizing the signal's `:host` to a String, falling back to
   `host-fn`."
  [host-fn]
  (fn enrich-host [signal]
    (let [existing (:host signal)]
      (cond
        (string? existing)
        signal

        (and (map? existing) (string? (:name existing)))
        (assoc signal :host (:name existing))

        :else
        (assoc signal :host (try (host-fn) (catch Throwable _ nil)))))))

(defn enrich-ctx-stage
  "Pipeline stage promoting keys from `:ctx` to top-level signal fields."
  ([] (enrich-ctx-stage default-promoted-ctx-keys))
  ([keys-to-promote]
   (let [keys-vec (vec keys-to-promote)
         keys-set (set keys-to-promote)]
     (fn enrich-ctx [signal]
       (if-let [ctx (:ctx signal)]
         (let [promoted-signal
               (reduce (fn [sig k]
                         (if (or (contains? sig k)
                                 (not (contains? ctx k)))
                           sig
                           (assoc sig k (get ctx k))))
                       signal
                       keys-vec)
               residual-ctx (reduce dissoc ctx keys-set)]
           (assoc promoted-signal :ctx residual-ctx))
         signal)))))

(defn sample-stage
  "Pipeline stage probabilistically dropping signals matched by `pred` (`rate` =
   keep probability); intentionally NOT in the default pipeline."
  [pred rate]
  (let [keep-rate (double rate)]
    (fn sample [signal]
      (let [matched? (try (pred signal) (catch Throwable _ false))]
        (if (and matched? (> (rand) keep-rate))
          nil
          signal)))))

(defn redact-stage
  "Pipeline stage replacing values for `keys-to-redact` with `<redacted>`
   anywhere in `:data`/`:ctx`."
  [keys-to-redact]
  (let [ks (set keys-to-redact)]
    (fn redact [signal]
      (try
        (cond-> signal
          (:data signal) (update :data #(walk-redact ks %))
          (:ctx signal)  (update :ctx  #(walk-redact ks %)))
        (catch Throwable t
          (binding [*out* *err*]
            (println (str "synthigy.log.pipeline/redact-stage threw: "
                          (some-> t .getClass .getName) ": "
                          (.getMessage t))))
          signal)))))

(defn enrich-topics-stage
  "Pipeline stage assoc'ing `:topics` computed by `classify-fn` onto the signal."
  [classify-fn]
  (fn enrich-topics [signal]
    (try
      (assoc signal :topics (classify-fn signal))
      (catch Throwable t
        (binding [*out* *err*]
          (println (str "synthigy.log.pipeline/enrich-topics-stage threw: "
                        (some-> t .getClass .getName) ": " (.getMessage t))))
        signal))))

(defn serialize-stage
  "Pipeline stage caching the serialized wire line at `line-key`."
  [serializer]
  (fn serialize [signal]
    (try
      (assoc signal line-key (serializer signal))
      (catch Throwable t
        (binding [*out* *err*]
          (println (str "synthigy.log.pipeline/serialize-stage threw: "
                        (some-> t .getClass .getName) ": "
                        (.getMessage t))))
        signal))))
