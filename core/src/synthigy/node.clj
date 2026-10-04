;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.node
  "This process's identity — hostname resolution and a hardware/OS
   fingerprint, used to tag data with which instance produced it
   (`synthigy.traffic`'s node-tagged rows, `synthigy.log`'s `:host`
   column). Pure — no lifecycle module; `id` resolves `SYNTHIGY_NODE_ID`
   env override → `fingerprint` → `hostname` → `\"unknown\"`, each a
   `delay` memoized once per JVM."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]])
  (:import
   [java.io File]
   [java.net InetAddress NetworkInterface]
   [java.security MessageDigest]))

(def hostname
  (delay
    (or (env :synthigy-log-host)
        (env :hostname)
        (try (.getHostName (InetAddress/getLocalHost))
             (catch Throwable _ nil)))))

(defn bytes->hex [^bytes b]
  (apply str (map #(format "%02x" %) b)))

(defn mac-addresses
  "Hex MAC of every non-loopback, non-virtual NIC, sorted for determinism."
  []
  (try
    (->> (NetworkInterface/getNetworkInterfaces)
         enumeration-seq
         (remove (fn [^NetworkInterface ni] (or (.isLoopback ni) (.isVirtual ni))))
         (keep (fn [^NetworkInterface ni] (.getHardwareAddress ni)))
         (map bytes->hex)
         sort vec)
    (catch Throwable _ [])))

(defn machine-id
  "/etc/machine-id (or the dbus fallback), often absent in minimal container
   images."
  []
  (some (fn [path]
          (try
            (let [f (File. ^String path)]
              (when (.exists f) (not-empty (str/trim (slurp f)))))
            (catch Throwable _ nil)))
        ["/etc/machine-id" "/var/lib/dbus/machine-id"]))

(defn sha256-hex [^String s]
  (bytes->hex (.digest (MessageDigest/getInstance "SHA-256") (.getBytes s "UTF-8"))))

(def fingerprint
  "Best-effort hardware/OS fingerprint, truncated to 12 hex chars; nil if every source is unavailable."
  (delay
    (try
      (let [mid (machine-id)
            sources (cond-> (mac-addresses)
                      true (conj (System/getProperty "os.arch" ""))
                      true (conj (or @hostname ""))
                      mid  (conj mid))]
        (when (seq (remove str/blank? sources))
          (subs (sha256-hex (str/join "|" sources)) 0 12)))
      (catch Throwable _ nil))))

(def id
  "This process's node identity. See the ns docstring."
  (delay (or (env :synthigy-node-id) @fingerprint @hostname "unknown")))
