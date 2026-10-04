;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: Elastic-2.0
;
;   Licensed under the Elastic License 2.0 — see LICENSE, or
;   <https://www.elastic.co/licensing/elastic-license>. Free to use, modify
;   and build your own products on; you may not provide Synthigy to third
;   parties as a hosted or managed service. Commercial licensing:
;   r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.boot
  "One boot order for every runtime — supervisor, dev REPL, embedding apps."
  (:require
   [patcho.lifecycle :as lifecycle]
   [synthigy.dataline]
   [clojure.string :as str]
   [synthigy.dataset.id :as id]))

(def shadowed-resources
  {"engine"         "synthigy/core"
   "audit"          "synthigy/audit"
   "log-store"      "synthigy/log/store"
   "server adapter" "synthigy/server"})

(defn resource-roots
  "Distinct classpath roots (jar or directory) providing resource."
  [^ClassLoader loader resource]
  (into #{}
        (map #(subs (str %) 0 (- (count (str %)) (count resource))))
        (enumeration-seq (.getResources loader resource))))

(defn duplicate-backends
  "Backend kinds appearing more than once on the classpath, as {kind count}; two of a kind means the JVM silently runs whichever came first."
  ([] (duplicate-backends (.getContextClassLoader (Thread/currentThread))))
  ([^ClassLoader loader]
   (into {}
         (keep (fn [[kind base]]
                 ;; AOT jars ship only __init.class, Clojars source jars only .clj
                 (let [n (count (into (resource-roots loader (str base "__init.class"))
                                      (resource-roots loader (str base ".clj"))))]
                   (when (> n 1) [kind n]))))
         shadowed-resources)))

(defn start!
  "Start the dataset, refuse a legacy euuid database, then start modules and Dataline."
  ([modules] (start! modules {}))
  ([modules {:keys [on-phase] :or {on-phase (fn [_])}}]
   (when-let [dupes (not-empty (duplicate-backends))]
     (throw (ex-info (str "Classpath carries more than one backend module: "
                          (str/join ", " (map (fn [[kind n]] (str n " " kind)) dupes))
                          ". Exactly one engine, one audit, one log-store and one server adapter module belong "
                          "on a classpath — the JVM would silently run whichever comes first.")
                     {:duplicates dupes})))
   (on-phase "starting :synthigy/dataset")
   (lifecycle/start! :synthigy/dataset)
   ;; never let IAM/OAuth setup touch a legacy euuid layout — migrate.xid first
   (when (= :euuid (id/key))
     (throw (ex-info (str "Legacy euuid-keyed database detected — Synthigy is "
                          "xid-native. Run the migration before starting.")
                     {:legacy true})))
   (doseq [m modules]
     (on-phase (str "starting " m))
     (lifecycle/start! m))
   (on-phase "starting Dataline")
   (lifecycle/start! :synthigy/dataline)))

(defn stop!
  "Mirror of start!: Dataline, the modules in reverse, then the database and everything on it."
  [modules]
  (lifecycle/stop! :synthigy/dataline)
  (doseq [m (reverse modules)] (lifecycle/stop! m))
  (lifecycle/stop! :synthigy/database))
