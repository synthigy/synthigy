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
   [clojure.java.io :as io]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.dataline]
   [clojure.string :as str]
   [synthigy.dataset.id :as id]
   [synthigy.log :as log]))

(def version
  (or (some-> (io/resource "synthigy/VERSION") slurp str/trim not-empty) "dev"))

(patch/current-version :synthigy/engine version)

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

(defn root-cause-message [^Throwable t]
  (loop [e t]
    (if-let [c (.getCause e)] (recur c) (or (ex-message e) (str (class e))))))

(defn closure
  "`topic` and every module it depends on, transitively."
  [topic]
  (loop [todo [topic] seen #{}]
    (if-let [t (first todo)]
      (recur (into (subvec todo 1) (when-not (seen t) (:depends-on (lifecycle/module-info t))))
             (conj seen t))
      seen)))

(defn errors-since
  "{topic root-cause} for every module whose start failed at or after `t0` (epoch ms)."
  [t0]
  (into {}
        (keep (fn [[topic {:keys [error ^java.util.Date timestamp]}]]
                (when (and timestamp (<= t0 (.getTime timestamp)))
                  [topic (root-cause-message error)])))
        @lifecycle/module-errors))

(defn require-each!
  "Load each namespace, logging failures; answers {ns cause} for those that did not load."
  [nss]
  (into {}
        (keep (fn [n]
                (try
                  (require (symbol (str n)))
                  nil
                  (catch Throwable t
                    (log/error! {:id ::namespace-failed
                                 :msg "Namespace failed to load — starting the rest"
                                 :data {:action :loading :subject :namespace :namespace (str n)}}
                                t)
                    [(str n) (root-cause-message t)]))))
        nss))

(defn start-each!
  "Start `modules` in order, skipping only those that need a failed one; answers {:failed {topic cause} :skipped {topic blocker}}."
  [modules on-phase]
  (let [t0 (System/currentTimeMillis)]
    (reduce
     (fn [{:keys [failed] :as outcome} m]
       (let [blocker (when-not (lifecycle/started? m)
                       (some #(when (contains? failed %) %) (closure m)))]
         (cond
           (contains? failed m)
           outcome

           blocker
           (do (log/warn {:id ::module-skipped
                          :data {:action :starting :subject :module :module m :blocked-by blocker}}
                         "Module skipped — a module it needs failed to start")
               (assoc-in outcome [:skipped m] blocker))

           :else
           (do (on-phase (str "starting " m))
               (try
                 (lifecycle/start! m)
                 outcome
                 (catch Throwable t
                   (log/error! {:id ::module-failed
                                :msg "Module failed to start — starting the others"
                                :data {:action :starting :subject :module :module m}}
                               t)
                   (update outcome :failed merge
                           (select-keys (errors-since t0) (closure m))
                           {m (root-cause-message t)})))))))
     {:failed {} :skipped {}}
     modules)))

(defn start!
  "Load `:namespaces`, start the dataset, refuse a legacy euuid database, then every module that can start and Dataline; throws naming what did not."
  ([modules] (start! modules {}))
  ([modules {:keys [on-phase namespaces] :or {on-phase (fn [_])}}]
   (when-let [dupes (not-empty (duplicate-backends))]
     (throw (ex-info (str "Classpath carries more than one backend module: "
                          (str/join ", " (map (fn [[kind n]] (str n " " kind)) dupes))
                          ". Exactly one engine, one audit, one log-store and one server adapter module belong "
                          "on a classpath — the JVM would silently run whichever comes first.")
                     {:duplicates dupes})))
   (let [unloaded (require-each! namespaces)]
     (on-phase "starting :synthigy/dataset")
     (lifecycle/start! :synthigy/dataset)
     ;; never let IAM/OAuth setup touch a legacy euuid layout — migrate.xid first
     (when (= :euuid (id/key))
       (throw (ex-info (str "Legacy euuid-keyed database detected — Synthigy is "
                            "xid-native. Run the migration before starting.")
                       {:legacy true})))
     (let [{:keys [failed skipped] :as outcome} (start-each! modules on-phase)]
       (on-phase "starting Dataline")
       (lifecycle/start! :synthigy/dataline)
       (when (or (seq unloaded) (seq failed) (seq skipped))
         (throw (ex-info (str "Did not start: "
                              (str/join "; " (concat (for [[n cause] unloaded] (str n " — " cause))
                                                     (for [[m cause] failed] (str m " — " cause))
                                                     (for [[m blocker] skipped] (str m " — needs " blocker)))))
                         (assoc outcome :unloaded unloaded))))))))

(defn stop!
  "Mirror of start!: Dataline, the modules in reverse, then the database and everything on it."
  [modules]
  (lifecycle/stop! :synthigy/dataline)
  (doseq [m (reverse modules)] (lifecycle/stop! m))
  (lifecycle/stop! :synthigy/database))
