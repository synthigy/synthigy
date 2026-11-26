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

(ns synthigy.apps.output
  "App output and usage the portal sends, kept in the engine's log store under the App, and read back by the App's people."
  (:require
   [clojure.string :as str]
   [synthigy.apps.deploy :as deploy]
   [synthigy.apps.volumes :as volumes]
   [synthigy.dataset :as dataset]
   [synthigy.engine :as engine]
   [synthigy.iam.access :as access]
   [synthigy.log :as log]
   [synthigy.log.query :as log.query]
   [synthigy.supervisor :as supervisor]))

(def max-lines-per-minute 1000)

(def max-line-chars 8192)

(defonce clients (atom {}))

(defn client-of
  "The client xid of a package, remembered."
  [package-xid]
  (when (string? package-xid)
    (or (@clients package-xid)
        (when-let [c (get-in (access/with-principal nil
                               (dataset/get-entity :apps/package {:xid package-xid} {:app [{:selections {:xid nil}}]}))
                             [:app :xid])]
          (swap! clients assoc package-xid c)
          c))))

(defonce budgets (atom {}))

(defn minute [] (quot (System/currentTimeMillis) 60000))

(defn take-budget!
  "How many of `n` lines the App may still log this minute, and the lines it lost in the minute before, once."
  [client n]
  (let [m (minute)
        [before after] (swap-vals! budgets update client
                              (fn [{bm :minute :as b}]
                                (let [b (if (= bm m) b {:minute m :used 0 :dropped 0 :lost (:dropped b 0)})
                                      ok (max 0 (min n (- max-lines-per-minute (:used b))))]
                                  (-> b (update :used + ok) (update :dropped + (- n ok)) (assoc :ok ok)))))
        b (after client)]
    [(:ok b) (when (not= (:minute (before client)) m) (:lost b))]))

(defn cut [text]
  (let [t (str text)]
    (if (> (count t) max-line-chars) (str (subs t 0 max-line-chars) "…") t)))

(defn output!
  "Log a batch of an App's output lines, within its budget."
  [{:keys [package lines dropped seq]}]
  (when-let [client (client-of package)]
    (let [[ok lost] (take-budget! client (count lines))
          lost (+ (or lost 0) (if (number? dropped) dropped 0))]
      (doseq [[i {:keys [at stream text]}] (map-indexed vector (take ok lines))]
        (log/info {:id ::line :data {:subject :app :app client :package package :stream (some-> stream name) :at at
                                     :seq seq :n i}}
                  (cut text)))
      (when (pos? lost)
        (log/warn {:id ::dropped :data {:subject :app :app client :package package :lines lost}}
                  (str lost " lines of output were dropped"))))))

(defn usage!
  "Keep the volume use from one usage sample of an App."
  [{vols :volumes}]
  (when (seq vols)
    (volumes/usage! vols)))

(def kinds {"output" :apps/output "event" :apps/event})

(defn app-logs
  "An App's output and events, for those who may read the App; newest first."
  [{:keys [app kind since until limit]}]
  (when-not (and (string? app) (not (str/blank? app))
                 (dataset/get-entity :iam/app {:xid app} {:xid nil}))
    (throw (ex-info "No such App, or you may not read it" {:code "APP_NOT_FOUND"})))
  (let [topic (if kind
                (or (kinds (name kind))
                    (throw (ex-info (str "kind is one of " (str/join ", " (keys kinds))) {:code "INVALID_LOG_FILTER"})))
                :apps)]
    (log.query/query (cond-> {:where {:topics [:has topic] [:data :app] app}
                              :limit (min 1000 (or limit 200))}
                       since (assoc :since since)
                       until (assoc :until until)))))

(defn name-apps
  "Give each ranked App of a host report its name and xid when the caller may read it; the others keep only their key."
  [ranked]
  (let [readable (into {}
                       (map (juxt (comp deploy/app-key :xid) identity))
                       (dataset/search-entity :iam/app nil {:xid nil :name nil}))]
    (mapv (fn [{:keys [app] :as r}]
            (if-let [{:keys [xid name]} (readable app)]
              (assoc r :xid xid :name name)
              r))
          ranked)))

(defn register! []
  (supervisor/register-method! "app.output" (fn [params] (output! params) nil))
  (supervisor/register-method! "app.usage" (fn [params] (usage! params) nil))
  (reset! engine/app-namer name-apps))

(defn unregister! []
  (supervisor/unregister-method! "app.output")
  (supervisor/unregister-method! "app.usage")
  (reset! engine/app-namer nil))

(defmethod engine/execute-operation "app-logs"
  [ctx]
  (assoc ctx :result (app-logs ctx)))
