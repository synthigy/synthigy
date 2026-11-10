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

(ns synthigy.files
  "The :synthigy/files module: file bytes stored behind the File System dataset."
  (:require
   [clojure.string :as str]
   [environ.core :refer [env]]
   [patcho.lifecycle :as lifecycle]
   [patcho.patch :as patch]
   [synthigy.audit.protocol :as audit]
   [synthigy.dataset :as dataset]
   [synthigy.db :as db]
   synthigy.files.guard
   [synthigy.files.folders :as folders]
   [synthigy.files.http :as http]
   [synthigy.files.legacy :as legacy]
   [synthigy.files.migrate :as migrate]
   synthigy.files.probe
   [synthigy.files.schema :as schema]
   [synthigy.files.setup :as setup]
   [synthigy.files.links :as links]
   [synthigy.files.sweeper :as sweeper]
   [synthigy.files.storage.local :as local]
   [synthigy.files.storage.s3 :as s3]
   [synthigy.log :as log]
   [synthigy.server.routes :as routes]
   [synthigy.supervisor :as supervisor]))

(defonce ^:dynamic *storage* nil)

(defn flag? [v]
  (contains? #{"1" "true" "yes" "on"} (some-> v str/trim str/lower-case)))

(defn storage-from-env
  "The blob store SYNTHIGY_FILES_STORAGE selects."
  []
  (case (some-> (env :synthigy-files-storage) str/trim str/lower-case not-empty)
    (nil "local") (local/create)
    "s3" (s3/create {:bucket (not-empty (env :synthigy-files-s3-bucket))
                     :region (not-empty (env :synthigy-files-s3-region))
                     :endpoint (not-empty (env :synthigy-files-s3-endpoint))
                     :path-style? (flag? (env :synthigy-files-s3-path-style))
                     :access-key (not-empty (env :synthigy-files-s3-access-key-id))
                     :secret-key (not-empty (env :synthigy-files-s3-secret-access-key))})
    (throw (ex-info (str "SYNTHIGY_FILES_STORAGE must be local or s3, got " (env :synthigy-files-storage))
                    {:code :storage-unavailable}))))

(defn require-durable-audit!
  "Refuse to run on the in-memory audit provider: file versions live in audit history and it forgets them."
  []
  (let [p audit/*audit-provider*]
    (when (and p (= "synthigy.audit.memory.MemoryAudit" (.getName (class p))))
      (throw (ex-info "Files need a durable audit provider (postgres, sqlite or clickhouse), not memory"
                      {:code :audit-required})))))

(defn watch-model!
  "Re-install the uniqueness DDL whenever a deploy changes the tables it names."
  []
  (dataset/add-model-watch!
   ::schema
   (fn [_ _ old new]
     (when (not= (schema/names old) (schema/names new))
       (let [d db/*db*]
         ;; never inline — the watch fires inside a deploy, and SQLite has one writer
         (future
           (try (schema/ensure! d new)
                (catch Throwable t
                  (log/error! {:id ::schema-failed :msg "File name uniqueness DDL failed"
                               :data {:action :installing :subject :files}}
                              t)))))))))

(defn handler [request] (http/handler (fn [] *storage*) request))

(defn start []
  (require-durable-audit!)
  (let [s (storage-from-env)]
    (setup/level-model!)
    (folders/ensure-root!)
    (patch/level! :synthigy/files)
    (schema/ensure!)
    (legacy/level! s)
    (watch-model!)
    (alter-var-root #'*storage* (constantly s))
    (routes/register-prefix! ::files "/files" #'handler)
    (sweeper/start! (fn [] *storage*))
    (links/start!)
    (log/info {:id ::started :data {:action :started :subject :files :storage (.getSimpleName (class s))}}
              "Files module started")))

(defn stop []
  (migrate/abort!)
  (sweeper/stop!)
  (links/stop!)
  (routes/unregister-prefix! ::files)
  (dataset/remove-model-watch! ::schema)
  (alter-var-root #'*storage* (constantly nil))
  (log/info {:id ::stopped :data {:action :stopped :subject :files}} "Files module stopped"))

(lifecycle/register-module!
 :synthigy/files
 {:depends-on [:synthigy/iam]
  :doc "File bytes for the File System dataset, on local disk or S3"
  :start start
  :stop stop})

(supervisor/register-method! "files.migrate" (fn [settings] (migrate/start! *storage* settings)))

(supervisor/register-method! "files.migrate.status" (fn [_] (migrate/status)))

(supervisor/register-method! "files.migrate.commit"
  (fn [_] (migrate/commit! #(alter-var-root #'*storage* (constantly %)))))

(supervisor/register-method! "files.migrate.abort" (fn [_] (migrate/abort!)))
