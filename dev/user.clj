(ns user
  "REPL utilities for Synthigy development.

  ## Starting the REPL

  Use the appropriate backend alias:
    clj -M:postgres:pedestal:dev
    clj -M:sqlite:pedestal:dev

  ## Available functions:

  - (start)        - Start full batteries-included server (OAuth, GraphQL, HTTP)
  - (start-core)   - Start core only (dataset + IAM, no server)
  - (stop)         - Stop all systems
  - (restart)      - Stop and start full system
  - (status)       - Show lifecycle system report

  Or use lifecycle directly:
  - (lifecycle/start! :synthigy/dataset :synthigy/iam)  - Start specific modules
  - (lifecycle/stop-all!)                                 - Stop all modules
  - (lifecycle/print-system-report)                       - Show system status
  - (lifecycle/system-report)                             - Get status as data"
  (:require
    [synthigy.data :as data]
    [synthigy.iam :as iam]))

(comment
  ;; 1. Setup - ensure namespaces are loaded
  (do
    (require '[synthigy.transit])
    (require '[synthigy.dataset.id :as id])
    (require '[synthigy.dataset :as dataset])
    (require '[synthigy.dataset.postgres.xid :as xid])
    (require '[synthigy.dataset.patch.model :as model]))

  ;; 2. Check current state
  (dataset/current-format)

  ;; 4. Run migration
  (xid/migrate-to-xid!)

  ;; 5. Reset migration (if needed)
  (xid/reset-xid-migration!)

  (iam/set-user {:name "admin"
                 :password "admin"
                 :active true
                 :roles [data/*ROOT*]}))
