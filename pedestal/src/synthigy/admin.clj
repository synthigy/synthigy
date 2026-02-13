(ns synthigy.admin
  "Pedestal-based admin service.

  Provides localhost-only HTTP API for system administration using Pedestal.

  Usage:
    ;; Start
    (start)

    ;; Stop
    (stop)

    ;; Get current port
    (port)

  Required Dependencies:
    - io.pedestal/pedestal.service
    - io.pedestal/pedestal.jetty

  Environment Variables:
    SYNTHIGY_ADMIN_PORT - Fixed port (default: random)"
  (:require
    [babashka.fs :as fs]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]
    [synthigy.admin.core :as admin.core]
    [synthigy.admin.pedestal :as admin.pedestal]
    [synthigy.admin.server :as admin.server]
    synthigy.db  ;; Backend loaded via classpath (postgres/src or sqlite/src)
    [synthigy.env :as senv]))

;;; ============================================================================
;;; State Management
;;; ============================================================================

(defonce ^:private server (atom nil))

;;; ============================================================================
;;; Port File Management
;;; ============================================================================

(defn- write-port-file!
  "Write the admin port to the port file."
  [port]
  (try
    (fs/create-dirs (fs/parent senv/admin-port))
    (spit senv/admin-port (str port))
    (log/infof "[ADMIN] Port file written: %s" senv/admin-port)
    (catch Exception e
      (log/error e "[ADMIN] Failed to write port file"))))

(defn- delete-port-file!
  "Delete the admin port file."
  []
  (try
    (when (fs/exists? senv/admin-port)
      (fs/delete senv/admin-port)
      (log/info "[ADMIN] Port file deleted"))
    (catch Exception e
      (log/error e "[ADMIN] Failed to delete port file"))))

;;; ============================================================================
;;; Lifecycle Functions
;;; ============================================================================

(declare stop)

(defn start
  "Start the admin server using Pedestal.

  Options:
    :port - Port to bind to (default: random)"
  ([] (start {}))
  ([{:keys [port]}]
   (when @server
     (log/warn "[ADMIN] Admin server already running, stopping first")
     (stop))

   (let [fixed-port (or port
                        (when-let [env-port (env :synthigy-admin-port)]
                          (try
                            (Integer/parseInt env-port)
                            (catch Exception _
                              (log/warnf "[ADMIN] Invalid port in SYNTHIGY_ADMIN_PORT: %s" env-port)
                              nil))))
         server-instance (admin.pedestal/create)]

     (log/info "[ADMIN] Starting admin service (Pedestal)...")
     (try
       (admin.server/start-server server-instance
                                  {:handler admin.core/app
                                   :host "127.0.0.1"
                                   :port fixed-port})
       (reset! server server-instance)

       (when-let [actual-port (admin.server/server-port server-instance)]
         (write-port-file! actual-port)
         (log/infof "[ADMIN] Admin service ready: http://127.0.0.1:%d/admin/info" actual-port))

       (catch Exception e
         (log/error e "[ADMIN] Failed to start admin server")
         (reset! server nil)
         (throw e))))))

(defn stop
  "Stop the admin server."
  []
  (when-let [server-instance @server]
    (log/info "[ADMIN] Stopping admin service...")
    (try
      (admin.server/stop-server server-instance)
      (delete-port-file!)
      (reset! server nil)
      (log/info "[ADMIN] Admin service stopped")
      (catch Exception e
        (log/error e "[ADMIN] Error stopping admin server")))))

(defn port
  "Get the current admin server port (or nil if not running)."
  []
  (when-let [server-instance @server]
    (admin.server/server-port server-instance)))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/admin
  {:depends-on [] ; Admin has no dependencies - it's the control plane
   :start (fn []
            (log/info "[ADMIN] Starting admin module...")
            (start)
            (log/info "[ADMIN] Admin module started"))
   :stop (fn []
           (log/info "[ADMIN] Stopping admin module...")
           (stop)
           (log/info "[ADMIN] Admin module stopped"))})

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point for Synthigy - starts the admin control plane.

  The admin service is the primary interface for managing Synthigy:
  - System initialization (datasets, IAM, encryption)
  - Server control (start/stop HTTP servers)
  - Superuser management
  - Health monitoring

  Control via the synthigy CLI agent:
    synthigy init encryption     # Initialize encryption with generated key
    synthigy init datasets       # Initialize dataset tables
    synthigy init iam            # Initialize IAM tables
    synthigy server start        # Start HTTP server (Pedestal)
    synthigy server stop         # Stop HTTP server
    synthigy superuser add       # Create superuser
    synthigy doctor              # Health checks

  Database backend selection via deps.edn aliases:
    clj -M:postgres:pedestal -m synthigy.admin  # PostgreSQL
    clj -M:sqlite:pedestal -m synthigy.admin    # SQLite

  Configuration via environment variables:
    SYNTHIGY_ADMIN_PORT - Fixed port (default: random)

  See https://github.com/synthigy/synthigy for complete documentation."
  [& _args]
  (try
    ;; Backend is loaded via classpath - just start the admin
    (log/info "Starting Synthigy admin control plane (Pedestal)...")
    (lifecycle/start! :synthigy/admin)
    (log/info "Synthigy admin ready. Use synthigy CLI for management.")
    (catch Throwable ex
      (log/error ex "Failed to start Synthigy admin")
      (.printStackTrace ex)
      (System/exit 1))))
