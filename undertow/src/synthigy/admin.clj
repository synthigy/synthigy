(ns synthigy.admin
  "Undertow admin service.

  Provides localhost-only HTTP API for system administration.

  Usage:
    (start)       ; Start on random port
    (start {:port 9000})
    (stop)
    (port)        ; Get current port

  Environment Variables:
    SYNTHIGY_ADMIN_PORT - Fixed port (default: random)"
  (:require
    [babashka.fs :as fs]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [patcho.lifecycle :as lifecycle]
    [ring.adapter.undertow :refer [run-undertow]]
    [synthigy.admin.core :as admin.core]
    synthigy.db
    [synthigy.env :as senv])
  (:import
    [java.net ServerSocket]))

;;; ============================================================================
;;; State
;;; ============================================================================

(defonce ^:private server (atom nil))
(defonce ^:private port-atom (atom nil))

;;; ============================================================================
;;; Port File Management
;;; ============================================================================

(defn- write-port-file! [port]
  (try
    (fs/create-dirs (fs/parent senv/admin-port))
    (spit senv/admin-port (str port))
    (log/infof "[ADMIN] Port file written: %s" senv/admin-port)
    (catch Exception e
      (log/error e "[ADMIN] Failed to write port file"))))

(defn- delete-port-file! []
  (try
    (when (fs/exists? senv/admin-port)
      (fs/delete senv/admin-port)
      (log/info "[ADMIN] Port file deleted"))
    (catch Exception e
      (log/error e "[ADMIN] Failed to delete port file"))))

(defn- find-free-port []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

;;; ============================================================================
;;; Lifecycle
;;; ============================================================================

(declare stop)

(defn start
  "Start the admin server.

  Options:
    :port - Port to bind to (default: random)"
  ([] (start {}))
  ([{:keys [port]}]
   (when @server
     (log/warn "[ADMIN] Already running, stopping first")
     (stop))

   (let [actual-port (or port
                         (when-let [p (env :synthigy-admin-port)]
                           (try (Integer/parseInt p) (catch Exception _ nil)))
                         (find-free-port))]
     (log/infof "[ADMIN] Starting on 127.0.0.1:%d (undertow)" actual-port)
     (try
       (reset! server (run-undertow admin.core/app {:host "127.0.0.1" :port actual-port}))
       (reset! port-atom actual-port)
       (write-port-file! actual-port)
       (log/infof "[ADMIN] Ready: http://127.0.0.1:%d/admin/info" actual-port)
       (catch Exception e
         (log/error e "[ADMIN] Failed to start")
         (reset! server nil)
         (reset! port-atom nil)
         (throw e))))))

(defn stop
  "Stop the admin server."
  []
  (when-let [s @server]
    (log/info "[ADMIN] Stopping...")
    (.stop s)
    (reset! server nil)
    (reset! port-atom nil)
    (delete-port-file!)
    (log/info "[ADMIN] Stopped")))

(defn port
  "Get the current admin server port (or nil if not running)."
  []
  @port-atom)

;;; ============================================================================
;;; Module Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/admin
  {:depends-on [:synthigy/iam]
   :start (fn []
            (log/info "[ADMIN] Starting admin service...")
            (start)
            (log/info "[ADMIN] Admin service started"))
   :stop (fn []
           (log/info "[ADMIN] Stopping admin service...")
           (stop)
           (log/info "[ADMIN] Admin service stopped"))})

;;; ============================================================================
;;; Main
;;; ============================================================================

(defn -main [& _]
  (try
    (start)
    (log/info "Admin server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error ex "Failed to start admin server")
      (System/exit 1))))
