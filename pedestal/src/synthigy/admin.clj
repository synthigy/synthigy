(ns synthigy.admin
  "Pedestal admin service.

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
    [environ.core :refer [env]]
    [io.pedestal.http :as http]
    [patcho.lifecycle :as lifecycle]
    [synthigy.admin.core :as admin.core]
    synthigy.db
    [synthigy.env :as senv]
    [synthigy.log :as log])
  (:import
    [java.net ServerSocket]))

;;; ============================================================================
;;; State
;;; ============================================================================

(defonce ^:private server (atom nil))

;;; ============================================================================
;;; Port File Management
;;; ============================================================================

(defn- write-port-file! [port]
  (try
    (fs/create-dirs (fs/parent senv/admin-port))
    (spit senv/admin-port (str port))
    (log/info {:id ::port-file-written :data {:action :written :subject :port-file :path senv/admin-port}}
              "Port file written")
    (catch Exception e
      (log/error! {:id ::port-file-write-failed :data {:action :writing :subject :port-file}} e))))

(defn- delete-port-file! []
  (try
    (when (fs/exists? senv/admin-port)
      (fs/delete senv/admin-port)
      (log/info {:id ::port-file-deleted :data {:action :deleted :subject :port-file}} "Port file deleted"))
    (catch Exception e
      (log/error! {:id ::port-file-delete-failed :data {:action :deleting :subject :port-file}} e))))

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
     (log/warn {:id ::already-running :data {:action :starting :subject :admin-server}} "Already running, stopping first")
     (stop))

   (let [actual-port (or port
                         (when-let [p (env :synthigy-admin-port)]
                           (try (Integer/parseInt p) (catch Exception _ nil)))
                         (find-free-port))
         ;; Wrap Ring handler as interceptor
         ring-interceptor {:name ::ring-handler
                           :enter (fn [ctx]
                                    (assoc ctx :response (admin.core/app (:request ctx))))}
         routes `#{["/*path" :any [~ring-interceptor]]}
         service-map {::http/routes routes
                      ::http/type :jetty
                      ::http/host "127.0.0.1"
                      ::http/port actual-port
                      ::http/join? false
                      ::http/resource-path nil}]

     (log/info {:id ::starting :data {:action :starting :subject :admin-server :host "127.0.0.1" :port actual-port}}
               "Starting admin server")
     (try
       (reset! server (http/start (http/create-server service-map)))
       (write-port-file! actual-port)
       (log/info {:id ::ready
                  :data {:url (format "http://127.0.0.1:%d/admin/info" actual-port)}}
                 "Admin server ready")
       (catch Exception e
         (log/error! {:id ::start-failed} e)
         (reset! server nil)
         (throw e))))))

(defn stop
  "Stop the admin server."
  []
  (when-let [s @server]
    (log/info {:id ::stopping :data {:action :stopping :subject :admin-server}} "Stopping...")
    (try
      (http/stop s)
      (catch Exception e
        (log/error! {:id ::stop-error} e)))
    (reset! server nil)
    (delete-port-file!)
    (log/info {:id ::stopped :data {:action :stopped :subject :admin}} "Stopped")))

(defn port
  "Get the current admin server port (or nil if not running)."
  []
  (when-let [s @server]
    (try
      (some-> s (get ::http/server) .getConnectors first .getLocalPort)
      (catch Exception _ nil))))

;;; ============================================================================
;;; Module Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/admin
  {:depends-on [:synthigy/iam]
   :doc "Admin UI + management routes"
   :start (fn []
            (log/info {:id ::lifecycle-starting :data {:action :starting}} "Starting admin service...")
            (start)
            (log/info {:id ::lifecycle-started :data {:action :started}} "Admin service started"))
   :stop (fn []
           (log/info {:id ::lifecycle-stopping :data {:action :stopping}} "Stopping admin service...")
           (stop)
           (log/info {:id ::lifecycle-stopped :data {:action :stopped}} "Admin service stopped"))})

;;; ============================================================================
;;; Main
;;; ============================================================================

(defn -main [& _]
  (try
    (start)
    (log/info {:id ::server-running :data {:action :ready :subject :admin-server}} "Admin server running. Press Ctrl+C to stop.")
    (catch Throwable ex
      (log/error! {:id ::server-start-failed :data {:action :starting :subject :admin-server}} ex)
      (System/exit 1))))
