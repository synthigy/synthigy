(ns synthigy.admin.httpkit
  "HTTP Kit adapter for admin server.

  Implements AdminServer protocol using HTTP Kit server.

  Dependencies (optional - add to your project if using HTTP Kit):
    http-kit/http-kit {:mvn/version \"2.8.0\"}"
  (:require
   [clojure.tools.logging :as log]
   [synthigy.admin.server :as server])
  (:import
   [java.net ServerSocket]))

;;; ============================================================================
;;; Utility Functions
;;; ============================================================================

(defn- find-free-port
  "Find a random free port on localhost."
  []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

;;; ============================================================================
;;; HTTP Kit Server Implementation
;;; ============================================================================

(defrecord HttpKitAdminServer [server-atom]
  server/AdminServer

  (start-server [this {:keys [handler host port]
                       :or {host "127.0.0.1"}}]
    (when @server-atom
      (log/warn "[ADMIN.HTTPKIT] Server already running, stopping first")
      (server/stop-server this))

    (let [actual-port (or port (find-free-port))]
      (log/infof "[ADMIN.HTTPKIT] Starting HTTP Kit admin server on %s:%d" host actual-port)
      (try
        ;; Dynamically require org.httpkit.server to avoid hard dependency
        (require 'org.httpkit.server)
        (let [run-server (resolve 'org.httpkit.server/run-server)
              stop-fn (run-server handler {:port actual-port
                                           :ip host
                                           :thread 4})] ; Small thread pool for admin
          (reset! server-atom {:stop-fn stop-fn
                               :port actual-port})
          (log/infof "[ADMIN.HTTPKIT] HTTP Kit admin server started: http://%s:%d/admin/info" host actual-port)
          this)
        (catch java.io.FileNotFoundException e
          (log/error "[ADMIN.HTTPKIT] HTTP Kit not found in classpath")
          (log/error "[ADMIN.HTTPKIT] Add dependency: http-kit/http-kit {:mvn/version \"2.8.0\"}")
          (throw (ex-info "HTTP Kit not available"
                          {:type :missing-dependency
                           :dependency "http-kit/http-kit"}
                          e)))
        (catch Exception e
          (log/error e "[ADMIN.HTTPKIT] Failed to start HTTP Kit admin server")
          (throw e)))))

  (stop-server [this]
    (when-let [{:keys [stop-fn]} @server-atom]
      (log/info "[ADMIN.HTTPKIT] Stopping HTTP Kit admin server...")
      (try
        (stop-fn :timeout 100) ; 100ms graceful shutdown
        (reset! server-atom nil)
        (log/info "[ADMIN.HTTPKIT] HTTP Kit admin server stopped")
        (catch Exception e
          (log/error e "[ADMIN.HTTPKIT] Error stopping HTTP Kit admin server"))))
    this)

  (server-port [this]
    (when-let [{:keys [port]} @server-atom]
      port)))

;;; ============================================================================
;;; Constructor
;;; ============================================================================

(defn create
  "Create a new HTTP Kit admin server instance.

  Note: Requires http-kit/http-kit dependency in your project."
  []
  (->HttpKitAdminServer (atom nil)))
