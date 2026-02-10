(ns synthigy.admin.jetty
  "Jetty adapter for admin server.

  Implements AdminServer protocol using Ring's Jetty adapter."
  (:require
   [clojure.tools.logging :as log]
   [ring.adapter.jetty :as jetty]
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
;;; Jetty Server Implementation
;;; ============================================================================

(defrecord JettyAdminServer [server-atom]
  server/AdminServer

  (start-server [this {:keys [handler host port]
                       :or {host "127.0.0.1"}}]
    (when @server-atom
      (log/warn "[ADMIN.JETTY] Server already running, stopping first")
      (server/stop-server this))

    (let [actual-port (or port (find-free-port))]
      (log/infof "[ADMIN.JETTY] Starting Jetty admin server on %s:%d" host actual-port)
      (try
        (let [jetty-server (jetty/run-jetty handler
                                            {:port actual-port
                                             :host host
                                             :join? false})]
          (reset! server-atom jetty-server)
          (log/infof "[ADMIN.JETTY] Jetty admin server started: http://%s:%d/admin/info" host actual-port)
          this)
        (catch Exception e
          (log/error e "[ADMIN.JETTY] Failed to start Jetty admin server")
          (throw e)))))

  (stop-server [this]
    (when-let [jetty-server @server-atom]
      (log/info "[ADMIN.JETTY] Stopping Jetty admin server...")
      (try
        (.stop jetty-server)
        (reset! server-atom nil)
        (log/info "[ADMIN.JETTY] Jetty admin server stopped")
        (catch Exception e
          (log/error e "[ADMIN.JETTY] Error stopping Jetty admin server"))))
    this)

  (server-port [this]
    (when-let [jetty-server @server-atom]
      (try
        (-> jetty-server
            (.getConnectors)
            (first)
            (.getLocalPort))
        (catch Exception e
          (log/error e "[ADMIN.JETTY] Error getting server port")
          nil)))))

;;; ============================================================================
;;; Constructor
;;; ============================================================================

(defn create
  "Create a new Jetty admin server instance."
  []
  (->JettyAdminServer (atom nil)))
