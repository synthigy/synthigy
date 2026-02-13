(ns synthigy.admin.pedestal
  "Pedestal adapter for admin server.

  Implements AdminServer protocol using Pedestal's Jetty integration."
  (:require
   [clojure.tools.logging :as log]
   [io.pedestal.http :as http]
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
;;; Pedestal Server Implementation
;;; ============================================================================

(defrecord PedestalAdminServer [server-atom]
  server/AdminServer

  (start-server [this {:keys [handler host port]
                       :or {host "127.0.0.1"}}]
    (when @server-atom
      (log/warn "[ADMIN.PEDESTAL] Server already running, stopping first")
      (server/stop-server this))

    (let [actual-port (or port (find-free-port))]
      (log/infof "[ADMIN.PEDESTAL] Starting Pedestal admin server on %s:%d" host actual-port)
      (try
        ;; Build Pedestal service map
        ;; Use a simple interceptor that delegates to the Ring handler
        (let [ring-handler-interceptor {:name ::admin-ring-handler
                                        :enter (fn [context]
                                                 (let [request (:request context)
                                                       response (handler request)]
                                                   (assoc context :response response)))}

              ;; Simple routing: catch everything
              routes `#{["/*path" :any [~ring-handler-interceptor]]}

              service-map {::http/routes routes
                           ::http/type :jetty
                           ::http/host host
                           ::http/port actual-port
                           ::http/join? false
                           ::http/resource-path nil}  ; No static resources

              pedestal-server (http/start (http/create-server service-map))]

          (reset! server-atom pedestal-server)
          (log/infof "[ADMIN.PEDESTAL] Pedestal admin server started: http://%s:%d/admin/info" host actual-port)
          this)
        (catch Exception e
          (log/error e "[ADMIN.PEDESTAL] Failed to start Pedestal admin server")
          (throw e)))))

  (stop-server [this]
    (when-let [pedestal-server @server-atom]
      (log/info "[ADMIN.PEDESTAL] Stopping Pedestal admin server...")
      (try
        (http/stop pedestal-server)
        (reset! server-atom nil)
        (log/info "[ADMIN.PEDESTAL] Pedestal admin server stopped")
        (catch Exception e
          (log/error e "[ADMIN.PEDESTAL] Error stopping Pedestal admin server"))))
    this)

  (server-port [this]
    (when-let [pedestal-server @server-atom]
      (try
        ;; Extract port from Pedestal's server map
        (let [jetty-server (get pedestal-server ::http/server)]
          (when jetty-server
            (-> jetty-server
                (.getConnectors)
                (first)
                (.getLocalPort))))
        (catch Exception e
          (log/error e "[ADMIN.PEDESTAL] Error getting server port")
          nil)))))

;;; ============================================================================
;;; Constructor
;;; ============================================================================

(defn create
  "Create a new Pedestal admin server instance.

  Note: Requires io.pedestal/pedestal.service and io.pedestal/pedestal.jetty
  dependencies in your project."
  []
  (->PedestalAdminServer (atom nil)))
