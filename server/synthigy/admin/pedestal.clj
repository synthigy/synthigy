(ns synthigy.admin.pedestal
  "Pedestal adapter for admin server.

  Implements AdminServer protocol using Pedestal's Jetty integration."
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
        ;; Dynamically require Pedestal to avoid hard dependency
        (require 'io.pedestal.http)
        (require 'io.pedestal.http.ring-middlewares)
        (let [http-ns (find-ns 'io.pedestal.http)
              create-server (ns-resolve http-ns 'create-server)
              start (ns-resolve http-ns 'start)

              ;; Build Pedestal service map
              ;; Use a simple interceptor that delegates to the Ring handler
              ring-handler-interceptor {:name ::admin-ring-handler
                                        :enter (fn [context]
                                                 (let [request (:request context)
                                                       response (handler request)]
                                                   (assoc context :response response)))}

              ;; Simple routing: catch everything
              routes `#{["/*path" :any [~ring-handler-interceptor]]}

              service-map {:io.pedestal.http/routes routes
                           :io.pedestal.http/type :jetty
                           :io.pedestal.http/host host
                           :io.pedestal.http/port actual-port
                           :io.pedestal.http/join? false
                           :io.pedestal.http/resource-path nil}  ; No static resources

              pedestal-server (start (create-server service-map))]

          (reset! server-atom pedestal-server)
          (log/infof "[ADMIN.PEDESTAL] Pedestal admin server started: http://%s:%d/admin/info" host actual-port)
          this)
        (catch java.io.FileNotFoundException e
          (log/error "[ADMIN.PEDESTAL] Pedestal not found in classpath")
          (log/error "[ADMIN.PEDESTAL] Add dependencies: io.pedestal/pedestal.service, io.pedestal/pedestal.jetty")
          (throw (ex-info "Pedestal not available"
                          {:type :missing-dependency
                           :dependencies ["io.pedestal/pedestal.service"
                                          "io.pedestal/pedestal.jetty"]}
                          e)))
        (catch Exception e
          (log/error e "[ADMIN.PEDESTAL] Failed to start Pedestal admin server")
          (throw e)))))

  (stop-server [this]
    (when-let [pedestal-server @server-atom]
      (log/info "[ADMIN.PEDESTAL] Stopping Pedestal admin server...")
      (try
        ;; Dynamically resolve stop function
        (require 'io.pedestal.http)
        (let [http-ns (find-ns 'io.pedestal.http)
              stop (ns-resolve http-ns 'stop)]
          (stop pedestal-server))
        (reset! server-atom nil)
        (log/info "[ADMIN.PEDESTAL] Pedestal admin server stopped")
        (catch Exception e
          (log/error e "[ADMIN.PEDESTAL] Error stopping Pedestal admin server"))))
    this)

  (server-port [this]
    (when-let [pedestal-server @server-atom]
      (try
        ;; Extract port from Pedestal's server map
        (let [jetty-server (get pedestal-server :io.pedestal.http/server)]
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
