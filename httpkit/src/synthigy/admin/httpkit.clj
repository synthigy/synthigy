(ns synthigy.admin.httpkit
  "HTTP Kit adapter for admin server.

  TODO: Implement HTTP Kit admin adapter.

  This is a stub - HTTP Kit adapter is not yet implemented."
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
;;; HTTP Kit Server Implementation (Stub)
;;; ============================================================================

(defrecord HttpKitAdminServer [server-atom]
  server/AdminServer

  (start-server [_this {:keys [_handler _host port]
                        :or {_host "127.0.0.1"}}]
    (let [_actual-port (or port (find-free-port))]
      (log/error "[ADMIN.HTTPKIT] HTTP Kit admin adapter not yet implemented")
      (throw (ex-info "HTTP Kit admin adapter not yet implemented"
                      {:type :not-implemented
                       :suggestion "Use :pedestal alias instead"}))))

  (stop-server [this]
    (when-let [_server @server-atom]
      (log/info "[ADMIN.HTTPKIT] Stopping HTTP Kit admin server (stub)...")
      (reset! server-atom nil))
    this)

  (server-port [_this]
    nil))

;;; ============================================================================
;;; Constructor
;;; ============================================================================

(defn create
  "Create a new HTTP Kit admin server instance (STUB).

  Note: HTTP Kit admin adapter is not yet implemented.
  Use Pedestal instead."
  []
  (->HttpKitAdminServer (atom nil)))
