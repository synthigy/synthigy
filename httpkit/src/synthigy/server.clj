(ns synthigy.server
  "HTTP Kit server implementation for Synthigy.

  TODO: Implement HTTP Kit server adapter.

  This is a stub - HTTP Kit server is not yet implemented.
  Use Pedestal backend instead: clj -M:postgres:pedestal or clj -M:sqlite:pedestal"
  (:require
    [clojure.tools.logging :as log]
    [patcho.lifecycle :as lifecycle]))

;;; ============================================================================
;;; Stub Implementation
;;; ============================================================================

(defn start
  "Start the HTTP Kit server (NOT YET IMPLEMENTED)."
  ([] (start {}))
  ([_opts]
   (throw (ex-info "HTTP Kit server not yet implemented. Use :pedestal alias instead."
                   {:type :not-implemented
                    :suggestion "clj -M:postgres:pedestal or clj -M:sqlite:pedestal"}))))

(defn stop
  "Stop the HTTP Kit server (NOT YET IMPLEMENTED)."
  []
  nil)

(defn -main
  "Main entry point (NOT YET IMPLEMENTED)."
  [& _]
  (log/error "HTTP Kit server not yet implemented. Use :pedestal alias instead.")
  (System/exit 1))

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/server
  {:depends-on [:synthigy/admin]
   :start (fn []
            (log/error "[SERVER] HTTP Kit server not yet implemented")
            (throw (ex-info "HTTP Kit server not yet implemented" {:type :not-implemented})))
   :stop (fn []
           (log/info "[SERVER] HTTP Kit server stopped (stub)"))})
