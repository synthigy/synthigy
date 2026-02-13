(ns synthigy.admin
  "HTTP Kit-based admin service.

  TODO: Implement HTTP Kit admin service.

  This is a stub - HTTP Kit admin is not yet implemented.
  Use Pedestal backend instead: clj -M:postgres:pedestal or clj -M:sqlite:pedestal"
  (:require
    [clojure.tools.logging :as log]
    [patcho.lifecycle :as lifecycle]
    synthigy.db))  ;; Backend loaded via classpath

;;; ============================================================================
;;; Stub Implementation
;;; ============================================================================

(defn start
  "Start the admin server using HTTP Kit (NOT YET IMPLEMENTED)."
  ([] (start {}))
  ([_opts]
   (throw (ex-info "HTTP Kit admin not yet implemented. Use :pedestal alias instead."
                   {:type :not-implemented
                    :suggestion "clj -M:postgres:pedestal or clj -M:sqlite:pedestal"}))))

(defn stop
  "Stop the admin server."
  []
  nil)

(defn port
  "Get the current admin server port (or nil if not running)."
  []
  nil)

;;; ============================================================================
;;; Module Lifecycle Registration
;;; ============================================================================

(lifecycle/register-module!
  :synthigy/admin
  {:depends-on []
   :start (fn []
            (log/error "[ADMIN] HTTP Kit admin not yet implemented")
            (throw (ex-info "HTTP Kit admin not yet implemented" {:type :not-implemented})))
   :stop (fn []
           (log/info "[ADMIN] HTTP Kit admin stopped (stub)"))})

;;; ============================================================================
;;; Main Entry Point
;;; ============================================================================

(defn -main
  "Main entry point (NOT YET IMPLEMENTED)."
  [& _args]
  (log/error "HTTP Kit admin not yet implemented. Use :pedestal alias instead.")
  (System/exit 1))
