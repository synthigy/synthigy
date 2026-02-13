(ns synthigy.admin.server
  "Protocol for admin server adapters.

  Provides a common interface for different HTTP server implementations
  (Pedestal, HTTP Kit, etc.).")

(defprotocol AdminServer
  "Protocol for admin HTTP server implementations."

  (start-server [this config]
    "Start the admin server with the given configuration.

    Config map:
      :handler - Ring handler function
      :host    - Host to bind to (default: 127.0.0.1)
      :port    - Port to bind to (default: random)

    Returns the server instance.")

  (stop-server [this]
    "Stop the admin server.

    Should be idempotent - safe to call multiple times.")

  (server-port [this]
    "Get the port the server is running on.

    Returns integer port number or nil if not running."))
