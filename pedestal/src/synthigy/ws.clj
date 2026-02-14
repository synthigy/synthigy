(ns synthigy.ws
  "Pedestal WebSocket handler for GraphQL subscriptions.

   Uses Jakarta WebSocket API via io.pedestal.websocket.
   Drop-in replacement for lacinia-pedestal's subscription-websocket-endpoint."
  (:require
    [clojure.core.async :refer [chan put! go-loop <!]]
    [clojure.tools.logging :as log]
    [io.pedestal.websocket :as pedestal-ws]
    [synthigy.ws.protocol :as proto]
    [synthigy.ws.lacinia :as lacinia])
  (:import
    [jakarta.websocket Session EndpointConfig]))

(defn endpoint
  "Creates a WebSocket endpoint map for Pedestal's ::http/websockets.

   Args:
     schema-provider - Compiled schema or (fn [] schema)
     opts            - Options:
                       :app-context         - Base context for resolvers
                       :context-fn          - (fn [connection-params] context)
                       :keep-alive-ms       - Keep-alive interval (default: 25000)
                       :send-buffer-or-n    - Send channel buffer (default: 10)
                       :session-initializer - (fn [Session EndpointConfig])
                       :context-initializer - (fn [context Session] context)
                       :idle-timeout-ms     - WebSocket idle timeout

   Returns:
     Endpoint map for ::http/websockets

   Usage:
     (assoc-in service-map [::http/websockets \"/graphql-ws\"]
       (ws/endpoint @schema {:app-context {:db *db*}}))"
  [schema-provider {:keys [app-context context-fn keep-alive-ms
                           send-buffer-or-n session-initializer
                           context-initializer idle-timeout-ms]
                    :or {keep-alive-ms 25000
                         send-buffer-or-n 10}}]
  (let [ws-handler (lacinia/subscription-handler
                     schema-provider
                     {:app-context (or app-context {})
                      :context-fn context-fn
                      :keep-alive-ms keep-alive-ms})]
    (cond-> {:subprotocols proto/supported-subprotocols

             :on-open
             (fn [^Session session ^EndpointConfig config]
               (when session-initializer
                 (session-initializer session config))

               (let [session-id (.getId session)
                     subprotocol (.getNegotiatedSubprotocol session)
                     _ (log/trace ::ws-open {:session-id session-id
                                             :subprotocol subprotocol})

                     send-ch (pedestal-ws/start-ws-connection session
                               {:send-buffer-or-n send-buffer-or-n})
                     ws-text-ch (chan 1)
                     response-ch (chan 10)

                     state ((:make-state ws-handler) session-id subprotocol)
                     state (assoc state
                                  :ws-text-ch ws-text-ch
                                  :response-ch response-ch
                                  :send-ch send-ch)
                     state (if context-initializer
                             (context-initializer state session)
                             state)]

                 (go-loop []
                   (when-some [msg (<! response-ch)]
                     (when (put! send-ch (proto/encode msg))
                       (recur))))

                 ((:start ws-handler) ws-text-ch response-ch state)
                 state))

             :on-text
             (fn [state text]
               (put! (:ws-text-ch state) text))

             :on-close
             (fn [state _session reason]
               (log/trace ::ws-close {:session-id (:session-id state)
                                      :reason reason})
               ((:on-close ws-handler) state))

             :on-error
             (fn [state _session ^Throwable error]
               (log/error error ::ws-error {:session-id (:session-id state)})
               ((:on-close ws-handler) state))}

      idle-timeout-ms (assoc :idle-timeout-ms idle-timeout-ms))))

(defn get-session-param
  "Get a request parameter from the WebSocket session.

   Useful for authentication in session-initializer."
  [^Session session param-name]
  (-> session
      .getRequestParameterMap
      (get param-name)
      first))
