(ns synthigy.ws
  "http-kit WebSocket handler for GraphQL subscriptions.

   Provides middleware and handlers for GraphQL subscriptions over WebSocket.
   Supports both graphql-ws and graphql-transport-ws protocols."
  (:require
    [clojure.core.async :refer [chan put! go-loop <!]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [org.httpkit.server :as hk]
    [synthigy.ws.protocol :as proto]
    [synthigy.ws.lacinia :as lacinia]))

(defn handler
  "Ring handler for WebSocket GraphQL subscriptions.

   Returns a handler that upgrades to WebSocket when appropriate.

   Args:
     schema-provider - Compiled schema or (fn [] schema)
     opts            - Options:
                       :context-fn    - (fn [connection-params] context)
                       :app-context   - Base context for resolvers
                       :keep-alive-ms - Keep-alive interval (default: 25000)
                       :on-connect    - (fn [request] truthy-or-nil) Auth hook

   Returns:
     Ring handler"
  [schema-provider {:keys [app-context context-fn keep-alive-ms on-connect]
                    :or {keep-alive-ms 25000}}]
  (let [ws-handler (lacinia/subscription-handler
                     schema-provider
                     {:app-context (or app-context {})
                      :context-fn context-fn
                      :keep-alive-ms keep-alive-ms})]
    (fn [request]
      (if-not (hk/websocket? request)
        {:status 400 :body "WebSocket upgrade required"}

        (if (and on-connect (nil? (on-connect request)))
          {:status 401 :body "Unauthorized"}

          (let [session-id (str (java.util.UUID/randomUUID))
                subprotocol (some-> (get-in request [:headers "sec-websocket-protocol"])
                                    (str/split #",\s*")
                                    first)
                ws-text-ch (chan 1)
                response-ch (chan 10)
                state-atom (atom nil)]

            (hk/as-channel request
              {:on-open
               (fn [ch]
                 (log/trace ::ws-open {:session-id session-id})
                 (let [state ((:make-state ws-handler) session-id subprotocol)]
                   (reset! state-atom (assoc state
                                             :ws-text-ch ws-text-ch
                                             :response-ch response-ch))
                   (go-loop []
                     (when-some [msg (<! response-ch)]
                       (when (hk/open? ch)
                         (hk/send! ch (proto/encode msg)))
                       (recur)))
                   ((:start ws-handler) ws-text-ch response-ch state)))

               :on-receive
               (fn [_ch text]
                 (put! ws-text-ch text))

               :on-close
               (fn [_ch status]
                 (log/trace ::ws-close {:session-id session-id :status status})
                 ((:on-close ws-handler) @state-atom))})))))))

(defn wrap-websocket
  "Ring middleware that upgrades WebSocket at specified path.

   Args:
     next-handler - Handler for non-WebSocket requests
     opts         - Options:
                    :schema        - Schema provider (required)
                    :path          - WebSocket path (default: \"/graphql-ws\")
                    :context-fn    - Context builder
                    :app-context   - Base context
                    :keep-alive-ms - Keep-alive interval
                    :on-connect    - Auth hook

   Returns:
     Ring handler"
  [next-handler {:keys [schema path] :or {path "/graphql-ws"} :as opts}]
  (let [ws-handler (handler schema (dissoc opts :schema :path))]
    (fn [request]
      (if (and (= (:uri request) path) (hk/websocket? request))
        (ws-handler request)
        (next-handler request)))))
