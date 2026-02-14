(ns synthigy.ws
  "Undertow WebSocket handler for GraphQL subscriptions.

   Works with ring-undertow-adapter (Luminus/Kit).

   ## With Kit/Reitit

   ```clojure
   (require '[synthigy.ws :as ws])
   (require '[synthigy.graphql :as graphql])

   (def routes
     [[\"/graphql\" {:get graphql-handler
                    :post graphql-handler}]
      [\"/graphql-ws\" {:get (ws/handler @schema {})}]])
   ```"
  (:require
    [clojure.core.async :refer [chan put! go-loop <!]]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [ring.adapter.undertow.websocket :as undertow-ws]
    [synthigy.ws.protocol :as proto]
    [synthigy.ws.lacinia :as lacinia]))

(defn handler
  "Ring handler for WebSocket GraphQL subscriptions.

   Returns a handler that returns :undertow/websocket response.

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
                      :context-fn (or context-fn (constantly {}))
                      :keep-alive-ms keep-alive-ms})]
    (fn [request]
      (if (and on-connect (not (on-connect request)))
        {:status 401 :body "Unauthorized"}

        (let [session-id (str (java.util.UUID/randomUUID))
              subprotocol (some-> (get-in request [:headers "sec-websocket-protocol"])
                                  (str/split #",\s*")
                                  first)
              ws-text-ch (chan 1)
              response-ch (chan 10)
              state-atom (atom nil)]

          {:undertow/websocket
           {:on-open
            (fn [{:keys [channel]}]
              (log/trace ::ws-open {:session-id session-id})
              (let [state ((:make-state ws-handler) session-id subprotocol)]
                (reset! state-atom (assoc state
                                          :ws-text-ch ws-text-ch
                                          :response-ch response-ch
                                          :channel channel))
                (go-loop []
                  (when-some [msg (<! response-ch)]
                    (undertow-ws/send (proto/encode msg) channel)
                    (recur)))
                ((:start ws-handler) ws-text-ch response-ch state)))

            :on-message
            (fn [{:keys [data]}]
              (when (string? data)
                (put! ws-text-ch data)))

            :on-close-message
            (fn [{:keys [_message]}]
              (log/trace ::ws-close {:session-id session-id})
              ((:on-close ws-handler) @state-atom))

            :on-error
            (fn [{:keys [error]}]
              (log/error error ::ws-error {:session-id session-id})
              ((:on-close ws-handler) @state-atom))}})))))

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
      (if (= (:uri request) path)
        (ws-handler request)
        (next-handler request)))))
