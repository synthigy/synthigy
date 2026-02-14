(ns synthigy.ws.subscription
  "Core GraphQL subscription handler - Apollo WebSocket protocol state machine.

  Server-agnostic implementation using core.async channels. Adapters provide:
  - ws-text-ch: incoming text messages from client
  - response-ch: outgoing response maps (adapter handles encoding + sending)

  The connection-loop implements the Apollo protocol state machine for handling
  connection lifecycle, keep-alive, and subscription management."
  (:require
    [clojure.core.async :as async :refer [chan go go-loop <! >! alt! close! put! thread]]
    [clojure.tools.logging :as log]
    [synthigy.ws.protocol :as proto]))

;; =============================================================================
;; Subscription Execution
;; =============================================================================

(defn- execute-operation
  "Execute a GraphQL operation (query, mutation, or subscription).

  For subscriptions, the execute-fn should return a map with:
    :source-stream - channel that will receive streamed values
    :cleanup-fn    - function to call when subscription ends

  Returns a shutdown channel - close it to terminate the subscription."
  [id payload execute-fn context response-ch subprotocol cleanup-ch]
  (let [{:keys [query variables operationName]} payload
        shutdown-ch (chan)]
    (thread
      (try
        (let [result (execute-fn context query variables operationName)]
          (cond
            ;; Subscription with source stream
            (:source-stream result)
            (let [{:keys [source-stream cleanup-fn]} result]
              (loop []
                (let [[v port] (async/alts!! [shutdown-ch source-stream])]
                  (cond
                    ;; Shutdown requested
                    (= port shutdown-ch)
                    (do
                      (when cleanup-fn (cleanup-fn))
                      (put! cleanup-ch id))

                    ;; Stream closed
                    (nil? v)
                    (do
                      (put! response-ch (proto/complete-message id))
                      (when cleanup-fn (cleanup-fn))
                      (put! cleanup-ch id))

                    ;; Got value - send it
                    :else
                    (do
                      (put! response-ch (proto/data-message subprotocol id v))
                      (recur))))))

            ;; Error result
            (:errors result)
            (do
              (put! response-ch (proto/error-message id result))
              (put! response-ch (proto/complete-message id))
              (put! cleanup-ch id))

            ;; Regular query/mutation result
            :else
            (do
              (put! response-ch (proto/data-message subprotocol id result))
              (put! response-ch (proto/complete-message id))
              (put! cleanup-ch id))))

        (catch Throwable t
          (log/error t ::execute-error {:id id})
          (put! response-ch (proto/error-message id (.getMessage t) {:exception (str t)}))
          (put! cleanup-ch id))))
    shutdown-ch))

;; =============================================================================
;; Connection Loop
;; =============================================================================

(defn connection-loop
  "Apollo protocol state machine for a WebSocket connection.

  Handles:
  - connection_init / connection_ack handshake
  - ping/pong (modern) and keep-alive (legacy)
  - start/subscribe and stop/complete for operations
  - connection_terminate

  Args:
    session-id    - Unique session ID for logging
    keep-alive-ms - Keep-alive interval in milliseconds
    ws-text-ch    - Channel of raw text messages from client
    response-ch   - Channel for response maps (will be encoded by adapter)
    execute-fn    - (fn [context query variables op-name] -> result)
    context-fn    - (fn [connection-params] -> context)
    subprotocol   - 'graphql-ws' or 'graphql-transport-ws'"
  [session-id keep-alive-ms ws-text-ch response-ch execute-fn context-fn subprotocol]
  (let [cleanup-ch (chan 10)
        ws-data-ch (chan 10)
        modern? (proto/modern? subprotocol)]

    ;; Parse incoming text to data
    (go-loop []
      (when-some [text (<! ws-text-ch)]
        (if-some [data (proto/decode text)]
          (>! ws-data-ch data)
          (>! response-ch (proto/connection-error-message {:message "Malformed JSON"})))
        (recur)))

    ;; Main state machine
    (go-loop [state {:subs {}
                     :connection-params nil
                     :initialized? false}]
      (alt!
        ;; Cleanup completed subscription
        cleanup-ch
        ([id]
         (log/trace ::cleanup {:session-id session-id :id id})
         (recur (update state :subs dissoc id)))

        ;; Keep-alive timeout
        (async/timeout keep-alive-ms)
        (do
          (log/trace ::keep-alive {:session-id session-id})
          (>! response-ch (if modern? (proto/pong-message) (proto/ka-message)))
          (recur state))

        ;; Client message
        ws-data-ch
        ([data]
         (if (nil? data)
           ;; Client disconnected
           (do
             (log/trace ::disconnect {:session-id session-id})
             (doseq [[_ shutdown-ch] (:subs state)]
               (close! shutdown-ch)))

           ;; Process message
           (let [{:keys [type id payload]} data]
             (log/trace ::message {:session-id session-id :type type :id id})
             (case type
               ;; Ping/Pong
               "ping"
               (do (>! response-ch (proto/pong-message payload))
                   (recur state))

               "pong"
               (recur state)

               ;; Connection init
               "connection_init"
               (do (>! response-ch (proto/ack-message))
                   (recur (assoc state
                                 :connection-params payload
                                 :initialized? true)))

               ;; Start operation
               ("start" "subscribe")
               (if (contains? (:subs state) id)
                 (do (log/trace ::duplicate-sub {:id id})
                     (recur state))
                 (let [context (context-fn (:connection-params state))
                       shutdown-ch (execute-operation
                                     id payload execute-fn context
                                     response-ch subprotocol cleanup-ch)]
                   (recur (assoc-in state [:subs id] shutdown-ch))))

               ;; Stop operation
               ("stop" "complete")
               (do (when-some [shutdown-ch (get-in state [:subs id])]
                     (close! shutdown-ch))
                   (recur state))

               ;; Terminate connection
               "connection_terminate"
               (do (log/trace ::terminate {:session-id session-id})
                   (doseq [[_ shutdown-ch] (:subs state)]
                     (close! shutdown-ch))
                   (close! response-ch))

               ;; Unknown
               (do (log/trace ::unknown-type {:type type :session-id session-id})
                   (when id
                     (>! response-ch (proto/error-message id "Unknown message type" {:type type})))
                   (recur state))))))))))

;; =============================================================================
;; Handler Factory
;; =============================================================================

(defn create-handler
  "Creates a subscription handler for use with WebSocket adapters.

  Returns a map with:
    :start    - (fn [ws-text-ch response-ch state] -> nil) Start connection loop
    :on-close - (fn [state] -> nil) Cleanup on close
    :make-state - (fn [session-id subprotocol] -> state) Create initial state

  Options:
    :execute-fn    - Required. (fn [ctx query vars op-name] -> result)
    :context-fn    - (fn [connection-params] -> context). Default: (constantly {})
    :keep-alive-ms - Keep-alive interval. Default: 25000"
  [{:keys [execute-fn context-fn keep-alive-ms]
    :or {context-fn (constantly {})
         keep-alive-ms 25000}}]
  {:pre [(fn? execute-fn)]}

  {:make-state
   (fn [session-id subprotocol]
     {:session-id session-id
      :subprotocol subprotocol
      :response-ch nil
      :ws-text-ch nil})

   :start
   (fn [ws-text-ch response-ch state]
     (connection-loop
       (:session-id state)
       keep-alive-ms
       ws-text-ch
       response-ch
       execute-fn
       context-fn
       (:subprotocol state)))

   :on-close
   (fn [state]
     (when-let [ch (:ws-text-ch state)]
       (close! ch))
     (when-let [ch (:response-ch state)]
       (close! ch)))})
