(ns synthigy.ws.lacinia
  "Lacinia integration for WebSocket GraphQL subscriptions.

  Provides execute-fn factory and high-level subscription-handler API
  that works with any compiled Lacinia schema."
  (:require
    [clojure.core.async :as async :refer [chan put! close!]]
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia :as lacinia]
    [com.walmartlabs.lacinia.constants :as constants]
    [com.walmartlabs.lacinia.executor :as executor]
    [com.walmartlabs.lacinia.parser :as parser]
    [com.walmartlabs.lacinia.resolve :as resolve]
    [com.walmartlabs.lacinia.validator :as validator]
    [synthigy.ws.subscription :as sub]))

;; =============================================================================
;; Query Execution
;; =============================================================================

(defn- execute-query-or-mutation
  "Execute a query or mutation, returning the result map."
  [app-context schema prepared-query]
  (let [result-promise (promise)
        result (-> app-context
                   (assoc constants/parsed-query-key prepared-query)
                   (executor/execute-query schema prepared-query))]
    (resolve/on-deliver! result #(deliver result-promise %))
    @result-promise))

(defn- execute-subscription
  "Execute a subscription, returning a map with :source-stream and :cleanup-fn.

  The source-stream is a channel that receives resolved values from the streamer."
  [app-context schema prepared-query]
  (let [source-ch (chan 1)]
    (letfn [(source-stream [value]
              (cond
                (nil? value)
                (close! source-ch)

                (resolve/is-resolver-result? value)
                (resolve/on-deliver! value source-stream)

                :else
                (let [result (-> app-context
                                 (assoc constants/parsed-query-key prepared-query
                                        ::executor/resolved-value value)
                                 (executor/execute-query schema prepared-query))]
                  (resolve/on-deliver! result #(put! source-ch %)))))]
      (let [cleanup-fn (executor/invoke-streamer
                         (assoc app-context constants/parsed-query-key prepared-query)
                         source-stream)]
        {:source-stream source-ch
         :cleanup-fn cleanup-fn}))))

;; =============================================================================
;; Execute Function Factory
;; =============================================================================

(defn create-execute-fn
  "Creates an execute-fn for the subscription handler.

  The execute-fn has signature:
    (fn [context query variables operation-name] -> result)

  For queries/mutations, returns {:data {...}} or {:errors [...]}
  For subscriptions, returns {:source-stream ch :cleanup-fn fn}

  Args:
    schema-provider - Compiled schema or (fn [] schema)
    app-context     - Base application context merged into execution context"
  [schema-provider app-context]
  (fn [context query variables operation-name]
    (try
      (let [schema (if (fn? schema-provider)
                     (schema-provider)
                     schema-provider)
            parsed (parser/parse-query schema query operation-name)
            prepared (parser/prepare-with-query-variables parsed variables)
            errors (validator/validate schema prepared {})]
        (if (seq errors)
          {:errors errors}
          (let [operation-type (-> prepared parser/operations :type)
                merged-context (merge app-context context)]
            (if (= operation-type :subscription)
              (execute-subscription merged-context schema prepared)
              (execute-query-or-mutation merged-context schema prepared)))))
      (catch Throwable t
        (log/error t ::execute-error)
        {:errors [{:message (.getMessage t)
                   :extensions {:exception (str (class t))}}]}))))

;; =============================================================================
;; High-Level API
;; =============================================================================

(defn subscription-handler
  "Creates a subscription handler for Lacinia schemas.

  This is the high-level API - combines create-execute-fn with
  sub/create-handler for easy integration with adapters.

  Args:
    schema-provider - Compiled Lacinia schema or (fn [] schema)
    opts - Options map:
           :app-context    - Base application context for resolvers
           :context-fn     - (fn [connection-params] -> context) for per-connection context
           :keep-alive-ms  - Keep-alive interval (default: 25000)

  Returns:
    Handler map for use with WebSocket adapters"
  [schema-provider {:keys [app-context context-fn keep-alive-ms]
                    :or {app-context {}
                         keep-alive-ms 25000}
                    :as opts}]
  (let [execute-fn (create-execute-fn schema-provider app-context)
        ;; Wrap context-fn to merge connection params into context
        wrapped-context-fn (if context-fn
                             (fn [connection-params]
                               (merge (context-fn connection-params)
                                      {::lacinia/connection-params connection-params}))
                             (fn [connection-params]
                               {::lacinia/connection-params connection-params}))]
    (sub/create-handler
      {:execute-fn execute-fn
       :context-fn wrapped-context-fn
       :keep-alive-ms keep-alive-ms})))
