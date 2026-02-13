(ns synthigy.graphql.adapter.httpkit
  "http-kit adapter with async-ready structure.

   Current: Sync execution via Ring handler
   Future: True async with core.async + http-kit channels

   http-kit is Ring-compatible but also supports async responses
   via the 3-arity handler form: (fn [request respond raise])"
  (:require
    [clojure.core.async :as async]
    [synthigy.graphql.http :as http]))

;;; ============================================================================
;;; Sync Handlers (Ring-compatible)
;;; ============================================================================

(defn wrap-graphql
  "Ring middleware that handles GraphQL at specified path.
   Non-matching requests pass through to next handler.

   Same as ring adapter - http-kit is Ring-compatible for sync handlers."
  [handler path schema-provider opts]
  (let [graphql-handler (http/handler schema-provider opts)]
    (fn [request]
      (if (= (:uri request) path)
        (graphql-handler request)
        (handler request)))))

(defn graphql-handler
  "Sync Ring handler for GraphQL.
   Same as ring adapter - http-kit is Ring-compatible."
  [schema-provider opts]
  (http/handler schema-provider opts))

;;; ============================================================================
;;; Async Handler (http-kit specific)
;;; ============================================================================

(defn async-handler
  "Async Ring handler for http-kit.

   Uses 3-arity Ring async form: (fn [request respond raise])

   Current implementation: Executes on go block (still blocking internally)
   Future: True async with channel-based execution pipeline

   Benefits:
   - Frees http-kit worker thread during execution
   - Better for high-concurrency scenarios

   Args:
     schema-provider - Schema, function, atom, or ref
     opts            - Options (same as graphql-handler)

   Example:
     (org.httpkit.server/run-server
       (async-handler compiled-schema opts)
       {:port 8080})"
  [schema-provider opts]
  (let [sync-handler (http/handler schema-provider opts)]
    (fn [request respond raise]
      (async/go
        (try
          (respond (sync-handler request))
          (catch Throwable t
            (raise t)))))))

;;; ============================================================================
;;; Future: True Async Implementation
;;; ============================================================================

;; Future async implementation would:
;; 1. Make execute-pipeline return a channel
;; 2. Use resolve/on-deliver! with channel put instead of promise
;; 3. Never block the http-kit thread
;;
;; (defn async-handler-v2
;;   [schema-provider opts]
;;   (fn [request respond raise]
;;     (let [result-ch (http/execute-pipeline-async ...)]
;;       (async/go
;;         (try
;;           (let [result (async/<! result-ch)]
;;             (respond (http/format-response result)))
;;           (catch Throwable t
;;             (raise t)))))))
