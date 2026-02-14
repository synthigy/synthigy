(ns synthigy.ws.protocol
  "Apollo GraphQL WebSocket protocol - message types and JSON encoding.

  Supports both protocol versions:
  - 'graphql-ws' (legacy subscriptions-transport-ws)
  - 'graphql-transport-ws' (modern graphql-ws library)

  Uses Jsonista for fast JSON encoding/decoding."
  (:require
    [jsonista.core :as json]))

;; =============================================================================
;; JSON Mapper
;; =============================================================================

(def ^:private mapper
  "Jsonista object mapper with keyword keys."
  (json/object-mapper {:decode-key-fn keyword}))

(defn encode
  "Encode Clojure map to JSON string."
  [m]
  (json/write-value-as-string m mapper))

(defn decode
  "Decode JSON string to Clojure map with keyword keys.
  Returns nil on parse error."
  [s]
  (try
    (json/read-value s mapper)
    (catch Exception _
      nil)))

;; =============================================================================
;; Message Types - Shared
;; =============================================================================

(def connection-init "connection_init")
(def connection-ack "connection_ack")
(def connection-error "connection_error")
(def connection-terminate "connection_terminate")

(def ping "ping")
(def pong "pong")
(def ka "ka")  ; keep-alive (legacy)

(def complete "complete")
(def error "error")

;; =============================================================================
;; Message Types - Protocol Specific
;; =============================================================================

;; Legacy (graphql-ws / subscriptions-transport-ws)
(def start "start")
(def stop "stop")
(def data-type "data")

;; Modern (graphql-transport-ws)
(def subscribe "subscribe")
(def next-type "next")

;; =============================================================================
;; Subprotocol Detection
;; =============================================================================

(def legacy-subprotocol "graphql-ws")
(def modern-subprotocol "graphql-transport-ws")

(def supported-subprotocols [legacy-subprotocol modern-subprotocol])

(defn modern?
  "Returns true if using modern graphql-transport-ws protocol."
  [subprotocol]
  (= subprotocol modern-subprotocol))

(defn response-type
  "Returns 'next' for modern protocol, 'data' for legacy."
  [subprotocol]
  (if (modern? subprotocol) next-type data-type))

;; =============================================================================
;; Message Constructors
;; =============================================================================

(defn ack-message
  "Connection acknowledgment."
  ([] {:type connection-ack})
  ([payload] {:type connection-ack :payload payload}))

(defn error-message
  "Error for an operation."
  ([id errors]
   {:type error
    :id id
    :payload (if (map? errors)
               errors
               {:errors (if (sequential? errors) errors [errors])})})
  ([id message details]
   {:type error
    :id id
    :payload {:errors [{:message message :extensions details}]}}))

(defn data-message
  "Data/next response for an operation."
  [subprotocol id payload]
  {:type (response-type subprotocol)
   :id id
   :payload payload})

(defn complete-message
  "Operation complete."
  [id]
  {:type complete :id id})

(defn ka-message
  "Keep-alive (legacy protocol)."
  []
  {:type ka})

(defn pong-message
  "Pong response."
  ([] {:type pong})
  ([payload] (cond-> {:type pong} payload (assoc :payload payload))))

(defn connection-error-message
  "Connection-level error."
  [error-map]
  {:type connection-error :payload error-map})
